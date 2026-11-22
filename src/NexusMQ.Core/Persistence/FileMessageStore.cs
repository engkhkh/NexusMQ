using System.Text.Json;
using System.Text.Json.Serialization;

namespace NexusMQ.Core.Persistence;

/// <summary>
/// Durable store using one append-only journal per entity (put/remove records), replayed on startup and compacted when it
/// holds many dead records. Journals are binary records with raw bodies (<see cref="BinaryRecords"/>) by default; JSON-lines
/// journals from earlier versions are still read, and converted to binary the next time they are loaded or compacted.
/// </summary>
public sealed class FileMessageStore : IMessageStore
{
    private sealed record JournalRecord(char Op, long Seq, BrokerMessage? Msg);

    private sealed class Journal(string path) : IDisposable
    {
        public readonly string FilePath = path;
        public readonly HashSet<long> Live = [];
        public long Records;
        public FileStream? File;
        /// <summary>Format of the file being appended to (decided when it is opened).</summary>
        public bool Binary;
        /// <summary>The file's tail is known to be intact (checked by a load or written by a rewrite).</summary>
        public bool TailChecked;

        public void Dispose()
        {
            File?.Dispose();
            File = null;
        }
    }

    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() },
        WriteIndented = false
    };
    private static readonly JsonSerializerOptions JsonNoBody = BinaryRecords.WithoutBodies(Json);

    private readonly string _root;
    private readonly bool _flushToDisk;
    private readonly int _compactionThreshold;
    private readonly bool _binary;
    private readonly Dictionary<string, Journal> _journals = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();
    /// <summary>Open <see cref="DeferFlush"/> scopes; while above zero, writes stay buffered until the scope ends.</summary>
    private int _deferDepth;
    private readonly HashSet<Journal> _dirty = [];

    /// <param name="directory">Folder where journals and topology.json are kept.</param>
    /// <param name="flushToDisk">Force an OS flush on every write (safest, slower).</param>
    /// <param name="compactionThreshold">Minimum journal records before compaction is considered.</param>
    /// <param name="format">How new and rewritten journals are written.</param>
    public FileMessageStore(string directory, bool flushToDisk = false, int compactionThreshold = 10_000,
        StorageFormat format = StorageFormat.Binary)
    {
        _root = Path.GetFullPath(directory);
        _flushToDisk = flushToDisk;
        _compactionThreshold = compactionThreshold;
        _binary = format == StorageFormat.Binary;
        Directory.CreateDirectory(Path.Combine(_root, "entities"));
        ConvertMismatchedJournals();
    }

    /// <summary>
    /// Bring every journal to the configured format at start-up, including ones nothing will load soon (a cluster follower
    /// only loads journals when it becomes leader). After one start with <c>Json</c>, every file is readable by older
    /// versions again; after one start with <c>Binary</c>, nothing is left in the old format. Only an 8-byte check per file
    /// when nothing needs converting.
    /// </summary>
    private void ConvertMismatchedJournals()
    {
        foreach (var file in Directory.GetFiles(Path.Combine(_root, "entities"), "*.journal"))
        {
            if (new FileInfo(file).Length == 0 || BinaryRecords.IsBinary(file) == _binary) continue;
            var entity = Path.GetFileNameWithoutExtension(file).Replace('~', '/');
            if (FileFor(entity) != file) continue; // name we cannot map back: leave it to a normal load
            Load(entity);
        }
    }

    private string FileFor(string entityPath)
    {
        // "orders/$deadletter" -> "orders~$deadletter.journal"
        var safe = entityPath.Replace('/', '~');
        foreach (var c in Path.GetInvalidFileNameChars()) safe = safe.Replace(c, '_');
        return Path.Combine(_root, "entities", safe + ".journal");
    }

    private Journal GetJournal(string entityPath)
    {
        if (!_journals.TryGetValue(entityPath, out var j))
        {
            j = new Journal(FileFor(entityPath));
            _journals[entityPath] = j;
        }
        return j;
    }

    /// <summary>
    /// Open the journal for appending. An existing file keeps its format (a JSON-lines file is converted when it is next
    /// loaded or compacted). A binary file whose last record was cut short by a crash is trimmed first, so new records are
    /// not written after the damaged one (where they could never be read).
    /// </summary>
    private FileStream Open(Journal j)
    {
        if (j.File is not null) return j.File;
        var info = new FileInfo(j.FilePath);
        if (info.Exists && info.Length > 0)
        {
            j.Binary = BinaryRecords.IsBinary(j.FilePath);
            if (j.Binary && !j.TailChecked)
            {
                long valid = 0;
                foreach (var _ in BinaryRecords.Read(j.FilePath, v => valid = v)) { }
                if (valid > 0 && valid < info.Length)
                    using (var trim = new FileStream(j.FilePath, FileMode.Open, FileAccess.Write)) trim.SetLength(valid);
            }
            j.TailChecked = true;
            j.File = new FileStream(j.FilePath, FileMode.Append, FileAccess.Write, FileShare.Read, 1 << 16);
        }
        else
        {
            j.Binary = _binary;
            j.File = new FileStream(j.FilePath, FileMode.Create, FileAccess.Write, FileShare.Read, 1 << 16);
            if (j.Binary) j.File.Write(BinaryRecords.Magic);
        }
        return j.File;
    }

    private static void WriteRecord(Stream stream, JournalRecord rec, bool binary)
    {
        if (!binary)
        {
            JsonSerializer.Serialize(stream, rec, Json); // UTF-8 straight to the file (no string in between)
            stream.WriteByte((byte)'\n');
            return;
        }
        if (rec.Msg is { } m)
            BinaryRecords.Write(stream, JsonSerializer.SerializeToUtf8Bytes(rec, JsonNoBody), m.Body, separateBody: true);
        else
            BinaryRecords.Write(stream, JsonSerializer.SerializeToUtf8Bytes(rec, Json), [], separateBody: false);
    }

    private void Append(Journal j, JournalRecord rec)
    {
        var f = Open(j);
        WriteRecord(f, rec, j.Binary);
        j.Records++;
        if (_deferDepth > 0) { _dirty.Add(j); return; }
        f.Flush(_flushToDisk);
    }

    /// <summary>
    /// Buffer writes until the returned scope is disposed, then write (and fsync) each touched journal once.
    /// Used when applying a batch of replicated changes: one flush per batch instead of one per change.
    /// </summary>
    public IDisposable DeferFlush()
    {
        lock (_gate) _deferDepth++;
        return new FlushScope(this);
    }

    /// <summary>Write out buffered changes now (also done when the last <see cref="DeferFlush"/> scope ends).</summary>
    public void Flush()
    {
        lock (_gate) FlushDirtyUnsafe();
    }

    private void FlushDirtyUnsafe()
    {
        foreach (var j in _dirty)
        {
            if (j.File is not { } f) continue; // closed (and so flushed) by Load/Compact/Delete in the meantime
            f.Flush(_flushToDisk);
        }
        _dirty.Clear();
    }

    private sealed class FlushScope(FileMessageStore store) : IDisposable
    {
        private int _done;
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _done, 1) == 1) return;
            lock (store._gate)
            {
                if (--store._deferDepth == 0) store.FlushDirtyUnsafe();
            }
        }
    }

    public void Put(string entityPath, BrokerMessage message)
    {
        lock (_gate)
        {
            var j = GetJournal(entityPath);
            var copy = message.Clone();
            if (copy.State == MessageState.Locked) copy.State = MessageState.Active;
            Append(j, new JournalRecord('P', message.SequenceNumber, copy));
            j.Live.Add(message.SequenceNumber);
        }
    }

    public void Remove(string entityPath, long sequenceNumber)
    {
        lock (_gate)
        {
            var j = GetJournal(entityPath);
            if (!j.Live.Remove(sequenceNumber)) return;
            Append(j, new JournalRecord('R', sequenceNumber, null));
        }
    }

    public IEnumerable<BrokerMessage> Load(string entityPath)
    {
        lock (_gate)
        {
            var j = GetJournal(entityPath);
            if (j.File is not null) j.File.Flush(_flushToDisk);
            j.Dispose();
            var messages = new SortedDictionary<long, BrokerMessage>();
            long records = 0;
            long valid = -1;
            var records0 = ReadJournal(j.FilePath, out var fileIsBinary, v => valid = v);
            foreach (var rec in records0)
            {
                records++;
                if (rec.Op == 'P' && rec.Msg is not null) messages[rec.Seq] = rec.Msg;
                else if (rec.Op == 'R') messages.Remove(rec.Seq);
            }
            j.Live.Clear();
            foreach (var seq in messages.Keys) j.Live.Add(seq);
            j.Records = records;
            if (valid >= 0 && valid < new FileInfo(j.FilePath).Length) // damaged tail after a crash: cut it off
                using (var trim = new FileStream(j.FilePath, FileMode.Open, FileAccess.Write)) trim.SetLength(valid);
            j.TailChecked = true;
            // A journal in the other format (JSON lines from an older version, or binary after switching to Json): convert it now.
            if (records > 0 && fileIsBinary != _binary) RewriteUnsafe(j, messages.Values);
            return messages.Values.ToList();
        }
    }

    /// <summary>Every readable record of a journal, in either format. A damaged tail (crash mid-write) is skipped.</summary>
    private static IEnumerable<JournalRecord> ReadJournal(string path, out bool isBinary, Action<long>? validBytes = null)
    {
        isBinary = false;
        if (!File.Exists(path)) return [];
        isBinary = BinaryRecords.IsBinary(path);
        return isBinary ? ReadBinary(path, validBytes) : ReadJsonLines(path);
    }

    private static IEnumerable<JournalRecord> ReadBinary(string path, Action<long>? validBytes = null)
    {
        foreach (var r in BinaryRecords.Read(path, validBytes))
        {
            JournalRecord? rec;
            try { rec = JsonSerializer.Deserialize<JournalRecord>(r.Json, r.SeparateBody ? JsonNoBody : Json); }
            catch (JsonException) { continue; } // intact record (checksum ok) we cannot understand: skip it, keep reading
            if (rec is null) continue;
            if (r.SeparateBody && rec.Msg is not null) rec.Msg.Body = r.Body;
            yield return rec;
        }
    }

    private static IEnumerable<JournalRecord> ReadJsonLines(string path)
    {
        foreach (var line in File.ReadLines(path))
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            JournalRecord? rec;
            try { rec = JsonSerializer.Deserialize<JournalRecord>(line, Json); }
            catch (JsonException) { continue; } // torn last line after a crash
            if (rec is not null) yield return rec;
        }
    }

    public void DeleteEntity(string entityPath)
    {
        lock (_gate)
        {
            if (_journals.Remove(entityPath, out var j)) j.Dispose();
            var file = FileFor(entityPath);
            if (File.Exists(file)) File.Delete(file);
        }
    }

    public bool NeedsCompaction(string entityPath)
    {
        lock (_gate)
        {
            return _journals.TryGetValue(entityPath, out var j)
                   && j.Records >= _compactionThreshold
                   && j.Records > j.Live.Count * 2;
        }
    }

    public void Compact(string entityPath, IEnumerable<BrokerMessage> liveMessages)
    {
        lock (_gate) RewriteUnsafe(GetJournal(entityPath), liveMessages);
    }

    /// <summary>Replace the journal with just the live messages, in the configured format (fsynced, then renamed into place).</summary>
    private void RewriteUnsafe(Journal j, IEnumerable<BrokerMessage> liveMessages)
    {
        if (j.File is not null) j.File.Flush(_flushToDisk);
        j.Dispose();
        _dirty.Remove(j);
        var tmp = j.FilePath + ".tmp";
        var live = new HashSet<long>();
        long records = 0;
        using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 16))
        {
            if (_binary) fs.Write(BinaryRecords.Magic);
            foreach (var m in liveMessages)
            {
                var copy = m.Clone();
                if (copy.State == MessageState.Locked) copy.State = MessageState.Active;
                WriteRecord(fs, new JournalRecord('P', m.SequenceNumber, copy), _binary);
                live.Add(m.SequenceNumber);
                records++;
            }
            fs.Flush(true);
        }
        File.Move(tmp, j.FilePath, overwrite: true);
        j.Live.Clear();
        j.Live.UnionWith(live);
        j.Records = records;
        j.TailChecked = true;
    }

    public void SaveTopology(TopologySnapshot topology)
    {
        lock (_gate)
        {
            var path = Path.Combine(_root, "topology.json");
            var tmp = path + ".tmp";
            File.WriteAllText(tmp, JsonSerializer.Serialize(topology, new JsonSerializerOptions(Json) { WriteIndented = true }));
            File.Move(tmp, path, overwrite: true);
        }
    }

    public TopologySnapshot? LoadTopology()
    {
        lock (_gate)
        {
            var path = Path.Combine(_root, "topology.json");
            return File.Exists(path) ? JsonSerializer.Deserialize<TopologySnapshot>(File.ReadAllText(path), Json) : null;
        }
    }

    public IEnumerable<string> ListEntities()
    {
        lock (_gate)
        {
            return Directory.GetFiles(Path.Combine(_root, "entities"), "*.journal")
                .Select(f => Path.GetFileNameWithoutExtension(f).Replace('~', '/'))
                .ToList();
        }
    }

    public void Clear()
    {
        lock (_gate)
        {
            FlushDirtyUnsafe();
            foreach (var j in _journals.Values) j.Dispose();
            _journals.Clear();
            foreach (var f in Directory.GetFiles(Path.Combine(_root, "entities"))) File.Delete(f);
            var topology = Path.Combine(_root, "topology.json");
            if (File.Exists(topology)) File.Delete(topology);
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            FlushDirtyUnsafe();
            foreach (var j in _journals.Values) j.Dispose();
            _journals.Clear();
        }
    }
}
