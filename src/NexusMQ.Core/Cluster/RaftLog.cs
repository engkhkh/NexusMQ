using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using NexusMQ.Core.Replication;
// ClusterMember lives in RaftNode.cs (same namespace).

namespace NexusMQ.Core.Cluster;

/// <summary>
/// One Raft log entry: a store change (<see cref="Op"/>), a membership change (<see cref="Config"/>), or neither
/// (the no-op a new leader appends).
/// </summary>
public sealed class RaftEntry
{
    public long Index { get; set; }
    public long Term { get; set; }
    public ReplicationOp? Op { get; set; }
    /// <summary>The full voting membership from this entry on (takes effect as soon as it is in the log).</summary>
    public List<ClusterMember>? Config { get; set; }
}

/// <summary>
/// Durable Raft log: <c>raft-log.bin</c> (binary records; <c>raft-log.jsonl</c> JSON lines with <see cref="NexusMQ.Core.Persistence.StorageFormat.Json"/>),
/// a header with the snapshot base followed by the entries, and <c>raft-state.json</c>
/// (current term, vote, last applied index). Entries up to <see cref="SnapshotIndex"/> are already reflected in the
/// message store and have been discarded.
/// </summary>
public sealed class RaftLog : IDisposable
{
    internal static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() }
    };

    private sealed record Header(long SnapshotIndex, long SnapshotTerm, List<ClusterMember>? Config = null);
    private sealed record State(long Term, string? VotedFor, long LastApplied);

    private readonly string _logFile;
    private readonly bool _binary;
    private static readonly JsonSerializerOptions JsonNoBody = NexusMQ.Core.Persistence.BinaryRecords.WithoutBodies(Json);
    private readonly string _stateFile;
    private readonly bool _flushToDisk;
    private readonly List<RaftEntry> _entries = [];
    private FileStream? _file;
    /// <summary>Serialized entries appended but not yet written to the file (guarded by the caller's lock).</summary>
    private MemoryStream _pending = new();
    private long _pendingIndex;
    /// <summary>Bumped by every rewrite of the file: pending bytes captured before a rewrite are already in it.</summary>
    private long _generation;
    /// <summary>Only one writer touches the file at a time (the flusher, or a rewrite).</summary>
    private readonly Lock _fileGate = new();
    private long _durableIndex;

    public long SnapshotIndex { get; private set; }
    public long SnapshotTerm { get; private set; }
    /// <summary>Membership in effect at <see cref="SnapshotIndex"/> (null if never recorded).</summary>
    public List<ClusterMember>? SnapshotConfig { get; private set; }
    public long CurrentTerm { get; private set; }
    public string? VotedFor { get; private set; }
    public long LastApplied { get; private set; }

    public long LastIndex => SnapshotIndex + _entries.Count;
    /// <summary>
    /// Highest entry known to be in the file (and on disk when flushing to disk). A leader counts itself towards a
    /// majority only up to here, and a follower acknowledges entries only once they are durable.
    /// </summary>
    public long DurableIndex => Interlocked.Read(ref _durableIndex);
    public long LastTerm => _entries.Count > 0 ? _entries[^1].Term : SnapshotTerm;
    public int Count => _entries.Count;

    /// <param name="directory">Folder for the log and its state file.</param>
    /// <param name="flushToDisk">fsync each write batch.</param>
    /// <param name="format">
    /// Binary (default): <c>raft-log.bin</c>, records with raw message bodies. Json: <c>raft-log.jsonl</c> as before 1.13.
    /// Either file is read; if it is not in the configured format it is converted on open (and the old file removed).
    /// </param>
    public RaftLog(string directory, bool flushToDisk = false, NexusMQ.Core.Persistence.StorageFormat format = NexusMQ.Core.Persistence.StorageFormat.Binary)
    {
        Directory.CreateDirectory(directory);
        var binFile = Path.Combine(directory, "raft-log.bin");
        var jsonFile = Path.Combine(directory, "raft-log.jsonl");
        _binary = format == NexusMQ.Core.Persistence.StorageFormat.Binary;
        _logFile = _binary ? binFile : jsonFile;
        _stateFile = Path.Combine(directory, "raft-state.json");
        _flushToDisk = flushToDisk;

        if (File.Exists(_stateFile))
        {
            var st = JsonSerializer.Deserialize<State>(File.ReadAllText(_stateFile), Json)!;
            (CurrentTerm, VotedFor, LastApplied) = (st.Term, st.VotedFor, st.LastApplied);
        }
        // A finished conversion leaves only the new file; after a crash in the middle of one, the .bin is already complete.
        var source = File.Exists(binFile) ? binFile : File.Exists(jsonFile) ? jsonFile : null;
        var damaged = false;
        if (source == binFile) damaged = LoadBinary(binFile);
        else if (source == jsonFile) LoadJsonLines(jsonFile);
        if (source is null || source != _logFile || damaged)
        {
            Rewrite(); // new log, a format change, or a cut-off tail: write the whole file again from memory
            if (source is not null && source != _logFile) File.Delete(source);
        }
        if (LastApplied < SnapshotIndex) LastApplied = SnapshotIndex;
        _durableIndex = LastIndex;
    }

    private void LoadJsonLines(string file)
    {
        var first = true;
        foreach (var line in File.ReadLines(file))
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            try
            {
                if (first)
                {
                    var h = JsonSerializer.Deserialize<Header>(line, Json)!;
                    (SnapshotIndex, SnapshotTerm, SnapshotConfig) = (h.SnapshotIndex, h.SnapshotTerm, h.Config);
                    first = false;
                    continue;
                }
                var e = JsonSerializer.Deserialize<RaftEntry>(line, Json)!;
                if (e.Index == LastIndex + 1) _entries.Add(e); // ignore anything out of order (torn writes)
            }
            catch (JsonException)
            {
                break; // torn last line after a crash
            }
        }
    }

    /// <summary>Read a binary log. True when its end was damaged (cut-off or corrupt record), so it must be rewritten.</summary>
    private bool LoadBinary(string file)
    {
        long valid = 0;
        var first = true;
        var damaged = false;
        foreach (var r in NexusMQ.Core.Persistence.BinaryRecords.Read(file, v => valid = v))
        {
            try
            {
                if (first)
                {
                    var h = JsonSerializer.Deserialize<Header>(r.Json, Json)!;
                    (SnapshotIndex, SnapshotTerm, SnapshotConfig) = (h.SnapshotIndex, h.SnapshotTerm, h.Config);
                    first = false;
                    continue;
                }
                var e = JsonSerializer.Deserialize<RaftEntry>(r.Json, r.SeparateBody ? JsonNoBody : Json)!;
                if (r.SeparateBody && e.Op?.Message is { } m) m.Body = r.Body;
                if (e.Index == LastIndex + 1) _entries.Add(e);
                else damaged = true;
            }
            catch (JsonException) { damaged = true; }
        }
        return damaged || valid < new FileInfo(file).Length;
    }

    /// <summary>Serialize one entry in this log's format (binary: JSON without the body, then the raw body).</summary>
    private void WriteEntry(Stream stream, RaftEntry entry)
    {
        if (!_binary)
        {
            JsonSerializer.Serialize(stream, entry, Json);
            stream.WriteByte((byte)'\n');
            return;
        }
        if (entry.Op is { Message: { } m, Messages: null })
            NexusMQ.Core.Persistence.BinaryRecords.Write(stream, JsonSerializer.SerializeToUtf8Bytes(entry, JsonNoBody), m.Body, separateBody: true);
        else
            NexusMQ.Core.Persistence.BinaryRecords.Write(stream, JsonSerializer.SerializeToUtf8Bytes(entry, Json), [], separateBody: false);
    }

    public long TermAt(long index)
    {
        if (index == 0) return 0;
        if (index == SnapshotIndex) return SnapshotTerm;
        if (index < SnapshotIndex || index > LastIndex) return -1;
        return _entries[(int)(index - SnapshotIndex - 1)].Term;
    }

    public RaftEntry Get(long index) => _entries[(int)(index - SnapshotIndex - 1)];

    /// <summary>The membership in effect at <paramref name="index"/>: the last config entry at or before it, else the snapshot's.</summary>
    public (List<ClusterMember>? Config, long Index) ConfigAt(long index)
    {
        for (var i = Math.Min(index, LastIndex); i > SnapshotIndex; i--)
            if (Get(i).Config is { } c) return (c, i);
        return (SnapshotConfig, SnapshotConfig is null ? 0 : SnapshotIndex);
    }

    /// <summary>The latest membership in the log (in effect now, committed or not).</summary>
    public (List<ClusterMember>? Config, long Index) LatestConfig() => ConfigAt(LastIndex);

    /// <summary>Entries from <paramref name="from"/> (inclusive), at most <paramref name="max"/>.</summary>
    public List<RaftEntry> Slice(long from, int max)
    {
        if (from <= SnapshotIndex) from = SnapshotIndex + 1;
        var start = (int)(from - SnapshotIndex - 1);
        if (start >= _entries.Count) return [];
        return _entries.GetRange(start, Math.Min(max, _entries.Count - start));
    }

    /// <summary>
    /// Add an entry in memory and queue it for the file. It becomes durable at the next <see cref="Flush"/> (group commit:
    /// one write, and one fsync when flushing to disk, for everything appended since the last flush).
    /// Call under the owner's lock, like every other mutating member except <see cref="Flush"/>.
    /// </summary>
    public void Append(RaftEntry entry)
    {
        _entries.Add(entry);
        WriteEntry(_pending, entry);
        _pendingIndex = entry.Index;
    }

    /// <summary>True when appended entries are waiting for <see cref="Flush"/>.</summary>
    public bool HasPending => _pending.Length > 0;

    /// <summary>
    /// Take the queued bytes for a flush (under the owner's lock). Pass the result to <see cref="Flush"/>, which may then
    /// run outside the lock so new entries can be appended while the disk works.
    /// </summary>
    public PendingWrite TakePending()
    {
        var p = new PendingWrite(_pending, _pendingIndex, _generation);
        _pending = new MemoryStream();
        return p;
    }

    public readonly record struct PendingWrite(MemoryStream Bytes, long UpToIndex, long Generation);

    /// <summary>Write (and fsync, if configured) queued entries. Safe to call outside the owner's lock.</summary>
    public void Flush(PendingWrite p)
    {
        lock (_fileGate)
        {
            // A rewrite since TakePending already wrote these entries (or truncated them away): nothing to do.
            if (p.Generation != _generation) return;
            if (p.Bytes.Length > 0)
            {
                var f = _file ??= new FileStream(_logFile, FileMode.Append, FileAccess.Write, FileShare.Read, 1, FileOptions.None);
                f.Write(p.Bytes.GetBuffer(), 0, (int)p.Bytes.Length);
                if (_flushToDisk) f.Flush(true);
            }
            if (p.UpToIndex > DurableIndex) Interlocked.Exchange(ref _durableIndex, p.UpToIndex);
        }
    }

    /// <summary>Flush everything queued so far, right now (under the owner's lock).</summary>
    public void FlushNow() => Flush(TakePending());

    /// <summary>Drop entries from <paramref name="index"/> onwards (conflicting uncommitted entries).</summary>
    public void TruncateFrom(long index)
    {
        var keep = (int)Math.Max(0, index - SnapshotIndex - 1);
        if (keep >= _entries.Count) return;
        _entries.RemoveRange(keep, _entries.Count - keep);
        Rewrite();
    }

    /// <summary>Discard entries up to <paramref name="index"/> (already applied to the store).</summary>
    public void CompactTo(long index)
    {
        if (index <= SnapshotIndex || index > LastIndex) return;
        var term = TermAt(index);
        var config = ConfigAt(index).Config;
        _entries.RemoveRange(0, (int)(index - SnapshotIndex));
        (SnapshotIndex, SnapshotTerm, SnapshotConfig) = (index, term, config);
        Rewrite();
    }

    /// <summary>Replace everything with a snapshot base (after installing a snapshot from the leader).</summary>
    public void ResetToSnapshot(long index, long term, List<ClusterMember>? config)
    {
        _entries.Clear();
        (SnapshotIndex, SnapshotTerm, SnapshotConfig) = (index, term, config);
        LastApplied = index;
        Rewrite();
        SaveState();
    }

    public void SetTerm(long term, string? votedFor)
    {
        (CurrentTerm, VotedFor) = (term, votedFor);
        SaveState();
    }

    public void SetLastApplied(long index, bool persist)
    {
        LastApplied = index;
        if (persist) SaveState();
    }

    public void SaveState()
    {
        var tmp = _stateFile + ".tmp";
        File.WriteAllText(tmp, JsonSerializer.Serialize(new State(CurrentTerm, VotedFor, LastApplied), Json));
        File.Move(tmp, _stateFile, overwrite: true);
    }

    /// <summary>Write the whole log file again from memory (recovery after a failed flush).</summary>
    public void RewriteAll() => Rewrite();

    private void Rewrite()
    {
        lock (_fileGate)
        {
            _file?.Dispose();
            _file = null;
            _generation++;
            _pending = new MemoryStream(); // everything in memory is written below
            var tmp = _logFile + ".tmp";
            using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 16))
            {
                var header = JsonSerializer.SerializeToUtf8Bytes(new Header(SnapshotIndex, SnapshotTerm, SnapshotConfig), Json);
                if (_binary)
                {
                    fs.Write(NexusMQ.Core.Persistence.BinaryRecords.Magic);
                    NexusMQ.Core.Persistence.BinaryRecords.Write(fs, header, [], separateBody: false);
                }
                else
                {
                    fs.Write(header);
                    fs.WriteByte((byte)'\n');
                }
                foreach (var e in _entries) WriteEntry(fs, e);
                fs.Flush(_flushToDisk);
            }
            File.Move(tmp, _logFile, overwrite: true);
            Interlocked.Exchange(ref _durableIndex, LastIndex);
        }
    }

    public void Dispose()
    {
        FlushNow();
        lock (_fileGate)
        {
            _file?.Dispose();
            _file = null;
        }
    }
}
