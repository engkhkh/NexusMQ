using NexusMQ.Core;
using NexusMQ.Core.Cluster;
using NexusMQ.Core.Persistence;
using NexusMQ.Core.Replication;

namespace NexusMQ.Tests;

/// <summary>Phase 15: binary journals and Raft log (raw bodies), reading and converting the JSON-lines files of older versions.</summary>
public sealed class Phase15Tests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "nexusmq-p15-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, true); } catch { }
    }

    private string Journal(string dir) => Path.Combine(dir, "entities", "q.journal");

    private static readonly byte[][] Bodies =
    [
        [], [0xFF], [0x00, 0xC3, 0x28], // empty, one byte, invalid UTF-8
        System.Text.Encoding.UTF8.GetBytes("héllo ✓"),
        Enumerable.Range(0, 200_000).Select(i => (byte)(i * 7)).ToArray()
    ];

    private static BrokerMessage Msg(long seq, byte[] body) => new()
    {
        SequenceNumber = seq, Body = body, Subject = $"s{seq}", ApplicationProperties = { ["k"] = $"v{seq}" },
        EnqueuedTimeUtc = DateTimeOffset.UnixEpoch.AddSeconds(seq)
    };

    private static void AssertSameMessages(IEnumerable<BrokerMessage> actual, IEnumerable<(long Seq, byte[] Body)> expected)
    {
        var a = actual.ToList();
        var e = expected.ToList();
        Assert.Equal(e.Select(x => x.Seq), a.Select(m => m.SequenceNumber));
        foreach (var (m, x) in a.Zip(e))
        {
            Assert.Equal(x.Body, m.Body);
            Assert.Equal($"s{x.Seq}", m.Subject);
            Assert.Equal($"v{x.Seq}", m.ApplicationProperties["k"]);
            Assert.Equal(DateTimeOffset.UnixEpoch.AddSeconds(x.Seq), m.EnqueuedTimeUtc);
        }
    }

    [Fact]
    public void Binary_journal_keeps_every_body_byte_for_byte_and_is_smaller_than_json()
    {
        var bin = Path.Combine(_root, "bin");
        var json = Path.Combine(_root, "json");
        foreach (var (dir, format) in new[] { (bin, StorageFormat.Binary), (json, StorageFormat.Json) })
        {
            using var store = new FileMessageStore(dir, format: format);
            for (var i = 0; i < Bodies.Length; i++) store.Put("q", Msg(i + 1, Bodies[i]));
            store.Remove("q", 2);
        }
        Assert.True(BinaryRecords.IsBinary(Journal(bin)));
        Assert.False(BinaryRecords.IsBinary(Journal(json)));
        Assert.True(new FileInfo(Journal(bin)).Length < new FileInfo(Journal(json)).Length * 0.8,
            $"binary {new FileInfo(Journal(bin)).Length} B vs json {new FileInfo(Journal(json)).Length} B");

        using var reopened = new FileMessageStore(bin);
        AssertSameMessages(reopened.Load("q"), Bodies.Select((b, i) => (Seq: (long)(i + 1), Body: b)).Where(x => x.Seq != 2));
    }

    [Theory]
    [InlineData(StorageFormat.Json, StorageFormat.Binary)] // upgrade: an older version's journal
    [InlineData(StorageFormat.Binary, StorageFormat.Json)] // going back: Json setting before a downgrade
    public void Journal_in_the_other_format_is_read_and_converted_on_load(StorageFormat written, StorageFormat configured)
    {
        var dir = Path.Combine(_root, "conv");
        using (var old = new FileMessageStore(dir, format: written))
            for (var i = 0; i < Bodies.Length; i++) old.Put("q", Msg(i + 1, Bodies[i]));

        using (var store = new FileMessageStore(dir, format: configured))
        {
            AssertSameMessages(store.Load("q"), Bodies.Select((b, i) => ((long)(i + 1), b)));
            Assert.Equal(configured == StorageFormat.Binary, BinaryRecords.IsBinary(Journal(dir))); // converted
            store.Put("q", Msg(99, [9, 9]));                                                    // and keeps appending in it
        }
        using var again = new FileMessageStore(dir, format: configured);
        var all = again.Load("q").ToList();
        Assert.Equal(Bodies.Length + 1, all.Count);
        Assert.Equal(new byte[] { 9, 9 }, all[^1].Body);
    }

    [Theory]
    [InlineData(StorageFormat.Json, StorageFormat.Binary)]
    [InlineData(StorageFormat.Binary, StorageFormat.Json)]
    public void Every_journal_is_converted_at_start_up_even_if_nothing_loads_it(StorageFormat written, StorageFormat configured)
    {
        var dir = Path.Combine(_root, "startup");
        using (var old = new FileMessageStore(dir, format: written))
        {
            old.Put("q", Msg(1, [1, 2, 3]));
            old.Put("orders/$deadletter", Msg(2, [4]));
        }
        using (new FileMessageStore(dir, format: configured)) { } // just start (a cluster follower never loads its journals)

        foreach (var file in Directory.GetFiles(Path.Combine(dir, "entities"), "*.journal"))
            Assert.Equal(configured == StorageFormat.Binary, BinaryRecords.IsBinary(file));
        using var check = new FileMessageStore(dir, format: written == StorageFormat.Json ? StorageFormat.Binary : StorageFormat.Json);
        Assert.Equal(new byte[] { 4 }, check.Load("orders/$deadletter").Single().Body);
    }

    [Fact]
    public void Journal_cut_off_by_a_crash_keeps_its_good_records_and_new_ones_are_not_lost_behind_the_damage()
    {
        var dir = Path.Combine(_root, "torn");
        using (var store = new FileMessageStore(dir))
            for (var i = 1; i <= 3; i++) store.Put("q", Msg(i, [(byte)i]));
        var file = Journal(dir);
        var good = new FileInfo(file).Length;
        using (var fs = new FileStream(file, FileMode.Append)) fs.Write([0x40, 0, 0, 0, 1, 2, 3]); // half a record

        // A follower appends without loading first: the damaged tail must be cut before writing.
        using (var store = new FileMessageStore(dir)) store.Put("q", Msg(4, [4]));
        Assert.True(new FileInfo(file).Length > good);
        using (var store = new FileMessageStore(dir))
            Assert.Equal([1L, 2, 3, 4], store.Load("q").Select(m => m.SequenceNumber));

        // Same through a load (the broker at start-up).
        using (var fs = new FileStream(file, FileMode.Append)) fs.Write([0x40, 0, 0]);
        using (var store = new FileMessageStore(dir))
        {
            Assert.Equal(4, store.Load("q").Count());
            store.Put("q", Msg(5, [5]));
        }
        using (var store = new FileMessageStore(dir))
            Assert.Equal([1L, 2, 3, 4, 5], store.Load("q").Select(m => m.SequenceNumber));
    }

    [Fact]
    public void Corrupted_record_is_detected_by_its_checksum()
    {
        var dir = Path.Combine(_root, "crc");
        using (var store = new FileMessageStore(dir))
            for (var i = 1; i <= 3; i++) store.Put("q", Msg(i, Enumerable.Repeat((byte)i, 1000).ToArray()));
        var file = Journal(dir);
        var bytes = File.ReadAllBytes(file);
        var at = Array.LastIndexOf(bytes, (byte)3); // inside the last record's body
        bytes[at] ^= 0x55;
        File.WriteAllBytes(file, bytes);

        using var store2 = new FileMessageStore(dir);
        Assert.Equal([1L, 2], store2.Load("q").Select(m => m.SequenceNumber)); // never a damaged body
    }

    private static RaftEntry Entry(long index, byte[] body) => new()
    {
        Index = index, Term = 1,
        Op = new ReplicationOp { Kind = ReplicationOpKind.Put, Entity = "q", Seq = index, Message = Msg(index, body) }
    };

    [Fact]
    public void Raft_log_converts_from_json_lines_and_back_without_losing_entries()
    {
        var dir = Path.Combine(_root, "raft");
        using (var log = new RaftLog(dir, format: StorageFormat.Json))
        {
            for (var i = 0; i < Bodies.Length; i++) log.Append(Entry(i + 1, Bodies[i]));
            log.Append(new RaftEntry { Index = Bodies.Length + 1, Term = 1, Config = [new ClusterMember { Id = "a", Url = "http://a" }] });
        }
        Assert.True(File.Exists(Path.Combine(dir, "raft-log.jsonl")));

        void Check(RaftLog log, int extra = 0)
        {
            Assert.Equal(Bodies.Length + 1 + extra, log.LastIndex);
            for (var i = 0; i < Bodies.Length; i++)
            {
                Assert.Equal(Bodies[i], log.Get(i + 1).Op!.Message!.Body);
                Assert.Equal($"v{i + 1}", log.Get(i + 1).Op!.Message!.ApplicationProperties["k"]);
            }
            Assert.Equal("a", log.Get(Bodies.Length + 1).Config!.Single().Id);
        }

        using (var log = new RaftLog(dir)) // binary by default: converts
        {
            Check(log);
            log.Append(Entry(Bodies.Length + 2, [7]));
        }
        Assert.True(File.Exists(Path.Combine(dir, "raft-log.bin")));
        Assert.False(File.Exists(Path.Combine(dir, "raft-log.jsonl")));

        using (var log = new RaftLog(dir, format: StorageFormat.Json)) // and back
        {
            Assert.Equal(Bodies.Length + 2, log.LastIndex);
            Assert.Equal(new byte[] { 7 }, log.Get(Bodies.Length + 2).Op!.Message!.Body);
        }
        Assert.False(File.Exists(Path.Combine(dir, "raft-log.bin")));
        using (var log = new RaftLog(dir, format: StorageFormat.Json)) Check(log, extra: 1);
    }

    [Fact]
    public void Raft_log_with_a_cut_off_tail_keeps_its_good_entries_and_appends_after_them()
    {
        var dir = Path.Combine(_root, "raft-torn");
        using (var log = new RaftLog(dir))
            for (var i = 1; i <= 5; i++) log.Append(Entry(i, [(byte)i]));
        var file = Path.Combine(dir, "raft-log.bin");
        var bytes = File.ReadAllBytes(file);
        File.WriteAllBytes(file, bytes[..^3]); // the last record loses its end

        using (var log = new RaftLog(dir))
        {
            Assert.Equal(4, log.LastIndex);
            log.Append(Entry(5, [50]));
        }
        using var again = new RaftLog(dir);
        Assert.Equal(5, again.LastIndex);
        Assert.Equal(new byte[] { 50 }, again.Get(5).Op!.Message!.Body);
    }
}
