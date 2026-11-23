using System.Buffers.Binary;
using NexusMQ.AspNetCore;
using NexusMQ.Core;
using NexusMQ.Core.Cluster;
using NexusMQ.Core.Replication;

namespace NexusMQ.Tests;

/// <summary>Phase 16: AppendEntries between servers with raw message bodies.</summary>
public sealed class Phase16Tests
{
    private static RaftEntry Put(long i, byte[] body) => new()
    {
        Index = i, Term = 2,
        Op = new ReplicationOp { Kind = ReplicationOpKind.Put, Entity = "q", Seq = i,
            Message = new BrokerMessage { SequenceNumber = i, Body = body, Subject = $"s{i}", ApplicationProperties = { ["k"] = $"v{i}" } } }
    };

    private static AppendRequest Sample()
    {
        var big = new byte[100_000];
        new Random(3).NextBytes(big);
        return new AppendRequest(2, "leader", 10, 1,
        [
            Put(11, [0xFF, 0x00]),
            new RaftEntry { Index = 12, Term = 2, Op = new ReplicationOp { Kind = ReplicationOpKind.Remove, Entity = "q", Seq = 11 } },
            Put(13, []),
            new RaftEntry { Index = 14, Term = 2, Config = [new ClusterMember { Id = "a", Url = "http://a" }] },
            Put(15, big)
        ], 12);
    }

    [Fact]
    public void Append_with_raw_bodies_round_trips_every_field_and_byte()
    {
        var original = Sample();
        var encoded = RaftFrames.TryEncodeAppend(original);
        Assert.NotNull(encoded);
        var decoded = RaftFrames.DecodeAppend(encoded);

        Assert.Equal((original.Term, original.LeaderId, original.PrevLogIndex, original.PrevLogTerm, original.LeaderCommit),
            (decoded.Term, decoded.LeaderId, decoded.PrevLogIndex, decoded.PrevLogTerm, decoded.LeaderCommit));
        Assert.Equal(original.Entries.Select(e => e.Index), decoded.Entries.Select(e => e.Index));
        foreach (var (a, b) in original.Entries.Zip(decoded.Entries))
        {
            Assert.Equal(a.Op?.Kind, b.Op?.Kind);
            Assert.Equal(a.Op?.Message?.Body, b.Op?.Message?.Body);
            Assert.Equal(a.Op?.Message?.ApplicationProperties["k"], b.Op?.Message?.ApplicationProperties["k"]);
            Assert.Equal(a.Config?.Single().Id, b.Config?.Single().Id);
        }
        // Smaller than JSON with base64 bodies.
        var json = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(original, NexusReplicationStream.Json);
        Assert.True(encoded!.Length < json.Length * 0.8, $"binary {encoded.Length} B vs json {json.Length} B");
    }

    [Fact]
    public void Entries_carrying_several_messages_are_left_to_json()
    {
        var r = new AppendRequest(1, "l", 0, 0,
            [new RaftEntry { Index = 1, Term = 1, Op = new ReplicationOp { Kind = ReplicationOpKind.Replace, Entity = "q",
                Messages = [new BrokerMessage { SequenceNumber = 1, Body = [1] }] } }], 0);
        Assert.Null(RaftFrames.TryEncodeAppend(r));
    }

    [Fact]
    public void Malformed_frames_are_rejected()
    {
        var good = RaftFrames.TryEncodeAppend(Sample())!;
        Assert.Throws<InvalidDataException>(() => RaftFrames.DecodeAppend(good.AsSpan(0, 5)));          // too short
        Assert.Throws<InvalidDataException>(() => RaftFrames.DecodeAppend(good.AsSpan(0, good.Length - 1))); // body cut off
        var extra = good.Concat(new byte[] { 0 }).ToArray();
        Assert.Throws<InvalidDataException>(() => RaftFrames.DecodeAppend(extra));                     // bytes left over
        var badCount = (byte[])good.Clone();
        var at = 4 + BinaryPrimitives.ReadInt32LittleEndian(badCount);
        BinaryPrimitives.WriteInt32LittleEndian(badCount.AsSpan(at), 7);                               // wrong body count
        Assert.Throws<InvalidDataException>(() => RaftFrames.DecodeAppend(badCount));
    }
}
