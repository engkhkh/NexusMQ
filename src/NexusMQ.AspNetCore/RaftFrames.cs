using System.Buffers.Binary;
using System.Text.Json;
using NexusMQ.Core.Cluster;
using NexusMQ.Core.Persistence;

namespace NexusMQ.AspNetCore;

/// <summary>
/// AppendEntries with raw message bodies, for the server-to-server WebSocket:
/// <code>[header length: int32][AppendRequest JSON without bodies][body count: int32][body length: int32]*[bodies]</code>
/// Bodies belong, in order, to the entries that carry one message (<c>Op.Message</c>). Base64 inside JSON made every body a
/// third bigger and cost encoding on the leader (once per follower) and decoding on each follower.
/// </summary>
public static class RaftFrames
{
    private static readonly JsonSerializerOptions JsonNoBody = BinaryRecords.WithoutBodies(NexusReplicationStream.Json);

    /// <summary>The binary form, or null when an entry carries several messages (sent as JSON instead).</summary>
    public static byte[]? TryEncodeAppend(AppendRequest request)
    {
        if (request.Entries.Any(e => e.Op?.Messages is not null)) return null;
        var bodies = request.Entries.Where(e => e.Op?.Message is not null).Select(e => e.Op!.Message!.Body).ToList();
        var header = JsonSerializer.SerializeToUtf8Bytes(request, JsonNoBody);
        var total = 4L + header.Length + 4 + 4L * bodies.Count + bodies.Sum(b => (long)b.Length);
        if (total > int.MaxValue) return null;
        var data = new byte[total];
        var span = data.AsSpan();
        BinaryPrimitives.WriteInt32LittleEndian(span, header.Length);
        header.CopyTo(span[4..]);
        var at = 4 + header.Length;
        BinaryPrimitives.WriteInt32LittleEndian(span[at..], bodies.Count);
        at += 4;
        foreach (var b in bodies) { BinaryPrimitives.WriteInt32LittleEndian(span[at..], b.Length); at += 4; }
        foreach (var b in bodies) { b.CopyTo(span[at..]); at += b.Length; }
        return data;
    }

    /// <summary>Read <see cref="TryEncodeAppend"/>'s output back into a request with its bodies.</summary>
    public static AppendRequest DecodeAppend(ReadOnlySpan<byte> data)
    {
        if (data.Length < 8) throw new InvalidDataException("append frame too short");
        var headerLength = BinaryPrimitives.ReadInt32LittleEndian(data);
        if (headerLength < 2 || headerLength > data.Length - 8) throw new InvalidDataException("bad header length");
        var request = JsonSerializer.Deserialize<AppendRequest>(data.Slice(4, headerLength), JsonNoBody)
                      ?? throw new InvalidDataException("empty append header");
        var at = 4 + headerLength;
        var count = BinaryPrimitives.ReadInt32LittleEndian(data[at..]);
        at += 4;
        var withBody = request.Entries.Where(e => e.Op?.Message is not null).ToList();
        if (count != withBody.Count || count < 0 || (long)count * 4 > data.Length - at) throw new InvalidDataException("body count does not match");
        var lengths = new int[count];
        for (var i = 0; i < count; i++) { lengths[i] = BinaryPrimitives.ReadInt32LittleEndian(data[at..]); at += 4; }
        for (var i = 0; i < count; i++)
        {
            if (lengths[i] < 0 || lengths[i] > data.Length - at) throw new InvalidDataException("body lengths do not add up");
            withBody[i].Op!.Message!.Body = data.Slice(at, lengths[i]).ToArray();
            at += lengths[i];
        }
        if (at != data.Length) throw new InvalidDataException("body lengths do not add up");
        return request;
    }
}
