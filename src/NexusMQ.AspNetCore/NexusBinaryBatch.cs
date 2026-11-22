using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using NexusMQ.Core;

namespace NexusMQ.AspNetCore;

/// <summary>
/// Send batches with raw bodies: <c>Content-Type: application/vnd.nexusmq.batch</c>, body
/// <c>[header length: int32 LE][header JSON: the usual message list, each with "bodyLength" and no body][bodies, in order]</c>.
/// Base64 inside JSON makes a body a third bigger and costs a lot of CPU to encode, escape and decode; this skips all of it.
/// Servers before 1.12 answer 415 to this content type and the client falls back to JSON.
/// </summary>
public static class NexusBinaryBatch
{
    public const string ContentType = "application/vnd.nexusmq.batch";

    internal static async Task<List<SendMessageRequest>> ReadAsync(HttpRequest request)
    {
        if (!IsBinary(request.ContentType))
        {
            if (!request.HasJsonContentType())
                throw new NexusException(NexusErrorCode.BadRequest, "Send JSON (application/json) or a binary batch (" + ContentType + ").");
            try
            {
                return await request.ReadFromJsonAsync<List<SendMessageRequest>>(NexusReplicationStream.Json, request.HttpContext.RequestAborted) ?? [];
            }
            catch (JsonException ex) { throw new NexusException(NexusErrorCode.BadRequest, "Invalid JSON: " + ex.Message); }
        }

        var data = await ReadAllAsync(request.BodyReader, request.HttpContext.RequestAborted);
        try { return Parse(data); }
        catch (Exception ex) when (ex is JsonException or ArgumentOutOfRangeException or InvalidDataException)
        {
            throw new NexusException(NexusErrorCode.BadRequest, "Invalid binary batch: " + ex.Message);
        }
    }

    internal static bool IsBinary(string? contentType) =>
        contentType is not null && contentType.StartsWith(ContentType, StringComparison.OrdinalIgnoreCase);

    /// <summary>Split a binary batch into its messages (each body copied out, as the broker keeps it).</summary>
    public static List<SendMessageRequest> Parse(ReadOnlySpan<byte> data)
    {
        if (data.Length < 4) throw new InvalidDataException("missing header length");
        var headerLength = BinaryPrimitives.ReadInt32LittleEndian(data);
        if (headerLength < 2 || headerLength > data.Length - 4) throw new InvalidDataException("bad header length");
        var messages = JsonSerializer.Deserialize<List<SendMessageRequest>>(data.Slice(4, headerLength), NexusReplicationStream.Json) ?? [];
        var bodies = data[(4 + headerLength)..];
        var offset = 0;
        foreach (var m in messages)
        {
            var length = m.BodyLength ?? 0;
            if (length < 0 || length > bodies.Length - offset) throw new InvalidDataException("body lengths do not add up");
            m.RawBody = bodies.Slice(offset, length).ToArray();
            offset += length;
        }
        if (offset != bodies.Length) throw new InvalidDataException("body lengths do not add up");
        return messages;
    }

    private static async Task<byte[]> ReadAllAsync(PipeReader reader, CancellationToken ct)
    {
        while (true)
        {
            var result = await reader.ReadAsync(ct);
            if (result.IsCompleted)
            {
                var bytes = result.Buffer.ToArray();
                reader.AdvanceTo(result.Buffer.End);
                return bytes;
            }
            reader.AdvanceTo(result.Buffer.Start, result.Buffer.End); // keep everything until the whole body is in
        }
    }
}
