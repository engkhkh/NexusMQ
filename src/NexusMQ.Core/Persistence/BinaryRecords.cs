using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace NexusMQ.Core.Persistence;

/// <summary>How durable files (journals and the Raft log) are written.</summary>
public enum StorageFormat
{
    /// <summary>Binary records with raw message bodies (default from 1.13). Files written this way cannot be read by older versions.</summary>
    Binary,
    /// <summary>JSON lines with base64 bodies, as up to 1.12. Use it to keep the option of going back to an older version.</summary>
    Json
}

/// <summary>
/// The binary record file format shared by message journals and the Raft log.
/// <code>
/// file   = magic "NMQBIN1\n" record*
/// record = [length: u32 LE][crc32c of the rest: u32 LE][flags: u8][json length: u32 LE][json][body]
///          length = 1 + 4 + json length + body length
/// </code>
/// <para>
/// The JSON holds everything except the message body, which follows as raw bytes (flag <see cref="SeparateBody"/>).
/// That avoids base64 (a third bigger, and costly to encode and decode) for every write and every replay.
/// A record that is cut short or fails its checksum ends the file, like a torn last line in the JSON-lines format.
/// </para>
/// </summary>
public static class BinaryRecords
{
    public static ReadOnlySpan<byte> Magic => "NMQBIN1\n"u8;
    public const byte SeparateBody = 1;
    private const int Fixed = 4 + 4 + 1 + 4;

    /// <summary>True when the file starts with the binary magic (false for JSON-lines files, empty or missing files).</summary>
    public static bool IsBinary(string path)
    {
        if (!File.Exists(path)) return false;
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        Span<byte> head = stackalloc byte[8];
        return fs.ReadAtLeast(head, 8, throwOnEndOfStream: false) == 8 && head.SequenceEqual(Magic);
    }

    /// <summary>Write one record: JSON metadata plus, optionally, a raw body.</summary>
    public static void Write(Stream stream, ReadOnlySpan<byte> json, ReadOnlySpan<byte> body, bool separateBody)
    {
        var length = 1 + 4 + json.Length + body.Length;
        var buffer = ArrayPool<byte>.Shared.Rent(8 + length);
        try
        {
            var span = buffer.AsSpan(0, 8 + length);
            BinaryPrimitives.WriteUInt32LittleEndian(span, (uint)length);
            span[8] = separateBody ? SeparateBody : (byte)0;
            BinaryPrimitives.WriteInt32LittleEndian(span[9..], json.Length);
            json.CopyTo(span[13..]);
            body.CopyTo(span[(13 + json.Length)..]);
            BinaryPrimitives.WriteUInt32LittleEndian(span[4..], Crc32C(span[8..]));
            stream.Write(span);
        }
        finally { ArrayPool<byte>.Shared.Return(buffer); }
    }

    /// <summary>One record as read back.</summary>
    public readonly record struct Record(byte[] Json, byte[] Body, bool SeparateBody);

    /// <summary>
    /// Read every intact record after the magic. Stops quietly at the first record that is cut short or fails its checksum
    /// (a write interrupted by a crash); <paramref name="validBytes"/> says where the good part of the file ends.
    /// </summary>
    public static IEnumerable<Record> Read(string path, Action<long>? validBytes = null)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 1 << 16);
        var head = new byte[8];
        if (fs.ReadAtLeast(head, 8, throwOnEndOfStream: false) != 8 || !head.AsSpan().SequenceEqual(Magic)) yield break;
        var good = 8L;
        while (true)
        {
            if (fs.ReadAtLeast(head, 8, throwOnEndOfStream: false) != 8) break;
            var length = BinaryPrimitives.ReadUInt32LittleEndian(head);
            var crc = BinaryPrimitives.ReadUInt32LittleEndian(head.AsSpan(4));
            if (length < 5 || length > fs.Length - fs.Position) break;
            var payload = new byte[length];
            if (fs.ReadAtLeast(payload, (int)length, throwOnEndOfStream: false) != length) break;
            if (Crc32C(payload) != crc) break;
            var jsonLength = BinaryPrimitives.ReadInt32LittleEndian(payload.AsSpan(1));
            if (jsonLength < 0 || jsonLength > length - 5) break;
            good = fs.Position;
            yield return new Record(payload.AsSpan(5, jsonLength).ToArray(), payload.AsSpan(5 + jsonLength).ToArray(), payload[0] == SeparateBody);
        }
        validBytes?.Invoke(good);
    }

    /// <summary>CRC-32C (Castagnoli), hardware-accelerated where the CPU supports it.</summary>
    public static uint Crc32C(ReadOnlySpan<byte> data)
    {
        var crc = 0xFFFFFFFFu;
        while (data.Length >= 8)
        {
            crc = BitOperations.Crc32C(crc, BinaryPrimitives.ReadUInt64LittleEndian(data));
            data = data[8..];
        }
        foreach (var b in data) crc = BitOperations.Crc32C(crc, b);
        return ~crc;
    }

    /// <summary>
    /// A copy of <paramref name="baseOptions"/> that leaves <see cref="BrokerMessage.Body"/> out of the JSON (the body goes
    /// after it as raw bytes).
    /// </summary>
    public static JsonSerializerOptions WithoutBodies(JsonSerializerOptions baseOptions) => new(baseOptions)
    {
        TypeInfoResolver = new DefaultJsonTypeInfoResolver
        {
            Modifiers =
            {
                ti =>
                {
                    if (ti.Type != typeof(BrokerMessage)) return;
                    for (var i = ti.Properties.Count - 1; i >= 0; i--)
                        if (ti.Properties[i].Name.Equals(nameof(BrokerMessage.Body), StringComparison.OrdinalIgnoreCase))
                            ti.Properties.RemoveAt(i);
                }
            }
        }
    };
}
