using System.Buffers.Binary;
using System.Net;
using System.Text;
using NexusMQ.AspNetCore;
using NexusMQ.Client;

namespace NexusMQ.Tests;

/// <summary>Phase 14: send batches with raw (binary) bodies, and the JSON fallback for older servers.</summary>
public sealed class Phase14Tests
{
    private static byte[] Batch(string headerJson, params byte[][] bodies)
    {
        var header = Encoding.UTF8.GetBytes(headerJson);
        var data = new byte[4 + header.Length + bodies.Sum(b => b.Length)];
        BinaryPrimitives.WriteInt32LittleEndian(data, header.Length);
        header.CopyTo(data, 4);
        var o = 4 + header.Length;
        foreach (var b in bodies) { b.CopyTo(data, o); o += b.Length; }
        return data;
    }

    [Fact]
    public void Binary_batch_is_split_into_messages_with_their_raw_bodies()
    {
        var data = Batch("""[{"subject":"a","bodyLength":3,"properties":{"k":"v"}},{"subject":"empty","bodyLength":0},{"subject":"b","bodyLength":2}]""",
            [0xFF, 0x00, 0x7F], [], [1, 2]);
        var messages = NexusBinaryBatch.Parse(data);
        Assert.Equal(["a", "empty", "b"], messages.Select(m => m.Subject));
        Assert.Equal(new byte[] { 0xFF, 0x00, 0x7F }, messages[0].ToOutgoing().Body);
        Assert.Empty(messages[1].ToOutgoing().Body);
        Assert.Equal(new byte[] { 1, 2 }, messages[2].ToOutgoing().Body);
        Assert.Equal("v", messages[0].Properties!["k"]);
    }

    [Theory]
    [InlineData("""[{"bodyLength":5}]""", 3)]   // claims more than is there
    [InlineData("""[{"bodyLength":1}]""", 3)]   // leaves bytes over
    [InlineData("""[{"bodyLength":-1}]""", 0)]  // negative
    public void Binary_batch_with_wrong_lengths_is_rejected(string header, int bodyBytes)
    {
        var data = Batch(header, new byte[bodyBytes]);
        Assert.Throws<InvalidDataException>(() => NexusBinaryBatch.Parse(data));
    }

    [Fact]
    public void Binary_batch_with_impossible_header_length_is_rejected()
    {
        Assert.Throws<InvalidDataException>(() => NexusBinaryBatch.Parse([1, 2]));
        var data = new byte[10];
        BinaryPrimitives.WriteInt32LittleEndian(data, int.MaxValue);
        Assert.Throws<InvalidDataException>(() => NexusBinaryBatch.Parse(data));
    }

    /// <summary>A server that refuses the binary content type with 415, like NexusMQ before 1.12.</summary>
    private sealed class OldServer : HttpMessageHandler
    {
        public readonly List<string> ContentTypes = [];
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var type = request.Content?.Headers.ContentType?.MediaType ?? "";
            ContentTypes.Add(type);
            if (type == "application/vnd.nexusmq.batch")
                return new HttpResponseMessage(HttpStatusCode.UnsupportedMediaType);
            var body = await request.Content!.ReadAsStringAsync(ct);
            Assert.Contains("bodyBase64", body);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("""{"accepted":1,"delivered":1,"sequenceNumbers":[1]}""", Encoding.UTF8, "application/json")
            };
        }
    }

    [Fact]
    public async Task Client_falls_back_to_json_when_the_server_refuses_binary_and_remembers_it()
    {
        var server = new OldServer();
        using var http = new HttpClient(server) { BaseAddress = new Uri("http://old-server/") };
        using var client = new NexusClient(http);
        var sender = client.CreateSender("orders");

        var first = await sender.SendMessageAsync(new NexusMessage([1, 2, 3]));
        await sender.SendMessageAsync(new NexusMessage("again"));

        Assert.Equal(1, first.Accepted);
        // Binary refused once, then JSON for that send and for the next one (no second 415).
        Assert.Equal(["application/vnd.nexusmq.batch", "application/json", "application/json"], server.ContentTypes);
    }
}
