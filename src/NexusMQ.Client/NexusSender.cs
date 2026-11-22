namespace NexusMQ.Client;

/// <summary>Sends to a queue, a topic, or an exchange ("exchanges/{name}").</summary>
public sealed class NexusSender
{
    private readonly NexusClient _client;

    internal NexusSender(NexusClient client, string entityPath)
    {
        _client = client;
        EntityPath = entityPath;
    }

    public string EntityPath { get; }

    public Task<NexusSendResult> SendMessageAsync(NexusMessage message, CancellationToken ct = default) =>
        SendMessagesAsync([message], null, ct);

    public Task<NexusSendResult> SendMessagesAsync(IEnumerable<NexusMessage> messages, CancellationToken ct = default) =>
        SendMessagesAsync(messages, null, ct);

    /// <summary>Publish to an exchange with an explicit routing key (RabbitMQ basic.publish).</summary>
    public Task<NexusSendResult> PublishAsync(string routingKey, NexusMessage message, CancellationToken ct = default) =>
        SendMessagesAsync([message], routingKey, ct);

    /// <summary>Schedule a message for later delivery. Returns its sequence number (queues only) for cancellation.</summary>
    public async Task<long> ScheduleMessageAsync(NexusMessage message, DateTimeOffset enqueueTime, CancellationToken ct = default)
    {
        message.ScheduledEnqueueTime = enqueueTime;
        var result = await SendMessageAsync(message, ct).ConfigureAwait(false);
        return result.SequenceNumbers.FirstOrDefault();
    }

    public Task CancelScheduledMessageAsync(long sequenceNumber, CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Delete, $"api/scheduled/{Uri.EscapeDataString(EntityPath)}/{sequenceNumber}", null, ct);

    private async Task<NexusSendResult> SendMessagesAsync(IEnumerable<NexusMessage> messages, string? routingKey, CancellationToken ct)
    {
        var list = messages as IReadOnlyList<NexusMessage> ?? messages.ToList();
        var url = $"api/send/{NexusClient.Encode(EntityPath)}";
        if (routingKey is not null) url += $"?routingKey={Uri.EscapeDataString(routingKey)}";

        if (_client.Options.BinaryBodies && System.Diagnostics.Stopwatch.GetTimestamp() >= Interlocked.Read(ref _client.BinarySendsRefusedUntil))
        {
            var batch = BinaryBatch(list);
            try
            {
                return await _client.SendAsync<NexusSendResult>(HttpMethod.Post, url, () => batch.Content(), ct).ConfigureAwait(false);
            }
            catch (NexusClientException ex) when (ex.StatusCode == 415)
            {
                // Server older than 1.12: use JSON with it (and try binary again in 5 minutes, after a possible upgrade).
                Interlocked.Exchange(ref _client.BinarySendsRefusedUntil,
                    System.Diagnostics.Stopwatch.GetTimestamp() + 300 * System.Diagnostics.Stopwatch.Frequency);
            }
        }
        return await _client.SendAsync<NexusSendResult>(HttpMethod.Post, url, list.Select(m => Metadata(m, base64: true)).ToList(), ct)
            .ConfigureAwait(false);
    }

    private static object Metadata(NexusMessage m, bool base64) => new
    {
        m.MessageId,
        m.CorrelationId,
        m.Subject,
        m.ContentType,
        m.To,
        m.ReplyTo,
        m.SessionId,
        Properties = m.ApplicationProperties.Count > 0 ? m.ApplicationProperties : null,
        BodyBase64 = base64 ? Convert.ToBase64String(m.Body) : null,
        BodyLength = base64 ? (int?)null : m.Body.Length,
        ScheduledEnqueueTimeUtc = m.ScheduledEnqueueTime,
        m.TimeToLive
    };

    /// <summary>
    /// <c>[header length: int32 LE][header JSON: messages with "bodyLength"][raw bodies in order]</c> as
    /// <c>application/vnd.nexusmq.batch</c>. Built once; each attempt gets a new HttpContent over the same bytes.
    /// </summary>
    private static BinaryBatchBody BinaryBatch(IReadOnlyList<NexusMessage> messages)
    {
        var header = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(messages.Select(m => Metadata(m, base64: false)).ToList(), NexusJson.Options);
        var total = 4 + header.Length + messages.Sum(m => (long)m.Body.Length);
        if (total > int.MaxValue) throw new ArgumentException("The batch is too large; send fewer messages at once.");
        var data = new byte[total];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(data, header.Length);
        header.CopyTo(data, 4);
        var offset = 4 + header.Length;
        foreach (var m in messages)
        {
            m.Body.CopyTo(data, offset);
            offset += m.Body.Length;
        }
        return new BinaryBatchBody(data);
    }

    private sealed record BinaryBatchBody(byte[] Data)
    {
        public HttpContent Content()
        {
            var c = new ByteArrayContent(Data);
            c.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/vnd.nexusmq.batch");
            return c;
        }
    }
}
