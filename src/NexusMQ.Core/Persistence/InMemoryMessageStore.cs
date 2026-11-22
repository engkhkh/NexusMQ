namespace NexusMQ.Core.Persistence;

/// <summary>Non-durable store. The broker keeps messages in memory anyway, so this is a no-op.</summary>
public sealed class InMemoryMessageStore : IMessageStore
{
    private TopologySnapshot? _topology;

    public void Put(string entityPath, BrokerMessage message) { }
    public void Remove(string entityPath, long sequenceNumber) { }
    public IEnumerable<BrokerMessage> Load(string entityPath) => [];
    public void DeleteEntity(string entityPath) { }
    public bool NeedsCompaction(string entityPath) => false;
    public void Compact(string entityPath, IEnumerable<BrokerMessage> liveMessages) { }
    public void SaveTopology(TopologySnapshot topology) => _topology = topology;
    public TopologySnapshot? LoadTopology() => _topology;
    public void Clear() => _topology = null;
    public IEnumerable<string> ListEntities() => [];
    public void Dispose() { }
}
