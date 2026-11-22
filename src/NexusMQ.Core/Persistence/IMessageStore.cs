namespace NexusMQ.Core.Persistence;

/// <summary>Durable storage for messages and topology. Calls for a single entity are serialized by the broker.</summary>
public interface IMessageStore : IDisposable
{
    /// <summary>Insert or replace a message (keyed by sequence number) in an entity.</summary>
    void Put(string entityPath, BrokerMessage message);

    void Remove(string entityPath, long sequenceNumber);

    IEnumerable<BrokerMessage> Load(string entityPath);

    void DeleteEntity(string entityPath);

    /// <summary>True when the entity's journal has grown enough that it should be rewritten.</summary>
    bool NeedsCompaction(string entityPath);

    /// <summary>Rewrite the entity's storage so it holds exactly <paramref name="liveMessages"/>.</summary>
    void Compact(string entityPath, IEnumerable<BrokerMessage> liveMessages);

    void SaveTopology(TopologySnapshot topology);

    TopologySnapshot? LoadTopology();

    /// <summary>Paths of all entities that have stored data.</summary>
    IEnumerable<string> ListEntities();

    /// <summary>Delete everything (all entities and the topology). Used by a standby before applying a full snapshot.</summary>
    void Clear();
}
