using System.Text;
using NexusMQ.Core;

namespace NexusMQ.Tests;

public class SessionTests
{
    private readonly ManualTimeProvider _time = new();
    private readonly Broker _broker;

    public SessionTests()
    {
        _broker = new Broker(options: new BrokerOptions { SweepInterval = null }, time: _time);
        _broker.CreateQueue("orders", new QueueOptions { RequiresSession = true, LockDuration = TimeSpan.FromSeconds(30) });
    }

    private void Send(string session, string body) =>
        _broker.Send("orders", [new OutgoingMessage { SessionId = session, Body = Encoding.UTF8.GetBytes(body) }]);

    private MessageEntity Q => _broker.GetEntity("orders");

    [Fact]
    public void Messages_without_session_id_are_rejected()
    {
        var ex = Assert.Throws<NexusException>(() => _broker.Send("orders", [new OutgoingMessage()]));
        Assert.Equal(NexusErrorCode.BadRequest, ex.Code);
    }

    [Fact]
    public void Plain_receive_is_rejected_on_session_entity() =>
        Assert.Equal(NexusErrorCode.BadRequest, Assert.Throws<NexusException>(() => Q.TryReceive(1, ReceiveMode.PeekLock)).Code);

    [Fact]
    public async Task Accept_next_session_gets_only_that_sessions_messages_in_order()
    {
        Send("customer-A", "a1");
        Send("customer-B", "b1");
        Send("customer-A", "a2");
        Send("customer-A", "a3");

        var session = Q.TryAcceptSession(null).Session!;
        Assert.Equal("customer-A", session.SessionId);

        var msgs = await Q.ReceiveFromSessionAsync(session.SessionId, session.SessionLockToken, 10, TimeSpan.Zero, ReceiveMode.PeekLock);
        Assert.Equal(["a1", "a2", "a3"], msgs.Select(m => Encoding.UTF8.GetString(m.Message.Body)).ToArray());
        foreach (var m in msgs) Q.Complete(m.LockToken!.Value);

        // Second receiver gets the other session, not the locked one.
        var other = Q.TryAcceptSession(null).Session!;
        Assert.Equal("customer-B", other.SessionId);
    }

    [Fact]
    public void Locked_session_cannot_be_accepted_twice()
    {
        Send("s1", "x");
        Q.TryAcceptSession("s1");
        Assert.Equal(NexusErrorCode.SessionCannotBeLocked, Assert.Throws<NexusException>(() => Q.TryAcceptSession("s1")).Code);
        Assert.Null(Q.TryAcceptSession(null).Session); // nothing else available
    }

    [Fact]
    public void Released_or_expired_session_can_be_accepted_again()
    {
        Send("s1", "x");
        var first = Q.TryAcceptSession("s1").Session!;
        Q.ReleaseSession("s1", first.SessionLockToken);
        var second = Q.TryAcceptSession(null).Session!;
        Assert.Equal("s1", second.SessionId);

        _time.Advance(TimeSpan.FromSeconds(31));
        _broker.Sweep();
        Assert.Equal("s1", Q.TryAcceptSession(null).Session!.SessionId);
    }

    [Fact]
    public async Task Expired_session_lock_rejects_receive()
    {
        Send("s1", "x");
        var s = Q.TryAcceptSession("s1").Session!;
        _time.Advance(TimeSpan.FromSeconds(31));
        var ex = await Assert.ThrowsAsync<NexusException>(() =>
            Q.ReceiveFromSessionAsync("s1", s.SessionLockToken, 1, TimeSpan.Zero, ReceiveMode.PeekLock));
        Assert.Equal(NexusErrorCode.SessionLockLost, ex.Code);
    }

    [Fact]
    public void Renew_session_lock_extends_it()
    {
        Send("s1", "x");
        var s = Q.TryAcceptSession("s1").Session!;
        _time.Advance(TimeSpan.FromSeconds(25));
        Q.RenewSessionLock("s1", s.SessionLockToken);
        _time.Advance(TimeSpan.FromSeconds(25));
        Q.RenewSessionLock("s1", s.SessionLockToken); // still valid
    }

    [Fact]
    public void Session_state_round_trips()
    {
        Send("s1", "x");
        var s = Q.TryAcceptSession("s1").Session!;
        Assert.Null(Q.GetSessionState("s1", s.SessionLockToken));

        Q.SetSessionState("s1", s.SessionLockToken, Encoding.UTF8.GetBytes("step-2"));
        Assert.Equal("step-2", Encoding.UTF8.GetString(Q.GetSessionState("s1", s.SessionLockToken)!));

        Q.SetSessionState("s1", s.SessionLockToken, null);
        Assert.Null(Q.GetSessionState("s1", s.SessionLockToken));
    }

    [Fact]
    public async Task Abandoned_session_message_is_redelivered_first()
    {
        Send("s1", "first");
        Send("s1", "second");
        var s = Q.TryAcceptSession("s1").Session!;
        var m = (await Q.ReceiveFromSessionAsync("s1", s.SessionLockToken, 1, TimeSpan.Zero, ReceiveMode.PeekLock)).Single();
        Q.Abandon(m.LockToken!.Value);

        var again = (await Q.ReceiveFromSessionAsync("s1", s.SessionLockToken, 1, TimeSpan.Zero, ReceiveMode.PeekLock)).Single();
        Assert.Equal("first", Encoding.UTF8.GetString(again.Message.Body));
        Assert.Equal(2, again.Message.DeliveryCount);
    }

    [Fact]
    public async Task Accept_next_session_long_polls_until_a_message_arrives()
    {
        using var broker = new Broker(options: new BrokerOptions { SweepInterval = null });
        broker.CreateQueue("q", new QueueOptions { RequiresSession = true });

        var accept = broker.AcceptSessionAsync("q", null, TimeSpan.FromSeconds(10));
        await Task.Delay(100);
        Assert.False(accept.IsCompleted);

        broker.Send("q", [new OutgoingMessage { SessionId = "late" }]);
        var session = await accept.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("late", session!.SessionId);
    }
}
