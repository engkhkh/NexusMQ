using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using NexusMQ.Client;

namespace NexusMQ.Tests;

/// <summary>
/// NexusConnect races a host's addresses. Before it, a name whose first address never answered (Windows: "localhost" → ::1
/// with the server on 127.0.0.1 only) made every server-to-server connection time out, so no cluster leader was elected.
/// </summary>
public sealed class NexusConnectTests
{
    [Fact]
    public async Task Connects_to_the_address_that_answers_even_when_the_first_one_never_does()
    {
        using var working = new TcpListener(IPAddress.Loopback, 0);
        working.Start();
        var port = ((IPEndPoint)working.LocalEndpoint).Port;

        // A listener whose accept queue is full: on Linux, connection attempts to it get no answer at all.
        using var stuck = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        stuck.Bind(new IPEndPoint(IPAddress.Parse("127.0.0.2"), port));
        stuck.Listen(0);
        var filler = new List<Socket>();
        for (var i = 0; i < 64; i++)
        {
            var f = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { Blocking = false };
            try { f.Connect(stuck.LocalEndPoint!); } catch (SocketException) { }
            filler.Add(f);
        }
        try
        {
            var sw = Stopwatch.StartNew();
            using var socket = await NexusConnect.ConnectAnyAsync([IPAddress.Parse("127.0.0.2"), IPAddress.Loopback], port,
                new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);
            Assert.Equal(IPAddress.Loopback, ((IPEndPoint)socket.RemoteEndPoint!).Address);
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(1.5), $"took {sw.ElapsedMilliseconds} ms");
        }
        finally
        {
            foreach (var f in filler) f.Dispose();
        }
    }

    [Fact]
    public async Task Fails_when_no_address_answers()
    {
        using var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop(); // nothing listens there any more
        await Assert.ThrowsAnyAsync<Exception>(() =>
            NexusConnect.ConnectAnyAsync([IPAddress.Loopback], port, new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token));
    }
}
