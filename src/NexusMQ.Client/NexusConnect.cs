using System.Net;
using System.Net.Http;
using System.Net.Sockets;

namespace NexusMQ.Client;

/// <summary>
/// Opens TCP connections by trying every address of a host name at the same time and keeping the first that answers.
/// <para>
/// .NET tries a name's addresses one after another. "localhost" (and many host names) resolve to an IPv6 and an IPv4
/// address; if the server listens on only one of them, Windows takes about 2 seconds to report the other as refused,
/// so every new connection waits 2 seconds, and a short connect timeout never reaches the working address at all.
/// Racing the addresses (like browsers' "Happy Eyeballs") connects as fast as the quickest one.
/// </para>
/// </summary>
public static class NexusConnect
{
    /// <summary>A handler that connects through <see cref="ConnectAsync"/>; for HttpClient and for WebSockets (as an invoker).</summary>
    public static SocketsHttpHandler CreateHandler(bool allowUntrustedCertificates = false, TimeSpan? connectTimeout = null)
    {
        var handler = new SocketsHttpHandler
        {
            ConnectCallback = ConnectAsync,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            ConnectTimeout = connectTimeout ?? TimeSpan.FromSeconds(15)
        };
        if (allowUntrustedCertificates) handler.SslOptions.RemoteCertificateValidationCallback = (_, _, _, _) => true;
        return handler;
    }

    /// <summary><see cref="SocketsHttpHandler.ConnectCallback"/> that races all of the host's addresses.</summary>
    public static async ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext context, CancellationToken ct)
    {
        var host = context.DnsEndPoint.Host;
        var port = context.DnsEndPoint.Port;
        var addresses = IPAddress.TryParse(host.Trim('[', ']'), out var literal)
            ? [literal]
            : await Dns.GetHostAddressesAsync(host, ct).ConfigureAwait(false);
        if (addresses.Length == 0) throw new SocketException((int)SocketError.HostNotFound);
        return new NetworkStream(await ConnectAnyAsync(addresses, port, ct).ConfigureAwait(false), ownsSocket: true);
    }

    /// <summary>Connect to whichever of <paramref name="addresses"/> answers first (all tried at once).</summary>
    internal static async Task<Socket> ConnectAnyAsync(IReadOnlyList<IPAddress> addresses, int port, CancellationToken ct)
    {
        using var race = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var attempts = addresses.Select(a => AttemptAsync(a, port, race.Token)).ToList();
        Exception? last = null;
        while (attempts.Count > 0)
        {
            var done = await Task.WhenAny(attempts).ConfigureAwait(false);
            attempts.Remove(done);
            if (done.IsCompletedSuccessfully)
            {
                race.Cancel(); // stop the others; close any that still manage to connect
                foreach (var other in attempts)
                    _ = other.ContinueWith(t => { if (t.IsCompletedSuccessfully) t.Result.Dispose(); }, TaskScheduler.Default);
                return done.Result;
            }
            last = done.Exception?.InnerException ?? last;
        }
        ct.ThrowIfCancellationRequested();
        throw last ?? new SocketException((int)SocketError.ConnectionRefused);
    }

    private static async Task<Socket> AttemptAsync(IPAddress address, int port, CancellationToken ct)
    {
        var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(new IPEndPoint(address, port), ct).ConfigureAwait(false);
            return socket;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }
}
