using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NexusMQ.AspNetCore;
using NexusMQ.Client;
using NexusMQ.Core;

namespace NexusMQ.Tests.Integration;

/// <summary>
/// A real NexusMQ server (Kestrel on a random localhost port, in-memory storage) for end-to-end tests
/// through the public client library: HTTP, WebSocket push, sessions and access keys.
/// </summary>
public sealed class LiveServer : IAsyncDisposable, IDisposable
{
    private readonly WebApplication _app;

    public string Url { get; }
    public Broker Broker => _app.Services.GetRequiredService<Broker>();
    public IServiceProvider Services => _app.Services;

    private LiveServer(WebApplication app, string url)
    {
        _app = app;
        Url = url;
    }

    public static LiveServer Start(Action<NexusServerOptions>? configure = null, bool https = false, int port = 0) =>
        StartAsync(configure, https, port).GetAwaiter().GetResult();

    /// <summary>A currently free TCP port (for servers that must know each other's URLs up front).</summary>
    public static int FreePort()
    {
        var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        var port = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    public static async Task<LiveServer> StartAsync(Action<NexusServerOptions>? configure = null, bool https = false, int port = 0)
    {
        var builder = WebApplication.CreateSlimBuilder();
        if (https)
        {
            var cert = SelfSignedCertificate();
            builder.WebHost.ConfigureKestrel(k => k.Listen(System.Net.IPAddress.Loopback, 0, l => l.UseHttps(cert)));
        }
        else
        {
            builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
        }
        builder.Logging.ClearProviders();
        builder.Services.AddNexusMQBroker(o =>
        {
            o.Durable = false;
            configure?.Invoke(o);
        });
        var app = builder.Build();
        app.MapNexusMQ("/api");
        await app.StartAsync();
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new LiveServer(app, address.Replace("127.0.0.1", "localhost"));
    }

    /// <summary>A throwaway self-signed certificate for "localhost".</summary>
    private static System.Security.Cryptography.X509Certificates.X509Certificate2 SelfSignedCertificate()
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var req = new System.Security.Cryptography.X509Certificates.CertificateRequest("CN=localhost", rsa,
            System.Security.Cryptography.HashAlgorithmName.SHA256, System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        var san = new System.Security.Cryptography.X509Certificates.SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        san.AddIpAddress(System.Net.IPAddress.Loopback);
        req.CertificateExtensions.Add(san.Build());
        using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1));
        return System.Security.Cryptography.X509Certificates.X509CertificateLoader.LoadPkcs12(cert.Export(
            System.Security.Cryptography.X509Certificates.X509ContentType.Pfx), null);
    }

    public NexusClient Client(string? key = null, NexusClientOptions? options = null) => new(Url, key, options);

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}
