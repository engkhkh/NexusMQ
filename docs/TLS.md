# Running NexusMQ over HTTPS

Without TLS, access keys and message bodies travel in clear text. Use HTTPS whenever the broker is reachable from
anything other than your own machine. Clients need no code changes: give them an `https://` URL, and push delivery
switches to `wss://` automatically.

There are three ways to do it. Pick one.

## 1. Kestrel serves HTTPS itself (simplest)

Put a certificate (`.pfx`) next to the server and configure it in `src/NexusMQ.Server/appsettings.json`:

```json
"Kestrel": {
  "Endpoints": {
    "Https": {
      "Url": "https://0.0.0.0:5681",
      "Certificate": { "Path": "certs/nexusmq.pfx", "Password": "…" }
    }
  }
}
```

Or use environment variables, which is better because the password stays out of files:

```powershell
$env:ASPNETCORE_URLS = "https://0.0.0.0:5681"
$env:Kestrel__Certificates__Default__Path = "C:\projects-claude\NexusMQ\certs\nexusmq.pfx"
$env:Kestrel__Certificates__Default__Password = "…"
dotnet run --project src/NexusMQ.Server
```

To get a certificate for development, run `.\scripts\new-dev-cert.ps1`. It exports the .NET development certificate for `localhost` and
trusts it on your PC. For another host name, use `-HostName mq.local`, which creates a self-signed certificate.

## 2. Docker

```powershell
.\scripts\new-dev-cert.ps1
$env:NEXUSMQ_CERT_PASSWORD = "change-me-cert"
docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d --build
```

The broker listens on `https://localhost:5681`. Plain HTTP on 5680 stays available inside the compose network so Prometheus can scrape it.
Remove the `5680:5680` mapping in `docker-compose.yml` if nothing outside should use HTTP.

## 3. Behind a reverse proxy (nginx, Caddy, IIS, Azure App Gateway…)

Let the proxy terminate TLS and forward to `http://nexusmq:5680`. The proxy must allow WebSocket upgrades on `/api/ws`
(nginx: `proxy_set_header Upgrade $http_upgrade; proxy_set_header Connection "upgrade";`) and must allow long requests,
because long-poll receives can wait up to 60 s. Set `proxy_read_timeout 90s`.

## Clients

```csharp
// Production: a certificate the machine trusts (public CA, or your company CA installed on the machine)
var client = new NexusClient("https://mq.example.com", key);

// Development only: accept a self-signed certificate
var dev = new NexusClient("https://localhost:5681", key, new NexusClientOptions { AllowUntrustedCertificates = true });
```

CLI: `nexusmq --url https://localhost:5681 stats`. For a self-signed development certificate, add `--insecure`.
If the certificate isn't trusted, the CLI says so and suggests the fix.

Never ship `AllowUntrustedCertificates` or `--insecure` to production. They turn off the protection against someone
intercepting the connection.
