# Creates a development TLS certificate for NexusMQ at .\certs\nexusmq.pfx
#   .\scripts\new-dev-cert.ps1                        # for https://localhost (uses the .NET dev certificate, trusted on this PC)
#   .\scripts\new-dev-cert.ps1 -HostName mq.local     # self-signed for another host name (not trusted until you import it)
# For production, use a certificate from your CA or Let's Encrypt instead.
param(
    [string]$HostName = "localhost",
    [string]$Password = "change-me-cert"
)
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
$certDir = Join-Path $root "certs"
New-Item -ItemType Directory -Force -Path $certDir | Out-Null
$pfx = Join-Path $certDir "nexusmq.pfx"

if ($HostName -eq "localhost") {
    dotnet dev-certs https --trust
    dotnet dev-certs https -ep $pfx -p $Password
} else {
    $cert = New-SelfSignedCertificate -DnsName $HostName -CertStoreLocation "Cert:\CurrentUser\My" -NotAfter (Get-Date).AddYears(1) `
        -KeyExportPolicy Exportable -KeyAlgorithm RSA -KeyLength 2048
    $secure = ConvertTo-SecureString -String $Password -Force -AsPlainText
    Export-PfxCertificate -Cert $cert -FilePath $pfx -Password $secure | Out-Null
    Write-Host "Self-signed certificate for $HostName created. Clients must trust it (import into Trusted Root) or use --insecure / AllowUntrustedCertificates for development." -ForegroundColor Yellow
}
Write-Host "Certificate: $pfx" -ForegroundColor Green
Write-Host "Password:    $Password  (set NEXUSMQ_CERT_PASSWORD to this for docker-compose.tls.yml)"
