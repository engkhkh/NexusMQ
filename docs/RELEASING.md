# CI and releasing

Two GitHub Actions workflows live in `.github/workflows`.

## `ci.yml`: every push to `main` and every pull request
1. **Build and test** on Ubuntu **and** Windows (all unit, integration, HA and Raft-cluster tests; test results are uploaded as an artifact).
2. **Pack** all four packages with a CI version (`1.7.0-ci.<run number>`), install the CLI tool from them as a smoke test,
   and upload the `.nupkg`/`.snupkg` files as the `packages` artifact.
3. **Docker:** build the image, start it, create a queue, send a message, and check the stats.
4. **Live regression:** `scripts/live-regression.sh` starts real servers and checks every feature end to end: failover, leader kill and
   membership changes under traffic must lose no confirmed message. Server logs are uploaded if it fails.
   Run it yourself on Linux/macOS/WSL with `./scripts/live-regression.sh` (or `ONLY="cluster" ./scripts/live-regression.sh` for one area).
   On pushes, CI also builds the previous commit's server and runs the **upgrade** checks against it (`OLD_SERVER`).

## `release.yml`: when you push a version tag
```powershell
git tag v1.7.0
git push origin v1.7.0
```
1. Runs the tests again.
2. Packs with the tag's version (`v1.7.0` → `1.7.0`; a tag like `v1.8.0-beta.1` makes a prerelease).
3. Pushes the packages to **GitHub Packages** (always, no setup needed) and to **nuget.org** (only if the `NUGET_API_KEY` secret is set).
4. Creates a **GitHub Release** with generated notes and the packages attached.
5. Builds a **multi-arch Docker image** (amd64 + arm64) and pushes `ghcr.io/<owner>/nexusmq:1.7.0`, `:1.7` and `:latest`.

You can also run it by hand from the Actions tab (**Run workflow**) and type a version.

## One-time setup

1. **Create the GitHub repository** and push this folder:
   ```powershell
   cd C:\projects-claude\NexusMQ
   git init -b main
   git add .
   git commit -m "NexusMQ 1.7.0"
   git remote add origin https://github.com/<you>/NexusMQ.git
   git push -u origin main
   ```
2. **nuget.org (optional):** sign in at nuget.org → your account → **API Keys** → **Create**. Scope it to *Push new packages and package versions*
   with glob `NexusMQ.*`. Then in GitHub go to **Settings → Secrets and variables → Actions → New repository secret**, name it `NUGET_API_KEY`, and paste the key.
   - If the `NexusMQ.*` IDs are already taken on nuget.org, add a repository **variable** `NEXUSMQ_PACKAGE_PREFIX`
     (for example `Engkhkh.NexusMQ`). The packages then become `Engkhkh.NexusMQ.Core`, and so on. Locally use `-p:NexusMQPackagePrefix=…`.
3. **Docker image visibility:** the first push creates a private package on ghcr.io. To make it public, open the package on your GitHub profile →
   **Package settings → Change visibility**.
4. **Using GitHub Packages as a NuGet source** (for private packages): add
   `https://nuget.pkg.github.com/<you>/index.json` as a package source with a GitHub personal access token that has `read:packages`.

## Upgrading servers to 1.13 (storage format)
1.13 writes its files in a new binary format and converts older files when it opens them. A 1.12 or older server **cannot read** the
converted files. To keep the option of going back, set `NexusMQ:StorageFormat` to `Json` before upgrading. To switch back later, set it to `Json`,
start 1.13 once (which converts the files back), then downgrade.

## Version bumps
Set `<Version>` in `Directory.Build.props` to the next version, commit, then tag `v<that version>` and push the tag.
Dependabot (`.github/dependabot.yml`) opens pull requests for NuGet, GitHub Actions and Docker base-image updates.
