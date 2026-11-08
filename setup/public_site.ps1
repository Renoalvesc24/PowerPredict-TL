# Public website from this PC (no account needed):
#  1. a guest-only copy of the website on port 8510 (no login: PP_PUBLIC_GUEST=1, PP_PUBLIC_NO_LOGIN=1)
#  2. a Cloudflare quick tunnel -> https://<random>.trycloudflare.com
#  3. live.json on GitHub Pages is updated, so https://renoalvesc24.github.io/PowerPredict-TL/ forwards to the tunnel
# Safe to run again: running parts are reused. Needs start_all.bat (Kafka, API, Spark, bot, MySQL) running.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$cf = Join-Path $env:LOCALAPPDATA "Programs\cloudflared\cloudflared.exe"
$state = Join-Path $env:LOCALAPPDATA "PowerPredictTL"
$log = Join-Path $state "tunnel.log"
$pages = Join-Path $state "pages"
$repo = "https://github.com/Renoalvesc24/PowerPredict-TL.git"
$permanent = "https://renoalvesc24.github.io/PowerPredict-TL/"
$metrics = "127.0.0.1:20241"
New-Item -ItemType Directory -Force $state | Out-Null

function Get-Procs([string]$pattern) {
    Get-CimInstance Win32_Process | Where-Object { $_.Name -ne "cmd.exe" -and $_.CommandLine -and $_.CommandLine -match $pattern }
}

# address of the running quick tunnel, asked from cloudflared itself (its metrics server)
function Get-TunnelUrl {
    foreach ($port in 20241..20245) {
        try {
            $r = Invoke-RestMethod -Uri "http://127.0.0.1:$port/quicktunnel" -TimeoutSec 2
            if ($r.hostname) { return "https://" + $r.hostname }
        } catch { }
    }
    return $null
}

# the site really answers through the tunnel (a stopped tunnel returns Cloudflare's error page)
function Test-Site([string]$url) {
    try {
        $r = Invoke-WebRequest -UseBasicParsing -Uri "$url/favicon.png" -TimeoutSec 10
        return ($r.StatusCode -eq 200 -and $r.Headers["Content-Type"] -like "image/*")
    } catch { return $false }
}

Write-Host "[1/3] Website publiku (konvidadu de'it) http://localhost:8510 ..."
if (Get-Procs "streamlit run dashboard.app\.py --server\.port 8510") {
    Write-Host "      ativu ona."
} else {
    Start-Process cmd -ArgumentList '/k', 'title PowerPredict - Public website && set PP_PUBLIC_GUEST=1&& set PP_PUBLIC_NO_LOGIN=1&& python -W ignore -m streamlit run dashboard\app.py --server.port 8510 --server.headless true' -WorkingDirectory $root
    Start-Sleep -Seconds 8
}

Write-Host "[2/3] Cloudflare tunnel ..."
$url = $null
if (Get-Procs "cloudflared.*--url http://localhost:8510") {
    $url = Get-TunnelUrl
    if ($url -and (Test-Site $url)) {
        Write-Host "      ativu ona."
    } else {
        Write-Host "      tunnel tuan la responde - hahu foun."
        Get-Procs "cloudflared.*--url http://localhost:8510" | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
        Start-Sleep -Seconds 2
        $url = $null
    }
}
if (-not $url) {
    Remove-Item $log -ErrorAction SilentlyContinue
    Start-Process cmd -ArgumentList '/k', "title PowerPredict - Cloudflare tunnel && `"$cf`" tunnel --no-autoupdate --metrics $metrics --url http://localhost:8510 --logfile `"$log`""
    for ($i = 0; $i -lt 60 -and -not $url; $i++) { Start-Sleep -Seconds 1; $url = Get-TunnelUrl }
    if (-not $url) { throw "Tunnel la hetan enderesu - haree janela 'PowerPredict - Cloudflare tunnel'." }
    $ok = $false
    for ($i = 0; $i -lt 30 -and -not $ok; $i++) { Start-Sleep -Seconds 2; $ok = Test-Site $url }
    if (-not $ok) { throw "Tunnel $url la responde - haree janela 'Public website' no 'Cloudflare tunnel'." }
}
Write-Host "      $url"

Write-Host "[3/3] Link permanente (GitHub Pages) ..."
if (-not (Test-Path (Join-Path $pages ".git"))) {
    git clone --quiet --branch gh-pages --single-branch $repo $pages
    git -C $pages config user.name "Reinaldo Nodi Moreno Alves Costa"
    git -C $pages config user.email "186493116+Renoalvesc24@users.noreply.github.com"
}
# the pages folder is a machine-managed copy: always start from GitHub's current version
git -C $pages fetch --quiet origin gh-pages
git -C $pages reset --quiet --hard origin/gh-pages
if ($LASTEXITCODE -ne 0) { throw "GitHub Pages copy la sinkroniza - haree internet / GitHub login." }
$utf8 = New-Object System.Text.UTF8Encoding $false
$html = Get-Content (Join-Path $root "setup\public_landing.html") -Raw -Encoding UTF8
[System.IO.File]::WriteAllText((Join-Path $pages "index.html"), $html, $utf8)
$live = @{ url = $url; updated = (Get-Date -Format "yyyy-MM-dd HH:mm") } | ConvertTo-Json -Compress
[System.IO.File]::WriteAllText((Join-Path $pages "live.json"), $live, $utf8)
git -C $pages add index.html live.json
git -C $pages diff --cached --quiet
if ($LASTEXITCODE -ne 0) {
    git -C $pages commit --quiet -m "Update live link"
    git -C $pages push --quiet origin HEAD:gh-pages
    if ($LASTEXITCODE -ne 0) { throw "Push ba GitHub Pages la susesu - koko fali start_public.bat." }
    Write-Host "      atualiza ona (GitHub Pages presiza ~1 minutu)."
} else {
    Write-Host "      la muda."
}
Write-Host ""
Write-Host "Link ba ema hotu:  $permanent" -ForegroundColor Green
Write-Host "(Taka janela 'Public website' no 'Cloudflare tunnel' atu para.)"
