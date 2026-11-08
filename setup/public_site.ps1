# Public website from this PC (no account needed):
#  1. a guest-only copy of the website on port 8510 (no login: PP_PUBLIC_GUEST=1, PP_PUBLIC_NO_LOGIN=1)
#  2. a Cloudflare quick tunnel -> https://<random>.trycloudflare.com
#  3. the permanent link https://renoalvesc24.github.io/PowerPredict-TL/ is updated to forward to the tunnel
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
New-Item -ItemType Directory -Force $state | Out-Null

function Test-Running([string]$pattern) {
    $p = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and $_.CommandLine -match $pattern }
    return [bool]$p
}

Write-Host "[1/3] Website publiku (konvidadu de'it) http://localhost:8510 ..."
if (Test-Running "streamlit run dashboard.app\.py --server\.port 8510") {
    Write-Host "      ativu ona."
} else {
    Start-Process cmd -ArgumentList '/k', 'title PowerPredict - Public website && set PP_PUBLIC_GUEST=1&& set PP_PUBLIC_NO_LOGIN=1&& python -W ignore -m streamlit run dashboard\app.py --server.port 8510 --server.headless true' -WorkingDirectory $root
    Start-Sleep -Seconds 8
}

Write-Host "[2/3] Cloudflare tunnel ..."
$url = $null
if ((Test-Running "cloudflared.*--url http://localhost:8510") -and (Test-Path $log)) {
    $m = Select-String -Path $log -Pattern 'https://[a-z0-9-]+\.trycloudflare\.com' | Select-Object -Last 1
    if ($m) { $url = $m.Matches[0].Value; Write-Host "      ativu ona." }
}
if (-not $url) {
    Remove-Item $log -ErrorAction SilentlyContinue
    Start-Process cmd -ArgumentList '/k', "title PowerPredict - Cloudflare tunnel && `"$cf`" tunnel --no-autoupdate --url http://localhost:8510 --logfile `"$log`""
    for ($i = 0; $i -lt 60 -and -not $url; $i++) {
        Start-Sleep -Seconds 1
        if (Test-Path $log) {
            $m = Select-String -Path $log -Pattern 'https://[a-z0-9-]+\.trycloudflare\.com' | Select-Object -Last 1
            if ($m) { $url = $m.Matches[0].Value }
        }
    }
}
if (-not $url) { throw "Tunnel la hetan URL - haree janela 'PowerPredict - Cloudflare tunnel'." }
Write-Host "      $url"

Write-Host "[3/3] Link permanente (GitHub Pages) ..."
if (-not (Test-Path (Join-Path $pages ".git"))) {
    git clone --quiet --branch gh-pages --single-branch $repo $pages
    git -C $pages config user.name "Reinaldo Nodi Moreno Alves Costa"
    git -C $pages config user.email "186493116+Renoalvesc24@users.noreply.github.com"
}
git -C $pages pull --quiet
$html = (Get-Content (Join-Path $root "setup\public_landing.html") -Raw -Encoding UTF8).Replace("__URL__", $url).Replace("__UPDATED__", (Get-Date -Format "yyyy-MM-dd HH:mm"))
[System.IO.File]::WriteAllText((Join-Path $pages "index.html"), $html, (New-Object System.Text.UTF8Encoding $false))
git -C $pages add index.html
git -C $pages diff --cached --quiet
if ($LASTEXITCODE -ne 0) {
    git -C $pages commit --quiet -m "Update live link"
    git -C $pages push --quiet
    Write-Host "      atualiza ona (GitHub Pages presiza ~1 minutu)."
} else {
    Write-Host "      la muda."
}
Write-Host ""
Write-Host "Link ba ema hotu:  $permanent" -ForegroundColor Green
Write-Host "(Taka janela 'Public website' no 'Cloudflare tunnel' atu para.)"
