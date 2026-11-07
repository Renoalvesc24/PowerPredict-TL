# PowerPredict TL - is a component already running?
# Exit code 0 = running, 1 = not running.
# Used by start_all.bat and setup\start_kafka.bat so a second launch never starts a
# duplicate. Two Kafka brokers on the same log dir corrupt the KRaft metadata log.
param([string]$Pattern = "", [int]$Port = 0)

if ($Port -gt 0) {
    if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) { exit 0 }
}
if ($Pattern) {
    $found = Get-CimInstance Win32_Process -Filter "Name='java.exe' OR Name='python.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -and ($_.CommandLine -cmatch $Pattern) }
    if ($found) { exit 0 }
}
exit 1
