<#
 experiments/run_all_v3.ps1
 Kullanim (repo kokunden, PowerShell, sanal ortam acik):
   .\experiments\run_all_v3.ps1                                   # 18 kosu
   .\experiments\run_all_v3.ps1 -Scenarios demand_surge -Seeds 1 -Modes adaptive -Version smoke   # duman testi
 Yarida kalirsa ayni komutla devam eder (basarili kosulari atlar).
#>
param(
  [string[]]$Scenarios = @("normal", "demand_surge", "lane_closure"),
  [int[]]$Seeds = @(1, 2, 3),
  [string[]]$Modes = @("fixed", "adaptive"),
  [string]$Version = "v3",
  [double]$StepDelay = 0.05,
  [int]$ExpectedWindows = 240,
  [int]$ExpectedRecords = 1200,
  [int]$ExpectedTls = 3,
  [int]$MaxFirstT = 20,
  [string]$Python = "python"
)

$ErrorActionPreference = "Continue"
Set-Location (Split-Path -Parent $PSScriptRoot)   # repo koku
$inv = [System.Globalization.CultureInfo]::InvariantCulture

function Fail([string]$msg) {
  Write-Host "[HATA] $msg" -ForegroundColor Red
  exit 1
}

function Read-DotEnv([string]$path) {
  $h = @{}
  Get-Content $path -Encoding UTF8 | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
      $i = $line.IndexOf("=")
      $h[$line.Substring(0, $i).Trim()] = $line.Substring($i + 1).Trim().Trim('"')
    }
  }
  return $h
}

if (-not (Test-Path ".env")) { Fail ".env bulunamadi (.env.example'dan olusturun)." }
$envv = Read-DotEnv ".env"
$chPort = if ($envv["CLICKHOUSE_PORT"]) { $envv["CLICKHOUSE_PORT"] } else { "8123" }
$chDb = if ($envv["CLICKHOUSE_DB"]) { $envv["CLICKHOUSE_DB"] } else { "traffic" }

function Invoke-CH([string]$sql) {
  $uri = "http://localhost:$chPort/?user=" + [uri]::EscapeDataString($envv["CLICKHOUSE_USER"]) +
         "&password=" + [uri]::EscapeDataString($envv["CLICKHOUSE_PASSWORD"])
  try {
    return [string](Invoke-RestMethod -Method Post -Uri $uri -Body $sql -ContentType "text/plain; charset=utf-8")
  } catch {
    return ""
  }
}

function Invoke-Probe {
  & $Python experiments/readiness_probe.py
  if ($LASTEXITCODE -ne 0) {
    Fail "Hazir olma sondasi basarisiz. 'docker compose logs spark controller' ile bakin."
  }
}

function Test-Run([string]$runId) {
  $sql = "SELECT tls_id, count(), min(sim_time_end), max(sim_time_end), sum(n_records) " +
         "FROM $chDb.metrics WHERE run_id = '$runId' GROUP BY tls_id ORDER BY tls_id FORMAT TabSeparated"
  $rows = @()
  for ($try = 0; $try -lt 15; $try++) {
    $res = Invoke-CH $sql
    $rows = @($res -split "`n" | Where-Object { $_.Trim() })
    $full = ($rows.Count -eq $ExpectedTls)
    if ($full) {
      foreach ($r in $rows) {
        if ([int]($r -split "`t")[1] -lt $ExpectedWindows) { $full = $false }
      }
    }
    if ($full) { break }
    Start-Sleep -Seconds 2
  }
  if ($rows.Count -ne $ExpectedTls) {
    return @{ ok = $false; detail = "kavsak sayisi $($rows.Count) (beklenen $ExpectedTls)" }
  }
  foreach ($r in $rows) {
    $f = $r -split "`t"
    $windows = [int]$f[1]
    $firstT = [double]::Parse($f[2], $inv)
    $records = [int]$f[4]
    if ($windows -ne $ExpectedWindows) { return @{ ok = $false; detail = "$($f[0]): pencere $windows (beklenen $ExpectedWindows)" } }
    if ($firstT -gt $MaxFirstT) { return @{ ok = $false; detail = "$($f[0]): first_t=$firstT (en fazla $MaxFirstT)" } }
    if ($records -ne $ExpectedRecords) { return @{ ok = $false; detail = "$($f[0]): kayit $records (beklenen $ExpectedRecords)" } }
  }
  return @{ ok = $true; detail = "windows=$ExpectedWindows x $ExpectedTls kavsak, kayit=$ExpectedRecords" }
}

# --- kayit defteri ---
New-Item -ItemType Directory -Force -Path "experiments/results" | Out-Null
$logPath = "experiments/results/run_log_$Version.csv"
if (-not (Test-Path $logPath)) { "run_id,status,detail,timestamp" | Out-File -FilePath $logPath -Encoding utf8 }
function Write-RunLog([string]$id, [string]$status, [string]$detail) {
  $line = "{0},{1},{2},{3}" -f $id, $status, ($detail -replace ",", ";"), (Get-Date).ToString("s")
  $line | Out-File -FilePath $logPath -Append -Encoding utf8
}
$done = @{}
Import-Csv $logPath | Where-Object { $_.status -eq "ok" } | ForEach-Object { $done[$_.run_id] = $true }

# --- yigini ayaga kaldir (yeniden olusturma YOK) ve hazir olmayi dogrula ---
Write-Host "[1/3] docker compose up -d" -ForegroundColor Cyan
docker compose up -d
if ($LASTEXITCODE -ne 0) { Fail "docker compose up basarisiz." }
docker compose --profile sim build producer
if ($LASTEXITCODE -ne 0) { Fail "producer imaji derlenemedi." }

Write-Host "[2/3] Hazir olma sondasi" -ForegroundColor Cyan
Invoke-Probe

# --- kosular ---
Write-Host "[3/3] Kosular" -ForegroundColor Cyan
$delayArg = $StepDelay.ToString($inv)
$total = $Scenarios.Count * $Seeds.Count * $Modes.Count
$n = 0
foreach ($sc in $Scenarios) {
  foreach ($seed in $Seeds) {
    foreach ($mode in $Modes) {
      $n++
      $runId = "exp_${sc}_${mode}_${Version}_s${seed}"
      if ($done.ContainsKey($runId)) { Write-Host "[$n/$total] $runId zaten tamam, atlandi."; continue }

      $startMs = ((& $Python experiments/run_clock.py) | Select-Object -First 1).ToString().Trim()
      Write-Host "[$n/$total] $runId (run_start_ms=$startMs, step_delay=$delayArg)" -ForegroundColor Yellow

      docker compose --profile sim run --rm producer --run-id $runId --scenario $sc --mode $mode --seed $seed --step-delay $delayArg --run-start-ms $startMs
      if ($LASTEXITCODE -ne 0) { Write-RunLog $runId "invalid" "producer cikis kodu $LASTEXITCODE"; Fail "producer basarisiz: $runId" }

      # Sonda: onceki pencereleri kapatir (flush) ve sonraki kosu icin hatti yeniden dogrular
      Invoke-Probe

      $tripinfo = "experiments/results/${runId}_tripinfo.xml"
      if (-not (Test-Path $tripinfo)) { Write-RunLog $runId "invalid" "tripinfo yok"; Fail "tripinfo bulunamadi: $tripinfo" }

      $chk = Test-Run $runId
      if (-not $chk.ok) {
        Write-RunLog $runId "invalid" $chk.detail
        Fail "Dogrulama basarisiz ($runId): $($chk.detail)"
      }
      Write-RunLog $runId "ok" $chk.detail
      Write-Host "    OK: $($chk.detail)" -ForegroundColor Green
    }
  }
}
Write-Host "Tum kosular tamam. Kayit: $logPath" -ForegroundColor Green
