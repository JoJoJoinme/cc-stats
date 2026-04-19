param(
    [Parameter(Mandatory = $true)]
    [string]$ServerUrl,
    [string]$IngestToken,
    [ValidateSet("user", "project")]
    [string]$Scope = "user",
    [ValidateSet("auto", "plugin", "hooks", "skip")]
    [string]$ClaudeMode = "auto",
    [int]$Interval = 20,
    [switch]$NoAutostart,
    [switch]$SkipBackfill,
    [switch]$SkipCostrictScan
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoDir = (Resolve-Path (Join-Path $ScriptDir "..")).Path

function Get-PythonCommand {
    if ($env:PYTHON_BIN) {
        return $env:PYTHON_BIN
    }
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        return "py -3"
    }
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) {
        return $python.Source
    }
    throw "python or py launcher is required"
}

$AppDataDir = if ($env:CC_STATS_HOME) {
    $env:CC_STATS_HOME
} elseif ($env:APPDATA) {
    Join-Path $env:APPDATA "cc-stats"
} else {
    Join-Path $HOME "AppData\Roaming\cc-stats"
}

$RuntimeDir = Join-Path $AppDataDir "runtime"
$RuntimePython = Join-Path $RuntimeDir "Scripts\python.exe"
New-Item -ItemType Directory -Force -Path $AppDataDir | Out-Null

$PythonCommand = Get-PythonCommand
try {
    if ($PythonCommand -eq "py -3") {
        & py -3 -m venv $RuntimeDir
    } else {
        & $PythonCommand -m venv $RuntimeDir
    }
    if ($LASTEXITCODE -ne 0) {
        throw "python -m venv failed"
    }
} catch {
    Write-Warning "python -m venv unavailable, falling back to virtualenv bootstrap"
    if ($PythonCommand -eq "py -3") {
        & py -3 -m pip install --disable-pip-version-check --user virtualenv
        & py -3 -m virtualenv $RuntimeDir
    } else {
        & $PythonCommand -m pip install --disable-pip-version-check --user virtualenv
        & $PythonCommand -m virtualenv $RuntimeDir
    }
}

& $RuntimePython -m pip install --disable-pip-version-check --upgrade pip setuptools wheel
& $RuntimePython -m pip install --disable-pip-version-check --upgrade $RepoDir

$InstallArgs = @(
    "-m", "cc_stats.cli", "client", "install",
    "--server-url", $ServerUrl,
    "--scope", $Scope,
    "--claude-mode", $ClaudeMode,
    "--interval", "$Interval"
)

if ($IngestToken) {
    $InstallArgs += @("--ingest-token", $IngestToken)
}
if ($NoAutostart) {
    $InstallArgs += "--no-autostart"
}
if ($SkipBackfill) {
    $InstallArgs += "--skip-backfill"
}
if ($SkipCostrictScan) {
    $InstallArgs += "--skip-costrict-scan"
}

Push-Location $RepoDir
try {
    & $RuntimePython @InstallArgs
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
