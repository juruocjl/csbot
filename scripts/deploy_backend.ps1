param(
    [string]$Branch = "main",
    [string]$Remote = "ubuntu@42.193.244.178",
    [string]$RemoteDir = "/home/ubuntu/csbot",
    [string]$FrontendDir = "/home/ubuntu/csbot/dist",
    [string]$FrontendBranch = "build-output",
    [string]$ServiceName = "csbot",
    [int]$JournalLines = 80,
    [string]$SshConfig = "$HOME\.ssh\config",
    [string]$IdentityFile = "$HOME\.ssh\id_rsa",
    [string]$KnownHosts = "$HOME\.ssh\known_hosts",
    [switch]$SkipPush,
    [switch]$SkipPull,
    [switch]$SkipFrontend,
    [switch]$SkipRestart
)

$ErrorActionPreference = "Stop"

function Invoke-Git {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Args)
    & git -C $RepoRoot @Args
    if ($LASTEXITCODE -ne 0) {
        throw "git $($Args -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Remote {
    param([string]$Command)

    $sshArgs = @(
        "-F", $SshConfig,
        "-i", $IdentityFile,
        "-o", "UserKnownHostsFile=$KnownHosts",
        "-o", "BatchMode=yes",
        $Remote,
        $Command
    )
    & ssh @sshArgs
    if ($LASTEXITCODE -ne 0) {
        throw "ssh command failed with exit code $LASTEXITCODE"
    }
}

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

Write-Host "== csbot backend deploy =="
Write-Host "repo: $RepoRoot"
Write-Host "remote: ${Remote}:$RemoteDir"

$trackedChanges = (& git -C $RepoRoot status --porcelain --untracked-files=no)
if ($trackedChanges) {
    Write-Host $trackedChanges
    throw "Tracked working-tree changes exist. Commit or stash them before deploy."
}

Invoke-Git fetch origin $Branch

$localHead = (& git -C $RepoRoot rev-parse $Branch).Trim()
$remoteHead = (& git -C $RepoRoot rev-parse "origin/$Branch").Trim()
if ($localHead -ne $remoteHead) {
    $mergeBase = (& git -C $RepoRoot merge-base $Branch "origin/$Branch").Trim()
    if ($mergeBase -eq $localHead) {
        throw "Local $Branch is behind origin/$Branch. Pull/rebase before deploy."
    }
    if ($mergeBase -ne $remoteHead) {
        throw "Local $Branch and origin/$Branch diverged. Resolve before deploy."
    }
}

if (-not $SkipPush) {
    Write-Host "== push $Branch =="
    Invoke-Git push origin $Branch
}

if (-not $SkipPull) {
    Write-Host "== remote git pull =="
    Invoke-Remote "cd $RemoteDir && (git pull --ff-only origin $Branch || { echo 'normal git pull failed; retrying without proxy'; git -c http.proxy= -c https.proxy= pull --ff-only origin $Branch; }) && git rev-parse --short HEAD"
}

if (-not $SkipFrontend) {
    Write-Host "== frontend build-output pull =="
    Invoke-Remote "if [ -d $FrontendDir/.git ]; then cd $FrontendDir && (git pull --ff-only origin $FrontendBranch || { echo 'normal frontend git pull failed; retrying without proxy'; git -c http.proxy= -c https.proxy= pull --ff-only origin $FrontendBranch; }) && git rev-parse --short HEAD; else echo 'frontend git dir not found: $FrontendDir'; fi"
}

if ($SkipRestart) {
    Write-Host "== restart skipped =="
    exit 0
}

Write-Host "== restart systemd service $ServiceName =="
Invoke-Remote "sudo -n systemctl restart $ServiceName"
Start-Sleep -Seconds 5
Invoke-Remote "echo SERVICE_STATUS; sudo -n systemctl --no-pager -l status $ServiceName; echo JOURNAL_TAIL; sudo -n journalctl -u $ServiceName -n $JournalLines --no-pager"

Write-Host "== done =="
