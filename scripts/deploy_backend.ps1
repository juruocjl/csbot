param(
    [string]$Branch = "main",
    [string]$Remote = "ubuntu@cgserver",
    [string]$RemoteDir = "/home/ubuntu/csbot",
    [string]$ScreenName = "csbot",
    [string]$UvPath = "/home/ubuntu/.local/bin/uv",
    [string]$SshConfig = "$HOME\.ssh\config",
    [string]$IdentityFile = "$HOME\.ssh\id_rsa",
    [string]$KnownHosts = "$HOME\.ssh\known_hosts",
    [switch]$SkipPush,
    [switch]$SkipPull,
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
    Invoke-Remote "cd $RemoteDir && git pull --ff-only origin $Branch && git rev-parse --short HEAD"
}

if ($SkipRestart) {
    Write-Host "== restart skipped =="
    exit 0
}

Write-Host "== restart $ScreenName =="
$restart = @"
screen -S $ScreenName -X quit 2>/dev/null || true
sleep 2
pgrep -f '[b]ot.py' | xargs -r kill -9
screen -wipe >/dev/null 2>&1 || true
cd $RemoteDir
screen -dmS $ScreenName bash -lc 'cd $RemoteDir && ENVIRONMENT=prod $UvPath run python bot.py > /tmp/csbot-start.log 2>&1'
sleep 10
echo SCREEN_LIST
screen -ls
echo PIDS
pgrep -af '[b]ot.py' || true
echo LOG_TAIL
tail -80 /tmp/csbot-start.log
"@
Invoke-Remote $restart

Write-Host "== done =="
