#!/usr/bin/env bash
# win.sh - drive the Windows verification host from the Linux dev box.
#
# Code is edited, committed, compiled and unit-tested on Linux; everything that *runs* - the
# dev.ps1 stack, the SeaTunnel engine stack, the e2e suite, browser checks, the offline
# package - runs on the Windows host. See docs/dev-workflow.md.
#
#   ./win.sh setup                       one-time: git remote `win`, Windows mirror branch sync/linux
#   ./win.sh sync                        mirror this working tree (incl. untracked files) to Windows
#   ./win.sh ps  '<PowerShell>'          run PowerShell in the Windows repo root (UTF-8; Node + JDK 21 on PATH)
#   ./win.sh bg  <name> '<PowerShell>'   same, detached from the SSH session -> .dev-runtime\remote\<name>-<ts>.log
#   ./win.sh log <name> [lines]          tail the newest .dev-runtime\remote\<name>-*.log
#   ./win.sh wait <name> [seconds]       poll that log until the detached run ends (exit 0 = it succeeded)
#   ./win.sh dev <dev.ps1 args...>       dev.ps1; `up` / `restart-backend` run detached (log dev-<command>), `down` detached + waited;
#                                        `up -Poc` starts the engine stack from its built image, no docker build
#   ./win.sh devlog backend|frontend [n] tail the backend / frontend log (dev.ps1 logs would follow forever)
#   ./win.sh e2e [mvn args...]           e2e suite against the running stack, detached (log e2e)
#   ./win.sh tunnel [stop]               Windows 18081 / 8003 / 18080 -> 127.0.0.1:28081 / 18003 / 28080 here
#
# Environment: WIN_HOST (ssh alias, default lxz-win), WIN_REPO (default C:\projects\data-synchronization),
# TUNNEL_PORTS (default "18081 8003 18080"), TUNNEL_OFFSET (local = Windows port + this, default 10000).
set -euo pipefail

WIN_HOST=${WIN_HOST:-lxz-win}
WIN_REPO=${WIN_REPO:-'C:\projects\data-synchronization'}
TUNNEL_PORTS=${TUNNEL_PORTS:-18081 8003 18080}
TUNNEL_OFFSET=${TUNNEL_OFFSET:-10000}
SYNC_BRANCH=sync/linux
DONE_MARK='[win.sh] done'
FAIL_MARK='[win.sh] failed'

die() { echo "win.sh: $*" >&2; exit 2; }

# Prepended to every remote script. No CLIXML progress records on stderr; UTF-8 out (cmd.exe,
# the host's default SSH shell, speaks GBK); and the toolchain an SSH logon lacks: nvm's
# C:\nvm\nvm4w\nodejs symlink cannot be traversed from an SSH session, so its target goes first
# on PATH, and JDK 21 goes ahead of the Oracle javapath shim, which is a Java 25.
prelude() {
    printf '%s' "\$ProgressPreference='SilentlyContinue';[Console]::OutputEncoding=[Text.Encoding]::UTF8;\$env:PATH=@((Get-Item 'C:\nvm\nvm4w\nodejs').Target)[0]+';'+\$env:JAVA_HOME+'\bin;'+\$env:PATH;Set-Location '$WIN_REPO';"
}

# UTF-16LE base64 for powershell -EncodedCommand: no cmd.exe quoting, no code-page loss.
encode() { iconv -f UTF-8 -t UTF-16LE | base64 -w0; }

# Single-quotes one argument for PowerShell (also stops PS 5.1 splitting -Dkey.with.dots=value).
psquote() { local s=${1//\'/\'\'}; printf "'%s'" "$s"; }

remote_ps() {
    local encoded
    # 6>&1: Write-Host (dev.ps1 prints everything with it) goes to the information stream, which a
    # redirected -EncodedCommand session would otherwise serialize as CLIXML. Stream 6 only - native
    # stderr stays unredirected (see remote_bg).
    encoded=$({ prelude; printf '& {\n%s\n} 6>&1' "$1"; } | encode)
    # cmd.exe caps a command line at 8191 characters.
    (( ${#encoded} <= 7900 )) || die "remote script too long (${#encoded} encoded chars); keep it under ~2.5 KB"
    ssh "$WIN_HOST" "powershell -NoProfile -NonInteractive -ExecutionPolicy Bypass -EncodedCommand $encoded"
}

# Win32-OpenSSH kills every process of a session when the session ends - Start-Process children
# included - so anything that must outlive the command (the backend and frontend dev.ps1
# launches, a long test run) is created through WMI instead: its parent is WmiPrvSE, not this
# session, and it inherits nothing from it - hence its own prelude, written next to the log as
# <name>-<ts>.ps1 (UTF-8 with BOM, or PS 5.1 reads it as GBK).
# Its output is redirected by cmd.exe at the process level, never with PowerShell stream
# redirection (*>&1, 2>&1, a pipe): under PS 5.1 that turns every stderr line of a native command
# (docker compose progress, say) into an ErrorRecord, which dev.ps1's
# $ErrorActionPreference = 'Stop' makes fatal. The log ends with a done / failed marker; a body
# signals failure by throwing.
remote_bg() {
    local name=$1 body=$2 ts base
    [[ $name =~ ^[A-Za-z0-9._-]+$ ]] || die "log name must be [A-Za-z0-9._-]: $name"
    ts=$(TZ=Asia/Shanghai date +%Y%m%d-%H%M%S)
    base="$WIN_REPO\\.dev-runtime\\remote\\$name-$ts"
    # The log is created before the launch, so `log` / `wait` never pick up an older run.
    remote_ps "New-Item -ItemType Directory -Force '.dev-runtime\remote' | Out-Null
\$script = @'
$(prelude)
try { & { $body }; '$DONE_MARK' } catch { \"$FAIL_MARK: \$_\"; exit 1 }
'@
[IO.File]::WriteAllText('$base.ps1', \$script, (New-Object Text.UTF8Encoding \$true))
[IO.File]::WriteAllText('$base.log', \"[win.sh] started $ts\`r\`n\")
\$r = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = 'cmd.exe /d /c powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File \"$base.ps1\" >> \"$base.log\" 2>&1'; CurrentDirectory = '$WIN_REPO' }
if (\$r.ReturnValue -ne 0) { throw \"Win32_Process.Create returned \$(\$r.ReturnValue)\" }
\"started pid \$(\$r.ProcessId), log $base.log\""
}

remote_log() {
    local name=$1 lines=${2:-40}
    remote_ps "\$f = Get-ChildItem '.dev-runtime\remote\\$name-*.log' -ErrorAction SilentlyContinue | Sort-Object LastWriteTime | Select-Object -Last 1; if (-not \$f) { throw 'no log .dev-runtime\remote\\$name-*.log' }; '== ' + \$f.FullName; Get-Content -Encoding UTF8 -Tail $lines \$f.FullName"
}

cmd_wait() {
    local name=$1 deadline=$((SECONDS + ${2:-900})) out=''
    while ((SECONDS < deadline)); do
        out=$(remote_log "$name" 40 2>&1 || true)
        if grep -qF "$DONE_MARK" <<<"$out"; then printf '%s\n' "$out"; return 0; fi
        if grep -qF "$FAIL_MARK" <<<"$out"; then printf '%s\n' "$out"; return 1; fi
        sleep 20
    done
    printf '%s\n' "$out"
    die "timed out waiting for $name"
}

cmd_setup() {
    cd "$(git rev-parse --show-toplevel)"
    git remote get-url win >/dev/null 2>&1 || git remote add win "$WIN_HOST:${WIN_REPO//\\//}"
    # cmd.exe keeps the single quotes git puts around the repository path; PowerShell strips
    # them. The pack stream passes through unchanged (checked with a random 200 KB blob).
    git config remote.win.uploadpack 'powershell -NoProfile -Command git upload-pack'
    git config remote.win.receivepack 'powershell -NoProfile -Command git receive-pack'
    # Windows: a push into the checked-out mirror branch updates its working tree, and is
    # refused while that tree has local changes - nothing made on Windows is overwritten.
    # No 2>&1 on git: under PS 5.1 its stderr notes ("Switched to a new branch") would become errors.
    remote_ps "git config receive.denyCurrentBranch updateInstead; if ((git symbolic-ref --short HEAD) -ne '$SYNC_BRANCH') { git switch -C '$SYNC_BRANCH' }; git status --short --branch"
}

cmd_sync() {
    cd "$(git rev-parse --show-toplevel)"
    local current
    current=$(git ls-remote --symref win HEAD | sed -n 's/^ref: refs\/heads\/\([^[:space:]]*\)[[:space:]]*HEAD$/\1/p')
    [[ $current == "$SYNC_BRANCH" ]] || die "Windows repo is on '${current:-detached HEAD}', not $SYNC_BRANCH - a push would not update its working tree; run ./win.sh setup"
    # The working tree as a commit (untracked, non-ignored files included) built in a scratch
    # index, so the real index, HEAD and branch stay untouched; a clean tree pushes HEAD itself.
    local idx tree head commit
    idx=$(mktemp)
    cp "$(git rev-parse --git-dir)/index" "$idx"
    GIT_INDEX_FILE=$idx git add -A
    tree=$(GIT_INDEX_FILE=$idx git write-tree)
    rm -f "$idx"
    head=$(git rev-parse HEAD)
    if [[ $tree == "$(git rev-parse 'HEAD^{tree}')" ]]; then
        commit=$head
    else
        commit=$(git commit-tree "$tree" -p "$head" -m "win.sh sync: working tree on top of $(git rev-parse --short HEAD)")
    fi
    git push --force win "$commit:refs/heads/$SYNC_BRANCH"
    if [[ $commit == "$head" ]]; then
        echo "Windows $SYNC_BRANCH = $(git log -1 --format='%h %s' "$commit")"
    else
        echo "Windows $SYNC_BRANCH = $(git rev-parse --short "$commit") (uncommitted changes on top of $(git log -1 --format='%h %s' HEAD))"
    fi
}

cmd_dev() {
    # Passed bare: a quoted '-Poc' would bind as a string to dev.ps1's positional -Target, not as
    # the switch. Its arguments are all plain words, so nothing else is accepted.
    local args='' a poc=0
    for a in "$@"; do
        [[ $a =~ ^-?[A-Za-z][A-Za-z0-9-]*$ ]] || die "unexpected dev.ps1 argument: $a"
        if [[ ${1:-} == up && ${a,,} == -poc ]]; then poc=1; continue; fi
        args+=" $a"
    done
    if [[ ${1:-} == up ]]; then
        echo "note: the frontend dev server cannot start from a remote (elevated) session - for browser checks run .\\dev.ps1 up in a terminal on Windows (docs/dev-workflow.md §6)" >&2
    fi
    if ((poc)); then
        # `dev.ps1 up -Poc` runs `docker compose up --build`. A remote session (SSH key logon, or
        # a WMI-created process) cannot reach the Windows credential store, so whenever the build
        # has to resolve the base image apache/seatunnel from Docker Hub - its local tag is gone -
        # it fails with "error getting credentials ... logon session does not exist". The built
        # image data-sync-poc/seatunnel:2.3.13 is current, so the engine stack starts from it
        # without a build; rebuilding it (after a change under deploy/local-stack/seatunnel/) is
        # left to an interactive session on Windows. dev-state.json is then marked poc, so that
        # `dev.ps1 down -All` stops the engine stack too.
        remote_bg dev-up "& docker compose --project-name data-sync-poc --file deploy\\local-stack\\compose.yml up --detach; if (\$LASTEXITCODE -ne 0) { throw \"engine stack: docker compose up failed with exit code \$LASTEXITCODE\" }; & .\\dev.ps1$args; \$s = Get-Content -Raw .dev-runtime\\dev-state.json | ConvertFrom-Json; \$s.poc = \$true; \$s | ConvertTo-Json | Set-Content -Encoding UTF8 .dev-runtime\\dev-state.json"
        return
    fi
    case ${1:-} in
        up | restart-backend) remote_bg "dev-$1" "& .\\dev.ps1$args" ;;
        # `down -All` runs docker compose stop, whose progress goes to stderr: run it detached
        # (cmd-level redirection, see remote_bg) and wait, so it cannot abort half-way.
        down) remote_bg dev-down "& .\\dev.ps1$args" && cmd_wait dev-down 600 ;;
        logs) die "dev.ps1 logs follows the log forever; use ./win.sh devlog backend|frontend [lines]" ;;
        *) remote_ps "& .\\dev.ps1$args" ;;
    esac
}

cmd_devlog() {
    local target=${1:-backend} lines=${2:-80}
    [[ $target == backend || $target == frontend ]] || die "usage: ./win.sh devlog backend|frontend [lines]"
    remote_ps "\$f = Get-ChildItem '.dev-runtime\\$target\\$target-*.out.log' -ErrorAction SilentlyContinue | Sort-Object LastWriteTime | Select-Object -Last 1; if (-not \$f) { throw 'no $target log yet' }; '== ' + \$f.FullName; Get-Content -Encoding UTF8 -Tail $lines \$f.FullName"
}

cmd_e2e() {
    local args='' a
    for a in "$@"; do args+=" $(psquote "$a")"; done
    remote_bg e2e "Set-Location server; & mvn -B -ntp -o -Pdev -pl ruoyi-modules/ruoyi-sync '-Dmaven.test.skip=false' '-Dgroups=e2e' '-Dsurefire.failIfNoSpecifiedTests=false'$args test; if (\$LASTEXITCODE -ne 0) { throw \"mvn exited with code \$LASTEXITCODE\" }"
}

cmd_tunnel() {
    # A connection of its own, with its own control socket: through the shared ControlMaster the
    # forwards would land on that master and the client would exit at once, leaving nothing to stop.
    local socket="$HOME/.ssh/win-tunnel-%C"
    if [[ ${1:-} == stop ]]; then
        ssh -o ControlPath="$socket" -O exit "$WIN_HOST" 2>/dev/null && echo "tunnel stopped" || echo "no tunnel running"
        return
    fi
    if ssh -o ControlPath="$socket" -O check "$WIN_HOST" 2>/dev/null; then
        echo "tunnel already running (./win.sh tunnel stop)"
        return
    fi
    # Local port = Windows port + TUNNEL_OFFSET, never the same number: a VS Code window attached to
    # this box over Remote-SSH auto-forwards every new listener here back to the Windows desktop on
    # the *same* port - a local 8003 made VS Code take Windows' 8003 from the frontend dev server.
    local forwards=() p shown=()
    for p in $TUNNEL_PORTS; do
        forwards+=(-L "$((p + TUNNEL_OFFSET)):127.0.0.1:$p")
        shown+=("127.0.0.1:$((p + TUNNEL_OFFSET))->$p")
    done
    ssh -fN -o ControlMaster=yes -o ControlPath="$socket" -o ControlPersist=no -o ExitOnForwardFailure=yes \
        "${forwards[@]}" "$WIN_HOST"
    echo "forwarding ${shown[*]} on $WIN_HOST (./win.sh tunnel stop)"
}

main() {
    local cmd=${1:-help}
    shift || true
    case $cmd in
        setup) cmd_setup ;;
        sync) cmd_sync ;;
        ps) [[ $# -ge 1 ]] || die "usage: ./win.sh ps '<PowerShell>'"; remote_ps "$1" ;;
        bg) [[ $# -ge 2 ]] || die "usage: ./win.sh bg <name> '<PowerShell>'"; remote_bg "$1" "$2" ;;
        log) [[ $# -ge 1 ]] || die "usage: ./win.sh log <name> [lines]"; remote_log "$@" ;;
        wait) [[ $# -ge 1 ]] || die "usage: ./win.sh wait <name> [seconds]"; cmd_wait "$@" ;;
        dev) [[ $# -ge 1 ]] || die "usage: ./win.sh dev <dev.ps1 args...>"; cmd_dev "$@" ;;
        devlog) cmd_devlog "$@" ;;
        e2e) cmd_e2e "$@" ;;
        tunnel) cmd_tunnel "$@" ;;
        help | -h | --help) sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//' ;;
        *) die "unknown command '$cmd' (./win.sh help)" ;;
    esac
}

main "$@"
