#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export MSYS_NO_PATHCONV=1

ZK_CONNECT="zk1:2181,zk2:2182,zk3:2183"
declare -A CLIENT_PORT=([zk1]=2181 [zk2]=2182 [zk3]=2183)
EVIDENCE=/dev/null

now() { date -u '+%Y-%m-%d %H:%M:%S.%3N'; }

evidence() {
  mkdir -p evidence
  EVIDENCE="evidence/$1"
  : > "$EVIDENCE"
  note "# $2"
  note "# начато: $(now) UTC"
}

note() { printf '%s\n' "$*" | tee -a "$EVIDENCE"; }

run() {
  note ""
  note "[$(now) UTC] \$ $1"
  eval "$1" 2>&1 | tee -a "$EVIDENCE"
  return "${PIPESTATUS[0]}"
}

zk_status() { run "docker exec $1 zkServer.sh status"; }
zk_srvr()   { run "docker exec $1 bash -c 'echo srvr | nc localhost ${CLIENT_PORT[$1]}'"; }
zk_cli()    { local srv=$1; shift; run "docker exec $srv timeout 30 zkCli.sh -waitforconnection -timeout 10000 -server $ZK_CONNECT $*"; }

zk_mode() { docker exec "$1" bash -c "echo srvr | nc localhost ${CLIENT_PORT[$1]}" 2>/dev/null | sed -n 's/^Mode: //p'; }

zk_leader() {
  local s
  for s in zk1 zk2 zk3; do [[ "$(zk_mode "$s")" == leader ]] && echo "$s"; done
}

wait_until() {
  local deadline=$((SECONDS + $1)); shift
  until eval "$*"; do
    (( SECONDS < deadline )) || { note "ТАЙМАУТ ожидания: $*"; return 1; }
    sleep 0.2
  done
}

ensemble_ready() { [[ -n "$(zk_leader)" ]]; }
