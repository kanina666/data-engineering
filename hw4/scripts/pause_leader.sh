#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

export FENCING=${1:?FENCING 0|1} RUN=${2:?имя каталога в logs/}
STAGE=${3:?префикс evidence-файлов}
export CHUNK_MB=${CHUNK_MB:-16} STEP_DELAY=${STEP_DELAY:-1.2} CKPT_EVERY=${CKPT_EVERY:-18}
PAUSE_AFTER_BLOCK=${PAUSE_AFTER_BLOCK:-38}
HOLD_SECONDS=${HOLD_SECONDS:-40}
EVENTS="evidence/$STAGE-events.txt"

log_of() { ls "logs/$RUN/$1"-*.log 2>/dev/null | head -1; }
log_has() { local f; f=$(log_of "$1") && [[ -n "$f" ]] && grep -q -- "$2" "$f"; }
event() { printf '%s=%s\n' "$1" "$2" >> "$EVENTS"; note "СОБЫТИЕ $1=$2"; }

evidence "$STAGE-01-reset.txt" "Подготовка запуска FENCING=$FENCING: чистое состояние ZooKeeper и журналов"
run "docker compose rm -sf coord-a coord-b"
run "docker compose up -d zk1 zk2 zk3 rustfs"
wait_until 60 ensemble_ready
zk_cli zk1 deleteall /result
zk_cli zk1 deleteall /epoch
zk_cli zk1 deleteall /checkpoint
zk_cli zk1 deleteall /election
zk_cli zk1 ls /
run "rm -rf logs/$RUN && mkdir -p logs/$RUN"
: > "$EVENTS"

evidence "$STAGE-02-start.txt" "Запуск двух координаторов, FENCING=$FENCING"
run "docker compose up -d coord-a"
wait_until 60 'log_has coord-a "стал лидером"'
run "docker compose up -d coord-b"
wait_until 60 'log_has coord-b "жду удаления предыдущего узла"'
LEADER=coord-a BACKUP=coord-b
run "grep -h -E 'подключился|очередь выборов|стал лидером|жду удаления' $(log_of coord-a) $(log_of coord-b)"
zk_cli zk1 ls /election
zk_cli zk1 get -s /epoch
event leader_container "$LEADER"
event leader_pid_in_container "$(sed -n 's/.*pid=\([0-9]*\)\].*/\1/p;q' "$(log_of $LEADER)")"
event leader_pid_on_docker_host "$(docker inspect -f '{{.State.Pid}}' $LEADER)"
run "docker top $LEADER"

evidence "$STAGE-03-pause.txt" "Приостановка лидера $LEADER посреди расчёта на ~${HOLD_SECONDS} с"
wait_until 300 "log_has $LEADER 'блок $PAUSE_AFTER_BLOCK обработан'"
note "# Разница часов хоста (Git Bash) и Docker VM: хост до / контейнер zk1 / хост после"
run "date -u '+%H:%M:%S.%3N'; docker exec zk1 date -u '+%H:%M:%S.%3N'; date -u '+%H:%M:%S.%3N'"
PAUSE_UTC=$(now)
docker pause "$LEADER" > /dev/null
note "[$PAUSE_UTC UTC] \$ docker pause $LEADER"
event paused_container "$LEADER"
event pause_utc "$PAUSE_UTC"
run "tail -n 3 $(log_of $LEADER)"
run "docker ps --filter name=$LEADER --format '{{.Names}} {{.Status}}'"
wait_until 120 "log_has $BACKUP 'стал лидером'"
run "grep -h -E 'удалён|стал лидером|продолжил с чекпоинта' $(log_of $BACKUP)"
wait_until 300 "log_has $BACKUP 'записал результат'"
zk_cli zk1 get -s /result
wait_until $((HOLD_SECONDS + 5)) "(( \$(date -u +%s) - \$(date -u -d '$PAUSE_UTC' +%s) >= HOLD_SECONDS ))"
RESUME_UTC=$(now)
docker unpause "$LEADER" > /dev/null
note "[$RESUME_UTC UTC] \$ docker unpause $LEADER"
event resume_utc "$RESUME_UTC"

evidence "$STAGE-04-after.txt" "Состояние после возобновления $LEADER"
run "docker wait $LEADER $BACKUP"
run "grep -h -E 'состояние сессии|BadVersionError|чекпоинт|записал результат|расчёт завершён' $(log_of $LEADER)"
run "grep -h -E 'стал лидером|продолжил с чекпоинта|записал результат' $(log_of $BACKUP)"
zk_cli zk1 get -s /result
zk_cli zk1 stat /result
zk_cli zk1 get -s /epoch
zk_cli zk1 get /checkpoint
LEADER_SESSION=$(sed -n 's/.*сессия \(0x[0-9a-f]*\),.*/\1/p' "$(log_of $LEADER)" | head -1)
note "# Сессия лидера $LEADER_SESSION в журналах серверов ZooKeeper: создание, истечение по таймауту, отказ после возобновления"
run "for s in zk1 zk2 zk3; do docker logs \$s 2>&1 | grep -F '$LEADER_SESSION' | sed \"s/^/\$s: /\"; done"
EXPIRE_UTC=$(for s in zk1 zk2 zk3; do docker logs $s 2>&1; done |
  sed -n "s/^\([0-9-]* [0-9:]*\),\([0-9]*\) .*Expiring session $LEADER_SESSION,.*/\1.\2/p" | head -1)
event session_expired_utc "$EXPIRE_UTC"
run "ls -la logs/$RUN"

evidence "$STAGE-05-check.txt" "Проверка запуска $RUN: записи результата, версия /result, BadVersionError, контрольные числа ДЗ № 3, время смены лидера"
run "docker compose run --rm --no-deps coord-a python src/check_run.py logs/$RUN $EVENTS --fencing $FENCING"
