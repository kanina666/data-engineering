#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

show_all() { local s; for s in "$@"; do zk_status "$s"; zk_srvr "$s"; done; }

evidence stage2-01-start.txt "Этап 2.1. Запуск трёх серверов и роли leader/follower"
run "docker compose rm -sf zk1 zk2 zk3"
run "rm -rf zk/zk*/data/version-2 zk/zk*/data/zookeeper_server.pid"
run "head zk/zk*/data/myid"
run "cat zk/zk1/conf/zoo.cfg"
run "diff <(grep ^server zk/zk1/conf/zoo.cfg) <(grep ^server zk/zk2/conf/zoo.cfg) && diff <(grep ^server zk/zk1/conf/zoo.cfg) <(grep ^server zk/zk3/conf/zoo.cfg) && echo 'строки server.N одинаковые во всех трёх конфигах'"
run "docker compose up -d zk1 zk2 zk3"
wait_until 60 ensemble_ready
show_all zk1 zk2 zk3
LEADER=$(zk_leader)
note ""
note "ИТОГ: лидер — $LEADER"

evidence stage2-02-stop-leader.txt "Этап 2.2. Остановка лидера ($LEADER): ансамбль из двух серверов работает"
run "docker compose stop $LEADER"
mapfile -t ALIVE < <(printf '%s\n' zk1 zk2 zk3 | grep -vx "$LEADER")
wait_until 60 ensemble_ready
show_all "$LEADER" "${ALIVE[@]}"
NEW_LEADER=$(zk_leader)
note ""
note "ИТОГ: старый лидер $LEADER остановлен, новый лидер — $NEW_LEADER"
zk_cli "${ALIVE[0]}" create /stage2-rw "written-without-$LEADER"
zk_cli "${ALIVE[1]}" get -s /stage2-rw
zk_cli "${ALIVE[1]}" set /stage2-rw "updated-without-$LEADER"
zk_cli "${ALIVE[0]}" get /stage2-rw

SECOND=$NEW_LEADER
LAST=$(printf '%s\n' "${ALIVE[@]}" | grep -vx "$SECOND")
evidence stage2-03-stop-second.txt "Этап 2.3. Остановка второго сервера ($SECOND): у $LAST нет кворума, ансамбль не отвечает"
run "docker compose stop $SECOND"
run "sleep 5"
show_all "$LEADER" "$SECOND" "$LAST"
run "docker exec $LAST bash -c 'echo ruok | nc localhost ${CLIENT_PORT[$LAST]}; echo; echo stat | nc localhost ${CLIENT_PORT[$LAST]}'"
note ""
note "# zkCli не может выполнить команды: соединение есть, но сервер без кворума не обслуживает запросы (timeout 30 с)"
zk_cli "$LAST" ls /
note "# код возврата: $?"
zk_cli "$LAST" create /stage2-no-quorum x
note "# код возврата: $?"

evidence stage2-04-restart.txt "Этап 2.4. Запуск остановленных серверов $LEADER и $SECOND: ансамбль восстановился"
run "docker compose start $LEADER $SECOND"
wait_until 60 '[[ $(zk_leader) && $(zk_mode zk1) && $(zk_mode zk2) && $(zk_mode zk3) ]]'
show_all zk1 zk2 zk3
zk_cli "$LEADER" get /stage2-rw
note ""
note "ИТОГ: роли после восстановления — zk1=$(zk_mode zk1), zk2=$(zk_mode zk2), zk3=$(zk_mode zk3)"

evidence stage2-05-ephemeral.txt "Этап 2.5. Эфемерный узел живёт, пока жива сессия создавшего его клиента"
WIN2_OUT=$(mktemp)
note "# Окно 2: отдельный процесс zkCli (docker exec -i zk3 zkCli.sh), команды получает через pipe и держит сессию"
note "[$(now) UTC] окно 2 \$ docker exec -i zk3 zkCli.sh -server $ZK_CONNECT"
coproc WIN2 { docker exec -i zk3 zkCli.sh -server "$ZK_CONNECT" > "$WIN2_OUT" 2>&1; }
note "[$(now) UTC] окно 2 > create -e /demo-ephemeral data"
echo "create -e /demo-ephemeral data" >&"${WIN2[1]}"
wait_until 30 "grep -q 'Created /demo-ephemeral' '$WIN2_OUT'"
note "# Окно 1: отдельные вызовы zkCli из zk1"
zk_cli zk1 ls /
zk_cli zk1 stat /demo-ephemeral
note "[$(now) UTC] окно 2 > quit"
echo "quit" >&"${WIN2[1]}"
wait "$WIN2_PID"
note "# окно 2 завершено (код $?), его вывод:"
tee -a "$EVIDENCE" < "$WIN2_OUT"
rm -f "$WIN2_OUT"
zk_cli zk1 ls /
zk_cli zk1 stat /demo-ephemeral
