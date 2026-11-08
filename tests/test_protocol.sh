#!/usr/bin/env bash
# test_protocol.sh - Prueba de integración y concurrencia de pipelined con pipectl
#
# Levanta el servidor con 2 agentes y prueba cada operación del protocolo
# submit, status, list, cancel, logs (con y sin --follow) y shutdown

AGENTS=2
WORKDIR=$(mktemp -d /tmp/test_protocol_XXXXXX)
SERVER_LOG="$WORKDIR/server.log"
SERVER_PID=""
PORT=""

passed=0
total=0

# Al salir, pase lo que pase, se apaga el servidor y se borra la carpeta temporal
cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
    fi
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

ctl() {
    ./pipectl -p "$PORT" "$@"
}

# Anota el resultado de una prueba: check "nombre" condición
check() {
    local name=$1
    shift
    total=$((total + 1))

    if "$@"; then
        echo "[OK] $name"
        passed=$((passed + 1))
    else
        echo "[FAIL] $name"
    fi
}

# Espera hasta que la ejecución llegue al estado indicado, máximo 3 segundos
wait_state() {
    local id=$1 state=$2 limit=$3
    local tries=$((limit * 10))

    for ((i = 0; i < tries; i++)); do
        if ctl status "$id" 2>/dev/null | grep -q "^Estado: $state "; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

# Saca el id de la respuesta de submit
submit_id() {
    ctl submit "$1" | sed -n 's/.*id de ejecución: \([0-9]*\).*/\1/p'
}

# Arranca el servidor en un puerto libre. Si el puerto está ocupado prueba otro
start_server() {
    for attempt in 1 2 3 4 5; do
        PORT=$((20000 + RANDOM % 20000))
        ./pipelined "$PORT" "$AGENTS" "$WORKDIR/runs" > "$SERVER_LOG" 2>&1 &
        SERVER_PID=$!

        for ((i = 0; i < 30; i++)); do
            if ctl list > /dev/null 2>&1; then
                return 0
            fi
            if ! kill -0 "$SERVER_PID" 2>/dev/null; then
                break
            fi
            sleep 0.1
        done

        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
    done
    return 1
}

echo "================================"
echo "   PRUEBAS DEL PROTOCOLO (sh)"
echo "================================"
echo

if [ ! -x ./pipelined ] || [ ! -x ./pipectl ]; then
    echo "[FAIL] faltan pipelined o pipectl, compile con make"
    exit 1
fi

if ! start_server; then
    echo "[FAIL] no se pudo arrancar el servidor"
    cat "$SERVER_LOG"
    exit 1
fi

# Submit de un pipeline válido y su estado final
test_submit_valid() {
    local id
    id=$(submit_id pipelines/valid_basic.yaml)
    [ -n "$id" ] && wait_state "$id" succeeded 10
}
check "submit de un pipeline válido termina en succeeded" test_submit_valid

# Cada YAML inválido se rechaza con código 1 y un mensaje de error
test_submit_invalid() {
    local file
    for file in pipelines/invalid_*.yaml; do
        if ctl submit "$file" > /dev/null 2>&1; then
            return 1
        fi
    done
    ctl submit pipelines/invalid_cycle.yaml 2>&1 | grep -q "ciclo"
}
check "submit rechaza los pipelines inválidos" test_submit_invalid

# Las variables pasan entre etapas
test_status_and_logs() {
    local id
    id=$(submit_id ejemplo.yaml)
    wait_state "$id" succeeded 10 || return 1

    ctl status "$id" | grep -q "Trabajo Mostrar \[succeeded\]" \
        && ctl logs "$id" Mostrar | grep -q "^resultado=[0-9][0-9]*$" \
        && ctl logs "$id" | grep -q "^== Build/Calcular ==$"
}
check "status y logs muestran los trabajos y sus variables" test_status_and_logs

# Tres pipelines de tres trabajos con dos agentes, nunca puede haber más de dos corriendo
test_agent_limit() {
    local ids=() id max=0 running

    for i in 1 2 3; do
        ids+=("$(submit_id pipelines/protocol_parallel.yaml)")
    done

    # Mientras corren se cuentan los trabajos en running de las tres ejecuciones
    for ((t = 0; t < 80; t++)); do
        running=0
        for id in "${ids[@]}"; do
            running=$((running + $(ctl status "$id" | grep -c "^  Trabajo .*\[running\]")))
        done
        [ "$running" -gt "$max" ] && max=$running

        if [ "$(ctl list | grep -c "  succeeded  ParallelPipeline")" -eq 3 ]; then
            break
        fi
        sleep 0.1
    done

    for id in "${ids[@]}"; do
        wait_state "$id" succeeded 15 || return 1
    done

    [ "$max" -ge 1 ] && [ "$max" -le "$AGENTS" ]
}
check "nunca hay más trabajos corriendo que agentes" test_agent_limit

# Cancelar una ejecución en curso la deja en canceled y no se puede cancelar dos veces
test_cancel() {
    local id
    id=$(submit_id pipelines/protocol_slow.yaml)
    wait_state "$id" running 5 || return 1

    ctl cancel "$id" > /dev/null \
        && wait_state "$id" canceled 5 \
        && ! ctl cancel "$id" > /dev/null 2>&1
}
check "cancel deja la ejecución en canceled" test_cancel

# logs --follow manda las líneas mientras corre y termina solo al acabar la ejecución
test_follow() {
    local id out start elapsed
    id=$(submit_id pipelines/protocol_follow.yaml)

    start=$SECONDS
    out=$(timeout 20 ./pipectl -p "$PORT" logs "$id" --follow) || return 1
    elapsed=$((SECONDS - start))

    # Las líneas tienen que llegar completas, incluso la que el script escribió en dos partes
    echo "$out" | grep -q "^\[Build/Lines\] linea 4$" \
        && echo "$out" | grep -q "^\[Build/Lines\] mitad... completa$" \
        && echo "$out" | grep -q "^\[Test/Last\] fin del pipeline$" \
        && ! echo "$out" | grep -q "^\[Build/Lines\] mitad...$" \
        && [ "$elapsed" -ge 2 ]
}
check "logs --follow sigue la ejecución hasta que termina" test_follow

# Los errores se responden con código 1 y el servidor sigue atendiendo
test_errors() {
    ! ctl status 999 > /dev/null 2>&1 \
        && ! ctl cancel 999 > /dev/null 2>&1 \
        && ! ctl logs 999 > /dev/null 2>&1 \
        && ! ctl logs 1 NoExiste > /dev/null 2>&1 \
        && ! ctl submit pipelines/no_existe.yaml > /dev/null 2>&1 \
        && ctl list > /dev/null
}
check "los errores no tumban al servidor" test_errors

# Muchos clientes al mismo tiempo reciben todos su respuesta
test_many_clients() {
    local pids=() pid ok=0

    for i in $(seq 1 20); do
        ctl list > /dev/null 2>&1 &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do
        wait "$pid" && ok=$((ok + 1))
    done

    [ "$ok" -eq 20 ]
}
check "20 clientes a la vez reciben respuesta" test_many_clients

# Shutdown con una ejecución corriendo, la cancela, el servidor termina bien y deja el puerto
test_shutdown() {
    local id code
    id=$(submit_id pipelines/protocol_slow.yaml)
    wait_state "$id" running 5 || return 1

    ctl shutdown > /dev/null || return 1

    for ((i = 0; i < 100; i++)); do
        kill -0 "$SERVER_PID" 2>/dev/null || break
        sleep 0.1
    done

    wait "$SERVER_PID"
    code=$?
    SERVER_PID=""

    [ "$code" -eq 0 ] && ! ctl list > /dev/null 2>&1
}
check "shutdown apaga el servidor de forma ordenada" test_shutdown

echo
echo "================================"
echo "Pruebas del protocolo: $passed/$total aprobadas"

if [ "$passed" -eq "$total" ]; then
    echo "Todas las pruebas pasaron!"
else
    echo "Hay pruebas que fallaron."
    echo "--- salida del servidor ---"
    cat "$SERVER_LOG"
fi

echo "================================"

[ "$passed" -eq "$total" ]
