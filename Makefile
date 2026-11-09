CC ?= gcc

# El enunciado pide -std=c23, pero con GCC 13 hay que usar -std=c2x
GCC_MAJOR := $(shell $(CC) -dumpversion 2>/dev/null | cut -d. -f1)
STD := $(if $(filter 1 2 3 4 5 6 7 8 9 10 11 12 13,$(GCC_MAJOR)),-std=c2x,-std=c23)

CFLAGS  = $(STD) -Wall -Wextra -pedantic -O2 -pthread -fopenmp -Iinclude -MMD -MP
LDFLAGS = -pthread -fopenmp
LDLIBS  = -lyaml -lm

# Si libyaml no está instalada se avisa antes de compilar
ifneq ($(MAKECMDGOALS),clean)
ifeq ($(shell pkg-config --exists yaml-0.1 2>/dev/null && echo si),)
$(error falta libyaml, instálela con: sudo apt install libyaml-dev)
endif
endif

BUILD    = build
BUILD_SP = build_sp

# Archivos con main, cada uno es un ejecutable, el resto se comparte
SERVER_MAIN = src/server.c
CLIENT_MAIN = src/pipectl.c
CLIENT_SRC  = $(CLIENT_MAIN) $(wildcard src/net_util.c)

ALL_SRC  = $(wildcard src/*.c)
CORE_SRC = $(filter-out $(SERVER_MAIN) $(CLIENT_MAIN),$(ALL_SRC))

ALL_OBJ     = $(ALL_SRC:src/%.c=$(BUILD)/%.o)
CORE_OBJ    = $(CORE_SRC:src/%.c=$(BUILD)/%.o)
CORE_OBJ_SP = $(CORE_SRC:src/%.c=$(BUILD_SP)/%.o)
CLIENT_OBJ  = $(CLIENT_SRC:src/%.c=$(BUILD)/%.o)

BINS = $(if $(wildcard $(SERVER_MAIN)),pipelined) $(if $(wildcard $(CLIENT_MAIN)),pipectl)

# Pruebas, cada tests se compila con los módulos compartidos y se corre
TEST_SRC = $(wildcard tests/test_*.c)
TEST_BIN = $(TEST_SRC:tests/%.c=$(BUILD)/%)
TEST_SH  = $(wildcard tests/test_*.sh)

.PHONY: all sinproteccion test bench clean

all: $(ALL_OBJ) $(BINS)

pipelined: $(CORE_OBJ) $(BUILD)/server.o
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

pipectl: $(CLIENT_OBJ)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

# Versión con -DSIN_PROTECCION para mostrar la race condition.
# Se compila en otra carpeta para no mezclar objetos con la versión normal
sinproteccion: $(if $(wildcard $(SERVER_MAIN)),pipelined_sp)

pipelined_sp: $(CORE_OBJ_SP) $(BUILD_SP)/server.o
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

$(BUILD)/%.o: src/%.c | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_SP)/%.o: CFLAGS += -DSIN_PROTECCION
$(BUILD_SP)/%.o: src/%.c | $(BUILD_SP)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/test_%: tests/test_%.c $(CORE_OBJ) | $(BUILD)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(CORE_OBJ) $(LDLIBS)

$(BUILD) $(BUILD_SP):
	mkdir -p $@

# Compilar y ejecutar todos los benchmarks
bench: all
	$(CC) $(CFLAGS) -o bench tools/bench.c src/net_util.c $(LDFLAGS)
	./bench -s ./pipelined -r 5 -a 1,2,4,8 -t 1,2,4,8 -e wide,deep,combined,wordcount,histogram -o benchmark.csv
	python3 tools/plot.py benchmark.csv

test: all $(TEST_BIN)
	@set -e; for t in $(TEST_BIN); do echo "== $$t"; $$t; done
	@set -e; for t in $(TEST_SH); do echo "== $$t"; bash $$t; done
	@if [ -z "$(TEST_BIN)$(TEST_SH)" ]; then echo "todavia no hay pruebas en tests/"; fi

clean:
	rm -rf $(BUILD) $(BUILD_SP) pipelined pipectl pipelined_sp bench

-include $(wildcard $(BUILD)/*.d $(BUILD_SP)/*.d)
