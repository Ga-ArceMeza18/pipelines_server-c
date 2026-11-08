// test_simple_tasks.c - Pruebas unitarias de Sleep@1 y Cpu@1

#define _POSIX_C_SOURCE 200809L

#include "simple_tasks.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int failures = 0;

#define CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FALLA %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        failures++; \
    } \
} while (0)

static double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1000000000.0;
}

// Contexto de prueba con sus propios inputs y una bitácora temporal
typedef struct {
    VarList inputs;
    VarList out;
    atomic_bool cancel;
    TaskCtx ctx;
} Fixture;

static void fixture_init(Fixture* f, int threads) {
    varlist_init(&f->inputs);
    varlist_init(&f->out);
    atomic_init(&f->cancel, false);

    f->ctx.inputs = &f->inputs;
    f->ctx.out = &f->out;
    f->ctx.log = tmpfile();
    f->ctx.workdir = ".";
    f->ctx.cancel = &f->cancel;
    f->ctx.deadline_s = 0.0;
    f->ctx.max_threads = threads;
}

static void fixture_destroy(Fixture* f) {
    fclose(f->ctx.log);
    varlist_destroy(&f->inputs);
    varlist_destroy(&f->out);
}

// Corre Cpu@1 y devuelve una copia de CPU_RESULT, o NULL si falló
static char* run_cpu(const char* iterations, const char* seed, int threads) {
    Fixture f;
    fixture_init(&f, threads);
    var_set(&f.inputs, "iterations", iterations);
    var_set(&f.inputs, "seed", seed);

    char* result = NULL;

    if (task_cpu(&f.ctx) == 0 && var_get(&f.out, "CPU_RESULT") != NULL) {
        result = strdup(var_get(&f.out, "CPU_RESULT"));
    }

    fixture_destroy(&f);
    return result;
}

static void test_sleep(void) {
    printf("[Sleep@1]\n");
    Fixture f;

    // Espera normal
    fixture_init(&f, 1);
    var_set(&f.inputs, "seconds", "0.3");
    double t0 = now_s();
    CHECK(task_sleep(&f.ctx) == 0);
    double elapsed = now_s() - t0;
    CHECK(elapsed >= 0.29 && elapsed < 1.0);
    fixture_destroy(&f);

    // Inputs inválidos
    const char* bad[] = {"abc", "-1", "", "2s", "nan"};

    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        fixture_init(&f, 1);
        var_set(&f.inputs, "seconds", bad[i]);
        CHECK(task_sleep(&f.ctx) == 1);
        fixture_destroy(&f);
    }

    // Sin el input
    fixture_init(&f, 1);
    CHECK(task_sleep(&f.ctx) == 1);
    fixture_destroy(&f);

    // Si se cancela termina enseguida 
    fixture_init(&f, 1);
    var_set(&f.inputs, "seconds", "10");
    atomic_store(&f.cancel, true);
    t0 = now_s();
    CHECK(task_sleep(&f.ctx) == 1);
    CHECK(now_s() - t0 < 0.5);
    fixture_destroy(&f);

    // Con tiempo límite se detiene cerca del deadline y no a los 10 segundos
    fixture_init(&f, 1);
    var_set(&f.inputs, "seconds", "10");
    f.ctx.deadline_s = now_s() + 0.3;
    t0 = now_s();
    CHECK(task_sleep(&f.ctx) == 1);
    elapsed = now_s() - t0;
    CHECK(elapsed >= 0.25 && elapsed < 1.0);
    fixture_destroy(&f);
}

static void test_cpu(void) {
    printf("[Cpu@1]\n");

    // El resultado no depende del número de hilos
    char* serial = run_cpu("5000000", "42", 1);
    CHECK(serial != NULL);

    int thread_counts[] = {2, 3, 4, 8};

    for (size_t i = 0; i < sizeof(thread_counts) / sizeof(int); i++) {
        char* parallel = run_cpu("5000000", "42", thread_counts[i]);
        CHECK(parallel != NULL && serial != NULL
            && strcmp(parallel, serial) == 0);
        free(parallel);
    }

    // Las mismas entradas dan el mismo resultado y otra semilla da otro
    char* again = run_cpu("5000000", "42", 4);
    char* other = run_cpu("5000000", "43", 4);
    CHECK(again != NULL && serial != NULL && strcmp(again, serial) == 0);
    CHECK(other != NULL && serial != NULL && strcmp(other, serial) != 0);
    free(again);
    free(other);
    free(serial);

    // Inputs inválidos
    CHECK(run_cpu("0", "1", 2) == NULL);
    CHECK(run_cpu("-5", "1", 2) == NULL);
    CHECK(run_cpu("abc", "1", 2) == NULL);
    CHECK(run_cpu("1000", "x", 2) == NULL);
    CHECK(run_cpu("1000000000000000", "1", 2) == NULL);

    // Cancelada antes de empezar
    Fixture f;
    fixture_init(&f, 2);
    var_set(&f.inputs, "iterations", "1000000000");
    var_set(&f.inputs, "seed", "7");
    atomic_store(&f.cancel, true);
    CHECK(task_cpu(&f.ctx) == 1);
    CHECK(var_get(&f.out, "CPU_RESULT") == NULL);
    fixture_destroy(&f);
}

int main(void) {
    test_sleep();
    test_cpu();

    if (failures == 0) {
        printf("  OK\n");
        return EXIT_SUCCESS;
    }

    printf("  %d verificación(es) fallaron\n", failures);
    return EXIT_FAILURE;
}
