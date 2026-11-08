// test_simple_tasks.c - Pruebas unitarias para Sleep@1 y Cpu@1

#define _POSIX_C_SOURCE 200809L

#include "simple_tasks.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define THREAD_COUNTS 4

static const int thread_counts[THREAD_COUNTS] = {1, 2, 4, 8};

typedef struct {
    const char* name;
    bool (*run)(void);
} SimpleTest;

// Contexto de una tarea con sus inputs, sus salidas y una bitácora temporal
typedef struct {
    VarList inputs;
    VarList out;
    atomic_bool cancel;
    TaskCtx ctx;
} Fixture;

// Hora actual en segundos
static double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    return ts.tv_sec + ts.tv_nsec / 1000000000.0;
}

// Inicializa el contexto de una tarea
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

// Libera los recursos del contexto
static void fixture_destroy(Fixture* f) {
    if (f->ctx.log != NULL) {
        fclose(f->ctx.log);
    }

    varlist_destroy(&f->inputs);
    varlist_destroy(&f->out);
}

// Ejecuta Sleep@1 con el valor de seconds indicado. NULL es sin el input
static int run_sleep(const char* seconds) {
    Fixture f;
    fixture_init(&f, 1);

    if (seconds != NULL) {
        var_set(&f.inputs, "seconds", seconds);
    }

    int result = task_sleep(&f.ctx);

    fixture_destroy(&f);

    return result;
}

// Ejecuta Cpu@1 y devuelve una copia de CPU_RESULT, o NULL si falló
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

// Comprueba que Sleep@1 espera el tiempo pedido
static bool test_sleep_waits(void) {
    double start = now_s();

    if (run_sleep("0.3") != 0) {
        return false;
    }

    double elapsed = now_s() - start;

    return elapsed >= 0.29 && elapsed < 1.0;
}

// Comprueba que Sleep@1 rechaza valores inválidos y la falta del input
static bool test_sleep_invalid(void) {
    const char* invalid[] = {"abc", "-1", "", "2s", "nan"};

    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        if (run_sleep(invalid[i]) == 0) {
            return false;
        }
    }

    return run_sleep(NULL) != 0;
}

// Comprueba que Sleep@1 termina enseguida si se cancela, aunque pida 10 segundos
static bool test_sleep_cancel(void) {
    Fixture f;
    fixture_init(&f, 1);

    var_set(&f.inputs, "seconds", "10");
    atomic_store(&f.cancel, true);

    double start = now_s();
    int result = task_sleep(&f.ctx);
    double elapsed = now_s() - start;

    fixture_destroy(&f);

    return result != 0 && elapsed < 0.5;
}

// Comprueba que Sleep@1 se detiene cerca del tiempo límite y no a los 10 segundos
static bool test_sleep_deadline(void) {
    Fixture f;
    fixture_init(&f, 1);

    var_set(&f.inputs, "seconds", "10");
    f.ctx.deadline_s = now_s() + 0.3;

    double start = now_s();
    int result = task_sleep(&f.ctx);
    double elapsed = now_s() - start;

    fixture_destroy(&f);

    return result != 0 && elapsed >= 0.25 && elapsed < 1.0;
}

// Comprueba que Cpu@1 da el mismo resultado con cualquier cantidad de hilos
static bool test_cpu_threads(void) {
    char* baseline = NULL;

    for (int i = 0; i < THREAD_COUNTS; i++) {
        char* result = run_cpu("5000000", "42", thread_counts[i]);

        if (result == NULL) {
            free(baseline);
            return false;
        }

        // Compara el resultado entre cantidades de hilos
        if (baseline != NULL && strcmp(result, baseline) != 0) {
            free(result);
            free(baseline);
            return false;
        }

        if (baseline == NULL) {
            baseline = result;
        } else {
            free(result);
        }
    }

    free(baseline);

    return true;
}

// Comprueba que las mismas entradas dan el mismo resultado y otra semilla da otro
static bool test_cpu_seed(void) {
    char* first = run_cpu("5000000", "42", 4);
    char* again = run_cpu("5000000", "42", 4);
    char* other = run_cpu("5000000", "43", 4);

    bool passed = first != NULL && again != NULL && other != NULL
        && strcmp(first, again) == 0 && strcmp(first, other) != 0;

    free(first);
    free(again);
    free(other);

    return passed;
}

// Comprueba que Cpu@1 rechaza entradas inválidas
static bool test_cpu_invalid(void) {
    const char* invalid[][2] = {
        {"0", "1"},
        {"-5", "1"},
        {"abc", "1"},
        {"1000", "x"},
        {"1000000000000000", "1"}
    };

    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        char* result = run_cpu(invalid[i][0], invalid[i][1], 2);

        if (result != NULL) {
            free(result);
            return false;
        }
    }

    return true;
}

// Comprueba que Cpu@1 se detiene si se cancela y no deja CPU_RESULT
static bool test_cpu_cancel(void) {
    Fixture f;
    fixture_init(&f, 2);

    var_set(&f.inputs, "iterations", "1000000000");
    var_set(&f.inputs, "seed", "7");
    atomic_store(&f.cancel, true);

    bool passed = task_cpu(&f.ctx) != 0 && var_get(&f.out, "CPU_RESULT") == NULL;

    fixture_destroy(&f);

    return passed;
}

int main(void) {
    SimpleTest tests[] = {
        {"Sleep@1 espera el tiempo pedido", test_sleep_waits},
        {"Sleep@1 rechaza entradas inválidas", test_sleep_invalid},
        {"Sleep@1 se detiene al cancelar", test_sleep_cancel},
        {"Sleep@1 se detiene con el tiempo límite", test_sleep_deadline},
        {"Cpu@1 da lo mismo con 1, 2, 4 y 8 hilos", test_cpu_threads},
        {"Cpu@1 depende solo de sus entradas", test_cpu_seed},
        {"Cpu@1 rechaza entradas inválidas", test_cpu_invalid},
        {"Cpu@1 se detiene al cancelar", test_cpu_cancel}
    };

    int total = sizeof(tests) / sizeof(tests[0]);
    int passed = 0;

    printf("================================\n");
    printf("  PRUEBAS UNITARIAS SIMPLE TASKS\n");
    printf("================================\n\n");

    for (int i = 0; i < total; i++) {
        if (tests[i].run()) {
            printf("[OK] %s\n", tests[i].name);
            passed++;
        } else {
            printf("[FAIL] %s\n", tests[i].name);
        }
    }

    printf("\n================================\n");
    printf("Pruebas de SimpleTasks: %d/%d aprobadas\n", passed, total);

    if (passed == total) {
        printf("Todas las pruebas pasaron!\n");
    } else {
        printf("Hay pruebas que fallaron.\n");
    }

    printf("================================\n");

    return passed == total ? EXIT_SUCCESS : EXIT_FAILURE;
}
