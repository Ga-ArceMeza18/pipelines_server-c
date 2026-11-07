// simple_tasks.c - Implementación de las tareas Sleep@1 y Cpu@1

#define _POSIX_C_SOURCE 200809L

#include "simple_tasks.h"

#include <errno.h>
#include <math.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define SLEEP_SLICE_S 0.1 // cada cuanto revisa Sleep si lo cancelaron
#define CPU_CHUNK (1ULL << 22) // iteraciones de Cpu entre cada revisión

// Topes para que un input muy grande no deje la tarea corriendo para siempre
#define MAX_SLEEP_S (24.0 * 60.0 * 60.0)
#define MAX_ITERATIONS 100000000000ULL

// Funciones privadas

// Hora actual en segundos
static double current_time(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    return ts.tv_sec + ts.tv_nsec / 1000000000.0;
}

// Revisa si cancelaron la ejecución o si ya se pasó el tiempo límite
static bool task_stopped(TaskCtx* ctx) {
    if (ctx->cancel != NULL && atomic_load(ctx->cancel)) {
        return true;
    }

    if (ctx->deadline_s > 0.0 && current_time() >= ctx->deadline_s) {
        return true;
    }

    return false;
}

// Pasa el texto a segundos, solo acepta números >= 0
static int parse_seconds(const char* text, double* out) {
    if (text == NULL || text[0] == '\0') {
        return -1;
    }

    char* end;
    errno = 0;
    double value = strtod(text, &end);

    if (errno != 0 || *end != '\0' || !isfinite(value) || value < 0.0) {
        return -1;
    }

    *out = value;
    return 0;
}

// Pasa el texto a entero sin signo, solo acepta dígitos
static int parse_unsigned(const char* text, unsigned long long* out) {
    if (text == NULL || text[0] < '0' || text[0] > '9') {
        return -1;
    }

    char* end;
    errno = 0;
    unsigned long long value = strtoull(text, &end, 10);

    if (errno != 0 || *end != '\0') {
        return -1;
    }

    *out = value;
    return 0;
}

// Revuelve los bits de un número (SplitMix64), el mismo número siempre da
// el mismo resultado así que no importa qué hilo lo calcule
static uint64_t mix64(uint64_t x) {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;

    return x ^ (x >> 31);
}

// Funciones públicas

int task_sleep(TaskCtx* ctx) {
    if (ctx == NULL || ctx->inputs == NULL || ctx->log == NULL) {
        return 1;
    }

    double seconds;

    if (parse_seconds(var_get(ctx->inputs, "seconds"), &seconds) != 0) {
        fprintf(ctx->log, "error: Sleep necesita un input 'seconds' >= 0\n");
        return 1;
    }

    if (seconds > MAX_SLEEP_S) {
        fprintf(ctx->log, "error: Sleep admite como máximo %.0f segundos\n",
            MAX_SLEEP_S);
        return 1;
    }

    fprintf(ctx->log, "Sleep: esperando %g segundos\n", seconds);

    // Se duerme en intervalos cortos para detenerse pronto si se cancela
    double end = current_time() + seconds;

    while (true) {
        if (task_stopped(ctx)) {
            fprintf(ctx->log, "Sleep: interrumpido\n");
            return 1;
        }

        double remaining = end - current_time();

        if (remaining <= 0.0) {
            break;
        }

        double slice = remaining < SLEEP_SLICE_S ? remaining : SLEEP_SLICE_S;
        struct timespec pause;
        pause.tv_sec = (time_t)slice;
        pause.tv_nsec = (long)((slice - (double)pause.tv_sec) * 1000000000.0);

        nanosleep(&pause, NULL);
    }

    fprintf(ctx->log, "Sleep: listo\n");
    return 0;
}

int task_cpu(TaskCtx* ctx) {
    if (ctx == NULL || ctx->inputs == NULL || ctx->log == NULL
        || ctx->out == NULL) {
        return 1;
    }

    unsigned long long iterations;
    unsigned long long seed;

    if (parse_unsigned(var_get(ctx->inputs, "iterations"), &iterations) != 0
        || iterations == 0 || iterations > MAX_ITERATIONS) {
        fprintf(ctx->log, "error: Cpu necesita un input 'iterations' entre 1 "
            "y %llu\n", MAX_ITERATIONS);
        return 1;
    }

    if (parse_unsigned(var_get(ctx->inputs, "seed"), &seed) != 0) {
        fprintf(ctx->log, "error: Cpu necesita un input 'seed' entero >= 0\n");
        return 1;
    }

    int threads = ctx->max_threads > 0 ? ctx->max_threads : 1;

    fprintf(ctx->log, "Cpu: %llu iteraciones, semilla %llu, %d hilo(s)\n",
        iterations, seed, threads);

    // La suma de enteros sin signo da el mismo valor sin importar el orden en que
    // sumen los hilos, por eso el resultado es igual con cualquier cantidad
    uint64_t total = 0;

    // Se hace por bloques para revisar entre cada uno si lo cancelaron
    for (unsigned long long start = 0; start < iterations; start += CPU_CHUNK) {
        if (task_stopped(ctx)) {
            fprintf(ctx->log, "Cpu: interrumpido\n");
            return 1;
        }

        unsigned long long stop = iterations - start < CPU_CHUNK
            ? iterations : start + CPU_CHUNK;
        uint64_t partial = 0;

        #pragma omp parallel for num_threads(threads) schedule(static) \
            reduction(+:partial)
        for (unsigned long long i = start; i < stop; i++) {
            partial += mix64((uint64_t)seed ^ mix64((uint64_t)i));
        }

        total += partial;
    }

    char text[32];
    snprintf(text, sizeof(text), "%llu", (unsigned long long)total);

    if (!var_set(ctx->out, "CPU_RESULT", text)) {
        fprintf(ctx->log, "error: no se pudo guardar CPU_RESULT\n");
        return 1;
    }

    fprintf(ctx->log, "Cpu: CPU_RESULT=%s\n", text);
    return 0;
}
