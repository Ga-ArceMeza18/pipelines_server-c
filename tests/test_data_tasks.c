// test_data_tasks.c - Pruebas unitarias para @Wordcount y @Histogram

#define _POSIX_C_SOURCE 200809L

#include "data_tasks.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define THREAD_COUNTS 4

static const int thread_counts[THREAD_COUNTS] = {1, 2, 4, 8};

static int failures = 0;
static char workdir[] = "/tmp/test_data_tasks_XXXXXX";

// Crea un archivo con el contenido indicado.
static int create_file(const char* name, const char* content) {
    char path[512];

    snprintf(path, sizeof(path), "%s/%s", workdir, name);

    FILE* file = fopen(path, "w");

    if (file == NULL) {
        return 0;
    }

    fputs(content, file);
    fclose(file);

    return 1;
}

// Lee el contenido completo de un archivo.
static char* read_file(const char* name) {
    char path[512];

    snprintf(path, sizeof(path), "%s/%s", workdir, name);

    FILE* file = fopen(path, "r");

    if (file == NULL) {
        return NULL;
    }

    fseek(file, 0, SEEK_END);
    long size = ftell(file);
    rewind(file);

    if (size < 0) {
        fclose(file);
        return NULL;
    }

    char* content = malloc((size_t)size + 1);

    if (content == NULL) {
        fclose(file);
        return NULL;
    }

    size_t read = fread(content, 1, (size_t)size, file);
    fclose(file);

    if (read != (size_t)size) {
        free(content);
        return NULL;
    }

    content[read] = '\0';

    return content;
}

// Inicializa el contexto de una tarea.
static void fixture_init(TaskCtx* ctx, VarList* inputs, VarList* out, FILE** log, atomic_bool* cancel, int threads) {
    varlist_init(inputs);
    varlist_init(out);

    atomic_init(cancel, false);

    *log = tmpfile();

    ctx->inputs = inputs;
    ctx->out = out;
    ctx->log = *log;
    ctx->workdir = workdir;
    ctx->cancel = cancel;
    ctx->deadline_s = 0.0;
    ctx->max_threads = threads;
}

// Libera los recursos del contexto.
static void fixture_destroy(VarList* inputs, VarList* out, FILE* log) {
    if (log != NULL) {
        fclose(log);
    }

    varlist_destroy(inputs);
    varlist_destroy(out);
}

// Ejecuta WordCount@1 con la cantidad de hilos indicada.
static int run_wordcount(int threads, const char* input, const char* output) {
    TaskCtx ctx;
    VarList inputs;
    VarList out;
    FILE* log;
    atomic_bool cancel;

    fixture_init(&ctx, &inputs, &out, &log, &cancel, threads);

    if (log == NULL) {
        fixture_destroy(&inputs, &out, log);
        return 1;
    }

    char thread_value[16];
    snprintf(thread_value, sizeof(thread_value), "%d", threads);

    var_set(&inputs, "file", input);
    var_set(&inputs, "output", output);
    var_set(&inputs, "threads", thread_value);

    int result = task_wordcount(&ctx);

    fixture_destroy(&inputs, &out, log);

    return result;
}

// Ejecuta Histogram@1 con la cantidad de hilos indicada.
static int run_histogram(int threads, const char* input, const char* output, const char* bins) {
    TaskCtx ctx;
    VarList inputs;
    VarList out;
    FILE* log;
    atomic_bool cancel;

    fixture_init(&ctx, &inputs, &out, &log, &cancel, threads);

    if (log == NULL) {
        fixture_destroy(&inputs, &out, log);
        return 1;
    }

    char thread_value[16];
    snprintf(thread_value, sizeof(thread_value), "%d", threads);

    var_set(&inputs, "file", input);
    var_set(&inputs, "output", output);
    var_set(&inputs, "bins", bins);
    var_set(&inputs, "threads", thread_value);

    int result = task_histogram(&ctx);

    fixture_destroy(&inputs, &out, log);

    return result;
}

// Comprueba WordCount@1.
static int test_wordcount(void) {
    const char* expected = "6\n";

    if (!create_file("words.txt", "hola mundo\nhola\tmundo 123\n\nfinal")) {
        return 0;
    }

    char* baseline = NULL;

    for (int i = 0; i < THREAD_COUNTS; i++) {
        int threads = thread_counts[i];

        if (run_wordcount(threads, "words.txt", "wordcount.txt") != 0) {
            free(baseline);
            return 0;
        }

        char* result = read_file("wordcount.txt");

        if (result == NULL) {
            free(baseline);
            return 0;
        }

        // Verifica el resultado esperado.
        if (strcmp(result, expected) != 0) {
            free(result);
            free(baseline);
            return 0;
        }

        // Compara el resultado entre cantidades de hilos.
        if (baseline != NULL && strcmp(result, baseline) != 0) {
            free(result);
            free(baseline);
            return 0;
        }

        if (baseline == NULL) {
            baseline = strdup(result);

            if (baseline == NULL) {
                free(result);
                return 0;
            }
        }

        free(result);
    }

    free(baseline);

    // Comprueba el manejo de un archivo inexistente.
    if (run_wordcount(2, "missing.txt", "wordcount_error.txt") == 0) {
        return 0;
    }

    return 1;
}

// Comprueba Histogram@1.
static int test_histogram(void) {
    const char* expected =
        "bin,lower,upper,count\n"
        "0,-2.0000000000,-1.0000000000,1\n"
        "1,-1.0000000000,0.0000000000,1\n"
        "2,0.0000000000,1.0000000000,1\n"
        "3,1.0000000000,2.0000000000,3\n";

    if (!create_file("numbers.txt", "-2 -1 0 1 2 2")) {
        return 0;
    }

    char* baseline = NULL;

    for (int i = 0; i < THREAD_COUNTS; i++) {
        int threads = thread_counts[i];

        if (run_histogram(threads, "numbers.txt", "histogram.csv", "4") != 0) {
            free(baseline);
            return 0;
        }

        char* result = read_file("histogram.csv");

        if (result == NULL) {
            free(baseline);
            return 0;
        }

        // Verifica la distribución esperada.
        if (strcmp(result, expected) != 0) {
            free(result);
            free(baseline);
            return 0;
        }

        // Compara el resultado entre cantidades de hilos.
        if (baseline != NULL && strcmp(result, baseline) != 0) {
            free(result);
            free(baseline);
            return 0;
        }

        if (baseline == NULL) {
            baseline = strdup(result);

            if (baseline == NULL) {
                free(result);
                return 0;
            }
        }

        free(result);
    }

    free(baseline);

    // Comprueba el manejo de un archivo inexistente.
    if (run_histogram(2, "missing.txt", "histogram_error.csv", "4") == 0) {
        return 0;
    }

    return 1;
}

// Elimina los archivos utilizados por las pruebas.
static void cleanup(void) {
    const char* files[] = {
        "words.txt",
        "wordcount.txt",
        "numbers.txt",
        "histogram.csv"
    };

    char path[512];

    for (int i = 0; i < 4; i++) {
        snprintf(path, sizeof(path), "%s/%s", workdir, files[i]);
        unlink(path);
    }

    rmdir(workdir);
}

int main(void) {
    printf("================================\n");
    printf("     PRUEBAS UNITARIAS DATA TASKS\n");
    printf("================================\n\n");

    if (mkdtemp(workdir) == NULL) {
        printf("[FAIL] No se pudo crear el directorio temporal\n");
        return EXIT_FAILURE;
    }

    if (test_wordcount()) {
        printf("[OK] WordCount@1\n");
    } else {
        printf("[FAIL] WordCount@1\n");
        failures++;
    }

    if (test_histogram()) {
        printf("[OK] Histogram@1\n");
    } else {
        printf("[FAIL] Histogram@1\n");
        failures++;
    }

    cleanup();

    printf("\n================================\n");
    printf("Pruebas de DataTasks: %d/2 aprobadas\n", 2 - failures);

    if (failures == 0) {
        printf("Todas las pruebas pasaron!\n");
    } else {
        printf("Hay pruebas que fallaron.\n");
    }

    printf("================================\n");

    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
