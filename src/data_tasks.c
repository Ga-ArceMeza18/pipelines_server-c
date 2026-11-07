// data_tasks.c - Implementación de tareas de procesamiento de datos en archivos

#include "data_tasks.h"

#include <ctype.h>
#include <math.h>
#include <omp.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// Funciones privadas

// Comprueba si la tarea debe detenerse.
static bool task_stopped(TaskCtx* ctx) {
    if (ctx->cancel != NULL && atomic_load(ctx->cancel)) {
        return true;
    }

    if (ctx->deadline_s > 0.0) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);

        double now = ts.tv_sec + ts.tv_nsec / 1000000000.0;

        if (now >= ctx->deadline_s) {
            return true;
        }
    }

    return false;
}

// Construye una ruta dentro del directorio de trabajo.
static char* make_path(const char* workdir, const char* path) {
    if (workdir == NULL || path == NULL || path[0] == '\0') {
        return NULL;
    }

    if (path[0] == '/') {
        return NULL;
    }

    // Evitar componentes "..".
    const char* p = path;

    while (*p != '\0') {
        const char* end = strchr(p, '/');
        size_t len = end ? (size_t)(end - p) : strlen(p);

        if (len == 2 && p[0] == '.' && p[1] == '.') {
            return NULL;
        }

        if (end == NULL) {
            break;
        }

        p = end + 1;
    }

    size_t size = strlen(workdir) + strlen(path) + 2;
    char* result = malloc(size);

    if (result == NULL) {
        return NULL;
    }

    snprintf(result, size, "%s/%s", workdir, path);

    return result;
}

// Obtiene la cantidad de hilos solicitada, respetando el límite del Runner.
static int get_threads(TaskCtx* ctx) {
    int threads = ctx->max_threads > 0 ? ctx->max_threads : 1;

    char* value = var_get(ctx->inputs, "threads");

    if (value != NULL) {
        int requested = atoi(value);

        if (requested > 0 && requested < threads) {
            threads = requested;
        }
    }

    return threads;
}

// =============================================================================
// Funciones públicas

int task_wordcount(TaskCtx* ctx) {
    if (ctx == NULL || ctx->inputs == NULL || ctx->workdir == NULL) {
        return 1;
    }

    // Obtener los nombres de archivo de entrada y salida desde las variables de entrada
    char* input = var_get(ctx->inputs, "file");
    char* output = var_get(ctx->inputs, "output");

    if (input == NULL || output == NULL) {
        fprintf(ctx->log, "error: faltan inputs de WordCount\n");
        return 1;
    }

    // Construir las rutas completas de los archivos de entrada y salida
    char* input_path = make_path(ctx->workdir, input);
    char* output_path = make_path(ctx->workdir, output);

    if (input_path == NULL || output_path == NULL) {
        free(input_path);
        free(output_path);
        return 1;
    }

    FILE* file = fopen(input_path, "rb");

    // Verificar si el archivo se abrió correctamente
    if (file == NULL) {
        fprintf(ctx->log, "error: no se pudo abrir %s\n", input);
        free(input_path);
        free(output_path);
        return 1;
    }

    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    rewind(file);

    // Verificar si se pudo obtener el tamaño del archivo
    if (file_size < 0) {
        fclose(file);
        free(input_path);
        free(output_path);
        return 1;
    }

    char* data = malloc((size_t)file_size + 1);

    // Verificar si se pudo asignar memoria para el contenido del archivo
    if (data == NULL) {
        fclose(file);
        free(input_path);
        free(output_path);
        return 1;
    }

    size_t bytes_read = fread(data, 1, (size_t)file_size, file);
    fclose(file);

    // Verificar si se pudo leer todo el contenido del archivo
    if (bytes_read != (size_t)file_size) {
        free(data);
        free(input_path);
        free(output_path);
        return 1;
    }

    data[bytes_read] = '\0';

    // Verificar si la tarea debe detenerse antes de continuar
    if (task_stopped(ctx)) {
        free(data);
        free(input_path);
        free(output_path);
        return 1;
    }

    unsigned long long total = 0;
    int threads = get_threads(ctx);

    // Contar palabras en paralelo utilizando OpenMP

    #pragma omp parallel for num_threads(threads) reduction(+:total) schedule(static)
    // Se añade un schedule(static) para distribuir las iteraciones de manera 
    // uniforme entre los hilos, lo que puede mejorar el rendimiento en archivos grandes.
    // static significa  que distribuye las iteraciones de manera uniforme entre los hilos,
    // antes de comenzar el ciclo
    for (long i = 0; i < file_size; i++) {
        if (i % 65536 == 0 && task_stopped(ctx)) {
            continue;
        }

        if (!isspace((unsigned char)data[i])) {
            if (i == 0 || isspace((unsigned char)data[i - 1])) {
                total++;
            }
        }
    }

    // Fin de la región paralela. Verificar si la tarea debe detenerse antes de continuar.

    if (task_stopped(ctx)) {
        free(data);
        free(input_path);
        free(output_path);
        return 1;
    }

    // Crear el archivo de resultado
    FILE* result = fopen(output_path, "w");

    if (result == NULL) {
        fprintf(ctx->log, "error: no se pudo crear %s\n", output);
        free(data);
        free(input_path);
        free(output_path);
        return 1;
    }

    fprintf(result, "%llu\n", total);
    fclose(result);

    fprintf(ctx->log, "WordCount: %llu palabras\n", total);

    free(data);
    free(input_path);
    free(output_path);

    return 0;
}

// =============================================================================

int task_histogram(TaskCtx* ctx) {
    if (ctx == NULL || ctx->inputs == NULL || ctx->workdir == NULL) {
        return 1;
    }

    // Obtener los nombres de archivo de entrada y salida desde las variables de entrada
    char* input = var_get(ctx->inputs, "file");
    char* output = var_get(ctx->inputs, "output");
    char* bins_value = var_get(ctx->inputs, "bins");

    if (input == NULL || output == NULL || bins_value == NULL) {
        fprintf(ctx->log, "error: faltan inputs de Histogram\n");
        return 1;
    }

    char* end;
    long bins_long = strtol(bins_value, &end, 10);

    if (*bins_value == '\0' || *end != '\0' ||
        bins_long <= 0 || bins_long > 1000000) {
        fprintf(ctx->log, "error: cantidad de bins inválida\n");
        return 1;
    }

    int bins = (int)bins_long;

    // Construir las rutas completas de los archivos de entrada y salida
    char* input_path = make_path(ctx->workdir, input);
    char* output_path = make_path(ctx->workdir, output);

    if (input_path == NULL || output_path == NULL) {
        free(input_path);
        free(output_path);
        return 1;
    }

    FILE* file = fopen(input_path, "r");

    if (file == NULL) {
        fprintf(ctx->log, "error: no se pudo abrir %s\n", input);
        free(input_path);
        free(output_path);
        return 1;
    }

    double* values = NULL;
    size_t count = 0;
    size_t capacity = 0;

    double value;
    double min = 0.0;
    double max = 0.0;

    int scan;

    // Leer los valores numéricos del archivo
    while ((scan = fscanf(file, "%lf", &value)) == 1) {
        if (!isfinite(value)) {
            fclose(file);
            free(values);
            free(input_path);
            free(output_path);
            return 1;
        }

        // Redimensionar el arreglo dinámico si es necesario
        if (count == capacity) {
            size_t new_capacity = capacity == 0 ? 1024 : capacity * 2;

            double* new_values = realloc(
                values,
                new_capacity * sizeof(double)
            );

            if (new_values == NULL) {
                fclose(file);
                free(values);
                free(input_path);
                free(output_path);
                return 1;
            }

            values = new_values;
            capacity = new_capacity;
        }

        // Almacenar el valor leído en el arreglo dinámico y actualizar los valores mínimo y máximo
        values[count++] = value;

        if (count == 1 || value < min) {
            min = value;
        }

        if (count == 1 || value > max) {
            max = value;
        }

        // Verificar si la tarea debe detenerse periódicamente para evitar bloqueos prolongados
        if (count % 4096 == 0 && task_stopped(ctx)) {
            fclose(file);
            free(values);
            free(input_path);
            free(output_path);
            return 1;
        }
    }

    fclose(file);

    // Verificar si se produjo un error de lectura o si no se leyeron valores
    if (scan != EOF || count == 0) {
        fprintf(ctx->log, "error: no se pudieron leer valores de %s\n", input);
        free(values);
        free(input_path);
        free(output_path);
        return 1;
    }

    int threads = get_threads(ctx);

    // Crear un arreglo para almacenar los histogramas locales de cada hilo y otro para el histograma final
    unsigned long long* local = calloc((size_t)threads * bins, sizeof(unsigned long long));
    unsigned long long* histogram = calloc(bins, sizeof(unsigned long long));

    if (local == NULL || histogram == NULL) {
        free(local);
        free(histogram);
        free(values);
        free(input_path);
        free(output_path);
        return 1;
    }

    // Contar los valores en paralelo utilizando OpenMP
    // Se utiliza num_threads(threads) para limitar el número de hilos según lo solicitado por el contexto de la tarea.
    #pragma omp parallel num_threads(threads)
    {
        int tid = omp_get_thread_num();

        // Se utiliza schedule(static) para distribuir las iteraciones de manera 
        // uniforme entre los hilos
        #pragma omp for schedule(static)
        for (size_t i = 0; i < count; i++) {
            if (task_stopped(ctx)) {
                continue;
            }

            int index = 0;

            // Calcular el índice del bin correspondiente al valor actual
            if (max != min) {
                double position = (values[i] - min) / (max - min);
                index = (int)(position * bins);

                if (index >= bins) {
                    index = bins - 1;
                }
            }

            local[(size_t)tid * bins + index]++;
        }
    }

    if (task_stopped(ctx)) {
        free(local);
        free(histogram);
        free(values);
        free(input_path);
        free(output_path);
        return 1;
    }

    // Combinar los histogramas locales.
    for (int i = 0; i < bins; i++) {
        for (int j = 0; j < threads; j++) {
            histogram[i] += local[(size_t)j * bins + i];
        }
    }

    FILE* result = fopen(output_path, "w");

    if (result == NULL) {
        fprintf(ctx->log, "error: no se pudo crear %s\n", output);
        free(local);
        free(histogram);
        free(values);
        free(input_path);
        free(output_path);
        return 1;
    }

    fprintf(result, "bin,lower,upper,count\n");

    // Escribir los resultados del histograma en el archivo de salida
    for (int i = 0; i < bins; i++) {
        double lower = min;
        double upper = max;

        if (max != min) {
            double width = (max - min) / bins;
            lower = min + i * width;
            upper = min + (i + 1) * width;
        }

        fprintf(result, "%d,%.10f,%.10f,%llu\n", i, lower, upper, histogram[i]);
    }

    fclose(result);

    fprintf(ctx->log, "Histogram: %zu valores en %d bins\n", count, bins);

    free(local);
    free(histogram);
    free(values);
    free(input_path);
    free(output_path);

    return 0;
}
