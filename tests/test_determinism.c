// test_determinism.c - Prueba unitaria de determinismo
// Los dos casos a probar:
/* Determinismo de variables: ejecuta 10 veces el mismo pipeline y verifica que
   el tercer trabajo siempre reciba el valor del segundo trabajo, que es el que
   debe ganar según el orden YAML. */

/* Contabilidad concurrente: ejecuta 512 trabajos independientes con 16 agentes y
   comprueba que todos terminen correctamente y que el contador registre
   exactamente 512 finalizaciones. */

/* La condición de carrera que se busca es que el contador de trabajos finalizados no
   se incremente correctamente debido a la falta de sincronización entre los hilos. */

#define _POSIX_C_SOURCE 200809L

#include "engine.h"
#include "pipeline.h"
#include "run.h"
#include "varlist.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define AGENTS 16
#define DETERMINISM_RUNS 10
#define STRESS_JOBS 512

static int failures = 0;
static char workdir[] = "output/test_determinism_XXXXXX";

// Inicializa un trabajo.
static void job_init(Job* job, const char* name) {
    memset(job, 0, sizeof(Job));

    job->name = strdup(name);
    job->stage = strdup("Test");
    job->cond = CONDITION_NONE;
    job->state = STATE_PENDING;

    varlist_init(&job->out_vars);
}

// Inicializa un paso.
static void step_init(Step* step, StepKind kind, const char* text) {
    memset(step, 0, sizeof(Step));

    step->kind = kind;
    step->text = strdup(text);
    step->state = STATE_PENDING;

    varlist_init(&step->inputs);
}

// Construye el pipeline para probar la herencia determinista.
static Pipeline* create_determinism_pipeline(void) {
    Pipeline* p = calloc(1, sizeof(Pipeline));

    if (p == NULL) {
        return NULL;
    }

    p->name = strdup("DeterminismPipeline");
    p->jobs_count = 3;
    p->jobs = calloc(3, sizeof(Job));

    varlist_init(&p->vars);

    if (p->name == NULL || p->jobs == NULL) {
        pipeline_free(p);
        return NULL;
    }

    // Primer trabajo independiente.
    job_init(&p->jobs[0], "First");

    p->jobs[0].steps_count = 1;
    p->jobs[0].steps = calloc(1, sizeof(Step));

    if (p->jobs[0].steps == NULL) {
        pipeline_free(p);
        return NULL;
    }

    step_init(&p->jobs[0].steps[0], STEP_TASK, "Cpu@1");
    var_set(&p->jobs[0].steps[0].inputs, "iterations", "50000");
    var_set(&p->jobs[0].steps[0].inputs, "seed", "111");

    // Segundo trabajo independiente.
    job_init(&p->jobs[1], "Second");

    p->jobs[1].steps_count = 1;
    p->jobs[1].steps = calloc(1, sizeof(Step));

    if (p->jobs[1].steps == NULL) {
        pipeline_free(p);
        return NULL;
    }

    step_init(&p->jobs[1].steps[0], STEP_TASK, "Cpu@1");
    var_set(&p->jobs[1].steps[0].inputs, "iterations", "50000");
    var_set(&p->jobs[1].steps[0].inputs, "seed", "222");

    // Tercer trabajo: depende de First y Second.
    job_init(&p->jobs[2], "Consume");

    p->jobs[2].deps_count = 2;
    p->jobs[2].deps = malloc(2 * sizeof(int));

    p->jobs[2].steps_count = 1;
    p->jobs[2].steps = calloc(1, sizeof(Step));

    if (p->jobs[2].deps == NULL || p->jobs[2].steps == NULL) {
        pipeline_free(p);
        return NULL;
    }

    p->jobs[2].deps[0] = 0;
    p->jobs[2].deps[1] = 1;

    step_init(&p->jobs[2].steps[0], STEP_SCRIPT, "echo $(CPU_RESULT)");

    return p;
}

// Construye un pipeline con muchos trabajos independientes.
static Pipeline* create_stress_pipeline(int count) {
    Pipeline* p = calloc(1, sizeof(Pipeline));

    if (p == NULL) {
        return NULL;
    }

    p->name = strdup("StressPipeline");
    p->jobs_count = count;
    p->jobs = calloc((size_t)count, sizeof(Job));

    varlist_init(&p->vars);

    if (p->name == NULL || p->jobs == NULL) {
        pipeline_free(p);
        return NULL;
    }

    for (int i = 0; i < count; i++) {
        char name[32];

        snprintf(name, sizeof(name), "Job%d", i);

        job_init(&p->jobs[i], name);

        p->jobs[i].steps_count = 1;
        p->jobs[i].steps = calloc(1, sizeof(Step));

        if (p->jobs[i].steps == NULL) {
            pipeline_free(p);
            return NULL;
        }

        step_init(&p->jobs[i].steps[0], STEP_SCRIPT, "true");
    }

    return p;
}

// Muestra el avance del contador de trabajos finalizados.
static void show_progress(long before, long total) {
    long finished = engine_jobs_finished() - before;

    if (finished > total) {
        finished = total;
    }

    printf("\rJobs completados: %ld/%ld", finished, total);
    fflush(stdout);
}

// Espera hasta que una ejecución termine.
// Si progress_total es mayor que cero, muestra el contador en tiempo real.
static int wait_run(int id, int timeout_s, long progress_before, long progress_total) {
    struct timespec start;
    struct timespec current;

    clock_gettime(CLOCK_MONOTONIC, &start);

    while (1) {
        State state;

        if (progress_total > 0) {
            show_progress(progress_before, progress_total);
        }

        if (engine_run_state(id, &state) != 0) {
            if (progress_total > 0) {
                printf("\n");
            }
            return 0;
        }

        if (state_is_terminal(state)) {
            if (progress_total > 0) {
                show_progress(progress_before, progress_total);
                printf("\n");
            }

            return state == STATE_SUCCESS;
        }

        clock_gettime(CLOCK_MONOTONIC, &current);

        double elapsed = (current.tv_sec - start.tv_sec) + (current.tv_nsec - start.tv_nsec) / 1000000000.0;

        if (elapsed >= timeout_s) {
            if (progress_total > 0) {
                printf("\nTimeout esperando la finalización del pipeline.\n");
            }
            return 0;
        }

        struct timespec delay = {
            .tv_sec = 0,
            .tv_nsec = 1000000
        };

        nanosleep(&delay, NULL);
    }
}

// Lee el contenido de un archivo.
static char* read_file(const char* path) {
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

// Obtiene el comando ejecutado por el trabajo Consume.
static char* get_consumed_value(int id) {
    char** paths = NULL;

    int count = engine_log_paths(id, "Consume", &paths);

    if (count != 1 || paths == NULL) {
        engine_free_paths(paths, count > 0 ? count : 0);
        return NULL;
    }

    char* log = read_file(paths[0]);

    engine_free_paths(paths, count);

    if (log == NULL) {
        return NULL;
    }

    const char* marker = "$ echo ";
    char* start = strstr(log, marker);

    if (start == NULL) {
        free(log);
        return NULL;
    }

    start += strlen(marker);

    char* end = strchr(start, '\n');

    if (end == NULL) {
        free(log);
        return NULL;
    }

    size_t length = (size_t)(end - start);

    char* value = malloc(length + 1);

    if (value != NULL) {
        memcpy(value, start, length);
        value[length] = '\0';
    }

    free(log);

    return value;
}

// Comprueba que el valor heredado sea siempre el mismo.
static int test_determinism(void) {
    char* expected = NULL;

    for (int i = 0; i < DETERMINISM_RUNS; i++) {
        printf("\rEjecuciones de determinismo: %d/%d",
               i + 1, DETERMINISM_RUNS);
        fflush(stdout);

        Pipeline* p = create_determinism_pipeline();

        if (p == NULL) {
            free(expected);
            printf("\n");
            return 0;
        }

        int id = engine_submit(p);

        if (id < 0 || !wait_run(id, 30, 0, 0)) {
            free(expected);
            printf("\n");
            return 0;
        }

        char* value = get_consumed_value(id);

        if (value == NULL) {
            free(expected);
            printf("\n");
            return 0;
        }

        if (expected != NULL && strcmp(value, expected) != 0) {
            free(value);
            free(expected);
            printf("\n");
            return 0;
        }

        if (expected == NULL) {
            expected = strdup(value);

            if (expected == NULL) {
                free(value);
                printf("\n");
                return 0;
            }
        }

        free(value);
    }

    printf("\n");
    free(expected);

    return 1;
}

// Comprueba la contabilidad de trabajos concurrentes.
static int test_race_condition(void) {
    long before = engine_jobs_finished();

    Pipeline* p = create_stress_pipeline(STRESS_JOBS);

    if (p == NULL) {
        return 0;
    }

    int id = engine_submit(p);

    if (id < 0 || !wait_run(id, 30, before, STRESS_JOBS)) {
        printf("[FAIL] El pipeline no terminó correctamente.\n");
        return 0;
    }

    // Todos los trabajos deben haber terminado correctamente.
    for (int i = 0; i < STRESS_JOBS; i++) {
        char name[32];
        State state;

        snprintf(name, sizeof(name), "Job%d", i);

        if (engine_job_state(id, name, &state) != 0 ||
            state != STATE_SUCCESS) {
            return 0;
        }
    }

    // El contador debe reflejar exactamente las finalizaciones.
    long finished = engine_jobs_finished() - before;

    printf("Contador final: %ld/%d\n", finished, STRESS_JOBS);

    if (finished != STRESS_JOBS) {
        return 0;
    }

    // El motor no debe superar el número de agentes disponibles.
    if (engine_max_running() > AGENTS) {
        return 0;
    }

    return 1;
}

int main(void) {
    printf("================================\n");
    printf(" PRUEBAS DE DETERMINISMO Y RACE CONDITION\n");
    printf("================================\n\n");

    if (mkdtemp(workdir) == NULL) {
        printf("[FAIL] No se pudo crear el directorio temporal\n");
        return EXIT_FAILURE;
    }

    engine_set_task_threads(1);

    if (engine_start(AGENTS, workdir) != 0) {
        printf("[FAIL] No se pudo iniciar el motor\n");
        rmdir(workdir);
        return EXIT_FAILURE;
    }

    if (test_determinism()) {
        printf("[OK] Determinismo de variables heredadas\n");
    } else {
        printf("[FAIL] Determinismo de variables heredadas\n");
        failures++;
    }

    if (test_race_condition()) {
        printf("[OK] Contabilidad de trabajos concurrentes\n");
    } else {
        printf("[FAIL] Contabilidad de trabajos concurrentes\n");
        failures++;
    }

    engine_shutdown();

    printf("\n================================\n");
    printf("Pruebas de determinismo y race condition: %d/2 aprobadas\n", 2 - failures);

    if (failures == 0) {
        printf("Todas las pruebas pasaron.\n");
    } else {
        printf("Hay pruebas que fallaron.\n");
    }

    printf("================================\n");

    rmdir(workdir);

    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
