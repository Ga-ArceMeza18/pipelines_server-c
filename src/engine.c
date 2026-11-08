// engine.c - Motor de ejecución: agentes (hilos OpenMP), cola de ejecuciones y consultas

#define _GNU_SOURCE
#include <omp.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "engine.h"
#include "run.h"
#include "runner.h"

/*
 * comm: proteger la contabilidad de fin de trabajo (pending, remaining, estados, contadores)
 * con -DSIN_PROTECCION se QUITA el candado de ese bloque: varios agentes que terminan
 * a la vez modifican los mismos contadores sin exclusión mutua (carrera deliberada)
 */
#ifdef SIN_PROTECCION
#define CONTABILIDAD_LOCK()   ((void)0)
#define CONTABILIDAD_UNLOCK() ((void)0)
#else
#define CONTABILIDAD_LOCK()   pthread_mutex_lock(&g_lock)
#define CONTABILIDAD_UNLOCK() pthread_mutex_unlock(&g_lock)
#endif

// =============================================================================
// Estado global del motor (todo bajo g_lock)

pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;  // candado global (extern en engine.h)
static pthread_cond_t g_cond = PTHREAD_COND_INITIALIZER;  // aviso a los agentes dormidos

static Run **runs = NULL;     // ejecuciones registradas
static size_t quantity = 0;   // cantidad de ejecuciones
static size_t capacity = 0;   // capacidad del arreglo de ejecuciones
static int n_agents = 0;      // agentes pedidos (hilos OpenMP)

static int task_threads_cfg = 0;  // hilos por tarea pedidos por el usuario (0 = automático)
static int task_threads_eff = 1;  // hilos por tarea efectivos, fijos tras engine_start
static int next_id = 1;           // próximo id de ejecución
static size_t rr_cursor = 0;      // ejecución desde la que se busca el próximo trabajo (turnos)

static bool started = false;   // el motor está en marcha
static bool stopping = false;  // el motor se está apagando: no se aceptan envíos

static pthread_t engine_thread;  // hilo que aloja la región paralela de OpenMP

static char *base_dir = NULL;  // carpeta de trabajo absoluta
static int running_now = 0;    // trabajos corriendo ahora
static int max_running = 0;    // pico de trabajos simultáneos observado
static long finished_total = 0;  // trabajos terminados desde el inicio

// =============================================================================
// Constructor de texto

typedef struct {
    char *s;
    size_t len;
    size_t cap;
} Buf;

// comm: agregar texto con formato printf a una cadena que crece sola
static void buf_printf(Buf *b, const char *fmt, ...) {
    va_list ap, ap2;

    // comm: medir cuánto ocupa el texto usando una copia de los argumentos
    va_start(ap, fmt);
    va_copy(ap2, ap);

    int need = vsnprintf(NULL, 0, fmt, ap);

    va_end(ap);

    if (need < 0) {
        va_end(ap2);
        return;
    }

    // comm: agrandar el buffer duplicando su capacidad hasta que el texto quepa
    if (b->len + (size_t)need + 1 > b->cap) {
        size_t ncap = b->cap ? b->cap * 2 : 256;

        while (ncap < b->len + (size_t)need + 1) {
            ncap *= 2;
        }

        char *t = realloc(b->s, ncap);

        if (t == NULL) {
            va_end(ap2);
            return;
        }

        b->s = t;
        b->cap = ncap;
    }

    // comm: escribir el texto y avanzar el largo
    vsnprintf(b->s + b->len, b->cap - b->len, fmt, ap2);
    va_end(ap2);

    b->len += (size_t)need;
}

// =============================================================================
// Lógica de estados (SIEMPRE con el candado tomado)

// comm: buscar una ejecución por su id
static Run *find_run(int id) {
    for (size_t i = 0; i < quantity; i++) {
        if (runs[i]->id == id) {
            return runs[i];
        }
    }

    return NULL;
}

// comm: cambiar a 'st' los pasos que siguen en cola (los que no corrieron)
static void steps_set_queued(Job *j, State st) {
    for (int i = 0; i < j->steps_count; i++) {
        if (j->steps[i].state == STATE_PENDING) {
            j->steps[i].state = st;
        }
    }
}

// comm: decidir el estado global de la ejecución cuando todos sus trabajos terminaron
static void run_finalize(Run *r) {
    // comm: ignorar llamadas repetidas (la recursión puede repetirlas)
    if (state_is_terminal(r->state)) {
        return;
    }

    // comm: revisar qué estados finales tuvieron los trabajos
    bool failed = false;
    bool partial = false;

    for (int i = 0; i < r->p->jobs_count; i++) {
        State s = r->p->jobs[i].state;

        if (s == STATE_FAILED) {
            failed = true;
        }

        if (s == STATE_PARTIALLY_SUCCESS) {
            partial = true;
        }
    }

    // comm: elegir el estado global: cancelada, fallida, parcial o exitosa
    if (atomic_load(&r->cancel)) {
        r->state = STATE_CANCELLED;
    } else if (failed) {
        r->state = STATE_FAILED;
    } else if (partial) {
        r->state = STATE_PARTIALLY_SUCCESS;
    } else {
        r->state = STATE_SUCCESS;
    }

    // comm: registrar el instante de fin (si nunca corrió nada, la duración es 0)
    r->end_s = now_s();

    if (r->start_s == 0.0) {
        r->start_s = r->end_s;
    }
}

static void job_terminated(Run *r, int ji);  // declaración adelantada (recursión mutua)

// comm: decidir si un trabajo corre o se salta cuando todas sus dependencias terminaron
static void job_deps_done(Run *r, int ji) {
    Job *j = &r->p->jobs[ji];

    // comm: resumir el resultado de las dependencias
    bool all_ok = true;
    bool any_failed = false;

    for (int d = 0; d < j->deps_count; d++) {
        switch (r->p->jobs[j->deps[d]].state) {
            case STATE_SUCCESS:
            case STATE_PARTIALLY_SUCCESS:
                break;
            case STATE_FAILED:
                any_failed = true;
                all_ok = false;
                break;
            default:
                all_ok = false;  // saltada o cancelada: no cuenta como éxito
                break;
        }
    }

    // comm: aplicar la condición del trabajo
    bool corre = true;

    switch (j->cond) {
        case CONDITION_FAILED: corre = any_failed; break;
        case CONDITION_ALWAYS: corre = true; break;
        case CONDITION_SUCCEEDED:
        case CONDITION_NONE:
        default: corre = all_ok; break;
    }

    // comm: saltar el trabajo y propagar el resultado a sus dependientes
    // si corre, queda en cola con pending == 0 y ya es elegible para un agente
    if (!corre) {
        j->state = STATE_SKIPPED;
        steps_set_queued(j, STATE_SKIPPED);
        job_terminated(r, ji);
    }
}

// comm: liberar a los dependientes de un trabajo terminado y revisar si la ejecución acabó
static void job_terminated(Run *r, int ji) {
    r->remaining--;

    // comm: recorrer los trabajos que aún esperan y descontar la dependencia terminada
    for (int k = 0; k < r->p->jobs_count; k++) {
        Job *o = &r->p->jobs[k];

        if (o->state != STATE_PENDING) {
            continue;
        }

        int hits = 0;

        for (int d = 0; d < o->deps_count; d++) {
            if (o->deps[d] == ji) {
                hits++;
            }
        }

        if (hits == 0) {
            continue;
        }

        o->pending -= hits;

        if (o->pending <= 0) {
            job_deps_done(r, k);
        }
    }

    // comm: cerrar la ejecución si ya no queda nada por terminar
    if (r->remaining == 0) {
        run_finalize(r);
    }
}

// comm: cancelar una ejecución (con el candado tomado)
static bool cancel_locked(Run *r) {
    if (state_is_terminal(r->state)) {
        return false;
    }

    // comm: avisar a los pasos en curso para que se detengan
    atomic_store(&r->cancel, true);

    // comm: cancelar los trabajos pendientes (los que corren terminan por su cuenta)
    for (int i = 0; i < r->p->jobs_count; i++) {
        Job *j = &r->p->jobs[i];

        if (j->state != STATE_PENDING) {
            continue;
        }

        j->state = STATE_CANCELLED;
        steps_set_queued(j, STATE_CANCELLED);
        r->remaining--;
    }

    // comm: terminar ya la ejecución si no había nada corriendo
    if (r->remaining == 0) {
        run_finalize(r);
    }

    return true;
}

// =============================================================================
// Agentes

// comm: elegir el próximo trabajo listo, repartiendo por turnos entre ejecuciones
// así ninguna ejecución se queda sin atender
static bool pick_next_job(Run **out_run, int *out_ji) {
    for (size_t i = 0; i < quantity; i++) {
        size_t idx = (rr_cursor + i) % quantity;
        Run *r = runs[idx];

        if (state_is_terminal(r->state)) {
            continue;
        }

        // comm: buscar en orden del YAML (determinista) un trabajo en cola sin dependencias
        for (int ji = 0; ji < r->p->jobs_count; ji++) {
            Job *j = &r->p->jobs[ji];

            if (j->state == STATE_PENDING && j->pending == 0) {
                rr_cursor = (idx + 1) % quantity;
                *out_run = r;
                *out_ji = ji;
                return true;
            }
        }
    }

    return false;
}

// comm: asentar el resultado de un trabajo y liberar a sus dependientes
static void finish_job(Run *r, int ji, State st, double dt) {
    CONTABILIDAD_LOCK();  // con -DSIN_PROTECCION no hace nada

    Job *j = &r->p->jobs[ji];

    j->state = st;
    j->duration_s = dt;
    finished_total++;
    job_terminated(r, ji);

    CONTABILIDAD_UNLOCK();

    // comm: despertar a los agentes, pues pudieron quedar trabajos listos
    pthread_cond_broadcast(&g_cond);
}

// comm: ejecutar el cuerpo de cada agente: tomar trabajos listos hasta que el motor se apague
static void agent_loop(void) {
    pthread_mutex_lock(&g_lock);  // el bucle corre con el candado, salvo al ejecutar el trabajo

    for (;;) {
        Run *r = NULL;
        int ji = 0;

        if (pick_next_job(&r, &ji)) {
            Job *j = &r->p->jobs[ji];

            // comm: ocupar el trabajo y marcar la ejecución como en curso si es el primero
            j->state = STATE_RUNNING;

            if (r->state == STATE_PENDING) {
                r->state = STATE_RUNNING;
                r->start_s = now_s();
            }

            // comm: actualizar el contador de agentes ocupados y su pico
            running_now++;

            if (running_now > max_running) {
                max_running = running_now;
            }

            // comm: soltar el candado mientras trabaja (si no, no habría paralelismo)
            pthread_mutex_unlock(&g_lock);

            double t0 = now_s();
            State st = run_job(r, j);
            double dt = now_s() - t0;

            finish_job(r, ji, st, dt);

            // comm: volver a tomar el candado y quedar libre para más trabajo
            pthread_mutex_lock(&g_lock);
            running_now--;
            continue;
        }

        // comm: terminar si el motor se apagó y no hay nada listo
        if (stopping) {
            break;
        }

        // comm: dormir (soltando el candado) hasta que haya novedades
        pthread_cond_wait(&g_cond, &g_lock);
    }

    pthread_mutex_unlock(&g_lock);
}

// comm: alojar la región paralela de OpenMP en un hilo POSIX
static void *engine_main(void *arg) {
    (void)arg;

    omp_set_dynamic(0);              // exige exactamente n_agents hilos
    omp_set_max_active_levels(2);    // permite un nivel anidado para los hilos de las tareas

    #pragma omp parallel num_threads(n_agents)
    {
        agent_loop();
    }

    return NULL;
}

// =============================================================================
// API pública

void engine_set_task_threads(int n) {
    // comm: guardar el tope pedido (se aplica en engine_start)
    task_threads_cfg = n;
}

int engine_task_threads(void) {
    // comm: devolver el tope efectivo (inmutable tras engine_start: no requiere candado)
    return task_threads_eff;
}

int engine_start(int agentes, const char *workdir) {
    // comm: validar argumentos y preparar la carpeta de trabajo absoluta
    if (agentes < 1 || workdir == NULL) {
        return -1;
    }

    if (fs_mkdir_p(workdir) != 0) {
        return -1;
    }

    char *abs = realpath(workdir, NULL);  // los hijos cambian de directorio: se necesita ruta absoluta

    if (abs == NULL) {
        return -1;
    }

    pthread_mutex_lock(&g_lock);

    // comm: rechazar un segundo arranque
    if (started) {
        pthread_mutex_unlock(&g_lock);
        free(abs);
        return -1;
    }

    base_dir = abs;
    n_agents = agentes;

    // comm: calcular los hilos por tarea: lo pedido por el usuario o un reparto de núcleos
    if (task_threads_cfg > 0) {
        task_threads_eff = task_threads_cfg;
    } else {
        int t = omp_get_num_procs() / agentes;

        task_threads_eff = t < 1 ? 1 : t;  // al menos 1: sin sobresuscripción
    }

    // comm: reiniciar el estado y los contadores del motor
    stopping = false;
    started = true;
    running_now = 0;
    max_running = 0;
    finished_total = 0;

    // comm: arrancar el hilo que aloja a los agentes
    if (pthread_create(&engine_thread, NULL, engine_main, NULL) != 0) {
        started = false;
        free(base_dir);
        base_dir = NULL;
        pthread_mutex_unlock(&g_lock);
        return -1;
    }

    pthread_mutex_unlock(&g_lock);

    return 0;
}

int engine_submit(Pipeline *p) {
    // comm: tomar posesión del pipeline (se libera si algo falla)
    if (p == NULL) {
        return -1;
    }

    // comm: reservar el id y copiar la carpeta base bajo el candado
    pthread_mutex_lock(&g_lock);

    if (!started || stopping) {
        pthread_mutex_unlock(&g_lock);
        pipeline_free(p);
        return -1;
    }

    int id = next_id++;
    char *dir = strdup(base_dir);  // copia: crear carpetas puede tardar y no se debe retener el candado

    pthread_mutex_unlock(&g_lock);

    if (dir == NULL) {
        pipeline_free(p);
        return -1;
    }

    // comm: crear las carpetas de la ejecución sin el candado
    Run *r = run_create(id, p, dir);

    free(dir);

    if (r == NULL) {
        pipeline_free(p);
        return -1;
    }

    // comm: publicar la ejecución bajo el candado, verificando que no se haya apagado el motor
    pthread_mutex_lock(&g_lock);

    if (stopping) {
        pthread_mutex_unlock(&g_lock);
        run_destroy(r);
        return -1;
    }

    // comm: agrandar el arreglo de ejecuciones si está lleno
    if (quantity == capacity) {
        size_t ncap = capacity ? capacity * 2 : 8;
        Run **t = realloc(runs, ncap * sizeof *runs);

        if (t == NULL) {
            pthread_mutex_unlock(&g_lock);
            run_destroy(r);
            return -1;
        }

        runs = t;
        capacity = ncap;
    }

    runs[quantity++] = r;

    // comm: decidir ya los trabajos sin dependencias (corren o quedan saltados según su condición)
    for (int ji = 0; ji < p->jobs_count; ji++) {
        Job *j = &p->jobs[ji];

        if (j->state == STATE_PENDING && j->deps_count == 0) {
            job_deps_done(r, ji);
        }
    }

    // comm: cerrar de inmediato un pipeline vacío o con todo saltado
    if (r->remaining == 0) {
        run_finalize(r);
    }

    pthread_mutex_unlock(&g_lock);

    // comm: despertar a los agentes y devolver el id al cliente
    pthread_cond_broadcast(&g_cond);

    return id;
}

bool engine_cancel(int id) {
    // comm: cancelar la ejecución si existe y sigue activa
    pthread_mutex_lock(&g_lock);

    Run *r = find_run(id);
    bool ok = (r != NULL) && cancel_locked(r);

    pthread_mutex_unlock(&g_lock);

    // comm: despertar a los agentes por si alguno esperaba
    if (ok) {
        pthread_cond_broadcast(&g_cond);
    }

    return ok;
}

void engine_shutdown(void) {
    // comm: ignorar el apagado si no hay motor o ya hay uno en curso (idempotente)
    pthread_mutex_lock(&g_lock);

    if (!started || stopping) {
        pthread_mutex_unlock(&g_lock);
        return;
    }

    // comm: rechazar nuevos envíos y cancelar las ejecuciones en curso
    stopping = true;

    for (size_t i = 0; i < quantity; i++) {
        cancel_locked(runs[i]);
    }

    pthread_mutex_unlock(&g_lock);

    // comm: despertar a los agentes dormidos y esperar a que todos terminen
    pthread_cond_broadcast(&g_cond);
    pthread_join(engine_thread, NULL);

    // comm: liberar todo el estado del motor (ya no hay agentes)
    pthread_mutex_lock(&g_lock);

    for (size_t i = 0; i < quantity; i++) {
        run_destroy(runs[i]);
    }

    free(runs);
    runs = NULL;
    quantity = 0;
    capacity = 0;

    free(base_dir);
    base_dir = NULL;

    rr_cursor = 0;
    started = false;
    stopping = false;  // permite volver a arrancar el motor

    pthread_mutex_unlock(&g_lock);
}

// =============================================================================
// Consultas

// comm: derivar el estado de una etapa a partir del estado de sus trabajos
static State stage_state(const Run *r, const char *stage) {
    int total = 0, q = 0, run = 0, part = 0, fail = 0, canc = 0, skip = 0;

    // comm: contar los trabajos de la etapa por estado
    for (int i = 0; i < r->p->jobs_count; i++) {
        const Job *j = &r->p->jobs[i];

        if (strcmp(j->stage ? j->stage : "", stage) != 0) {
            continue;
        }

        total++;

        switch (j->state) {
            case STATE_PENDING:           q++; break;
            case STATE_RUNNING:           run++; break;
            case STATE_PARTIALLY_SUCCESS: part++; break;
            case STATE_FAILED:            fail++; break;
            case STATE_CANCELLED:         canc++; break;
            case STATE_SKIPPED:           skip++; break;
            case STATE_SUCCESS:           break;
        }
    }

    // comm: elegir el estado de la etapa por prioridad
    if (run > 0) return STATE_RUNNING;
    if (q == total) return STATE_PENDING;
    if (q > 0) return STATE_RUNNING;  // unos terminaron y otros esperan: sigue en curso
    if (fail > 0) return STATE_FAILED;
    if (canc > 0) return STATE_CANCELLED;
    if (part > 0) return STATE_PARTIALLY_SUCCESS;
    if (skip == total) return STATE_SKIPPED;

    return STATE_SUCCESS;
}

// comm: formatear una duración como "1.23s", o "-" si aún no termina
static void fmt_dur(char *out, size_t n, State st, double d) {
    if (state_is_terminal(st)) {
        snprintf(out, n, "%.2fs", d);
    } else {
        snprintf(out, n, "-");
    }
}

char *engine_status(int id) {
    // comm: tomar una foto consistente: nadie modifica el estado mientras se arma el texto
    pthread_mutex_lock(&g_lock);

    Run *r = find_run(id);

    if (r == NULL) {
        pthread_mutex_unlock(&g_lock);
        return NULL;
    }

    // comm: escribir el encabezado con el estado global y la duración
    Buf b = {0};
    double dur = 0.0;

    if (r->start_s != 0.0) {
        dur = (state_is_terminal(r->state) ? r->end_s : now_s()) - r->start_s;
    }

    buf_printf(&b, "Ejecucion %d: %s\n", r->id, r->p->name ? r->p->name : "(sin nombre)");
    buf_printf(&b, "Estado: %s  Duracion: %.2fs\n", state_name(r->state), dur);

    // comm: recolectar las etapas distintas en orden de aparición
    int nj = r->p->jobs_count;
    const char **stages = calloc((size_t)(nj > 0 ? nj : 1), sizeof *stages);
    int ns = 0;

    for (int i = 0; stages != NULL && i < nj; i++) {
        const char *sn = r->p->jobs[i].stage ? r->p->jobs[i].stage : "";
        bool vista = false;

        for (int s = 0; s < ns; s++) {
            if (strcmp(stages[s], sn) == 0) {
                vista = true;
            }
        }

        if (!vista) {
            stages[ns++] = sn;
        }
    }

    // comm: imprimir etapa por etapa, con sus trabajos y los pasos de cada trabajo
    for (int s = 0; s < ns; s++) {
        buf_printf(&b, "Etapa %s [%s]\n", stages[s], state_name(stage_state(r, stages[s])));

        for (int i = 0; i < nj; i++) {
            const Job *j = &r->p->jobs[i];

            if (strcmp(j->stage ? j->stage : "", stages[s]) != 0) {
                continue;
            }

            char d[32];

            fmt_dur(d, sizeof d, j->state, j->duration_s);
            buf_printf(&b, "  Trabajo %s [%s] %s\n", j->name ? j->name : "?",
                       state_name(j->state), d);

            for (int k = 0; k < j->steps_count; k++) {
                const Step *st = &j->steps[k];

                fmt_dur(d, sizeof d, st->state, st->duration_s);
                buf_printf(&b, "    Paso %d (%s) [%s] %s exit=%d", k + 1,
                           step_kind_name(st->kind), state_name(st->state), d, st->exit_code);

                // comm: mostrar un resumen de una línea (máximo 60 caracteres)
                // en publish el nombre del artefacto está en artifact; en los demás, en text
                const char *txt = (st->kind == STEP_PUBLISH) ? st->artifact : st->text;

                if (txt != NULL) {
                    size_t len = strcspn(txt, "\n");

                    if (len > 60) {
                        len = 60;
                    }

                    buf_printf(&b, " : %.*s", (int)len, txt);
                }

                buf_printf(&b, "\n");
            }
        }
    }

    free(stages);
    pthread_mutex_unlock(&g_lock);

    return b.s ? b.s : strdup("");
}

char *engine_list(void) {
    // comm: tomar una foto consistente de todas las ejecuciones
    pthread_mutex_lock(&g_lock);

    Buf b = {0};

    if (quantity == 0) {
        buf_printf(&b, "(sin ejecuciones)\n");
    }

    // comm: escribir una línea por ejecución con id, estado y nombre
    for (size_t i = 0; i < quantity; i++) {
        const Run *r = runs[i];

        buf_printf(&b, "%d  %s  %s\n", r->id, state_name(r->state),
                   r->p->name ? r->p->name : "(sin nombre)");
    }

    pthread_mutex_unlock(&g_lock);

    return b.s ? b.s : strdup("");  // quien llama lo libera
}

int engine_run_state(int id, State *out) {
    // comm: copiar el estado de la ejecución si existe
    pthread_mutex_lock(&g_lock);

    Run *r = find_run(id);

    if (r != NULL) {
        *out = r->state;
    }

    pthread_mutex_unlock(&g_lock);

    return r ? 0 : -1;
}

int engine_job_state(int id, const char *job, State *out) {
    int rc = -1;

    pthread_mutex_lock(&g_lock);

    Run *r = find_run(id);

    // comm: buscar el trabajo por nombre (gana el primero que coincida) y copiar su estado
    for (int i = 0; r != NULL && job != NULL && i < r->p->jobs_count; i++) {
        if (r->p->jobs[i].name != NULL && strcmp(r->p->jobs[i].name, job) == 0) {
            *out = r->p->jobs[i].state;
            rc = 0;
            break;
        }
    }

    pthread_mutex_unlock(&g_lock);

    return rc;
}

void engine_free_paths(char **paths, int n) {
    if (paths == NULL) {
        return;
    }

    // comm: liberar cada ruta y luego el arreglo
    for (int i = 0; i < n; i++) {
        free(paths[i]);
    }

    free(paths);
}

int engine_log_paths(int id, const char *job, char ***paths) {
    *paths = NULL;

    int count = -1;

    pthread_mutex_lock(&g_lock);

    Run *r = find_run(id);

    if (r != NULL) {
        // comm: elegir el rango de trabajos: todos, o solo el que coincida con el nombre
        int first = 0;
        int last = r->p->jobs_count;

        if (job != NULL) {
            first = -1;

            for (int i = 0; i < r->p->jobs_count; i++) {
                if (r->p->jobs[i].name != NULL && strcmp(r->p->jobs[i].name, job) == 0) {
                    first = i;
                    break;
                }
            }

            last = first + 1;
        }

        // comm: armar el arreglo con la ruta de la bitácora de cada trabajo del rango
        if (first >= 0) {
            int n = last - first;
            char **arr = calloc((size_t)(n > 0 ? n : 1), sizeof *arr);
            bool ok = arr != NULL;

            for (int i = 0; ok && i < n; i++) {
                arr[i] = run_job_log_path(r, first + i);

                if (arr[i] == NULL) {
                    ok = false;
                }
            }

            if (ok) {
                *paths = arr;
                count = n;
            } else {
                engine_free_paths(arr, n);
            }
        }
    }

    pthread_mutex_unlock(&g_lock);

    return count;
}

int engine_max_running(void) {
    // comm: leer el pico de trabajos simultáneos (para pruebas)
    pthread_mutex_lock(&g_lock);

    int v = max_running;

    pthread_mutex_unlock(&g_lock);

    return v;
}

long engine_jobs_finished(void) {
    // comm: leer el total de trabajos terminados (para la prueba de -DSIN_PROTECCION)
    pthread_mutex_lock(&g_lock);

    long v = finished_total;

    pthread_mutex_unlock(&g_lock);

    return v;
}
