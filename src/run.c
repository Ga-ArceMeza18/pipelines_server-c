#define _GNU_SOURCE  // asprintf, strdup
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include "run.h"

// =============================================================================
// Utilidades pequeñas

double now_s(void) {
    // comm: leer el reloj monotónico y convertirlo a segundos
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

const char *state_name(State s) {
    // comm: traducir cada valor del enum a su nombre legible
    switch (s) {
        case STATE_PENDING:           return "queued";
        case STATE_RUNNING:           return "running";
        case STATE_SUCCESS:           return "succeeded";
        case STATE_PARTIALLY_SUCCESS: return "partiallySucceeded";
        case STATE_FAILED:            return "failed";
        case STATE_CANCELLED:         return "canceled";
        case STATE_SKIPPED:           return "skipped";
    }

    return "unknown";
}

const char *step_kind_name(StepKind k) {
    // comm: traducir cada tipo de paso a su nombre legible
    switch (k) {
        case STEP_SCRIPT:   return "script";
        case STEP_TASK:     return "task";
        case STEP_PUBLISH:  return "publish";
        case STEP_DOWNLOAD: return "download";
    }

    return "unknown";
}

bool state_is_terminal(State s) {
    // comm: considerar final todo estado que no sea en cola ni en ejecución
    return s != STATE_PENDING && s != STATE_RUNNING;
}

int fs_mkdir_p(const char *path) {
    // comm: rechazar rutas inválidas
    if (path == NULL || path[0] == '\0') {
        return -1;
    }

    // comm: trabajar sobre una copia modificable de la ruta
    char *tmp = strdup(path);

    if (tmp == NULL) {
        return -1;
    }

    size_t len = strlen(tmp);

    // comm: crear cada prefijo de la ruta cortando la cadena en cada separador
    for (size_t i = 1; i < len; i++) {
        if (tmp[i] != '/') {
            continue;
        }

        tmp[i] = '\0';

        if (mkdir(tmp, 0755) != 0 && errno != EEXIST) {
            free(tmp);
            return -1;
        }

        tmp[i] = '/';
    }

    // comm: crear el último componente (puede existir ya)
    int rc = mkdir(tmp, 0755);
    int saved = errno;

    free(tmp);

    return (rc == 0 || saved == EEXIST) ? 0 : -1;
}

// =============================================================================
// Nombres y rutas

// comm: convertir un nombre del YAML en algo seguro para usar como nombre de archivo
static char *sanear(const char *s) {
    if (s == NULL) {
        s = "";
    }

    char *out = strdup(s);

    if (out == NULL) {
        return NULL;
    }

    // comm: reemplazar por '_' todo carácter que no sea alfanumérico, '_', '-' o '.'
    for (char *c = out; *c; c++) {
        unsigned char u = (unsigned char)*c;

        if (!isalnum(u) && *c != '_' && *c != '-' && *c != '.') {
            *c = '_';
        }
    }

    return out;
}

// comm: armar el nombre base "<indice>_<etapa>__<trabajo>"
// es único por trabajo y no contiene rutas peligrosas
static char *nombre_trabajo(const Run *r, int ji) {
    const Job *j = &r->p->jobs[ji];

    char *st = sanear(j->stage);
    char *jb = sanear(j->name);
    char *out = NULL;

    if (st != NULL && jb != NULL) {
        if (asprintf(&out, "%03d_%s__%s", ji, st, jb) < 0) {
            out = NULL;
        }
    }

    free(st);
    free(jb);

    return out;
}

char *run_job_log_path(const Run *r, int ji) {
    char *n = nombre_trabajo(r, ji);

    if (n == NULL) {
        return NULL;
    }

    // comm: armar la ruta <dir>/logs/<nombre>.log
    char *out = NULL;

    if (asprintf(&out, "%s/logs/%s.log", r->dir, n) < 0) {
        out = NULL;
    }

    free(n);

    return out;
}

char *run_job_workspace(const Run *r, int ji) {
    char *n = nombre_trabajo(r, ji);

    if (n == NULL) {
        return NULL;
    }

    // comm: armar la ruta <dir>/work/<nombre>
    char *out = NULL;

    if (asprintf(&out, "%s/work/%s", r->dir, n) < 0) {
        out = NULL;
    }

    free(n);

    return out;
}

char *run_artifact_path(const Run *r, const char *name) {
    // comm: rechazar nombres vacíos, con '/' (permitiría escapar de la carpeta) o especiales
    if (name == NULL || name[0] == '\0') {
        return NULL;
    }

    if (strchr(name, '/') != NULL) {
        return NULL;
    }

    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
        return NULL;
    }

    // comm: armar la ruta <dir>/artifacts/<name> (quien llama la libera)
    char *out = NULL;

    if (asprintf(&out, "%s/artifacts/%s", r->dir, name) < 0) {
        out = NULL;
    }

    return out;
}

// =============================================================================
// Ciclo de vida de la ejecución

Run *run_create(int id, Pipeline *p, const char *base_dir) {
    // comm: validar los argumentos recibidos
    if (p == NULL || base_dir == NULL) {
        return NULL;
    }

    // comm: reservar la ejecución en ceros y fijar sus valores iniciales
    Run *r = calloc(1, sizeof *r);

    if (r == NULL) {
        return NULL;
    }

    r->id = id;
    r->p = p;
    r->state = STATE_PENDING;
    atomic_init(&r->cancel, false);
    r->remaining = p->jobs_count;

    if (asprintf(&r->dir, "%s/run-%d", base_dir, id) < 0) {
        free(r);
        return NULL;
    }

    // comm: crear las subcarpetas de la ejecución (logs, work y artifacts)
    const char *nombres[] = {"logs", "work", "artifacts"};

    for (int i = 0; i < 3; i++) {
        char *sub = NULL;

        if (asprintf(&sub, "%s/%s", r->dir, nombres[i]) < 0) {
            free(r->dir);
            free(r);
            return NULL;
        }

        if (fs_mkdir_p(sub) != 0) {
            free(sub);
            free(r->dir);
            free(r);
            return NULL;
        }

        free(sub);
    }

    // comm: dejar cada trabajo y cada paso en su estado inicial
    for (int ji = 0; ji < p->jobs_count; ji++) {
        Job *j = &p->jobs[ji];

        j->state = STATE_PENDING;
        j->pending = j->deps_count;  // dependencias que faltan por terminar
        j->duration_s = 0.0;

        for (int si = 0; si < j->steps_count; si++) {
            j->steps[si].state = STATE_PENDING;
            j->steps[si].exit_code = 0;
            j->steps[si].duration_s = 0.0;
        }

        // comm: crear la bitácora vacía para que logs --follow la encuentre
        char *log = run_job_log_path(r, ji);

        if (log != NULL) {
            int fd = open(log, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);

            if (fd >= 0) {
                close(fd);
            }

            free(log);
        }
    }

    return r;
}

void run_destroy(Run *r) {
    if (r == NULL) {
        return;
    }

    // comm: liberar el pipeline, la carpeta y la ejecución
    pipeline_free(r->p);
    free(r->dir);
    free(r);
}
