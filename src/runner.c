#define _GNU_SOURCE  // asprintf
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "data_tasks.h"
#include "engine.h"
#include "process.h"
#include "runner.h"
#include "simple_tasks.h"
#include "task_ctx.h"

// =============================================================================
// Bitácora

// comm: agregar UNA línea a la bitácora con un solo write() sobre un archivo O_APPEND
static void log_write(const char *path, const char *fmt, ...) {
    // comm: armar la línea completa en memoria (si no cabe, se corta sin romper memoria)
    char buf[1024];
    va_list ap;

    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);

    if (n < 0) {
        return;
    }

    if ((size_t)n >= sizeof buf) {
        n = (int)sizeof buf - 1;
    }

    // comm: escribir de una sola vez para que la línea no se mezcle ni se parta
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);

    if (fd < 0) {
        return;
    }

    ssize_t w = write(fd, buf, (size_t)n);
    (void)w;

    close(fd);
}

// =============================================================================
// Estados de los pasos (siempre bajo g_lock)

// comm: fijar estado, código de salida y duración de un paso en un solo bloque
// status y list pueden estar leyendo, por eso se asienta todo junto
static void step_update(Step *s, State st, int exit_code, double dur) {
    pthread_mutex_lock(&g_lock);

    s->state = st;
    s->exit_code = exit_code;
    s->duration_s = dur;

    pthread_mutex_unlock(&g_lock);
}

// comm: cambiar solo el estado de un paso
static void step_set_state(Step *s, State st) {
    pthread_mutex_lock(&g_lock);
    s->state = st;
    pthread_mutex_unlock(&g_lock);
}

// comm: marcar con 'st' los pasos desde 'from' que sigan en cola
static void mark_remaining(Job *job, int from, State st) {
    pthread_mutex_lock(&g_lock);

    for (int i = from; i < job->steps_count; i++) {
        if (job->steps[i].state == STATE_PENDING) {
            job->steps[i].state = st;
        }
    }

    pthread_mutex_unlock(&g_lock);
}

// =============================================================================
// Sistema de archivos (artefactos)

// comm: validar que una ruta sea relativa y sin ".." (no escapa del espacio de trabajo)
static bool path_safe(const char *p) {
    if (p == NULL || p[0] == '\0' || p[0] == '/') {
        return false;
    }

    // comm: revisar componente por componente
    const char *s = p;

    while (*s) {
        const char *e = strchr(s, '/');
        size_t len = e ? (size_t)(e - s) : strlen(s);

        if (len == 2 && s[0] == '.' && s[1] == '.') {
            return false;
        }

        if (e == NULL) {
            break;
        }

        s = e + 1;
    }

    return true;
}

// comm: copiar un archivo regular por bloques de 64 KB
static int copy_file(const char *src, const char *dst, mode_t mode) {
    // comm: abrir el origen sin seguir enlaces simbólicos y crear el destino
    int in = open(src, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);

    if (in < 0) {
        return -1;
    }

    int out = open(dst, O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW | O_CLOEXEC, mode & 0777);

    if (out < 0) {
        close(in);
        return -1;
    }

    char buf[65536];
    int rc = 0;

    // comm: leer un bloque y escribirlo completo (write puede escribir menos de lo pedido)
    for (;;) {
        ssize_t n = read(in, buf, sizeof buf);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }

            rc = -1;
            break;
        }

        if (n == 0) {
            break;
        }

        ssize_t off = 0;

        while (off < n) {
            ssize_t w = write(out, buf + off, (size_t)(n - off));

            if (w < 0) {
                if (errno == EINTR) {
                    continue;
                }

                rc = -1;
                break;
            }

            off += w;
        }

        if (rc != 0) {
            break;
        }
    }

    // comm: cerrar ambos archivos (close puede reportar errores de escritura diferida)
    close(in);

    if (close(out) != 0) {
        rc = -1;
    }

    return rc;
}

// comm: copiar un archivo o un directorio de forma recursiva
static int copy_tree(const char *src, const char *dst) {
    // comm: identificar el tipo del origen sin seguir enlaces simbólicos
    struct stat st;

    if (lstat(src, &st) != 0) {
        return -1;
    }

    if (S_ISREG(st.st_mode)) {
        return copy_file(src, dst, st.st_mode);
    }

    if (!S_ISDIR(st.st_mode)) {
        return 0;  // enlaces, sockets, etc.: se omiten
    }

    // comm: crear el directorio destino y abrir el origen
    if (mkdir(dst, (st.st_mode & 0777) | 0700) != 0 && errno != EEXIST) {
        return -1;
    }

    DIR *d = opendir(src);

    if (d == NULL) {
        return -1;
    }

    int rc = 0;
    struct dirent *e;

    // comm: copiar cada entrada; ante un error se detiene
    while ((e = readdir(d)) != NULL) {
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) {
            continue;
        }

        char s[PATH_MAX], t[PATH_MAX];

        if (snprintf(s, sizeof s, "%s/%s", src, e->d_name) >= (int)sizeof s ||
            snprintf(t, sizeof t, "%s/%s", dst, e->d_name) >= (int)sizeof t) {
            rc = -1;
            break;
        }

        if (copy_tree(s, t) != 0) {
            rc = -1;
            break;
        }
    }

    closedir(d);

    return rc;
}

// comm: borrar un archivo o directorio (limpieza tras un publish fallido)
static void remove_tree(const char *path) {
    struct stat st;

    if (lstat(path, &st) != 0) {
        return;
    }

    // comm: borrar primero el contenido si es un directorio
    if (S_ISDIR(st.st_mode)) {
        DIR *d = opendir(path);

        if (d != NULL) {
            struct dirent *e;

            while ((e = readdir(d)) != NULL) {
                if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) {
                    continue;
                }

                char c[PATH_MAX];

                if (snprintf(c, sizeof c, "%s/%s", path, e->d_name) < (int)sizeof c) {
                    remove_tree(c);
                }
            }

            closedir(d);
        }

        rmdir(path);
    } else {
        unlink(path);
    }
}

// =============================================================================
// Variables heredadas

// comm: marcar en vis todos los ancestros (directos e indirectos) del trabajo ji
static void marcar_ancestros(const Pipeline *p, int ji, bool *vis) {
    const Job *j = &p->jobs[ji];

    for (int d = 0; d < j->deps_count; d++) {
        int a = j->deps[d];

        if (a < 0 || a >= p->jobs_count || vis[a]) {
            continue;
        }

        vis[a] = true;
        marcar_ancestros(p, a, vis);
    }
}

// comm: copiar a env las variables de salida de todos los ancestros de 'job'
// regla determinista para variables repetidas: gana el ancestro con MÁS ancestros propios
// (el más profundo, así un descendiente siempre pisa a su ancestro) y, si empatan
// (trabajos paralelos), gana el que aparece DESPUÉS en el YAML
// el resultado no depende de qué hilo terminó primero
static void heredar_variables(const Run *run, const Job *job, VarList *env) {
    const Pipeline *p = run->p;
    int ji = (int)(job - p->jobs);
    int n = p->jobs_count;

    // comm: reservar los arreglos auxiliares (ancestros, profundidad y marcas temporales)
    bool *anc = calloc((size_t)n, sizeof *anc);
    bool *tmp = calloc((size_t)n, sizeof *tmp);
    int *cnt = calloc((size_t)n, sizeof *cnt);

    if (anc == NULL || tmp == NULL || cnt == NULL) {
        free(anc);
        free(tmp);
        free(cnt);
        return;  // sin memoria: no se hereda nada
    }

    // comm: calcular los ancestros del trabajo y cuántos ancestros tiene cada uno
    marcar_ancestros(p, ji, anc);

    for (int j = 0; j < n; j++) {
        if (!anc[j]) {
            continue;
        }

        memset(tmp, 0, (size_t)n * sizeof *tmp);
        marcar_ancestros(p, j, tmp);

        for (int k = 0; k < n; k++) {
            if (tmp[k]) {
                cnt[j]++;
            }
        }
    }

    // comm: copiar las variables de menos profundo a más profundo, y en orden del YAML dentro de cada nivel
    // la última en escribirse gana
    for (int nivel = 0; nivel < n; nivel++) {
        for (int j = 0; j < n; j++) {
            if (!anc[j] || cnt[j] != nivel) {
                continue;
            }

            // ese ancestro ya terminó: nadie escribe sus variables
            const VarList *ov = &p->jobs[j].out_vars;

            for (int i = 0; i < ov->size; i++) {
                var_set(env, ov->items[i].name, ov->items[i].value);
            }
        }
    }

    free(anc);
    free(tmp);
    free(cnt);
}

// =============================================================================
// Ejecución de un paso

// comm: ejecutar un paso según su tipo
// devuelve 0 si ok, > 0 si falló, o PROC_TIMEOUT / PROC_CANCELED
static int run_step(Run *run, Job *job, Step *step, const VarList *env, double deadline,
                    const char *log, const char *ws, VarList *newvars) {
    (void)job;  // por ahora el paso no necesita datos del trabajo

    switch (step->kind) {

    // comm: ejecutar un comando de shell como proceso hijo
    case STEP_SCRIPT: {
        // comm: sustituir $(VAR) y dejar el comando en la bitácora
        char *cmd = substitute(step->text ? step->text : "", env);

        if (cmd == NULL) {
            return 1;
        }

        log_write(log, "$ %s\n", cmd);

        // comm: calcular el tiempo restante del trabajo (0 = sin límite)
        double rem = 0.0;

        if (deadline > 0.0) {
            rem = deadline - now_s();

            if (rem <= 0.0) {
                free(cmd);
                return PROC_TIMEOUT;
            }
        }

        // comm: lanzar el comando con /bin/sh y registrar si ni siquiera pudo lanzarse
        char *argv[] = {"/bin/sh", "-c", cmd, NULL};
        int rc = run_process(argv, log, run, rem, ws, newvars);

        free(cmd);

        if (rc == PROC_ERROR) {
            log_write(log, "error: no se pudo ejecutar el comando\n");
            return 1;
        }

        return rc;
    }

    // comm: ejecutar una tarea integrada (Sleep, Cpu, WordCount o Histogram)
    case STEP_TASK: {
        // comm: copiar los inputs sustituyendo $(VAR) en cada valor
        VarList in;

        varlist_init(&in);

        for (int i = 0; i < step->inputs.size; i++) {
            char *v = substitute(step->inputs.items[i].value, env);

            if (v == NULL) {
                varlist_destroy(&in);
                return 1;
            }

            var_set(&in, step->inputs.items[i].name, v);
            free(v);  // var_set hizo su propia copia
        }

        // comm: abrir la bitácora con buffer por línea para que cada línea salga completa
        FILE *lf = fopen(log, "a");

        if (lf == NULL) {
            varlist_destroy(&in);
            return 1;
        }

        setvbuf(lf, NULL, _IOLBF, 0);

        // comm: armar el contexto que ve la tarea
        TaskCtx ctx = {
            .inputs = &in,
            .out = newvars,
            .log = lf,
            .workdir = ws,
            .cancel = &run->cancel,
            .deadline_s = deadline,
            .max_threads = engine_task_threads(),
        };

        // comm: elegir la función de la tarea según su nombre
        int rc;
        const char *t = step->text ? step->text : "";

        if (strcmp(t, "Sleep@1") == 0) {
            rc = task_sleep(&ctx);
        } else if (strcmp(t, "Cpu@1") == 0) {
            rc = task_cpu(&ctx);
        } else if (strcmp(t, "WordCount@1") == 0) {
            rc = task_wordcount(&ctx);
        } else if (strcmp(t, "Histogram@1") == 0) {
            rc = task_histogram(&ctx);
        } else {
            fprintf(lf, "error: tarea desconocida '%s'\n", t);
            rc = 1;
        }

        fclose(lf);
        varlist_destroy(&in);

        // comm: reservar los negativos para PROC_* y distinguir si el fallo fue por cancelación o timeout
        if (rc < 0) {
            rc = 1;
        }

        if (rc != 0) {
            if (atomic_load(&run->cancel)) {
                return PROC_CANCELED;
            }

            if (deadline > 0.0 && now_s() >= deadline) {
                return PROC_TIMEOUT;
            }
        }

        return rc;
    }

    // comm: publicar un archivo o directorio del espacio de trabajo como artefacto
    // en publish, text guarda la ruta a publicar y artifact guarda el nombre del artefacto
    case STEP_PUBLISH: {
        char *path = substitute(step->text ? step->text : "", env);
        char *art = substitute(step->artifact ? step->artifact : "", env);
        int rc = 1;
        char *adir = NULL;
        char *src = NULL;

        if (art == NULL || path == NULL) {
            rc = 1;
        } else if (!path_safe(path)) {
            // comm: rechazar rutas que intenten salirse del espacio de trabajo
            log_write(log, "error: ruta no permitida '%s'\n", path);
        } else if ((adir = run_artifact_path(run, art)) == NULL) {
            log_write(log, "error: nombre de artefacto invalido '%s'\n", art);
        } else if (asprintf(&src, "%s/%s", ws, path) < 0) {
            src = NULL;  // asprintf deja basura si falla
        } else {
            struct stat st;

            if (lstat(src, &st) != 0 || (!S_ISREG(st.st_mode) && !S_ISDIR(st.st_mode))) {
                log_write(log, "error: no existe '%s' para publicar\n", path);
            } else if (mkdir(adir, 0755) != 0) {
                // mkdir es atómico: solo UN trabajo puede crear el artefacto
                log_write(log, "error: el artefacto '%s' ya existe o no se pudo crear\n", art);
            } else {
                // comm: copiar el directorio completo o el archivo dentro de la carpeta del artefacto
                int cr;

                if (S_ISDIR(st.st_mode)) {
                    cr = copy_tree(src, adir);
                } else {
                    const char *base = strrchr(path, '/');
                    char *dst = NULL;

                    base = base ? base + 1 : path;
                    cr = (asprintf(&dst, "%s/%s", adir, base) < 0)
                        ? -1 : copy_file(src, dst, st.st_mode);

                    free(dst);
                }

                // comm: registrar el resultado y no dejar artefactos a medias si falló
                if (cr == 0) {
                    log_write(log, "artefacto '%s' publicado\n", art);
                    rc = 0;
                } else {
                    log_write(log, "error: no se pudo copiar el artefacto '%s'\n", art);
                    remove_tree(adir);
                }
            }
        }

        free(adir);
        free(src);
        free(art);
        free(path);

        return rc;
    }

    // comm: copiar un artefacto publicado al espacio de trabajo
    // en download, text guarda el nombre del artefacto y artifact guarda el directorio destino
    case STEP_DOWNLOAD: {
        char *art = substitute(step->text ? step->text : "", env);
        char *path = substitute(step->artifact ? step->artifact : "", env);
        int rc = 1;
        char *adir = NULL;
        char *dst = NULL;

        if (art == NULL || path == NULL) {
            rc = 1;
        } else if (!path_safe(path)) {
            log_write(log, "error: ruta no permitida '%s'\n", path);
        } else if ((adir = run_artifact_path(run, art)) == NULL) {
            log_write(log, "error: nombre de artefacto invalido '%s'\n", art);
        } else {
            struct stat st;

            if (stat(adir, &st) != 0 || !S_ISDIR(st.st_mode)) {
                // el artefacto no existe (nadie lo publicó): el paso falla sin esperar
                log_write(log, "error: el artefacto '%s' no existe\n", art);
            } else if (asprintf(&dst, "%s/%s", ws, path) < 0) {
                dst = NULL;
            } else if (fs_mkdir_p(dst) != 0 || copy_tree(adir, dst) != 0) {
                log_write(log, "error: no se pudo descargar '%s'\n", art);
            } else {
                log_write(log, "artefacto '%s' descargado en '%s'\n", art, path);
                rc = 0;
            }
        }

        free(adir);
        free(dst);
        free(art);
        free(path);

        return rc;
    }
    }

    return 1;  // tipo de paso desconocido
}

// =============================================================================
// Ejecución del trabajo

// comm: ejecutar los pasos de un trabajo en orden y decidir su estado final
static State ejecutar_pasos(Run *run, Job *job, VarList *env, const char *ws, const char *log) {
    // comm: calcular el límite absoluto del trabajo (0 = sin límite)
    double deadline = (job->timeout_s > 0) ? now_s() + (double)job->timeout_s : 0.0;
    bool partial = false;  // true si algún paso con continueOnError falló

    for (int i = 0; i < job->steps_count; i++) {
        Step *s = &job->steps[i];

        // comm: detener el trabajo si se pidió cancelar entre pasos
        if (atomic_load(&run->cancel)) {
            mark_remaining(job, i, STATE_CANCELLED);
            return STATE_CANCELLED;
        }

        // comm: fallar el trabajo si el tiempo se agotó entre pasos
        if (deadline > 0.0 && now_s() >= deadline) {
            log_write(log, "error: tiempo limite del trabajo excedido\n");
            mark_remaining(job, i, STATE_SKIPPED);
            return STATE_FAILED;
        }

        // comm: dejar el encabezado del paso en la bitácora y marcarlo en ejecución
        log_write(log, "[paso %d] %s\n", i + 1, step_kind_name(s->kind));
        step_set_state(s, STATE_RUNNING);

        // comm: ejecutar el paso midiendo su duración
        double t0 = now_s();
        VarList newvars;

        varlist_init(&newvars);

        int rc = run_step(run, job, s, env, deadline, log, ws, &newvars);
        double dt = now_s() - t0;

        // comm: propagar las variables nuevas a los pasos siguientes y a los trabajos dependientes
        for (int k = 0; k < newvars.size; k++) {
            var_set(env, newvars.items[k].name, newvars.items[k].value);
            var_set(&job->out_vars, newvars.items[k].name, newvars.items[k].value);
        }

        varlist_destroy(&newvars);

        int code = (rc >= 0) ? rc : -1;  // código que se muestra en status

        // comm: continuar con el siguiente paso si este tuvo éxito
        if (rc == 0) {
            step_update(s, STATE_SUCCESS, 0, dt);
            continue;
        }

        // comm: cancelar el trabajo si el paso fue cancelado mientras corría
        if (rc == PROC_CANCELED) {
            step_update(s, STATE_CANCELLED, code, dt);
            mark_remaining(job, i + 1, STATE_CANCELLED);
            return STATE_CANCELLED;
        }

        // comm: fallar el trabajo si venció su tiempo límite
        if (rc == PROC_TIMEOUT) {
            log_write(log, "error: tiempo limite del trabajo excedido\n");
            step_update(s, STATE_FAILED, code, dt);
            mark_remaining(job, i + 1, STATE_SKIPPED);
            return STATE_FAILED;
        }

        // comm: tratar el fallo normal: tolerarlo con continueOnError o fallar el trabajo
        step_update(s, STATE_FAILED, code, dt);

        if (s->continue_on_error) {
            partial = true;
            continue;
        }

        mark_remaining(job, i + 1, STATE_SKIPPED);
        return STATE_FAILED;
    }

    return partial ? STATE_PARTIALLY_SUCCESS : STATE_SUCCESS;
}

State run_job(Run *run, Job *job) {
    // comm: preparar el espacio de trabajo y la bitácora propios del trabajo
    int ji = (int)(job - run->p->jobs);
    char *ws = run_job_workspace(run, ji);
    char *log = run_job_log_path(run, ji);
    State result = STATE_FAILED;  // pesimista hasta demostrar lo contrario

    if (ws == NULL || log == NULL || fs_mkdir_p(ws) != 0) {
        if (log != NULL) {
            log_write(log, "error: no se pudo crear el espacio de trabajo\n");
        }

        mark_remaining(job, 0, STATE_SKIPPED);
    } else {
        // comm: armar las variables visibles: primero las globales, luego las de los ancestros
        VarList env;

        varlist_init(&env);

        for (int i = 0; i < run->p->vars.size; i++) {
            var_set(&env, run->p->vars.items[i].name, run->p->vars.items[i].value);
        }

        heredar_variables(run, job, &env);

        // comm: correr los pasos y liberar las variables locales
        result = ejecutar_pasos(run, job, &env, ws, log);

        varlist_destroy(&env);
    }

    free(ws);
    free(log);

    return result;  // el Engine asienta el estado final
}
