#define _GNU_SOURCE                      // asprintf
#include <dirent.h>                      // opendir, readdir
#include <errno.h>                       // errno
#include <fcntl.h>                       // open
#include <limits.h>                      // PATH_MAX
#include <pthread.h>                     // g_lock
#include <stdarg.h>                      // va_list para log_write
#include <stdio.h>                       // snprintf, FILE
#include <stdlib.h>                      // free
#include <string.h>                      // strcmp, strrchr
#include <sys/stat.h>                    // stat, lstat, mkdir
#include <unistd.h>                      // read, write, close, unlink, rmdir
#include "engine.h"                      // g_lock y engine_task_threads()
#include "process.h"                     // run_process
#include "runner.h"                      // declaración de run_job
#include "task_ctx.h"                    // TaskCtx y las funciones de las tareas

/* ---------- bitácora ---------- */

/* Agrega UNA línea a la bitácora con un solo write() sobre un archivo O_APPEND. */
static void log_write(const char *path, const char *fmt, ...) {
    char buf[1024];                      // la línea completa se arma en memoria
    va_list ap;                          // argumentos variables
    va_start(ap, fmt);                   // empieza a leerlos
    int n = vsnprintf(buf, sizeof buf, fmt, ap);  // formatea como printf
    va_end(ap);                          // termina de leerlos
    if (n < 0) return;                   // error de formato
    if ((size_t)n >= sizeof buf) n = (int)sizeof buf - 1;  // si no cupo, se corta (nunca se rompe memoria)
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);  // append: el SO asegura que se escribe al final
    if (fd < 0) return;                  // sin bitácora no hay nada que hacer
    ssize_t w = write(fd, buf, (size_t)n);  // un solo write: la línea no se mezcla ni se parte
    if (w < 0) { close(fd); return; }    // si falló, solo cerramos
    close(fd);                           // cierra el archivo
}

/* ---------- estados de los pasos (siempre bajo g_lock) ---------- */

static void step_update(Step *s, State st, int exit_code, double dur) {  // fija estado, código y duración
    pthread_mutex_lock(&g_lock);         // status/list pueden estar leyendo: se asienta todo junto
    s->state = st;                       // nuevo estado
    s->exit_code = exit_code;            // código de salida
    s->duration_s = dur;                 // duración
    pthread_mutex_unlock(&g_lock);       // libera
}

static void step_set_state(Step *s, State st) {  // solo cambia el estado
    pthread_mutex_lock(&g_lock);         // protege contra lecturas de status
    s->state = st;                       // nuevo estado
    pthread_mutex_unlock(&g_lock);       // libera
}

static void mark_remaining(Job *job, int from, State st) {  // pasos from..fin que sigan en queued
    pthread_mutex_lock(&g_lock);         // un solo candado para todos
    for (int i = from; i < job->n_steps; i++) {  // recorre los pasos restantes
        if (job->steps[i].state == STATE_QUEUED) job->steps[i].state = st;  // los que no corrieron
    }
    pthread_mutex_unlock(&g_lock);       // libera
}

/* ---------- sistema de archivos (artefactos) ---------- */

static bool path_safe(const char *p) {   // ruta relativa y sin ".." (no escapa del espacio de trabajo)
    if (p == NULL || p[0] == '\0' || p[0] == '/') return false;  // vacía o absoluta
    const char *s = p;                   // inicio del componente actual
    while (*s) {                         // recorre componente por componente
        const char *e = strchr(s, '/');  // fin del componente
        size_t len = e ? (size_t)(e - s) : strlen(s);  // largo del componente
        if (len == 2 && s[0] == '.' && s[1] == '.') return false;  // ".." prohibido
        if (e == NULL) break;            // era el último
        s = e + 1;                       // siguiente componente
    }
    return true;                         // ruta aceptable
}

static int copy_file(const char *src, const char *dst, mode_t mode) {  // copia un archivo regular
    int in = open(src, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);  // no sigue enlaces simbólicos
    if (in < 0) return -1;               // no se pudo abrir el origen
    int out = open(dst, O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW | O_CLOEXEC, mode & 0777);  // crea el destino
    if (out < 0) { close(in); return -1; }  // no se pudo crear
    char buf[65536];                     // buffer de 64 KB
    int rc = 0;                          // resultado
    for (;;) {                           // copia por bloques
        ssize_t n = read(in, buf, sizeof buf);  // lee un bloque
        if (n < 0) {                     // error de lectura
            if (errno == EINTR) continue;  // interrumpido por señal: reintenta
            rc = -1;                     // error real
            break;                       // sale
        }
        if (n == 0) break;               // fin del archivo
        ssize_t off = 0;                 // cuánto se ha escrito de este bloque
        while (off < n) {                // write puede escribir menos de lo pedido
            ssize_t w = write(out, buf + off, (size_t)(n - off));  // escribe el resto
            if (w < 0) {                 // error de escritura
                if (errno == EINTR) continue;  // reintenta
                rc = -1;                 // error real
                break;                   // sale del while
            }
            off += w;                    // avanza
        }
        if (rc != 0) break;              // error: no seguimos
    }
    close(in);                           // cierra origen
    if (close(out) != 0) rc = -1;        // close puede reportar errores de escritura diferida
    return rc;                           // 0 ok, -1 error
}

static int copy_tree(const char *src, const char *dst) {  // copia archivo o directorio (recursivo)
    struct stat st;                      // tipo del origen
    if (lstat(src, &st) != 0) return -1; // lstat no sigue enlaces simbólicos
    if (S_ISREG(st.st_mode)) return copy_file(src, dst, st.st_mode);  // archivo normal
    if (!S_ISDIR(st.st_mode)) return 0;  // enlaces, sockets, etc.: se omiten
    if (mkdir(dst, (st.st_mode & 0777) | 0700) != 0 && errno != EEXIST) return -1;  // crea el directorio destino
    DIR *d = opendir(src);               // abre el directorio origen
    if (d == NULL) return -1;            // no se pudo
    int rc = 0;                          // resultado
    struct dirent *e;                    // entrada actual
    while ((e = readdir(d)) != NULL) {   // recorre las entradas
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) continue;  // se omiten
        char s[PATH_MAX], t[PATH_MAX];   // rutas hijas
        if (snprintf(s, sizeof s, "%s/%s", src, e->d_name) >= (int)sizeof s ||
            snprintf(t, sizeof t, "%s/%s", dst, e->d_name) >= (int)sizeof t) {  // ruta demasiado larga
            rc = -1;                     // error
            break;                       // sale
        }
        if (copy_tree(s, t) != 0) { rc = -1; break; }  // copia recursiva; ante error se detiene
    }
    closedir(d);                         // cierra el directorio
    return rc;                           // 0 ok, -1 error
}

static void remove_tree(const char *path) {  // borra archivo o directorio (limpieza tras un publish fallido)
    struct stat st;                      // tipo
    if (lstat(path, &st) != 0) return;   // no existe
    if (S_ISDIR(st.st_mode)) {           // directorio: primero su contenido
        DIR *d = opendir(path);          // abre
        if (d != NULL) {                 // si se pudo
            struct dirent *e;            // entrada
            while ((e = readdir(d)) != NULL) {  // recorre
                if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) continue;  // omite
                char c[PATH_MAX];        // ruta hija
                if (snprintf(c, sizeof c, "%s/%s", path, e->d_name) < (int)sizeof c) remove_tree(c);  // recursivo
            }
            closedir(d);                 // cierra
        }
        rmdir(path);                     // borra el directorio ya vacío
    } else {
        unlink(path);                    // borra el archivo
    }
}

/* ---------- variables heredadas ---------- */

/*
 * Copia a env las out_vars de todos los ancestros (directos e indirectos) de 'job'.
 * REGLA DETERMINISTA para variables repetidas: gana el ancestro con MÁS ancestros propios
 * (el "más profundo" en el grafo, así un descendiente siempre pisa a su ancestro) y,
 * si empatan (trabajos paralelos), gana el que aparece DESPUÉS en el YAML.
 * Así el resultado no depende de qué hilo terminó primero.
 */
static void heredar_variables(const Run *run, const Job *job, VarList *env) {
    const Pipeline *p = run->p;          // el pipeline
    int ji = (int)(job - p->jobs);       // índice de este trabajo
    int n = p->n_jobs;                   // cantidad de trabajos
    int *cnt = calloc((size_t)n, sizeof *cnt);  // cnt[j] = cuántos ancestros tiene j
    if (cnt == NULL) return;             // sin memoria: no se hereda nada
    for (int j = 0; j < n; j++) {        // para cada trabajo...
        if (!p->ancestor[ji][j]) continue;  // ...que sea ancestro de este
        for (int k = 0; k < n; k++) {    // cuenta los ancestros de ese ancestro
            if (p->ancestor[j][k]) cnt[j]++;  // uno más
        }
    }
    for (int nivel = 0; nivel < n; nivel++) {  // de menos profundo a más profundo
        for (int j = 0; j < n; j++) {    // y dentro del nivel, en orden del YAML
            if (!p->ancestor[ji][j] || cnt[j] != nivel) continue;  // solo ancestros de este nivel
            const VarList *ov = &p->jobs[j].out_vars;  // variables que definió ese ancestro (ya terminó: nadie las escribe)
            for (size_t i = 0; i < ov->count; i++) {   // copia cada una
                var_set(env, ov->items[i].name, ov->items[i].value);  // la última en escribirse gana
            }
        }
    }
    free(cnt);                           // libera el arreglo auxiliar
}

/* ---------- ejecución de un paso ---------- */

/* Devuelve 0 si ok, > 0 si falló, o PROC_TIMEOUT / PROC_CANCELED. */
static int run_step(Run *run, Job *job, Step *step, const VarList *env, double deadline,
                    const char *log, const char *ws, VarList *newvars) {
    (void)job;                           // por ahora el paso no necesita datos del trabajo
    switch (step->kind) {                // cada tipo de paso se ejecuta distinto
    case STEP_SCRIPT: {                  // comando de shell
        char *cmd = substitute(step->text ? step->text : "", env);  // reemplaza $(VAR)
        if (cmd == NULL) return 1;       // sin memoria
        log_write(log, "$ %s\n", cmd);   // deja el comando en la bitácora (determinista)
        double rem = 0.0;                // tiempo restante del trabajo (0 = sin límite)
        if (deadline > 0.0) {            // el trabajo tiene timeout
            rem = deadline - now_s();    // lo que queda
            if (rem <= 0.0) { free(cmd); return PROC_TIMEOUT; }  // ya venció
        }
        char *argv[] = {"/bin/sh", "-c", cmd, NULL};  // el shell interpreta el comando
        int rc = run_process(argv, log, run, rem, ws, newvars);  // lo ejecuta como proceso hijo
        free(cmd);                       // libera el comando
        if (rc == PROC_ERROR) {          // no se pudo ni lanzar
            log_write(log, "error: no se pudo ejecutar el comando\n");  // lo deja registrado
            return 1;                    // cuenta como fallo normal
        }
        return rc;                       // código de salida o PROC_*
    }
    case STEP_TASK: {                    // tarea integrada
        VarList in;                      // inputs con variables sustituidas
        var_init(&in);                   // lista vacía
        for (size_t i = 0; i < step->inputs.count; i++) {  // copia cada input
            char *v = substitute(step->inputs.items[i].value, env);  // reemplaza $(VAR)
            if (v == NULL) { var_free(&in); return 1; }  // sin memoria
            var_set(&in, step->inputs.items[i].name, v);  // guarda el input ya sustituido
            free(v);                     // var_set hizo su propia copia
        }
        FILE *lf = fopen(log, "a");      // la tarea escribe en la bitácora por FILE*
        if (lf == NULL) { var_free(&in); return 1; }  // sin bitácora no se corre
        setvbuf(lf, NULL, _IOLBF, 0);    // buffer por línea: cada línea sale completa
        TaskCtx ctx = {                  // contexto que ve la tarea
            .inputs = &in,               // entradas
            .out = newvars,              // dónde dejar variables de salida
            .log = lf,                   // bitácora
            .workdir = ws,               // espacio de trabajo
            .cancel = &run->cancel,      // bandera de cancelación
            .deadline_s = deadline,      // límite del trabajo
            .max_threads = engine_task_threads(),  // tope de hilos OpenMP
        };
        int rc;                          // resultado de la tarea
        const char *t = step->text ? step->text : "";  // nombre de la tarea
        if (strcmp(t, "Sleep@1") == 0) rc = task_sleep(&ctx);           // Persona 3
        else if (strcmp(t, "Cpu@1") == 0) rc = task_cpu(&ctx);          // Persona 3
        else if (strcmp(t, "WordCount@1") == 0) rc = task_wordcount(&ctx);   // Persona 1
        else if (strcmp(t, "Histogram@1") == 0) rc = task_histogram(&ctx);   // Persona 1
        else {                           // el validador debería haberla rechazado
            fprintf(lf, "error: tarea desconocida '%s'\n", t);  // lo deja registrado
            rc = 1;                      // falla el paso
        }
        fclose(lf);                      // vacía y cierra la bitácora
        var_free(&in);                   // libera los inputs
        if (rc < 0) rc = 1;              // los negativos están reservados para PROC_*
        if (rc != 0) {                   // si falló, ¿fue por cancelación o timeout?
            if (atomic_load(&run->cancel)) return PROC_CANCELED;  // se pidió cancelar
            if (deadline > 0.0 && now_s() >= deadline) return PROC_TIMEOUT;  // venció el tiempo
        }
        return rc;                       // 0 ok, >0 fallo
    }
    case STEP_PUBLISH: {                 // publicar archivo o directorio como artefacto
        char *art = substitute(step->artifact ? step->artifact : "", env);  // nombre del artefacto
        char *path = substitute(step->path ? step->path : "", env);         // qué publicar
        int rc = 1;                      // por omisión: fallo
        char *adir = NULL, *src = NULL;  // destino y origen
        if (art == NULL || path == NULL) {  // sin memoria
            rc = 1;                      // falla
        } else if (!path_safe(path)) {   // intenta salirse del espacio de trabajo
            log_write(log, "error: ruta no permitida '%s'\n", path);  // registra
        } else if ((adir = run_artifact_path(run, art)) == NULL) {  // nombre de artefacto inválido
            log_write(log, "error: nombre de artefacto invalido '%s'\n", art);  // registra
        } else if (asprintf(&src, "%s/%s", ws, path) < 0) {  // arma el origen
            src = NULL;                  // asprintf deja basura si falla
        } else {
            struct stat st;              // tipo del origen
            if (lstat(src, &st) != 0 || (!S_ISREG(st.st_mode) && !S_ISDIR(st.st_mode))) {  // no existe o es raro
                log_write(log, "error: no existe '%s' para publicar\n", path);  // registra
            } else if (mkdir(adir, 0755) != 0) {  // mkdir es atómico: solo UN trabajo puede crear el artefacto
                log_write(log, "error: el artefacto '%s' ya existe o no se pudo crear\n", art);  // registra
            } else {
                int cr;                  // resultado de la copia
                if (S_ISDIR(st.st_mode)) {  // directorio: se copia su contenido
                    cr = copy_tree(src, adir);  // recursivo
                } else {                 // archivo: se copia dentro de la carpeta del artefacto
                    const char *base = strrchr(path, '/');  // último componente
                    base = base ? base + 1 : path;          // nombre del archivo
                    char *dst = NULL;    // destino final
                    cr = (asprintf(&dst, "%s/%s", adir, base) < 0) ? -1 : copy_file(src, dst, st.st_mode);  // copia
                    free(dst);           // libera
                }
                if (cr == 0) {           // todo bien
                    log_write(log, "artefacto '%s' publicado\n", art);  // registra
                    rc = 0;              // éxito
                } else {                 // falló la copia
                    log_write(log, "error: no se pudo copiar el artefacto '%s'\n", art);  // registra
                    remove_tree(adir);   // no deja artefactos a medias
                }
            }
        }
        free(adir); free(src); free(art); free(path);  // libera todo (free(NULL) es válido)
        return rc;                       // 0 ok, 1 fallo
    }
    case STEP_DOWNLOAD: {                // copiar un artefacto al espacio de trabajo
        char *art = substitute(step->artifact ? step->artifact : "", env);  // nombre del artefacto
        char *path = substitute(step->path ? step->path : "", env);         // directorio destino
        int rc = 1;                      // por omisión: fallo
        char *adir = NULL, *dst = NULL;  // origen y destino
        if (art == NULL || path == NULL) {  // sin memoria
            rc = 1;                      // falla
        } else if (!path_safe(path)) {   // intenta salirse del espacio de trabajo
            log_write(log, "error: ruta no permitida '%s'\n", path);  // registra
        } else if ((adir = run_artifact_path(run, art)) == NULL) {  // nombre inválido
            log_write(log, "error: nombre de artefacto invalido '%s'\n", art);  // registra
        } else {
            struct stat st;              // tipo del artefacto
            if (stat(adir, &st) != 0 || !S_ISDIR(st.st_mode)) {  // no existe (nadie lo publicó)
                log_write(log, "error: el artefacto '%s' no existe\n", art);  // falla el paso, sin esperar
            } else if (asprintf(&dst, "%s/%s", ws, path) < 0) {  // arma el destino
                dst = NULL;              // asprintf deja basura si falla
            } else if (fs_mkdir_p(dst) != 0 || copy_tree(adir, dst) != 0) {  // crea destino y copia
                log_write(log, "error: no se pudo descargar '%s'\n", art);  // registra
            } else {
                log_write(log, "artefacto '%s' descargado en '%s'\n", art, path);  // registra
                rc = 0;                  // éxito
            }
        }
        free(adir); free(dst); free(art); free(path);  // libera todo
        return rc;                       // 0 ok, 1 fallo
    }
    }
    return 1;                            // tipo desconocido
}

/* ---------- ejecución del trabajo ---------- */

static State ejecutar_pasos(Run *run, Job *job, VarList *env, const char *ws, const char *log) {
    double deadline = (job->timeout_s > 0) ? now_s() + (double)job->timeout_s : 0.0;  // límite absoluto del trabajo
    bool partial = false;                // true si algún paso con continueOnError falló
    for (int i = 0; i < job->n_steps; i++) {  // los pasos corren en orden, uno tras otro
        Step *s = &job->steps[i];        // paso actual
        if (atomic_load(&run->cancel)) { // cancelación pedida entre pasos
            mark_remaining(job, i, STATE_CANCELED);  // los pendientes no se ejecutan
            return STATE_CANCELED;       // el trabajo queda cancelado
        }
        if (deadline > 0.0 && now_s() >= deadline) {  // el tiempo se agotó entre pasos
            log_write(log, "error: tiempo limite del trabajo excedido\n");  // registra
            mark_remaining(job, i, STATE_SKIPPED);  // los pasos restantes no corren
            return STATE_FAILED;         // al vencer, el trabajo falla
        }
        log_write(log, "[paso %d] %s\n", i + 1, step_kind_name(s->kind));  // encabezado del paso
        step_set_state(s, STATE_RUNNING);  // visible para status
        double t0 = now_s();             // inicio del paso
        VarList newvars;                 // variables definidas por este paso
        var_init(&newvars);              // lista vacía
        int rc = run_step(run, job, s, env, deadline, log, ws, &newvars);  // ejecuta
        double dt = now_s() - t0;        // duración
        for (size_t k = 0; k < newvars.count; k++) {  // propaga las variables nuevas
            var_set(env, newvars.items[k].name, newvars.items[k].value);          // visibles para los pasos siguientes
            var_set(&job->out_vars, newvars.items[k].name, newvars.items[k].value);  // visibles para los trabajos que dependen de este
        }
        var_free(&newvars);              // libera la lista temporal
        int code = (rc >= 0) ? rc : -1;  // código que se muestra en status
        if (rc == 0) {                   // éxito
            step_update(s, STATE_SUCCEEDED, 0, dt);  // asienta
            continue;                    // siguiente paso
        }
        if (rc == PROC_CANCELED) {       // cancelado mientras corría
            step_update(s, STATE_CANCELED, code, dt);  // el paso queda cancelado
            mark_remaining(job, i + 1, STATE_CANCELED);  // y los pendientes también
            return STATE_CANCELED;       // el trabajo queda cancelado
        }
        if (rc == PROC_TIMEOUT) {        // venció el tiempo del trabajo
            log_write(log, "error: tiempo limite del trabajo excedido\n");  // registra
            step_update(s, STATE_FAILED, code, dt);  // el paso falló
            mark_remaining(job, i + 1, STATE_SKIPPED);  // los demás no corren
            return STATE_FAILED;         // el trabajo falla
        }
        step_update(s, STATE_FAILED, code, dt);  // fallo normal del paso
        if (s->continue_on_error) {      // se tolera el fallo
            partial = true;              // el trabajo terminará como partiallySucceeded
            continue;                    // sigue con el siguiente paso
        }
        mark_remaining(job, i + 1, STATE_SKIPPED);  // los pasos restantes no corren
        return STATE_FAILED;             // el trabajo falla
    }
    return partial ? STATE_PARTIALLY_SUCCEEDED : STATE_SUCCEEDED;  // todos los pasos terminaron
}

State run_job(Run *run, Job *job) {
    int ji = (int)(job - run->p->jobs);  // índice del trabajo dentro del pipeline
    char *ws = run_job_workspace(run, ji);  // su espacio de trabajo propio
    char *log = run_job_log_path(run, ji);  // su bitácora propia
    State result = STATE_FAILED;         // pesimista hasta demostrar lo contrario
    if (ws == NULL || log == NULL || fs_mkdir_p(ws) != 0) {  // no se pudo preparar
        if (log != NULL) log_write(log, "error: no se pudo crear el espacio de trabajo\n");  // registra
        mark_remaining(job, 0, STATE_SKIPPED);  // ningún paso corre
    } else {
        VarList env;                     // variables visibles para este trabajo
        var_init(&env);                  // lista vacía
        for (size_t i = 0; i < run->p->vars.count; i++) {  // primero las globales del pipeline
            var_set(&env, run->p->vars.items[i].name, run->p->vars.items[i].value);  // copia
        }
        heredar_variables(run, job, &env);  // luego las de los ancestros (pisan a las globales)
        result = ejecutar_pasos(run, job, &env, ws, log);  // corre los pasos
        var_free(&env);                  // libera las variables locales
    }
    free(ws);                            // libera la ruta del espacio de trabajo
    free(log);                           // libera la ruta de la bitácora
    return result;                       // el Engine asienta el estado final
}
