#define _GNU_SOURCE                      // realpath, strdup
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
 * Protección de la contabilidad de fin de trabajo (pending--, remaining--, estados, contadores).
 * Con -DSIN_PROTECCION se QUITA el candado de ese bloque: varios agentes que terminan a la vez
 * modifican los mismos contadores sin exclusión mutua (condición de carrera deliberada).
 */
#ifdef SIN_PROTECCION
#define CONTABILIDAD_LOCK()   ((void)0)  // no hace nada: carrera a propósito
#define CONTABILIDAD_UNLOCK() ((void)0)  // no hace nada
#else
#define CONTABILIDAD_LOCK()   pthread_mutex_lock(&g_lock)    // exclusión mutua normal
#define CONTABILIDAD_UNLOCK() pthread_mutex_unlock(&g_lock)  // libera
#endif

/* ---------- estado global del motor (todo bajo g_lock) ---------- */

pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;   // candado global (extern en engine.h)
static pthread_cond_t g_cond = PTHREAD_COND_INITIALIZER;  // los agentes duermen aquí si no hay trabajo listo
static Run **runs = NULL;                // todas las ejecuciones conocidas (arreglo dinámico)
static size_t quantity = 0;              // cuántas hay
static size_t capacity = 0;              // espacio reservado
static int n_agents = 0;                 // agentes pedidos (hilos OpenMP)
static int task_threads_cfg = 0;         // lo que pidió el usuario (0 = automático)
static int task_threads_eff = 1;         // valor efectivo, fijo después de engine_start
static int next_id = 1;                  // siguiente id de ejecución
static size_t rr_cursor = 0;             // desde qué ejecución se busca el próximo trabajo (turnos)
static bool started = false;             // el motor está en marcha
static bool stopping = false;            // se está apagando: no se aceptan envíos
static pthread_t engine_thread;          // hilo que contiene la región paralela de OpenMP
static char *base_dir = NULL;            // carpeta de trabajo absoluta
static int running_now = 0;              // trabajos corriendo ahora
static int max_running = 0;              // pico observado
static long finished_total = 0;          // trabajos terminados (contador de la demo de la carrera)

/* ---------- constructor de texto ---------- */

typedef struct { char *s; size_t len; size_t cap; } Buf;  // cadena que crece sola

static void buf_printf(Buf *b, const char *fmt, ...) {    // como sprintf pero agregando al final
    va_list ap, ap2;                     // dos copias: una para medir, otra para escribir
    va_start(ap, fmt);                   // inicia la primera
    va_copy(ap2, ap);                    // duplica la lista de argumentos
    int need = vsnprintf(NULL, 0, fmt, ap);  // mide cuánto ocupa
    va_end(ap);                          // cierra la primera
    if (need < 0) { va_end(ap2); return; }   // error de formato
    if (b->len + (size_t)need + 1 > b->cap) {  // no cabe: agranda
        size_t ncap = b->cap ? b->cap * 2 : 256;  // duplica (o arranca en 256)
        while (ncap < b->len + (size_t)need + 1) ncap *= 2;  // hasta que quepa
        char *t = realloc(b->s, ncap);   // reasigna
        if (t == NULL) { va_end(ap2); return; }  // sin memoria: se pierde este fragmento
        b->s = t;                        // nuevo bloque
        b->cap = ncap;                   // nueva capacidad
    }
    vsnprintf(b->s + b->len, b->cap - b->len, fmt, ap2);  // escribe de verdad
    va_end(ap2);                         // cierra la segunda
    b->len += (size_t)need;              // avanza el largo
}

/* ---------- lógica de estados (SIEMPRE con el candado tomado) ---------- */

static Run *find_run(int id) {           // busca una ejecución por id
    for (size_t i = 0; i < quantity; i++) {  // recorre
        if (runs[i]->id == id) return runs[i];  // encontrada
    }
    return NULL;                         // no existe
}

static void steps_set_queued(Job *j, State st) {  // pasos que siguen en queued -> st
    for (int i = 0; i < j->n_steps; i++) {  // recorre
        if (j->steps[i].state == STATE_QUEUED) j->steps[i].state = st;  // solo los que no corrieron
    }
}

static void run_finalize(Run *r) {       // todos los trabajos terminaron: decide el estado global
    if (state_is_terminal(r->state)) return;  // ya finalizada (llamada repetida por recursión)
    bool failed = false, partial = false;  // qué se vio
    for (int i = 0; i < r->p->n_jobs; i++) {  // revisa cada trabajo
        State s = r->p->jobs[i].state;   // su estado
        if (s == STATE_FAILED) failed = true;  // alguno falló
        if (s == STATE_PARTIALLY_SUCCEEDED) partial = true;  // alguno terminó con avisos
    }
    if (atomic_load(&r->cancel)) r->state = STATE_CANCELED;  // el usuario canceló
    else if (failed) r->state = STATE_FAILED;                // algún trabajo falló
    else if (partial) r->state = STATE_PARTIALLY_SUCCEEDED;  // todo bien salvo avisos
    else r->state = STATE_SUCCEEDED;     // todo bien
    r->end_s = now_s();                  // instante de fin
    if (r->start_s == 0.0) r->start_s = r->end_s;  // si nunca corrió nada, duración 0
}

static void job_terminated(Run *r, int ji);  // declaración adelantada (recursión mutua)

/* Todas las dependencias de ji terminaron: ¿corre o se salta según su condition? */
static void job_deps_done(Run *r, int ji) {
    Job *j = &r->p->jobs[ji];            // el trabajo
    bool all_ok = true, any_failed = false;  // resumen de sus dependencias
    for (int d = 0; d < j->n_deps; d++) {  // revisa cada dependencia
        switch (r->p->jobs[j->deps[d]].state) {  // su estado final
            case STATE_SUCCEEDED:
            case STATE_PARTIALLY_SUCCEEDED: break;  // cuenta como éxito
            case STATE_FAILED: any_failed = true; all_ok = false; break;  // falló
            default: all_ok = false; break;  // skipped o canceled: no es éxito
        }
    }
    bool corre = true;                   // ¿se ejecuta?
    switch (j->cond) {                   // según la condición del trabajo
        case COND_SUCCEEDED: corre = all_ok; break;      // todas con éxito
        case COND_FAILED:    corre = any_failed; break;  // alguna falló
        case COND_ALWAYS:    corre = true; break;        // siempre
    }
    if (!corre) {                        // no corre: queda skipped
        j->state = STATE_SKIPPED;        // estado final
        steps_set_queued(j, STATE_SKIPPED);  // sus pasos también
        job_terminated(r, ji);           // y se propaga a sus dependientes
    }
    /* si corre se queda en QUEUED con pending == 0: ya es elegible para un agente */
}

/* El trabajo ji llegó a un estado final: libera a los que dependen de él y revisa si la Run terminó. */
static void job_terminated(Run *r, int ji) {
    r->remaining--;                      // un trabajo menos por terminar
    for (int k = 0; k < r->p->n_jobs; k++) {  // busca los dependientes
        Job *o = &r->p->jobs[k];         // candidato
        if (o->state != STATE_QUEUED) continue;  // solo los que aún esperan
        int hits = 0;                    // cuántas veces depende de ji
        for (int d = 0; d < o->n_deps; d++) {  // revisa sus dependencias
            if (o->deps[d] == ji) hits++;  // coincide
        }
        if (hits == 0) continue;         // no depende de ji
        o->pending -= hits;              // una dependencia menos
        if (o->pending <= 0) job_deps_done(r, k);  // ya puede decidirse
    }
    if (r->remaining == 0) run_finalize(r);  // ya no queda nada: cierra la ejecución
}

static bool cancel_locked(Run *r) {      // cancela una ejecución (candado tomado)
    if (state_is_terminal(r->state)) return false;  // ya terminó: nada que cancelar
    atomic_store(&r->cancel, true);      // los pasos en curso lo ven y se detienen
    for (int i = 0; i < r->p->n_jobs; i++) {  // los pendientes no se ejecutan
        Job *j = &r->p->jobs[i];         // el trabajo
        if (j->state != STATE_QUEUED) continue;  // los que corren terminan por su cuenta
        j->state = STATE_CANCELED;       // cancelado
        steps_set_queued(j, STATE_CANCELED);  // sus pasos también
        r->remaining--;                  // ya no hay que esperarlo
    }
    if (r->remaining == 0) run_finalize(r);  // si no había nada corriendo, termina ya
    return true;                         // se canceló
}

/* ---------- agentes ---------- */

/* Elige el próximo trabajo listo; reparte por turnos entre ejecuciones para que ninguna se muera de hambre. */
static bool pick_next_job(Run **out_run, int *out_ji) {
    for (size_t i = 0; i < quantity; i++) {  // prueba cada ejecución una vez
        size_t idx = (rr_cursor + i) % quantity;  // empieza donde quedó la vez anterior
        Run *r = runs[idx];              // la ejecución
        if (state_is_terminal(r->state)) continue;  // ya terminó
        for (int ji = 0; ji < r->p->n_jobs; ji++) {  // trabajos en orden del YAML (determinista)
            Job *j = &r->p->jobs[ji];    // el trabajo
            if (j->state == STATE_QUEUED && j->pending == 0) {  // listo para correr
                rr_cursor = (idx + 1) % quantity;  // la próxima búsqueda arranca en la ejecución siguiente
                *out_run = r;            // devuelve la ejecución
                *out_ji = ji;            // y el trabajo
                return true;             // hay trabajo
            }
        }
    }
    return false;                        // nada listo
}

/* Asienta el resultado de un trabajo y libera a sus dependientes. */
static void finish_job(Run *r, int ji, State st, double dt) {
    CONTABILIDAD_LOCK();                 // con -DSIN_PROTECCION esto no hace nada
    Job *j = &r->p->jobs[ji];            // el trabajo
    j->state = st;                       // estado final
    j->duration_s = dt;                  // duración
    finished_total++;                    // contador global
    job_terminated(r, ji);               // pending--, remaining--, posible cierre de la Run
    CONTABILIDAD_UNLOCK();               // libera
    pthread_cond_broadcast(&g_cond);     // despierta a los agentes: pudieron quedar trabajos listos
}

/* Cuerpo de cada agente: toma trabajos listos hasta que el motor se apague. */
static void agent_loop(void) {
    pthread_mutex_lock(&g_lock);         // todo el bucle corre con el candado, salvo la ejecución del trabajo
    for (;;) {                           // hasta que se apague
        Run *r = NULL;                   // ejecución elegida
        int ji = 0;                      // trabajo elegido
        if (pick_next_job(&r, &ji)) {    // ¿hay algo listo?
            Job *j = &r->p->jobs[ji];    // el trabajo
            j->state = STATE_RUNNING;    // este agente lo ocupa
            if (r->state == STATE_QUEUED) {  // primer trabajo de la ejecución
                r->state = STATE_RUNNING;    // la ejecución pasa a running
                r->start_s = now_s();        // y arranca su cronómetro
            }
            running_now++;               // un agente más ocupado
            if (running_now > max_running) max_running = running_now;  // actualiza el pico
            pthread_mutex_unlock(&g_lock);   // se suelta el candado mientras trabaja (si no, no habría paralelismo)
            double t0 = now_s();         // inicio
            State st = run_job(r, j);    // ejecuta el trabajo completo
            double dt = now_s() - t0;    // duración
            finish_job(r, ji, st, dt);   // asienta el resultado
            pthread_mutex_lock(&g_lock); // vuelve a tomar el candado
            running_now--;               // este agente queda libre
            continue;                    // busca más trabajo
        }
        if (stopping) break;             // apagado y nada listo: este agente termina
        pthread_cond_wait(&g_cond, &g_lock);  // duerme (suelta el candado) hasta que haya novedades
    }
    pthread_mutex_unlock(&g_lock);       // libera antes de salir
}

static void *engine_main(void *arg) {    // hilo POSIX que aloja la región paralela de OpenMP
    (void)arg;                           // no se usa
    omp_set_dynamic(0);                  // exige exactamente n_agents hilos (no menos)
    omp_set_max_active_levels(2);        // permite un nivel anidado: las tareas de datos abren su propio equipo
    #pragma omp parallel num_threads(n_agents)  // un hilo OpenMP por agente
    {
        agent_loop();                    // cada agente ejecuta el mismo bucle
    }
    return NULL;                         // al salir, todos los agentes terminaron
}

/* ---------- API pública ---------- */

void engine_set_task_threads(int n) {    // fija el tope de hilos por tarea
    task_threads_cfg = n;                // se usa en engine_start
}

int engine_task_threads(void) {          // lo consulta el Runner
    return task_threads_eff;             // inmutable después de engine_start: no requiere candado
}

int engine_start(int agentes, const char *workdir) {
    if (agentes < 1 || workdir == NULL) return -1;  // argumentos inválidos
    if (fs_mkdir_p(workdir) != 0) return -1;        // crea la carpeta de trabajo
    char *abs = realpath(workdir, NULL);            // ruta absoluta (los hijos cambian de directorio)
    if (abs == NULL) return -1;          // no se pudo resolver
    pthread_mutex_lock(&g_lock);         // protege el estado global
    if (started) {                       // ya había un motor corriendo
        pthread_mutex_unlock(&g_lock);   // libera
        free(abs);                       // descarta la ruta
        return -1;                       // no se puede arrancar dos veces
    }
    base_dir = abs;                      // guarda la carpeta
    n_agents = agentes;                  // guarda el número de agentes
    if (task_threads_cfg > 0) task_threads_eff = task_threads_cfg;  // el usuario lo fijó
    else {                               // automático: reparte los núcleos entre los agentes
        int t = omp_get_num_procs() / agentes;  // núcleos por agente
        task_threads_eff = t < 1 ? 1 : t;       // al menos 1 -> sin sobresuscripción
    }
    stopping = false;                    // no se está apagando
    started = true;                      // el motor está activo
    running_now = 0;                     // reinicia contadores
    max_running = 0;                     // reinicia el pico
    finished_total = 0;                  // reinicia el contador
    if (pthread_create(&engine_thread, NULL, engine_main, NULL) != 0) {  // arranca los agentes
        started = false;                 // falló: se deshace
        free(base_dir);                  // libera la ruta
        base_dir = NULL;                 // evita puntero colgante
        pthread_mutex_unlock(&g_lock);   // libera
        return -1;                       // error
    }
    pthread_mutex_unlock(&g_lock);       // libera
    return 0;                            // listo
}

int engine_submit(Pipeline *p) {         // siempre toma posesión de p (lo libera si falla)
    if (p == NULL) return -1;            // nada que enviar
    pthread_mutex_lock(&g_lock);         // reserva el id
    if (!started || stopping) {          // no se aceptan envíos
        pthread_mutex_unlock(&g_lock);   // libera
        pipeline_free(p);                // el motor era dueño del pipeline
        return -1;                       // rechazado
    }
    int id = next_id++;                  // id único
    char *dir = strdup(base_dir);        // copia: crear carpetas puede tardar y no queremos tener el candado
    pthread_mutex_unlock(&g_lock);       // libera
    if (dir == NULL) { pipeline_free(p); return -1; }  // sin memoria
    Run *r = run_create(id, p, dir);     // crea carpetas y deja todo en queued (sin candado)
    free(dir);                           // ya no se necesita
    if (r == NULL) { pipeline_free(p); return -1; }  // no se pudo crear
    pthread_mutex_lock(&g_lock);         // ahora sí se publica
    if (stopping) {                      // se apagó mientras tanto
        pthread_mutex_unlock(&g_lock);   // libera
        run_destroy(r);                  // libera la Run y el pipeline
        return -1;                       // rechazado
    }
    if (quantity == capacity) {          // el arreglo está lleno
        size_t ncap = capacity ? capacity * 2 : 8;  // duplica
        Run **t = realloc(runs, ncap * sizeof *runs);  // reasigna
        if (t == NULL) {                 // sin memoria
            pthread_mutex_unlock(&g_lock);  // libera
            run_destroy(r);              // libera todo
            return -1;                   // rechazado
        }
        runs = t;                        // nuevo arreglo
        capacity = ncap;                 // nueva capacidad
    }
    runs[quantity++] = r;                // la ejecución ya es visible
    for (int ji = 0; ji < p->n_jobs; ji++) {  // los trabajos sin dependencias se deciden ya
        Job *j = &p->jobs[ji];           // el trabajo
        if (j->state == STATE_QUEUED && j->n_deps == 0) job_deps_done(r, ji);  // corre o queda skipped según su condition
    }
    if (r->remaining == 0) run_finalize(r);  // pipeline vacío (o todo saltado)
    pthread_mutex_unlock(&g_lock);       // libera
    pthread_cond_broadcast(&g_cond);     // despierta a los agentes
    return id;                           // el cliente recibe el id
}

bool engine_cancel(int id) {
    pthread_mutex_lock(&g_lock);         // protege el estado
    Run *r = find_run(id);               // busca
    bool ok = (r != NULL) && cancel_locked(r);  // cancela si existe y sigue activa
    pthread_mutex_unlock(&g_lock);       // libera
    if (ok) pthread_cond_broadcast(&g_cond);  // por si había agentes esperando
    return ok;                           // true si se canceló
}

void engine_shutdown(void) {
    pthread_mutex_lock(&g_lock);         // protege el estado
    if (!started || stopping) {          // no hay motor o ya hay un apagado en curso
        pthread_mutex_unlock(&g_lock);   // libera
        return;                          // idempotente
    }
    stopping = true;                     // no se aceptan más envíos y los agentes saldrán al quedar libres
    for (size_t i = 0; i < quantity; i++) cancel_locked(runs[i]);  // POLÍTICA: se cancelan las ejecuciones en curso
    pthread_mutex_unlock(&g_lock);       // libera
    pthread_cond_broadcast(&g_cond);     // despierta a los agentes dormidos
    pthread_join(engine_thread, NULL);   // espera a que todos los agentes terminen (y sus procesos hijos)
    pthread_mutex_lock(&g_lock);         // ya no hay agentes: limpieza segura
    for (size_t i = 0; i < quantity; i++) run_destroy(runs[i]);  // libera ejecuciones y pipelines
    free(runs);                          // libera el arreglo
    runs = NULL;                         // sin puntero colgante
    quantity = 0;                        // vacío
    capacity = 0;                        // sin reserva
    free(base_dir);                      // libera la carpeta
    base_dir = NULL;                     // sin puntero colgante
    rr_cursor = 0;                       // reinicia el turno
    started = false;                     // el motor ya no existe
    stopping = false;                    // se podría volver a arrancar
    pthread_mutex_unlock(&g_lock);       // libera
}

/* ---------- consultas ---------- */

static State stage_state(const Run *r, const char *stage) {  // estado derivado de una etapa
    int total = 0, q = 0, run = 0, part = 0, fail = 0, canc = 0, skip = 0;  // contadores
    for (int i = 0; i < r->p->n_jobs; i++) {  // recorre los trabajos
        const Job *j = &r->p->jobs[i];   // el trabajo
        if (strcmp(j->stage ? j->stage : "", stage) != 0) continue;  // de otra etapa
        total++;                         // uno más en la etapa
        switch (j->state) {              // cuenta por estado
            case STATE_QUEUED: q++; break;
            case STATE_RUNNING: run++; break;
            case STATE_PARTIALLY_SUCCEEDED: part++; break;
            case STATE_FAILED: fail++; break;
            case STATE_CANCELED: canc++; break;
            case STATE_SKIPPED: skip++; break;
            case STATE_SUCCEEDED: break;
        }
    }
    if (run > 0) return STATE_RUNNING;   // algo está corriendo
    if (q == total) return STATE_QUEUED; // nada ha empezado
    if (q > 0) return STATE_RUNNING;     // unos terminaron y otros esperan: la etapa sigue en curso
    if (fail > 0) return STATE_FAILED;   // alguno falló
    if (canc > 0) return STATE_CANCELED; // alguno se canceló
    if (part > 0) return STATE_PARTIALLY_SUCCEEDED;  // con avisos
    if (skip == total) return STATE_SKIPPED;         // todos saltados
    return STATE_SUCCEEDED;              // todo bien
}

static void fmt_dur(char *out, size_t n, State st, double d) {  // "1.23s" o "-" si aún no termina
    if (state_is_terminal(st)) snprintf(out, n, "%.2fs", d);    // ya tiene duración final
    else snprintf(out, n, "-");          // en curso o en espera
}

char *engine_status(int id) {
    pthread_mutex_lock(&g_lock);         // foto consistente: nadie modifica mientras se arma el texto
    Run *r = find_run(id);               // busca
    if (r == NULL) { pthread_mutex_unlock(&g_lock); return NULL; }  // no existe
    Buf b = {0};                         // texto de salida
    double dur = 0.0;                    // duración de la ejecución
    if (r->start_s != 0.0) dur = (state_is_terminal(r->state) ? r->end_s : now_s()) - r->start_s;  // en curso: hasta ahora
    buf_printf(&b, "Ejecucion %d: %s\n", r->id, r->p->name ? r->p->name : "(sin nombre)");  // encabezado
    buf_printf(&b, "Estado: %s  Duracion: %.2fs\n", state_name(r->state), dur);  // estado global
    int nj = r->p->n_jobs;               // cantidad de trabajos
    const char **stages = calloc((size_t)(nj > 0 ? nj : 1), sizeof *stages);  // etapas distintas, en orden de aparición
    int ns = 0;                          // cuántas etapas hay
    for (int i = 0; stages != NULL && i < nj; i++) {  // las recolecta
        const char *sn = r->p->jobs[i].stage ? r->p->jobs[i].stage : "";  // etapa del trabajo
        bool vista = false;              // ¿ya estaba?
        for (int s = 0; s < ns; s++) if (strcmp(stages[s], sn) == 0) vista = true;  // la busca
        if (!vista) stages[ns++] = sn;   // nueva etapa
    }
    for (int s = 0; s < ns; s++) {       // imprime etapa por etapa
        buf_printf(&b, "Etapa %s [%s]\n", stages[s], state_name(stage_state(r, stages[s])));  // etapa
        for (int i = 0; i < nj; i++) {   // sus trabajos
            const Job *j = &r->p->jobs[i];  // el trabajo
            if (strcmp(j->stage ? j->stage : "", stages[s]) != 0) continue;  // de otra etapa
            char d[32];                  // duración formateada
            fmt_dur(d, sizeof d, j->state, j->duration_s);  // la arma
            buf_printf(&b, "  Trabajo %s [%s] %s\n", j->name ? j->name : "?", state_name(j->state), d);  // trabajo
            for (int k = 0; k < j->n_steps; k++) {  // sus pasos
                const Step *st = &j->steps[k];  // el paso
                fmt_dur(d, sizeof d, st->state, st->duration_s);  // duración del paso
                buf_printf(&b, "    Paso %d (%s) [%s] %s exit=%d", k + 1, step_kind_name(st->kind), state_name(st->state), d, st->exit_code);  // paso
                const char *txt = st->kind == STEP_PUBLISH || st->kind == STEP_DOWNLOAD ? st->artifact : st->text;  // qué mostrar
                if (txt != NULL) {       // resumen de una línea
                    size_t len = strcspn(txt, "\n");  // hasta el primer salto de línea
                    if (len > 60) len = 60;           // máximo 60 caracteres
                    buf_printf(&b, " : %.*s", (int)len, txt);  // lo agrega
                }
                buf_printf(&b, "\n");    // fin de la línea del paso
            }
        }
    }
    free(stages);                        // libera el arreglo auxiliar
    pthread_mutex_unlock(&g_lock);       // libera
    return b.s ? b.s : strdup("");       // nunca devuelve NULL para una ejecución existente (salvo sin memoria)
}

char *engine_list(void) {
    pthread_mutex_lock(&g_lock);         // foto consistente
    Buf b = {0};                         // texto de salida
    if (quantity == 0) buf_printf(&b, "(sin ejecuciones)\n");  // lista vacía
    for (size_t i = 0; i < quantity; i++) {  // una línea por ejecución
        const Run *r = runs[i];          // la ejecución
        buf_printf(&b, "%d  %s  %s\n", r->id, state_name(r->state), r->p->name ? r->p->name : "(sin nombre)");  // id estado nombre
    }
    pthread_mutex_unlock(&g_lock);       // libera
    return b.s ? b.s : strdup("");       // el llamador libera
}

int engine_run_state(int id, State *out) {
    pthread_mutex_lock(&g_lock);         // protege
    Run *r = find_run(id);               // busca
    if (r != NULL) *out = r->state;      // copia el estado
    pthread_mutex_unlock(&g_lock);       // libera
    return r ? 0 : -1;                   // 0 existe, -1 no
}

int engine_job_state(int id, const char *job, State *out) {
    int rc = -1;                         // por omisión: no existe
    pthread_mutex_lock(&g_lock);         // protege
    Run *r = find_run(id);               // busca la ejecución
    for (int i = 0; r != NULL && job != NULL && i < r->p->n_jobs; i++) {  // busca el trabajo
        if (r->p->jobs[i].name != NULL && strcmp(r->p->jobs[i].name, job) == 0) {  // mismo nombre
            *out = r->p->jobs[i].state;  // copia el estado
            rc = 0;                      // encontrado
            break;                       // el primero que coincida
        }
    }
    pthread_mutex_unlock(&g_lock);       // libera
    return rc;                           // 0 ok, -1 no existe
}

void engine_free_paths(char **paths, int n) {  // libera el resultado de engine_log_paths
    if (paths == NULL) return;           // nada que liberar
    for (int i = 0; i < n; i++) free(paths[i]);  // cada ruta
    free(paths);                         // el arreglo
}

int engine_log_paths(int id, const char *job, char ***paths) {
    *paths = NULL;                       // salida vacía por omisión
    int count = -1;                      // resultado
    pthread_mutex_lock(&g_lock);         // protege
    Run *r = find_run(id);               // busca la ejecución
    if (r != NULL) {                     // si existe
        int first = 0, last = r->p->n_jobs;  // por omisión: todos los trabajos
        if (job != NULL) {               // un trabajo concreto
            first = -1;                  // todavía no encontrado
            for (int i = 0; i < r->p->n_jobs; i++) {  // lo busca
                if (r->p->jobs[i].name != NULL && strcmp(r->p->jobs[i].name, job) == 0) { first = i; break; }  // el primero
            }
            last = first + 1;            // solo ese
        }
        if (first >= 0) {                // se encontró (o son todos)
            int n = last - first;        // cuántas rutas
            char **arr = calloc((size_t)(n > 0 ? n : 1), sizeof *arr);  // arreglo de rutas
            bool ok = arr != NULL;       // ¿todo bien?
            for (int i = 0; ok && i < n; i++) {  // arma cada ruta
                arr[i] = run_job_log_path(r, first + i);  // ruta de la bitácora
                if (arr[i] == NULL) ok = false;  // sin memoria
            }
            if (ok) { *paths = arr; count = n; }  // éxito
            else engine_free_paths(arr, n);  // deshace
        }
    }
    pthread_mutex_unlock(&g_lock);       // libera
    return count;                        // cantidad de rutas o -1
}

int engine_max_running(void) {           // para pruebas
    pthread_mutex_lock(&g_lock);         // protege
    int v = max_running;                 // copia
    pthread_mutex_unlock(&g_lock);       // libera
    return v;                            // pico observado
}

long engine_jobs_finished(void) {        // para la prueba de -DSIN_PROTECCION
    pthread_mutex_lock(&g_lock);         // protege la lectura
    long v = finished_total;             // copia
    pthread_mutex_unlock(&g_lock);       // libera
    return v;                            // total
}
