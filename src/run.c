#define _GNU_SOURCE                     
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

/* ---------- utilidades pequeñas ---------- */

double now_s(void) {                     
    struct timespec ts;                  // estructura con segundos y nanosegundos
    clock_gettime(CLOCK_MONOTONIC, &ts); // lee el reloj monotónico
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;  // lo convierte a segundos con decimales
}

const char *state_name(State s) {        // comm: nombre legible de un estado
    switch (s) {                         // un caso por cada valor del enum
        if (state == STATE_QUEUED) {
            return "queued";
        }
        else if (state == STATE_RUNNING) {
            return "running";
        }
        else if (state == STATE_SUCCEEDED) {
            return "succeeded";
        }
        else if (state == STATE_PARTIALLY_SUCCEEDED) {
            return "partiallySucceeded";
        }   
        else if (state == STATE_FAILED) {
            return "failed";
        }      
        else if (state == STATE_CANCELED) {
            return "canceled";
        }
        else if (state == STATE_SKIPPED) {
            return "skipped";
        }
        else {
            return "unknown";
        }
    }
}

const char *step_kind_name(StepKind k) { // comm: nombre legible del tipo de paso
    if (k == STEP_SCRIPT) {
        return "script";
    }
    else if (k == STEP_TASK) {
        return "task";
    }
    else if (k == STEP_PUBLISH) {
        return "publish";
    }
    else if (k == STEP_DOWNLOAD) {
        return "download";
    }
    else {
        return "unknown";
    }
}

bool state_is_terminal(State s) { // comm: saber si el run terminó
    if (s != STATE_QUEUED && s != STATE_RUNNING) {
        return true;
    }
    else {
        return false;
    }
}

int fs_mkdir_p(const char *path) {       // comm: crear una ruta completa, como mkdir -p
    if (path == NULL || path[0] == '\0') {
        return -1;
    }  // comm: ruta inválida
    
    
    char *tmp = strdup(path);            // comm: copia (que puede modificarse) de la ruta
    if (tmp == NULL) {
        return -1;
    }   // comm: sin memoria

    // comm: determinar largo de la ruta
    size_t len = strlen(tmp);            

    // comm: recorre buscando cada separador
    for (size_t i = 1; i < len; i++) {   
        if (tmp[i] != '/') { 
            continue;
        }
        
        // comm: cortar la cadena justo antes del separador
        tmp[i] = '\0';     
        
        // comm: crear el prefijo
        if (mkdir(tmp, 0755) != 0 && errno != EEXIST) {  
            free(tmp);                  
            return -1;                 
        }

        tmp[i] = '/';                   
    }

    // comm: crear el último componente
    int rc = mkdir(tmp, 0755);           
    int saved = errno;                   
    free(tmp);        
    // comm: se creó o ya existía                  
    if (rc == 0 || saved == EEXIST) {
        return 0;
    }
    else {
        return -1;
    }  
}

/* Convertir un nombre del YAML en algo seguro para usar como nombre de archivo. */
static char *sanear(const char *s) {     

    if (s == NULL) {
        s = "";
    }
    char *out = strdup(s);               // comm: copia modificable

    if (out == NULL) {
        return NULL;
    } // comm: sin memoria

    // comm: recorrer cada carácter
    for (char *c = out; *c; c++) {     
        
        // comm: evitar problemas de signo con isalnum
        unsigned char u = (unsigned char)*c;  
        if (!isalnum(u) && *c != '_' && *c != '-' && *c != '.') *c = '_';  
    }
    return out;    
}

/* "<indice>_<etapa>__<trabajo>": único por trabajo y sin rutas peligrosas. */

// comm: base del nombre de carpeta y bitácora
static char *nombre_trabajo(const Run *r, int ji) { 

    const Job *j = &r->p->jobs[ji];      // val: el trabajo
    char *st = sanear(j->stage);         // val: etapa saneada
    char *jb = sanear(j->name);          // val: trabajo saneado
    char *out = NULL;                    // val: resultado

    // comm: si ambas copias salen bien
    if (st != NULL && jb != NULL) {      
        // comm: si asprintf falla, out es NULL
        if (asprintf(&out, "%03d_%s__%s", ji, st, jb) < 0) out = NULL; 
    }

    // comm: liberar trabajos
    free(st);                           
    free(jb);                            
    return out;                          
}

char *run_job_log_path(const Run *r, int ji) {  // path: <dir>/logs/<nombre>.log

    char *n = nombre_trabajo(r, ji);     // val: nombre base

    if (n == NULL) {
        return NULL;
    }  // comm: sin memoria

    char *out = NULL;                    // val: resultado

    // comm: arma la ruta y retorna NULL si sucedió algún error
    if (asprintf(&out, "%s/logs/%s.log", r->dir, n) < 0) out = NULL;  

    // comm: liberar
    free(n);                             
    return out;                          
}

char *run_job_workspace(const Run *r, int ji) {  // path: <dir>/work/<nombre>

    char *n = nombre_trabajo(r, ji);     // val: nombre base
    if (n == NULL) {
        return NULL;
    } // comm: sin memoria

    char *out = NULL;                    // val: resultado

    // comm: arma la ruta y retorna NULL si sucedió algún error
    if (asprintf(&out, "%s/work/%s", r->dir, n) < 0) out = NULL;  

    // comm: liberar
    free(n);                             
    return out;                          
}

char *run_artifact_path(const Run *r, const char *name) {  // path: <dir>/artifacts/<name>

    if (name == NULL || name[0] == '\0') { 
        return NULL;  
    } // comm: nombre vacío no sirve
    
    if (strchr(name, '/') != NULL) { 
        return NULL; 
    }  // comm: una '/' permitiría escapar de la carpeta

    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) { 
        return NULL;
    }  // nombres especiales
    
    char *out = NULL;                    // val: resultado

    if (asprintf(&out, "%s/artifacts/%s", r->dir, name) < 0) out = NULL;  // arma la ruta
    
    // comm: el llamador libera
    return out;                          
}

/* ---------- ciclo de vida de la Run ---------- */

Run *run_create(int id, Pipeline *p, const char *base_dir) {  
    if (p == NULL || base_dir == NULL){
        return NULL;  // comm: argumentos inválidos
    } 
    Run *r = calloc(1, sizeof *r);       // comm: Run en ceros

    if (r == NULL) { 
        return NULL;
    } // comm: sin memoria

    r->id = id;                          // val: identificador
    r->p = p;                            // val: pipeline

    // comm: toda ejecución nace en queued
    r->state = STATE_QUEUED;     
    
    // comm: nadie ha pedido cancelar
    atomic_init(&r->cancel, false); 

    // comm: defect value
    r->remaining = p->n_jobs;            

    if (asprintf(&r->dir, "%s/run-%d", base_dir, id) < 0) { 
        free(r);                         // comm: libera la Run
        return NULL;                     // comm: falló
    }

    char *sub = NULL;                    // val: subcarpeta 

    const char *nombres[] = {"logs", "work", "artifacts"};   // val: subcarpetas por ejecución

    for (int i = 0; i < 3; i++) {        // comm: crear cada carpeta
        if (asprintf(&sub, "%s/%s", r->dir, nombres[i]) < 0 || fs_mkdir_p(sub) != 0) {  
            
            // comm: liberar vals
            free(sub);                   
            free(r->dir);                
            free(r);                   
            return NULL;                 
        }

        free(sub);                      
        sub = NULL;                      
    }

    for (int ji = 0; ji < p->n_jobs; ji++) {  // comm: dejar cada trabajo en su estado inicial
        Job *j = &p->jobs[ji];           // val: el trabajo
        j->state = STATE_QUEUED;         // val: espera
        j->pending = j->n_deps;          // val: cuántas dependencias faltan
        j->duration_s = 0.0;             // val: aún sin duración

        var_init(&j->out_vars);          // lista de variables de salida vacía

        for (int si = 0; si < j->n_steps; si++) { 

            j->steps[si].state = STATE_QUEUED;     // val: espera
            j->steps[si].exit_code = 0;            // val: sin código de salida aún
            j->steps[si].duration_s = 0.0;         // val: sin duración aún

        }

        char *log = run_job_log_path(r, ji);       // bitácora del trabajo

        if (log != NULL) {               // comm: si se pudo armar la ruta

            // comm: se crea la bitácora vacía
            int fd = open(log, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);  // comm: para que logs --follow la encuentre
            
            // comm: no la necesitamos abierta
            if (fd >= 0) close(fd);      
            
            free(log);                   
        }
    }
    return r;                           
}

void run_destroy(Run *r) {               // comm: liberar todo lo que la Run posee en memoria
    if (r == NULL) { return; }               
    pipeline_free(r->p);                 
    free(r->dir);                        
    free(r);                             
}
