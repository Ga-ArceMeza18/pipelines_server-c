#ifndef RUN_H
#define RUN_H

#include <stdatomic.h>
#include <stdbool.h>
#include "pipeline.h"

// datatype: run
typedef struct {
    int id;              // comm: identificar la ejecución
    Pipeline *p;         // comm: apuntar al pipeline que la ejecución posee
    State state;         // comm: guardar el estado global de la ejecución
    atomic_bool cancel;  // comm: indicar si se pidió cancelar
    char *dir;           // comm: guardar la carpeta de la ejecución
    int remaining;       // comm: contar los trabajos que faltan por terminar
    double start_s;      // comm: registrar el instante en que inició
    double end_s;        // comm: registrar el instante en que terminó
} Run;

// comm: crear las carpetas de la ejecución y dejar todos los trabajos en cola
Run *run_create(int id, Pipeline *p, const char *base_dir);

// comm: liberar la ejecución y su pipeline (sin borrar archivos del disco)
void run_destroy(Run *r);

// comm: armar la ruta de la bitácora del trabajo ji
char *run_job_log_path(const Run *r, int ji);

// comm: armar la ruta del espacio de trabajo del trabajo ji
char *run_job_workspace(const Run *r, int ji);

// comm: armar la ruta del artefacto name (NULL si el nombre es inseguro)
char *run_artifact_path(const Run *r, const char *name);

// comm: crear una ruta completa, como mkdir -p
int fs_mkdir_p(const char *path);

// comm: traducir un estado a texto legible
const char *state_name(State s);

// comm: traducir un tipo de paso a texto legible
const char *step_kind_name(StepKind k);

// comm: determinar si un estado es final (ya no cambiará)
bool state_is_terminal(State s);

// comm: obtener la hora monotónica actual en segundos
double now_s(void);

#endif