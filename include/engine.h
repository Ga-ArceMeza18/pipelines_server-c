#ifndef ENGINE_H
#define ENGINE_H

#include <pthread.h>
#include <stdbool.h>
#include "pipeline.h"

// comm: compartir el candado global con el Runner para proteger los estados
extern pthread_mutex_t g_lock;

// ----- ciclo de vida -----

// comm: fijar el tope de hilos OpenMP por tarea (0 = automático)
void engine_set_task_threads(int n);

// comm: crear los agentes y arrancar el motor
int engine_start(int agentes, const char *workdir);

// comm: apagar el motor de forma ordenada
void engine_shutdown(void);

// ----- operaciones de los comandos -----

// comm: encolar una ejecución nueva y devolver su id (toma posesión del pipeline)
int engine_submit(Pipeline *p);

// comm: cancelar una ejecución mediante su id
bool engine_cancel(int id);

// comm: generar el texto de estado de una ejecución
char *engine_status(int id);

// comm: generar el listado de todas las ejecuciones
char *engine_list(void);

// ----- consultas para LogReader -----

// comm: consultar el estado de una ejecución
int engine_run_state(int id, State *out);

// comm: consultar el estado de un trabajo dentro de una ejecución
int engine_job_state(int id, const char *job, State *out);

// comm: obtener las rutas de las bitácoras de un trabajo
// job NULL = todos en orden del YAML; devuelve la cantidad o -1
int engine_log_paths(int id, const char *job, char ***paths);

// comm: liberar el resultado de engine_log_paths
void engine_free_paths(char **paths, int n);

// ----- datos del motor -----

// comm: consultar el tope de hilos que usa cada tarea integrada
int engine_task_threads(void);

// comm: consultar el máximo de trabajos simultáneos observado
int engine_max_running(void);

// comm: consultar los trabajos terminados desde el inicio del motor
long engine_jobs_finished(void);

#endif