#ifndef ENGINE_H                        
#define ENGINE_H                         

#include <pthread.h>                    
#include <stdbool.h>                    
#include "pipeline.h"                    

extern pthread_mutex_t g_lock;

// ----- ciclo de vida ----- 

// comm: cantidad de hilos OpenMP
void engine_set_task_threads(int n);     

// comm:  crear los agentes
int  engine_start(int agentes, const char *workdir);  

// comm: apagar ordenado 
void engine_shutdown(void);             

// ----- operaciones de los comandos -----

// comm: crear un run y lo deja en cola
int   engine_submit(Pipeline *p);      

// comm: cancelar un proceso mediante id
bool  engine_cancel(int id);            

// comm: generar un texto de información obre una ejecución
char *engine_status(int id);

// comm: devolver una serie de acciones, ordenadas en lista
char *engine_list(void);            

// ----- consultas para LogReader (Persona 3) ----- 

// comm: status de una corrida
int  engine_run_state(int id, State *out);

// comm: status de un trabajo
int  engine_job_state(int id, const char *job, State *out);

// comm: buscar las rutas de los archivos .log de un trabajo
int  engine_log_paths(int id, const char *job, char ***paths);  // job NULL = todos en orden del YAML; devuelve cantidad o -1

// comm liberar lo que devolvió engine_log_paths
void engine_free_paths(char **paths, int n);                

// ----- datos del motor ----- 

// comm: calcular tope de hilos que puede usar cada tarea integrada
int  engine_task_threads(void);          

// comm: máximo de trabajos simultáneos
int engine_max_running(void);           

//comm: trabajos que han sido realizados desde el inicio del programa
long engine_jobs_finished(void);         

#endif