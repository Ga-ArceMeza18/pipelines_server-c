#ifndef RUN_H                       
#define RUN_H                            

#include <stdatomic.h>                   
#include <stdbool.h>                     
#include "pipeline.h"                   

// Una ejecución (run) de un pipeline. 
typedef struct Run {

    // se definen variables importantes
    int id;                             
    Pipeline *p;                         
    State state;                         
    atomic_bool cancel;                  
    char *dir;                           // carpeta de la ejecución
    int remaining;                       // trabajos pendientes
    double start_s;                      // momento en el que inició la corrida
    double end_s;                        // instante en que terminó
} Run;

// comm: crear carpetas y dejar todo en la cola
Run *run_create(int id, Pipeline *p, const char *base_dir);  

// comm: libera la run y su Pipeline (no borra archivos del disco)
void run_destroy(Run *r);               

// comm: ruta de la bitácora del trabajo en ji
char *run_job_log_path(const Run *r, int ji);        

// comm: ruta del espacio de trabajo del trabajo ji 
char *run_job_workspace(const Run *r, int ji);          

// comm: ruta del artefacto name
char *run_artifact_path(const Run *r, const char *name);     

// comm: crear toda la ruta
int fs_mkdir_p(const char *path);      

// comm: convertir un estado interno en texto
const char *state_name(State s);         

// comm: convertir una instrucción a texto
const char *step_kind_name(StepKind k); 

// comm: saber si una ejecución terminó, sigue activa o cuánto tiempo duró
bool state_is_terminal(State s);         

// comm: reloj único
double now_s(void);                     

#endif
