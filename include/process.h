#ifndef PROCESS_H                        // guarda de inclusión
#define PROCESS_H                        // define la guarda

#include "run.h"                         // Run (para leer la bandera de cancelación)

// comm: errores predefinidos
#define PROC_ERROR -1               // no se pudo lanzar o esperar el proceso
#define PROC_TIMEOUT -2               // venció el tiempo límite y se mató al grupo
#define PROC_CANCELED -3              // se canceló la ejecución y se mató al grupo

// comm: correr un proceso usando args previamente definidos
int run_process(char *const argv[], const char *log, Run *run, double timeout_s, const char *cwd, VarList *newvars);

#endif