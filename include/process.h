#ifndef PROCESS_H
#define PROCESS_H

#include "run.h"

#define PROC_ERROR    -1  // comm: indicar que no se pudo lanzar o esperar el proceso
#define PROC_TIMEOUT  -2  // comm: indicar que venció el tiempo y se mató al grupo
#define PROC_CANCELED -3  // comm: indicar que se canceló la ejecución y se mató al grupo

// comm: ejecutar un comando como proceso hijo, escribir su salida en la bitácora
// y leer las líneas ##setvariable hacia newvars
// devuelve el código de salida (>= 0) o un código PROC_*
int run_process(char *const argv[], const char *log, Run *run, double timeout_s, const char *cwd, VarList *newvars);

#endif