#ifndef TASK_CTX_H
#define TASK_CTX_H

#include "varlist.h"

#include <stdatomic.h>
#include <stdio.h>

// Estructura que representa el contexto de una tarea
typedef struct {
    const VarList* inputs; // Lista de variables de entrada
    VarList* out; // Lista de variables de salida
    FILE* log; // Archivo de registro para la tarea
    const char* workdir; // Directorio de trabajo para la tarea
    const atomic_bool* cancel; // Puntero a una variable atómica que indica si la tarea debe cancelarse
    double deadline_s; // Tiempo límite en segundos para la ejecución de la tarea
    int max_threads; // Número máximo de hilos que la tarea puede utilizar
} TaskCtx;

#endif
