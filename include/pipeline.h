// Declaraciones para la estructura de datos del pipeline
#ifndef PIPELINE_H
#define PIPELINE_H

#include <stdbool.h>
#include "varlist.h"

// Estados de ejecución
typedef enum {
    STATE_PENDING,
    STATE_RUNNING,
    STATE_SUCCESS,
    STATE_FAILED,
    STATE_SKIPPED,
    STATE_CANCELLED
} State;

// Condiciones
typedef enum {
    CONDITION_SUCCEEDED,
    CONDITION_FAILED,
    CONDITION_ALWAYS,
    CONDITION_NONE
} Condition;

// Tipos de pasos
typedef enum {
    STEP_SCRIPT,
    STEP_TASK,
    STEP_PUBLISH,
    STEP_DOWNLOAD
} StepKind;

// Paso de un trabajo
typedef struct {
    StepKind kind; // Tipo de paso: script, task, publish o download
    char* text; // Texto del paso (script o task)
    char* artifact; // Nombre del artefacto (para publish o download)
    VarList inputs; // Variables de entrada (para publish o download)
    bool continue_on_error; // Indica si continuar el trabajo en caso de error
    State state; // Estado del paso
    int exit_code; // Código de salida del paso
} Step;

// Trabajo
typedef struct {
    char* name; // Nombre del trabajo
    char* stage; // Nombre de la etapa a la que pertenece el trabajo
    int* deps; // Índices de los trabajos de los que depende este trabajo
    int deps_count; // Número de dependencias
    char** pending_deps; // Nombres de los trabajos pendientes de los que depende este trabajo
    int pending_deps_count; // Número de dependencias pendientes
    Condition cond; // Condición de ejecución del trabajo
    int timeout_s; // Tiempo máximo de ejecución del trabajo en segundos
    Step* steps; // Pasos del trabajo
    int steps_count; // Número de pasos
    State state; // Estado del trabajo
    int pending; // Número de pasos pendientes
    VarList out_vars; // Variables de salida del trabajo
} Job;

// Pipeline
typedef struct {
    char* name; // Nombre del pipeline
    VarList vars; // Variables del pipeline
    Job* jobs; // Trabajos del pipeline
    int jobs_count; // Número de trabajos
    bool** ancestor; // Matriz de ancestros, dependencias (jobs_count x jobs_count)
} Pipeline;

#endif
