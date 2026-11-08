// data_tasks.h - Tareas de procesamiento de datos en archivos

#ifndef TASKS_DATA_H
#define TASKS_DATA_H

#include "task_ctx.h"

// Función para contar palabras en un archivo
int task_wordcount(TaskCtx* ctx);

// Función para generar un histograma de palabras en un archivo
int task_histogram(TaskCtx* ctx);

#endif
