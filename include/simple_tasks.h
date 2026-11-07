// simple_tasks.h - Tareas integradas Sleep@1 y Cpu@1

#ifndef SIMPLE_TASKS_H
#define SIMPLE_TASKS_H

#include "task_ctx.h"

// Espera los segundos que vienen en el input "seconds"
int task_sleep(TaskCtx* ctx);

// Hace un cálculo con "iterations" y "seed" y guarda el resultado en CPU_RESULT
int task_cpu(TaskCtx* ctx);

#endif
