// runner.h - Ejecución completa de un trabajo dentro de un agente

#ifndef RUNNER_H
#define RUNNER_H

#include "run.h"

// comm: ejecutar un trabajo completo en el hilo que la llama (un agente)
// prepara el espacio de trabajo, hereda variables de los ancestros,
// corre los pasos en orden y aplica continueOnError, timeout y cancelación
// resultados posibles: STATE_SUCCESS, STATE_PARTIALLY_SUCCESS, STATE_FAILED o STATE_CANCELLED
State run_job(Run *run, Job *job);

#endif