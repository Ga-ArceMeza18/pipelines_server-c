#ifndef RUNNER_H                        
#define RUNNER_H                        

#include "run.h"                  

/*
 Ejecuta un trabajo completo en el hilo que la llama (un agente):
 prepara su espacio de trabajo, hereda variables de sus ancestros, corre los pasos en orden, aplica continueOnError / timeout / cancelación y 
 actualiza el estado de cada paso.
 Posibles resultados: SUCCEEDED, PARTIALLY_SUCCEEDED, FAILED o CANCELED.
*/
State run_job(Run *run, Job *job);

#endif 
