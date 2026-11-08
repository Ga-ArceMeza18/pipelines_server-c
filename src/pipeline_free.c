#include "pipeline.h"

#include <stdlib.h>

// comm: liberar la memoria de un paso
static void free_step(Step* step) {
    free(step->text);
    free(step->artifact);
    varlist_destroy(&step->inputs);
}

// comm: liberar la memoria de un trabajo y de sus pasos
static void free_job(Job* job) {
    free(job->name);
    free(job->stage);
    free(job->deps);

    for (int i = 0; i < job->pending_deps_count; i++) {
        free(job->pending_deps[i]);
    }

    free(job->pending_deps);

    for (int i = 0; i < job->steps_count; i++) {
        free_step(&job->steps[i]);
    }

    free(job->steps);
    varlist_destroy(&job->out_vars);
}

void pipeline_free(Pipeline* pipeline) {
    if (pipeline == NULL) {
        return;
    }

    // comm: liberar cada trabajo y el arreglo de trabajos
    for (int i = 0; i < pipeline->jobs_count; i++) {
        free_job(&pipeline->jobs[i]);
    }

    free(pipeline->jobs);

    // comm: liberar la matriz de ancestros si alguien la creó
    if (pipeline->ancestor != NULL) {
        for (int i = 0; i < pipeline->jobs_count; i++) {
            free(pipeline->ancestor[i]);
        }

        free(pipeline->ancestor);
    }

    // comm: liberar las variables, el nombre y el pipeline
    varlist_destroy(&pipeline->vars);
    free(pipeline->name);
    free(pipeline);
}
