// validator.c - Implementación de la función de validación de la entrada de pipeline en formato YAML

#include "validator.h"

#include "validator.h"

#include <stdlib.h>
#include <string.h>

// Funciones auxiliares privadas.
// Valida que los nombres de los trabajos sean únicos.
static bool validate_job_names(Pipeline* pipeline, ErrorList* errors);
// Valida que las dependencias de los trabajos sean válidas.
static bool validate_dependencies(Pipeline* pipeline, ErrorList* errors);
// Valida que no existan ciclos en las dependencias de los trabajos.
static bool validate_cycles(Pipeline* pipeline, ErrorList* errors);
// Valida que los artefactos publicados por los trabajos sean válidos.
static bool validate_artifacts(Pipeline* pipeline, ErrorList* errors);
// Buscar un trabajo por nombre en el pipeline por medio de DFS
static bool visit_job(Pipeline* pipeline,int index, int* colors, ErrorList* errors);
// Identifica si un trabajo publica un artefacto específico.
static bool job_publishes(Job* job, const char* artifact);
// Busca un trabajo que publique un artefacto específico en los ancestros de un trabajo dado.
static bool find_ancestor_publisher(Pipeline* pipeline, int index, const char* artifact, bool* visited);

// Función principal
bool validate_pipeline(Pipeline* pipeline, ErrorList* errors) {
    if (pipeline == NULL || errors == NULL) {
        return false;
    }

    bool valid = true;
    if (!validate_job_names(pipeline, errors)) {
        valid = false;
    }
    if (!validate_dependencies(pipeline, errors)) {
        valid = false;
    }
    if (!validate_cycles(pipeline, errors)) {
        valid = false;
    }
    if (!validate_artifacts(pipeline, errors)) {
        valid = false;
    }

    return valid;
}

// =============================================================================

static bool validate_job_names(Pipeline* pipeline, ErrorList* errors) {
    bool valid = true;

    for (int i = 0; i < pipeline->jobs_count; i++) {
        Job* current = &pipeline->jobs[i];

        if (current->name == NULL || current->name[0] == '\0') {
            errors_add(errors, 0, "Un Job tiene un nombre vacío");
            valid = false;
            continue;
        }

        for (int j = i + 1; j < pipeline->jobs_count; j++) {
            if (pipeline->jobs[j].name != NULL &&
                strcmp(current->name, pipeline->jobs[j].name) == 0) {

                errors_add(errors, 0, "Existen Jobs con nombres duplicados");
                valid = false;
            }
        }
    }

    return valid;
}

// =============================================================================

static bool validate_dependencies(Pipeline* pipeline, ErrorList* errors) {
    bool valid = true;

    for (int i = 0; i < pipeline->jobs_count; i++) {
        Job* job = &pipeline->jobs[i];

        for (int j = 0; j < job->deps_count; j++) {
            int dependency = job->deps[j];

            if (dependency < 0 || dependency >= pipeline->jobs_count) {
                errors_add(errors, 0, "Una dependencia referencia un Job inexistente");
                valid = false;
            }
        }
    }

    return valid;
}

// =============================================================================

static bool visit_job(
    Pipeline* pipeline,
    int index,
    int* colors,
    ErrorList* errors
) {
    colors[index] = 1;

    Job* job = &pipeline->jobs[index];

    for (int i = 0; i < job->deps_count; i++) {
        int dependency = job->deps[i];

        if (dependency < 0 || dependency >= pipeline->jobs_count) {
            continue;
        }

        if (colors[dependency] == 1) {
            errors_add(errors, 0, "Se detectó un ciclo entre las dependencias");
            return false;
        }

        if (colors[dependency] == 0) {
            if (!visit_job(pipeline, dependency, colors, errors)) {
                return false;
            }
        }
    }

    colors[index] = 2;
    return true;
}

static bool validate_cycles(Pipeline* pipeline, ErrorList* errors) {
    int* colors = calloc(pipeline->jobs_count, sizeof(int));

    if (pipeline->jobs_count > 0 && colors == NULL) {
        errors_add(errors, 0, "No se pudo reservar memoria para validar dependencias");
        return false;
    }

    bool valid = true;

    for (int i = 0; i < pipeline->jobs_count; i++) {
        if (colors[i] == 0) {
            if (!visit_job(pipeline, i, colors, errors)) {
                valid = false;
            }
        }
    }

    free(colors);
    return valid;
}

// =============================================================================

static bool job_publishes(Job* job, const char* artifact) {
    for (int i = 0; i < job->steps_count; i++) {
        Step* step = &job->steps[i];

        if (step->kind == STEP_PUBLISH &&
            step->artifact != NULL &&
            strcmp(step->artifact, artifact) == 0) {

            return true;
        }
    }

    return false;
}

// =============================================================================

static bool find_ancestor_publisher(Pipeline* pipeline, int index, const char* artifact, bool* visited) {
    Job* job = &pipeline->jobs[index];

    for (int i = 0; i < job->deps_count; i++) {
        int dependency = job->deps[i];

        if (dependency < 0 || dependency >= pipeline->jobs_count) {
            continue;
        }

        if (visited[dependency]) {
            continue;
        }

        visited[dependency] = true;

        if (job_publishes(&pipeline->jobs[dependency], artifact)) {
            return true;
        }

        if (find_ancestor_publisher(pipeline, dependency, artifact, visited)) {
            return true;
        }
    }

    return false;
}

// =============================================================================

static bool validate_artifacts(Pipeline* pipeline, ErrorList* errors) {
    bool valid = true;

    for (int i = 0; i < pipeline->jobs_count; i++) {
        Job* job = &pipeline->jobs[i];

        for (int j = 0; j < job->steps_count; j++) {
            Step* step = &job->steps[j];

            if (step->kind == STEP_PUBLISH) {
                if (step->artifact == NULL || step->artifact[0] == '\0') {
                    errors_add(errors, 0, "Un artefacto publicado tiene un nombre vacío");
                    valid = false;
                    continue;
                }

                // Comprobar si otro Job publica el mismo artefacto.
                for (int k = i + 1; k < pipeline->jobs_count; k++) {
                    if (job_publishes(&pipeline->jobs[k], step->artifact)) {
                        errors_add(errors, 0, "Existen artefactos publicados con nombres duplicados");
                        valid = false;
                        break;
                    }
                }
            }

            else if (step->kind == STEP_DOWNLOAD) {
                if (step->text == NULL || step->text[0] == '\0') {
                    errors_add(errors, 0, "Una descarga tiene un nombre de artefacto vacío");
                    valid = false;
                    continue;
                }

                bool* visited = calloc(pipeline->jobs_count, sizeof(bool));

                if (pipeline->jobs_count > 0 && visited == NULL) {
                    errors_add(errors, 0, "No se pudo reservar memoria para validar artefactos");
                    valid = false;
                    continue;
                }

                bool found = find_ancestor_publisher(pipeline, i, step->text, visited);

                free(visited);

                if (!found) {
                    errors_add(errors, 0, "Un Job descarga un artefacto que no publica ninguno de sus antecesores");
                    valid = false;
                }
            }
        }
    }

    return valid;
}
