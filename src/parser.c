// parser.c - Implementación para manejar la entrada de pipeline en formato YAML

#include "parser.h"
#include "pipeline.h"
#include "validator.h"

#include <limits.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <yaml.h>

// Funciones auxiliarse para parser

// Declaraciones de funciones privadas
// Función para parsear el contenido principal del pipeline
static bool parse_root(yaml_parser_t* parser, Pipeline* pipeline, ErrorList* errors);
// Función para parsear la lista de variables del pipeline
static bool parse_variables(yaml_parser_t* parser, VarList* vars, ErrorList* errors);
// Función para agregar una dependencia pendiente a un job
static bool add_pending_dependency(Job* job, const char* dependency);
// Función para resolver las dependencias entre jobs y convertirlos en índices
static bool resolve_dependencies(Pipeline* pipeline, ErrorList* errors);
// Función para parsear la lista de stages del pipeline
static bool parse_stages(yaml_parser_t* parser, Pipeline* pipeline, ErrorList* errors);
// Función para parsear la lista de jobs de un stage
static bool parse_jobs(yaml_parser_t* parser, Pipeline* pipeline, const char* stage, ErrorList* errors);
// Función para parsear un job individual
static bool parse_job(yaml_parser_t* parser, Pipeline* pipeline, const char* stage, ErrorList* errors);
// Función para liberar la memoria reservada para un job
static void destroy_step(Step* step);
// Función para copiar un valor escalar de un evento YAML a una cadena de caracteres
static char* copy_scalar(const yaml_event_t* event);
// Función para parsear la lista de pasos de un job
static bool parse_steps(yaml_parser_t* parser, Job* job, ErrorList* errors);
// Función para parsear un paso individual
static bool parse_step(yaml_parser_t* parser, Step* step, ErrorList* errors);
// Función para parsear la lista de variables de entrada de un paso
static bool parse_inputs(yaml_parser_t* parser, VarList* inputs, ErrorList* errors);


// Función principal
Pipeline* parse_pipeline(const char* yaml, size_t len, ErrorList* errors) {
    if (yaml == NULL || errors == NULL) {
        return NULL;
    }

    yaml_parser_t parser;
    yaml_event_t event;

    Pipeline* pipeline = calloc(1, sizeof(Pipeline));

    if (pipeline == NULL) {
        return NULL;
    }

    varlist_init(&pipeline->vars);

    // Inicializar el parser de libyaml.
    if (!yaml_parser_initialize(&parser)) {
        free(pipeline);
        return NULL;
    }

    // Establecer el contenido YAML que se va a analizar.
    yaml_parser_set_input_string(&parser, (const unsigned char*)yaml, len);

    bool success = true;

    // Leer el inicio del flujo.
    if (!yaml_parser_parse(&parser, &event)) {
        success = false;
    } else {
        success = event.type == YAML_STREAM_START_EVENT;
        yaml_event_delete(&event);
    }

    // Leer el inicio del documento.
    if (success) {
        if (!yaml_parser_parse(&parser, &event)) {
            success = false;
        } else {
            success = event.type == YAML_DOCUMENT_START_EVENT;
            yaml_event_delete(&event);
        }
    }

    // Procesar el contenido principal del pipeline.
    if (success) {
        success = parse_root(&parser, pipeline, errors);
    }

    // Leer el final del documento.
    if (success) {
        if (!yaml_parser_parse(&parser, &event)) {
            success = false;
        } else {
            success = event.type == YAML_DOCUMENT_END_EVENT;
            yaml_event_delete(&event);
        }
    }

    if (success) {
        success = resolve_dependencies(pipeline, errors);
    }

    // Leer el final del flujo.
    if (success) {
        if (!yaml_parser_parse(&parser, &event)) {
            success = false;
        } else {
            success = event.type == YAML_STREAM_END_EVENT;
            yaml_event_delete(&event);
        }
    }

    // Reportar errores de sintaxis de libyaml.
    if (!success) {
        if (parser.error != YAML_NO_ERROR) {
            errors_add(errors, (int)parser.problem_mark.line + 1, 
                    parser.problem != NULL ? parser.problem 
                    : "Error al analizar YAML");
        } else {
            errors_add(errors, 0, "Estructura YAML inesperada");
        }
    }

    yaml_parser_delete(&parser);

    // Si el parsing falla, no se valida el pipeline.
    if (!success) {
        varlist_destroy(&pipeline->vars);
        free(pipeline->name);
        free(pipeline);
        return NULL;
    }

    // Validar el pipeline construido.
    if (!validate_pipeline(pipeline, errors)) {
        varlist_destroy(&pipeline->vars);
        free(pipeline->name);
        free(pipeline);
        return NULL;
    }

    return pipeline;
}

// ===========================================================================
// Funciones privadas

static bool parse_root(yaml_parser_t* parser, Pipeline* pipeline, ErrorList* errors) {
    yaml_event_t event;

    // El documento debe comenzar con un mapping.
    // Un mapping es un conjunto de pares clave-valor, que en YAML se representa con indentación y ":".
    if (!yaml_parser_parse(parser, &event)) {
        return false;
    }

    if (event.type != YAML_MAPPING_START_EVENT) {
        errors_add(errors, 1, "El pipeline debe ser un mapping");
        yaml_event_delete(&event);
        return false;
    }

    yaml_event_delete(&event);

    bool has_name = false;
    bool has_stages = false;

    // Recorrer las propiedades del pipeline.
    while (true) {

        if (!yaml_parser_parse(parser, &event)) {
            return false;
        }

        // Fin del mapping raíz.
        if (event.type == YAML_MAPPING_END_EVENT) {
            yaml_event_delete(&event);
            break;
        }

        // Las claves del mapping deben ser escalares.
        if (event.type != YAML_SCALAR_EVENT) {
            errors_add(errors, (int)event.start_mark.line + 1, "Se esperaba una clave del pipeline");

            yaml_event_delete(&event);
            return false;
        }

        char* key = malloc(event.data.scalar.length + 1);

        if (key == NULL) {
            yaml_event_delete(&event);
            return false;
        }

        memcpy(key, event.data.scalar.value, event.data.scalar.length);
        key[event.data.scalar.length] = '\0';
        yaml_event_delete(&event);

        // Leer el valor asociado a la clave.
        // El valor leído es el nombre del pipeline
        if (strcmp(key, "name") == 0) {

            if (has_name) {
                errors_add(errors, 0, "La clave name está duplicada");
                free(key);
                return false;
            }

            if (!yaml_parser_parse(parser, &event)) {
                free(key);
                return false;
            }

            if (event.type != YAML_SCALAR_EVENT) {
                errors_add(errors, (int)event.start_mark.line + 1, "El nombre del pipeline debe ser un string");

                yaml_event_delete(&event);
                free(key);
                return false;
            }

            pipeline->name = malloc(event.data.scalar.length + 1);

            if (pipeline->name == NULL) {
                yaml_event_delete(&event);
                free(key);
                return false;
            }

            memcpy(pipeline->name, event.data.scalar.value, event.data.scalar.length);
            pipeline->name[event.data.scalar.length] = '\0';
            yaml_event_delete(&event);
            has_name = true;
        
        // El valor leído es la lista de variables del pipeline
        } else if (strcmp(key, "variables") == 0) {

            if (!parse_variables(parser, &pipeline->vars, errors)) {
                free(key);
                return false;
            }

        // El valor leído es la lista de stages del pipeline
        } else if (strcmp(key, "stages") == 0) {

            if (has_stages) {
                errors_add(errors, 0, "La clave stages está duplicada");
                free(key);
                return false;
            }

            if (!parse_stages(parser, pipeline, errors)) {
                free(key);
                return false;
            }

            has_stages = true;

        // El valor leído no es reconocido, se reporta un error.
        } else {
            errors_add(errors, 0, "Clave desconocida en el pipeline");

            free(key);
            return false;
        }

        // Liberar la memoria del valor leído.
        free(key);
    }

    // Comprobar las propiedades obligatorias.
    if (!has_name) {
        errors_add(errors, 0, "Falta la propiedad obligatoria name");
        return false;
    }

    if (!has_stages) {
        errors_add(errors, 0, "Falta la propiedad obligatoria stages");
        return false;
    }

    return true;
}

// ===========================================================================

static bool parse_variables(yaml_parser_t* parser, VarList* vars, ErrorList* errors) {
    yaml_event_t event;

    // Leer el inicio del mapping.
    if (!yaml_parser_parse(parser, &event)) {
        return false;
    }

    if (event.type != YAML_MAPPING_START_EVENT) {
        errors_add(errors, (int)event.start_mark.line + 1, "variables debe ser un mapping");

        yaml_event_delete(&event);
        return false;
    }

    yaml_event_delete(&event);

    // Leer los pares clave-valor.
    while (true) {
        if (!yaml_parser_parse(parser, &event)) {
            return false;
        }

        // Final del mapping.
        if (event.type == YAML_MAPPING_END_EVENT) {
            yaml_event_delete(&event);
            return true;
        }

        // La clave debe ser escalar.
        if (event.type != YAML_SCALAR_EVENT) {
            errors_add(errors, (int)event.start_mark.line + 1, "El nombre de una variable debe ser escalar");

            yaml_event_delete(&event);
            return false;
        }

        int line = (int)event.start_mark.line + 1;
        char* key = malloc(event.data.scalar.length + 1);

        if (key == NULL) {
            yaml_event_delete(&event);
            return false;
        }

        memcpy(key, event.data.scalar.value, event.data.scalar.length);
        key[event.data.scalar.length] = '\0';

        yaml_event_delete(&event);

        // Comprobar que la variable no esté duplicada.
        for (int i = 0; i < vars->size; i++) {
            if (strcmp(vars->items[i].name, key) == 0) {
                errors_add(errors, line, "Variable duplicada");

                free(key);
                return false;
            }
        }

        // Leer el valor de la variable.
        if (!yaml_parser_parse(parser, &event)) {
            free(key);
            return false;
        }

        if (event.type != YAML_SCALAR_EVENT) {
            errors_add(errors, (int)event.start_mark.line + 1, "El valor de una variable debe ser escalar");

            yaml_event_delete(&event);
            free(key);
            return false;
        }

        char* value = malloc(event.data.scalar.length + 1);

        if (value == NULL) {
            yaml_event_delete(&event);
            free(key);
            return false;
        }

        memcpy(value, event.data.scalar.value, event.data.scalar.length);
        value[event.data.scalar.length] = '\0';

        yaml_event_delete(&event);

        // Almacenar la variable.
        if (!var_set(vars, key, value)) {
            errors_add(errors, line, "No se pudo almacenar la variable");

            free(key);
            free(value);
            return false;
        }

        free(key);
        free(value);
    }
}

// ===========================================================================

static bool add_pending_dependency(Job* job, const char* dependency) {
    // Evitar dependencias duplicadas.
    for (int i = 0; i < job->pending_deps_count; i++) {
        if (strcmp(job->pending_deps[i], dependency) == 0) {
            return true;
        }
    }

    char* name = malloc(strlen(dependency) + 1);

    if (name == NULL) {
        return false;
    }

    strcpy(name, dependency);

    char** new_deps = realloc(job->pending_deps, sizeof(char*) * (job->pending_deps_count + 1));

    if (new_deps == NULL) {
        free(name);
        return false;
    }

    job->pending_deps = new_deps;
    job->pending_deps[job->pending_deps_count] = name;
    job->pending_deps_count++;

    return true;
}

// ===========================================================================

static bool resolve_dependencies(Pipeline* pipeline, ErrorList* errors) {
    // Recorrer todos los trabajos.
    for (int i = 0; i < pipeline->jobs_count; i++) {
        Job* job = &pipeline->jobs[i];

        // Recorrer las dependencias pendientes del trabajo.
        for (int j = 0; j < job->pending_deps_count; j++) {
            char* dependency_name = job->pending_deps[j];
            int dependency_index = -1;

            // Buscar el trabajo por su nombre.
            for (int k = 0; k < pipeline->jobs_count; k++) {
                if (strcmp(pipeline->jobs[k].name, dependency_name) == 0) {
                    dependency_index = k;
                    break;
                }
            }

            // La dependencia no existe.
            if (dependency_index == -1) {
                errors_add(errors, 0, "Dependencia de trabajo inexistente");

                return false;
            }

            // Agregar el índice al arreglo deps.
            int* new_deps = realloc(job->deps, sizeof(int) * (job->deps_count + 1));

            if (new_deps == NULL) {
                return false;
            }

            job->deps = new_deps;
            job->deps[job->deps_count] = dependency_index;
            job->deps_count++;
        }

        // Liberar los nombres temporales.
        for (int j = 0; j < job->pending_deps_count; j++) {
            free(job->pending_deps[j]);
        }

        free(job->pending_deps);

        job->pending_deps = NULL;
        job->pending_deps_count = 0;
    }

    return true;
}

// ===========================================================================

static bool parse_stages(yaml_parser_t* parser, Pipeline* pipeline,ErrorList* errors) {
    yaml_event_t event;

    // Leer el inicio de la secuencia.
    if (!yaml_parser_parse(parser, &event)) {
        return false;
    }

    if (event.type != YAML_SEQUENCE_START_EVENT) {
        errors_add(errors, (int)event.start_mark.line + 1, "stages debe ser una secuencia");

        yaml_event_delete(&event);
        return false;
    }

    yaml_event_delete(&event);
    char* previous_stage = NULL;

    // Recorrer las etapas.
    while (true) {
        if (!yaml_parser_parse(parser, &event)) {
            free(previous_stage);
            return false;
        }

        // Final de la secuencia.
        if (event.type == YAML_SEQUENCE_END_EVENT) {
            yaml_event_delete(&event);
            free(previous_stage);
            return true;
        }

        // Cada etapa debe ser un mapping.
        if (event.type != YAML_MAPPING_START_EVENT) {
            errors_add(errors, (int)event.start_mark.line + 1, "Cada stage debe ser un mapping");

            yaml_event_delete(&event);
            free(previous_stage);
            return false;
        }

        yaml_event_delete(&event);

        char* stage_name = NULL;
        bool has_jobs = false;
        char** stage_dependencies = NULL;
        int stage_dependencies_count = 0;
        bool has_depends = false;

        Condition stage_condition = CONDITION_SUCCEEDED;
        bool has_condition = false;

        // Leer las propiedades de la etapa.
        while (true) {
            if (!yaml_parser_parse(parser, &event)) {
                free(stage_name);
                free(previous_stage);
                return false;
            }

            if (event.type == YAML_MAPPING_END_EVENT) {
                yaml_event_delete(&event);
                break;
            }

            if (event.type != YAML_SCALAR_EVENT) {
                errors_add(errors, (int)event.start_mark.line + 1, "Se esperaba una propiedad de stage");

                yaml_event_delete(&event);
                free(stage_name);
                free(previous_stage);
                return false;
            }

            int line = (int)event.start_mark.line + 1;
            char* key = malloc(event.data.scalar.length + 1);

            if (key == NULL) {
                yaml_event_delete(&event);
                free(stage_name);
                free(previous_stage);
                return false;
            }

            memcpy(key, event.data.scalar.value, event.data.scalar.length);
            key[event.data.scalar.length] = '\0';

            yaml_event_delete(&event);

            if (strcmp(key, "stage") == 0) {
                if (stage_name != NULL) {
                    errors_add(errors, line, "Propiedad stage duplicada");
                    free(key);
                    free(stage_name);
                    free(previous_stage);
                    return false;
                }

                if (!yaml_parser_parse(parser, &event)) {
                    free(key);
                    free(previous_stage);
                    return false;
                }

                if (event.type != YAML_SCALAR_EVENT) {
                    errors_add(errors, (int)event.start_mark.line + 1, "El nombre de stage debe ser escalar");

                    yaml_event_delete(&event);
                    free(key);
                    free(previous_stage);
                    return false;
                }

                stage_name = malloc(event.data.scalar.length + 1);

                if (stage_name == NULL) {
                    yaml_event_delete(&event);
                    free(key);
                    free(previous_stage);
                    return false;
                }

                memcpy(stage_name, event.data.scalar.value, event.data.scalar.length);
                stage_name[event.data.scalar.length] = '\0';
                yaml_event_delete(&event);

            } else if (strcmp(key, "jobs") == 0) {
                if (has_jobs || stage_name == NULL) {
                    errors_add(errors, line, "jobs duplicado o stage debe aparecer primero");

                    free(key);
                    free(stage_name);
                    free(previous_stage);
                    return false;
                }

                if (!parse_jobs(parser, pipeline, stage_name, errors)) {
                    free(key);
                    free(stage_name);
                    free(previous_stage);
                    return false;
                }

                has_jobs = true;


            } else if (strcmp(key, "dependsOn") == 0) {
                if (has_depends) {
                    errors_add(errors, line, "Propiedad dependsOn duplicada");
                    free(key);
                    free(stage_name);
                    free(previous_stage);
                    return false;
                }

                if (!yaml_parser_parse(parser, &event)) {
                    free(key);
                    free(stage_name);
                    free(previous_stage);
                    return false;
                }

                if (event.type == YAML_SCALAR_EVENT) {
                    char* dependency = malloc(event.data.scalar.length + 1);

                    if (dependency == NULL) {
                        yaml_event_delete(&event);
                        free(key);
                        free(stage_name);
                        free(previous_stage);
                        return false;
                    }

                    memcpy(dependency, event.data.scalar.value, event.data.scalar.length);
                    dependency[event.data.scalar.length] = '\0';
                    yaml_event_delete(&event);

                    char** new_dependencies = realloc(stage_dependencies, sizeof(char*) * (stage_dependencies_count + 1));

                    if (new_dependencies == NULL) {
                        free(dependency);
                        free(key);
                        free(stage_name);
                        free(previous_stage);
                        return false;
                    }

                    stage_dependencies = new_dependencies;
                    stage_dependencies[stage_dependencies_count++] = dependency;

                } else if (event.type == YAML_SEQUENCE_START_EVENT) {
                    yaml_event_delete(&event);

                    while (true) {
                        if (!yaml_parser_parse(parser, &event)) {
                            free(key);
                            free(stage_name);
                            free(previous_stage);
                            return false;
                        }

                        if (event.type == YAML_SEQUENCE_END_EVENT) {
                            yaml_event_delete(&event);
                            break;
                        }

                        if (event.type != YAML_SCALAR_EVENT) {
                            errors_add(errors, (int)event.start_mark.line + 1, "Cada dependencia de stage debe ser un nombre");

                            yaml_event_delete(&event);
                            free(key);
                            free(stage_name);
                            free(previous_stage);
                            return false;
                        }

                        char* dependency = malloc(event.data.scalar.length + 1);

                        if (dependency == NULL) {
                            yaml_event_delete(&event);
                            free(key);
                            free(stage_name);
                            free(previous_stage);
                            return false;
                        }

                        memcpy(dependency, event.data.scalar.value, event.data.scalar.length);
                        dependency[event.data.scalar.length] = '\0';
                        yaml_event_delete(&event);

                        char** new_dependencies = realloc(stage_dependencies, sizeof(char*) * (stage_dependencies_count + 1));

                        if (new_dependencies == NULL) {
                            free(dependency);
                            free(key);
                            free(stage_name);
                            free(previous_stage);
                            return false;
                        }

                        stage_dependencies = new_dependencies;
                        stage_dependencies[stage_dependencies_count++] = dependency;
                    }
                } else {
                    errors_add(errors, (int)event.start_mark.line + 1,
                            "dependsOn debe ser un nombre o una secuencia");

                    yaml_event_delete(&event);
                    free(key);
                    free(stage_name);
                    free(previous_stage);
                    return false;
                }

                has_depends = true;

            } else if (strcmp(key, "condition") == 0) {
                if (has_condition) {
                    errors_add(errors, line, "Propiedad condition duplicada");
                    free(key);
                    free(stage_name);
                    free(previous_stage);
                    return false;
                }

                if (!yaml_parser_parse(parser, &event)) {
                    free(key);
                    free(stage_name);
                    free(previous_stage);
                    return false;
                }

                if (event.type != YAML_SCALAR_EVENT) {
                    errors_add(errors, (int)event.start_mark.line + 1, "condition debe ser escalar");

                    yaml_event_delete(&event);
                    free(key);
                    free(stage_name);
                    free(previous_stage);
                    return false;
                }

                if (strncmp((char*)event.data.scalar.value, "succeeded()", event.data.scalar.length) == 0) {
                    stage_condition = CONDITION_SUCCEEDED;
                } else if (strncmp((char*)event.data.scalar.value, "failed()", event.data.scalar.length) == 0) {
                    stage_condition = CONDITION_FAILED;
                } else if (strncmp((char*)event.data.scalar.value, "always()", event.data.scalar.length) == 0) {
                    stage_condition = CONDITION_ALWAYS;
                } else {
                    errors_add(errors, line, "Condición desconocida");

                    yaml_event_delete(&event);
                    free(key);
                    free(stage_name);
                    free(previous_stage);
                    return false;
                }

                yaml_event_delete(&event);
                has_condition = true;
                
            } else {
                errors_add(errors, line, "Propiedad desconocida en stage");

                free(key);
                free(stage_name);
                free(previous_stage);
                return false;
            }

            free(key);
        }

        // Validar que la etapa tenga un nombre y al menos un job.
        if (stage_name == NULL || !has_jobs) {
            errors_add(errors, 0, "Cada stage requiere stage y jobs");

            free(stage_name);
            free(previous_stage);
            return false;
        }

        // Revisa has_condition y asigna la condición a los jobs del stage si es necesario
        if (has_condition) {
            for (int i = 0; i < pipeline->jobs_count; i++) {
                Job* job = &pipeline->jobs[i];

                if (strcmp(job->stage, stage_name) == 0) {
                    job->cond = stage_condition;
                }
            }
        }

        // Agregar dependencias implícitas respecto a la etapa anterior.
        if (previous_stage != NULL) {
            for (int i = 0; i < pipeline->jobs_count; i++) {
                Job* job = &pipeline->jobs[i];

                if (strcmp(job->stage, stage_name) != 0) {
                    continue;
                }

                for (int j = 0; j < pipeline->jobs_count; j++) {
                    Job* previous_job = &pipeline->jobs[j];

                    if (strcmp(previous_job->stage, previous_stage) == 0) {
                        if (!add_pending_dependency(job, previous_job->name)) {
                            free(stage_name);
                            free(previous_stage);
                            return false;
                        }
                    }
                }
            }
        }

        free(previous_stage);
        previous_stage = stage_name;
    }
}

// ===========================================================================

static bool parse_jobs(yaml_parser_t* parser, Pipeline* pipeline, const char* stage, ErrorList* errors) {
    yaml_event_t event;

    // Leer el inicio de la secuencia de trabajos.
    if (!yaml_parser_parse(parser, &event)) {
        return false;
    }

    if (event.type != YAML_SEQUENCE_START_EVENT) {
        errors_add(errors, (int)event.start_mark.line + 1, "jobs debe ser una secuencia");

        yaml_event_delete(&event);
        return false;
    }

    yaml_event_delete(&event);

    // Recorrer los trabajos.
    while (true) {
        if (!yaml_parser_parse(parser, &event)) {
            return false;
        }

        // Final de la secuencia.
        if (event.type == YAML_SEQUENCE_END_EVENT) {
            yaml_event_delete(&event);
            return true;
        }

        // Cada trabajo debe ser un mapping.
        if (event.type != YAML_MAPPING_START_EVENT) {
            errors_add(errors, (int)event.start_mark.line + 1, "Cada job debe ser un mapping");

            yaml_event_delete(&event);
            return false;
        }

        // Consumimos el inicio del mapping.
        yaml_event_delete(&event);

        // Construir y agregar el trabajo.
        if (!parse_job(parser, pipeline, stage, errors)) {
            return false;
        }
    }
}

// ===========================================================================

// Liberar la memoria reservada para un trabajo.
static void destroy_job(Job* job) {
    free(job->name);
    free(job->stage);
    free(job->deps);

    for (int i = 0; i < job->pending_deps_count; i++) {
        free(job->pending_deps[i]);
    }

    free(job->pending_deps);

    for (int i = 0; i < job->steps_count; i++) {
        free(job->steps[i].text);
        free(job->steps[i].artifact);
        varlist_destroy(&job->steps[i].inputs);
    }

    free(job->steps);
    varlist_destroy(&job->out_vars);
}

// ===========================================================================

// Construir un trabajo.
static bool parse_job(yaml_parser_t* parser, Pipeline* pipeline, const char* stage, ErrorList* errors) {
    yaml_event_t event;

    Job job;
    memset(&job, 0, sizeof(Job));

    // Inicializar los valores predeterminados.
    job.state = STATE_PENDING;
    job.cond = CONDITION_SUCCEEDED;
    job.timeout_s = 0;

    varlist_init(&job.out_vars);

    // Guardar el nombre de la etapa.
    job.stage = malloc(strlen(stage) + 1);

    if (job.stage == NULL) {
        destroy_job(&job);
        return false;
    }

    strcpy(job.stage, stage);

    bool has_name = false;
    bool has_depends = false;
    bool has_condition = false;
    bool has_timeout = false;
    bool has_steps = false;

    // Recorrer las propiedades del trabajo.
    while (true) {
        if (!yaml_parser_parse(parser, &event)) {
            destroy_job(&job);
            return false;
        }

        // Final del mapping.
        if (event.type == YAML_MAPPING_END_EVENT) {
            yaml_event_delete(&event);
            break;
        }

        // Las claves deben ser escalares.
        if (event.type != YAML_SCALAR_EVENT) {
            errors_add(errors, (int)event.start_mark.line + 1, "Se esperaba una propiedad del job");

            yaml_event_delete(&event);
            destroy_job(&job);
            return false;
        }

        int line = (int)event.start_mark.line + 1;
        char* key = malloc(event.data.scalar.length + 1);

        if (key == NULL) {
            yaml_event_delete(&event);
            destroy_job(&job);
            return false;
        }

        memcpy(key, event.data.scalar.value, event.data.scalar.length);
        key[event.data.scalar.length] = '\0';

        yaml_event_delete(&event);

        // Leer el nombre del trabajo.
        if (strcmp(key, "job") == 0) {
            if (has_name) {
                errors_add(errors, line, "Propiedad job duplicada");
                free(key);
                destroy_job(&job);
                return false;
            }

            if (!yaml_parser_parse(parser, &event)) {
                free(key);
                destroy_job(&job);
                return false;
            }

            if (event.type != YAML_SCALAR_EVENT) {
                errors_add(errors, (int)event.start_mark.line + 1, "El nombre del job debe ser escalar");

                yaml_event_delete(&event);
                free(key);
                destroy_job(&job);
                return false;
            }

            job.name = malloc(event.data.scalar.length + 1);

            if (job.name == NULL) {
                yaml_event_delete(&event);
                free(key);
                destroy_job(&job);
                return false;
            }

            memcpy(job.name, event.data.scalar.value, event.data.scalar.length);
            job.name[event.data.scalar.length] = '\0';

            yaml_event_delete(&event);
            has_name = true;
        }

        // Leer los nombres de las dependencias.
        else if (strcmp(key, "dependsOn") == 0) {
            if (has_depends) {
                errors_add(errors, line, "Propiedad dependsOn duplicada");
                free(key);
                destroy_job(&job);
                return false;
            }

            if (!yaml_parser_parse(parser, &event)) {
                free(key);
                destroy_job(&job);
                return false;
            }

            // Caso 1: una única dependencia.
            if (event.type == YAML_SCALAR_EVENT) {
                char* dependency = malloc(event.data.scalar.length + 1);

                if (dependency == NULL) {
                    yaml_event_delete(&event);
                    free(key);
                    destroy_job(&job);
                    return false;
                }

                memcpy(dependency, event.data.scalar.value, event.data.scalar.length);
                dependency[event.data.scalar.length] = '\0';

                yaml_event_delete(&event);

                if (!add_pending_dependency(&job, dependency)) {
                    free(dependency);
                    free(key);
                    destroy_job(&job);
                    return false;
                }

                free(dependency);
            }

            // Caso 2: una lista de dependencias.
            else if (event.type == YAML_SEQUENCE_START_EVENT) {
                yaml_event_delete(&event);

                while (true) {
                    if (!yaml_parser_parse(parser, &event)) {
                        free(key);
                        destroy_job(&job);
                        return false;
                    }

                    if (event.type == YAML_SEQUENCE_END_EVENT) {
                        yaml_event_delete(&event);
                        break;
                    }

                    if (event.type != YAML_SCALAR_EVENT) {
                        errors_add(errors, (int)event.start_mark.line + 1, "Cada dependencia debe ser un nombre");
                        yaml_event_delete(&event);
                        free(key);
                        destroy_job(&job);
                        return false;
                    }

                    char* dependency = malloc(event.data.scalar.length + 1);

                    if (dependency == NULL) {
                        yaml_event_delete(&event);
                        free(key);
                        destroy_job(&job);
                        return false;
                    }

                    memcpy(dependency, event.data.scalar.value, event.data.scalar.length);
                    dependency[event.data.scalar.length] = '\0';

                    yaml_event_delete(&event);

                    if (!add_pending_dependency(&job, dependency)) {
                        free(dependency);
                        free(key);
                        destroy_job(&job);
                        return false;
                    }

                    free(dependency);
                }
            }

            // Cualquier otro tipo de valor es inválido.
            else {
                errors_add(errors, (int)event.start_mark.line + 1, "dependsOn debe ser un nombre o una secuencia");
                yaml_event_delete(&event);
                free(key);
                destroy_job(&job);
                return false;
            }

            has_depends = true;
        }

        // Leer la condición de ejecución.
        else if (strcmp(key, "condition") == 0) {
            if (has_condition) {
                errors_add(errors, line, "Propiedad condition duplicada");
                free(key);
                destroy_job(&job);
                return false;
            }

            if (!yaml_parser_parse(parser, &event)) {
                free(key);
                destroy_job(&job);
                return false;
            }

            if (event.type != YAML_SCALAR_EVENT) {
                errors_add(errors, (int)event.start_mark.line + 1, "condition debe ser escalar");

                yaml_event_delete(&event);
                free(key);
                destroy_job(&job);
                return false;
            }

            char* condition = malloc(event.data.scalar.length + 1);

            if (condition == NULL) {
                yaml_event_delete(&event);
                free(key);
                destroy_job(&job);
                return false;
            }

            memcpy(condition, event.data.scalar.value, event.data.scalar.length);
            condition[event.data.scalar.length] = '\0';

            yaml_event_delete(&event);

            if (strcmp(condition, "succeeded()") == 0) {
                job.cond = CONDITION_SUCCEEDED;
            } else if (strcmp(condition, "failed()") == 0) {
                job.cond = CONDITION_FAILED;
            } else if (strcmp(condition, "always()") == 0) {
                job.cond = CONDITION_ALWAYS;
            } else {
                errors_add(errors, line, "Condición desconocida");

                free(condition);
                free(key);
                destroy_job(&job);
                return false;
            }

            free(condition);
            has_condition = true;
        }

        // Leer el tiempo máximo de ejecución.
        else if (strcmp(key, "timeoutInSeconds") == 0) {
            if (has_timeout) {
                errors_add(errors, line, "Propiedad timeout duplicada");
                free(key);
                destroy_job(&job);
                return false;
            }

            if (!yaml_parser_parse(parser, &event)) {
                free(key);
                destroy_job(&job);
                return false;
            }

            if (event.type != YAML_SCALAR_EVENT) {
                errors_add(errors, (int)event.start_mark.line + 1, "timeout debe ser un número");

                yaml_event_delete(&event);
                free(key);
                destroy_job(&job);
                return false;
            }

            char* timeout = malloc(event.data.scalar.length + 1);

            if (timeout == NULL) {
                yaml_event_delete(&event);
                free(key);
                destroy_job(&job);
                return false;
            }

            memcpy(timeout, event.data.scalar.value, event.data.scalar.length);
            timeout[event.data.scalar.length] = '\0';

            yaml_event_delete(&event);

            char* end;
            errno = 0;

            long seconds = strtol(timeout, &end, 10);

            if (errno == ERANGE || end == timeout ||
                *end != '\0' || seconds <= 0 || seconds > INT_MAX) {
                errors_add(errors, line, "timeout debe ser un entero positivo");

                free(timeout);
                free(key);
                destroy_job(&job);
                return false;
            }

            job.timeout_s = (int)seconds;

            free(timeout);
            has_timeout = true;
        }

        // Leer los pasos del trabajo.
        else if (strcmp(key, "steps") == 0) {
            if (has_steps) {
                errors_add(errors, line, "Propiedad steps duplicada");
                free(key);
                destroy_job(&job);
                return false;
            }

            if (!parse_steps(parser, &job, errors)) {
                free(key);
                destroy_job(&job);
                return false;
            }

            has_steps = true;
        }

        // Propiedad desconocida.
        else {
            errors_add(errors, line, "Propiedad desconocida en job");

            free(key);
            destroy_job(&job);
            return false;
        }

        free(key);
    }

    // Comprobar las propiedades obligatorias.
    if (!has_name) {
        errors_add(errors, 0, "Falta la propiedad obligatoria job");
        destroy_job(&job);
        return false;
    }

    if (!has_steps) {
        errors_add(errors, 0, "Falta la propiedad obligatoria steps");
        destroy_job(&job);
        return false;
    }

    // Agregar el trabajo al pipeline.
    Job* new_jobs = realloc(pipeline->jobs, sizeof(Job) * (pipeline->jobs_count + 1));

    if (new_jobs == NULL) {
        destroy_job(&job);
        return false;
    }

    pipeline->jobs = new_jobs;
    pipeline->jobs[pipeline->jobs_count] = job;
    pipeline->jobs_count++;

    return true;
}

// ===========================================================================

static void destroy_step(Step* step) {
    free(step->text);
    free(step->artifact);
    varlist_destroy(&step->inputs);
}

// ===========================================================================

static char* copy_scalar(const yaml_event_t* event) {
    char* value = malloc(event->data.scalar.length + 1);

    if (value == NULL) {
        return NULL;
    }

    memcpy(value, event->data.scalar.value, event->data.scalar.length);
    value[event->data.scalar.length] = '\0';

    return value;
}


// ===========================================================================

static bool parse_steps(yaml_parser_t* parser, Job* job, ErrorList* errors) {
    yaml_event_t event;

    /* Se espera el inicio de una secuencia. */
    if (!yaml_parser_parse(parser, &event)) {
        return false;
    }

    if (event.type != YAML_SEQUENCE_START_EVENT) {
        errors_add(errors, event.start_mark.line + 1, "steps debe ser una secuencia");
        yaml_event_delete(&event);
        return false;
    }

    yaml_event_delete(&event);

    while (true) {
        if (!yaml_parser_parse(parser, &event)) {
            return false;
        }

        /* Fin de la secuencia de Steps. */
        if (event.type == YAML_SEQUENCE_END_EVENT) {
            yaml_event_delete(&event);
            return true;
        }

        /* Cada Step debe ser un mapping. */
        if (event.type != YAML_MAPPING_START_EVENT) {
            errors_add(errors, event.start_mark.line + 1, "Cada Step debe ser un mapping");
            yaml_event_delete(&event);
            return false;
        }

        yaml_event_delete(&event);

        Step step;

        if (!parse_step(parser, &step, errors)) {
            return false;
        }

        /* Agregar el Step al arreglo del Job. */
        Step* new_steps = realloc(job->steps, sizeof(Step) * (job->steps_count + 1));

        if (new_steps == NULL) {
            errors_add(errors, 0, "No se pudo reservar memoria para los Steps");
            destroy_step(&step);
            return false;
        }

        job->steps = new_steps;
        job->steps[job->steps_count] = step;
        job->steps_count++;
    }
}

// ===========================================================================

static bool parse_step(yaml_parser_t* parser, Step* step, ErrorList* errors) {
    yaml_event_t event;

    memset(step, 0, sizeof(Step));

    step->state = STATE_PENDING;
    step->exit_code = 0;
    step->continue_on_error = false;

    varlist_init(&step->inputs);

    bool has_kind = false;
    bool has_inputs = false;
    bool has_artifact = false;
    bool has_path = false;
    bool has_continue = false;

    while (true) {
        if (!yaml_parser_parse(parser, &event)) {
            destroy_step(step);
            return false;
        }

        // Fin del mapping del Step.
        if (event.type == YAML_MAPPING_END_EVENT) {
            yaml_event_delete(&event);
            break;
        }

        if (event.type != YAML_SCALAR_EVENT) {
            errors_add(errors, event.start_mark.line + 1,
                       "Se esperaba una propiedad del Step");
            yaml_event_delete(&event);
            destroy_step(step);
            return false;
        }

        char* key = copy_scalar(&event);
        yaml_event_delete(&event);

        if (key == NULL) {
            destroy_step(step);
            return false;
        }

        /* Leer el valor de la propiedad. */
        if (!yaml_parser_parse(parser, &event)) {
            free(key);
            destroy_step(step);
            return false;
        }

        bool success = true;

        if (strcmp(key, "script") == 0 || strcmp(key, "task") == 0 ||
            strcmp(key, "publish") == 0 || strcmp(key, "download") == 0) {

            if (has_kind) {
                errors_add(errors, event.start_mark.line + 1, "Un Step debe tener exactamente un tipo");
                success = false;
            } else if (event.type != YAML_SCALAR_EVENT) {
                errors_add(errors, event.start_mark.line + 1, "El tipo de Step debe tener un valor escalar");
                success = false;
            } else {
                step->text = copy_scalar(&event);

                if (step->text == NULL) {
                    success = false;
                } else {
                    has_kind = true;

                    if (strcmp(key, "script") == 0) {
                        step->kind = STEP_SCRIPT;
                    } else if (strcmp(key, "task") == 0) {
                        step->kind = STEP_TASK;
                    } else if (strcmp(key, "publish") == 0) {
                        step->kind = STEP_PUBLISH;
                    } else {
                        step->kind = STEP_DOWNLOAD;
                    }
                }
            }
        }

        else if (strcmp(key, "inputs") == 0) {
            if (has_inputs) {
                errors_add(errors, event.start_mark.line + 1, "La propiedad inputs está duplicada");
                success = false;
            } else if (event.type != YAML_MAPPING_START_EVENT) {
                errors_add(errors, event.start_mark.line + 1, "inputs debe ser un mapping");
                success = false;
            } else {
                has_inputs = true;

                // El inicio del mapping ya fue consumido; parse_inputs lee el resto.
                yaml_event_delete(&event);
                free(key);

                if (!parse_inputs(parser, &step->inputs, errors)) {
                    destroy_step(step);
                    return false;
                }

                continue;
            }
        }

        else if (strcmp(key, "artifact") == 0) {
            if (has_artifact) {
                errors_add(errors, event.start_mark.line + 1, "La propiedad artifact está duplicada");
                success = false;
            } else if (event.type != YAML_SCALAR_EVENT) {
                errors_add(errors, event.start_mark.line + 1, "artifact debe tener un valor escalar");
                success = false;
            } else {
                step->artifact = copy_scalar(&event);
                has_artifact = true;

                if (step->artifact == NULL) {
                    success = false;
                }
            }
        }

        else if (strcmp(key, "path") == 0) {
            if (has_path) {
                errors_add(errors, event.start_mark.line + 1, "La propiedad path está duplicada");
                success = false;
            } else if (event.type != YAML_SCALAR_EVENT) {
                errors_add(errors, event.start_mark.line + 1, "path debe tener un valor escalar");
                success = false;
            } else {
                step->artifact = copy_scalar(&event);
                has_path = true;

                if (step->artifact == NULL) {
                    success = false;
                }
            }
        }

        else if (strcmp(key, "continueOnError") == 0) {
            if (has_continue) {
                errors_add(errors, event.start_mark.line + 1, "La propiedad continueOnError está duplicada");
                success = false;
            } else if (event.type != YAML_SCALAR_EVENT) {
                errors_add(errors, event.start_mark.line + 1, "continueOnError debe ser un booleano");
                success = false;
            } else if (strcmp((char*)event.data.scalar.value, "true") == 0) {
                step->continue_on_error = true;
                has_continue = true;
            } else if (strcmp((char*)event.data.scalar.value, "false") == 0) {
                step->continue_on_error = false;
                has_continue = true;
            } else {
                errors_add(errors, event.start_mark.line + 1, "continueOnError debe ser true o false");
                success = false;
            }
        }

        else {
            errors_add(errors, event.start_mark.line + 1, "Propiedad desconocida en Step");
            success = false;
        }

        yaml_event_delete(&event);
        free(key);

        if (!success) {
            destroy_step(step);
            return false;
        }
    }

    // Validar que el Step tenga un tipo.
    if (!has_kind) {
        errors_add(errors, 0, "El Step no especifica un tipo");
        destroy_step(step);
        return false;
    }

    // Validaciones específicas de cada tipo.
    if (step->kind == STEP_SCRIPT) {
        if (has_inputs || has_artifact || has_path) {
            errors_add(errors, 0, "Un Step script no admite inputs, artifact ni path");
            destroy_step(step);
            return false;
        }
    }

    else if (step->kind == STEP_TASK) {
        if (has_artifact || has_path) {
            errors_add(errors, 0, "Un Step task no admite artifact ni path");
            destroy_step(step);
            return false;
        }
    }

    else if (step->kind == STEP_PUBLISH) {
        if (!has_artifact || has_path || has_inputs) {
            errors_add(errors, 0, "Un Step publish requiere artifact y no admite inputs ni path");
            destroy_step(step);
            return false;
        }
    }

    else if (step->kind == STEP_DOWNLOAD) {
        if (!has_path || has_artifact || has_inputs) {
            errors_add(errors, 0, "Un Step download requiere path y no admite inputs ni artifact");
            destroy_step(step);
            return false;
        }
    }

    return true;
}

// ===========================================================================

static bool parse_inputs(yaml_parser_t* parser, VarList* inputs, ErrorList* errors) {
    yaml_event_t event;

    while (true) {
        if (!yaml_parser_parse(parser, &event)) {
            return false;
        }

        if (event.type == YAML_MAPPING_END_EVENT) {
            yaml_event_delete(&event);
            return true;
        }

        if (event.type != YAML_SCALAR_EVENT) {
            errors_add(errors, event.start_mark.line + 1, "La clave de input debe ser escalar");
            yaml_event_delete(&event);
            return false;
        }

        char* key = copy_scalar(&event);
        yaml_event_delete(&event);

        if (key == NULL) {
            return false;
        }

        // Leer el valor del input.
        if (!yaml_parser_parse(parser, &event)) {
            free(key);
            return false;
        }

        if (event.type != YAML_SCALAR_EVENT) {
            errors_add(errors, event.start_mark.line + 1, "El valor de input debe ser escalar");
            yaml_event_delete(&event);
            free(key);
            return false;
        }

        char* value = copy_scalar(&event);
        yaml_event_delete(&event);

        if (value == NULL) {
            free(key);
            return false;
        }

        // Comprobar que no exista una clave duplicada.
        bool duplicate = false;

        for (int i = 0; i < inputs->size; i++) {
            if (strcmp(inputs->items[i].name, key) == 0) {
                duplicate = true;
                break;
            }
        }

        if (duplicate) {
            errors_add(errors, 0, "Input duplicado");
            free(key);
            free(value);
            return false;
        }

        if (!var_set(inputs, key, value)) {
            errors_add(errors, 0, "No se pudo almacenar el input");
            free(key);
            free(value);
            return false;
        }

        free(key);
        free(value);
    }
}
