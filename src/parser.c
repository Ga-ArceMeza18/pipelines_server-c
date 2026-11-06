// parser.c - Implementación para manejar la entrada de pipeline en formato YAML

#include "parser.h"
#include "pipeline.h"
#include "validator.h"

#include <yaml.h>
#include <stdlib.h>
#include <string.h>

// Funciones auxiliarse para parser

// Declaraciones de funciones privadas
static bool parse_root(yaml_parser_t* parser, Pipeline* pipeline, ErrorList* errors);

static bool parse_variables(yaml_parser_t* parser, VarList* vars, ErrorList* errors);

static bool parse_stages(yaml_parser_t* parser, Pipeline* pipeline, ErrorList* errors);

static bool parse_jobs(yaml_parser_t* parser, Pipeline* pipeline, const char* stage, ErrorList* errors);

static bool parse_job(yaml_parser_t* parser, Pipeline* pipeline, const char* stage, ErrorList* errors);

static bool parse_steps(yaml_parser_t* parser, Job* job, ErrorList* errors);

static bool parse_step(yaml_parser_t* parser, Step* step, ErrorList* errors);

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
