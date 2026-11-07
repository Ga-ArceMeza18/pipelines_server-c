// parser.h - Estructura de datos para manejar la entrada de pipeline en formato YAML

#ifndef PARSER_H
#define PARSER_H

#include "pipeline.h"
#include "errors.h"

#include <stddef.h>

Pipeline* parse_pipeline(const char* yaml, size_t len, ErrorList* errors);

#endif
