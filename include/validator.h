// validator.h - Estructura de datos para validar la entrada de pipeline en formato YAML

#ifndef VALIDATOR_H
#define VALIDATOR_H

#include "pipeline.h"
#include "errors.h"

#include <stdbool.h>

bool validate_pipeline(Pipeline* pipeline, ErrorList* errors);

#endif
