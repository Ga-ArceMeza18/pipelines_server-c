# Servidor de Pipelines Concurrente

Proyecto de **CI-0117: Programación Paralela y Concurrente**.

Este proyecto implementa un servidor concurrente de pipelines. Permite enviar archivos YAML con trabajos y pasos, ejecutarlos de acuerdo con sus dependencias y consultar su estado mediante un cliente de línea de comandos.

## 1. Requisitos

Para compilar y ejecutar el proyecto se necesita:

- Linux.
- GCC (con soporte para C23 o C2x).
- GNU Make.
- Python 3, para generar las gráficas de los benchmarks.
- `libyaml` y sus archivos de desarrollo.
- OpenMP y soporte para hilos POSIX.

En Ubuntu, las dependencias principales se pueden instalar con:

```bash
sudo apt update
sudo apt install build-essential gcc make libyaml-dev python3
```

## 2. Compilación

Desde la carpeta raíz del repositorio, ejecutar:

```bash
make
```

Esto compila el proyecto y genera los ejecutables:

- `pipelined`: servidor de pipelines.
- `pipectl`: cliente de línea de comandos.

Los archivos objeto y dependencias de compilación se guardan en `build/`.

El Makefile selecciona automáticamente `-std=c2x` cuando detecta GCC 13 o anterior, y `-std=c23` para versiones posteriores.

### Otros comandos de compilación

Compilar la versión sin protecciones:

```bash
make sinproteccion
```

Esta versión genera `pipelined_sp`, compilado con `-DSIN_PROTECCION`, para evidenciar una condición de carrera en el recurso compartido elegido por el grupo.

Ejecutar las pruebas:

```bash
make test
```

Este comando compila y ejecuta las pruebas disponibles en `tests/`, tanto en C como en scripts de shell.

Ejecutar los benchmarks:

```bash
make bench
```

Este comando compila el programa de benchmarking, ejecuta los experimentos configurados y genera:

- `benchmark.csv`: resultados de las mediciones.
- Gráficas generadas por `tools/plot.py`.

Limpiar los archivos generados:

```bash
make clean
```

Elimina los directorios de compilación y los ejecutables generados.

## 3. Iniciar el servidor

El servidor es el proceso encargado de recibir las solicitudes del cliente, validar los pipelines, ejecutarlos y conservar sus estados, bitácoras y artefactos.

Su invocación utiliza los siguientes argumentos:

```bash
./pipelined <puerto> <agentes> <directorio_de_trabajo>
```

Por ejemplo:

```bash
mkdir -p workdir
./pipelined 5000 4 ./workdir
```

En este ejemplo:

- `5000`: puerto TCP donde escucha el servidor.
- `4`: cantidad máxima de trabajos que pueden ejecutarse simultáneamente entre todos los pipelines.
- `./workdir`: directorio donde el servidor almacena los archivos relacionados con las ejecuciones.

El número de agentes determina cuántos trabajos pueden ejecutarse al mismo tiempo. Los pasos de un mismo trabajo se ejecutan en orden.

**Importante:** el servidor debe permanecer activo mientras se utiliza el cliente. Para trabajar con el sistema, se puede iniciar en una terminal y ejecutar los comandos del cliente desde otra.

## 4. Utilizar el cliente

`pipectl` permite comunicarse con el servidor y realizar las siguientes operaciones.

| Operación | Descripción |
|---|---|
| `submit <archivo.yaml>` | Envía un pipeline al servidor. Si es válido, devuelve un identificador de ejecución. |
| `status <id>` | Consulta el estado de una ejecución, sus etapas, trabajos y pasos, incluyendo sus duraciones. |
| `logs <id>` | Muestra la bitácora de una ejecución. |
| `logs <id> <trabajo>` | Muestra la bitácora de un trabajo específico. |
| `logs <id> [trabajo] --follow` | Muestra nuevas líneas de la bitácora conforme se generan. |
| `cancel <id>` | Solicita cancelar una ejecución. |
| `list` | Muestra las ejecuciones conocidas por el servidor y sus estados. |
| `shutdown` | Solicita el apagado ordenado del servidor. |

Los comandos anteriores representan las operaciones que debe soportar el cliente. La forma exacta de invocarlos desde la terminal depende de los argumentos implementados en `pipectl.c`.

### Ejemplo de flujo de trabajo

1. Iniciar el servidor.
2. Enviar un archivo YAML mediante `submit`.
3. Guardar el identificador devuelto por el servidor.
4. Consultar el progreso mediante `status`.
5. Consultar los registros mediante `logs`.
6. Utilizar `logs --follow` para observar la ejecución en tiempo real.
7. Si es necesario, solicitar la cancelación mediante `cancel`.
8. Al terminar de utilizar el sistema, solicitar el apagado mediante `shutdown`.

## 5. Formato de los pipelines

Los pipelines se describen mediante archivos YAML.

La estructura general incluye:

- `name`: nombre del pipeline.
- `variables`: variables que pueden sustituirse mediante la sintaxis `$(NOMBRE)`.
- `stages`: lista de etapas.
- `jobs`: trabajos pertenecientes a cada etapa.
- `dependsOn`: dependencias entre trabajos.
- `condition`: condición para ejecutar una etapa o trabajo.
- `timeoutInSeconds`: límite de tiempo de un trabajo.
- `steps`: pasos ordenados que ejecuta cada trabajo.

Los pasos pueden ser:

- `script`: ejecuta un comando de shell.
- `task`: ejecuta una tarea integrada.
- `publish`: publica un archivo o directorio como artefacto.
- `download`: descarga un artefacto publicado al espacio de trabajo del trabajo.

### Ejemplo de pipeline

```yaml
name: Ejemplo

variables:
  MENSAJE: "Hola desde el pipeline"

stages:
  - stage: procesamiento
    jobs:
      - job: primero
        steps:
          - script: "echo $(MENSAJE)"
          - task: Sleep@1
            inputs:
              seconds: 2

      - job: segundo
        dependsOn: primero
        steps:
          - script: "echo El primer trabajo terminó"
```

En este ejemplo, el trabajo `primero` ejecuta dos pasos en orden. El trabajo `segundo` depende de `primero`, por lo que no puede comenzar hasta que su dependencia haya terminado.

## 6. Tareas integradas

El sistema incluye tareas que pueden utilizarse mediante la clave `task` y sus correspondientes `inputs`.

| Tarea | Propósito |
|---|---|
| `Sleep@1` | Simula una operación que espera durante un intervalo de tiempo. |
| `Cpu@1` | Ejecuta un cálculo con carga de CPU. |
| `WordCount@1` | Procesa un archivo y cuenta palabras. |
| `Histogram@1` | Procesa un archivo y genera un histograma. |

Las tareas de procesamiento de datos utilizan OpenMP para distribuir parte del trabajo entre varios hilos.

Los nombres y parámetros aceptados por cada tarea deben corresponder con los definidos en la implementación del proyecto.

## 7. Pruebas y experimentos

Para ejecutar todas las pruebas disponibles:

```bash
make test
```

Para ejecutar los experimentos de rendimiento:

```bash
make bench
```

El benchmark evalúa distintas configuraciones de agentes e hilos, y genera resultados para analizar el comportamiento del sistema.

Los resultados se guardan en `benchmark.csv` y se procesan mediante `tools/plot.py`.

Para comparar el comportamiento normal con la versión que desactiva una protección:

```bash
make clean
make
make sinproteccion
```

La versión normal genera `pipelined`, mientras que la versión sin protecciones genera `pipelined_sp`.

## 8. Apagado

Para finalizar el servidor, utilizar la operación `shutdown` del cliente.

El apagado debe realizarse de manera ordenada, evitando aceptar nuevas ejecuciones y liberando los recursos utilizados por el servidor.
