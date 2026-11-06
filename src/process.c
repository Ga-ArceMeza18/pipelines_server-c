#define _GNU_SOURCE                      // posix_spawn_file_actions_addchdir_np, environ, getline
#include <ctype.h>                       // isalnum
#include <errno.h>                       // errno, EINTR
#include <fcntl.h>                       // O_WRONLY, O_APPEND, ...
#include <signal.h>                      // kill, sigset_t
#include <spawn.h>                       // posix_spawn y sus atributos
#include <stdio.h>                       // FILE, getline
#include <stdlib.h>                      // free
#include <string.h>                      // strncmp, strchr
#include <sys/stat.h>                    // stat
#include <sys/wait.h>                    // waitpid, waitid
#include <time.h>                        // nanosleep
#include <unistd.h>                      // environ
#include "process.h"                     // declaraciones de este módulo

#define POLL_NS     10000000L            // revisa cada 10 ms si el hijo terminó / se canceló
#define GRACE_TICKS 200                  // 200 * 10 ms = 2 s de gracia entre SIGTERM y SIGKILL

static void dormir_poll(void) {          // pausa corta entre revisiones
    struct timespec ts = {0, POLL_NS};   // 0 s + 10 ms
    nanosleep(&ts, NULL);                // si lo interrumpe una señal no importa: se vuelve a revisar
}

/*
 * Mata TODO el grupo de procesos (el script y sus hijos/nietos) y cosecha al líder.
 * Primero pide terminar (SIGTERM), da 2 s de gracia y luego mata sin piedad (SIGKILL).
 * Usamos waitid(WNOWAIT) para "mirar" si ya terminó SIN cosecharlo: así el pid no puede
 * reutilizarse antes de enviar SIGKILL al grupo.
 */
static void matar_grupo(pid_t pgid) {    // pgid == pid del líder (lo fijamos con POSIX_SPAWN_SETPGROUP)
    kill(-pgid, SIGTERM);                // el signo negativo significa "a todo el grupo"
    for (int i = 0; i < GRACE_TICKS; i++) {  // espera hasta 2 s a que el líder termine solo
        siginfo_t si;                    // información del hijo
        si.si_pid = 0;                   // se queda en 0 si aún no terminó
        if (waitid(P_PID, (id_t)pgid, &si, WEXITED | WNOHANG | WNOWAIT) == 0 && si.si_pid == pgid) break;  // ya terminó
        dormir_poll();                   // espera un poco y reintenta
    }
    kill(-pgid, SIGKILL);                // mata cualquier sobreviviente del grupo
    int st;                              // estado de salida (se descarta)
    while (waitpid(pgid, &st, 0) < 0 && errno == EINTR) {}  // cosecha al líder (evita zombi)
}

static bool nombre_valido(const char *n) {  // NOMBRE de variable: letras, dígitos y '_'
    if (n == NULL || n[0] == '\0') return false;  // vacío no sirve
    for (const char *c = n; *c; c++) {   // revisa cada carácter
        unsigned char u = (unsigned char)*c;  // evita signo en isalnum
        if (!isalnum(u) && *c != '_') return false;  // carácter no permitido
    }
    return true;                         // nombre correcto
}

/* Lee la bitácora desde 'desde' y guarda en vars cada línea "##setvariable NOMBRE=valor". */
static void leer_setvariable(const char *log, off_t desde, VarList *vars) {
    FILE *f = fopen(log, "r");           // abre la bitácora para lectura
    if (f == NULL) return;               // si no se puede, no hay variables
    if (fseeko(f, desde, SEEK_SET) != 0) {  // salta lo que escribieron pasos anteriores
        fclose(f);                       // cierra
        return;                          // no podemos posicionarnos
    }
    char *line = NULL;                   // buffer que getline va agrandando
    size_t cap = 0;                      // capacidad del buffer
    ssize_t n;                           // largo de la línea leída
    while ((n = getline(&line, &cap, f)) >= 0) {  // lee línea por línea sin límite de largo
        while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r')) line[--n] = '\0';  // quita fin de línea
        if (strncmp(line, "##setvariable ", 14) != 0) continue;  // solo las líneas con el formato
        char *eq = strchr(line + 14, '=');  // separa NOMBRE de valor
        if (eq == NULL) continue;        // sin '=' no es válida
        *eq = '\0';                      // termina el nombre
        const char *nombre = line + 14;  // nombre de la variable
        const char *valor = eq + 1;      // valor (puede estar vacío)
        if (!nombre_valido(nombre)) continue;  // nombre inválido: se ignora
        var_set(vars, nombre, valor);    // si se repite en el mismo paso gana la última (orden secuencial)
    }
    free(line);                          // libera el buffer de getline
    fclose(f);                           // cierra la bitácora
}

int run_process(char *const argv[], const char *log, Run *run, double timeout_s,
                const char *cwd, VarList *newvars) {
    if (argv == NULL || argv[0] == NULL || log == NULL || run == NULL) return PROC_ERROR;  // argumentos inválidos
    struct stat sb;                      // para saber dónde empieza este paso en la bitácora
    off_t desde = (stat(log, &sb) == 0) ? sb.st_size : 0;  // los ##setvariable de pasos anteriores no cuentan

    posix_spawn_file_actions_t fa;       // qué hacer con los descriptores en el hijo
    posix_spawnattr_t at;                // atributos del hijo (grupo, señales)
    if (posix_spawn_file_actions_init(&fa) != 0) return PROC_ERROR;  // inicializa acciones
    if (posix_spawnattr_init(&at) != 0) {  // inicializa atributos
        posix_spawn_file_actions_destroy(&fa);  // limpia lo ya creado
        return PROC_ERROR;               // falló
    }

    int rc = 0;                          // acumula errores de configuración
    rc |= posix_spawn_file_actions_addopen(&fa, 0, "/dev/null", O_RDONLY, 0);  // stdin = /dev/null
    rc |= posix_spawn_file_actions_addopen(&fa, 1, log, O_WRONLY | O_CREAT | O_APPEND, 0644);  // stdout -> bitácora (append)
    rc |= posix_spawn_file_actions_adddup2(&fa, 1, 2);  // stderr va al mismo lugar que stdout
    if (cwd != NULL) rc |= posix_spawn_file_actions_addchdir_np(&fa, cwd);  // el hijo arranca en el espacio de trabajo
#if defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 34))
    rc |= posix_spawn_file_actions_addclosefrom_np(&fa, 3);  // cierra sockets/archivos heredados del servidor
#endif
    sigset_t vacio, todas;               // máscara vacía y conjunto de todas las señales
    sigemptyset(&vacio);                 // el hijo no hereda señales bloqueadas
    sigfillset(&todas);                  // y todas vuelven a su acción por omisión
    rc |= posix_spawnattr_setsigmask(&at, &vacio);     // fija la máscara del hijo
    rc |= posix_spawnattr_setsigdefault(&at, &todas);  // fija las acciones por omisión
    rc |= posix_spawnattr_setpgroup(&at, 0);           // el hijo es líder de un grupo nuevo (pgid = su pid)
    rc |= posix_spawnattr_setflags(&at, POSIX_SPAWN_SETPGROUP | POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETSIGDEF);  // activa lo anterior
    if (rc != 0) {                       // algo de la configuración falló
        posix_spawn_file_actions_destroy(&fa);  // limpia
        posix_spawnattr_destroy(&at);    // limpia
        return PROC_ERROR;               // no lanzamos nada
    }

    pid_t pid;                           // pid del hijo
    int err = posix_spawnp(&pid, argv[0], &fa, &at, argv, environ);  // lanza (seguro con varios hilos, a diferencia de fork)
    posix_spawn_file_actions_destroy(&fa);  // ya no se necesitan
    posix_spawnattr_destroy(&at);        // ya no se necesitan
    if (err != 0) return PROC_ERROR;     // no se pudo lanzar (ej. cwd inexistente)

    double t0 = now_s();                 // para medir el tiempo límite
    int result = PROC_ERROR;             // resultado final
    int status = 0;                      // estado devuelto por waitpid
    for (;;) {                           // bucle de espera con revisión de cancelación y timeout
        siginfo_t si;                    // información del hijo
        si.si_pid = 0;                   // 0 = aún no terminó
        int w = waitid(P_PID, (id_t)pid, &si, WEXITED | WNOHANG | WNOWAIT);  // mira sin cosechar
        if (w < 0 && errno != EINTR) {   // el hijo desapareció o hay un error real
            result = PROC_ERROR;         // no podemos saber cómo terminó
            break;                       // evita un bucle infinito
        }
        if (w == 0 && si.si_pid == pid) {  // ya terminó (sin cosechar)
            kill(-pid, SIGKILL);         // elimina procesos que el script dejó en segundo plano
            while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}  // ahora sí cosecha
            if (WIFEXITED(status)) result = WEXITSTATUS(status);        // salió con exit(n)
            else if (WIFSIGNALED(status)) result = 128 + WTERMSIG(status);  // lo mató una señal
            else result = PROC_ERROR;    // estado inesperado
            break;                       // terminó
        }
        if (atomic_load(&run->cancel)) {  // el usuario canceló la ejecución
            matar_grupo(pid);            // mata el grupo completo
            result = PROC_CANCELED;      // avisa al Runner
            break;                       // salimos
        }
        if (timeout_s > 0 && now_s() - t0 >= timeout_s) {  // venció el tiempo
            matar_grupo(pid);            // mata el grupo completo
            result = PROC_TIMEOUT;       // avisa al Runner
            break;                       // salimos
        }
        dormir_poll();                   // espera 10 ms antes de volver a mirar
    }

    if (result >= 0 && newvars != NULL) leer_setvariable(log, desde, newvars);  // solo si terminó por sí mismo
    return result;                       // código de salida o PROC_*
}
