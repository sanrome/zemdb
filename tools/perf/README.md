# Arnés de medición

El generador de carga y los scripts de las [evaluaciones de performance](../../docs/proposals/2026-10-evaluaciones-performance.md), que la [Fase 3.12](../../docs/proposals/2026-11-fase3_12-rendimiento-servidor.md) usa para medir cada lote antes y después (DS8). Es un workspace propio, fuera del de la raíz (`exclude` en el `Cargo.toml` de la raíz): no lo compilan la CI ni `cargo test --workspace`.

## Qué hay

| | |
|---|---|
| `src/main.rs` | El generador. Modos `http` (una sala por HTTP contra el binario del servidor, con *k* pedidos en vuelo) e `inproc` (la misma carga directo al actor, sin HTTP), de la Evaluación 1. |
| `src/rooms.rs` | El modo `rooms`, de la Evaluación 2: muchas salas, con clientes que escriben a ritmo humano y se ponen al día como el SDK (SSE, `/sync`, `Ack`, heartbeat). |
| `run_e1.sh`, `profile_e1.sh`, `summarize.sh` | Una medición de la Evaluación 1 (una línea JSON por corrida), su perfil de CPU con `perf`, y la mediana de las repeticiones. |
| `run_e2.sh`, `e2_all.sh`, `e2f_all.sh`, `summarize_e2.sh` | Una configuración de la Evaluación 2 contra un servidor nuevo, todas las mediciones de §4.4, las de los modos de frescura (§4.8), y la mediana de las repeticiones. |
| `cloud_disk.sh` | Un disco de nube emulado, con un límite de escrituras por segundo, para E2-disco (Lote 7 de la Fase 3.12): ver abajo. |
| `results/` | Los datos crudos de las evaluaciones. En `e1-block-in-place-*.jsonl` y `e1-mediciones.jsonl`, la lista de commits lentos de cada corrida está resumida: cuántos hubo y los 20 más largos. En `results/fase3_12/`, los de la línea de base de la Fase 3.12 y los de cada lote. |
| `diagnostics/` | Los cambios de diagnóstico del servidor, como parches (DP1): no están en el código de `main`. |

## Preparar la máquina

- **Linux**, con `taskset` y `jq`. Las evaluaciones usaron 4 núcleos: el servidor en 0 y 1, el generador en 2 y 3 (`SERVER_CPUS` y `GEN_CPUS`).
- **El rango de puertos locales,** para las decenas de miles de conexiones de la Evaluación 2: `echo "1024 65000" > /proc/sys/net/ipv4/ip_local_port_range` (como root).
- **El límite de archivos abiertos por proceso** (`ulimit -n`) acota cuántos streams SSE se pueden abrir: uno por sala y uno por stream. Con 20.000, alcanza para unas 1.750 salas de 10 clientes.
- **`perf`,** para los perfiles: en Ubuntu, `apt-get install linux-tools-generic`; `profile_e1.sh` lo busca en `PERF` o en el camino de la versión 6.8.
- **tmpfs** en `/dev/shm`, para medir sin el costo del disco; y un directorio en el disco real (`DISK_ROOT`).
- **Un disco de nube emulado,** cuando la máquina no tiene uno con un límite de operaciones por segundo (como root, con cgroup v1): `tools/perf/cloud_disk.sh setup IMG /mnt/zemdb-cloud` arma un archivo `IMG` con ext4 en dos loop devices apilados y lo monta; `cloud_disk.sh limit IMG 3000` pone el límite de escrituras por segundo (0 lo saca), que cuenta cada escritura y cada flush del sistema de archivos, como un disco; y las corridas van con `DISK_ROOT=/mnt/zemdb-cloud/e2` y `--disk $(tools/perf/cloud_disk.sh disk IMG)`. No emula la latencia de un disco de red, y un reinicio de la máquina pierde los loop devices.

## Compilar

```bash
cargo build --release -p zemdb-server        # desde la raíz del repo: el servidor que se mide
(cd tools/perf && cargo build --release)     # el generador
```

Los scripts toman el servidor de `target/release/` de la raíz y el generador de `tools/perf/target/release/`. Los dos se compilan con información de depuración (`CARGO_PROFILE_RELEASE_DEBUG=true` para el servidor), que no cambia el código generado y le da nombres a los perfiles.

## Los escenarios de la Fase 3.12

Los de §5.2 de su plan, con las condiciones de DP3 (3 repeticiones; en las corridas largas, 5 s de calentamiento y 30 s medidos en la Evaluación 1, 15 y 60 s en la 2).

| Escenario | Comando |
|---|---|
| E1-tmpfs | `KS="1 8 64" tools/perf/run_e1.sh tmpfs salida.jsonl` |
| E1-disco | `KS="1 8 64" tools/perf/run_e1.sh disk salida.jsonl` |
| E1-actor | `KS="1 8 64" tools/perf/run_e1.sh inproc salida.jsonl` |
| E2-tmpfs | `REPS=3 tools/perf/run_e2.sh tmpfs salida.jsonl 2c-r1000 --rooms 1000 --clients 10 --period 10 --sse 1 --warmup 15 --secs 60` |
| E2-disco | lo mismo con `disk` en lugar de `tmpfs` |
| E2-modos | `tools/perf/e2f_all.sh salida.jsonl` (o una línea de ese script) |

El resumen: `tools/perf/summarize.sh salida.jsonl` para la Evaluación 1, y `tools/perf/summarize_e2.sh salida.jsonl` para la 2; la última columna dice si todas las repeticiones son válidas. Una corrida corta para probar que todo anda: `KS=1 REPS=1 WARMUP=1 SECS=2 tools/perf/run_e1.sh tmpfs /tmp/prueba.jsonl`.

## Los diagnósticos

Se aplican sobre una copia local, se compila el servidor, se mide, y se descartan (`git checkout crates/server`):

| | Qué hace | Cómo se activa |
|---|---|---|
| `diagnostics/inline-io.patch` | Corre la E/S de los actores en el mismo hilo, el worker (§3.7.4 de las evaluaciones): hasta el Lote 2 de la Fase 3.12, sin `block_in_place`; desde el Lote 3, sin el pool de E/S | `ZEMDB_PERF_INLINE_IO=1` al correr los scripts |
| (sin parche) | El presupuesto de respuestas agrandado | `ZEMDB_MAX_INFLIGHT_RESPONSE_BYTES=4294967296` |

`git apply tools/perf/diagnostics/inline-io.patch` desde la raíz. Los parches se escribieron sobre el `main` de la Fase 3.12; un lote que toque esos archivos puede hacer que dejen de aplicar.
