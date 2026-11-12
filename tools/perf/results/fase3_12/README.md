# Mediciones de la Fase 3.12

Los datos crudos de las mediciones de cada lote del [plan de la Fase 3.12](../../../../docs/proposals/2026-11-fase3_12-rendimiento-servidor.md), una línea JSON por corrida, como las escriben `run_e1.sh` y `run_e2.sh`. Se resumen con `summarize.sh` y `summarize_e2.sh`. Todas, en una VM de 4 vCPU y 15 GB, con el servidor en los núcleos 0 y 1, el generador en el 2 y el 3, y el mismo binario del generador para las corridas por HTTP.

## `base-0b03a77/`: la línea de base de la fase

Sobre `0b03a77`, el commit anterior al Lote 2, medida al arrancar ese lote (1 de Noviembre de 2026), con 3 repeticiones:

- `e1-tmpfs.jsonl`: E1-tmpfs con 1, 8 y 64 pedidos en vuelo.
- `e2-tmpfs.jsonl`: E2-tmpfs con 500 y 1.000 salas (`2c-r500`, `2c-r1000`) y el diagnóstico con 4 GiB de presupuesto (`2c-budget-r1000`).
- `e1-disk.jsonl`, `e1-inproc.jsonl`, `e2-disk.jsonl` y `e2-modes.jsonl`: E1-disco, E1-actor, E2-disco y E2-modos, medidos al cerrar el Lote 2, en la misma sesión.

## `lote2/`: el presupuesto de respuestas

La base es `0b03a77`; el resultado, el código del lote. "Resultado 1" es el lote antes de que `InflightBudget::take` buscara la parte del cliente una sola vez; "resultado final", el lote tal como se entregó, salvo el tope de un frame en la reserva del registro y del esquema, que no cambia nada en estos escenarios.

- `final-*.jsonl`: la medición del lote. Tres rondas intercaladas (base, resultado final, base, resultado final…): E1-tmpfs con 1 y 8 pedidos en vuelo, y E2-tmpfs con 500 y 1.000 salas, de fábrica, y con 1.000 salas y 4 GiB de presupuesto sobre la base (`base-4gib-2c-r1000`).
- `first-*.jsonl`: la primera medición del resultado 1, sin intercalar, una hora después de la línea de base.
- `ab-e2-tmpfs-r1000.jsonl`, `ab-e1-tmpfs-*.jsonl`: corridas intercaladas del diagnóstico con 4 GiB contra el resultado 1 (E2 con 1.000 salas) y de la base contra el resultado 1 (E1 con 1 y 8), que mostraron que la diferencia de la primera medición era la máquina, que se volvió más lenta.
- `ab2-e1-tmpfs-k8-*.jsonl`: E1-tmpfs con 8 pedidos en vuelo, intercalado, base contra resultado final.
- `probe-catchup-k8-*.jsonl`: E1-tmpfs con 8 pedidos en vuelo y un generador instrumentado (una copia local, fuera del repo) que cuenta los catch-ups: `probe_acks`, `probe_catchup_ops` (las operaciones de todos los catch-ups), `probe_empty` y `probe_has_more`.

## `lote3/`: la E/S fuera de los workers

La base es `414cab3`; el resultado, el código del lote con `ZEMDB_IO_THREADS` = 8; el diagnóstico, `414cab3` con `inline-io.patch` (la E/S en el worker).

- `first-*.jsonl`: la línea de base del paso 0, al arrancar el lote: tres rondas de la base y el diagnóstico (E1-tmpfs con 1 y 8 pedidos en vuelo; E2-tmpfs y E2-disco con 500 y 1.000 salas).
- `sweep-io-threads.jsonl` y `sweep-io-threads-capped.jsonl`: el barrido de `ZEMDB_IO_THREADS` (1 a 16) en disco y en tmpfs, sin y con el tope de hilos bloqueantes del runtime, una corrida cada uno.
- `variants.jsonl`: tres formas del pool, intercaladas, en tmpfs con 1.000 salas (`result`: `spawn_blocking` con semáforos, sin el tope de hilos; `maxblock`: con el tope; `dedicated`: hilos propios).
- `final-*.jsonl`: la medición del lote, tres rondas intercaladas de la base, el resultado y el diagnóstico.
- `final-e2-r750.jsonl`: 750 salas en tmpfs y en disco, tres rondas intercaladas de la base y el resultado, medidas después de la revisión.
- `final-e1-disk-*.jsonl`: E1-disco con 1 y 8 pedidos en vuelo, tres rondas intercaladas de la base y el resultado con el valor de fábrica final (16).
- `variant-gqi-*.jsonl`: E1-disco del resultado contra la misma variante con `global_queue_interval(4)` en el runtime, tres rondas intercaladas.
- `sweep-io-threads-more.jsonl`: dos corridas más de 4 y 16 en disco con 500 y 1.000 salas, que con la primera dan tres de cada uno.
- `profile-*.json`, `.stat` y `.txt`: el perfil en tmpfs con 1.000 salas (la corrida del generador; `perf stat`, con el CPU, los cambios de contexto y las migraciones; y el perfil plano por biblioteca y símbolo).

## `lote4/`: el *group commit*

La base es `9214df3` (el Lote 3); el resultado, el código del lote; la variante de diagnóstico, el código del lote con grupos de un commit (`GROUP_MAX_COMMITS` = 1).

- `first-*.jsonl`: la línea de base del paso 0, una ronda de la base al arrancar el lote (E1-disco con 1, 8 y 64 pedidos en vuelo; E1-tmpfs con 1 y 8; E2-disco con 1.000 salas y E2-tmpfs con 500).
- `final-e1-*.jsonl` y `final-e2.jsonl`: la medición del lote, tres rondas intercaladas de la base y el resultado: E1-disco con 1, 8 y 64 pedidos en vuelo, E1-tmpfs con 1 y 8, E2-tmpfs con 500 salas y E2-disco con 750 y 1.000. Con 64 en vuelo, casi todos los commits de la base pasan de 10 ms, así que su lista `stalls` hace pesados los archivos de E1-disco.
- `final-e2-r500-more.jsonl`: cuatro rondas más del control E2-tmpfs con 500 salas, intercaladas, empezando por el resultado.
- `final-e2-r500-variant.jsonl`: el mismo control, cuatro rondas con el orden rotado de la base, el resultado y la variante.
- `profile-*.json`, `.stat` y `.txt`: el perfil del control E2-tmpfs con 500 salas, dos rondas de la base, el resultado y la variante (la corrida del generador; `perf stat`, con el CPU, los cambios de contexto y las migraciones; y el perfil plano por biblioteca y símbolo, de la base y el resultado).

## `lote4b/`: el último grupo dañado

La base es `d386522` (el Lote 4); el resultado, el código del lote (cada registro lleva la distancia al inicio de su grupo, 8 bytes más). La VM cambió desde el Lote 4 (kernel `6.18.44-fc-v230`, el disco unas dos veces más rápido), así que los números no se comparan con los de `lote4/`.

- `first-*.jsonl`: la línea de base del paso 0, una ronda de la base al arrancar el lote (E1-disco y E1-tmpfs con 1 y 8 pedidos en vuelo).
- `final-e1-*.jsonl` y `final-e2.jsonl`: la medición del lote, tres rondas intercaladas de la base y el resultado: E1-disco y E1-tmpfs con 1 y 8 pedidos en vuelo, y el control E2-tmpfs con 500 salas.
- `extra-e1-disk-*.jsonl`: tres rondas más de E1-disco con 1 pedido en vuelo, intercaladas, empezando por el resultado.

## `lote5/`: el SSE que trae las operaciones

La base es `e4300ef` (el Lote 4b, con el arreglo de un test); el resultado, el código del lote (el evento `ops`). La misma VM que `lote4b/`. El generador es el del lote, que lee el evento `ops` y sigue entendiendo `head_advanced`, y en cada medición es el mismo binario para la base y el resultado.

- `final-*.jsonl`: la medición del lote, con el código final (después de los arreglos de la revisión de delta: el evento lleva la encarnación de la sala, y el total cuenta un costo fijo por evento). Tres rondas intercaladas: el escenario del lote (`final-e2f.jsonl`, E2-modos con 1.000 salas, modo a con `--ack sync` y modo c con `--ack periodic`, que en la base, sin el evento `ops`, es el modo b) y los controles (`final-e1-*.jsonl`, E1-tmpfs con 1 y 8 pedidos en vuelo; `final-e2.jsonl`, E2-tmpfs con 500 salas).
- `first-*.jsonl`: la misma medición, antes de la revisión de delta, con su propio generador (que leía el evento sin la encarnación). La base de las dos mediciones es el mismo binario; en esta primera sesión la máquina estuvo más lenta (la base, 0,93 ms de CPU por commit en el modo c, contra 0,85 en la final).
- `profile-first-e2f-c-result.json`: la corrida del perfil de CPU del resultado de la primera medición, en modo c (40 s, con `perf` grabando 15 s; su propagación sufre el costo de `perf`).

## `lote6/`: el catch-up sin copias

La base es `2c0a908` (el Lote 5); el resultado, el código del lote (las operaciones del búfer en RAM, compartidas). Una VM como la de `lote5/` (kernel `6.18.44-fc-v233`), el 4 de Noviembre de 2026: los números se comparan solo dentro de este directorio. E1-actor (`inproc`) corre el actor y el generador en un proceso, así que cada árbol tiene su binario del generador, de la misma fuente; en las corridas por HTTP, el generador es el mismo binario para la base y el resultado (el de `2c0a908`).

- `first-e1-inproc-base.jsonl`: la línea de base del paso 0, tres repeticiones de E1-actor con 8 y 64 pedidos en vuelo sobre la base.
- `final-*.jsonl`: la medición del lote, tres rondas intercaladas de la base y el resultado: el escenario del lote (`final-e1-inproc-*.jsonl`, E1-actor con 8 y 64) y los controles (`final-e1-tmpfs-*.jsonl`, E1-tmpfs con 1 y 8; `final-e2.jsonl`, E2-tmpfs con 500 salas). `final-e2-more.jsonl`: cuatro rondas más de ese control, intercaladas, empezando por el resultado.
- `profile-*.json` y `profile-*-flat.txt`: los perfiles de E1-actor (la corrida del generador y las 60 primeras líneas del perfil plano por símbolo) de la base con 64 pedidos en vuelo y del resultado con 64 y con 8.
- `proto-*.jsonl` y `proto.py`: el prototipo de diagnóstico de DS10 (fuera del código del lote; `proto.py` lo aplica sobre una copia del árbol): `proto-both-*` con sus dos partes (la reserva de a tramos de 256 KiB y `fit_in_response` sin la segunda medición), dos rondas intercaladas con el resultado (`proto-both-e1-inproc-result.jsonl`); `proto-reservation-*` y `proto-measure-*`, cada parte sola, dos rondas intercaladas con el resultado (`proto-halves-e1-inproc-result.jsonl`).

## `lote6b/`: la reserva por tramos

La base es `cf3e374` (el Lote 6); el resultado, el código del lote (la reserva de una lectura por tramos de 256 KiB, y sin `fit_in_response`). Otra VM (4 vCPU Intel Xeon a 2,30 GHz, kernel `6.18.44-fc-v310`), el 7 y 8 de Noviembre de 2026: los números se comparan solo dentro de este directorio. Como en `lote6/`, E1-actor (`inproc`) lleva el servidor adentro del generador, así que cada árbol tiene su binario del generador, de la misma fuente; en las corridas por HTTP, el generador es el mismo binario para la base y el resultado (el de `cf3e374`).

- `first-e1-inproc-base.jsonl`: la línea de base del paso 0, tres repeticiones de E1-actor con 8 y 64 pedidos en vuelo sobre la base, el 6 de Noviembre de 2026, antes de que la VM se reiniciara (kernel `6.18.44-fc-v300`, el mismo procesador).
- `final-*.jsonl`: la medición del lote, tres rondas intercaladas de la base y el resultado: el escenario del lote (`final-e1-inproc-*.jsonl`, E1-actor con 8 y 64) y los controles (`final-e1-tmpfs-*.jsonl`, E1-tmpfs con 1 y 8; `final-e2.jsonl`, E2-tmpfs con 500 salas). `final-e1-tmpfs1-more-*.jsonl`: cuatro rondas más de E1-tmpfs con 1 pedido en vuelo, intercaladas, empezando por el resultado.
- `first-catchups-base.jsonl` y `final-catchups.jsonl`: el largo de los catch-ups de E1-actor con 8 y 64 pedidos en vuelo (cuántas operaciones trae cada commit confirmado después del calentamiento: media, p50, p90, p99, máximo, cuántos llegan a 100 y cuántos salen con `has_more`), con un generador instrumentado que no está en el repo (`catchup_patch.py` lo arma sobre una copia del árbol); en `final-catchups.jsonl`, dos repeticiones intercaladas de la base y el resultado. Sus commits por segundo no se comparan: el generador instrumentado cuenta cada respuesta.

## `lote7/`: el roster cada 10 s

E2-disco en un disco de nube emulado (`tools/perf/cloud_disk.sh`): un archivo de 3 GB con ext4, en dos loop devices apilados, con el límite de escrituras por segundo de blkio (cgroup v1) sobre el de abajo, que cuenta cada escritura y cada flush del sistema de archivos. Sin la latencia de un disco de red. El generador recibe `--disk loop1`, y los scripts, `DISK_ROOT=/mnt/zemdb-cloud/e2`. Tres VMs, todas de 4 vCPU Intel Xeon a 2,10 GHz y 15 GiB, con Rust 1.97.0 (la VM se reinició varias veces durante el lote): los números se comparan solo dentro de cada medición. En todas, el generador es el mismo binario para las dos variantes (el de `efb3ec4`).

- `decision-e2-disk.jsonl`: la medición de la decisión de DS7, sobre `efb3ec4`, el 9 y 10 de Noviembre de 2026 (kernel `6.18.44-fc-v310`): `main` y el diagnóstico que escribe el roster en el tick cada 10 s (`roster-every.patch`, que aplica sobre `efb3ec4`, con `ZEMDB_PERF_ROSTER_EVERY_SECS=10`), intercalados, sin límite y con 3.000 y 1.000 escrituras por segundo, con 500 y 1.000 salas (y 250 con 1.000). Dos rondas: la VM se reinició durante la tercera, y a la segunda le falta la última corrida.
- `first-*`: la primera medición del lote, sin la fase de cada sala, el 10 de Noviembre de 2026 (kernel `6.18.44-fc-v338`): `first-e2-disk.jsonl`, la base (`efb3ec4`) y el resultado intercalados, 3 rondas, sin límite y con 3.000 por segundo, con 500 y 1.000 salas; los controles, `first-e1-tmpfs-*.jsonl` (E1-tmpfs con 1 y 8) y `first-e2-tmpfs.jsonl` (E2-tmpfs con 500 salas). `first-probe-*`: una corrida más del resultado con 3.000 por segundo y 500 salas, con las operaciones al disco de cada segundo (`first-probe-disk-ops-per-sec.txt`, el segundo y las escrituras más los flush): la ráfaga de los rosters cada 10 s.
- `second-*`: la segunda, con la fase sola (antes de la grilla), el 11 de Noviembre de 2026 (kernel `6.18.44-fc-v378`), con los mismos escenarios y controles que `first-*`.
- `final-*`: la medición del lote sobre el código final, con la grilla, el 12 de Noviembre de 2026, en la misma VM que `second-*`, con los mismos escenarios y controles.
- `phase-e2-disk.jsonl`: lo que cuesta la fase, en la misma VM que `second-*`, el 11 de Noviembre de 2026, sobre la versión con la fase sola: la base, el resultado sin la fase (la primera escritura de cada sala en su primer tick con un cambio) y el resultado, intercalados, 3 rondas rotando el orden, sin límite, con 500 y 1.000 salas.
