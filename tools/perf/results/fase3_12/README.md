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
