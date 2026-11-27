# Mediciones del análisis de las escrituras

Los datos crudos del [análisis de las escrituras del servidor](../../../../docs/proposals/2026-11-analisis-escrituras-servidor.md), del 27 de Noviembre de 2026, sobre `ede29c4` con `diagnostics/write-stages.patch` aplicado. Una VM de 4 vCPU Intel Xeon a 2,10 GHz, 15 GiB, kernel `6.18.44-fc-v880`, Rust 1.97.0; el servidor en los núcleos 0 y 1, el generador en el 2 y el 3. 3 repeticiones de 15 s tras 3 s de calentamiento, con 1, 8 y 64 pedidos en vuelo.

- `ideal.jsonl`: el modo `ideal` del generador, 4 millones de commits, sin y con la operación en la confirmación (`echo`).
- `stages.jsonl`: `run_stages.sh` con los casos `tmpfs`, `disk`, `nofsync` y `nowal`. Cada línea trae el resultado del generador y los histogramas de las etapas; `stages.py` los resume.
- `stages-extra.jsonl`: las variantes de diagnóstico `tmpfs+inline`, `nowal+inline`, `inproc-tmpfs`, `inproc-nowal` e `inproc-nowal+inline`.
- `overhead-base.jsonl` y `overhead-probes-off.jsonl`: lo que cuestan las sondas. El servidor sin el parche (tmpfs y disco) y el servidor con el parche sin grabar (`PROBES=0`, tmpfs), con el mismo generador.
- `multi-room.jsonl`: varias salas a la vez, un generador por sala, una corrida de 12 s por configuración, con el servidor sin sondas activas.
