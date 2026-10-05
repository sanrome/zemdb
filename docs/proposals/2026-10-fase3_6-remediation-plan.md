# Plan de Remediación — Fase 3.6

**Fecha:** 2 de Octubre de 2026
**Reemplaza a:** [`docs/audits/2026-09-fase3_5-audit.md`](../audits/2026-09-fase3_5-audit.md) y [`docs/proposals/2026-09-fase3_5-proposal.md`](2026-09-fase3_5-proposal.md)
**Tipo:** Documento vivo (auditoría verificada + plan + seguimiento en un solo lugar)

---

## 0. Cómo usar este documento

- Cada defecto de la auditoría anterior fue **verificado contra el código** (no contra su descripción). Se conservan los IDs `DEF-xx` originales para mantener la trazabilidad; los defectos nuevos encontrados en la revisión arrancan en `DEF-58`.
- Los defectos están agrupados en **lotes** ordenados por prioridad de ejecución. Cada lote es una unidad de trabajo revisable (uno o pocos commits).
- Cada ítem tiene: severidad real, estado, problema verificado, solución y test exigido.
- Al cerrar un ítem se actualiza su estado aquí y se anota el commit.

**Estados:** ⬜ Pendiente · 🟡 Parcial · ✅ Hecho · ⏸ Diferido (Fase 4) · ❌ Descartado (falso o sin valor)

**Reglas de trabajo** (ver `GEMINI.md`):
1. Tests según la estructura de dos niveles de `GEMINI.md` (unitarios en `src/.../<modulo>/tests.rs`, integración en `tests/`), con al menos un test negativo que reproduzca el fallo y que falle sin el fix.
2. Sin códigos de auditoría (`DEF-xx`) en código, tests ni docstrings.
3. Cada lote cierra con `cargo test --workspace` y `cargo clippy --workspace --all-targets -- -D warnings` en verde.

---

## 1. Hechos de contexto que condicionan las severidades

1. **`zemdb-server` no depende de `zemdb-storage`.** El servidor tiene su propio log (`server/src/log/`). Los defectos de storage solo afectan a futuros consumidores (cliente).
2. **`zemdb-client` es un stub** (`add()`). Todo lo que depende de un cliente real se difiere a Fase 4.
3. **`WalReader::next_record` y `decode_wal_record_from_slice` solo los usan tests.** La recuperación real usa `replay_wal_file` (`storage/src/disk/recovery.rs`).
4. **La poda por TTL/cuota del log del servidor ignora cursores de clientes.** Los escenarios de "agotamiento de disco" por retención anclada quedan acotados por esa poda.
5. **El actor de sala es single-threaded** (`&mut self` + `select!`): no hay TOCTOU entre chequeos y escrituras dentro de un mismo comando.

---

## 2. Decisiones de diseño

| ID | Decisión | Estado |
|---|---|---|
| **D1** | Commit de un cliente cuyo `last_ack_seq` está por debajo de la retención: **rechazar antes de secuenciar** (`BehindCompaction`, sin efectos) vs. **aceptar y devolver `CommitAck` con `needs_snapshot: true`**. El rechazo cumple la regla 1 ("validar primero"); aceptar es más amigable para local-first (un cliente offline mucho tiempo puede vaciar su outbox sin esperar un snapshot). | **Decidida: rechazar.** Podría volverse configurable en el futuro. |
| **D2** | Un commit ya durable (`append` exitoso) **nunca** responde `Err`. Si no se puede armar el catch-up, responde `CommitAck` con `catchup_ops: []` y `has_more: true`; el siguiente `/sync` reporta el error real. No viola la regla 4 porque `has_more: true` no oculta nada: obliga al cliente a pedir más. | Adoptada |
| **D3** | `is_compacting` pasa a `AtomicBool` fuera del `RwLock` de la sala, con guard RAII cuyo `Drop` lo resetea (regla 2). Un `Drop` no puede hacer `.await` sobre un lock de Tokio, por eso el flag sale del lock. | Adoptada |
| **D4** | Group commit / micro-batching en el actor: **descartado** para v0.1. Con 2–50 clientes por sala la cola casi nunca tiene más de un commit. | Adoptada |
| **D5** | Metadatos corruptos al abrir: el roster de clientes (`meta_clients_*.json`) se trata como recuperable (arranca vacío con un warning; los clientes se vuelven a registrar y la poda proactiva queda detenida hasta entonces, que es la dirección segura). `meta_room.json` y los esquemas corruptos hacen fallar la apertura. | Adoptada |
| **D6** | Cursores de clientes: se actualizan en memoria con un flag de "sucio" y se persisten (escritura atómica) en el tick de mantenimiento; registro, desregistro y apagado se persisten en el momento. Un crash pierde ≤500 ms de avance de cursores. Persistir en cada ack/commit sumaría dos fsync por operación. **Corrección tras la revisión del Lote 3:** como la poda usa el cursor en memoria, antes de cualquier poda que vaya a borrar un segmento se persiste el roster (misma regla "registrar antes de borrar" que `log_meta.json`); así el cursor persistido nunca queda por debajo de lo podado. | Adoptada |
| **D7** | Falla de escritura o `fsync` en un commit del servidor: el actor responde error interno (reintentable), lo registra y **termina**; el `RoomManager` relanza la sala en el siguiente request y la recupera desde disco. Si el registro llegó al disco, la deduplicación lo reconoce y un reintento con el mismo `mutation_id` recibe su `CommitAck`. | Adoptada |
| **D8** | Apagado ordenado: SIGINT y SIGTERM; dejar de aceptar conexiones, cortar los streams SSE (si no, el apagado esperaría para siempre), esperar los requests en curso, `shutdown_all` con guardado del roster, y timeout total de 10 s tras el cual se fuerza la salida. | Adoptada |

---

## 3. Resumen

| Lote | Tema | Ítems | Prioridad | Progreso |
|---|---|---|---|---|
| 1 | Consistencia del log y del commit | DEF-01, 36, 07, 58, 59, 67, 04, 29, 06 | Inmediata | ✅ 9/9 |
| 2 | Durabilidad del motor de storage | DEF-02, 03, 65, 19(storage), 12 | Inmediata | ✅ 5/5 |
| 3 | Durabilidad de metadatos del servidor | DEF-60, 19(server), 16, 48, 68, 69 | Alta | ✅ 6/6 |
| 4 | Autenticación e identificadores | DEF-61, 41, 66 | Alta | 0/3 |
| 5 | Relay de snapshots | DEF-62, 10, 63, 52, 51, 39, 40, 38, 31(relay), 08(relay), 15 | Alta | 0/11 |
| 6 | Ciclo de vida de clientes y señalización | DEF-53, 05, 27, 75, 76 (24 y 26 descartados) | Alta | 0/5 |
| 7 | Protocolo wire y errores HTTP | DEF-46, 28, 11, 43, 64, 22, 71, 74 | Media | 0/8 |
| 8 | Rendimiento, portabilidad y ciclo de vida de salas | DEF-50, 35, 55, 37, 08, 31(locks), 25(server), 72, 77, 79 | Media | 0/10 |
| 9 | Robustez e higiene de storage y core | DEF-14, 23, 49, 17, 25(storage), 18, 09, 45, 33, 44, 47, 30, 13, 20, 21, 57, 70, 78 (32 descartado) | Media/Baja | 1/18 |
| 10 | Diferido a Fase 4 / descartado | DEF-34, 54, 42, 56, 73 | — | — |

**Avance total:** 21 de 75 ítems activos resueltos. Al cerrar cada ítem se actualiza su estado, su commit y esta tabla.

**Severidades corregidas respecto de la auditoría anterior:** de los 7 "críticos" originales, solo DEF-01 lo es. DEF-02, 03 y 04 son Altos; DEF-05 y 06 son Medios; DEF-34 es Bajo. DEF-24, 26 y 56 son falsos en la práctica.

---

## Reestructuración de tests (transversal)

> Objetivo: aplicar la estructura de dos niveles de `GEMINI.md` (unitarios en `src/.../<modulo>/tests.rs`, integración en `tests/`) y cerrar la visibilidad de los internals que hoy son `pub` solo porque los tests los usan.

**Estado actual** (19 archivos, ~170 tests, todos en `tests/`):
- Varios tests de integración del servidor importan internals directamente (`zemdb_server::log::`, `actor::lease`, `actor::command`, `dedup`, `relay`, `api::router`, etc.), y por eso todos los módulos del servidor son `pub mod`.
- `server_integration_tests.rs` (1844 líneas) y `room_actor_tests.rs` (999) mezclan temas distintos.

**Estrategia: incremental, no un big-bang.**
1. **Tests nuevos** del plan: los que verifican invariantes internos van directamente como unitarios (por ejemplo `compute_tail_seq`, `HotBuffer::get_range`, los estados de la compactación y los puntos de fallo).
2. **Al tocar un módulo** en un lote, sus tests existentes que dependen de internals se mueven a `src/.../<modulo>/tests.rs`, y el módulo pasa a `pub(crate)` si ya nadie fuera del crate lo usa.
3. **Al final de la fase**, en un paso dedicado: revisar lo que quede, dividir los archivos grandes de integración por tema (sync, commit, onboarding, relay, auth) y ajustar las visibilidades restantes.

**Clasificación tentativa** (se confirma al tocar cada módulo):

| Archivo actual | Destino |
|---|---|
| `server/tests/tiered_log_tests.rs`, `unified_wal_tests.rs` | Unitarios en `server/src/log/` |
| Partes de `room_actor_tests.rs` que usan `RoomCommand` y el lease tracker | Unitarios en `server/src/actor/` |
| `server_integration_tests.rs`, `room_lifecycle_resilience_tests.rs`, `snapshot_relay_tests.rs`, `auth_tests.rs` | Integración (HTTP / API pública); dividir por tema |
| `storage/tests/*` | Mayormente integración (usan la API de `StorageEngine` y archivos en disco). `format::` pasa a unitario |
| `core/tests/*` | Integración (API pública de core); `wal_frame_tests` se revisa con DEF-25 |
| `client/tests/client_tests.rs` | Se reemplaza en Fase 4 |

---

## Lote 1 — Consistencia del log y del commit (servidor)

> Objetivo: que `/sync` y `/commit` nunca entreguen huecos de secuencia ni rechacen ilegítimamente a clientes cuyos deltas existen en disco.
> Orden interno obligatorio: DEF-07 y DEF-58 antes de dar por cerrado DEF-01.

#### DEF-01 · Crítico · ✅ Hecho (eb8baa6 + 8a0d36b + este lote)
**Problema.** Cuando el `HotBuffer` desalojaba ops por TTL, `fetch_deltas` servía desde RAM saltando lo que estaba en `active.wal`, entregando huecos de secuencia. El disparador realista es el TTL (5 min), no la cuota: rotación y cuota usan el mismo umbral.
**Hecho en eb8baa6.** `get_range` rechaza cursores anteriores a la RAM; los tiers se leen en orden cronológico (cold → sealed → active → RAM); los límites entre tiers y los solapes están bien manejados.
**Falta.**
1. ~~DEF-07: la guarda de retención corta con `BehindCompaction` antes de llegar al puente con `active.wal`.~~ Resuelto junto con DEF-07; cubierto por `fetch_after_ram_ttl_eviction_bridges_from_active_wal`.
2. ~~DEF-29: `handle_commit` convierte el nuevo error de discontinuidad en un hueco (`vec![seq_op]`).~~ Resuelto con DEF-29.
3. ~~Un hueco real detectado en la lectura debe mapearse a `BehindCompaction`, no a un error interno de WAL.~~ Hecho (`sequence_gap`, test `gap_inside_retained_range_reports_behind_compaction`).
4. ~~El test `test_tiered_log_eviction_gap_bridged_from_active_wal` no ejercita lo que dice.~~ Renombrado a `..._from_sealed_segments` con comentarios corregidos; el caso de `active.wal` lo cubre el test unitario nuevo.
5. Pendiente menor (Lote 8): en la ruta lenta de `fetch_deltas`, el tier de RAM quedó prácticamente sin uso, porque `active.wal` ya cubre todo lo que está en RAM.
**Test.** Desalojo por TTL sin rotación → `run_maintenance_sync` → `fetch_deltas` desde antes de la RAM devuelve la secuencia contigua desde `active.wal`.

#### DEF-36 · Bajo · ✅ Hecho (eb8baa6)
Off-by-one del fast path (`from_seq + 1 >= min_ram`). Solo costaba rendimiento.

#### DEF-07 · Alto · ✅ Hecho (8a0d36b)
**Problema.** `prune_and_update_tail` y `prune_older_than` (código duplicado) calculan `tail_seq` como cold → sealed → `min_ram`, salteando `active.wal`. Si la RAM desalojó por TTL, `tail_seq` salta y se rechaza con `BehindCompaction` a clientes cuyos deltas están en disco.
**Solución.** Una única función `compute_tail_seq()` usada por ambas rutas: `primer cold.start` → `primer sealed.start` → `active_start_seq` → `head + 1` (ver DEF-58). Sin `.min(min_ram)`: si no hay segmentos sellados, toda op en RAM está también en `active.wal`, así que `active_start` ya es el mínimo.
**Test.** Desalojo TTL + mantenimiento → `tail_seq == active_start` y un cliente con cursor en `active.wal` sincroniza sin `BehindCompaction`.

#### DEF-58 · Alto · ✅ Hecho (8a0d36b) (nuevo)
**Problema.** Con el log vacío (todo podado), `tail_seq = head_seq`. La guarda es `from < tail - 1`, así que un cliente en `head - 1` pasa, recibe una respuesta vacía con `has_more = false` y **nunca recibe la op `head`**. Alcanzable tras poda por TTL/cuota del cold tier o una vez aplicado DEF-37.
**Solución.** Con el log vacío, `tail_seq = head + 1` (incluido en `compute_tail_seq`).
**Test.** Podar todo → cliente en `head - 1` recibe `BehindCompaction`; cliente en `head` recibe vacío.

#### DEF-59 · Medio · ✅ Hecho (8a0d36b) (nuevo)
**Problema.** Si `active.wal` existe pero está vacío al abrir la sala (crash, o torn write truncado a 0), `inspect_active_segment` fija `active_file` y `append_record` (`warm_disk.rs`) nunca fija `active_start_seq`. Consecuencias: `active_ops_count()` queda en 0, el archivo nunca rota, y DEF-07 deja de funcionar.
**Solución.** `append_record` fija `active_start_seq` cuando el segmento activo no tiene ops, independientemente de si el archivo ya estaba abierto.
**Test.** Abrir sala con `active.wal` de 0 bytes → appends → `active_start_seq` correcto y rotación al llegar a la cuota.

#### DEF-67 · Alto · ✅ Hecho (nuevo)
**Problema.** `TieredLog::open_or_create` reconstruye `head_seq` solo a partir de los segmentos en disco. La poda por TTL/cuota del cold tier puede borrar **todos** los segmentos (por ejemplo, en una sala inactiva después de la rotación). Al reiniciar, `head_seq` vuelve a 0 y el servidor **reutiliza números de secuencia** ya entregados a los clientes. La poda por cursores (`prune_older_than`) no llega a esto porque nunca borra el segmento que contiene `head`, pero la poda por TTL/cuota sí.
**Solución aplicada.** `log_meta.json` en el directorio de la sala guarda `pruned_through_seq`, escrito con `durable::write_atomic` **antes** de borrar segmentos en ambas rutas de poda. Al abrir, `head = max(head de los segmentos, pruned_through_seq)`. Un `log_meta.json` ilegible hace fallar la apertura en lugar de reiniciar la secuencia.
**Test.** Podar todo por TTL → reabrir → `head_seq` se conserva y el siguiente append usa `head + 1`.

#### DEF-04 · Alto · ✅ Hecho
**Problema.** `handle_commit` secuencia, persiste, avanza `head`, registra dedup y emite SSE; recién después llama a `fetch_deltas`, que puede fallar con `BehindCompaction`. El cliente cree que su mutación fue rechazada cuando en realidad quedó confirmada y replicada.
**Error de la propuesta anterior.** Ponía el chequeo "al inicio del método", **antes** de la deduplicación: un reintento de una mutación ya confirmada recibiría `BehindCompaction`, reproduciendo el mismo split-brain; y si la LRU la desaloja, el reintento posterior se aplicaría dos veces. El gate actual de `Dormant`/`Bootstrapping` tiene el mismo problema de orden.
**Solución.** Orden en `handle_commit`: (1) dedup → si es duplicado, rama de DEF-06; (2) chequeo de retención `last_ack_seq >= tail - 1` (según D1: rechazar sin efectos, o marcar `needs_snapshot`); (3) secuenciar y persistir; (4) catch-up según D2.
**Test.** Cliente detrás de la retención hace commit → no se secuencia nada, `head` no cambia, no hay evento SSE (o, con D1 = aceptar, `CommitAck` con `needs_snapshot`). Reintento de una mutación ya confirmada desde un cliente ahora rezagado → `CommitAck` con el `assigned_seq` original.

#### DEF-29 · Medio · ✅ Hecho
**Problema.** Si `fetch_deltas` falla después del append, el actor responde `(vec![seq_op], false)`, saltando las ops intermedias: hueco de secuencia en el cliente.
**Error de la propuesta anterior.** "Propagar el error": la mutación ya es durable, así que responder `Err` reintroduce DEF-04.
**Solución.** D2: `CommitAck` con `assigned_seq`, `catchup_ops: []`, `has_more: true`.
**Test.** Forzar fallo de `fetch_deltas` post-append → `CommitAck` con `has_more: true` y sin huecos.

#### DEF-06 · Medio · ✅ Hecho
**Problema verificado.** La afirmación principal de la auditoría es falsa: si `last_ack_seq >= existing_seq`, el cliente ya tiene su op y no hace falta reenviarla. Los problemas reales son dos: (a) `from_seq = existing_seq` reenvía ops que el cliente ya tiene cuando `existing_seq < last_ack_seq`; (b) `unwrap_or_default()` traga `BehindCompaction` y responde vacío con `has_more: false`, así que el cliente cree estar al día y se salta el hueco.
**Solución.** En la rama de duplicado, usar siempre `from_seq = last_ack_seq` (igual que la ruta normal). Eliminar `unwrap_or_default()`; ante error, aplicar D2 (la mutación ya está confirmada).
**Test.** Reintento con `last_ack_seq` antiguo → catch-up contiguo que incluye la op original. Reintento con cursor fuera de retención → `CommitAck` con `has_more: true`, nunca `has_more: false` vacío.

---

## Lote 2 — Durabilidad del motor de storage

> Objetivo: ninguna secuencia de fallos (incluidos dos seguidos) pierde mutaciones confirmadas.

#### DEF-02 · Alto · ✅ Hecho
**Problema verificado.** (a) Tras un fallo en la fase 2 de `compact_room_cow`, `.wal.compacting` queda huérfano; la próxima compactación hace `rename(.wal → .wal.compacting)` y lo pisa. Si esa segunda compactación también falla (disco lleno es persistente) o hay un crash antes de su fase 3, se pierde todo lo que había entre el snapshot viejo y el primer corte. Un solo fallo seguido de reinicio no pierde nada (la recuperación fusiona el huérfano). (b) Cualquier `?` después de `is_compacting = true` (líneas del rename, de la creación del nuevo WAL, del join y del rename del snapshot) deja el flag trabado y `compact_room` devuelve `Ok` para siempre sin compactar. (c) Si falla la creación del nuevo `.wal` después del rename, `room.wal_file` sigue escribiendo en el archivo `.compacting`.
**Solución.**
1. D3: `is_compacting` como `AtomicBool` + guard RAII.
2. Fase 1: si `.compacting` ya existe, **anexar** el `.wal` actual al `.compacting`, `sync_all`, y recién después truncar `.wal` (no hacer rename). Un crash en medio solo duplica registros, que DEF-65 tolera.
3. Si falla algo después del rename, revertirlo antes de devolver el error.
**Descartado de la propuesta anterior:** nombres generacionales y el "rollback-merge" de la fase 2 (que en sí mismo no era atómico).
**Test.** Inyectar fallo en fase 2 dos veces seguidas → reabrir → todas las mutaciones presentes. Fallo con `?` en cada punto → el flag queda en `false` y la siguiente compactación corre.
**Aplicado.** Además de lo anterior, tras la revisión independiente: el WAL se lee por su propio handle bloqueado (en Windows un segundo handle no puede leer un archivo bloqueado); si el anexo al `.compacting` falla a medias, se trunca de vuelta a su largo previo; si el rollback de la rotación falla, la sala queda marcada como fallida (`StorageError::RoomFailed`) hasta reabrirla. Puntos de fallo cubiertos por tests: `phase2`, `rotate_open`, `rotate_rollback`, `absorb`, `absorb_write`. No cubiertos: error de join del worker, rename del snapshot en fase 3 y errores de limpieza (cubiertos por la recuperación, pero sin test dedicado).

#### DEF-03 · Alto · ✅ Hecho
**Problema verificado.** En `recover_room`, `remove_file(.wal.compacting)` (`recovery.rs:382`) se ejecuta **antes** de la reescritura in-place del WAL activo (`:411`). Matar el proceso en esa ventana pierde el contenido de `.compacting`; una reescritura cortada deja bytes desalineados (corrupción o truncado de registros confirmados). Requiere un crash durante la recuperación de otro crash: Alto, no Crítico.
**Errores de la propuesta anterior (tmp → rename → delete).** (a) El `wal_file` abierto y bloqueado (`flock`) seguiría apuntando al inodo viejo, ya desvinculado: las escrituras posteriores se perderían en silencio. (b) Un crash entre el rename y el borrado de `.compacting` duplica registros en el siguiente replay.
**Solución aplicada.** Si `.compacting` tenía registros vivos: reconstruir el estado en memoria (snapshot + `.compacting` + `.wal`), escribir un **snapshot nuevo** con `write_snapshot_file` (tmp → `sync_all` → rename → `sync_dir`), luego truncar `.wal`, y recién entonces borrar `.compacting` y hacer `sync_dir`. Nunca se reescribe in-place y nunca se cambia el handle bloqueado. Cualquier crash intermedio queda cubierto porque el replay saltea los registros `<= head_seq` (DEF-65).
**Test.** Dejar los archivos en cada estado intermedio posible (simulando crash en cada paso) → reabrir → estado completo y sin duplicados.

#### DEF-65 · Medio · ✅ Hecho (nuevo)
**Problema.** El replay solo descarta `seq <= snapshot_seq`; no descarta registros ya aplicados en el mismo replay. Cualquier duplicación por crash (DEF-02 y DEF-03) agranda el WAL y depende de que las operaciones sean idempotentes.
**Solución.** En `replay_wal_file`, descartar también `op.seq <= head_seq` actual. Es seguro porque `apply_batch` exige contigüidad. Además, un registro que no continúa la secuencia (`head_seq + 1`) es un hueco y la recuperación falla con `WalCorruption` (regla 4).
**Test.** WAL con un rango duplicado → replay produce el estado correcto y `head_seq` correcto.

#### DEF-19 (storage) · Medio · ✅ Hecho
**Problema.** `compactor.rs` ignora el resultado de `sync_dir` y del borrado del `.compacting`. La auditoría no vio que el rename de la fase 1 y la creación del nuevo `.wal` tampoco tienen `sync_dir`, así que la entrada de directorio del nuevo WAL puede no ser durable.
**Solución.** `sync_dir` tras el rename y la creación del WAL en la fase 1; propagar errores. Si `sync_dir` falla después de un rename exitoso, actualizar `snapshot_seq` y liberar el flag antes de devolver el error.

#### DEF-12 · Bajo · ✅ Hecho
**Problema verificado.** `apply_batch` con `ops` vacío escribe un frame de `ops_count = 0`. La afirmación de que "corrompe la recuperación" es **falsa**: `replay_wal_file` lo acepta. Solo `WalReader` (usado en tests) lo rechaza.
**Solución.** Retorno anticipado en `DiskStorageEngine::apply_batch` si `ops.is_empty()`. En `MemoryStorageEngine` un lote vacío ya no tenía efectos (no hay WAL).

---

## Lote 3 — Durabilidad de metadatos del servidor

#### DEF-60 · Alto · ✅ Hecho (nuevo)
**Problema.** `meta_room.json` se escribe in-place con `fs::write` (`actor/manager.rs:111`), sin tmp ni rename. Un crash a mitad de la escritura deja un archivo truncado y la sala no vuelve a cargar.
**Solución.** Usar `durable::write_atomic` (ya creado en `server/src/durable.rs` para DEF-67) para meta de sala, esquemas y roster en register/deregister.
**Test.** Archivo `.tmp` residual o destino truncado → la sala carga la última versión válida.

#### DEF-19 (server) · Medio · ✅ Hecho
**Problema.** Leases y esquemas usan tmp + rename, pero sin `fsync` ni `sync_dir`. Un roster corrupto hoy hace fallar la carga (`lease.rs:55`).
**Solución.** Usar `write_atomic`. Un roster ilegible se trata como recuperable: arrancar vacío y loguear un warning (los clientes se vuelven a registrar).

#### DEF-16 · Medio · ✅ Hecho
**Problema.** `handle_commit` no registra `last_ack_seq` en el lease tracker (contradice ARCHITECTURE.md:228), así que los escritores continuos no avanzan el suelo de retención. El impacto de "agotar el disco" está exagerado (la poda por TTL/cuota sigue funcionando).
**Solución.** `record_ack(last_ack_seq)` en el commit (ya es monótono: `if ack_seq > entry.last_ack_seq`). Hoy `record_ack` reescribe todo el roster en JSON en cada llamada: pasar a cursor en memoria + flag dirty, persistido en el tick; `write_atomic` solo en register/deregister.
**Test.** Cliente que solo hace commits → el suelo de retención avanza y se podan segmentos.

#### DEF-48 · Bajo · ✅ Hecho
**Problema.** Sin apagado ordenado. La durabilidad de commits no está en riesgo (hay `sync_data` antes del ack), pero `RoomCommand::Shutdown` solo corta el loop sin guardar leases, y `ctrl_c()` no captura SIGTERM.
**Solución.** `axum::serve(...).with_graceful_shutdown(SIGINT | SIGTERM)` → `shutdown_all()`; el handler de `Shutdown` hace `lease_tracker.save()` (con `write_atomic`).
**Aplicado** según D8, en `api/serve.rs` y `main.rs`. En Windows escucha `ctrl_c`, `ctrl_close` y `ctrl_shutdown`; **ese código no se compiló todavía** (no hay target de Windows instalado), se verifica con DEF-73.

#### DEF-68 · Alto · ✅ Hecho (nuevo)
**Problema.** Si en un commit el `write` al WAL sale bien pero falla el `fsync`, hoy se responde error y la sala sigue operando. Después de un `fsync` fallido el estado del disco es incierto: el kernel puede descartar las páginas sucias, y un `fsync` posterior "exitoso" no garantiza que esos bytes lleguen al disco ("fsyncgate"). Además, el registro escrito puede quedar en el archivo y reaparecer al reiniciar como una op confirmada que el cliente cree rechazada.
**Solución.** Regla 1 de `GEMINI.md`: ante una falla de I/O a mitad de una escritura durable, la sala se marca como inválida (rechaza comandos con un error de servicio no disponible), el actor termina, y la próxima apertura recupera desde disco. No se reintenta el `fsync`.
**Test.** Un punto de fallo `#[cfg(test)]` en el `sync_data` del WAL → el commit falla, los comandos siguientes son rechazados, y al reabrir la sala el estado sale del disco.
**Hecho en storage.** `DiskStorageEngine::apply_batch` marca la sala como fallida (`StorageError::RoomFailed`) si falla la escritura o el `fsync` del WAL; escrituras y compactaciones se rechazan hasta reabrir. Lo mismo si `apply_snapshot` no puede persistir el estado ya reemplazado en memoria. **Hecho en el servidor** según D7: si falla la escritura o el `fsync`, el actor responde error reintentable y termina; el `RoomManager` lo relanza (esperando de forma cancel-safe a que el actor anterior libere sus archivos) y la sala se recupera desde disco. Si lo que falla es la rotación de segmento con el registro ya durable, el commit se confirma igual y la sala se reinicia (D2).

#### DEF-69 · Medio · ✅ Hecho (nuevo, revisión del Lote 2)
**Problema.** En el servidor, `WarmDiskLog::rotate_active_segment` renombra `active.wal` y `ColdDiskLog::compress_warm_segment_sync` renombra el `.tmp` a cold y borra el warm, ambos sin `sync_dir`. Tras un corte de luz, el borrado puede persistir y el rename no, dejando un hueco entre segmentos. Ya no hay pérdida silenciosa (la lectura devuelve `BehindCompaction`), pero sí datos perdidos.
**Solución.** `durable::sync_dir` después de cada rename y antes de borrar el segmento warm.

---

## Lote 4 — Autenticación e identificadores

#### DEF-61 · Alto · ⬜ (nuevo)
**Problema.** `RoomId::new` (`core/src/id.rs`) no valida nada y los IDs de sala terminan en rutas de archivo (`rooms/{id}`, `{id}_{seq}.snap.zst`). Un ID con `/` o `..` permite path traversal.
**Solución.** Constructor validado: `RoomId` con charset `[A-Za-z0-9_-]` y longitud 1..=64. Para `ClientId`, verificar si llega a rutas; como mínimo, prohibir `/`, `\`, `..` y caracteres de control, y acotar la longitud. Validar también en la deserialización.
**Test.** IDs con `../`, `/`, vacíos o demasiado largos son rechazados en la API y en la deserialización.

#### DEF-41 · Medio · ⬜
**Problema verificado.** El token es `client.room.exp.sig` y `split('.')` exige 4 partes: los IDs con puntos nunca validan (problema de disponibilidad).
**Error de la propuesta anterior (`rsplitn(3, '.')`).** Sigue sin poder separar cliente y sala, y además la firma cubre el string unido: el token de (cliente `a.b`, sala `c`) es idéntico byte a byte al de (cliente `a`, sala `b.c`). Cualquier regla fija de partición convierte esto en **confusión de autorización entre salas**.
**Solución.** Token `base64url(client).base64url(room).exp.sig` (el alfabeto base64url no contiene `.`), con la firma calculada sobre una codificación con separación de dominio y longitudes prefijadas (p. ej. `"zemdb-client-token-v1" ‖ len ‖ client ‖ len ‖ room ‖ exp`).
**Test.** IDs con puntos validan correctamente; tokens de pares (cliente, sala) distintos con la misma concatenación no son intercambiables.

#### DEF-66 · Info · ⬜ (nuevo)
ARCHITECTURE.md dice HMAC-SHA256, pero el código usa BLAKE3 con clave. Actualizar la documentación.

---

## Lote 5 — Relay de snapshots

#### DEF-62 · Alto · ⬜ (nuevo)
**Problema.** `multipart_uploads` (`relay.rs:51`) no limita `total_bytes`, `total_chunks` ni la cantidad de sesiones (la clave incluye `head_seq`, así que se pueden abrir sesiones ilimitadas). Cualquier poseedor de un token puede agotar la RAM durante el TTL de 10 minutos.
**Solución.** Tope de `total_bytes` (tamaño máximo de snapshot), tope de sesiones concurrentes por sala (1–2), y rechazo de chunks fuera de rango.

#### DEF-10 · Medio · ⬜
**Problema verificado.** El token ya restringe a la sala, así que el atacante es un miembro de la sala o un cliente con bugs. El impacto de "desbordar el disco" es falso (TTL/cuota + TTL del snapshot de 600 s). El impacto real es el **envenenamiento de snapshots**: cualquier miembro puede reemplazar uno bueno por basura o por un seq menor o mayor; un seq mayor que `head` engaña a los clientes que arrancan desde él.
**Solución.** Antes de indexar, consultar al actor (es in-process): aceptar solo si `tail - 1 ≤ S ≤ head` y `S ≥ snapshot activo`. Validar magic, versión y CRC de `ZMSN` sin descomprimir (el CRC cubre el cuerpo comprimido); para eso la cabecera del envelope se mueve a core. En el actor, usar el seq del snapshot para el suelo de retención solo si está en rango.

#### DEF-63 · Medio · ⬜ (nuevo)
**Problema.** `stage_snapshot` es last-writer-wins (el snapshot activo puede retroceder), y dos llamadas concurrentes para la misma sala pueden borrarse los archivos entre sí (`relay.rs:200-206`).
**Solución.** Aceptación monótona (con DEF-10) y lock por sala alrededor de stage + borrado del anterior.

#### DEF-52 · Medio-Bajo · ⬜
`recover_disk_snapshots` inserta en el orden de `read_dir`: un archivo viejo puede pisar uno nuevo. Quedarse con el de mayor seq y borrar los demás.

#### DEF-51 · Medio · ⬜
**Problema verificado.** Cada `SnapshotChunk` ya incluye `snapshot_head_seq` y `snapshot_hash`, y BLAKE3 impide aplicar un snapshot mezclado: el resultado es reintentos, no corrupción (la auditoría lo sobrestima).
**Solución.** Anclar la petición por **hash** (anclar solo por seq no alcanza: una nueva subida con el mismo seq pisa la misma ruta con otro hash). Si el snapshot anclado ya no existe, devolver un error tipado (`SnapshotSuperseded`) con el seq y el hash actuales para que el cliente reinicie desde el chunk 0.

#### DEF-39 · Medio · ⬜
Bomba de descompresión en `decode_snapshot_envelope`. Validar `uncompressed_len <= cap` antes de descomprimir, decodificar con `zstd::stream::Decoder` + `.take(cap + 1)` y acotar la pre-reserva.

#### DEF-40 · Bajo · ⬜
Límites asimétricos entre subida y descarga, y `get_chunk` sin cota superior. Hacer `clamp` del `chunk_size` (el cliente se entera por `total_chunks`). Bug adicional: con `chunk_size = 0` se devuelven chunks vacíos (`relay.rs:253` usa `chunk_size` en vez de `chunk_size_u64`).

#### DEF-38 · Medio · ⬜
`recover_disk_snapshots` **y** `stage_snapshot` retienen el snapshot completo en RAM (`data: Bytes`). Servir los chunks leyendo del archivo bajo demanda dentro de `spawn_blocking` (portable; `read_at` es solo Unix).

#### DEF-31 (relay) · Bajo-Medio · ⬜
`delete_room` no purga el relay: una sala recreada dentro del TTL de 600 s sirve el snapshot de la sala borrada. Agregar `snapshot_relay.purge_room(room_id)`.

#### DEF-08 (relay) · Bajo · ⬜
`upload_snapshot` hace I/O bloqueante dentro del handler async y escribe hasta 16 MB sin `fsync` antes del rename. Mover a `spawn_blocking` y aplicar `write_atomic`.

#### DEF-15 · Bajo · ⬜
Mover los handlers HTTP de `relay.rs` a `api/relay.rs`, **en el mismo cambio** que DEF-10/11/38/51, porque esos tocan los mismos handlers.

---

## Lote 6 — Ciclo de vida de clientes y señalización

#### DEF-53 · Alto · ⬜
**Problema.** Si un cliente necesita un snapshot y no hay ninguno en el relay, nadie se entera: `RoomEvent` solo tiene `HeadAdvanced` y `SchemaReloaded`. Se agrava al resolver DEF-04.
**Error de la propuesta anterior.** Un evento solo por SSE no alcanza: SSE es opcional y de primer plano, y el canal broadcast descarta a los receptores lentos.
**Solución.** Un flag `snapshot_wanted` en las respuestas de heartbeat, Sync y CommitAck, más el evento SSE como acelerador. Se activa cuando un cliente se registra como `Bootstrapping` sin snapshot activo, o cuando Sync devuelve `BehindCompaction` sin snapshot. Con debounce. Se apaga cuando el relay acepta un snapshot válido (DEF-10).

#### DEF-05 · Medio · ⬜
**Problema verificado.** No es permanente (`register_client` sobrescribe el estado). Los problemas reales: (a) un cliente `Disconnected` pasa a `Dormant` a los 90 s sin importar su cursor y recibe un 410 engañoso; (b) `handle_commit` rechaza a clientes `Bootstrapping`, aunque ARCHITECTURE.md:176 dice que un commit los promueve; (c) un heartbeat de un cliente `Dormant` recibe 410; (d) `handle_ack` no chequea la retención.
**Solución.** `Dormant` significa solo "excluido del cálculo de retención". Quitar los gates `is_dormant`; cada handler decide por el cursor que trae (Sync ya lo hace; agregar el chequeo a Ack, y a Commit vía DEF-04) y promueve a `Connected` si tiene éxito. El heartbeat chequea el cursor guardado contra `tail - 1`.
**Test.** Cliente `Dormant` con cursor válido → Sync/Ack/Commit funcionan y queda `Connected`. Cliente `Dormant` con cursor expirado → `BehindCompaction`.

#### DEF-27 · Medio · ⬜
`reload_schema_for_rooms` mantiene un guard de lectura de `DashMap` a través de `.await`. Hay escritores concurrentes (`get_or_spawn`, `create_room`, `delete_room`), así que es un deadlock real aunque requiere una carrera. Recolectar los `(RoomId, Sender)` en un `Vec` antes del bucle.

#### DEF-24 · ❌ Descartado
El tick de 500 ms ya recalcula el suelo de retención; el retraso máximo es de un tick. Opcional: extraer el cálculo del suelo a un helper.

#### DEF-26 · ❌ Descartado
Un cliente `Bootstrapping` tiene por definición el cursor por debajo de `tail - 1`, así que en el siguiente tick pasa de `Disconnected` a `Dormant`. La poda se bloquea unos 500 ms, no 90 s.

#### DEF-75 · Bajo · ⬜ (nuevo, revisión del Lote 3)
**Problema.** El test existente `test_room_actor_retention_anchor_protects_deltas_during_snapshot` pasa aunque se quite el ancla de retención: sin ella el suelo es 10, solo se poda el segmento [1,5], `tail` queda en 6 y un sync desde 5 sigue funcionando. No prueba lo que dice.
**Solución.** Rehacer el escenario para que, sin el ancla, el sync desde el seq del snapshot reciba `BehindCompaction`.

#### DEF-76 · Bajo · ⬜ (nuevo, revisión del Lote 3)
**Problema.** `ClientLeaseTracker::record_ack` sobre un cliente no registrado lo inserta como `Connected`; `handle_sync` puede llegar a ese camino.
**Solución.** Ignorar (o rechazar) acks de clientes no registrados.

---

## Lote 7 — Protocolo wire y errores HTTP

#### DEF-46 · Bajo · ⬜
**Problema verificado.** El diagnóstico de la auditoría era incorrecto. El bug real: `encode_message` permite payloads de 16 MiB (frame de 16 MiB + 4), pero `decode_message` rechaza frames de más de 16 MiB *contando* el header. Subir el límite HTTP a 17 MB no cambia nada.
**Solución.** El decode chequea `len > MAX_MESSAGE_SIZE + PROTOCOL_HEADER_LEN`, y `DefaultBodyLimit` usa esa misma expresión.

#### DEF-28 · Medio · ⬜
Errores tipados en el codec (`VersionMismatch`, `InvalidMagic`, …). Toda falla de decodificación pasa a 400 (hoy es 500), y la de versión mapea a `ErrorCode::ProtocolVersionMismatch`.

#### DEF-11 · Medio-Bajo · ⬜
Los endpoints de chunks responden texto plano. Agregar `ErrorCode::BadRequest` y `ServerError::BadRequest` (el snippet de la propuesta anterior usaba una variante inexistente) y reutilizar `data_plane::binary_error`.

#### DEF-43 · Medio-Bajo · ⬜
Unos 20 sitios en `data_plane.rs` y `sse.rs` devuelven `ServerError::Config` (500) para errores del cliente. Usar `BadRequest` (400) para payloads inválidos y 403 cuando la sala del token no coincide con la de la ruta.

#### DEF-64 · Bajo · ⬜ (nuevo)
`request_chunk` devuelve `ErrorCode::Unauthorized` con HTTP 400 cuando la sala no coincide. Debe ser 403.

#### DEF-22 · Bajo-Medio · ⬜
**Error de la propuesta anterior.** Quitar `impl IntoResponse for ServerError`: el control plane y SSE sí deben responder JSON.
**Solución.** Solo el rechazo del extractor `ClientAuth` (Data Plane) emite un frame binario `ServerMessage::Error`.

#### DEF-71 · Bajo · ⬜ (nuevo, revisión del Lote 1)
**Problema.** `tail_seq` cambió de forma visible para el cliente: una sala nueva reporta `1` (antes `0`) y una sala podada por completo reporta `tail_seq = head_seq + 1` (mayor que `head`) en `Registered`/`RegisterResponse`. Un cliente que asuma `tail <= head` se equivoca. Además, las guardas `tail > 0` de `lease.rs` quedaron siempre verdaderas.
**Solución.** Documentar la semántica en la especificación del protocolo (ARCHITECTURE.md §7) antes de la Fase 4, y simplificar las guardas de `lease.rs` usando `TieredLog::is_behind_retention`.

#### DEF-74 · Bajo · ⬜ (nuevo, revisión del Lote 3)
Los errores reintentables (sala reiniciándose tras un fallo de I/O según D7, respuesta del actor descartada) se devuelven como 500 `Internal`. Un 503 con un código de error propio indicaría al cliente que reintentar es seguro.

---

## Lote 8 — Rendimiento y ciclo de vida de salas (servidor)

> Habilitador común: un **índice de segmentos en memoria** en `TieredLog` (metadatos de cold y sealed, más inicio y fin del activo), actualizado en rotate, compress y prune. Elimina los `read_dir` del tick y de cada `fetch_deltas` por la ruta lenta.

#### DEF-50 · Bajo · ⬜
Cada tick de 500 ms hace unos 8 `read_dir` más `stat` por archivo, y `prune_older_than` corre además en cada Ack. Es CPU y syscalls (los metadatos están en la caché del SO), no saturación de disco.
**Error de la propuesta anterior.** Subir el tick a 30 s también retrasa la detección de leases (`check_timeouts` corre en el mismo tick).
**Solución.** Separar en dos ticks: leases en memoria cada 1–5 s; mantenimiento de disco cada 30–60 s, servido desde el índice. Llevar los bytes en disco por sala de forma incremental.

#### DEF-35 · Medio · ⬜
Al crear la sala se descomprime todo el cold tier y se lee todo el warm, de forma síncrona en `RoomActor::spawn`. Solo hacen falta las últimas `ram_max_ops` ops, los últimos `dedup_lru_capacity` mutation IDs y `head` (que sale de los nombres de segmento y de `active.wal`). Leer hacia atrás hasta cubrir ambas cuotas e hidratar en orden cronológico para conservar la recencia de la LRU.

#### DEF-55 · Bajo-Medio · ⬜
Las salas nunca se apagan. Agregar un reaper que apague el actor tras N minutos sin clientes conectados ni comandos. Reduce también el costo de DEF-37 y DEF-50.

#### DEF-37 · Bajo · ⬜ (requiere DEF-07 y DEF-58)
La ventana TTL de la RAM solo se aplica en `append`. No es una fuga: está acotada a `ram_max_ops` por sala. Aplicar la ventana en el mantenimiento **solo después** de DEF-07/58; antes produce `tail = head` y `BehindCompaction` masivo.

#### DEF-08 · Bajo-Medio · ⬜
El `sync_data` de cada commit bloquea un worker de Tokio (en macOS es `F_FULLFSYNC`, 5–20 ms). Solo afecta cuando muchas salas hacen commit a la vez. Envolver el append con fsync en `tokio::task::block_in_place`, y la descompresión cold en `spawn_blocking`. Group commit descartado (D4).

#### DEF-31 (spawn_locks) · Bajo · ⬜
**Error de la propuesta anterior.** Un `spawn_locks.remove` directo rompe la exclusión mutua: quien espera con el mutex viejo y un llamador nuevo terminan con mutex distintos.
**Solución.** Quitarlo solo bajo el guard, con `remove_if` según el refcount del `Arc`.

#### DEF-25 (server) · Bajo · ⬜
`warm_disk.rs` y `cold_disk.rs` ya usan `decode_wal_batch_from_slice`. Lo que está triplicado es el bucle de lectura: extraer un helper.

#### DEF-72 · Medio · ⬜ (nuevo, revisión del Lote 2)
**Problema.** Windows es plataforma objetivo. `WarmDiskLog::read_range` (y `recover_all`) leen `active.wal` con `std::fs::read`, un segundo handle, mientras `active_file` tiene el lock exclusivo de `fs2`. En Windows ese lock es obligatorio (`LockFileEx`) y la lectura falla: el catch-up desde `active.wal` tras desalojo por TTL de RAM no funcionaría.
**Solución.** Leer `active.wal` a través del handle que ya tiene el lock (o con un lock compartido coordinado). Verificar en CI con Windows (DEF-73).

#### Nota sobre DEF-08
La revisión del Lote 1 señaló dos casos más de I/O bloqueante en el actor: `record_pruned_through` (escritura atómica de `log_meta.json`) y los `sync_dir` de la fase 3 de la compactación de storage. Se tratan junto con DEF-08.

#### DEF-77 · Bajo · ⬜ (nuevo, revisión del Lote 3)
`ColdDiskLog` usa `cold_path.with_extension("tmp")` (`segment_X_Y.wal.tmp`); un `.tmp` residual de una compresión interrumpida nunca se limpia.

#### DEF-79 · Bajo · ⬜ (nuevo)
Tests dependientes de tiempos: `tiered_log_tests::test_tiered_log_behind_compaction_eviction` (cold TTL de 100 ms) falla a veces con la suite completa en paralelo. Revisar los tests con TTL cortos y pasarlos a tiempo simulado o márgenes amplios.

---

## Lote 9 — Robustez e higiene de storage y core

#### DEF-14 · Medio · ⬜
El centinela `table_id == 0` reasigna IDs. Es alcanzable vía JSON de admin con la tabla 0 fuera de orden, con una clave de mapa distinta del ID propio, o con IDs duplicados (que hoy se reasignan en silencio). Usar `Option<u16>` en el builder, y que la deserialización use una inserción estricta que falle ante colisión o desajuste, **nunca** `add_table` con auto-asignación.

#### DEF-23 · Bajo · ⬜
Unificar la precondición de sala abierta en `apply_snapshot`. Además, `MemoryStorageEngine::apply_snapshot` cambia el `Arc` de la sala: un escritor que todavía tiene el viejo escribe en una copia muerta. Reemplazar el contenido in-place bajo el lock de la sala.

#### DEF-49 · Bajo-Medio · ⬜
El lock de escritura de la sala se retiene durante `sync_data`. El RwLock de Tokio es justo, así que los lectores esperan aproximadamente un fsync: es latencia, no inanición. Si se separa, el mutex del WAL debe cubrir validación + append + fsync, y la rotación de la fase 1 debe tomarlo también.

#### DEF-17 · Bajo-Medio · ⬜
El primer write por tabla después de un `scan` (o mientras la compactación retiene el `Arc`) clona el `BTreeMap` entero bajo el lock. Usar `imbl::OrdMap` (con serde); locks por tabla no resuelven esto.

#### DEF-25 (storage) · Bajo · ⬜
`recovery.rs` duplica unas 170 líneas de parseo de WAL y además se comporta distinto del decoder de core (heurísticas de torn write diferentes). Core expone `parse_batch_header` y `decode_batch_payload`, y la recuperación los usa (lee en streaming, así que no puede usar directamente la API de slice).

#### DEF-18 · Bajo · ✅ Hecho
Guardar `room_id` en `DiskRoomState`. El ID mal derivado solo aparece en errores `RoomLocked`. Resuelto junto con las correcciones de la revisión del Lote 2, porque el estado de sala fallida necesitaba el ID.

#### DEF-09 y DEF-45 · Bajo · ⬜
Sin uso en producción. Eliminar `decode_wal_record_from_slice` y `WalReader::next_record` (o hacer que el lector devuelva lotes completos) y adaptar los tests. Si se conserva el lector, que saltee los frames vacíos.

#### DEF-33 · Bajo · ⬜
`encode_wal_batch` con un struct prestado (`ops: &[SequencedOperation]`). Serializa los mismos bytes.

#### DEF-44, DEF-47, DEF-30 · Bajo/Info · ⬜
Una sola adquisición del lock en las consultas por nombre; renombrar el parámetro de `scan`; buscar por referencia antes de clonar la PK en `TableBuffer::apply` (el doc comment actual afirma lo contrario).

#### DEF-13, DEF-20, DEF-21 · Bajo · ⬜
Encapsulamiento (regla 5). DEF-13 necesita además `Schema::add_column`, porque `schema_registry.rs` usa `tables_by_id.get_mut`. Para DEF-20 no alcanza con hacer privado el campo: `get_mut`, `remove` y `Deserialize` permiten el mismo bypass. En DEF-21, `Operation` es un tipo wire que llega por `Deserialize`; la garantía real es `validate_operation`, que ya se ejecuta.

#### DEF-70 · Bajo · ⬜ (nuevo, revisión del Lote 2)
**Problema.** `apply_snapshot` acepta un snapshot cuyo `head_seq` es menor que el de la sala. Si hay un crash entre el rename del snapshot y el truncado del WAL, la recuperación encuentra registros que no continúan la secuencia y la sala no abre (`WalCorruption`). Antes se aplicaban en silencio sobre un estado incorrecto.
**Solución.** Rechazar en `apply_snapshot` un snapshot con `head_seq` menor que el actual (no hay caso legítimo de retroceso).

#### DEF-78 · Bajo · ⬜ (nuevo, revisión del Lote 3)
`SchemaRegistry::register_schema` y `add_column` no tienen lock por esquema: dos escrituras concurrentes sobre el mismo id comparten el mismo `.tmp`, y `add_column` es leer-modificar-escribir. Serializar las escrituras por id de esquema.

#### DEF-32 · ❌ Descartado
El input es un WAL local acotado por longitud y CRC, y bincode 1.3 ya valida longitudes contra el slice. **La solución propuesta rompería todos los WAL existentes**: se escriben con `bincode::serialize` (enteros de ancho fijo), y `DefaultOptions::new()` decodifica varints. Si alguna vez se agrega un límite, usar `.with_fixint_encoding().allow_trailing_bytes().with_limit(..)`.

#### DEF-57 · Bajo · ⬜ (parcial ahora)
`zemdb-client` no compila para wasm32 (tokio `full` arrastra mio). Ahora: quitar las dependencias `tokio` y `zstd`, que no se usan. Agregar `zemdb-storage` recién cuando haya código que lo necesite (Fase 4).

---

## Lote 10 — Diferido a Fase 4 / descartado

#### DEF-34 · Bajo · ⏸
Sin consumidor hoy. Ya existen alternativas: cerrar y reabrir la sala con el nuevo esquema, o `apply_snapshot`, que reemplaza el esquema. Cuando exista el cliente, basta con cambiar el esquema bajo el lock de escritura de la sala más un chequeo append-only; no hace falta `Arc<Schema>`, porque los scans no leen el esquema.

#### DEF-54 · Medio · ⏸
Usar `ruzstd` en wasm32 para descomprimir snapshots `ZMSN`, junto con la descompresión acotada de DEF-39.

#### DEF-42 · Info · ⏸
ARCHITECTURE.md:169 presenta la descarga directa como alternativa al protocolo por chunks, que ya está implementado. Aclarar el documento; implementar el endpoint, si se quiere, después de DEF-38.

#### DEF-73 · Info · ⏸ (nuevo)
Windows es plataforma objetivo, pero hoy nada se prueba en Windows. Agregar CI (por ejemplo GitHub Actions) con `windows-latest` que corra `cargo test --workspace`; ahí se verifican DEF-72 y la lectura del WAL por su propio handle en storage.

#### DEF-56 · ❌ Descartado
Devolver las columnas en el orden de la proyección es el comportamiento estándar y coincide con lo documentado (`options.rs:133`). Rellenar con `Null` anula el sentido de proyectar. A lo sumo, documentarlo y rechazar índices fuera de rango.
