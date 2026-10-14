# Auditoría Post-Fase 3.7

**Fecha:** 14 de Octubre de 2026
**Commit auditado:** `698e8d6` (`main`). La auditoría se hizo sobre `2a5f7b3`; después entraron 53 commits de refactor (la aplicación de la auditoría de calidad de código, cuyos reportes se borraron después; siguen en el historial de git hasta `f7e439b`), y cada hallazgo se re-verificó en `698e8d6`. Las referencias `ruta:línea` son de `698e8d6`. Ver §1.1.
**Alcance:** `crates/core`, `crates/storage`, `crates/server`, `crates/client`, `README.md`, `ARCHITECTURE.md`, `ROADMAP.md`, `GEMINI.md`, `.github/workflows/ci.yml`
**Auditoría anterior:** [`2026-10-post-fase3_6-audit.md`](2026-10-post-fase3_6-audit.md) (DEF-87 a DEF-113, resuelta en el [plan de la Fase 3.7](../proposals/2026-10-fase3_7-remediation-plan.md))
**Plan de resolución:** [`docs/proposals/2026-10-fase3_8-remediation-plan.md`](../proposals/2026-10-fase3_8-remediation-plan.md)

---

## 1. Metodología

- **7 auditores en paralelo**, cada uno con una perspectiva distinta y en modo solo lectura (los tests de reproducción se escribieron sobre copias del repo en un directorio temporal):
  1. Rust: panics, aritmética, errores tragados, invariantes de tipos, wasm32.
  2. Bases de datos: durabilidad, WAL, recuperación, compactación, snapshots, log del servidor.
  3. Sincronización: secuenciación, catchup, cursores, idempotencia, retención, onboarding por snapshot.
  4. Concurrencia async (Tokio): actores, cancelación, locks, apagado.
  5. Seguridad: autenticación, entradas hostiles, DoS, límites HTTP.
  6. Rendimiento y recursos.
  7. Tests, coherencia código↔documentación y CI (con mutation testing manual: 19 mutantes, 15 muertos, 4 vivos).
- **Sin forzar hallazgos:** cada auditor tenía que citar `ruta:línea` y un escenario de falla seguible en el código; un área sin problemas se reporta como tal (ver §5).
- **Consolidación:** la pérdida de operaciones de tablas desconocidas en la recuperación la reportaron por separado los auditores de Rust y de bases de datos (DEF-115). El hallazgo de un snapshot "usable" por debajo de un hueco (DEF-119) y el segmento warm que reaparece (DEF-120) comparten síntoma y se cruzan entre sí.
- **Estados de verificación:**
  - **Confirmado (coordinador):** el coordinador lo comprobó también en el código.
  - **Confirmado:** el auditor lo reprodujo con un test o recorrió el camino completo.
  - **Plausible:** el mecanismo existe, pero el impacto depende del despliegue, del FS o de un disparador poco frecuente.
- **IDs:** se continúa la numeración `DEF-xx`. Esta auditoría va de **DEF-115 a DEF-142**.

### 1.1. Segunda pasada: el refactor de calidad (`2a5f7b3..698e8d6`)

Después de la primera pasada entraron 53 commits que aplican la auditoría de calidad (129 archivos, +6.300/−11.900 líneas, casi todo en tests). Se hicieron dos cosas:

1. **Re-verificación de DEF-115..139 en `698e8d6`:** ninguno quedó resuelto ni desapareció. 22 siguen igual y 3 cambiaron de forma (DEF-134, 137, 138). Los 4 mutantes de DEF-131..134 siguen sobreviviendo. Las mediciones de rendimiento y las reproducciones de DEF-116, 118, 119 y 123 a 126 no se repitieron: los caminos están intactos y los diffs de esas zonas son solo de formato, nombres y comentarios.
2. **Revisión de regresiones del refactor**, en tres frentes:
   - **Producción de core y storage (27 commits):** sin regresiones de comportamiento, sin cambios de formato en disco ni wire, y sin validaciones alcanzables eliminadas. Se usó un fuzz diferencial de `parse_uuid` (3 M entradas, 0 diferencias) y se compiló para wasm32.
   - **Producción del servidor (15 commits):** sin regresiones. Los tests de integración de `2a5f7b3` compilan y pasan contra el código de `698e8d6`.
   - **Tests:** de 690 a 667. Cada test que desapareció tiene destino (fusiones, casos tabulados, duplicados borrados o renombres), y varios asserts quedaron más estrictos. Dos debilitamientos confirmados con mutación (DEF-140, DEF-141).

   Además aparecieron documentos que el refactor dejó desactualizados (DEF-142) y un link de rustdoc roto nuevo (DEF-138).

### Estado del build

| Chequeo | `2a5f7b3` | `698e8d6` |
|---|---|---|
| `cargo fmt --all -- --check` | Sin diferencias | Sin diferencias |
| `cargo clippy --workspace --all-targets -- -D warnings` | Sin warnings | Sin warnings |
| `cargo test --workspace` | 690 en verde | 667 en verde |
| `cargo clippy --target wasm32-unknown-unknown` (core, storage, client) `-D warnings` | Sin warnings | Compila (build verificado) |

Un fallo intermitente por tiempos (`http_edge_tests::body_sent_drip_by_drip_fails_when_its_time_is_up`, con la máquina bajo carga) pasó en todas las repeticiones; no se considera hallazgo.

---

## 2. Hechos de contexto que condicionan las severidades

1. **`zemdb-server` no depende de `zemdb-storage`.** Los defectos de storage (DEF-115, 129, 130, 133, 141) afectan al cliente de la Fase 4, que todavía no existe (`zemdb-client` es un stub).
2. **El servidor es el único binario desplegable hoy.** Lo que lo afecta tiene impacto inmediato si se expone.
3. **No hay ningún panic alcanzable desde la red** y `unsafe` está prohibido en todo el workspace. No se encontró ningún defecto de la auditoría anterior (DEF-87..113) reabierto.

---

## 3. Resumen

| Severidad | Cantidad | IDs |
|---|---|---|
| 🔴 Alto | 1 | DEF-116 |
| 🟠 Medio | 9 | DEF-115, 117, 118, 119, 121, 122, 131, 132, 133 |
| 🟡 Bajo | 18 | DEF-120, 123–130, 134–142 |
| **Total** | **28** | |

**Lo más importante:** un cliente autenticado que no lee sus respuestas retiene memoria y conexiones sin plazo (DEF-116). Las operaciones confirmadas de una tabla se pierden si una sala de storage se reabre con un esquema que no la tiene (DEF-115). Y el roster se reescribe con 2 fsync en casi cada Ack y en cada registro (DEF-117, DEF-121). Durabilidad básica, autenticación y concurrencia de actores salieron limpias, y el refactor de calidad no introdujo regresiones de comportamiento.

### Matriz

| ID | Sev. | Verificación | Crate | Resumen |
|---|---|---|---|---|
| DEF-115 | Medio | Confirmado (coordinador) | storage | La recuperación saltea las operaciones de tablas que no están en el esquema y avanza el head; la compactación las pierde. |
| DEF-116 | Alto | Confirmado | server | Un cliente que no lee la respuesta retiene la conexión y el cuerpo codificado sin plazo (HTTP/2 incluido). |
| DEF-117 | Medio | Confirmado (coordinador) | server | Casi todo Ack reescribe el roster con 2 fsync aunque la poda no borre nada. |
| DEF-118 | Medio | Confirmado | server, core | Una sala recreada con el mismo id acepta el cursor de la sala borrada. |
| DEF-119 | Medio | Confirmado | server | Un snapshot "usable" por debajo de un hueco del log apaga la demanda: los clientes quedan en bucle. |
| DEF-120 | Bajo | Plausible | server | Un segmento warm que no se pudo borrar reaparece tras la poda del cold y retrocede `tail_seq`. |
| DEF-121 | Medio | Confirmado | server | Cada registro reescribe el roster completo con 2 fsync; una tormenta de reconexiones cuesta O(N²). |
| DEF-122 | Medio | Confirmado | server | Reabrir una sala decodifica operaciones completas solo para hidratar el dedup. |
| DEF-123 | Bajo | Confirmado | server | El dedup es por sala, no por cliente: un `MutationId` ajeno se confirma sin aplicarse. |
| DEF-124 | Bajo | Confirmado | server | Un solo miembro puede ocupar todas las conexiones del servidor con streams SSE. |
| DEF-125 | Bajo | Confirmado (coordinador) | server | Un `DELETE /admin/rooms/:id` cancelado deja la sala viva pero sin su snapshot. |
| DEF-126 | Bajo | Confirmado | server | Una subida por chunks no ancla la retención y puede fallar al completarse con `BadRequest`. |
| DEF-127 | Bajo | Plausible | server, core | Un cambio de esquema solo se avisa por SSE; ninguna respuesta del protocolo lo señala. |
| DEF-128 | Bajo | Plausible | server | `Path::exists()` trata un error de I/O como "no existe" (`active.wal`, `meta_room.json`). |
| DEF-129 | Bajo | Plausible | storage | `open_room` crea `data_dir` sin sincronizar el directorio padre. |
| DEF-130 | Bajo | Confirmado | storage | `compaction_ratio` acepta NaN, 0 y negativos: compacta en cada batch. |
| DEF-131 | Medio | Confirmado | server (tests) | Ningún test cubre que un cliente `Bootstrapping` no frene la poda. |
| DEF-132 | Medio | Confirmado | server (tests) | Ningún test cubre que la poda por TTL/cuota se detenga en el primer segmento conservado. |
| DEF-133 | Medio | Confirmado | storage (tests) | Ningún test cubre un hueco de secuencia dentro de un mismo batch. |
| DEF-134 | Bajo | Confirmado | server (tests) | Ningún test comprueba que la compresión a cold corra fuera del worker (el test que lo prometía se borró). |
| DEF-135 | Bajo | Confirmado (coordinador) | docs | `ROADMAP.md` contradice el código (HMAC, rutas del relay, `Dormant`, `CryptoConcurrencyBounds`). |
| DEF-136 | Bajo | Confirmado | docs | `ARCHITECTURE.md` promete portabilidad total de snapshots sin la salvedad de Zstd en wasm32. |
| DEF-137 | Bajo | Confirmado | core, storage | Quedan doc comments con la afirmación de la línea de caché. |
| DEF-138 | Bajo | Confirmado | docs | La doc pública enlaza a 7 ítems privados; la CI no corre `cargo doc`. |
| DEF-139 | Bajo | Confirmado | CI | La CI no verifica el MSRV declarado ni corre clippy sobre wasm32. |
| DEF-140 | Bajo | Confirmado (coordinador) | server (tests) | El test de overrides de ciclo de vida ya no verifica que la config del servidor llegue a la sala (refactor). |
| DEF-141 | Bajo | Confirmado | storage (tests) | Se perdió la cobertura de que el chequeo de secuencia va antes que el de esquema (refactor). |
| DEF-142 | Bajo | Confirmado (coordinador) | docs | Documentación que el refactor dejó desactualizada. |

---

## 4. Detalle de los hallazgos

### Durabilidad y recuperación

#### DEF-115 · Medio · Confirmado (coordinador) · La recuperación saltea operaciones de tablas fuera del esquema
- **Dónde:** `crates/storage/src/disk/recovery.rs:201-205` (`if schema.has_table_by_id(..) { apply_operation }` y después `*head_seq = op.seq` siempre); `recovery.rs:320` (`fill_missing_tables` conserva las tablas del snapshot que el esquema no conoce); `crates/storage/src/disk/mod.rs:686` (`apply_snapshot` cambia el esquema solo en memoria, nunca lo persiste).
- **Qué pasa:** `open_room(room, schema)` acepta cualquier esquema. Las operaciones del WAL sobre una tabla que no está en él se descartan sin error, pero el head avanza. La próxima compactación escribe un snapshot en ese head sin esas operaciones y vacía el WAL: la pérdida queda permanente y el estado no es ningún prefijo del log.
- **Escenario (reproducido por dos auditores):** abrir con `{a, b}`, aplicar `[ins a/1, ins b/7]`, cerrar; reabrir con `{a}` (versión vieja de la app, otra pestaña, o un crash que dejó el esquema persistido por el cliente en la versión anterior); aplicar otro batch y compactar; reabrir con `{a, b}`: el head es 3 y `get(b, 7)` devuelve `None`.
- **Relación:** distinto de DEF-34 (recargar el esquema sin cerrar la sala).
- **Solución:** en `replay_wal_file`, devolver un error (`SchemaMismatch` o `TableNotFound`) ante una operación de una tabla desconocida; en `recover_room`, rechazar un snapshot con tablas fuera del esquema.

#### DEF-120 · Bajo · Plausible · Un segmento warm que no se pudo borrar reaparece tras la poda
- **Dónde:** `crates/server/src/log/cold_disk.rs:95-102` (un fallo de `remove_file` del warm queda en `cleanup_error` y la compresión devuelve `Ok`); `tiered_log.rs:417-423` (solo hace `warn!`); `segment_index.rs:113-125` (al reabrir, el warm solo se descarta si existe el cold del mismo rango); `tiered_log.rs:155-156` (`scan` no filtra por `pruned_through_seq`); `tiered_log.rs:553-558` (`compute_tail_seq`).
- **Escenario:** se comprime A [1..10] pero falla el borrado de `A.wal`; el TTL o la cuota borran `A.wal.zst` y B [11..20]; al reiniciar, `scan` indexa `A.wal` como warm y `tail_seq` retrocede a 1 aunque falta [11..20]. No se entregan datos incorrectos (el hueco da `BehindCompaction`), pero un snapshot con seq en [0..10] pasa a ser "usable" y produce el bucle de DEF-119.
- **Solución:** guardar la ruta del warm que no se pudo borrar y reintentar el borrado en el próximo `expire`/`prune_segments`; en `SegmentIndex::scan`, descartar los segmentos con `end_seq <= pruned_through_seq`.

#### DEF-129 · Bajo · Plausible · `open_room` no sincroniza el padre de `data_dir`
- **Dónde:** `crates/storage/src/disk/mod.rs:383` (`create_dir_all` de tokio). El servidor ya tiene `durable::create_dir_all_synced` (`crates/server/src/durable.rs:33`).
- **Escenario:** primer `open_room` sobre un `data_dir` nuevo, batches confirmados con fsync, corte de energía. En un FS que no ordena la metadata del padre, el directorio puede no existir al reiniciar.
- **Solución:** llevar `create_dir_all_synced` a storage (o a core) y usarlo en `open_room`.

#### DEF-128 · Bajo · Plausible · `exists()` trata un error de I/O como "no existe" en el servidor
- **Dónde:** `crates/server/src/log/warm_disk.rs:210` (`inspect_active_segment`) y `crates/server/src/actor/manager.rs:485` (`create_room`). El mismo patrón en `warm_disk.rs:162`, `manager.rs:356`, `:408`, `:524`, `cold_disk.rs:57` y `schema_registry.rs:41` (`is_file()`). Es la misma clase que DEF-108, que solo se corrigió en storage.
- **Escenario:** un `stat` que falla de forma transitoria sobre `active.wal` hace que la sala abra con un log vacío en ese segmento; el próximo commit reutiliza secuencias ya confirmadas y escribe detrás de los frames viejos. En `create_room`, se sobrescribe un `meta_room.json` existente.
- **Solución:** `try_exists()?` o `metadata()` propagando el error, con un fail point como el de storage.

#### DEF-130 · Bajo · Confirmado · `compaction_ratio` sin validar
- **Dónde:** `crates/storage/src/disk/mod.rs:66-69` y `:524` (`(snapshot_len as f64 * ratio) as u64`); `zstd_level` en `:78-81`.
- **Qué pasa:** con NaN, 0 o un valor negativo el umbral queda en 0 y cada `apply_batch` por encima de `min_compaction_bytes` reescribe y recomprime el snapshot entero.
- **Solución:** validar `ratio.is_finite() && ratio > 0.0` en el builder o en `DiskStorageEngine::new`; lo mismo con `zstd_level`.

### Sincronización y protocolo

#### DEF-118 · Medio · Confirmado · Una sala recreada acepta el cursor de la sala borrada
- **Dónde:** `crates/server/src/actor/room.rs:451-453` y `:691-699` (solo rechaza `current_seq > head`); `lease.rs:197-227`; `manager.rs:471-512` (`create_room` no crea ningún identificador de encarnación); `crates/core/src/protocol/messages.rs:113-121` y `:295-305`. El token sigue valiendo porque solo firma `(client, room_id, exp)`.
- **Escenario (reproducido):** el cliente `old` aplica 1..5 en R; el admin borra R y la recrea con el mismo id; otro cliente hace 7 commits; `old` se registra con `Some(5)`, queda `Connected` y `sync(5)` le entrega desde la 6. Diverge en silencio, y si tiene el cursor más alto puede ser designado para subir un snapshot con datos de la sala vieja. Si la nueva sala tiene `head < 5`, recibe `InvalidSequence` (400, no reintentable) y el contrato no define la salida.
- **Solución:** un `room_incarnation` aleatorio (u128) en `meta_room.json`, devuelto en `Registered` y exigido en `RegisterClient`; si no coincide, avisarle al cliente que descarte su estado local.

#### DEF-119 · Medio · Confirmado · Un snapshot "usable" por debajo de un hueco apaga la demanda
- **Dónde:** `crates/server/src/actor/room.rs:627-635` (el hueco llama a `request_snapshot`); `room.rs:755-758` (vuelve sin hacer nada si hay un snapshot "usable"); `room.rs:743-748` y `crates/server/src/log/retention.rs:24-30` ("usable" solo compara contra `tail_seq` y `head_seq`); `room.rs:846-848` (el ancla usa ese snapshot); `crates/server/src/relay/download.rs:51-57`.
- **Escenario (reproducido):** snapshot S=2, se pierde el segmento 3–4 (o DEF-120), un cliente restaura S y `sync(2)` da `BehindCompaction`. Como S sigue siendo "usable", la demanda no se enciende, nadie es designado y el ancla impide podar el hueco. El cliente queda en "descargar S → sync → 410" hasta que S vence por TTL (7 días por defecto).
- **Solución:** en la rama de hueco de `handle_sync`, si `usable_snapshot_seq() <= from_seq`, descartar ese snapshot del relay antes de `request_snapshot()`; y guardar en `TieredLog` el fin del rango ilegible para que `is_usable_snapshot` exija `S >= fin_del_hueco`.

#### DEF-126 · Bajo · Confirmado · Una subida por chunks no ancla la retención
- **Dónde:** `crates/server/src/actor/room.rs:846-848` (el piso solo mira el snapshot activo); `crates/server/src/relay/upload.rs:317-323` → `relay/types.rs:43-52` (la verificación de rango al completar responde `BadRequest`).
- **Escenario (reproducido):** subida que arranca en S=4 con la cola en 1; la sala sigue hasta 10 y todos hacen ack; la poda deja la cola en 9; el último chunk falla con `BadRequest`. Con snapshots grandes y carga sostenida puede fallar en cada intento, y `BadRequest` está documentado como no reintentable.
- **Solución:** incluir la seq de la subida en curso (no inactiva) en el piso de poda; responder con `SnapshotSuperseded` u otro código reintentable cuando el rango se movió.

#### DEF-127 · Bajo · Plausible · El cambio de esquema solo se avisa por SSE
- **Dónde:** `crates/server/src/api/sse.rs:22-27` y `crates/server/src/actor/command.rs:70-71` dicen que SSE es opcional, pero `CommitAck`, `SyncBatch` y `HeartbeatAck` (`messages.rs:221`, `:239`, `:278`) no llevan versión de esquema. Del lado del cliente, `crates/storage/src/memory/state.rs:77-96` y `crates/core/src/schema/validation.rs:270-275`, `:314-320` rechazan operaciones de un esquema más nuevo.
- **Escenario:** un cliente sin SSE (o que recibe `lagged`) sigue con el esquema viejo; se agrega una columna y otro cliente la actualiza; el `apply_batch` local del primero falla y nada le indica que pida `GetSchema`.
- **Solución:** un `schema_version` en esas respuestas, o documentar como regla del SDK que ante un error de validación al aplicar se pida `GetSchema` y se reintente.

#### DEF-123 · Bajo · Confirmado · El dedup es por sala, no por cliente
- **Dónde:** `crates/server/src/dedup.rs:10` (`LruCache<MutationId, SequenceNumber>`), `crates/server/src/actor/room.rs:486`.
- **Escenario (reproducido):** el cliente A hace commit con un `MutationId` que B ya usó: recibe `CommitAck` con la seq de B y su operación nunca se aplica. Puede ser malicioso (ids predecibles) o accidental (un SDK que numere por cliente). El protocolo no exige ids aleatorios.
- **Solución:** clave `blake3(client_id ‖ mutation_id)` truncada a 16 bytes (sin cambiar el formato del WAL), o documentar que `MutationId` debe ser aleatorio de 128 bits.

### Seguridad y borde HTTP

#### DEF-116 · Alto · Confirmado · Un cliente que no lee la respuesta la retiene sin plazo
- **Dónde:** `crates/server/src/api/serve.rs:360-373` (`idle_for` espera sin timeout mientras `in_progress > 0`); `serve.rs:60-62` (el doc dice que un cliente HTTP/2 que deja de leer se desconecta, lo que solo vale cuando hyper ya tomó todo el cuerpo); `serve.rs:196`, `:220` (100 streams por conexión). Respuestas grandes: `data_plane.rs:224` (`SyncBatch` de hasta ~16 MiB, `crates/core/src/protocol/limits.rs:41`) y `relay/download.rs:69` (chunks de hasta 4 MiB, `relay/types.rs:15`).
- **Qué pasa:** la request sigue "en curso" hasta que hyper toma el último frame del cuerpo, y eso depende de la ventana de control de flujo (HTTP/2) o del socket (HTTP/1.1), que maneja el cliente. Sin `WINDOW_UPDATE` o sin leer, la conexión y el `Bytes` de la respuesta quedan retenidos indefinidamente.
- **Escenario (reproducido):** un miembro de cualquier sala llena el log con filas grandes y abre una conexión h2 con 100 `POST /sync` sin leer los cuerpos: hasta ~1,6 GB retenidos por conexión. Pocas conexiones alcanzan para un OOM. En la reproducción, 20 streams de ~2 MiB subieron el RSS en 33 MiB y bloquearon otra conexión con `max_connections=1`.
- **Ya conocido en parte:** la variante HTTP/1.1 sin amplificación figura como residuo de DEF-95 en el plan de la Fase 3.7. La variante HTTP/2 y la retención de memoria son nuevas y contradicen el doc de `serve.rs`.
- **Solución:** un plazo de progreso de escritura configurable: si no hubo progreso en ese plazo con requests en curso, se cierra la conexión (el keep-alive de SSE cuenta como progreso). Además, acotar la memoria de respuestas en vuelo y corregir el doc.

#### DEF-124 · Bajo · Confirmado · Streams SSE ocupan todas las conexiones
- **Dónde:** `crates/server/src/api/sse.rs:28-62` (sin tope por cliente, sala ni global); `serve.rs:141`, `:149` (semáforo global), `:360-373`.
- **Escenario (reproducido):** con un token válido de cualquier sala, el atacante abre `max_connections` conexiones con `GET /rooms/:id/events`. Le cuesta casi nada y los clientes de todas las demás salas quedan en el backlog. D45 decidió que SSE no tiene plazo, pero no contempló este efecto entre tenants.
- **Solución:** tope de streams SSE por `(sala, client_id)` y global, o reservar una fracción de `max_connections` para conexiones que no son SSE.

### Concurrencia

#### DEF-125 · Bajo · Confirmado (coordinador) · Un DELETE de sala cancelado la deja sin snapshot
- **Dónde:** `crates/server/src/api/control_plane.rs:139-146` llama a `manager.rs:515-530` (`delete_room`) directamente, sin una tarea propia (a diferencia de `add_column`, `control_plane.rs:94`, y de `run_to_completion` en el relay). Efecto en `crates/server/src/relay/lifecycle.rs:21-43`.
- **Escenario (reproducido):** el handler se descarta (el admin corta la conexión) durante el `run_blocking` de `purge_room`: el snapshot ya salió de memoria y sus archivos se borran, pero `remove_dir_all` nunca corre. La sala sigue existiendo, sin snapshot, y las subidas en curso reciben "Room X was deleted". Se recupera sola y no se pierden commits.
- **Solución:** correr `delete_room` en `tokio::spawn` y esperar el `JoinHandle` desde el handler, como `add_column`; documentar que no es seguro cancelarla.

### Rendimiento

#### DEF-117 · Medio · Confirmado (coordinador) · Casi todo Ack reescribe el roster con 2 fsync
- **Dónde:** `crates/server/src/actor/room.rs:672` → `prune_to_retention_floor` (`room.rs:839-866`): la única salida temprana es `retention_floor <= tail_seq` (`:851`); si no sale, `persist_if_dirty()` (`:856`) serializa todo el roster y hace `write_atomic` (`lease.rs:129-143`: `sync_all`, rename, `sync_dir`). Después, `prune_older_than` no encuentra segmentos que borrar.
- **Qué pasa:** en régimen estable los cursores están dentro de `active.wal` o del primer segmento. El piso supera al tail pero ningún segmento termina por debajo, así que cada Ack que avanza un cursor paga la serialización completa y 2 fsync sincrónicos en el actor. `ARCHITECTURE.md:132` dice que el roster se escribe antes de una poda "that deletes segments".
- **Medición:** ~8,8 ms por Ack contra ~3,6 ms por commit (macOS). 20 clientes con un Ack por commit a 10 commits/s superarían 1,7 s de fsync por segundo de actor, con lo que crece la cola y aparecen 504.
- **Solución:** antes de persistir, comprobar que haya algún segmento con `end_seq < retention_floor`; si no, dejar el cursor dirty para el tick rápido.

#### DEF-121 · Medio · Confirmado · Cada registro reescribe el roster completo
- **Dónde:** `crates/server/src/actor/lease.rs:197-227` (`register_client` → `write_roster` en `:217`), igual en `deregister_client` (`:262-277`); invocado desde `room.rs:341` → `:457` en cada conexión de un cliente.
- **Medición:** 12,2 ms por registro con 1.000 clientes (14 s para registrarlos a todos) y 15,1 ms con 5.000 (126 s).
- **Escenario:** tras un reinicio o un corte de red, 2.000 clientes de una sala reconectan a la vez; la mayoría vence `ACTOR_TIMEOUT` (5 s), reintenta y empeora la cola, y los commits esperan detrás. DEF-103 (JSON compacto, olvido de `Dormant`) no cambia el costo O(N) + fsync por cambio de membresía.
- **Solución:** un re-registro de un cliente presente con cursor igual o menor no cambia la membresía: marcarlo dirty en lugar de escribir. Alternativas: agrupar los `RegisterClient` en cola, o un journal append-only compactado en el tick.

#### DEF-122 · Medio · Confirmado · Reabrir una sala decodifica operaciones completas para hidratar el dedup
- **Dónde:** `crates/server/src/log/tiered_log.rs:679-720` (`read_newest`, con `ReadBudget::ops(usize::MAX)` y seq 0 en `:694-697`), `tiered_log.rs:169-173`; se llama en cada spawn (`room.rs:164-171`). `hot_buffer.rs:137-142` (`rehydrate`).
- **Medición:** 10.000 operaciones de 20 KB: 42 ms de apertura con ventana de dedup 0 contra 1.023 ms con 10.000 (24×); en frío se suma la descompresión.
- **Escenario:** con `idle_timeout` de 10 min, las salas medianas se cierran y reabren todo el tiempo; cada reapertura, aunque la dispare un heartbeat, lee cientos de MB y acerca a los que esperan el spawn lock a los 5 s de `ACTOR_TIMEOUT`. Agravante: `rehydrate` carga hasta 8 MiB de operaciones viejas como si fueran frescas.
- **Solución:** guardar al sellar un segmento sus pares `(mutation_id, seq)` en un archivo aparte y leer solo eso al abrir. Para el HotBuffer, no cargar operaciones de segmentos sellados.

### Tests

#### DEF-131 · Medio · Confirmado · Sin test de que `Bootstrapping` no frene la poda
- **Dónde:** `crates/server/src/actor/lease.rs:328-341` (filtro en `:338`); `ARCHITECTURE.md:127`.
- **Evidencia:** el mutante `.filter(|c| c.state == Connected || c.state == Bootstrapping)` deja toda la suite del servidor en verde, en `2a5f7b3` y en `698e8d6`.
- **Solución:** en `src/actor/tests/lease.rs`, registrar `active` con `Some(20)` y `newbie` con `None` (tail 10) y comprobar `min_connected_ack_seq() == Some(20)`. El auditor verificó que este test mata el mutante.

#### DEF-132 · Medio · Confirmado · Sin test de que la poda por TTL/cuota no deje huecos
- **Dónde:** `crates/server/src/log/tiered_log.rs:471-497` (`expire`, `break` en `:487`); `ARCHITECTURE.md:144`, regla 4 de `GEMINI.md`.
- **Evidencia:** cambiar `break` por `continue` deja todo en verde, en los dos commits. `failed_compression_does_not_skip_expiry` no lo detecta. El escenario es real: un warm que falla la verificación de compresión queda delante de segmentos cold.
- **Solución:** test en `src/log/tests/tiered_log.rs`: corromper `segment_1_5.wal`, mantenimiento, `max_room_disk_bytes = 1`, mantenimiento otra vez, y `segment_6_10.wal.zst` tiene que seguir existiendo. Verificado contra el mutante.

#### DEF-133 · Medio · Confirmado · Sin test de hueco dentro de un batch
- **Dónde:** `crates/storage/src/memory/state.rs:77-96` (`validate_batch`, comprobación en `:86`), usado por los dos motores. El único test es `crates/storage/tests/common/mod.rs:195-212` (`sequence_gap_is_rejected`, un solo op con seq 42).
- **Evidencia:** validar solo el primer op deja la suite de storage en verde en los dos commits; un batch `[1, 3]` se escribiría al WAL y la sala no volvería a abrir.
- **Solución:** `apply_batch(vec![insert(1), insert(3)])` tiene que dar `SequenceMismatch` y la sala tiene que reabrir, en los dos motores.

#### DEF-134 · Bajo · Confirmado · Nada comprueba que la compresión a cold use `spawn_blocking`
- **Dónde:** `crates/server/src/log/cold_disk.rs:106-115` (`spawn_blocking` en `:112`).
- **Cambió con el refactor:** en `2a5f7b3`, `test_tiered_log_async_maintenance_spawn_blocking` prometía comprobarlo y no lo hacía. `b012e1a` lo borró por duplicado de `test_tiered_log_cold_compression_and_read`. Desapareció el test engañoso, pero no el hueco: comprimir inline sigue dejando la suite en verde (408/408).
- **Solución:** que un fail point registre el `ThreadId` en `compress_warm_segment_sync` y que un test con runtime `current_thread` compruebe que no es el suyo.

#### DEF-140 · Bajo · Confirmado (coordinador) · El test de overrides ya no verifica que la config llegue a la sala
- **Dónde:** `crates/server/tests/server_integration_tests.rs:236-243` compara `effective["lease_timeout_secs"]` y `effective["snapshot_demand_ttl_secs"]` contra `server.config`; `crates/server/tests/common/mod.rs:112-118` arma la config con `..ServerConfig::default()`, cuyos valores salen de `RoomLifecyclePolicy::default()` (`crates/server/src/config.rs:124`, `:129`).
- **Cambió con el refactor:** en `2a5f7b3` el test configuraba 60 s en los dos campos, distinto de los defaults de la política (90 s y 7 días). Con los fixtures compartidos, config y default coinciden y el assert pasa aunque la config se ignore.
- **Evidencia:** cambiar `config.default_lifecycle_policy()` por `RoomLifecyclePolicy::default()` en `actor/manager.rs:179` deja el test nuevo en verde; el viejo, portado, falla (`left: 90, right: 60`). En toda la suite solo lo detecta el unitario `demand_that_is_not_renewed_expires`, y nada verifica el `lease_timeout_secs`.
- **Solución:** arrancar el test con `TestServer::start(|c| { c.lease_timeout_secs = 60; c.snapshot_demand_ttl_secs = 60; })` y comparar contra el literal.

#### DEF-141 · Bajo · Confirmado · Se perdió la cobertura del orden secuencia → esquema en `validate_batch`
- **Dónde:** `crates/storage/src/memory/state.rs:77-96`; `crates/storage/tests/common/mod.rs:195` (`sequence_gap_is_rejected`, con una fila válida).
- **Cambió con el refactor:** en `2a5f7b3`, `disk_wal_resilience_tests.rs:439` (`test_disk_apply_batch_sequence_mismatch`) mandaba una fila inválida con seq 42 y exigía `SequenceMismatch`, así que probaba, sin decirlo, que la secuencia se valida antes que el esquema. El helper compartido usa una fila válida.
- **Evidencia:** mover `validate_operation` antes del chequeo de secuencia deja la suite de storage en verde; el test viejo, portado, falla con `SchemaValidation`.
- **A decidir:** el test viejo de memoria ya usaba una fila válida, así que puede que el orden nunca haya sido un contrato. Si importa (que un hueco se reporte como hueco aunque el batch traiga además una fila inválida), agregar ese caso a `sequence_gap_is_rejected`; si no, documentarlo y descartar el hallazgo.

### Documentación y CI

#### DEF-135 · Bajo · Confirmado (coordinador) · `ROADMAP.md` contradice el código
- `ROADMAP.md:670`, `:742` dan como pendiente `CryptoConcurrencyBounds`, que `:626`, `:709` marcan como hecho y está en `crates/core/src/crypto.rs:16-22`.
- `ROADMAP.md:386` dice HMAC-SHA256; el código usa BLAKE3 con clave (`api/auth.rs:70-76`).
- `ROADMAP.md:392` lista `/snapshot-chunk`; las rutas reales son `/snapshot/chunk`, `/snapshot/upload` y `/snapshot/upload-chunk` (`router.rs:122-127`), y falta `/ack` (`router.rs:116`).
- `ROADMAP.md:372` dice que un `Dormant` que reconecta recibe `BehindCompaction`; el código decide por el cursor (`ARCHITECTURE.md:129`).
- **Solución:** corregirlas, o marcar §4.3 como diseño histórico que remite a `ARCHITECTURE.md`.

#### DEF-136 · Bajo · Confirmado · Portabilidad de snapshots sin la salvedad de wasm32
- **Dónde:** `ARCHITECTURE.md:355`; en wasm32, `deserialize_zstd` y `decompress_bounded` fallan (`crates/storage/src/snapshot.rs:189-199`, `:292-297`), y los peers nativos generan snapshots Zstd.
- **Ya conocido:** DEF-54, diferido a la Fase 4. Lo nuevo es que la documentación vigente lo omite.
- **Solución:** agregar la limitación en §8.

#### DEF-137 · Bajo · Confirmado · Quedan afirmaciones sobre la línea de caché
- **Dónde:** `crates/core/src/value/row.rs:12` ("40 bytes, fits a cache line"; el refactor `423c465` suavizó la frase, pero la afirmación sigue) y `crates/storage/src/disk/format.rs:14` ("L1 cache line aligned", sin cambios). DEF-113 las quitó de los demás documentos.
- **Solución:** dejar solo los tamaños.

#### DEF-138 · Bajo · Confirmado · Links de rustdoc a ítems privados
- **Dónde** (`cargo doc --workspace --no-deps`, 7 warnings en `698e8d6`):
  - `core/src/protocol/wal_frame.rs:304` → `MAX_PAYLOAD_CHECKS`
  - `storage/src/disk/mod.rs:232` → `DiskRoom` y `compactor::compact_room_cow` (este segundo es nuevo, de `591bde9`)
  - `storage/src/disk/mod.rs:615` → `install_applied_snapshot`
  - `server/src/actor/lease.rs:51` → `PersistedClient`
  - `server/src/actor/manager.rs:446` → `SHUTDOWN_CONCURRENCY`
  - `server/src/log/policy.rs:187` → `PolicySetting`
- **Solución:** quitar los corchetes o hacer públicos los ítems; agregar a la CI `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.

#### DEF-139 · Bajo · Confirmado · La CI no verifica MSRV ni clippy en wasm32
- **Dónde:** `.github/workflows/ci.yml` solo usa `stable`, aunque `Cargo.toml:13` declara `rust-version = "1.85"` y el README (`:49`) dice "1.88+ to run the tests". El job wasm solo hace `cargo build`, sin clippy ni `-D warnings`. Hoy clippy en wasm32 está limpio, pero nada lo protege.
- **Solución:** un job con 1.85 (`cargo build --workspace --lib --bins`) y otro con 1.88 (`cargo test`); en el job wasm, `cargo clippy --target wasm32-unknown-unknown -p zemdb-core -p zemdb-storage -p zemdb-client -- -D warnings`.

#### DEF-142 · Bajo · Confirmado (coordinador) · Documentación que el refactor dejó desactualizada
- `crates/storage/src/memory/state.rs:137`: el doc de `RoomSnapshotPayload` dice "serialized by compaction", pero desde `cf4c199` la compactación serializa `RoomSnapshotRef`. Nadie serializa ya `RoomSnapshotPayload` y su `derive(Serialize)` quedó muerto (los bytes son idénticos, mismos campos en el mismo orden).
- `ARCHITECTURE.md:26` lista `as_slice()` entre las APIs de `PrimaryKey` y `CompactRow`; `ec83d9e` lo eliminó (quedan `values()` y `AsRef<[Value]>`).
- `crates/server/src/actor/room.rs:914-915`: el comentario de `panic_message` ("Text of a caught panic payload…") quedó pegado arriba del de `room_dir` (`39ab132`), y `panic_message` quedó sin doc.
- `ROADMAP.md:73` menciona `with_default_origin`, eliminado en `f069685` (entrada histórica; corregirla es opcional).
- **Solución:** corregir el doc y quitar `Serialize` de `RoomSnapshotPayload`; `as_slice()` → `values()`; mover el comentario a `panic_message`.

### Ya conocidos que siguen abiertos

- **DEF-85:** la ventana de dedup se limita al log retenido más la LRU. Matiz nuevo: con poda por cursores y apagado por inactividad, la ventana real puede ser de minutos, no de días. Además, `SequencedOperation` no lleva `mutation_id`, así que un cliente no puede reconocer su propio commit sin confirmar en un sync.
- **DEF-34:** no hay `reload_schema` en storage (DEF-115 es distinto, aunque relacionado).
- **DEF-81:** un miembro malicioso puede subir un snapshot falso.
- **Residuo de DEF-95:** una conexión SSE semiabierta retiene el permiso de conexión hasta que TCP falla.
- **Residuos de la auditoría de calidad:** `mutation/op.rs` ("zero heap pointers"), `wal_frame.rs` ("little-endian" para el magic) y tres comentarios desactualizados en tests.

### Observaciones (no son defectos)

- `DeregisterClient` se describe como "advance retention immediately" (`messages.rs`), pero la retención avanza en el próximo ack o tick lento (≤ 30 s).
- Las ramas `#[cfg(target_arch = "wasm32")]` solo se compilan, nunca se ejecutan en tests (suite wasm planificada para la Fase 5).
- Los tokens no se pueden revocar (stateless por diseño): siguen valiendo tras un deregister y para una sala recreada con el mismo id (ver DEF-118).
- El refactor rompió API pública de `zemdb_server` (`ServerConfig` ya no deriva `Serialize`; se privatizaron constantes de `config` y `error` y `MAX_CONCURRENT_STREAMS`; se borró `ClientLeaseTracker::is_registered`). Nada del workspace la usa; solo afectaría a un consumidor externo.
- El fail point `relay_remove_file` pasó a llamarse `remove_file` y ahora cubre también los borrados del log, del roster y de la metadata (solo existe en builds de test).
- `ClientLeaseTracker` se reexporta como `pub` (`crates/server/src/lib.rs`) y fuera del actor solo lo usan los tests.

---

## 5. Áreas revisadas sin hallazgos

- **Durabilidad de storage:** fsync antes del ack en `apply_batch`, `PendingWrite` ante cancelación, publicación atómica de batches para lectores, truncado de torn writes, detección de huecos, todos los puntos de crash de la compactación y del fold de `.wal.compacting`, `apply_snapshot` con validación completa y re-chequeo del head bajo el lock.
- **Log del servidor:** append con `sync_data` y sync del directorio, rotación, compresión a cold con verificación antes de borrar, `log_meta.json` atómico antes de podar, `head_seq` tras una poda total, contigüidad del HotBuffer.
- **Protocolo y rangos:** `fetch_deltas` (RAM y disco), `is_behind_tail`, `has_more`, catchup de 100 operaciones: sin off-by-one, pérdidas ni duplicados. Toda compactación que deja atrás a un cliente se le señala con 410.
- **Idempotencia dentro de la ventana de dedup**, incluso tras un fallo de append o de rotación.
- **Estados del lease y squashing** (latente: las 9 combinaciones equivalen a enviar en orden).
- **Autenticación:** `AdminAuth` antes de leer el body, tokens con BLAKE3 y comparación en tiempo constante, tokens atados a la sala del path, `ensure_payload_identity`, secretos validados al arrancar. Ningún endpoint accesible sin el token correcto ni con el de otra sala.
- **Decodificación acotada (D44)**, límites de headers y body, relay (aritmética de `UploadLayout`, nombres de archivo sin path traversal, hash y envelope verificados antes de promover), descompresión acotada de snapshots.
- **Concurrencia:** spawn lock y ciclo de vida del manager, cancelación en `ask`, `select!` del actor, captura de panics, `block_in_place`, ningún lock std ni guard de `DashMap` a través de `.await`, relay con `run_to_completion`, SSE, apagado ordenado.
- **Rust:** ningún panic alcanzable desde la red, aritmética de secuencias protegida, sin casts truncantes alcanzables en 32 bits/wasm32.
- **Rendimiento:** camino de commit (un fsync, decisión D4), catchup con presupuesto, `SegmentIndex`, dedup acotado (DEF-96), compresión acotada por pasada (DEF-99), FDs acotados por sala, relay.
- **Tests:** 15 de 19 mutantes sobre áreas críticas (auth cross-room, BehindCompaction, relay, idempotencia tras reinicio, rotación, recuperación de WAL) los mata la suite. Los 43 `sleep` son seguros; la suite pasó con load ~160.
- **Documentación numérica:** coinciden con el código todos los defaults, rangos, constantes, tamaños de structs, magics y endpoints de `ARCHITECTURE.md` y del README.
- **Refactor de calidad (`2a5f7b3..698e8d6`):**
  - **Handlers del actor:** cada camino responde una sola vez y en el mismo orden respecto de dedup, retención, append y poda.
  - **Log:** el mismo formato de nombres de segmento y el mismo orden de fsync y rename.
  - **`durable`:** no se perdió ningún fsync de directorio.
  - **Relay:** las mismas validaciones antes de promover.
  - **Extractores de auth:** mismo orden, sin leer el body antes de autenticar.
  - **Config:** mismos defaults.
  - **Core:** validaciones de esquema equivalentes (los dos chequeos quitados eran inalcanzables), `canonical()` de -0.0, mismas opciones de bincode, frames WAL con los mismos bytes.
  - **Storage:** el scan por lotes compartido es una máquina de estados equivalente; `run_blocking` y el manejo de fallas y recovery factorizados conservan los efectos y su orden.
  - **Tests:** ningún assert reemplazado por `is_ok`/`is_err`, sin pasar de tiempo pausado a `sleep` reales, y varios asserts más estrictos (secuencias completas en lugar de largo/primero/último, `Operation` completas, content-type de los errores binarios).
