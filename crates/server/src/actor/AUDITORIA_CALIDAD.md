# Auditoría de calidad — `crates/server/src/actor/`

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `room.rs`, `manager.rs`, `lease.rs`, `command.rs`, `snapshot_demand.rs`, `mod.rs` (código fuente) y revisión rápida de `tests/room.rs`, `tests/manager.rs`, `tests/lease.rs`, `tests/snapshot_demand.rs` (solo duplicación de setup/helpers).

## Resumen

- Es un módulo **maduro y bien escrito**: respeta "validar primero, mutar después" (todas las rechazos de `Commit` ocurren antes de `tiered_log.append`; `register_client`/`deregister_client` escriben el roster antes de tocar memoria), usa RAII para los locks de spawn (`SpawnGuard`, `SpawnLockWait`) y documenta con cuidado el orden de efectos (cerrar el mailbox antes de responder, persistir cursores antes de podar). No encontré referencias a códigos de auditoría ni narrativa histórica en el código.
- **Ahorro estimado: ~115–145 líneas sobre 2.680 de código fuente (≈4–5,5 %)**, sin tocar la arquitectura. El grueso está en `room.rs` (handlers que repiten `let _ = reply.send(Err(..)); return;`) y en `command.rs` (`RoomCommand::reject` con 13 brazos idénticos de 3 líneas).
- Principales oportunidades: (1) que los handlers de `Sync`/`Ack`/`Heartbeat`/`Register` devuelvan `Result` y se responda una sola vez, con un helper para el chequeo "cursor más allá del head" repetido 4 veces; (2) colapsar `RoomCommand::reject` a un brazo por línea; (3) pequeñas simplificaciones en `lease.rs` (`min_connected_ack_seq`, `observe`, `dirty |=`) y un helper `durable` para "borrar el `.tmp` huérfano ignorando `NotFound`" repetido en 3 archivos.
- Comentarios: densidad alta (≈18 % en `room.rs`) y en general de buena calidad (explican el *por qué*). Lo recortable son ~35 comentarios de pasos numerados (`// 0.`, `// 7.`, …) que repiten el nombre de la función o la línea de abajo, y algunos doc-comments que repiten la firma.
- Tests: hay duplicación de setup entre `tests/room.rs` y `tests/manager.rs` (construcción de `RoomManager` + registry + relay + timings, `test_schema`, `POLL_CEILING`) y bucles de sondeo escritos a mano; ahorro estimado en tests ~60–90 líneas.

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| `room.rs` | 999 | 727 | 99 | 82 | 91 | ~55–65 |
| `manager.rs` | 674 | 480 | 107 | 28 | 59 | ~12–15 |
| `lease.rs` | 601 | 430 | 115 | 4 | 52 | ~20–25 |
| `command.rs` | 213 | 157 | 35 | 0 | 21 | ~26 |
| `snapshot_demand.rs` | 180 | 122 | 37 | 3 | 18 | ~5 |
| `mod.rs` | 13 | 12 | 0 | 0 | 1 | 0 |
| **Subtotal fuente** | **2.680** | **1.928** | **393** | **117** | **242** | **~115–145 (4–5,5 %)** |
| `tests/room.rs` | 1.857 | 1.606 | 26 | 67 | 158 | ~35–50 |
| `tests/manager.rs` | 1.065 | 926 | 4 | 50 | 85 | ~15–25 |
| `tests/lease.rs` | 654 | 546 | 2 | 15 | 91 | ~10–15 |
| `tests/snapshot_demand.rs` | 269 | 222 | 1 | 8 | 38 | 0 |

## Comentarios

**Densidad y calidad.** Alta en `room.rs` (181 líneas de doc+comentario de 999) y `manager.rs` (135 de 674); casi todo explica invariantes, orden de efectos o decisiones de crash-safety, y eso hay que conservarlo. No hay referencias a códigos de auditoría (`C-xx`, `DEF-xx`, `M-xx`, "Lote") ni narrativa de "antes hacíamos X".

**Qué recortar:**

- **Pasos numerados que repiten el código** (39 comentarios `// N.` entre `room.rs` y `manager.rs`). Los redundantes:
  - `room.rs:162` ("3. Monotonic head sequence derived directly from TieredLog"), `room.rs:165` ("4. Open client lease tracker"), `room.rs:168` ("5. Channels").
  - `room.rs:469`, `room.rs:630`, `room.rs:685` ("0. Check if client is registered in the room roster", tres veces, delante de `ensure_registered`).
  - `room.rs:475`, `room.rs:636`, `room.rs:694` (chequeo contra `head_seq`; el de `:694` además exagera: "to prevent catastrophic log truncation").
  - `room.rs:555` ("7. Record in DedupLruCache"), `room.rs:558` ("8. Advance local head sequence"), `room.rs:565` ("10. Broadcast signal-only SSE event…"), `room.rs:645`, `room.rs:652` ("3. Fetch continuous multi-tier delta batch").
  - `manager.rs:239`, `:244`, `:247`, `:264` ("5. Resolve Schema definition from registry"), `:270`, `:488` ("Validate schema exists in SchemaRegistry").
  - Si se aplica Q1, desaparecen solos los de `handle_sync`/`handle_ack`. Conviene quitar la numeración en los que se conservan (los saltos 5 → 5b → 6 y 3 → 3b muestran que ya se insertaron pasos a mano).
- **Comentarios que exceden 100 columnas**: `room.rs:529` (116: "6. Write-Through append to TieredLog (Hot Buffer RAM + synchronous active.wal disk sync with mutation_id)"; basta "6. Durable append (RAM + fsync de `active.wal`)"), `room.rs:74` (doc de `RoomActor`, 118), `manager.rs:514`, `lease.rs:185`.
- **Doc-comments que repiten la firma**: `lease.rs:360` ("Checks if a client is registered…"), `lease.rs:372`, `lease.rs:377`, `manager.rs:422` ("Gets an existing active sender for the room, if running."), `manager.rs:456` ("Returns a list of all currently active RoomIds."). Se pueden dejar en una línea más útil o quitar.
- **Redundancia entre docs**: "Production always uses [`ActorTimings::default`]; tests shorten them." aparece en `room.rs:42-43` y `room.rs:130-131` (y una variante en `manager.rs:158`). En `open` alcanza con "with the given `timings`".

**Qué conservar (ejemplos):** `room.rs:209-215` (manejo de pánico y qué recibe cada caller), `room.rs:290-292`, `room.rs:307-311`, `room.rs:484-486` (dedup antes de rechazos por estado), `room.rs:501-504`, `room.rs:521-523`, `room.rs:530-533` + `:542-543` (fail-stop tras fallo de append y por qué se cierra el mailbox antes de responder), `room.rs:575-577`, `room.rs:603-605`, `room.rs:668-670`, `room.rs:703-705`, `room.rs:882-884` (persistir cursores antes de borrar segmentos), `room.rs:889-891`, `room.rs:897-898`; `manager.rs:104-109` y `:127-131` (razonamiento de `strong_count == 2`), `manager.rs:252-256`, `:392-393` (por qué no se toma el spawn lock en la tarea), `:502-503`, `:583-585`, `:595-599`, `:605-608`, `:618-620`; `lease.rs:37-39`, `:52-55`, `:63-65`, `:72-77`, `:432-439` (efecto de un crash sobre la última actividad), `:485-487`; `snapshot_demand.rs:1-6`, `:15-17`, `:109-110`. Las notas de compatibilidad de formato (`lease.rs:424-425`, `:458-459`) no son narrativa histórica: justifican por qué se aceptan dos formatos de archivo.

## Sugerencias

### Q1. Handlers de `room.rs` que devuelven `Result` y responden una vez, más helper para "cursor más allá del head"
- **Dónde:** `room.rs:332-362` (Register), `room.rs:623-677` (`handle_sync`), `room.rs:679-723` (`handle_ack`), `room.rs:728-745` (`handle_heartbeat`); chequeo `InvalidSequence` repetido en `room.rs:340-346`, `:476-482`, `:637-643`, `:695-701`.
- **Problema:** cada rechazo ocupa 3–7 líneas con la forma `let _ = reply.send(Err(..)); return;`; `handle_ack` usa un `match` de 7 líneas solo para propagar el error de `ensure_registered`. El error `InvalidSequence { expected: self.head_seq, actual }` se construye igual en 4 sitios.
- **Propuesta:** `fn sync(&mut self, client_id: &ClientId, from_seq, max_batch_size) -> Result<SyncBatchResponse, ServerError>` (ídem `ack`, `heartbeat`, `register`) y en `handle_command` `let _ = reply.send(self.sync(&client_id, from_seq, max_batch_size));`. Helper:
  ```rust
  /// A cursor beyond the head was never delivered by this room.
  fn check_within_head(&self, seq: SequenceNumber) -> Result<(), ServerError> {
      if seq > self.head_seq {
          return Err(ServerError::InvalidSequence { expected: self.head_seq, actual: seq });
      }
      Ok(())
  }
  ```
  `ack` queda:
  ```rust
  let stored_cursor = self.ensure_registered(client_id)?;
  self.check_within_head(ack_seq)?;
  // (conservar el comentario de :703-705)
  if self.tiered_log.is_behind_retention(ack_seq.max(stored_cursor)) {
      self.reject_behind_log(client_id, ack_seq);
      return Err(ServerError::BehindCompaction);
  }
  self.observe(client_id, Some(ack_seq));
  self.prune_to_retention_floor();
  Ok(self.head_seq)
  ```
  En `register`: `if let Some(seq) = current_seq { self.check_within_head(seq)?; }` y `if self.lease_tracker.register_client(..)? == ClientState::Bootstrapping { self.request_snapshot(); }` antes de construir la respuesta (mismo orden: `request_snapshot` ocurre antes de `usable_snapshot_seq()`). Además saca ~30 líneas de `handle_command` (hoy ~135 líneas, `room.rs:325-459`).
- **Semántica:** en todos los caminos actuales `reply.send` es el último efecto antes de salir, así que construir el `Result` y enviarlo al final preserva el orden (incluido `observe` antes de calcular `snapshot_wanted_for`). En `sync`, el `request_snapshot()` ante `BehindCompaction` del `fetch_deltas` (`:671-673`) se mantiene antes del `return Err(e)`.
- **Ahorro estimado:** ~30–35 líneas · **Riesgo:** bajo · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — en Register/sync/ack/heartbeat cada camino envía exactamente una respuesta y `reply.send` es el último efecto (no hay nada después en `handle_command`), así que devolver `Result` y enviarlo en `handle_command` preserva el orden: `reject_behind_log` y `request_snapshot` siguen antes del `Err`, `observe` antes de `snapshot_wanted_for`, `register_client` con error no dispara `request_snapshot`. `ClientState` deriva `PartialEq` (`lease.rs:19`), así que `== ClientState::Bootstrapping` compila. Líneas citadas correctas.

### Q2. `RoomCommand::reject`: un brazo por línea
- **Dónde:** `command.rs:171-211`
- **Problema:** 13 brazos idénticos de 3 líneas (`=> { let _ = reply.send(Err(err)); }`). No se pueden unir con `|` porque cada `reply` tiene otro tipo.
- **Propuesta:** usar una expresión, que rustfmt deja en una línea:
  ```rust
  match self {
      RoomCommand::RegisterClient { reply, .. } => drop(reply.send(Err(err))),
      RoomCommand::DeregisterClient { reply, .. } => drop(reply.send(Err(err))),
      // …
      RoomCommand::Shutdown { reply } => drop(reply.send(())),
  }
  ```
  `drop` del `Result` equivale a `let _ =` (el `Err` devuelto contiene el valor no entregado y se descarta igual). Con los lints del workspace (`Cargo.toml:18-21`) no aplica `drop_non_drop` porque el tipo tiene drop glue; **a verificar** con clippy. Alternativa sin `drop`: `{ let _ = …; }` en una línea con `#[rustfmt::skip]` en el `match`.
- **Ahorro estimado:** ~26 líneas · **Riesgo:** bajo · **Prioridad:** alta
- **Verificación (2.ª pasada):** ⚠️ corregida — la propuesta es válida y equivalente (el `Err` no entregado se descarta en el acto en ambos casos), pero el motivo del lint es otro: en `Shutdown` el valor es `Result<(), ()>`, que es `Copy` y sin drop glue, y aun así no salta `dropping_copy_types` (rustc) ni `drop_non_drop` (clippy) porque ambos eximen `drop(llamada)` cuando es el cuerpo directo de un brazo de `match`. Condición: escribir `=> drop(..),` sin llaves; con `=> { drop(..) }` la exención no aplica.

### Q3. `handle_commit`: rechazos con `?` mediante un `commit` interno
- **Dónde:** `room.rs:461-598`
- **Problema:** seis rechazos con `let _ = reply.send(Err(..)); return ControlFlow::Continue(());` (3–7 líneas cada uno). Es el único handler que además devuelve `ControlFlow`, por eso no encaja directamente en Q1.
- **Propuesta:** `fn commit(&mut self, …, flow: &mut ControlFlow<()>) -> Result<CommitResponse, ServerError>` que marca `*flow = ControlFlow::Break(())` donde hoy se devuelve `Break` (fallo de `append` en `:534-551` y `rotation_error` en `:578-589`), y `handle_commit` queda en `let mut flow = Continue(()); let res = self.commit(.., &mut flow); let _ = reply.send(res); flow`. Los pasos 0 y 1 pasan a `self.ensure_registered(&client_id)?; self.check_within_head(last_ack_seq)?;`. El `receiver.close()` sigue ocurriendo antes de la respuesta, que es la garantía documentada en `:542-543`. **A verificar**: el parámetro de salida es menos idiomático; si no convence, aplicar solo los pasos 0–1 con `check_within_head` (~8 líneas).
- **Ahorro estimado:** ~12–15 líneas · **Riesgo:** medio · **Prioridad:** media
- **Verificación (2.ª pasada):** ✅ confirmada — con `*flow = Break` en `:534-551` y `:578-589`, `receiver.close()` sigue ocurriendo antes del `reply.send` (que pasa a `handle_commit`), y en rotación `snapshot_wanted_for` sigue calculándose después del `close`. Tras el `append` durable no queda ningún `?`, así que el commit persistido no puede convertirse en `Err`; mantener esa regla explícita en un comentario. Alternativa sin parámetro de salida: devolver `(Result<..>, ControlFlow<()>)`.

### Q4. Simplificaciones locales en `lease.rs`
- **Dónde:** `lease.rs:338-358`, `lease.rs:242-245`, `lease.rs:254-262`, `lease.rs:327-329`, `lease.rs:197-201` + `:246-250`
- **Problema / Propuesta:**
  - `min_connected_ack_seq`: el `if self.clients.is_empty() { return None; }` (`:339-341`) sobra: con el mapa vacío `any` da `false` y `min()` da `None`. Y el `let any_disconnected = …; if any_disconnected` puede ir en un solo `if self.clients.values().any(|c| c.state == ClientState::Disconnected) { return None; }`. El comentario `:343` ("If any non-dormant member is Disconnected") es tautológico, porque un `Disconnected` nunca es `Dormant`.
  - `observe`: `let cursor = reported_cursor.map_or(entry.last_ack_seq, |r| r.max(entry.last_ack_seq));` en vez del `match` de 4 líneas.
  - `observe` y `check_timeouts`: `if changed { self.dirty = true; }` → `self.dirty |= changed;` (y `self.dirty |= modified;`). Mismo efecto: nunca baja `dirty` a `false`.
  - La regla "behind tail → `Bootstrapping`, si no `Connected`" está duplicada en `register_client` y `observe`. Un `ClientState::active_for(cursor, tail_seq)` deja la regla del doc de `ClientState` (`:15-17`) en un solo sitio. En líneas sale neutro; lo que gana es claridad.
- **Ahorro estimado:** ~10–12 líneas · **Riesgo:** bajo · **Prioridad:** media

### Q5. Helper `durable` para borrar el `.tmp` huérfano ignorando `NotFound`
- **Dónde:** `lease.rs:84-88`, `manager.rs:657-661` (y fuera de alcance `log/tiered_log.rs:864-868`)
- **Problema:** el mismo `match fs::remove_file(durable::tmp_path_for(p)) { Ok(()) => {}, Err(e) if NotFound => {}, Err(e) => return Err(e.into()) }` en 3 sitios.
- **Propuesta:** `pub(crate) fn remove_stale_tmp(path: &Path) -> io::Result<()>` en `durable.rs`, y en cada sitio `durable::remove_stale_tmp(&path)?;`. Se mantiene la propagación de cualquier error distinto de `NotFound`.
- **Ahorro estimado:** ~8 líneas en `actor/` (~12 en total, menos ~7 del helper) · **Riesgo:** bajo · **Prioridad:** media

### Q6. Comentarios de pasos numerados redundantes
- **Dónde:** ver la sección "Comentarios" (`room.rs:162,165,168,469,475,555,558,565,630,636,645,652,685,694`; `manager.rs:239,244,247,264,270,488`).
- **Propuesta:** borrarlos y quitar la numeración de los que se conservan. Acortar `room.rs:529` a una línea.
- **Ahorro estimado:** ~12–15 líneas · **Riesgo:** nulo · **Prioridad:** media

### Q7. Constantes mágicas en `room.rs`
- **Dónde:** `room.rs:610` (`fetch_deltas(last_ack_seq, 100)`), `room.rs:653` (`clamp(1, 1000)`), `room.rs:169` (`mpsc::channel(1024)`), `room.rs:170` (`broadcast::channel(256)`).
- **Problema:** límites de protocolo y de capacidad sin nombre ni explicación, junto a constantes que sí están documentadas (`FAST_TICK_PERIOD`, `SLOW_TICK_PERIOD`).
- **Propuesta:** `const COMMIT_CATCHUP_BATCH: u32 = 100;`, `const MAX_SYNC_BATCH: u32 = 1000;`, `const MAILBOX_CAPACITY: usize = 1024;`, `const EVENTS_CAPACITY: usize = 256;`, cada una con una línea de doc. Suma líneas, pero gana claridad.
- **Ahorro estimado:** −6 a −8 líneas (suma) · **Riesgo:** nulo · **Prioridad:** baja

### Q8. `manager.rs`: regla de "olvidar el lock" duplicada en dos `Drop` y `unreachable!` innecesario
- **Dónde:** `manager.rs:110-115`, `manager.rs:132-137`, `manager.rs:297-300`
- **Problema:** ambos `Drop` repiten `locks.remove_if(room_id, |_, lock| Arc::ptr_eq(lock, X) && Arc::strong_count(lock) == 2)`: el invariante delicado del módulo vive en dos copias. En `lock_room`, `match wait.acquiring.as_mut() { Some(a) => a.await, None => unreachable!(..) }` ocupa 4 líneas.
- **Propuesta:** `fn forget_lock_if_unused(locks: &DashMap<RoomId, Arc<Mutex<()>>>, room_id: &RoomId, lock: &Arc<Mutex<()>>)` llamado desde ambos `Drop` (mismo orden: en `SpawnLockWait` se sigue haciendo antes `self.acquiring = None`; en `SpawnGuard` se llama con `OwnedMutexGuard::mutex(&self.guard)` mientras el guard sigue vivo). Y `let guard = wait.acquiring.as_mut().expect("the acquisition is set above").await;` (mismo pánico en el caso imposible).
- **Ahorro estimado:** ~4–5 líneas · **Riesgo:** bajo · **Prioridad:** media (por centralizar el invariante, más que por las líneas)

### Q9. `parse_roster` y conversiones de tiempo en `lease.rs`
- **Dónde:** `lease.rs:579-594`, `lease.rs:563-577`, `lease.rs:475` + `:527-529`
- **Propuesta:**
  - Demanda: `raw_demand.filter(|raw| !raw.is_null()).and_then(|raw| { let t = serde_json::from_value::<PersistedDemand>(raw).ok().and_then(|d| d.to_time()); if t.is_none() { warn!(…); } t })`. Mismo resultado y mismo warning; ~4 líneas menos.
  - El `Err(e) => { warn!(…) }` de 5 líneas (`:569-575`) puede ser `Err(e) => warn!(path = %path.display(), error = %e, "Skipping invalid entry in clients roster"),` (rustfmt no reformatea los argumentos de `tracing` con `%`).
  - `UNIX_EPOCH.checked_add(Duration::from_millis(ms))` está en `:475` y en `PersistedDemand::to_time` (`:528`): un `fn from_unix_ms(ms: u64) -> Option<SystemTime>`, simétrico a `unix_ms` (`:505`), lo unifica.
- **Ahorro estimado:** ~6–8 líneas · **Riesgo:** bajo · **Prioridad:** baja

### Q10. Pequeños retoques en `room.rs`
- **Dónde y propuesta:**
  - `room.rs:892-895`: `let retention_floor = self.usable_snapshot_seq().map_or(min_ack, |snap| min_ack.min(snap));` (−3).
  - `room.rs:200-204`: `info!(room = %room_id, head_seq = %head_seq, "Opened RoomActor");` (−4).
  - `room.rs:466-467`, `:628`, `:683`, `:731`: rutas completas `tokio::sync::oneshot::Sender` y `zemdb_core::mutation::Operation`. Importar `oneshot` y `Operation` como en `command.rs` (0 líneas, más legible).
  - `room.rs:145` y `manager.rs:401-403` construyen la misma ruta `data_dir/rooms/<id>`. Un único `pub(crate) fn room_dir(data_dir: &Path, room_id: &RoomId) -> PathBuf` deja el layout en disco en un solo lugar (~0–1 línea; los tests también lo repiten, por ejemplo `tests/manager.rs:292-294`).
- **Ahorro estimado:** ~7–8 líneas · **Riesgo:** bajo · **Prioridad:** baja

### Q11. `snapshot_demand.rs`: `extend_until` y `restore`
- **Dónde:** `snapshot_demand.rs:80-91`, `:103-105`
- **Propuesta:** `self.active.get_or_insert_with(|| ActiveDemand { expires_at, designee: None, excluded: HashSet::new() }).expires_at = expires_at;` (mismo efecto: crea la demanda si no existe y siempre fija `expires_at`). `wall_now.duration_since(renewed_at).unwrap_or_default()` cabe en una línea.
- **Ahorro estimado:** ~4–5 líneas · **Riesgo:** bajo · **Prioridad:** baja

### Q12. API pública solo usada por tests en `lease.rs` (a verificar)
- **Dónde:** `lease.rs:361-363` (`is_registered`), `lease.rs:378-380` (`get_client_mut`)
- **Problema:** `is_registered` solo se usa en `actor/tests/lease.rs` y equivale a `get_client(id).is_some()`. `get_client_mut` permite cambiar `state`/`last_ack_seq` sin marcar `dirty`, lo que rompe el encapsulamiento del tracker; lo usan los tests unitarios y `tests/server_integration_tests.rs:1670,1689`, así que no puede ser `#[cfg(test)]`.
- **Propuesta:** reemplazar `is_registered` por `get_client(..).is_some()` en los tests y borrarla (−4). Para `get_client_mut`, como mínimo documentarla como "solo para tests" (`#[doc(hidden)]`). Es un cambio de API pública re-exportada en `lib.rs:14`: **a verificar**.
- **Ahorro estimado:** ~4 líneas · **Riesgo:** bajo · **Prioridad:** baja

## Lo que conviene dejar como está

- **`RoomActor::run` + `run_loop` + `drain_for_idle_shutdown`** (`room.rs:216-321`): son largos, pero cada rama tiene una razón documentada (pánico → `Unavailable` a la cola; idle → drenar antes de parar; disco en la salida si el idle es más corto que el tick lento).
- **Doble `persist_roster_at_stop` en `Shutdown`** (`room.rs:257` y `:293`): parece redundante, pero el primero persiste **antes de responder** (el caller sabe que el roster quedó escrito) y el segundo cubre las demás salidas; tras un primer guardado exitoso el segundo es un no-op, y si el primero falló, reintenta.
- **`handle_commit` paso a paso** (`room.rs:461-598`): aunque Q3 recorta los rechazos, el orden (dedup → retención → schema → tamaño → append → efectos en memoria → SSE → catch-up → rotación) y sus comentarios de *por qué* (`:484-486`, `:501-504`, `:521-523`, `:530-533`, `:553`, `:575-577`) son la especificación viva del commit y deben quedarse.
- **`reject_behind_log`** (`room.rs:782-784`): es un wrapper de una línea sobre `observe`, pero el nombre y su doc (por qué el cursor no se mueve y por qué no siempre se pide snapshot) documentan una semántica no obvia en los tres call sites.
- **`SpawnGuard` / `SpawnLockWait` y sus docs** (`manager.rs:96-138`): el razonamiento sobre `strong_count == 2` y el orden de drop es imprescindible; solo conviene unificar la llamada (Q8).
- **`RoomActor::open` con `#[allow(clippy::too_many_arguments)]`** (`room.rs:134-144`) y su duplicación con `spawn`: agrupar parámetros en un struct sería un cambio de API mayor del que busca esta auditoría.
- **`instant_before` con búsqueda binaria** (`lease.rs:488-502`): parece rebuscado, pero resuelve un caso real (un `Instant` no puede ir más atrás que el arranque de la máquina) y tiene test (`activity_before_the_clock_can_reach_saturates`).
- **`fit_in_response`** (`room.rs:977-995`) y **`panic_message`** (`room.rs:965-971`): ya son compactos e idiomáticos.
- **`ClientLeaseTracker::client_counts` con tupla de 5** (`lease.rs:399-421`): un struct sería más claro, pero suma líneas y cambia la API usada por tests (`.4`); no compensa.
- **`RoomManager::ask`** (`manager.rs:552-590`): el bucle de reintentos y la doble cota de tiempo (`ACTOR_TIMEOUT` y luego `ACTOR_EXIT_WAIT` fuera de ella) están justificados en su doc y en `:583-585`.
- **`snapshot_demand.rs`** en general: está bien factorizado; `designate` (`:139-175`) es denso pero claro.

## Tests

Revisión rápida, solo de duplicación de setup y helpers:

- **Setup de `RoomManager` duplicado** entre `tests/room.rs:123-150` (`new_manager_with`) y `tests/manager.rs:16-45` (`new_manager`): ambos crean `ServerConfig { data_dir, .. }`, `SchemaRegistry` en `schemas/`, `SnapshotRelay::new(dir/snapshots, 60s, max_snapshot_bytes)` y fijan ticks de 500 ms (`TEST_TIMINGS` en `room.rs:21-25` frente a un literal equivalente en `manager.rs:39-43`). También se duplican `test_schema()` (`room.rs:27-32` = `manager.rs:9-14`) y `POLL_CEILING` (`room.rs:17` = `manager.rs:281`). Un módulo de soporte común, por ejemplo `actor/tests/support.rs` incluido con `#[path]` desde ambos o un `#[cfg(test)] mod test_support` en `actor/mod.rs`, ahorraría ~25–30 líneas. **A verificar** que encaja con la convención de tests del repo.
- **Bucles de sondeo a mano** en `tests/room.rs:310-321`, `:338-345`, `:1325-1332` (y una variante en `:1733`), mientras `tests/manager.rs:284-290` ya tiene `wait_until`. Las condiciones de `room.rs` son asíncronas (`metrics(&sender).await`, `snapshot_wanted(..).await`), así que haría falta una variante `wait_until_async(what, || async { .. })` o una macro; ahorro ~15 líneas.
- **`tests/lease.rs`**: el patrón `tracker.get_client_mut(&c).unwrap().last_heartbeat = Instant::now() - Duration::from_secs(N);` se repite 7 veces (`:272`, `:296`, `:330`, `:340`, `:374`, `:526`, `:592`), siempre en 2 líneas. Un helper `fn set_inactive_for(tracker, client, secs)` ahorra ~7 líneas y se lee mejor. `seq(n)` también está duplicado en `tests/room.rs:46` y `tests/lease.rs:7` (trivial).
- **`tests/snapshot_demand.rs`**: el fixture `Room` (`:11-41`) ya concentra el setup; nada que sugerir.
- No detecté tests evidentemente redundantes en la pasada rápida. Los nombres son descriptivos y cada test cubre un escenario distinto.
