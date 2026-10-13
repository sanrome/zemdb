# Auditoría de calidad — `crates/storage/src/disk/`

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `disk/mod.rs`, `disk/wal.rs`, `disk/format.rs`, `disk/compactor.rs`, `disk/recovery.rs`, y sus tests unitarios `disk/tests/compactor.rs`, `disk/tests/recovery.rs`, `src/tests/disk.rs`

> Es código de durabilidad. Ninguna sugerencia cambia el orden de write / flush / fsync / rename / `sync_dir`, ni descarta resultados de I/O, ni toca las ventanas de crash documentadas. Las que tocan API pública o el hilo en que corre un fail point van marcadas **"a verificar"**.

## Resumen

- El código está en buen estado: las invariantes de crash-safety están bien explicadas, el RAII (`PendingWrite`, `CompactionFlag`, `StagedSnapshot`) se usa de forma consistente y no hay referencias a códigos de auditoría ni comentarios con historia de cambios. Lo que sobra es **duplicación mecánica**, no lógica innecesaria.
- Ahorro estimado en producción: **~150–210 líneas de 2170 (7–10 %)**, de las cuales ~35 dependen de borrar API pública sin uso (Q7, a verificar). En tests: **~140–190 líneas de 1699 (8–11 %)**. En total, **~290–400 líneas de 3869 (7–10 %)**.
- La mayor oportunidad es el **scan por lotes duplicado línea por línea** con `memory/mod.rs` (`DiskScanState` + el `unfold`, ~75 líneas en `mod.rs:298-377,427-436`).
- Le siguen tres patrones repetidos: el `if let Err(err) = … { wal.mark_failed(format!("…: {err}")); return Err(err); }` (6 sitios), el `spawn_blocking(…).await.map_err(|e| Other(format!("Join error: {e}")))??` (10 sitios) y la fase 2 de `compact_room_cow`, que hace a mano lo mismo que `stage_snapshot`.
- `FileHeader::compute_crc` reescribe a mano los primeros 36 bytes de `encode`, así que el layout está duplicado. Hay además API pública sin ningún uso en el workspace (`room_file_path`, `FILE_VERSION`, `WalReader::read_all`, el `pub use` de `replay_wal_records` en `recovery`).
- En los tests, los tres archivos redefinen los mismos helpers (`schema`, `insert`, `open_engine`, `assert_rows`, `staged_snapshot_files`), y en cada uno se repiten bloques "cerrar y reabrir", "spawn de un batch" y "espera `RoomFailed`".

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| `disk/mod.rs` | 825 | 612 | 107 | 26 | 80 | ~95–115 |
| `disk/compactor.rs` | 581 | 415 | 98 | 16 | 52 | ~30–40 |
| `disk/recovery.rs` | 471 | 356 | 42 | 25 | 48 | ~8–12 (+ posible +5 si se parte `recover_room`) |
| `disk/format.rs` | 178 | 131 | 25 | 0 | 22 | ~12–20 |
| `disk/wal.rs` | 115 | 84 | 17 | 0 | 14 | ~5–27 (27 si se quita `read_all`, a verificar) |
| **Producción** | **2170** | **1598** | **289** | **67** | **216** | **~150–210** |
| `disk/tests/compactor.rs` | 426 | 347 | 9 | 11 | 59 | ~45–60 |
| `disk/tests/recovery.rs` | 506 | 409 | 9 | 14 | 74 | ~40–55 |
| `src/tests/disk.rs` | 767 | 640 | 13 | 30 | 84 | ~45–60 |
| **Tests** | **1699** | **1396** | **31** | **55** | **217** | **~140–190** (incluye ~50 por un módulo de helpers común) |

## Comentarios

**Densidad:** alta en producción (≈16 % de las líneas son `///`/`//`), pero casi toda justificada: explica el *por qué* (orden de locks, ventanas de crash, por qué Tokio necesita `flush` antes de `sync`, por qué no se abre el WAL en modo append en Windows). No hay códigos de auditoría (C-01, DEF-xx…) ni narrativa del tipo "antes hacíamos X". Lo único con aire histórico son los dos alias de compatibilidad de `format.rs`.

**Conservar tal cual (son la especificación de la durabilidad):**
- `mod.rs:126-135` (`PendingWrite`: por qué se marca la room como fallida en `Drop`), `mod.rs:184-200` (orden de los tres locks), `mod.rs:438-446` (por qué hay dos locks de archivo), `mod.rs:575-578`, `mod.rs:602-604`, `mod.rs:782-784`.
- `compactor.rs:60-88` (protocolo de tres fases y qué pasa si falla cada una), `compactor.rs:254-261`, `compactor.rs:305-308`, `compactor.rs:498-511`, `compactor.rs:550-555` (`sync_file`) y los comentarios de rollback `compactor.rs:201-206`, `275`, `287-288`.
- `recovery.rs:126-137`, `recovery.rs:374-379`, `recovery.rs:405-406`, `recovery.rs:316-318`.

**Recortar o reescribir:**
- `mod.rs:218-231`, la doc de `DiskStorageEngine`: tono de marketing ("High-performance", "True CoW compaction", "Fast in-memory tables") y repite lo que ya documentan `DiskRoom` (`mod.rs:184-200`) y `compact_room_cow` (`compactor.rs:60-88`). Alcanza con 5–6 líneas: los dos archivos por room, el formato de cada uno y un enlace a `DiskRoom` y `compact_room_cow`. **~5–7 líneas.**
  - **Estado:** ✅ aplicada (paso 1)
- Comentarios numerados de `apply_batch` que repiten el código: `mod.rs:572` ("// 2. Encode WAL records into framed byte buffer") y `mod.rs:624` ("// 5. Check if compaction threshold is triggered"). Conviene quitar la numeración y quedarse con el texto de los pasos 3 y 4, que explica el *por qué*. `mod.rs:562` puede quedar en "// Validate against the schema and the head." **~2 líneas.**
  - **Estado:** ✅ aplicada (paso 1)
- `compactor.rs:131` "// Locks released immediately": engaña un poco, porque `_flag` sigue vivo hasta el final de la función. Mejor quitarlo o escribir "WAL mutex and state lock released here; the flag lives until the end".
  - **Estado:** ✅ aplicada (paso 1) (con el texto alternativo propuesto)
- `compactor.rs:96`, `134`, `153` ("// Phase N: …") repiten la doc de la función. Sirven como señales: se pueden dejar, pero recortadas a "// Phase 1", "// Phase 2", "// Phase 3".
  - **Estado:** ✅ aplicada (paso 1)
- `format.rs:8`, `format.rs:14`: "Alias … for backward compatibility". Dentro del workspace nadie usa `FILE_VERSION`, y `MAGIC_BYTES` solo lo usa un test de integración (ver Q7).
- `format.rs:152-177`: las cuatro docs de los wrappers repiten el nombre de la función. Basta un solo comentario encima del bloque: "Wrappers that convert `WalFrameError` into `StorageError`".
  - **Estado:** ✅ aplicada (paso 1)
- `wal.rs:13-16` y `wal.rs:26-29`: el mismo párrafo copiado dos veces (desaparece con Q6).
  - **Estado:** ✅ aplicada (paso 1) (junto con Q6)
- `recovery.rs:226-240` (doc de `recover_room`) y los comentarios de paso `recovery.rs:256`, `353`, `396`, `431` se solapan. Si se aplica Q8 (partir la función), cada paso se documenta una sola vez en su helper.
  - **Estado:** ✅ aplicada (paso 2) (junto con Q8): la carga del snapshot y la limpieza se documentan en sus helpers; `recover_room` conserva la lista de los cuatro pasos, porque los pasos 3 y 4 y la comprobación del final dañado de `.wal.compacting` siguen en su cuerpo
- `mod.rs:379` "Fast lookup of an open room handle." no aporta nada a la firma `get_room(&self, room_id) -> Result<Arc<DiskRoom>, _>` y se puede borrar. Lo mismo vale para `mod.rs:240` ("Creates a new `DiskStorageEngine` with the given configuration options.") y `wal.rs:60` y `wal.rs:65`.
  - **Estado:** ✅ aplicada (paso 1)

## Sugerencias

### Q1 — Extraer el scan por lotes compartido con `MemoryStorageEngine`
- **Dónde:** `mod.rs:298-377` (estado + `unfold`) y `mod.rs:427-436` (`DiskScanState`). Es idéntico, salvo el orden de dos `if` independientes, a `memory/mod.rs:102-182` + `memory/mod.rs:184-193` (`MemoryScanState`).
- **Problema:** ~85 líneas duplicadas casi literalmente en los dos motores. Un arreglo en una copia no llega a la otra.
- **Propuesta:** mover a `engine.rs`, junto a `apply_scan_transforms`, una función `pub(crate) fn batched_scan(table: Table, options: ScanOptions) -> RowStream<'static>` con el `struct ScanState` privado. En `scan_table` quedaría:
  ```rust
  let table_data = { /* lectura bajo el read lock, sin cambios */ };
  Ok(batched_scan(table_data, options))
  ```
  Dentro del helper, las dos ramas `Forward`/`Backward` pueden compartir el cálculo de límites (`let (start, end) = match (&state.cursor, direction) {...}`), lo que ahorra ~6 líneas más. El orden "actualizar `remaining_limit`" / "actualizar `cursor`" difiere entre las dos copias, pero ambas operaciones son independientes, así que no cambia nada.
- **Ahorro estimado:** ~75 líneas en `disk/mod.rs` (y otras tantas en `memory/mod.rs`, fuera de este alcance) · **Riesgo:** bajo (es un traslado puro, los scans no hacen I/O) · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — comparé `memory/mod.rs:102-193` con `disk/mod.rs:298-436`: son idénticos salvo el nombre del struct y el orden cursor/`remaining_limit`, que son independientes (uno depende de `batch_items.last()` y el otro de `count`). En `engine.rs` compila en las dos plataformas: `memory::Table` existe en wasm, el estado es `Send` como hoy y `RowStream<'static>` se convierte en `RowStream<'_>`. Faltaría importar `RangeBounds`/`VecDeque` allí.
- **Estado:** ✅ aplicada (paso 2): `engine::batched_scan`, con las ramas `Forward`/`Backward` compartiendo el cálculo de límites

### Q2 — Helper `DiskWal::fail_on_err` para el patrón "marcar la room como fallida y propagar"
- **Dónde:** `mod.rs:596-599`, `mod.rs:791-794`, `compactor.rs:111-114`, `compactor.rs:222-227`, `compactor.rs:274-278`, `compactor.rs:286-291`.
- **Problema:** seis repeticiones de
  ```rust
  if let Err(err) = result {
      wal.mark_failed(format!("<contexto>: {err}"));
      return Err(err);
  }
  ```
  Todos los mensajes siguen exactamente el formato `"{contexto}: {err}"`.
- **Propuesta:**
  ```rust
  impl DiskWal {
      /// Marks the room as failed if `result` is an error, and returns `result` unchanged.
      pub(crate) fn fail_on_err<T>(&mut self, context: &str, result: Result<T, StorageError>) -> Result<T, StorageError> {
          if let Err(err) = &result {
              self.mark_failed(format!("{context}: {err}"));
          }
          result
      }
  }
  // uso:
  wal.fail_on_err("Reading the active WAL failed", read_result)?;
  ```
  En `apply_batch` queda `pending.wal().fail_on_err("WAL append failed", write_result)?;`. `pending` se suelta al hacer `?`, cuando `failure` ya está puesto, así que su `Drop` no hace nada, igual que hoy después de `pending.fail(...)`. Si además se reemplaza `mod.rs:791-794`, `PendingWrite::fail` (`mod.rs:153-156`) queda sin uso y se borra. El orden "marcar y después devolver el error" no cambia. **No** aplicar en `compactor.rs:203-211` ni en `compactor.rs:336-341`: ahí se marca la room solo cuando falla el rollback, y se devuelve el error original.
- **Ahorro estimado:** ~18 líneas menos 6 del helper, más 4 de `fail` borradas: **~15 netas** · **Riesgo:** bajo · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — los 6 sitios y sus líneas están bien, todos son `StorageError` con formato `"{ctx}: {err}"`. `PendingWrite::fail` solo se usa en 597 y 792, y el `Drop` no hace nada si `failure` ya está puesto. Matiz: hay que guardar el resultado en una variable antes. `wal.fail_on_err(ctx, truncate_wal(wal, …).await)` no compila porque toma `&mut` dos veces.
- **Estado:** ✅ aplicada (paso 2) en los 6 sitios, guardando antes el resultado en una variable donde hacía falta; `PendingWrite::fail` queda sin uso y se borra

### Q3 — Un único helper para `spawn_blocking` + `JoinError`
- **Dónde:** `mod.rs:492-494`, `mod.rs:700-709`, `mod.rs:734-741`, `compactor.rs:139-150`, `compactor.rs:427-436`, `compactor.rs:464-468`, `compactor.rs:573-575`, `recovery.rs:83-85`, `recovery.rs:319-323`, `recovery.rs:386-390` (y dos más en `memory/mod.rs:330,367`).
- **Problema:** `.await.map_err(|e| StorageError::Other(format!("Join error: {e}")))??` aparece 10 veces en el módulo.
- **Propuesta:** en el crate (p. ej. `sys.rs` o `error.rs`, con `#[cfg(not(target_arch = "wasm32"))]`, porque en wasm Tokio solo trae `sync`):
  ```rust
  /// Runs `f` on Tokio's blocking pool and flattens its result.
  pub(crate) async fn run_blocking<T, E>(
      f: impl FnOnce() -> Result<T, E> + Send + 'static,
  ) -> Result<T, StorageError>
  where
      T: Send + 'static,
      E: Into<StorageError> + Send + 'static,
  {
      tokio::task::spawn_blocking(f)
          .await
          .map_err(|e| StorageError::Other(format!("Join error: {e}")))?
          .map_err(Into::into)
  }
  ```
  Tiene que ser genérico en `E`: `sync_dir` (`compactor.rs:573`) y `find_valid_frame` (`recovery.rs:83`) devuelven `std::io::Result`, y los demás sitios devuelven `StorageError`. Así cada sitio queda en `run_blocking(move || …).await?`. El mensaje no cambia, y la conversión `io::Error → StorageError::Io` es la misma que hace hoy el segundo `?`. `compact_room` (`mod.rs:418-423`, "Compaction task failed") usa `tokio::spawn` y otro mensaje, así que queda como está.
- **Ahorro estimado:** ~20 líneas en los 10 sitios menos ~13 del helper: **~7 netas en el módulo** (más si `memory/mod.rs` también lo usa). El beneficio está sobre todo en la legibilidad (`??` y `map_err` repetidos). · **Riesgo:** bajo · **Prioridad:** media
- **Estado:** ✅ aplicada (paso 2): `sys::run_blocking`, genérico en el tipo de error, en los 10 sitios del módulo (y los 2 de `memory`)

### Q4 — La fase 2 de `compact_room_cow` duplica `stage_snapshot`
- **Dónde:** `compactor.rs:135-151` frente a `compactor.rs:416-437`.
- **Problema:** las dos serializan `{head_seq, tables}` con bincode en el blocking pool y llaman a `stage_snapshot_blocking`. Solo cambian el struct (`RoomSnapshotPayload` con tablas movidas frente a `RoomSnapshotRef` con referencia, que serializan igual: mismo orden de campos, y recovery deserializa ambos como `RoomSnapshotPayload`) y el fail point `compaction.phase2`, que se evalúa dentro del closure.
- **Propuesta:**
  ```rust
  fail_point::check("compaction.phase2", &room.snap_path)?;
  let staged = stage_snapshot(&room.snap_path, cut_seq, &snapshot_tables, options.zstd_level, &room.room_id).await?;
  ```
- **Ahorro estimado:** ~12 líneas · **Riesgo:** medio, **a verificar**. El fail point pasaría a evaluarse en el hilo async y no en el blocking pool. Hoy `check` es `cfg(test)` y no hace nada en producción, y en tests devuelve el mismo `Err` antes de serializar, que es lo que pasa hoy, pero conviene confirmarlo con los 9 tests que lo arman. Además se añade un clon O(1) de las tablas. No cambia el orden de I/O. · **Prioridad:** media
- **Verificación (2.ª pasada):** ✅ confirmada — `check` solo usa el `Mutex` global (no depende del hilo), con el mismo path (`room.snap_path`) y el mismo orden después de `pause("compaction.staging")`. `RoomSnapshotRef` y `RoomSnapshotPayload` serializan igual. Matiz: el clon es O(n.º de tablas), no O(1), y `snapshot_tables` queda vivo hasta el final del bloque. Conviene un `drop` explícito para no retener nodos viejos durante la fase 3.
- **Estado:** ✅ aplicada (paso 2), con `drop(snapshot_tables)` explícito antes de la fase 3. Los tests que arman `compaction.phase2` siguen probando lo mismo: todos esperan que `compact_room` falle y deje `.wal.compacting`, y el fail point sigue devolviendo el error antes de serializar y de crear el archivo temporal, con el mismo path y después de `pause("compaction.staging")`

### Q5 — `FileHeader::compute_crc` reescribe el layout de `encode`
- **Dónde:** `format.rs:68-79` frente a `format.rs:82-94`.
- **Problema:** los primeros 36 bytes se escriben dos veces, con el mismo código. Si cambia el layout, hay que tocar los dos sitios.
- **Propuesta:**
  ```rust
  /// CRC32 of the first 36 bytes of the encoded header (everything before `header_crc`).
  fn compute_crc(&self) -> u32 {
      crc32fast::hash(&self.encode()[..36])
  }
  ```
  `encode` escribe exactamente esos bytes en `0..36`, y `header_crc` va en `36..40`, que el hash no incluye, así que el resultado es idéntico (también en `new`, donde `header_crc` vale 0 en ese momento). Opcional: una constante `const CRC_COVERED: usize = 36;` en lugar del número mágico, que aparece en `format.rs:30,68,70`.
- **Ahorro estimado:** ~9 líneas · **Riesgo:** bajo · **Prioridad:** alta (corrige una duplicación del formato en disco)
- **Verificación (2.ª pasada):** ✅ confirmada — `encode()[..36]` produce los mismos bytes que `buf`, y `header_crc` (36..40) y `reserved` quedan fuera. Las líneas 30/68/70 son correctas.
- **Estado:** ✅ aplicada (paso 1)

### Q6 — `WalWriter::write_record` es `write_batch` con un slice de un elemento
- **Dónde:** `wal.rs:13-24`.
- **Propuesta:**
  ```rust
  /// Encodes and appends a single sequenced operation (see [`Self::write_batch`]).
  pub async fn write_record(file: &mut File, op: &SequencedOperation) -> Result<usize, StorageError> {
      Self::write_batch(file, std::slice::from_ref(op)).await
  }
  ```
  `encode_wal_batch(from_ref(op), None)` es justo lo que hace `encode_batch(from_ref(op))`, así que los bytes y el `flush` salen iguales.
- **Ahorro estimado:** ~5 líneas · **Riesgo:** bajo · **Prioridad:** baja
- **Estado:** ✅ aplicada (paso 1)

### Q7 — API pública sin uso o de "compatibilidad" (a verificar)
- **Dónde y uso real (grep en todo el workspace):**
  - `mod.rs:262-265` `room_file_path`: "Compatibility helper", **0 usos**.
  - `format.rs:14-15` `FILE_VERSION`: **0 usos**.
  - `format.rs:8-9` `MAGIC_BYTES`: solo en `tests/disk_wal_resilience_tests.rs:7,29`, que puede usar `MAGIC`.
  - `wal.rs:93-114` `WalReader::read_all`: **0 usos**. Hace lo mismo que `format::replay_wal_records` (que delega en `zemdb_core`).
  - `recovery.rs:15` `pub use crate::disk::format::replay_wal_records;`: **0 usos** por esa ruta.
  - `recovery.rs:24` `pub struct RecoveredRoom` y `recovery.rs:242` `pub async fn recover_room`: solo los usa `disk/mod.rs`. Podrían ser `pub(crate)`.
- **Problema:** como `disk` y `format` son `pub` en `lib.rs:13-18`, estas piezas son API pública del crate. Si nadie fuera del workspace depende de ellas, son código muerto.
- **Propuesta:** borrarlas o bajarlas a `pub(crate)` si el crate no tiene consumidores externos. Si los tiene, dejarlas y marcarlas con `#[deprecated]`.
- **Ahorro estimado:** ~30 líneas · **Riesgo:** bajo en el workspace, **a verificar** fuera de él (es un cambio de API pública) · **Prioridad:** media

### Q8 — Partir `recover_room` (~225 líneas) en pasos con nombre
- **Dónde:** `recovery.rs:242-467`.
- **Problema:** una sola función hace cinco cosas: cargar el snapshot (`256-336`), limpiar los `.snap.tmp.*` (`338-351`, con cuatro niveles de `if let`), reproducir `.wal.compacting` (`353-394`), reproducir el WAL activo y truncar el torn write (`396-429`) y plegar (`431-448`).
- **Propuesta:** extraer, sin reordenar nada:
  - `async fn load_snapshot(snap_path, room_id, schema) -> Result<(tables, snapshot_seq, head_seq, snapshot_len), _>` para `256-336`;
  - `async fn remove_staged_snapshots(snap_path) -> Result<(), _>` para `338-351`, aplanado con `let Some(parent) = … else { return Ok(()) };` y `let Ok(mut entries) = … else { return Ok(()) };`;
  - `async fn replay_compacting_wal(...) -> Result<(bool, Option<(WalFrameError, Option<SequenceNumber>)>), _>` para `359-394`.

  La secuencia de efectos de `recover_room` no cambia (snapshot → limpieza → `.compacting` → WAL activo → truncado → pliegue → `seek(End)`). **Observación, fuera del alcance de estilo:** la limpieza de `338-351` ignora en silencio los errores de `read_dir`/`next_entry`. Es limpieza best-effort y hay que conservar ese comportamiento, pero conviene que un comentario lo diga de forma explícita.
- **Ahorro estimado:** 0 a +5 líneas (gana claridad, no líneas) · **Riesgo:** bajo · **Prioridad:** media
- **Estado:** ✅ aplicada (paso 2): `load_snapshot` (devuelve un struct `BaseSnapshot` en lugar de una tupla de cuatro campos), `remove_staged_snapshots` (aplanada y con un comentario que dice que la limpieza es best-effort) y `replay_compacting_wal`. El orden de efectos no cambia

### Q9 — `lock_room_files`: dos bloques idénticos de abrir y bloquear
- **Dónde:** `mod.rs:452-468`.
- **Propuesta:**
  ```rust
  let open_locked = |path: &std::path::Path| -> Result<std::fs::File, StorageError> {
      let file = std::fs::OpenOptions::new().create(true).read(true).write(true).truncate(false).open(path)?;
      file.try_lock_exclusive().map_err(|_| StorageError::RoomLocked(room_id.clone()))?;
      Ok(file)
  };
  let lock = open_locked(lock_path)?; // the lock file first, before touching any other file
  Ok((lock, open_locked(wal_path)?))
  ```
  Conserva el orden: el `.lock` se abre y se bloquea antes de abrir el WAL, y el `?` corta antes del segundo.
- **Ahorro estimado:** ~6 líneas · **Riesgo:** bajo · **Prioridad:** baja
- **Estado:** ✅ aplicada (paso 1)

### Q10 — Segunda comprobación en `apply_snapshot` con un `match` de tres ramas
- **Dónde:** `mod.rs:766-780`.
- **Propuesta:**
  ```rust
  let current = room.state.read().await.head_seq;
  let refusal = match wal.ensure_usable() {
      Err(err) => Some(Err(err)),
      Ok(()) => snapshot_head_outcome(current, payload.head_seq),
  };
  if let Some(outcome) = refusal {
      staged.discard().await;
      return outcome;
  }
  ```
  Es la misma prioridad (primero `ensure_usable`, después la cabeza), la misma limpieza y el mismo valor de retorno.
- **Ahorro estimado:** ~5 líneas · **Riesgo:** bajo · **Prioridad:** baja
- **Estado:** ✅ aplicada (paso 1) sobre el código actual, que ya agrupaba `ensure_usable` y `snapshot_head_outcome` en un `match` de tres ramas

### Q11 — Detalles menores de redacción
- `recovery.rs:264-267` y `recovery.rs:360-363`: `std::fs::OpenOptions::new().read(true).open(p)?` es `std::fs::File::open(p)?` (sigue siendo síncrono, la semántica no cambia), y `snap_meta` se usa una sola vez: `let snap_file_len = snap_file.metadata().await?.len();`. **~4 líneas.**
- `format.rs:98-99` y `format.rs:121-122`: `let magic: [u8; 4] = bytes[0..4].try_into().unwrap();` y lo mismo para `reserved`, con el mismo estilo que `115-119`. **~2 líneas.**
- `mod.rs:487-491`: con Q3 se puede quitar el bloque que clona `lock_path`/`wal_path`/`room_id` en una tupla y pasarlo directo al closure.
- `mod.rs:319-322`: `state.remaining_limit.map_or(BATCH_SIZE, |l| l.min(BATCH_SIZE))` (queda absorbido por Q1).
- `compactor.rs:379-380`: `let tmp_path = snap_path.with_extension(format!("snap.tmp.{}", uuid::Uuid::new_v4()));`. El `to_string()` intermedio sobra. **1 línea.**
- `compactor.rs:461-471`: con Q3, `write_empty_snapshot_file` queda en ~4 líneas.
- **Estado:** ✅ aplicada (paso 1) en `recovery.rs:264-267` y `360-363`, `format.rs:98-99` y `121-122` y `compactor.rs:379-380`. Las viñetas de `mod.rs:487-491`, `mod.rs:319-322` y `compactor.rs:461-471` dependen de Q1 y Q3 y quedan para el paso 2.
- **Estado:** ✅ aplicada (paso 2) en `mod.rs:319-322` (absorbida por el scan compartido, con `map_or`) y `compactor.rs:461-471` (`run_blocking` deja el bloque en una llamada). ⏭️ `mod.rs:487-491` solo en parte: el closure de `run_blocking` es `'static` y tiene que ser dueño de sus valores, así que los clones de `lock_path`/`wal_path`/`room_id` siguen haciendo falta; el bloque queda alrededor de una sola llamada a `run_blocking`.

## Lo que conviene dejar como está

- **`PendingWrite` y su `Drop` (`mod.rs:136-172`)**: el flag `completed` es el patrón RAII que pide GEMINI.md. Cubre futures abandonados y pánicos, y está probado (`tests/disk.rs:315`, `:508`, `:543-626`). No hay que reemplazarlo por otro mecanismo.
- **Los `async { …; Ok::<(), StorageError>(()) }.await` seguidos de un manejo de error** (`mod.rs:580-595`, `compactor.rs:105-110`, `154-159`, `217-221`, `268-273`, `318-335`): agrupan los pasos que comparten la misma reacción ante un fallo y dejan explícito el punto exacto donde se marca la room como fallida. Q2 solo acorta el manejo, no estos bloques.
- **`append_synced` partiendo el buffer en dos `write_all` (`compactor.rs:319-323`)**: existe para inyectar un fallo a mitad del append. Parece raro, pero es necesario para el test de `compaction.absorb_write`.
- **`rotate_wal_to_compacting` con rollback y doble manejo de errores (`compactor.rs:187-229`)**: cada rama tiene una consecuencia distinta (deshacer el cambio o marcar la room como fallida). Es largo, pero cada línea cuenta.
- **`absorb_wal_into_compacting` leyendo con el handle bloqueado (`compactor.rs:262-293`)**: la doc explica la razón (Windows). No hay que reemplazarlo por un `tokio::fs::read(path)`.
- **`sync_file` y su `#[allow(clippy::disallowed_methods)]` (`compactor.rs:550-563`)**: es el único punto de sync de archivos Tokio, y su doc explica por qué el `flush` es obligatorio. `WalWriter::write_flushed` (`wal.rs:39-44`) existe por la misma razón.
- **Las docs largas de `compact_room_cow`, `install_applied_snapshot`, `DiskRoom` y `replay_wal_file`**: son la especificación de las ventanas de crash. Recortarlas haría perder información que el código no puede expresar.
- **`read_full` (`recovery.rs:54-67`)**: `read_exact` no sirve, porque hay que distinguir "0 bytes" de "header incompleto".
- **`classify_anomaly` y `highest_seq_in_valid_frames`**: tienen poco código y lo que hacen no es trivial. Están bien separadas.
- **El builder de `DiskStorageOptions` (`mod.rs:52-94`)**: las docs de los setters repiten las de los campos, pero es API pública documentada y en un builder es lo esperado.

## Tests

**Generales.** Los tests son de muy buena calidad: cada uno nombra un escenario de crash concreto, usan fail points deterministas en lugar de `sleep` y los timeouts están acotados y explicados. El problema es la repetición.

- **T1 — Helpers duplicados entre los tres archivos (~50 líneas).** `USERS`, `schema()` (idéntico en `tests/compactor.rs:11-19`, `tests/recovery.rs:12-20` y `tests/disk.rs:12-20`), `insert`/`insert_named`/`insert_key` (tres variantes de lo mismo), `open_engine` (`tests/compactor.rs:32-36` = `tests/disk.rs:37-41`), `assert_rows` (`tests/compactor.rs:48-57` = `tests/recovery.rs:38-47`) y `staged_snapshot_files` (`tests/recovery.rs:261-268` = `tests/disk.rs:57-64`). Propuesta: un módulo `#[cfg(test)]` común, por ejemplo `disk/tests/support.rs`, declarado una sola vez desde `disk/mod.rs` con `#[cfg(test)] #[path = "tests/support.rs"] pub(crate) mod test_support;`. Respeta la convención de que los tests no viven en el código fuente, pero **a verificar** si el repo ya tiene un lugar acordado para fixtures compartidos.
  - **Estado:** ✅ aplicada (paso 3) en `disk/tests/support.rs`, sin tocar `disk/mod.rs`: el módulo se declara desde `src/tests/disk.rs` (`#[path = "../disk/tests/support.rs"] pub(super) mod support;`), que ya es un módulo `#[cfg(test)]` de `disk`, así que lo ven `disk::compactor::tests` y `disk::recovery::tests`. Reúne `USERS`, `schema`, `sequenced` (único lugar con `with_default_origin`), `insert_named`/`insert_key`/`insert`, `engine_at`, `open_engine`, `reopen`, `apply_range`, `has_key`, `assert_rows`, `assert_room_failed`, `fail_compaction_leaving_compacting_wal`, `staged_snapshot_files`, `spawn_batch` y `spawn_compaction`. `snapshot_with_rows` sigue en `src/tests/disk.rs`, su único usuario.

- **T2 — `disk/tests/compactor.rs` (~45–60 líneas):**
  - `failed_rotation_rollback_…` (`204-228`), `failed_wal_sync_before_compaction_…` (`231-250`) y `failed_directory_sync_after_wal_rotation_…` (`253-273`) son el mismo test con distintos fail points. Un helper `async fn failed_step_marks_room_failed(fail_points: &[&'static str])` y tres tests de una línea, igual que ya se hace con `failed_phase3_step_loses_no_batch`. **~25 líneas.**
  - "Cerrar y reabrir" (`engine.close_room(..).unwrap(); let reopened = open_engine(..).await;`) se repite 10 veces. Con un helper `reopen(engine, dir, room)`: **~10 líneas.**
  - "Dejar un huérfano" (`apply_range(1..=5)` + `arm("compaction.phase2")` + `assert!(compact_room.is_err())`) se repite en `77-79`, `131-133`, `154-156`, `179-181`, `308-310`. `tests/recovery.rs:60-72` ya tiene esa idea (`leave_orphan_compacting`). **~10 líneas.**
  - `assert!(matches!(engine.apply_batch(&room, vec![insert(6)]).await, Err(StorageError::RoomFailed { .. })))` aparece 4 veces aquí y 4 en `tests/disk.rs`. Un helper `assert_room_failed` iría bien en T1.
  - `353`: la línea de doc supera el ancho del resto y está mal partida ("…still in effect. With `reopen_first`, the room is reopened from the").
  - `129`, `177`: usar `compacting_wal_path(&wal_path)` (`pub(crate)`) en lugar de repetir `with_extension("wal.compacting")`. Lo mismo en `tests/recovery.rs:121,415,430,455,470`.
  - **Estado:** ✅ aplicada (paso 3) en todos los puntos. `failed_step_marks_room_failed(room, fail_points)` comprueba también en los tres tests que `compact_room` devuelve `RoomFailed` (antes solo lo hacía el de rollback); los tres pasan. El nombre de cada sala se conserva.

- **T3 — `disk/tests/recovery.rs` (~40–55 líneas):**
  - `crash_while_folding_compacting_wal_recovers_all_data` (`85-96`) y `crash_before_compacting_wal_removal_recovers_all_data` (`99-110`) solo cambian el nombre del fail point. Se pueden unir en un `for fail_point_name in ["recovery.fold", "recovery.fold_cleanup"]`, como ya hace `484-506`. **~12 líneas.**
  - `append_batch` (`74-82`) es `append_raw(path, &encode_wal_batch(ops, None).unwrap())` (`append_raw` está en `316-324`), y `189-194` reimplementa `append_raw` en línea. **~12 líneas.**
  - `assert!(matches!(result, Err(StorageError::WalCorruption(_))), "...{result:?}")` aparece en `173`, `227-230`, `362-365`, `402-405` y `475-478`. Un helper `assert_corruption(&result, ctx)` ahorraría **~10 líneas.**
  - `216-217`: `frame_len(name_len)` y la construcción de `first` repiten `encode_wal_batch(&[insert_named(1, 1, &"x".repeat(n))], None)`. Un closure `frame(n)` que devuelva el `Vec` resolvería las dos cosas. **~3 líneas.**
  - **Estado:** ✅ aplicada (paso 3), salvo la fusión de los dos tests de fold en un `for`: las instrucciones de este paso piden un test por modo de fallo, así que comparten el cuerpo `crash_while_folding_recovers_all_data(room, fail_point)` y quedan dos tests de una línea. `assert_wal_corruption(&result, context)` cubre los cinco sitios y `frame(n)` devuelve el `Vec`.

- **T4 — `src/tests/disk.rs` (~45–60 líneas):**
  - `288-296` reconstruye a mano lo mismo que `snapshot_with_rows(&room, 101, 5)` (claves `100 + seq`, `seq` 1..=5). **~8 líneas.**
  - El bloque `tokio::spawn({ let (engine, room) = (engine.clone(), room.clone()); async move { engine.apply_batch(&room, vec![insert(n)]).await } })` aparece en `151-154`, `185-188`, `190-193`, `234-237`, `322-325` y `516-519`, y la variante con `compact_room` en `223-226`, `375-378`, `662-665` y `696-699`. Con dos helpers (`spawn_batch(&engine, &room, ops)`, `spawn_compaction(&engine, &room)`): **~25 líneas.**
  - `713-717` y `724-730` no usan `has_key` (`82-88`), y los bucles "head == N y todas las claves 1..=N" de `207-210`, `245-248` y `723-730` son el `assert_rows` de los otros archivos (ver T1). **~12 líneas.**
  - `91-116`: es el único test que construye `DiskStorageOptions` a mano en lugar de usar `open_engine`. **~3 líneas.**
  - `351`: crea un motor solo para calcular `wal_path`. `engine.wal_file_path(&room)` después de `352` da lo mismo.
  - `647` y `705`: `std::time::Duration` con la ruta completa, cuando ya se usa dos veces. Basta importarlo.
  - `593-626`: los cuatro tests de `compaction_aborted_at(pause, orphan, after, direct)` reciben dos `bool` posicionales, y en la llamada `(…, false, AfterAbort::WriteAndCompact, true)` no se lee qué significa cada uno. Se puede pasar `direct` a un enum `Caller::{Direct, CompactRoom}` como ya se hizo con `AfterAbort`. Gana claridad, no líneas.
  - **Estado:** ✅ aplicada (paso 3) en todos los puntos, con `Caller::{Direct, CompactRoom}` en lugar del `bool`. Los `RoomFailed` usan `assert_room_failed` y "cerrar y reabrir" usa `reopen`.

- **Lo que no hay que tocar en los tests:** los tests de un escenario por función con nombres descriptivos (no conviene fusionarlos en tablas grandes, porque el nombre es la documentación del caso de crash) y los comentarios que explican qué estado deja el fallo inyectado (p. ej. `tests/compactor.rs:159-160`, `tests/disk.rs:157-158` y `:229-230`, `tests/recovery.rs:148-149`).
