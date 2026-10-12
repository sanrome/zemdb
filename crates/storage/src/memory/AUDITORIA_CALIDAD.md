# Auditoría de calidad — `crates/storage/src/memory/`

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `memory/mod.rs`, `memory/state.rs`, `memory/tests/state.rs`, `tests/memory.rs` (tests de `memory/mod.rs`)

## Resumen

- Código en buen estado: nombres claros, invariantes documentadas (clon O(1) de `imbl::OrdMap`, reemplazo in-place en `apply_snapshot`, defensa anti-OOM al deserializar). No hay códigos de auditoría (C-xx, DEF-xx, M-xx) en el módulo.
- **Mayor oportunidad:** la máquina de estados del scan (`MemoryScanState` + `futures::stream::unfold`, `mod.rs:102-193`) está **copiada línea por línea** en `disk/mod.rs:298-372` + `426-435` (`DiskScanState`). Ambas trabajan sobre un `Table` ya clonado, así que una sola función compartida elimina ~80 líneas del crate.
- Dentro de esa máquina de estados sobran ~14 líneas: dos ramas `Forward/Backward` que solo cambian los límites, un `match` reemplazable por `map_or` y dos chequeos redundantes (`count == 0`, `*rem == 0`).
- El `map_err` de lock envenenado aparece 9 veces (`"Room lock poisoned"` ×6, `"Engine lock poisoned"` ×3): un helper ahorra ~14 líneas.
- Ahorro estimado **sin contar Q1**: ~70–85 líneas (~8–10 % de las 872 líneas del alcance). **Q1** mueve ~85 líneas de `mod.rs` a un helper compartido y ahorra ~80 netas en el crate, porque desaparece la copia de `disk`.

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---|
| `memory/mod.rs` | 404 | 329 | 20 | 8 | 47 | ~45 (+ ~85 que salen con Q1) |
| `memory/state.rs` | 265 | 199 | 39 | 0 | 27 | ~25 |
| `memory/tests/state.rs` | 55 | 46 | 2 | 0 | 7 | 0 |
| `tests/memory.rs` | 148 | 129 | 0 | 5 | 14 | ~10–12 |
| **Total** | **872** | 703 | 61 | 13 | 95 | **~80 (≈9 %)**, más Q1 |

## Comentarios

Densidad baja-moderada (74 líneas de doc/comentario sobre 872) y en general de buena calidad: explican el *por qué*. Recortes y correcciones puntuales:

- **Recortar (redundantes):**
  - `mod.rs:249` `// 1. Validate schema and strict monotonic sequence` y `mod.rs:252` `// 2. Apply operations to in-memory tables` repiten los nombres `validate_batch`/`apply_operation`. Se pueden borrar (−2).
  - `mod.rs:35` `/// Creates a new, empty MemoryStorageEngine.`: aceptable en una API pública, pero si se aplica Q7a (`new` → `Self::default()`) basta con la doc del struct.
  - `mod.rs:22-25`: cuatro líneas sobre los locks que caben en dos (“Un `RwLock` por sala: lecturas concurrentes dentro de una sala, escrituras exclusivas solo sobre esa sala.”) (−2).
  - `state.rs:22` `Holds the room schema, monotonic head sequence number, and in-memory tables indexed by table_id.` enumera los tres campos del struct; se puede borrar (−2 con la línea `///` en blanco).
  - `state.rs:31` `Creates a new RoomState initialized from a schema with empty tables.` repite la firma y el cuerpo.
- **Corregir (inexacto):** `state.rs:147` `Payload format for deserialized database snapshots.` También se **serializa**: `disk/compactor.rs:141-146` construye un `RoomSnapshotPayload` y lo pasa a `bincode::serialize`. Mejor: “Payload de un snapshot con tablas propias (compactación y decodificación).”
- **Conservar (valiosos):**
  - `state.rs:12-17`: por qué `Table` es un B-tree persistente (clon O(1), escrituras copian O(log n)).
  - `state.rs:155-160`: por qué no se usa el `Deserialize` de `imbl` (reserva por longitud declarada → abort). Es crítico para la seguridad.
  - `state.rs:57-60`: resolver bajo el mismo guard que se lee.
  - `mod.rs:93-94` y `mod.rs:311`: clon O(1) para leer/serializar sin el lock.
  - `mod.rs:338-341`, `350` y `377-378`: semántica de `apply_snapshot` (reemplazo in-place, doble `get_room` por cierre/reapertura durante la decodificación).
  - `memory/tests/state.rs:6` y `26` (“as written while tables were `Arc<BTreeMap>`”): es narrativa histórica, pero el test verifica justamente la compatibilidad de formato con ese encoding, así que está justificada.

## Sugerencias

### Q1. Una sola máquina de estados de scan para memoria y disco
- **Dónde:** `memory/mod.rs:102-193` (construcción de `MemoryScanState`, `unfold` y struct) ≡ `disk/mod.rs:298-372` + `426-435` (`DiskScanState`).
- **Problema:** son idénticas salvo el nombre del struct y el orden de dos bloques independientes (actualización del cursor y de `remaining_limit`). En las dos `table_data: Table` ya es un clon propio. Cualquier corrección hay que hacerla dos veces.
- **Propuesta:** extraer `pub(crate) fn table_stream<'a>(table_data: Table, options: ScanOptions) -> RowStream<'a>` (en `engine.rs`, junto a `apply_scan_transforms`, o en `memory/state.rs`, donde ya vive `Table`) con el struct de estado privado. `scan_table` queda en:
  ```rust
  let table_data = { /* lock + resolve + clone, igual que hoy */ };
  Ok(table_stream(table_data, options))
  ```
  Así `apply_scan_transforms` pasa a tener un solo llamador y puede ser privada.
- **Ahorro estimado:** ~80 líneas netas en el crate (sale la copia de `disk`) · **Riesgo:** bajo (mismo código; `disk/mod.rs` lo audita otro agente, coordinar) · **Prioridad:** alta

### Q2. Simplificar el cuerpo del `unfold` (aplica a la versión compartida de Q1)
- **Dónde:** `memory/mod.rs:122-170`.
- **Problema:**
  1. Las ramas `Forward`/`Backward` (`131-153`) repiten el cálculo de límites, el `range` y `apply_scan_transforms`. Solo cambian el límite que se excluye y el `.rev()`.
  2. `123-126`: `match` sobre `Option` para un mínimo.
  3. `156-159`: `if count == 0 { return None; }` es redundante. Con el lote vacío, `buffer.extend` no agrega nada y `buffer.pop_front().map(..)` (`177`) ya devuelve `None`; los efectos intermedios (`exhausted`, `cursor`) recaen sobre un estado que se descarta.
  4. `167-169`: `if *rem == 0 { state.exhausted = true; }` es redundante. El chequeo de la línea `118` (`state.remaining_limit == Some(0)`) ya corta en la llamada siguiente, después de vaciar el buffer.
- **Propuesta:**
  ```rust
  let batch_limit = state.remaining_limit.map_or(BATCH_SIZE, |l| l.min(BATCH_SIZE));
  let (start, end) = match (&state.cursor, state.direction) {
      (None, _) => (state.range.start_bound(), state.range.end_bound()),
      (Some(cur), ScanDirection::Forward) => (Bound::Excluded(cur), state.range.end_bound()),
      (Some(cur), ScanDirection::Backward) => (state.range.start_bound(), Bound::Excluded(cur)),
  };
  let rows = state.table_data.range::<_, PrimaryKey>((start, end));
  let projection = state.projection.clone();
  let batch_items: Vec<_> = match state.direction {
      ScanDirection::Forward => apply_scan_transforms(rows, projection, Some(batch_limit)).collect(),
      ScanDirection::Backward => apply_scan_transforms(rows.rev(), projection, Some(batch_limit)).collect(),
  };
  let count = batch_items.len();
  state.exhausted = count < batch_limit;
  if let Some(rem) = state.remaining_limit.as_mut() {
      *rem = rem.saturating_sub(count);
  }
  ```
  Nota: hoy `exhausted` solo pasa de `false` a `true`. Asignarlo directamente es equivalente, porque si ya fuera `true` la línea `118` habría retornado antes. Los préstamos (`start`/`end`/`rows` sobre `state`) terminan en el `collect()`, igual que hoy.
- **Ahorro estimado:** ~14 líneas · **Riesgo:** bajo · **Prioridad:** alta (junto con Q1)

### Q3. Helper para el lock envenenado
- **Dónde:** `mod.rs:49-51`, `68-70`, `88-90`, `200-203`, `219-222`, `239-241`, `302-304`, `313-315`, `380-382` (9 apariciones).
- **Problema:** `.map_err(|e| StorageError::Other(format!("Room lock poisoned: {e}")))` (o `"Engine …"`) obliga a rustfmt a partir cada adquisición del lock en 3–4 líneas.
- **Propuesta:**
  ```rust
  fn poisoned<G>(lock: &'static str) -> impl FnOnce(PoisonError<G>) -> StorageError {
      move |e| StorageError::Other(format!("{lock} lock poisoned: {e}"))
  }
  // …
  let room_state = room_arc.read().map_err(poisoned("Room"))?;
  ```
  El mensaje queda idéntico (`"Room lock poisoned: …"` / `"Engine lock poisoned: …"`).
- **Ahorro estimado:** ~14 líneas netas · **Riesgo:** bajo · **Prioridad:** alta

### Q4. `validate_batch` puede reutilizar `resolve_table_id`
- **Dónde:** `state.rs:88-93`.
- **Problema:** reimplementa el chequeo de `TableRef::Id` de `resolve_table_id` (`state.rs:68`, `74`): mismo `has_table_by_id` y mismo `TableNotFound { table: format!("id:{id}") }`.
- **Propuesta:** `resolve_table_id(room_id, schema, TableRef::Id(op.op.table_id()))?;` produce exactamente el mismo error, en el mismo orden respecto del chequeo de secuencia. Deja el formato `"id:{id}"` en un solo lugar.
- **Ahorro estimado:** ~5 líneas · **Riesgo:** bajo · **Prioridad:** media

### Q5. `deserialize_tables` sin visitor propio
- **Dónde:** `state.rs:161-184`.
- **Problema:** `TablesVisitor` existe para no reservar memoria según el número de tablas declarado. Pero el `Deserialize` de `HashMap` en serde (1.0.229 en `Cargo.lock`) ya limita la reserva previa con `size_hint::cautious`. El riesgo real son las **filas**, y eso ya lo resuelve `RowsAsRead`.
- **Propuesta:**
  ```rust
  fn deserialize_tables<'de, D: Deserializer<'de>>(d: D) -> Result<HashMap<u16, Table>, D::Error> {
      let tables = HashMap::<u16, RowsAsRead>::deserialize(d)?;
      Ok(tables.into_iter().map(|(id, RowsAsRead(t))| (id, t)).collect())
  }
  ```
  Las claves duplicadas se comportan igual (la última gana). Hay que ajustar la doc de `155-160` para decir que serde acota la reserva del mapa de tablas y `RowsAsRead` la de filas.
- **Ahorro estimado:** ~15 líneas · **Riesgo:** medio · **Prioridad:** media · **A verificar:** que `size_hint::cautious` se aplique a `HashMap` en la versión usada. Lo cubren `memory/tests/state.rs` y los tests de snapshots maliciosos.

### Q6. Helper de “ejecutar en el blocking pool o en línea en wasm”
- **Dónde:** `mod.rs:319-332` y `353-375`. El mismo patrón `spawn_blocking(..).await.map_err(|e| StorageError::Other(format!("Join error: {e}")))??` aparece 12 veces en `crates/storage/src` (incluido `disk`).
- **Problema:** `apply_snapshot` duplica la llamada a `decode_received_snapshot` en la rama wasm (`369-375`), y el mapeo del `JoinError` se repite.
- **Propuesta:** `pub(crate) async fn run_blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T, StorageError> + Send + 'static) -> Result<T, StorageError>`, con implementación por `cfg`: en wasm, `f()`. El `pause_blocking` queda dentro del closure con `#[cfg(not(target_arch = "wasm32"))]`.
- **Ahorro estimado:** ~6 líneas en `memory`, más en `disk` · **Riesgo:** medio (cfg, bounds `Send` en wasm) · **Prioridad:** baja

### Q7. Ajustes menores
- **a)** `mod.rs:36-40`: `new()` → `Self::default()` (el struct ya deriva `Default` y produce lo mismo) (−2).
- **b)** `mod.rs:224-229`: `rooms.remove(room_id).ok_or_else(|| StorageError::RoomNotFound(room_id.clone()))?;` + `info!` + `Ok(())` (−2).
- **c)** `state.rs:120-123`: `.map(|t| t.columns().len()).unwrap_or(0)` → `.map_or(0, |t| t.columns().len())` (−2).
- **d)** `state.rs:33-38`: `Self { tables: empty_tables(&schema), schema, head_seq: SequenceNumber::default() }`. Los campos se evalúan en orden de escritura, así que `tables` se calcula antes de mover `schema` (−1).
- **e)** `mod.rs:249/252`: borrar los comentarios numerados (−2).
- **Ahorro estimado:** ~9 líneas · **Riesgo:** bajo · **Prioridad:** baja

## Lo que conviene dejar como está

- **Destructuring de `RoomState` en `apply_batch` (`mod.rs:243-247`).** Son 5 líneas, pero hacen falta para tomar préstamos disjuntos (`&schema` y `&mut tables`/`&mut head_seq`) del mismo guard.
- **`if seq > *head_seq { *head_seq = seq; }` (`mod.rs:255-257`).** Tras `validate_batch` las secuencias son consecutivas y bastaría `*head_seq = seq`, pero la guarda no depende de esa invariante y cuesta una línea.
- **`RowsAsRead` y su visitor (`state.rs:186-211`).** Es la defensa contra snapshots que declaran miles de millones de filas; el `Deserialize` de `imbl` reserva por adelantado.
- **Doble `get_room` en `apply_snapshot` (`mod.rs:351` y `379`).** Es intencional y está comentado: reporta “sala no abierta” sin decodificar, y entrega el snapshot a la sala abierta *al terminar* de decodificar.
- **`decode_received_snapshot` y su doc (`state.rs:222-261`).** Es largo, pero documenta cada validación previa a la mutación (principio “validar antes de mutar”).
- **Doc de `apply_snapshot` (`mod.rs:338-341`).** Explica una garantía de concurrencia que no surge del código.

## Tests

- **`tests/memory.rs`.** Tres tests (`56`, `87`, `118`) repiten el armado de “motor fuente + sala + `apply_batch` de 3 inserts + `create_snapshot`” (`64-73`, `91-95`, `122-126`). Un helper `async fn snapshot_of(room: &RoomId, keys: impl IntoIterator<Item = i64>) -> Vec<u8>` ahorra ~10 líneas. Además, `67-71` (`[10, 11, 12].iter().enumerate()…`) se vuelve más legible.
- **`tests/memory.rs:97-100` y `128-131`.** Repiten el nombre de la sala como literal en `Path::new("…")`. `Path::new(room.as_str())` refleja exactamente la clave que usa producción (`mod.rs:355`, `PathBuf::from(room_id.as_str())`) y evita que el literal se desincronice.
- **`tests/memory.rs:80`.** `zemdb_core::SequenceNumber::from(3u64)`: los demás tests comparan con `.get() == 3`. Unificar el estilo.
- **`memory/tests/state.rs`.** Es compacto y cubre la compatibilidad de bytes con el encoding `BTreeMap`; no hay nada que recortar.
- **Cobertura.** Si se aplica Q2, conviene un test de scan con `limit` exacto múltiplo de `BATCH_SIZE` (64) y otro con `Backward` + cursor. Los tests de integración (`crates/storage/tests/query_pushdowns_tests.rs`) cubren gran parte; hay que verificar que incluyan esos bordes.
