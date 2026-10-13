# Auditoría de calidad — `crates/server/src/log/`

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `tiered_log.rs`, `policy.rs`, `cold_disk.rs`, `warm_disk.rs`, `segment_index.rs`, `segment_reader.rs`, `hot_buffer.rs`, `budget.rs`, `retention.rs`, `io_probe.rs`, `mod.rs` y sus tests en `log/tests/` (revisados con menos profundidad).

## Actualización tras el Lote 8 (`e88874b`)

El commit `e88874b` llegó a `main` después de esta auditoría y cambió el módulo. El resto del informe sigue citando las líneas de `7bcb6bb`; acá están las posiciones actuales donde cambiaron.

- **Q1 resuelta.** El Lote 8 borró `read_range`/`read_range_with_mutations` de `warm_disk.rs` y `cold_disk.rs`, y `tests/cold_disk.rs` ahora llama directo a `read_cold_file`. También desaparecieron las docs duplicadas de esas funciones que se mencionan en "Comentarios".
- **Q2 sigue valiendo, con menos usos.** `parse_segment_filename` está en `warm_disk.rs:311` y la usan solo `segment_index.rs:93` y `cold_disk.rs:67`. El helper `segment_start_seq` de Q1 ya no hace falta. Mover la función a `segment_index.rs` y simplificarla con `strip_prefix`/`split_once` sigue siendo correcto, con un ahorro de ~8 líneas.
- **Q3 sigue valiendo.** `len`, `bytes`, `is_empty` y `max_seq` de `HotBuffer` ahora son `#[cfg(test)]`; en producción solo se usa `min_seq()`. Los campos siguen en `hot_buffer.rs:18-19` y `after_eviction` en `:79`. Si se aplica la propuesta, el `max_seq()` derivado de `back()` también queda `#[cfg(test)]`.
- **Q6:** el mensaje `RoomLocked` de `active.wal` ya no lo comparan los tests de integración, sino `tests/warm_disk.rs:190` y `:232`, adonde se movieron los tests de flock. Sigue sin poder cambiarse. Los dos `try_lock_exclusive` están en `warm_disk.rs:83` y `:224`.
- **Q8 resuelta casi entera.** `run_maintenance_sync_at` ya no duplica el pase de mantenimiento: es `#[cfg(test)]` y corre `run_maintenance_at` en un runtime propio, así que la precedencia de errores ya no puede divergir. Solo queda la parte (a), devolver el `cold_path` desde `warm_segments_due` (~2 líneas).
- **API pública:** `log` ahora es privado al crate y `lib.rs` solo reexporta `RoomLifecycleOverrides` y `RoomLifecyclePolicy`. Las marcas "a verificar si hay consumidores externos" de este informe ya no aplican.
- **Comentarios:** los dos desactualizados siguen sin corregir. `tiered_log.rs:200` pasó a `:201` (rotación "or age") y `policy.rs:298` a `:299` ("seal a segment per operation"). `policy.rs:49` sigue en el mismo lugar.
- **Tests:** a este módulo se movieron varios tests de integración:
  - los de `crates/server/tests/tiered_log_tests.rs` y `unified_wal_tests.rs` están en `log/tests/tiered_log.rs`, que pasó de 1 165 a 1 854 líneas;
  - los dos de flock de `room_lifecycle_resilience_tests.rs` están en `log/tests/warm_disk.rs`, que pasó de 170 a 257;
  - el de rangos del `HotBuffer` está en el archivo nuevo `log/tests/hot_buffer.rs`.

  Por eso ahora aplican acá las sugerencias Q4, Q5, Q6, Q8 y Q11 de la parte B de `crates/server/tests/AUDITORIA_CALIDAD.md`:
  - helpers `append_range`, `seqs` y `segment_path`;
  - la consulta de rangos como tabla de casos;
  - fusionar tres pares de tests casi iguales;
  - un fixture para el WAL escrito;
  - absorber el test de flock simple en el de estrés.

  `make_op` ahora está copiado tres veces (`tests/tiered_log.rs:12`, `tests/warm_disk.rs:6`, `tests/hot_buffer.rs:6`), lo que refuerza la propuesta de `test_support` de la sección "Tests". El bucle `for … append` aparece más de las 17 veces que se contaron, así que el helper rinde más.
  - **Estado:** ✅ aplicada (paso 3) en las cinco sugerencias de la parte B:
    - **Q4:** `append_plain(log, first, last)` (sin ids de mutación) junto al `append_range` que ya existía, `seqs` en `tests/support.rs` (lo usa `assert_contiguous`) y `segment_file(dir, first, last, extension)`, que escribe el nombre a mano en lugar de usar `segment_path`. Las aserciones por índice (`deltas[0]`, `deltas[4]`, `enumerate`) pasaron a `assert_contiguous`, que compara la secuencia entera; el test de recuperación tras un crash compara las operaciones completas con `make_op`.
    - **Q5:** la consulta de rangos del `HotBuffer` es una tabla `(cursor, presupuesto, esperado)`; el test pasó a llamarse `range_queries_return_only_contiguous_operations_within_the_budget` y conserva el comentario del cursor anterior al buffer.
    - **Q6:** se borraron `test_tiered_log_async_maintenance_spawn_blocking` (lo cubre `test_tiered_log_cold_compression_and_read`, que ya usa `run_maintenance().await`), `test_tiered_log_fast_path_off_by_one_boundary` (su consulta con límite 5 es ahora una iteración de `test_tiered_log_hot_buffer_sliding_window_no_zero_eviction`) y `test_tiered_log_hierarchical_cache_inversion` (la lectura desde el medio del buffer tras una rotación ya la hace la consulta desde el cursor 4 del test de ventana deslizante, y sin rotación `test_tiered_log_hot_buffer_fast_read`).
    - **Q8:** `write_log(dir, last)` escribe `1..=last` con `append_range` y devuelve el `active.wal`, y `file_len` reemplaza los `metadata(..).len()`. Lo usan los tres tests de WAL dañado; los de ida y vuelta y de deduplicación conservan sus ids (`i + 1000`, `i * 42`).
    - **Q11:** el test de flock simple es ahora la primera aserción del de estrés, y los `match` de tres brazos pasaron a un `is_locked` con `matches!`.
    - De paso, `bytes_on_disk` reemplaza las dos sumas de tamaños de `segments/` y `fully_pruned_log` el armado repetido en cinco tests (ver "Tests").
- **Ahorro actualizado:**
  - **Código fuente:** ~90–120 líneas. Se descuentan Q1 y casi toda Q8.
  - **Tests:** ~250–350 líneas, ya contando los que se movieron.

## Resumen
- El código está bien escrito: los invariantes de durabilidad (tmp → fsync → rename → `sync_dir`, `log_meta.json` antes de borrar segmentos, poda solo de prefijos) están explícitos y comentados. No hay referencias a códigos de auditoría ni narrativa histórica en los comentarios.
- La duplicación principal está entre `warm_disk.rs` y `cold_disk.rs`: `read_range`/`read_range_with_mutations` repetidos (y sin usos fuera de los tests unitarios), el preludio de lectura de un segmento (presupuesto → `io_probe` → abrir con `NotFound` = vacío) y el parseo del nombre del archivo, repetido tres veces. Además `parse_segment_filename` vive en `warm_disk.rs` cuando es la inversa de `segment_path` (`segment_index.rs`).
- `HotBuffer` guarda `min_seq`/`max_seq` como campos cuando salen siempre de `entries.front()/back()`. Derivarlos elimina la contabilidad duplicada y `after_eviction`.
- Hay patrones chicos que se repiten: «borrar el archivo y tolerar `NotFound`» (4 veces en el módulo), «abrir `active.wal` y tomar el lock» (2 veces), y el cierre del pase de mantenimiento, repetido en la versión async y en la sync.
- Comentarios: tienen buena densidad y en general explican el *por qué*. Hay algo de lenguaje de marketing en `tiered_log.rs` y `hot_buffer.rs` («sub-millisecond», «Unified 4-tier … engine»), y **dos comentarios desactualizados**: la rotación por edad no existe (`tiered_log.rs:200`, `policy.rs:49`).
- **Ahorro estimado:** ~120–150 líneas en el código fuente (≈4,5–5,5 % de 2788) y ~100–140 en los tests (≈5–7 % de ~1980), sin tocar el orden de I/O, los fsync ni la contigüidad.

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| tiered_log.rs | 882 | 634 | 152 | 19 | 77 | ~25–30 |
| policy.rs | 395 | 291 | 68 | 0 | 36 | ~13 |
| cold_disk.rs | 364 | 281 | 47 | 6 | 30 | ~25–30 |
| warm_disk.rs | 352 | 269 | 33 | 11 | 39 | ~40–45 |
| segment_index.rs | 218 | 166 | 31 | 2 | 19 | ~0 neto (recibe el parseo y pierde envoltorios) |
| hot_buffer.rs | 206 | 144 | 32 | 3 | 27 | ~25–30 |
| segment_reader.rs | 201 | 159 | 24 | 0 | 18 | −10 (recibe el helper `open_segment`) |
| budget.rs | 65 | 46 | 12 | 0 | 7 | 0 |
| io_probe.rs | 54 | 35 | 14 | 0 | 5 | 0 |
| retention.rs | 34 | 14 | 16 | 0 | 4 | 0 |
| mod.rs | 17 | 16 | 0 | 0 | 1 | 0 |
| **Fuente** | **2788** | | | | | **~120–150** |
| tests/tiered_log.rs | 1165 | 951 | 15 | 42 | 157 | ~40–50 |
| tests/policy.rs | 203 | 193 | 0 | 1 | 9 | ~50–70 |
| tests/cold_disk.rs | 177 | 153 | 0 | 0 | 24 | ~10 |
| tests/warm_disk.rs | 170 | 135 | 5 | 4 | 26 | ~5 |
| tests/segment_reader.rs | 128 | 104 | 1 | 1 | 22 | ~5 |
| resto de los tests | 141 | | | | | ~3 |

## Comentarios

**Densidad:** hay un ~17 % de doc en `tiered_log.rs` y un ~15–17 % en `policy.rs`/`hot_buffer.rs`/`cold_disk.rs`. Es razonable para código de durabilidad. Casi todo lo largo explica el orden de efectos o la recuperación tras un crash, y eso se conserva.

**Conservar (explican el por qué, la seguridad ante crash o invariantes):**
- `cold_disk.rs:27-38`, `41-52` (orden tmp/fsync/rename/sync_dir/verificación/borrado y qué significa `cleanup_error`), `72-73`, `87`, `186-194`, `346-349`.
- `warm_disk.rs:27-28` (lock obligatorio en Windows), `85`, `89-90` (la entrada de directorio se sincroniza antes de confirmar), `100`, `109-110`, `153-154`, `172`, `205-206`.
- `tiered_log.rs:21-25`, `31-33`, `39-41`, `122-124`, `224-225`, `314-316`, `482-483`, `492`, `538`, `545-548`, `566-569`, `685-694`, `791-794`, `813-815`, `858-861`.
- `segment_index.rs:64-68` (limpieza de compresiones interrumpidas), `96-97` (Windows); `segment_reader.rs:1-8`; el `//!` de `retention.rs` (es el contrato de retención); `io_probe.rs:1-7`.

**Desactualizados o incorrectos (corregir, ahorro 0 líneas):**
- `tiered_log.rs:200`: «Rotates and seals segment if size **or age** threshold is exceeded». La rotación solo ocurre por cantidad (`ram_max_ops`) o por bytes (`segment_max_bytes`) (`tiered_log.rs:232-233`). No hay rotación por edad.
- `policy.rs:49`: `ram_ttl` es la «TTL … before **segment rotation** and eviction». `ram_ttl` solo se usa en `hot_buffer.rs:141` para desalojar de RAM. Propuesta: «Maximum age of an operation in RAM; older ones are evicted (they stay on disk).»
- `policy.rs:298` (**a verificar**): «A RAM TTL of 0 would seal a segment per operation». Según el código, una TTL de 0 vacía la RAM en cada `apply_sliding_window` y no sella nada. El motivo real del mínimo de 1 s parece ser «every read would go to disk».
- `segment_index.rs:143`: dice «Adds a segment.», pero `insert` también **reemplaza** el segmento con el mismo `start_seq`, y `mark_compressed` (`tiered_log.rs:470`) depende de eso. Propuesta: «Adds the segment, replacing the one with the same `start_seq`.»
- **Estado:** ✅ aplicada (paso 1)

**Recortar (redundantes o de marketing), unas 25–30 líneas:**
- `tiered_log.rs:98-101`: «Unified 4-tier immutable log coordinator and delta query engine. Manages the full lifecycle…». Mezcla «4 tiers» con «Tier 4 boundary» (`:279`). Bastaría una línea: «Room delta log over RAM, `.wal` and `.wal.zst` segments, with write-through appends.»
- `tiered_log.rs:196-201`: la lista numerada repite el cuerpo y además tiene el error de arriba. Alcanza con el párrafo `:202-203` más una línea. `:217` y `:220` («// 1. Write-Through to Warm Disk», «// 2. Add to RAM HotBuffer…») repiten el código, y `:228-230` se puede reducir a una línea («Seal the segment at `ram_max_ops` operations or `segment_max_bytes`; RAM keeps its window.»).
- `tiered_log.rs:266-269` («Unified multi-tier delta query engine. Transparently streams…»), `:279` («// Eviction check (Tier 4 boundary)», repite `is_behind_retention`) y `:290-292` («sub-microsecond time without touching disk or executing system calls»), que entra en una línea.
- `tiered_log.rs:141-145`: el corte de línea quedó mal («…keeping only those operations,\n so the cost…»). Conviene volver a cortar el párrafo.
- `hot_buffer.rs:9-12` («Ensures zero-squashing…, guaranteeing absolute sequence contiguity… sub-millisecond response times»): se puede dejar en una línea. Además, `:41` («Creates an empty hot buffer.»), `:89-90` («Exploits … in constant time without linear filtering», que se puede reducir a «The start index is `from_seq + 1 - min_seq`: the buffer is contiguous.») y `:105-107`, que puede ir en una línea.
- `cold_disk.rs:111-113` (la doc del async puede ir en una línea: «[`compress_warm_segment_sync`] on the blocking pool.»). Con Q1 también desaparecen las docs duplicadas en `cold_disk.rs:125`/`134-136` y `warm_disk.rs:210`/`219-221`.
- `warm_disk.rs:65` y `policy.rs:60` tienen más de 100 columnas, y el primero dice «ensures physical durability on disk» cuando alcanza con «synced with `sync_data`».
- **Estado:** ✅ aplicada (paso 1). Además se recortaron las docs de `ColdDiskLog`/`WarmDiskLog` («Tier 2/3 …») y de `force_rotate_warm`, `rehydrate` y `evict_older_than`.

## Sugerencias

### Q1. Unificar `read_range`/`read_range_with_mutations` en warm y cold
- **Dónde:** `warm_disk.rs:210-233`, `cold_disk.rs:125-148`.
- **Problema:** los dos archivos tienen el mismo par de funciones: `read_range` delega en `read_range_with_mutations` y descarta las mutaciones, y esta parsea el `start_seq` del nombre con la misma cadena `file_name().and_then(to_str).and_then(parse…)`. `read_range_with_mutations` no tiene ningún llamador fuera de `read_range` (grep en todo el workspace), y `read_range` solo se usa en `tests/cold_disk.rs:62` y `tests/warm_disk.rs:133`.
- **Propuesta:** un helper `segment_start_seq(path: &Path, tier: Tier) -> Option<SequenceNumber>` en `segment_index.rs` (ver Q2), y una sola función por tipo:
  ```rust
  pub fn read_range(path: &Path, from_seq: SequenceNumber, limit: usize) -> Result<Vec<SequencedOperation>, ServerError> {
      let start_seq = segment_start_seq(path, Tier::Cold);
      read_cold_file(path, start_seq, from_seq, &mut ReadBudget::ops(limit)).map(|(ops, _)| ops)
  }
  ```
  Si se prefiere no tocar la API pública, conservar `read_range_with_mutations` y aplicar solo el helper (ahorro ~8).
- **Ahorro estimado:** ~25 líneas · **Riesgo:** bajo. Es API `pub` sin usos en el workspace, así que **queda a verificar** si hay consumidores externos del crate. · **Prioridad:** alta.
- **Verificación (2.ª pasada):** ✅ confirmada — `read_range_with_mutations` solo la llaman los `read_range` (grep en `crates/`), `read_range` solo los tests citados, y ningún otro crate depende de `zemdb-server`; `read_cold_file`/`read_segment_file` devuelven `RecoveredLogData`, así que `.map(|(ops, _)| ops)` compila. Conservar en el doc "un archivo inexistente no devuelve nada".

### Q2. Llevar `parse_segment_filename` junto a `segment_path` y simplificarla
- **Dónde:** `warm_disk.rs:334-348`. Se usa en `segment_index.rs:93`, `cold_disk.rs:65-68` y `:142-146`, y en `warm_disk.rs:227-231`.
- **Problema:** es la inversa de `segment_index::segment_path`, pero vive en `warm_disk.rs`, y los llamadores repiten las extensiones como literales (`".wal"`, `".wal.zst"`) aunque `segment_index.rs:11-14` ya tiene `WARM_EXTENSION`/`COLD_EXTENSION`. El cuerpo arma un `Vec` para validar dos partes.
- **Propuesta:** moverla a `segment_index.rs` y dejar el `pub use` o reexport si se quiere conservar la ruta pública:
  ```rust
  pub fn parse_segment_filename(name: &str, extension: &str) -> Option<(u64, u64)> {
      let core = name.strip_prefix("segment_")?.strip_suffix(extension)?;
      let (start, end) = core.split_once('_')?;
      Some((start.parse().ok()?, end.parse().ok()?))
  }
  /// `(start, end)` of the segment file at `path` of `tier`.
  pub(crate) fn parse_segment_path(path: &Path, tier: Tier) -> Option<(u64, u64)> { … }
  ```
  El comportamiento es equivalente: con `split_once`, un tercer `_` queda en `end` y `u64::parse` lo rechaza, igual que el `parts.len() != 2` actual. `strip_*` además evita el slicing manual. `cold_disk.rs:65-68` pasa a ser `parse_segment_path(warm_path, Tier::Warm)`.
- **Ahorro estimado:** ~10 líneas (sumado a Q1) · **Riesgo:** bajo · **Prioridad:** alta.
- **Verificación (2.ª pasada):** ✅ confirmada — líneas correctas; `split_once` + `parse` rechaza lo mismo que `parts.len() != 2` (`1_2_3`, `1`, `_`, `+5` se aceptan/rechazan igual). Única diferencia: con prefijo y extensión solapados (p. ej. `segment_x` y `"_x"`) el slicing actual entra en pánico y `strip_*` devuelve `None`; con `.wal`/`.wal.zst` es inalcanzable. Al moverla a `segment_index` (que es `pub(crate)`) desaparece el ciclo de imports `segment_index`→`warm_disk`.
- **Estado:** ✅ aplicada (paso 2). Recibe el `Tier` en lugar de la extensión (con Q11), así que `parse_segment_path` no hace falta: `cold_disk.rs` pasa `Tier::Warm`.

### Q3. Derivar `min_seq`/`max_seq` de `HotBuffer` en vez de guardarlos
- **Dónde:** `hot_buffer.rs:18-19`, `50-62`, `78-84`, `148`, `163`, `166-180`, `197-205`.
- **Problema:** los dos campos siempre valen `entries.front()/back().op.seq`. Se mantienen a mano en `append`, en `rehydrate` y en `after_eviction` (que existe solo para eso). Es estado duplicado que puede desincronizarse.
- **Propuesta:** eliminar los campos y `after_eviction`:
  ```rust
  pub fn min_seq(&self) -> Option<SequenceNumber> { self.entries.front().map(|e| e.op.seq) }
  pub fn max_seq(&self) -> Option<SequenceNumber> { self.entries.back().map(|e| e.op.seq) }
  // append: if let Some(last) = self.max_seq() { if … { return Err(…) } } self.push(Entry::new(op));
  // rehydrate: *self = Self::default(); for op in ops { self.push(Entry::new(op)); }
  ```
  La semántica es idéntica: hoy `max_seq` es `None` exactamente cuando la cola está vacía (`after_eviction`), y entre un `pop_front` y `after_eviction` no se lee. De paso, los dos `while let … { if … { pop } else { break } }` (`:140-146`, `:155-161`) pasan a `while self.entries.front().is_some_and(|e| …) { self.pop_front(); }`. En `get_range` (`:96-115`), el chequeo `entries.is_empty()` y el `match self.min_seq` se reducen a un solo `let Some(min) = self.min_seq() else { return Vec::new() };`.
- **Ahorro estimado:** ~25 líneas · **Riesgo:** bajo · **Prioridad:** alta.
- **Verificación (2.ª pasada):** ✅ confirmada — los únicos mutadores de `entries` son `push` (vía `append`/`rehydrate`, que actualizan los campos) y `pop_front` (solo en `apply_sliding_window_at` y `evict_older_than`, ambos cierran con `after_eviction`); no hay truncado ni rollback externo (`tiered_log.rs` solo llama `append`, `rehydrate`, `evict_older_than`, `apply_sliding_window`, `get_range`, `min_seq`). Buffer vaciado por desalojo: hoy `max_seq = None` y `append` acepta cualquier `seq`; derivado de `back()` ocurre lo mismo. `append` con error no muta en ninguna versión. En `get_range` hay que conservar el `budget.is_exhausted()`.
- **Estado:** ✅ aplicada (paso 2). `append` lee el último de `entries.back()`, y `max_seq()` sigue siendo `#[cfg(test)]`.

### Q4. Helper «abrir un segmento para leer» compartido entre warm y cold
- **Dónde:** `warm_disk.rs:299-312`, `cold_disk.rs:327-338`.
- **Problema:** los dos lectores repiten la misma secuencia: si el presupuesto está agotado devuelven vacío, registran el evento en `io_probe`, y abren el archivo con `NotFound` interpretado como vacío. El directorio padre se calcula con el mismo `unwrap_or_else(|| Path::new(""))`.
- **Propuesta:** en `segment_reader.rs`:
  ```rust
  /// The file at `path` for a read within `budget`: `None` if the budget is exhausted or the
  /// file is missing (the caller detects the gap it leaves).
  pub(crate) fn open_segment(path: &Path, event: IoEvent, budget: &ReadBudget) -> Result<Option<File>, ServerError>
  ```
  Los llamadores quedan en `let Some(file) = open_segment(path, IoEvent::SegmentRead, budget)? else { return Ok(Default::default()) };`. Se mantiene el mismo orden de efectos (presupuesto → registro → open).
- **Ahorro estimado:** ~10 líneas netas · **Riesgo:** bajo · **Prioridad:** media.
- **Estado:** ✅ aplicada (paso 2)

### Q5. `remove_file` que tolera `NotFound`, repetido 4 veces
- **Dónde:** `tiered_log.rs:523-527`, `tiered_log.rs:864-868`, `segment_index.rs:132-136`, `cold_disk.rs:74-78` (variante que solo hace `warn`).
- **Problema:** las cuatro repiten el mismo `match` de 4–5 líneas. Fuera del alcance aparece igual en `actor/lease.rs:86`, `actor/manager.rs:659` y `relay/files.rs:110`.
- **Propuesta:** `durable::remove_file_if_exists(path) -> io::Result<()>`. En `prune_segments`, `remove_file_if_exists(&segment.path)?;` (el `?` ya convierte a `ServerError`). En `cold_disk.rs:74`, `if let Err(e) = durable::remove_file_if_exists(&tmp_path) { tracing::warn!(…) }`. No cambia ni el orden ni los `sync_dir` posteriores.
- **Ahorro estimado:** ~10 líneas en el módulo (−7 por el helper en `durable.rs`, que se recupera con los otros tres usos) · **Riesgo:** bajo · **Prioridad:** media.
- **Estado:** ✅ aplicada (paso 2). El helper unifica el que ya tenía `relay/files.rs` y conserva su fail point, que pasó a llamarse `remove_file`. Los dos `sync_dir(x.parent()…)` de `compress_warm_segment_sync` usan además `durable::sync_parent`.

### Q6. Abrir y bloquear `active.wal` en un solo helper
- **Dónde:** `warm_disk.rs:75-84` y `:243-250`.
- **Problema:** las dos aperturas de `active.wal` repiten `OpenOptions` más `try_lock_exclusive` con el mismo mensaje `RoomLocked`. Ese mensaje lo comparan `tests/room_lifecycle_resilience_tests.rs:396` y `:812`, así que no se puede cambiar.
- **Propuesta:** `fn open_active_locked(path: &Path, create: bool) -> Result<File, ServerError>` con `.read(true).write(true).create(create).truncate(false)`. Para `create = false` es lo mismo que hoy, porque `truncate` es `false` por defecto. El `created = !active_path.exists()` se sigue calculando antes de abrir. También vale agregar `fn active_path(&self) -> PathBuf`: hoy `self.segments_dir.join(ACTIVE_SEGMENT)` se repite en `:71`, `:160`, `:204` y `:238`, y `tiered_log.rs:347` usa el literal `"active.wal"` en lugar de la constante.
- **Ahorro estimado:** ~8 líneas · **Riesgo:** bajo · **Prioridad:** media.
- **Estado:** ✅ aplicada (paso 2). `active_path` es `pub(crate)` y reemplaza también el literal de `tiered_log.rs`.

### Q7. `with_overrides`: alias corto para que rustfmt no parta las líneas
- **Dónde:** `policy.rs:123-160`.
- **Problema:** con el nombre `overrides`, rustfmt (con `chain_width` 60 por defecto) parte en 3–4 líneas 7 de los 12 campos.
- **Propuesta:** `let o = overrides;`. Así `segment_max_bytes`, `warm_disk_ttl`, `cold_disk_ttl`, `max_room_disk_bytes`, `lease_timeout` y `dormant_after` entran en una línea cada uno, y solo `forget_dormant_after` y `snapshot_demand_ttl` siguen partidos. Es un cambio puramente de formato.
- **Ahorro estimado:** ~13 líneas · **Riesgo:** nulo · **Prioridad:** media.
- **Estado:** ✅ aplicada (paso 1)

### Q8. Cierre duplicado del pase de mantenimiento async y sync
- **Dónde:** `tiered_log.rs:360-408`.
- **Problema:** `run_maintenance` y `run_maintenance_sync_at` repiten el cálculo de `cold_path` (`cold_segment_path(&self.segments_dir, …)`), la llamada a `record_compression` con `first_error.get_or_insert` y el cierre `expire` + `first_error.map_or(expired, Err)?`. La precedencia de errores podría divergir entre los dos.
- **Propuesta:** (a) que `warm_segments_due` devuelva `(SegmentEntry, PathBuf)` con el `cold_path` ya calculado. (b) Extraer `fn finish_maintenance(&mut self, clock, mut report, first_error: Option<ServerError>) -> Result<MaintenanceReport, ServerError>` con las 3 líneas del cierre. En el async quedaría `blocking_io(|| self.finish_maintenance(clock, report, first_error))`: meter el `map_or` dentro del closure de `blocking_io` no cambia efectos, porque es puro.
- **Ahorro estimado:** ~6 líneas · **Riesgo:** bajo · **Prioridad:** media-baja.
- **Estado:** ⏭️ no aplicada — (b) ya la resolvió el Lote 8, y con `run_maintenance_at` como único llamador de `warm_segments_due`, mover allí el cálculo de `cold_path` (a) no ahorra líneas: solo cambia de lugar la llamada y obliga a devolver una tupla.

### Q9. `FileError` con `thiserror`
- **Dónde:** `cold_disk.rs:280-290`.
- **Problema:** el `impl Display` + `impl Error` es boilerplate escrito a mano, y el crate ya depende de `thiserror` (`crates/server/Cargo.toml:14`).
- **Propuesta:** `#[derive(Debug, thiserror::Error)] #[error("{0}")] struct FileError(io::Error);`. El campo posicional sin `#[source]`/`#[from]` no se toma como `source()`, así que el comportamiento es idéntico al actual: mismo `Display` y `source() == None`.
- **Ahorro estimado:** ~6 líneas · **Riesgo:** bajo · **Prioridad:** baja.
- **Estado:** ✅ aplicada (paso 1)

### Q10. `compress_to`: comparar la secuencia sin `unwrap_or_default`
- **Dónde:** `cold_disk.rs:233-240`.
- **Problema:** primero se verifica `next_seq.is_some_and(...)` y después se hace `next_seq.unwrap_or_default()` dentro del mensaje, lo que obliga al lector a convencerse de que el default nunca se usa.
- **Propuesta:**
  ```rust
  if let Some(expected) = next_seq.filter(|&expected| expected != seq) {
      return Err(invalid(format!("operation {seq}, expected {expected}")));
  }
  ```
  El mensaje no cambia.
- **Ahorro estimado:** ~2 líneas · **Riesgo:** nulo · **Prioridad:** baja.
- **Estado:** ✅ aplicada (paso 1)

### Q11. `Tier::extension()` en lugar de dos envoltorios de ruta
- **Dónde:** `segment_index.rs:10-14`, `86-92`, `185-214`.
- **Problema:** `warm_segment_path`/`cold_segment_path` solo eligen la extensión, y `scan` arma a mano el par `(tier, extension)`.
- **Propuesta:** `impl Tier { fn extension(self) -> &'static str }` y `pub(crate) fn segment_path(dir, start, end, tier: Tier)`. Los usos son pocos: 2 en `warm_disk.rs`, 3 en `tiered_log.rs` y 2 en `tests/segment_index.rs:11-12`. `scan` puede iterar `[Tier::Cold, Tier::Warm]` buscando la extensión que coincide. El orden importa, porque `.wal.zst` debe probarse antes que `.wal`, así que hay que conservarlo.
- **Ahorro estimado:** ~8 líneas · **Riesgo:** bajo · **Prioridad:** baja.
- **Estado:** ✅ aplicada (paso 2)

### Q12. `rotate_active_segment` con `let-else`
- **Dónde:** `warm_disk.rs:151-180`.
- **Problema:** todo el cuerpo queda anidado dentro de un `if let … else { Ok(None) }`.
- **Propuesta:** `let (Some(start), Some(end)) = (…) else { return Ok(None) };`, igual que en `tiered_log.rs:245-250`. Se mantiene el orden flush/sync_all → rename → reset → sync_dir.
- **Ahorro estimado:** ~2 líneas y un nivel menos de anidación · **Riesgo:** nulo · **Prioridad:** baja.
- **Estado:** ✅ aplicada (paso 1)

### Q13. Detalles menores
- `Ok((Vec::new(), Vec::new()))` aparece en `cold_disk.rs:328`, `:336` y `warm_disk.rs:240`, `:300`, `:310`. Se puede escribir `Ok(Default::default())`. No ahorra líneas, pero es más corto.
- `RecoveredLogData` se define en `warm_disk.rs:17` y lo usan cold, tiered y segment_reader. Su lugar natural es `segment_reader.rs`, junto a `FrameRead`. Prioridad baja.
- `read_newest` (`tiered_log.rs:702-726`): `chunk.0`/`chunk.1` se leen mejor desestructurando (`let (chunk_ops, chunk_mutations) = match …`), y `distinct` puede salir de `active_mutations` después de desestructurar `active`. No ahorra líneas, mejora la claridad.
- `io_probe::IoEvent::ActiveWalPathRead` (`warm_disk.rs:303-307`): en producción nunca se llama a `read_segment_file` con `active.wal`, así que la rama solo sirve como guarda del test `tests/tiered_log.rs:405`, que verifica que el conteo sea 0. Conviene dejarla, ver abajo.
- **Estado:** ✅ aplicada (paso 1) en `Ok(Default::default())` y en la desestructuración de `read_newest`. Mover `RecoveredLogData` a `segment_reader.rs` cruza archivos y queda para el paso 2; `ActiveWalPathRead` se deja, como indica el punto.
- **Estado:** ✅ aplicada (paso 2) en `RecoveredLogData`, que ahora está en `segment_reader.rs`.

## Lo que conviene dejar como está
- **El bucle de frames de `compress_to` (`cold_disk.rs:207-254`) frente a `read_frames` (`segment_reader.rs:85-178`).** Se parecen, pero la semántica es opuesta. `compress_to` es estricta: cualquier anomalía es un error que impide comprimir, verifica cada payload y la continuidad completa contra el nombre, y copia los bytes crudos al encoder y al digest. `read_frames` tolera colas rotas (`anomaly`) y saltea payloads sin verificarlos. Unificarlas exigiría parámetros de modo que oscurecen las dos rutas de integridad.
- **El manejo de anomalías en cold y en warm.** `cold_disk.rs:346-358` (re-descomprime, porque el stream no puede retroceder) y `finish_file_read` (`warm_disk.rs:321-332`, con seek) resuelven lo mismo con medios distintos. El comentario `:347-349` justifica el costo.
- **`DigestWriter`, `TaggedFile` y `decompression_error`** (`cold_disk.rs:151-316`). Son necesarios para distinguir un error de I/O de corrupción del stream zstd y para verificar la copia fría antes de borrar la tibia.
- **`compress_warm_segment_sync`** (`cold_disk.rs:53-109`): el orden rename → `sync_dir` → verificación → `remove_file` → `sync_dir` es el núcleo de la durabilidad. El chequeo `!warm_path.exists()` (`:57`) parece redundante con el `File::open` posterior, pero cambia la variante de error (`Wal` en vez de `Io`), así que se deja.
- **`prune_segments`/`record_pruned_through`** (`tiered_log.rs:515-562`): el orden (meta durable → borrados → `sync_dir` → desalojo de RAM → `tail_seq`) y la actualización del índice aunque falle un borrado están justificados.
- **`PolicyJson` y sus dos `From`** (`policy.rs:331-391`): son el precio de `#[serde(into/from)]` con segundos redondeados hacia arriba. Las 12 `#[serde(skip_serializing_if = …)]` de `RoomLifecycleOverrides` solo se reducirían con `serde_with::skip_serializing_none`, que no está entre las dependencias y no justifica agregarse.
- **Los `match` de `PolicySetting::field`/`env_var`/`bounds`**: una tabla única ahorraría unas 3 líneas a costa de legibilidad.
- **`ReadBudget`, `retention.rs`, `io_probe.rs` y `segment_reader.rs` en general**: son compactos y están bien documentados.
- **`contiguous_suffix`, `RamFill::keep_newest` y `newest_distinct`** (`tiered_log.rs:738-811`): son cortos y cada uno documenta un invariante de contigüidad o de memoria acotada.
- **`lock_log`** (`tiered_log.rs:829-843`) frente al lock de `active.wal`: el mensaje y el archivo difieren, y el de `active.wal` lo comparan tests de integración. Compartir el helper ahorraría unas 2 líneas.

## Tests
- **Constructores de operaciones duplicados:** `make_op` está en `tests/tiered_log.rs:10` y `tests/warm_disk.rs:6`, `op` en `tests/segment_reader.rs:7` (con timestamp `seq` en vez de `seq * 1000`), y hay un constructor inline en `tests/cold_disk.rs:70-73`. Los constructores de frames `frames` (`tests/cold_disk.rs:66-80`), `segment` (`tests/segment_reader.rs:13-21`) y `write_active` (`tests/warm_disk.rs:79-89`) son el mismo bucle, y `seq()` se repite en `tests/retention.rs:3` y `tests/segment_index.rs:4`. Un módulo `#[cfg(test)] mod test_support` declarado en `log/mod.rs` con `#[path = "tests/support.rs"]` ahorraría unas 30 líneas. **Queda a verificar** que encaje con la convención del repo, porque no encontré precedentes de un módulo de soporte compartido en el crate.
  - **Estado:** ✅ aplicada (paso 3). `log/mod.rs` declara `#[cfg(test)] #[path = "tests/support.rs"] mod test_support` con `seq`, `make_op`, `seqs`, `frames` y `segment_file`; los tests siguen siendo hijos de cada módulo. `tests/segment_reader.rs` y `tests/cold_disk.rs` usan ahora `make_op` (timestamp `seq * 1000` en vez de `seq`): sus offsets se calculan, así que no dependen del tamaño de los frames. `shorten_warm_segment` conserva su bucle porque escribe ids de mutación.
- **`tests/tiered_log.rs`:** el bucle `for seq in 1..=N { log.append(make_op(seq), None).unwrap(); }` aparece unas 17 veces (`:49`, `:61`, `:79`, `:104`, `:132`, `:154`, `:173`, `:198`, `:216`, `:235`, `:255`, `:377`, `:855`, `:896`, `:914`, `:1009`, `:1034`). Un helper `append_plain(&mut log, 1, N)` ahorra unas 34 líneas. No conviene reutilizar `append_range` (`:287`), porque agrega `MutationId` y cambiaría los datos de esos tests. `reopen_after_full_prune_preserves_head` y `reopen_after_full_prune_continues_sequence` (`:194-229`) comparten el setup completo, que se puede extraer como `fully_pruned_log(dir)` (~8 líneas).
  - **Estado:** ✅ aplicada (paso 3). `append_plain(log, first, last)` reemplaza los bucles sin ids de mutación (los de `large_op` y los que comprueban `head_seq` en cada vuelta se dejan). `fully_pruned_log` devuelve el log abierto, así que también lo usan `fully_pruned_log_rejects_cursor_behind_head`, `append_after_full_prune_keeps_tail_on_new_op` y `reopen_with_corrupt_log_meta_fails_instead_of_resetting_head`.
- **`tests/policy.rs:43-147`:** cada caso de la tabla ocupa 7 líneas por el literal `RoomLifecycleOverrides { campo: Some(x), ..Default::default() }`. Construirlos con `serde_json::from_value(json!({"ram_max_ops": 0}))` o con una pequeña macro deja cada caso en una línea (~50–70 líneas). **A verificar:** el camino por serde suma la deserialización al test; si se quiere evitarla, un closure por campo también sirve.
  - **Estado:** ✅ aplicada (paso 3) con `serde_json::from_value(json!({ field: value }))`: el nombre del campo es a la vez la clave y el texto esperado (`lifecycle.{field}`), y `deny_unknown_fields` hace fallar un nombre mal escrito.
- Los 4 `sleep(Duration::from_millis(60))` de `tests/tiered_log.rs` (`:64`, `:82`, `:157`, `:380`) se podrían reemplazar por `MaintenanceClock::after`/`apply_sliding_window_at`, que ya existen para eso. Serían más rápidos y deterministas. **A verificar** caso por caso: algunos ejercitan el desalojo dentro de `append`, que usa `Instant::now()`.
  - **Estado:** ⏭️ no aplicada — en los cuatro casos el `sleep` precede a un `append` cuyo desalojo por TTL (con `Instant::now()`) es lo que el test ejercita; `MaintenanceClock` solo llega al pase de mantenimiento.
- La cobertura es buena: crash-safety (fallos inyectados de `sync_dir`), colas rotas frente a corrupción, contigüidad, conteo de I/O. No vi tests redundantes entre sí.
