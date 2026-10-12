# Auditoría de calidad — `crates/storage/src/` (archivos de nivel superior)

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `engine.rs`, `snapshot.rs`, `lib.rs`, `options.rs`, `error.rs`, `sys.rs`, `fail_point.rs`, `tests/snapshot.rs`. Se compara además `fail_point.rs` con `crates/server/src/fail_point.rs`. Quedan fuera `tests/disk.rs` y `tests/memory.rs`; el segundo está en `memory/AUDITORIA_CALIDAD.md`.

## Resumen

- Código sólido y bien comentado en lo crítico (límites de descompresión, defensa anti-OOM de `PayloadReader`, contrato de `apply_snapshot`). `error.rs` ya usa `thiserror`. No hay códigos de auditoría (C-xx, DEF-xx, M-xx) ni narrativa histórica en estos archivos.
- **`engine.rs`** duplica `apply_scan_transforms` completo (28 líneas) solo para quitar un bound `Send` en wasm. Se resuelve con un trait `MaybeSend`, con el mismo patrón que `EngineConcurrencyBounds` del mismo archivo.
- **`options.rs`** reimplementa a mano `Bound::as_ref` y `RangeBounds::contains` (~20 líneas).
- **`fail_point.rs`** repite tres veces el patrón “buscar, quitar y disparar”, y protege con `cfg(test)` llamadas que ya son no-op fuera de tests. Además duplica casi literalmente `ARMED`/`arm`/`check` de `crates/server/src/fail_point.rs` (solo cambia el tipo de error).
- Ahorro estimado: **~75–90 líneas (~7–8 % de 1083)**. A eso se suma que `engine.rs` es el lugar natural para la máquina de estados de scan compartida (Q1 de `memory/AUDITORIA_CALIDAD.md`), que ahorra ~80 líneas en el crate.

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---|
| `engine.rs` | 184 | 127 | 36 | 0 | 21 | ~30 |
| `snapshot.rs` | 308 | 221 | 52 | 1 | 34 | ~12–15 |
| `fail_point.rs` | 243 | 176 | 45 | 0 | 22 | ~15–18 |
| `options.rs` | 179 | 139 | 19 | 0 | 21 | ~20 |
| `error.rs` | 81 | 57 | 7 | 0 | 17 | ~3 (a verificar) |
| `lib.rs` | 26 | 21 | 0 | 0 | 5 | 0 |
| `sys.rs` | 25 | 18 | 6 | 0 | 1 | ~1 |
| `tests/snapshot.rs` | 37 | 30 | 0 | 3 | 4 | 0 |
| **Total** | **1083** | 789 | 165 | 4 | 125 | **~80 (≈7–8 %)** |

## Comentarios

La densidad es adecuada (≈15 % doc) y la mayoría explica el *por qué*. Puntos concretos:

- **Vagos o imprecisos:**
  - `snapshot.rs:32` `Encapsulates a serialized room snapshot into the universal canonical container.` y `snapshot.rs:69` `Decodes and validates a universal snapshot envelope`: “universal canonical” no aporta nada. Mejor: “Envuelve `payload_bytes` en un sobre `ZMSN`, comprimiéndolo con Zstandard si `compress_zstd`.”
  - `engine.rs:44` `(move semantics)`: es ruido, la firma ya recibe `Vec` por valor.
  - `engine.rs:127`: la doc de `apply_scan_transforms` ocupa una sola línea de más de 100 columnas (“…lazily over an iterator of rows without eager allocation”). Partirla o acortarla.
- **Recortables:**
  - `fail_point.rs:1-12`: la doc de módulo, de 12 líneas, enumera cada función, y las docs de cada función ya lo dicen. Se puede reducir a ~5 líneas (propósito, no-op fuera de tests, armado por ruta, disparo único) (−6).
  - `engine.rs:52`, `60`, `77`, `85`, `88`: docs de una línea que repiten el nombre del método (“Fetches a single row by primary key…”). Son aceptables en un trait público; no vale la pena tocarlas.
- **Falta (no suma ahorro, pero suma claridad):** `EngineConcurrencyBounds` (`engine.rs:12-20`) no tiene doc. Una línea (“`Send + Sync` salvo en wasm32”) explicaría el patrón.
- **Conservar sí o sí:**
  - `snapshot.rs:21-25` y `76-81`: el tamaño declarado no es confiable; la descompresión se corta un byte después del tamaño declarado.
  - `snapshot.rs:138-144`: dónde se acota cada reserva de memoria.
  - `snapshot.rs:205-210`: por qué `PayloadReader` reemplaza al lector de bincode.
  - `engine.rs:91-109`: contrato completo de `apply_snapshot`. Es largo pero es la especificación que siguen ambos motores.
  - `error.rs:37-62`: por qué existen `SnapshotBehind`, `SnapshotHeadMismatch` y `RoomFailed`.
  - `sys.rs:3-8`: por qué no se hace nada fuera de Unix.
  - `fail_point.rs:217-222`: por qué `pause_blocking` tiene timeout.
  - `tests/snapshot.rs:17-19`: qué ataque reproduce el test.

## Sugerencias

### Q1. Una sola `apply_scan_transforms` para todas las plataformas
- **Dónde:** `engine.rs:127-184`.
- **Problema:** las dos versiones son idénticas salvo `+ Send` en el bound de `I` (`135` vs `164`). Además, los cuatro llamadores (`memory/mod.rs:139,151`, `disk/mod.rs:335,347`) pasan siempre `Some(batch_limit)`, así que el `Option` y el `match limit` (`151-154`, `180-183`) sobran.
- **Propuesta:**
  ```rust
  #[cfg(not(target_arch = "wasm32"))]
  pub(crate) trait MaybeSend: Send {}
  #[cfg(not(target_arch = "wasm32"))]
  impl<T: Send> MaybeSend for T {}
  #[cfg(target_arch = "wasm32")]
  pub(crate) trait MaybeSend {}
  #[cfg(target_arch = "wasm32")]
  impl<T> MaybeSend for T {}

  pub(crate) fn apply_scan_transforms<'a, I>(iter: I, projection: Option<Vec<u16>>, limit: usize) -> ScanIterator<'a>
  where
      I: Iterator<Item = (&'a PrimaryKey, &'a CompactRow)> + MaybeSend + 'a,
  {
      Box::new(iter.take(limit).map(move |(pk, row)| { /* igual que hoy */ }))
  }
  ```
  `I: MaybeSend` implica `I: Send` por el supertrait, que es lo que necesita la coerción a `Box<dyn … + Send>`. Si se aplica Q1 de `memory/AUDITORIA_CALIDAD.md` (scan compartido), la función queda con un solo llamador y puede ser privada. Los alias `ScanIterator`/`RowStream` duplicados por `cfg` (`22-28`, `119-125`) **deben quedar**: `dyn Trait + MaybeSend` no es válido porque solo los auto-traits pueden sumarse a un `dyn`.
- **Ahorro estimado:** ~30 líneas · **Riesgo:** bajo · **Prioridad:** alta · **A verificar:** que compile en `wasm32`.
- **Verificación (2.ª pasada):** ✅ confirmada — `I: MaybeSend` implica `I: Send` porque el supertrait se propaga, y eso alcanza para convertir a `Box<dyn … + Send>`. Los 4 llamadores pasan `Some(batch_limit)` (grep en `crates/`). `take` antes de `map` es equivalente porque el closure es puro. Solo falta compilar en wasm32.

### Q2. `KeyRange`: delegar en la biblioteca estándar
- **Dónde:** `options.rs:37-49` y `52-68`.
- **Problema:** `start_bound`/`end_bound` reimplementan `Bound::as_ref()` (estable desde Rust 1.65; el workspace no fija `rust-version`). `contains` reimplementa `RangeBounds::contains`, que usa exactamente las mismas comparaciones.
- **Propuesta:**
  ```rust
  pub fn contains(&self, pk: &PrimaryKey) -> bool {
      RangeBounds::contains(self, pk)
  }
  // impl RangeBounds<PrimaryKey> for KeyRange
  fn start_bound(&self) -> Bound<&PrimaryKey> { self.start.as_ref() }
  fn end_bound(&self) -> Bound<&PrimaryKey> { self.end.as_ref() }
  ```
  El método inherente `contains` no tiene usos en el workspace, pero es API pública: conviene conservarlo como delegación en vez de borrarlo.
- **Ahorro estimado:** ~20 líneas · **Riesgo:** bajo · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — `Value` y `PrimaryKey` tienen un `Ord` total (`core/src/value/scalar.rs:307`), así que `start <= pk` equivale a `pk >= start`. `Bound::as_ref` existe desde 1.65, y no hay `rust-version` ni toolchain fijado.

### Q3. `fail_point.rs`: helper de búsqueda y guardas `cfg` redundantes
- **Dónde:** `fail_point.rs:44-50`, `147-153` y `227-233` (buscar y quitar repetido), además de `73-78` y `96-103`.
- **Problema:**
  1. Tres copias de `iter().position(|(n, p, ..)| *n == name && p == path).map(|pos| v.remove(pos))`.
  2. `io_result` y `read_only_handle` envuelven `if check(..).is_err()` en `#[cfg(test)]` y agregan `#[cfg(not(test))] let _ = (..)`. `check` ya devuelve `Ok(())` fuera de tests y es `#[inline(always)]`; `panic_point` (`59-63`) usa justamente ese patrón sin `cfg`.
- **Propuesta:**
  ```rust
  #[cfg(test)]
  fn take<T>(list: &Mutex<Vec<T>>, hit: impl Fn(&T) -> bool) -> Option<T> {
      let mut list = list.lock().unwrap();
      list.iter().position(hit).map(|pos| list.remove(pos))
  }
  ```
  Con ese helper, `check`, `pause` y `pause_blocking` usan `take(&ARMED, |(n, p)| *n == name && p == path)`. En `io_result` y `read_only_handle` hay que quitar los `cfg` como en `panic_point`.
- **Ahorro estimado:** ~10–12 líneas (más ~6 si se recorta la doc del módulo) · **Riesgo:** bajo · **Prioridad:** media · **A verificar:** que `read_only_handle` sin `cfg` no produzca avisos de import o uso en builds no-test (`AsyncWriteExt` está dentro del bloque).

### Q4. `snapshot.rs`: API muerta, mensajes duplicados e imports repetidos
- **Dónde:** `snapshot.rs:15-19`, `36` + `56`, `199-203` + `300-304`, `274-283`, `165` + `223` + `272`.
- **Problema:**
  1. `COMPRESSION_RAW`/`COMPRESSION_ZSTD` (`15-19`) son `pub` pero no se usan en todo el workspace (`grep` sin resultados), y `SnapshotCompression::*.flag()` ya cumple esa función.
  2. Silencian el parámetro sin uso de dos formas distintas: `#[allow(unused_variables)] zstd_level` (`36`) y `let _ = compress_zstd;` (`56`).
  3. Los dos stubs de wasm repiten el literal `"Zstandard-compressed snapshots are not supported in wasm32 build"`.
  4. `decompress_bounded` repite `format!("Zstd decompression failed: {e}")` dos veces.
  5. Hay tres `use std::io::Read;` locales.
  6. Rendimiento, sin cambio de líneas: con payload sin comprimir, `encode_snapshot_envelope` copia el payload dos veces (`payload_bytes.to_vec()` en `51`/`57` y luego `extend_from_slice` en `64`). Un `Cow<[u8]>` evita la primera copia.
- **Propuesta:** borrar las constantes; usar `let _ = (compress_zstd, zstd_level);` en la rama wasm y quitar el `allow`; definir un `fn zstd_unsupported() -> StorageError` (o una constante) para los stubs; usar un closure `let failed = |e| StorageError::SnapshotCorruption(format!("Zstd decompression failed: {e}"));`; dejar un único `#[cfg(not(target_arch = "wasm32"))] use std::io::{BufReader, Read};` arriba. Los mensajes `"larger than declared"` y `"length mismatch"` los compara `tests/snapshot_decompression_bounds_tests.rs:56,64`: **no cambiarlos**.
- **Ahorro estimado:** ~12–15 líneas · **Riesgo:** bajo (las constantes son API pública: **a verificar** si hay consumidores externos) · **Prioridad:** media

### Q5. `error.rs`: variante sin uso
- **Dónde:** `error.rs:56-57` (`EngineClosed`) y `74-80`.
- **Problema:** nada construye ni compara `StorageError::EngineClosed` (0 referencias en `crates/`). Además, `zemdb_core::WalFrameError` se escribe tres veces con ruta completa.
- **Propuesta:** eliminar la variante, o documentar para qué se reserva. Importar `use zemdb_core::WalFrameError;`.
- **Ahorro estimado:** ~3 líneas · **Riesgo:** medio (enum público: quitar una variante rompe un `match` exhaustivo externo; **a verificar**) · **Prioridad:** baja
- **Verificación (2.ª pasada):** ✅ confirmada — no hay ninguna referencia a `EngineClosed` en los `.rs` del repo. `StorageError` no es `#[non_exhaustive]` y `lib.rs` lo reexporta, así que quitar la variante rompe semver. El riesgo medio es correcto.

### Q6. `sys.rs`: encadenar
- **Dónde:** `sys.rs:17-18`.
- **Propuesta:** `std::fs::File::open(target)?.sync_all()?;`, igual que la copia del server (`crates/server/src/durable.rs:26`).
- **Ahorro estimado:** ~1 línea · **Riesgo:** bajo · **Prioridad:** baja

### Q7. Helper de `spawn_blocking` con mapeo del `JoinError` (todo el crate)
- **Dónde:** 12 apariciones de `.map_err(|e| StorageError::Other(format!("Join error: {e}")))` en `crates/storage/src` (por ejemplo `memory/mod.rs:330`, `367` y `disk/mod.rs:494`, `709`, `741`).
- **Propuesta:** `pub(crate) async fn run_blocking<T>(f) -> Result<T, StorageError>` en `sys.rs` o `engine.rs`, con variante wasm que ejecuta en línea. Detalle en Q6 de `memory/AUDITORIA_CALIDAD.md`.
- **Ahorro estimado:** ~10–15 líneas en el crate · **Riesgo:** medio · **Prioridad:** baja
- **Verificación (2.ª pasada):** ⚠️ corregida — las 12 apariciones están bien, pero la firma `FnOnce() -> Result<T, StorageError>` no sirve para `compactor.rs:573` ni para `recovery.rs:83`, que devuelven `io::Result`. Tiene que ser genérica en `E: Into<StorageError>`, como en Q3 de disk. En wasm el bound `Send` sobra (usar `MaybeSend` de Q1). Además, `fail_point` no existe en wasm (`lib.rs:5-6`), así que `pause_blocking` y `fail_key` van con `cfg` dentro del closure.

## Duplicación entre crates (solo anotada)

- **`fail_point.rs` (storage) vs `crates/server/src/fail_point.rs`.**
  - La doc de módulo (primer párrafo, `storage:1-3` ≈ `server:1-7`) y el bloque `ARMED` + `arm` + `check` (`storage:17-19,33-55` ≡ `server:16-42`) son idénticos salvo el tipo de error (`StorageError::Io` vs `ServerError::Io`).
  - El `hook` del server (`server:61-79`) y el `pause` de storage (`142-161`) siguen el mismo patrón de buscar, quitar y ejecutar.
  - Compartirlos no es trivial: `#[cfg(test)]` solo se activa en el crate bajo test, así que haría falta un crate o módulo común con un *feature* (p. ej. `fail-points`) habilitado desde `dev-dependencies`. Además, `zemdb-server` **no** depende de `zemdb-storage`, así que el lugar común sería `zemdb-core`. Es un cambio de infraestructura; no se recomienda solo para ahorrar ~25 líneas.
- **`sys::sync_dir` (`sys.rs:9-25`) vs `crates/server/src/durable.rs:17-33`.** Son la misma implementación. La del server agrega `injected("sync_dir", ..)` para fault injection. Por la misma razón (no hay dependencia entre crates), se anota pero no se propone unificar.

## Lo que conviene dejar como está

- **Las seis `impl From<Range*> for KeyRange` (`options.rs:70-120`).** Una macro ahorraría ~25 líneas pero se lee peor. Un `impl<R: RangeBounds<PrimaryKey>> From<R>` genérico choca con `impl From<T> for T`, porque `KeyRange` también implementa `RangeBounds`.
- **Constructores `direction`/`forward`/`backward` de `ScanOptions` (`options.rs:150-166`).** `forward()` y `direction()` no tienen usos en el workspace, pero son API pública de builder, baratas y simétricas.
- **`PayloadReader` y su `impl BincodeRead` (`snapshot.rs:205-268`).** Es necesario para que un string declarado de longitud enorme no reserve memoria (lo prueba `tests/snapshot.rs:16`).
- **`checked_envelope` + `decode_snapshot_envelope_with_limit` + `decode_snapshot_payload`.** La factorización ya es buena. Devolver `declared_len` desde `checked_envelope` ahorraría 2 líneas: opcional.
- **`Pause` y `BlockingPause` (`fail_point.rs:106-124`, `181-199`).** Se parecen, pero usan canales distintos a propósito: la versión bloqueante necesita `recv_timeout` de `std::sync::mpsc` para no colgar un runtime de un solo hilo. Unificarlas con genéricos no compensa.
- **`lib.rs`.** Son 26 líneas de módulos y re-exports, sin nada que recortar. `pub mod format` (`lib.rs:15-18`) lo usan los tests de integración (`compaction_lifecycle_tests.rs`).
- **Doc de `apply_snapshot` en el trait (`engine.rs:91-109`).** Es la especificación compartida por ambos motores.

## Tests

- **`tests/snapshot.rs` (37 líneas).** Es correcto y conciso: cubre ida y vuelta, string con longitud declarada gigante (anti-OOM) y UTF-8 inválido. Los dos `assert!(matches!(.., Err(StorageError::SnapshotCorruption(_))), "{result:?}")` (`22-25`, `33-36`) podrían ir a un helper `assert_corrupt(result)`, pero con solo dos usos no compensa.
- **Hueco a considerar.** No hay test unitario de `deserialize_zstd` con `exact_len` distinto del consumido ni con datos sobrantes después del valor (`snapshot.rs:180-190`). Hay que verificar si los cubre `crates/storage/tests/snapshot_decompression_bounds_tests.rs`.
- **`fail_point.rs`, `options.rs` y `engine.rs`** no tienen tests unitarios propios. Se ejercitan desde los tests de los motores y los de integración (`query_pushdowns_tests.rs` usa `KeyRange::from(..=..)`). Si se aplica Q2, un test mínimo de `KeyRange::contains` con los tres tipos de `Bound` cuesta ~10 líneas y blinda el cambio.
