# Auditoría de calidad — `crates/storage/tests/`

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `apply_snapshot_tests.rs`, `compaction_lifecycle_tests.rs`, `disk_wal_resilience_tests.rs`, `memory_engine_tests.rs`, `query_pushdowns_tests.rs`, `snapshot_decompression_bounds_tests.rs`, `snapshot_portability_tests.rs` (3306 líneas)

> **Actualización tras el Lote 8 (`e88874b`).** `compaction_lifecycle_tests.rs` cambió en `test_disk_concurrent_readers_and_writers` (`:134-182`). Ahora escribe la fila 1 antes de que arranquen los lectores, usa un closure `batch(i)` y verifica cada lectura.
> - El punto de `successful_reads` calculado y tirado con `let _ =`, en "Asserts verbosos", quedó resuelto.
> - Los bloques de fila de ese test que se cuentan en Q1 ya usan un helper local.
> - Las líneas de este archivo posteriores a `:180` están 3 más abajo. Por ejemplo, el `.snap.tmp.*` de Q4 ahora se crea en `:396`.
> - El resto del informe no cambia.

## Resumen

- La cobertura es buena y variada, con muchos casos de unhappy path: WAL corrupto, escrituras cortadas, cola con ceros, crash en cada fase de la compactación, flock, snapshots malformados y límites de descompresión. No sobra ningún escenario. El problema es **la forma**: casi todo el volumen es setup copiado y pegado.
- Hay dos estilos. `apply_snapshot_tests.rs` y `snapshot_decompression_bounds_tests.rs` están bien escritos: tienen helpers chicos, funciones genéricas `<E: StorageEngine>` con envoltorios `memory_*`/`disk_*` de 3 líneas y tablas de casos con mensaje por caso. Los otros cinco archivos repiten a mano filas, operaciones, `get(...).await.unwrap().unwrap()` y bucles de scan.
- **Ahorro estimado: ~700–900 líneas netas (21–27 % del alcance)**, ya restadas las ~70 líneas de un `tests/common/mod.rs` nuevo. Casi todo sale de `disk_wal_resilience_tests.rs` (~330), `query_pushdowns_tests.rs` (~250) y `compaction_lifecycle_tests.rs` (~160).
- Principales oportunidades:
  - (Q1) un `tests/common/mod.rs` con el esquema `users` y builders de filas y operaciones. Hoy hay 4 copias idénticas del esquema y 40 copias multilínea del bloque `CompactRow::new(vec![...4 valores...])` + `SequencedOperation::with_default_origin(..., Operation::insert(...))`.
  - (Q2) llevar los pares memoria/disco con cuerpo duplicado al patrón genérico que ya usa `apply_snapshot_tests.rs`.
  - (Q3) un helper `scan_rows` para los 14 bucles `while let Some(item) = stream.next().await` de `query_pushdowns_tests.rs`.
- Comentarios: hay 157 líneas `//`. La mayoría son pasos numerados que narran el código (`// 1. Open room`, `// Close room`, `// Point lookups`). Se pueden borrar unas 90 o 100. Hay que conservar las que explican el *por qué*: el lock obligatorio en Windows, el estado previo al crash, el handle de solo lectura y el orden de los chequeos. No aparecen códigos de auditoría (C-xx, DEF-xx…).

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| `apply_snapshot_tests.rs` | 402 | 339 | 14 | 3 | 46 | ~8 |
| `compaction_lifecycle_tests.rs` | 446 | 363 | 0 | 35 | 48 | ~150–180 |
| `disk_wal_resilience_tests.rs` | 1163 | 948 | 0 | 58 | 157 | ~300–380 |
| `memory_engine_tests.rs` | 356 | 292 | 0 | 18 | 46 | ~100–120 |
| `query_pushdowns_tests.rs` | 546 | 472 | 0 | 27 | 47 | ~230–280 |
| `snapshot_decompression_bounds_tests.rs` | 168 | 142 | 5 | 3 | 18 | ~10 |
| `snapshot_portability_tests.rs` | 225 | 184 | 0 | 13 | 28 | ~40–55 |
| `common/mod.rs` (nuevo) | — | — | — | — | — | +60–75 |
| **Total** | **3306** | 2740 | 19 | 157 | 390 | **~700–900 netas** |

## Comentarios

**Densidad y calidad.** Los cinco archivos de "estilo viejo" tienen un comentario cada 10 a 15 líneas. Más o menos dos tercios repiten la línea siguiente:

- `memory_engine_tests.rs`: líneas 27, 30, 34, 38 y 41 (`// 1. Open room` … `// 5. Querying closed room should fail with RoomNotFound`), 82 (`// Point lookups`) y 120 (`// Delete Bob`).
- `disk_wal_resilience_tests.rs`: líneas 322 (`// Close room`), 325 (`// Instantiate brand-new engine on same directory`), 517 (`// Apply mutation batch`), 530 (`// WAL file must now have content (> 0 bytes)`), 534 (`// Explicitly compact room`), 669 (`// 2. Close room`) y 727 (`// Reopen and ensure both operations persist`).
- `compaction_lifecycle_tests.rs`: líneas 47 (`// Create compressed snapshot`), 51 (`// Restore into room_b`), 200 (`// Insert 5 rows`), 317 y 431 (`// Verify head sequence advanced to 15`, justo encima de un `assert_eq!` que ya lo dice).
- `query_pushdowns_tests.rs`: líneas 45, 66, 87, 129, 141, 156 y 427 (`// 4. Continue reading from the original scan stream`).
- `snapshot_portability_tests.rs`: líneas 22, 59, 90, 96 y 117.

Si se aplica Q1–Q3, buena parte de estos comentarios desaparece sola, porque el nombre del helper ya dice lo mismo.

**Imprecisos o mejorables.**

- `disk_wal_resilience_tests.rs:826` dice "an obsolete op with seq 1 (or 2)". El test solo escribe seq 1. Hay que sacar "(or 2)".
- `disk_wal_resilience_tests.rs:181-182` usa el índice mágico `file_bytes[20]` con el comentario "Tamper with payload byte inside first record". Es más claro escribir `file_bytes[BATCH_HEADER_SIZE + 2]`, con la constante que ya está importada en la línea 7 (18 + 2 = 20, el mismo byte). Lo mismo pasa en `disk_wal_resilience_tests.rs:54`: `header[16]` con "Tamper with head_seq byte" se puede dejar como está, porque el comentario ya explica el offset.
- `disk_wal_resilience_tests.rs:872` dice "If stale_op had been replayed, score might have been 999 or caused sequence violation". Se puede acortar a "Replaying stale_op would set 999 or break the sequence".
- `compaction_lifecycle_tests.rs:225` habla de "leaked .snap.tmp files", pero el test busca `.tmp.` en cualquier nombre. Conviene alinear el comentario con lo que se verifica.

**Hay que conservar** (aportan el *por qué* o una invariante):

- `disk_wal_resilience_tests.rs:271-272`: hay que cerrar la sala antes de leer el WAL porque el lock es obligatorio en Windows.
- `disk_wal_resilience_tests.rs:357`: el largo no se sigue aunque el checksum del header coincida.
- `disk_wal_resilience_tests.rs:1153`: `write_all` acepta la escritura sobre un handle de solo lectura y la falla aparece en el pool bloqueante.
- `disk_wal_resilience_tests.rs:214`, `:221` y `:676`: torn write en EOF frente a cola de ceros en disco preasignado.
- `compaction_lifecycle_tests.rs:267-269` y `:364-368`: describen el estado intermedio de la compactación que se fabrica en disco. Son la especificación del test.
- `snapshot_decompression_bounds_tests.rs:33`, `:52` y `:146`: orden de los chequeos y por qué el byte extra queda fuera del largo declarado.
- `apply_snapshot_tests.rs`: todos los `///` (líneas 37-38, 52-53, 60, 73, 164-166, 256, 263-264 y 274-275). Son cortos y explican la intención de cada helper.

## Sugerencias

### Q1. `tests/common/mod.rs` con esquema, builders de filas y operaciones y lecturas
- **Dónde:**
  - **Esquema `users` idéntico (4 copias):** `compaction_lifecycle_tests.rs:8-19`, `disk_wal_resilience_tests.rs:13-24`, `memory_engine_tests.rs:8-19`, `query_pushdowns_tests.rs:11-22`.
  - **Bloque fila de 4 valores + `with_default_origin(Operation::insert(...))`, de 10 a 18 líneas por copia (40 bloques; se indica la línea del `CompactRow::new(vec![`):**
    - `compaction_lifecycle_tests.rs`: 34, 81, 143, 202, 250, 272, 293, 347, 371, 401.
    - `disk_wal_resilience_tests.rs`: 154, 160, 201, 238, 288, 294, 518, 658, 715, 798, 888, 937, 964, 981, 1086.
    - `memory_engine_tests.rs`: 55, 61, 149, 167, 232, 275.
    - `query_pushdowns_tests.rs`: 32, 116, 184, 237, 302, 365, 396, 464, 495.
  - **Filas de 2 valores con el mismo envoltorio de operación:** unas 10 más, en los tests de formato de `disk_wal_resilience_tests.rs`.
  - **`engine.get(&room, "users", &PrimaryKey::single(n)).await.unwrap()[.unwrap()]`:** unas 33 veces, de 4 a 5 líneas cada una tras rustfmt (6 en compaction, 15 en disk_wal, 11 en memory).
- **Problema:** es el grueso del volumen. Cada test vuelve a escribir una fila `users` completa, envuelve una sola operación en 4 líneas de `vec![SequencedOperation::with_default_origin(...)]` y hace la lectura con 5 líneas. Además, el nombre de la fila varía sin motivo entre "User {i}", "User_{i}", "Name_{i}", "User Name {i}" y "Concurrent User {i}".
- **Propuesta:** crear `crates/storage/tests/common/mod.rs` e importarlo con `mod common; use common::*;` en cada archivo. Este es el patrón estándar de Cargo para los tests de integración. Como el módulo no tiene `#[test]`, no se compila como test aparte.
  ```rust
  #![allow(dead_code)] // Cada archivo de tests/ es un crate y usa solo una parte.
  pub const USERS_TABLE: u16 = 0;
  pub fn users_schema() -> Schema { /* las 6 líneas actuales */ }
  pub fn row(id: i64, name: &str, score: i64, active: bool) -> CompactRow { … }
  /// La fila canónica de los tests: "User {id}", score id*10, activa si id es par.
  pub fn user_row(id: i64) -> CompactRow { row(id, &format!("User {id}"), id * 10, id % 2 == 0) }
  pub fn insert(seq: u64, id: i64, row: CompactRow, ts: u64) -> SequencedOperation { … }
  pub fn inserts(ids: RangeInclusive<i64>, ts: u64) -> Vec<SequencedOperation> // seq = id
  pub fn update(seq: u64, id: i64, col: usize, value: Value, ts: u64) -> SequencedOperation { … }
  pub async fn get_user<E: StorageEngine>(e: &E, room: &RoomId, id: i64) -> Option<CompactRow> { … }
  ```
  Antes, en `compaction_lifecycle_tests.rs:248-261`, son 14 líneas:
  ```rust
  let mut initial_ops = Vec::new();
  for i in 1..=10i64 {
      let row = CompactRow::new(vec![ /* 4 valores */ ]);
      initial_ops.push(SequencedOperation::with_default_origin(i as u64,
          Operation::insert(USERS_TABLE, PrimaryKey::single(i), row, 100)));
  }
  engine.apply_batch(&room_id, initial_ops).await.unwrap();
  ```
  Después, una línea:
  ```rust
  engine.apply_batch(&room_id, inserts(1..=10, 100)).await.unwrap();
  ```
  **Cuidado con `-D warnings`.** El CI corre `cargo clippy --workspace --all-targets -- -D warnings` (`.github/workflows/ci.yml:42`). Sin el `#![allow(dead_code)]`, cada archivo que no use todos los helpers rompe el CI.

  **A verificar** en cada test que compara valores concretos de la fila:
  - Al pasar a `user_row`, hay que actualizar los strings esperados: `"User Name 10"` (`compaction_lifecycle_tests.rs:64`), `"Concurrent User 15"` (`:187`), `"User_10"`, `"User_9"` y `"User_8"` (`query_pushdowns_tests.rs:216-224`) y `"Name_1"` (`:168`).
  - `test_limit_and_projection_pushdowns` necesita `active = (i % 2 == 0)` (lo compara en `query_pushdowns_tests.rs:169`), y eso es justo lo que da `user_row`.
  - Los tests que usan filas con nombres propios ("Alice", "Bob", "Phantom {i}") deben seguir usando `row(...)` explícito.
  - Los timestamps (10, 20, 100, 200, 300, 1000) no se comparan en ningún assert de estos archivos, pero conviene mantenerlos como parámetro y no unificarlos por las dudas.
- **Ahorro estimado:** ~400–480 líneas brutas, ~340–410 netas · **Riesgo:** bajo · **Prioridad:** alta
- **Verificación (2.ª pasada):** ⚠️ corregida — `user_row` no reproduce todas las filas. `compaction_lifecycle_tests.rs:34` usa score `i * 100`, y muchos bloques usan `Bool(true)`, entre ellos el ejemplo 248-261, así que `inserts(1..=10, 100)` no es idéntico. Ningún assert lee esos valores: el 300 de :121 viene de un update, :386/390/485/489 usan `i*10` y :169 espera `false`. Los tests siguen pasando, pero hay que documentarlo como cambio de datos.

### Q2. Llevar los pares memoria/disco al patrón genérico de `apply_snapshot_tests.rs`
- **Dónde:**
  - `query_pushdowns_tests.rs:356-451` contra `453-546`: snapshot isolation, ~95 líneas casi idénticas. Solo cambian el engine, el `RoomId` y dos comentarios.
  - `query_pushdowns_tests.rs:228-289` contra `291-354`: streaming multi-batch, ~60 líneas idénticas.
  - `memory_engine_tests.rs:268-296` contra `disk_wal_resilience_tests.rs:438-462`: sequence mismatch.
  - `memory_engine_tests.rs:298-356` contra `disk_wal_resilience_tests.rs:561-639`: crecimiento de la fila por un update de columna. La versión de disco agrega la reapertura.
  - `snapshot_portability_tests.rs:188-225`: la misma validación de esquema escrita dos veces en línea.
- **Problema:** el cuerpo del test está duplicado. Un cambio de semántica, por ejemplo en el batch size, obliga a editar dos copias, y es fácil que diverjan: los comentarios de las líneas 392 y 491, y de 436 y 535, ya difieren.
- **Propuesta:** usar el mismo esquema que `apply_snapshot_tests.rs:92-150` y `:302-402`. Se escribe una `async fn scan_is_snapshot_isolated<E: StorageEngine>(engine: &E)` y dos envoltorios `#[tokio::test]` de 3 a 4 líneas. Los checks extra de disco, como reabrir y releer, quedan en el envoltorio `disk_*`, igual que en `disk_snapshot_ahead_replaces_room_contents_durably` (`apply_snapshot_tests.rs:345-359`).
  - Para los pares que viven en archivos distintos (sequence mismatch, columna dinámica), el cuerpo genérico puede ir en `common/mod.rs` y los envoltorios quedarse donde están, así no se mueve cobertura.
  - Un `fn disk_engine(dir)` en `common` (hoy está en `apply_snapshot_tests.rs:298-300`) completa el patrón.
- **Ahorro estimado:** ~170–200 líneas · **Riesgo:** bajo. El trait `StorageEngine` expone `scan`, `apply_batch`, `get` y `get_head_seq` (`src/engine.rs:70-89`), y `apply_snapshot_tests.rs` ya lo usa así. · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — el `diff` de 356-451 contra 453-546 y de 228-289 contra 291-354 muestra que solo cambian el engine, el tempdir, el `RoomId` y los comentarios. El `tempdir` tiene que vivir en el envoltorio de disco.

### Q3. Helper `scan_rows` y vectores de claves por rango en `query_pushdowns_tests.rs`
- **Dónde:**
  - 14 bucles `while let Some(item) = …next().await` en las líneas 51, 72, 94, 135, 147, 208, 256, 270, 284, 321, 335, 349, 430 y 529, cada uno con su `let mut stream = engine.scan(...).await.unwrap();` de 4 líneas.
  - Vectores de claves escritos a mano en las líneas 57-63, 78-84 y 100-104.
- **Problema:** se repiten unas 8 a 10 líneas por consulta.
- **Propuesta:**
  ```rust
  async fn scan_rows<E: StorageEngine>(e: &E, room: &RoomId, opts: ScanOptions) -> Vec<(PrimaryKey, CompactRow)> {
      e.scan(room, "users", opts).await.unwrap().map(Result::unwrap).collect().await
  }
  fn pks(ids: impl IntoIterator<Item = i64>) -> Vec<PrimaryKey> { ids.into_iter().map(PrimaryKey::single).collect() }
  // uso:
  let got: Vec<_> = scan_rows(&engine, &room, ScanOptions::new().backward()).await.into_iter().map(|(pk, _)| pk).collect();
  assert_eq!(got, pks((1..=5).rev()));
  ```
  Los conteos con `limit` (líneas 134-139, 283-288 y 348-353) pasan a ser `assert_eq!(scan_rows(...).await.len(), 75)`. Se conserva el `unwrap` de cada ítem. Los tests de snapshot isolation (líneas 384-390 y 483-489) leen las dos primeras filas con `stream.next()` antes de mutar y no pueden usar el helper para esa parte, pero sí para la cola de la línea 430.
- **Ahorro estimado:** ~90–110 líneas (se superpone en parte con Q2) · **Riesgo:** bajo · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — los 14 bucles consumen todo el stream (no hay `break`) y hacen `unwrap` de cada ítem, y `scan_rows` hace lo mismo.

### Q4. Helper para "sala con WAL o compactación interrumpida" (tres tests fabrican el mismo estado)
- **Dónde:** `compaction_lifecycle_tests.rs:238-332`, `compaction_lifecycle_tests.rs:335-446` y `disk_wal_resilience_tests.rs:926-1029`.
- **Problema:** los tres siguen los mismos pasos:
  1. abrir la sala;
  2. insertar 1..=N;
  3. `compact_room`;
  4. `close_room`;
  5. escribir `wal.compacting` con un rango;
  6. escribir `wal` con otro rango;
  7. reabrir;
  8. verificar que `head` vale M, que todas las filas están y que `wal.compacting` ya no existe.

  Entre los tres son ~310 líneas, y las rutas se construyen de tres formas distintas: `format!("room_{}.wal", room_id.as_str())`, un literal `"room_room-compacting-crash.wal"` y `engine.wal_file_path(...)`.
- **Propuesta:** son estados intermedios distintos y obligatorios por GEMINI.md §6, así que hay que mantener los tres tests. Lo que se puede compartir es esto:
  ```rust
  /// Sala compactada hasta `snapshot`, con `compacting` en `.wal.compacting` y `wal` en el WAL activo, cerrada.
  async fn seed_interrupted_compaction(dir: &Path, room: &RoomId, snapshot: RangeInclusive<i64>,
      compacting: RangeInclusive<i64>, wal: RangeInclusive<i64>) -> (PathBuf /*wal*/, PathBuf /*compacting*/)
  async fn assert_users_present<E: StorageEngine>(e: &E, room: &RoomId, ids: RangeInclusive<i64>)
  ```
  Para las rutas, usar `engine.wal_file_path(&room)` (`src/disk/mod.rs:258`) y `.with_extension("wal.compacting")`, que es exactamente lo que hace `src/disk/compactor.rs:21`. El tercer test agrega su `.snap.tmp.*` aparte.

  **A verificar:** los tres usan timestamps distintos por segmento (100, 200, 300) y no los comparan en ningún assert. Si se pasa `ts` como parámetro, no cambia nada.
- **Ahorro estimado:** ~120–150 líneas · **Riesgo:** bajo-medio (hay que fabricar exactamente los mismos bytes en disco) · **Prioridad:** alta
- **Verificación (2.ª pasada):** ⚠️ corregida — el `.snap.tmp.*` lo crea el **segundo** test (`compaction_lifecycle_tests.rs:393`), no el tercero. El tercero (`disk_wal_resilience_tests.rs:976-977, 993-994`) usa rutas literales y `std::fs::write`; los otros dos usan `tokio::fs::write`, con los mismos bytes. `wal_file_path` (pub) + `with_extension("wal.compacting")` reproduce bien `compacting_wal_path`.

### Q5. Helper "escribir, cerrar, alterar el WAL y reabrir" para los tests de corrupción en disco
- **Dónde:** `disk_wal_resilience_tests.rs:144-189` (byte intermedio alterado, debe dar `WalCorruption`), `:191-226` (último byte alterado, se recupera como torn write), `:228-276` (5 bytes de basura, se trunca), `:641-744` (1024 ceros, se trunca) y `:876-923` (byte de payload del `.snap` alterado, debe dar `SnapshotCorruption`).
- **Problema:** cada test repite unas 15 líneas: `tempdir`, `options`, `engine`, `open_room`, filas, `apply_batch`, `close_room`, `read`, mutación, `write` y engine nuevo con `open_room`. Lo que de verdad cambia entre ellos es la mutación de bytes y el resultado esperado.
- **Propuesta:**
  ```rust
  /// Abre `room`, escribe una fila por id en un batch cada una, cierra y aplica `corrupt` al WAL.
  async fn write_then_corrupt_wal(dir: &Path, room: &RoomId, ids: &[i64], corrupt: impl FnOnce(&mut Vec<u8>)) -> DiskStorageOptions
  ```
  Cada test queda en mutación + reapertura + asserts, unas 10 a 15 líneas en vez de 35 a 50. Hay que mantenerlos como tests separados: son modos de fallo distintos y obligatorios. **No conviene** convertirlos en una tabla, porque los resultados esperados no son homogéneos (error, head 0, head 1 + truncado, escritura posterior).
- **Ahorro estimado:** ~60–80 líneas · **Riesgo:** bajo · **Prioridad:** media

### Q6. Asserts verbosos o redundantes
- **Dónde y propuesta:**
  - **`match err { Variante(..) => assert..., other => panic!(...) }` (7 líneas):** aparece en `disk_wal_resilience_tests.rs:424-429` (RoomLocked), `:455-461` y `memory_engine_tests.rs:289-295` (SequenceMismatch). Se reemplaza por `assert!(matches!(err, StorageError::SequenceMismatch { expected, actual } if expected.get() == 1 && actual.get() == 42), "{err:?}")`, que es el estilo de `apply_snapshot_tests.rs:100-107` (2 o 3 líneas).
  - **`match res { WalBatchDecodeResult::Ok { .. } => …, other => panic!(..) }`:** en `disk_wal_resilience_tests.rs:72-82`, `:395-406`, `:768-771` y `:1137-1140`. Se cambia por `let WalBatchDecodeResult::Ok { ops, bytes_consumed, .. } = res else { panic!("{res:?}") };`. En `:773-776`, como `WalBatchDecodeResult: PartialEq` (ya se usa en `:1141-1144`), queda `assert_eq!(reader.next_batch().unwrap(), WalBatchDecodeResult::CleanEof)`.
  - **Asserts por índice sobre una fila completa:** en `disk_wal_resilience_tests.rs:617-621` y `:634-638`, y en `memory_engine_tests.rs:351-355`, se reemplazan por `assert_eq!(row, CompactRow::new(vec![Value::Int(1), Value::String("Initial".into()), Value::Null, Value::String("Disk Note".into())]))`. `CompactRow` deriva `PartialEq` (`crates/core/src/value/row.rs:109`), así que el chequeo del largo queda incluido.
  - **Asserts implícitos en otro:**
    - `disk_wal_resilience_tests.rs:137-139` (`ops.len()`, `ops[0]` y `ops[1]`) es lo mismo que `assert_eq!(ops, vec![op1, op2])`.
    - En `:401`, `decoded_ops.len() == 2` está antes de `decoded_ops == ops`.
    - `query_pushdowns_tests.rs:213-225` (`names.len()` y tres índices) es un solo `assert_eq!` sobre el vector.
    - En `query_pushdowns_tests.rs:447-450` y `:544-545`, `contains(5)` y `!contains(11)` ya quedan cubiertos por `remaining_pks == 3..=10`. Se pueden borrar los asserts y dejar la intención en un comentario de una línea.
  - **Contadores tautológicos:**
    - `memory_engine_tests.rs:181-199`: `reads` siempre llega a 10 y el `get` descarta su `Result`.
    - `compaction_lifecycle_tests.rs:158-176`: `successful_reads` se calcula y se tira con `let _ =`.

    Se puede quitar el contador y dejar solo el bucle de lecturas concurrentes. Cambiar `let _ =` por `.unwrap()` endurecería el test (**a verificar**: cambia lo que se comprueba, aunque la sala está abierta y `get` debería devolver `Ok`).
  - **`assert!(x.await.is_ok())`:** en `memory_engine_tests.rs:28` y `:39`. Con `.unwrap()` se ve el error si falla.
  - **Bytes uno por uno:** `snapshot_portability_tests.rs:149-152` asigna 4 bytes por separado. Queda `bad_magic[..4].copy_from_slice(b"BAD!")`.
- **Ahorro estimado:** ~70–90 líneas · **Riesgo:** bajo · **Prioridad:** media

### Q7. Tabla de casos para la corrupción del envelope en `snapshot_portability_tests.rs`
- **Dónde:** `snapshot_portability_tests.rs:136-186`.
- **Problema:** son 4 bloques casi iguales (magic, versión, CRC, truncado). Cada uno arma los bytes, llama a `apply_snapshot` y compara con `SnapshotCorruption`.
- **Propuesta:** aplicar el mismo patrón que `apply_snapshot_tests.rs:181-222`:
  ```rust
  let cases: [(&str, Vec<u8>); 4] = [("magic", bad_magic), ("version", bad_version), ("crc", bad_crc), ("truncated", valid[..10].to_vec())];
  for (fault, bytes) in cases {
      let res = mem_engine.apply_snapshot(&room_id, schema.clone(), bytes.into(), 0u64.into()).await;
      assert!(matches!(res, Err(StorageError::SnapshotCorruption(_))), "{fault}: {res:?}");
  }
  ```
  Agregar el nombre del caso al mensaje además mejora el diagnóstico.
- **Ahorro estimado:** ~15–20 líneas · **Riesgo:** bajo · **Prioridad:** media

### Q8. Rutas de archivos: usar las del engine en lugar de literales
- **Dónde:** `disk_wal_resilience_tests.rs:179`, `215`, `252`, `672`, `828`, `910`, `976` y `993`, y `compaction_lifecycle_tests.rs:284-286`, `305-307`, `383-385`, `391-393` y `413-415`.
- **Problema:** los literales `"room_room-torn.wal"` duplican el id de la sala, que ya está en `RoomId::new("room-torn")`, y se rompen en silencio si alguien cambia uno de los dos. En compaction el `format!` ocupa 3 líneas por ruta.
- **Propuesta:** usar `engine.wal_file_path(&room_id)` y `engine.snap_file_path(&room_id)`, que son públicos (`src/disk/mod.rs:248` y `:258`) y ya se usan en `disk_wal_resilience_tests.rs:1115` y `apply_snapshot_tests.rs:323-324`. Conviene **dejar un solo test que fije el layout** con literales, `test_dual_file_storage_layout_and_compaction_truncation` (`:505-506`), porque ese es su propósito.
- **Ahorro estimado:** ~10–15 líneas · **Riesgo:** bajo · **Prioridad:** baja

### Q9. Pequeños en los archivos que ya están bien
- `apply_snapshot_tests.rs:39-50`: `snapshot_with_keys` repite el bucle de `open_with_keys` (`:74-82`). Queda `open_with_keys(&source, &room, keys).await; source.create_snapshot(&room).await.unwrap().into()`. El efecto es idéntico: `open_room` + un batch por clave con `insert(seq, key)`. Ahorro ~6.
- `snapshot_decompression_bounds_tests.rs:80-83` repite `items_schema()` (`:106-111`), y `:98-103` repite `corruption_message` (`:24-29`). Queda `assert!(corruption_message(result).contains("exceeds the maximum"))`. Ahorro ~8.
- `disk_wal_resilience_tests.rs:433-434`: el `let schema2 = test_schema();` sobra, porque `schema` se movió pero se puede pasar `test_schema()` directo. También sobra el `row.clone()` de `:209`, porque esa fila no se reutiliza. El de `:246` sí hace falta: se compara en `:269`.
- `disk_wal_resilience_tests.rs:475` y `:600`, y `query_pushdowns_tests.rs:414` y `:513`: escriben `zemdb_core::ColumnUpdate::new` con la ruta completa en vez de importarlo. Con el helper `update(...)` de Q1 desaparece.
- `compaction_lifecycle_tests.rs:226-234`: el bucle `while let` con flag `found_tmp` se reemplaza por `assert!(!std::fs::read_dir(dir)?.any(|e| e.unwrap().file_name().to_string_lossy().contains(".tmp.")), …)`. Ahorro ~5. **A verificar:** pasa de `tokio::fs` a `std::fs`, lo que en un test no cambia lo que se comprueba.
- **Ahorro estimado:** ~25 líneas · **Riesgo:** bajo · **Prioridad:** baja

## Lo que conviene dejar como está

- **`apply_snapshot_tests.rs` en su conjunto.** Es el modelo a seguir: helpers con doc de una línea, una tabla de faltas (`:181-222`) con el nombre del caso en el mensaje y el check final de que el mismo snapshot sin la falla sí se aplica (`:242-253`), que demuestra que cada rechazo se debe solo a su falla. Los envoltorios `memory_*`/`disk_*` de 3 a 4 líneas no merecen una macro: se ganarían unas 20 líneas a costa de legibilidad y de los nombres de test en el reporte.
- **`snapshot_decompression_bounds_tests.rs`.** Tiene un caso por test con nombre descriptivo y asserts sobre el texto del error. Ese texto lo comparan los propios tests, así que no hay que tocarlo.
- **`test_wal_decode_never_trusts_an_oversized_record_len` (`disk_wal_resilience_tests.rs:346-374`).** El armado byte a byte del header con su CRC es largo, pero es la única forma de fabricar un header "válido" con un largo hostil. El bucle sobre `[MAX+1, u32::MAX]` ya es una tabla.
- **Los dos tests de crash de compactación de `compaction_lifecycle_tests.rs` y el de `disk_wal_resilience_tests.rs:926`.** Hay que mantener los tres como tests separados (Q4 solo comparte el setup). Cada uno fabrica un estado intermedio distinto en disco, como pide GEMINI.md §6.
- **`test_multi_thread_flock_concurrency_stress` (`disk_wal_resilience_tests.rs:1031-1107`).** La barrera, el conteo de perdedores y el `assert` de un solo ganador son la lógica del test. Solo se puede compactar la escritura final con Q1, unas 12 líneas.
- **`test_wal_replay_skips_operations_before_snapshot_seq` (`disk_wal_resilience_tests.rs:779-874`).** La escritura manual al WAL con `OpenOptions` + `sync_all` es necesaria para fabricar el estado. Solo se pueden compactar las operaciones con Q1.
- **Los comentarios de crash-safety y de plataforma** listados en "Comentarios → Hay que conservar".

## Tests

- **Cobertura adversaria:** completa para el alcance. Ninguna sugerencia elimina un escenario. Q2, Q4 y Q5 comparten el cuerpo o el setup y mantienen un `#[tokio::test]` por modo de fallo y por engine.
- **Redundancia real entre archivos.**
  - Hay solapamiento parcial. `memory_engine_tests::test_snapshot_create_and_restore` (`:225-266`) y `compaction_lifecycle_tests::test_disk_zstd_snapshot_and_restore` (`:21-65`) prueban el ciclo crear → aplicar snapshot, que también cubren `snapshot_portability_tests::test_cross_engine_snapshot_portability` y `apply_snapshot_tests::snapshot_ahead_replaces_contents`. No sugiero borrarlos, porque cada uno ejercita un camino propio: compresión zstd, sala distinta en el mismo engine. Sí conviene compactarlos con Q1.
  - `test_file_header_rejects_invalid_magic` y `test_file_header_rejects_corrupted_crc` (`disk_wal_resilience_tests.rs:43-57`) podrían ser una tabla de dos casos, pero ya ocupan 6 líneas cada uno. No vale la pena.
- **Nombres:** cinco archivos usan el prefijo `test_` y los otros dos usan frases descriptivas (`memory_rejects_snapshot_behind_room_head`). No ahorra líneas, pero unificar hacia el estilo descriptivo mejora el reporte de `cargo test`. Prioridad baja y solo si se tocan esos tests por otro motivo.
- **Orden de adopción sugerido:** primero Q1, que es la base y lo que más rinde; después Q3 y Q2 en `query_pushdowns_tests.rs`; luego Q4 y Q5 en los tests de disco; Q6–Q9 se pueden hacer de forma oportunista. Después de cada paso hay que correr `cargo test -p zemdb-storage` y `cargo clippy --all-targets -- -D warnings`, por los `dead_code` de `common` (ver Q1).
