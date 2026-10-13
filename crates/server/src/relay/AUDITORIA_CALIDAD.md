# Auditoría de calidad — `crates/server/src/relay/`

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `relay/{mod,upload,download,files,types,recovery,lifecycle}.rs`, tests `relay/tests/{common,upload,download,files,lifecycle,recovery}.rs` y `src/tests/relay.rs` (los tests se revisaron con menos profundidad).

## Resumen

- El código está bien: funciones cortas, errores tipados, RAII para el contador de uploads (`PendingUpload`), un lock por sala y la secuencia tmp → fsync → rename → `sync_dir` aislada en `files.rs`. No hay narrativa histórica ni códigos de auditoría en los comentarios.
- Ahorro estimado en el código fuente: **~55–70 líneas de 1 344 (≈4–5 %)**. Viene sobre todo de un helper común para el cierre de `stage_snapshot` y `stage_chunk` (Q1), de `Option::take_if` (Q2) y de un `impl Display` que nadie usa (Q3).
- Ahorro estimado en tests: **~150–200 líneas de 1 413 (≈11–14 %)**. Casi todo sale de un helper `part_of(n, index, &snapshot)` que reemplaza ~28 llamadas `part(n, i, layout, hash, &data[ranges[i].clone()])` de 5 a 13 líneas cada una (rustfmt).
- Comentarios: densidad adecuada (≈10 % doc). La mayoría explica el *por qué* (crash-safety, orden de fsync, concurrencia) y hay que conservarlos. El único con un problema real es el doc de `SnapshotRelay` (`mod.rs:70`), que está desactualizado.
- Ninguna sugerencia toca el orden tmp+fsync+rename+`sync_dir` ni descarta resultados de I/O.

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| `mod.rs` | 191 | 135 | 39 | 0 | 17 | ~8–10 |
| `upload.rs` | 467 | 397 | 28 | 6 | 36 | ~30–38 |
| `download.rs` | 139 | 114 | 13 | 0 | 12 | ~3 |
| `files.rs` | 225 | 177 | 24 | 0 | 24 | ~10 |
| `types.rs` | 76 | 47 | 17 | 0 | 12 | 0 |
| `recovery.rs` | 133 | 113 | 5 | 6 | 9 | 0 |
| `lifecycle.rs` | 113 | 90 | 9 | 4 | 10 | ~5–7 |
| **Fuente (subtotal)** | **1 344** | **1 073** | **135** | **16** | **120** | **~55–70** |
| `tests/upload.rs` | 748 | 677 | 0 | 20 | 51 | ~110–140 |
| `tests/common.rs` | 184 | 160 | 4 | 0 | 20 | +10 a +20 (helpers nuevos) |
| `tests/lifecycle.rs` | 155 | 134 | 0 | 7 | 14 | ~20–25 |
| `tests/recovery.rs` | 150 | 134 | 0 | 5 | 11 | ~20–25 |
| `tests/download.rs` | 108 | 85 | 0 | 8 | 15 | ~6 |
| `tests/files.rs` | 16 | 15 | 0 | 0 | 1 | 0 |
| `src/tests/relay.rs` | 52 | 43 | 1 | 1 | 7 | ~3 |
| **Tests (subtotal)** | **1 413** | **1 248** | **5** | **41** | **119** | **~150–200 netas** |

## Comentarios

**Densidad y calidad:** son 135 líneas de doc y 16 de `//` sobre 1 344. Están bien redactados, en inglés y en presente. No hay referencias a auditorías (C-xx, DEF-xx…) ni frases del tipo "antes hacíamos" o "este fix". Casi todos los comentarios `//` justifican algo que el código no deja ver.

**Recortar o corregir:**
- `mod.rs:70`: el doc de `SnapshotRelay` ("Ephemeral relay facilitating state-transfer chunks between peers or cold snapshots and bootstrapping clients.") está **desactualizado**. Los snapshots viven en disco y se recuperan al arrancar (`recovery.rs`), así que no son "ephemeral", y el relay no pasa chunks "between peers". Propuesta: `/// Snapshot relay of every room (see the module docs).`
- `mod.rs:113`: "if one is currently staged and valid": en realidad comprueba "unexpired" (`active()`, `mod.rs:121-124`). Propuesta: `/// Sequence of the room's unexpired active snapshot. Reads memory only, so the room actor can call it.` (2 líneas en vez de 3).
- `mod.rs:132` y `lifecycle.rs:91` ("Requires the room lock."): sobran, porque la firma recibe `&mut RoomSlot` y ese valor solo se obtiene a través del `Mutex`. El mismo aviso en `upload.rs:436` (`check_newer_than_active`) sí hay que **conservarlo**: ahí el tipo no lo garantiza.
- `mod.rs:85-89`: el doc de `new` repite casi palabra por palabra los de `clear_uploads`/`recover_snapshots` (`recovery.rs:23`, `recovery.rs:41`). Es API pública, así que es aceptable. Si se quiere, alcanza con 2 líneas y un enlace a esos métodos. Prioridad baja.
- `files.rs:30` ("Runs blocking file work on Tokio's blocking pool."): repite el doc del módulo (`files.rs:1-2`). Se puede quitar o dejar; es una sola línea.
- **Estado:** ✅ aplicada (paso 1) en `mod.rs:70`, `mod.rs:113`, los dos "Requires the room lock." y `files.rs:30`. El doc de `new` (`mod.rs:85-89`) se deja: es API pública y el punto es opcional.

**Conservar (valiosos):**
- `mod.rs:1-17`: reglas de aceptación y serialización por sala. Es el contrato del módulo.
- `upload.rs:175` (validación y hash fuera del lock), `upload.rs:344` (la sesión termina pase lo que pase), `upload.rs:363-364` (un único `sync_file` antes de verificar).
- `files.rs:120-121`: el handle de escritura que exige Windows para `sync_all`.
- `files.rs:154-156`: semántica de `promote` si falla el `sync_dir`.
- `recovery.rs:78-79`, `recovery.rs:106-107` y `recovery.rs:115-116`: invariante de no volver a un snapshot más viejo, y por qué un error al borrar no aborta.
- `lifecycle.rs:28-29` (por qué se escanea el directorio) y `lifecycle.rs:65-66` (por qué se conserva un lock referenciado).
- `upload.rs:399-404`: semántica de `try_lock` en `has_upload_in_progress`.
- `download.rs:15-22`: contrato del ancla `snapshot_hash`. Es largo, pero describe el protocolo.

## Sugerencias

### Q1. Un helper común para promover e instalar (`stage_snapshot` / `stage_chunk`)
- **Dónde:** `upload.rs:195-228` y `upload.rs:356-396`.
- **Problema:** los dos caminos terminan igual. Calculan `final_path`, clonan `uploads_dir`/`snapshots_dir` y `final_path`, y dentro de `run_blocking` hacen `preparar → promote` y, si algo falla, `remove_file_if_exists(src)` con `warn!`. Después hacen `install(StagedSnapshot { .., staged_at: Instant::now(), path: final_path })`. Son unas 34 + 41 líneas casi idénticas. Además, en `upload.rs:359` hay un `session.path.clone()` innecesario: `session` ya es propio y su `path` no se vuelve a usar.
- **Propuesta:** un método privado en `upload.rs`. La parte que cambia entre los dos caminos (escribir el tmp, o sincronizar y verificar el `.part`) entra como closure:
  ```rust
  /// Runs `prepare` on the upload file `src`, promotes it and installs it as the room's
  /// active snapshot. On any error `src` is deleted.
  async fn promote_and_install(
      &self, room_id: &RoomId, slot: &mut RoomSlot, src: PathBuf,
      head_seq: SequenceNumber, total_bytes: u64, snapshot_hash: [u8; 32],
      prepare: impl FnOnce(&Path) -> Result<(), ServerError> + Send + 'static,
  ) -> Result<(), ServerError> {
      let final_path = self.snapshots_dir.join(snapshot_file_name(room_id, head_seq, &snapshot_hash));
      let (uploads_dir, snapshots_dir, dest) =
          (self.uploads_dir.clone(), self.snapshots_dir.clone(), final_path.clone());
      run_blocking(move || {
          let result = prepare(&src)
              .and_then(|()| Ok(promote(&src, &dest, &uploads_dir, &snapshots_dir)?));
          if result.is_err() {
              if let Err(err) = remove_file_if_exists(&src) {
                  warn!(path = ?src, error = %err, "Failed to delete rejected snapshot upload");
              }
          }
          result
      })
      .await?;
      self.install(room_id, slot, StagedSnapshot {
          head_seq, total_bytes, snapshot_hash, staged_at: Instant::now(), path: final_path,
      })
      .await;
      Ok(())
  }
  ```
  Las llamadas quedan así: `stage_snapshot` pasa `move |p| Ok(write_new_file(p, &data)?)`; `stage_chunk` pasa `move |p| { sync_file(p)?; verify_snapshot_file(p, &snapshot_hash)?; Ok(()) }` si se agrega el `From<VerifyError>` de Q3, o mantiene el `match` de 3 líneas de `upload.rs:368-373` (`Io → ServerError::from`, que es `ServerError::Io` por `#[from]` en `error.rs:19`; `Invalid → BadRequest`). En ambos casos el mapeo de errores es el mismo que hoy.
- **Preservación:** se mantienen el orden de efectos (preparar → rename → `sync_dir(snapshots)` → `sync_dir(uploads)` → borrar el `src` solo si hubo error → `install`) y los mensajes de error (`BLAKE3…` e `Invalid snapshot envelope: …`, que los tests comparan en `tests/upload.rs:396` y `:433`). El mapeo `io::Error → ServerError::from` también queda igual. Solo cambia el texto de **un** `warn!` (`upload.rs:209`, "after an error"); no lo compara ningún test (verificado con grep).
- **Ahorro estimado:** ~20–25 líneas · **Riesgo:** bajo-medio (hay que firmar bien la closure `Send + 'static`) · **Prioridad:** alta.
- **Verificación (2.ª pasada):** ✅ confirmada — `promote` (rename → `sync_dir(snapshots)` → `sync_dir(uploads)`, con borrado del final si falla el sync) no cambia; `write_new_file` sigue haciendo `sync_all` antes del rename; el borrado de `src` ante error y el `ServerError::from(io::Error)` vía `?` son equivalentes; `&mut guard` se coerciona a `&mut RoomSlot` como hoy en `install`. Requiere `From<VerifyError>` (Q3) o mantener el `match` en el closure de `stage_chunk`.
- **Estado:** ✅ aplicada (paso 2) con el `From<VerifyError> for ServerError` en `files.rs`, así que el closure de `stage_chunk` usa `?`. Con `&self`, el helper recibe 8 parámetros: lleva `#[allow(clippy::too_many_arguments)]`, como `RoomActor::open`, en lugar de agrupar `head_seq`, `total_bytes` y `snapshot_hash` en una tupla. El `warn!` de `stage_snapshot` pasa a decir "Failed to delete rejected snapshot upload", como el de `stage_chunk`.

### Q2. `Option::take_if` en `install` y `expire_locked`
- **Dónde:** `mod.rs:135-143` y `lifecycle.rs:101-106`.
- **Problema:** el patrón `if x.as_ref().is_some_and(p) { if let Some(v) = x.take() { … } }` hace dos pasos para algo que la std ya ofrece (`Option::take_if`, estable desde 1.80; el toolchain es 1.97 y no hay `rust-version`).
- **Propuesta:**
  ```rust
  // mod.rs
  obsolete.extend(slot.upload.take_if(|u| u.head_seq <= staged.head_seq).map(|u| u.path));
  // lifecycle.rs
  if let Some(idle) = slot.upload.take_if(|u| u.is_idle(now)) {
      info!(room = %room_id, seq = %idle.head_seq, "Discarding idle snapshot upload");
      doomed.push(idle.path);
  }
  ```
  Opcional: `mod.rs:145-149` también puede ir en una línea (`obsolete.extend(self.snapshots.insert(..).map(|p| p.path).filter(|p| *p != new_path))`). Ojo: es menos legible.
- **Ahorro estimado:** ~8 líneas · **Riesgo:** bajo · **Prioridad:** alta.
- **Verificación (2.ª pasada):** ✅ confirmada — `Option::take_if(FnOnce(&mut T) -> bool)` existe desde 1.80 y el `rustc` local es 1.97.0; sin `rust-version` en ningún `Cargo.toml`; mismo efecto (solo toma si el predicado es verdadero).
- **Actualización tras el Lote 8 (`e88874b`):** el workspace ahora fija `rust-version = "1.85"`. `Option::take_if` es de 1.80, así que la sugerencia sigue siendo válida.
- **Estado:** ✅ aplicada (paso 1). La variante opcional en una línea de `mod.rs:145-149` no se aplicó, por ser menos legible.

### Q3. Quitar el `impl Display for VerifyError`, que nadie usa
- **Dónde:** `files.rs:181-188`.
- **Problema:** grep no encuentra ningún uso de `VerifyError` como `Display` (ni `to_string`, ni `%err`, ni `{err}`). `recovery.rs:101-108` y `upload.rs:371-372` siempre desestructuran las variantes y formatean el `String` o el `io::Error` interno. Es código muerto que el compilador no marca porque es un `impl` de trait.
- **Propuesta:** borrarlo. Si se quiere simplificar `upload.rs:368-373` (o la closure de Q1), se puede sumar un `impl From<VerifyError> for ServerError` de 6 líneas (`Io → ServerError::from`, `Invalid → BadRequest`) y usar `?`. El ahorro neto sería menor, pero el código queda más claro.
- **Ahorro estimado:** ~9 líneas (≈3 si se agrega el `From`) · **Riesgo:** bajo · **Prioridad:** alta.
- **Verificación (2.ª pasada):** ✅ confirmada — `VerifyError` (`pub(super)`, `derive(Debug)`) solo se desestructura en `recovery.rs:101-108` y `upload.rs:370-372`; no hay `impl Error` ni formateo con `%`/`{}` del enum entero.
- **Estado:** ✅ aplicada (paso 1), sin el `From<VerifyError>`: solo serviría a la closure de Q1, que es del paso 2.

### Q4. Corregir el doc desactualizado de `SnapshotRelay`
- **Dónde:** `mod.rs:70`.
- **Problema y propuesta:** ver la sección Comentarios. Contradice el diseño actual (persistencia en disco y recuperación al arrancar).
- **Ahorro estimado:** 0 líneas · **Riesgo:** nulo · **Prioridad:** alta (claridad).
- **Verificación (2.ª pasada):** ✅ confirmada — `mod.rs:70` sigue diciendo "Ephemeral relay…", contradice la persistencia en disco y la recuperación al arrancar.
- **Estado:** ✅ aplicada (paso 1)

### Q5. Un solo chequeo de tamaño máximo
- **Dónde:** `upload.rs:162-168` y `upload.rs:50-54`.
- **Problema:** el mismo chequeo y el mismo mensaje ("Snapshot of {n} bytes exceeds the maximum of {max} bytes") están escritos dos veces.
- **Propuesta:** `fn check_size(total: u64, max: u64) -> Result<(), ServerError>` junto a `check_seq_positive` (`upload.rs:452`), usado desde `stage_snapshot` y `UploadLayout::new`. El orden de validaciones de `UploadLayout::new` no cambia: sigue después de los chequeos `== 0`.
- **Ahorro estimado:** ~4 líneas · **Riesgo:** bajo · **Prioridad:** media.
- **Estado:** ✅ aplicada (paso 2)

### Q6. Un helper `bad_request` para los `BadRequest` con literal
- **Dónde:** `upload.rs:41-43`, `:46-48`, `:454-456`; `download.rs:33-36`.
- **Problema:** `Err(ServerError::BadRequest("…".to_string()))` ocupa 3–4 líneas por rustfmt.
- **Propuesta:** `fn bad_request(msg: impl Into<String>) -> ServerError` en `mod.rs` (`pub(super)`), o en `crate::error` si otros módulos lo quieren (fuera de alcance). Cada caso queda en una línea: `return Err(bad_request("total_chunks must be greater than 0"));`. Los `format!` pueden seguir como están.
- **Ahorro estimado:** ~8 líneas · **Riesgo:** bajo · **Prioridad:** media-baja.
- **Estado:** ✅ aplicada (paso 2) en `mod.rs` (privada del módulo, visible para `upload.rs` y `download.rs`) para los cuatro mensajes literales; los `format!` siguen como están.

### Q7. `has_upload_in_progress` más compacto
- **Dónde:** `upload.rs:405-428`.
- **Problema:** usa la ruta completa `std::sync::Arc::clone` (sin `use`) y un `let in_progress = match …; in_progress`. Con edition 2021 ese `let` **sí cumple una función**: el `Result<MutexGuard>` temporal de la expresión de cola viviría más que `slot` (E0597). Si se deja así, conviene un comentario de 1 línea que lo explique.
- **Propuesta (a verificar que compile):**
  ```rust
  let Some(slot) = self.slots.get(room_id).map(|s| Arc::clone(s.value())) else {
      return false;
  };
  // A held lock counts as an upload in progress.
  slot.try_lock()
      .map_or(true, |g| g.upload.as_ref().is_some_and(|u| !u.is_idle(Instant::now())))
  ```
  Con `map_or`, el `Result` se *mueve* a la llamada y el guard se suelta dentro de la closure, así que no debería quedar ningún temporal prestado. Si el compilador no lo acepta, dejar el `let` con el comentario.
- **Ahorro estimado:** ~5 líneas · **Riesgo:** bajo (solo de compilación) · **Prioridad:** media.
- **Estado:** ✅ aplicada (paso 1). Compila con `map_or`. Como `upload.rs` importa ahora `Arc`, el reexport de `Arc` en `tests/common.rs` quedó sin uso y se borró.

### Q8. Deduplicar directorios con un set en `remove_files`
- **Dónde:** `mod.rs:160-175`.
- **Problema:** la búsqueda manual `dirs.iter().any(|d: &PathBuf| d == dir)` se mezcla con el loop de borrado.
- **Propuesta:** primero borrar todos los paths, igual que ahora. Después: `let dirs: BTreeSet<&Path> = paths.iter().filter_map(|p| p.parent()).collect();` y sincronizar cada uno con su `warn!`. Se mantiene "todos los borrados antes de cualquier `sync_dir`"; solo cambia el orden en que se sincronizan los ≤2 directorios, que no tiene efecto observable. No se descarta ningún resultado.
- **Ahorro estimado:** ~4 líneas · **Riesgo:** bajo · **Prioridad:** baja.
- **Estado:** ✅ aplicada (paso 1) con un `Vec<&Path>` y `contains` en lugar del `BTreeSet`, para conservar el orden en que se sincronizan los directorios (el de su primera aparición).

### Q9. Juntar las salas a barrer en `cleanup_expired` con un `BTreeSet`
- **Dónde:** `lifecycle.rs:47-55`.
- **Propuesta:** `let rooms: BTreeSet<RoomId> = expired.chain(slots).collect();` en vez de `collect + extend + sort + dedup`. El orden de iteración sigue siendo el de `Ord`, igual que con `sort()`. `Chain` agota el primer iterador de `DashMap`, y suelta sus guards, antes de empezar el segundo.
- **Ahorro estimado:** ~3 líneas · **Riesgo:** bajo · **Prioridad:** baja.
- **Estado:** ✅ aplicada (paso 1)

### Q10. Detalles menores
- `files.rs:108-112`: `match fs::remove_file(path) { Err(e) if e.kind() == NotFound => Ok(()), other => other }`. Ahorra 1 línea.
- `upload.rs:202`, `upload.rs:360` y `lifecycle.rs:31` clonan el mismo par de directorios. Con Q1 quedan dos usos, y no vale la pena otro helper.
- **Ahorro estimado:** ~1–2 líneas · **Riesgo:** nulo · **Prioridad:** baja.
- **Estado:** ✅ aplicada (paso 1)

## Lo que conviene dejar como está

- **`promote` (`files.rs:157-172`)**, **`write_new_file`**, **`sync_file`** y el **orden de efectos en `remove_files`/`recover_snapshots`**: implementan tmp+fsync+rename+`sync_dir` y borran el archivo final si falla el sync del directorio. No hay que compactarlos más.
- **`UploadLayout::new` (`upload.rs:35-78`)**: son 5 validaciones con mensajes específicos y accionables. Aparte de Q5 y Q6, su largo está justificado.
- **La desestructuración de `SnapshotChunkUpload` (`upload.rs:243-252`)**: mueve los campos sin clonar; es la forma idiomática.
- **El `match` de `continues` (`upload.rs:283-303`)**: con los guards en orden se lee como la tabla de decisión que documenta `upload.rs:234-237`.
- **Los dos `ServerError::Internal("Upload session vanished")` (`upload.rs:327-329`, `:345-347`)**: son defensivos, para un estado imposible. Unificarlos con `Option::insert` choca con el borrow checker en la rama `continues`, y la alternativa sería `expect`. Mejor dejarlos.
- **`recover_snapshots` (`recovery.rs:42-128`)**: tiene 87 líneas, pero son una sola pasada con tres etapas claras (clasificar, elegir por sala con `settled`, borrar). Partirla en tres funciones no acortaría nada.
- **`PendingUpload` (`upload.rs:119-146`)**: RAII correcto, como pide GEMINI.md, en vez de un contador manual.
- **`RecoveredFile` como tupla (`recovery.rs:20`)**: un `struct` agregaría ~6 líneas, y la tupla solo se usa en un lugar y está documentada.
- **`get_chunk` (`download.rs:23-82`)**: el `match` sobre `(active, anchor)` es la forma más corta y legible de expresar los 4 casos.

## Tests

- **El mayor ahorro de todo el módulo (~100–130 líneas):** `tests/upload.rs` y `tests/lifecycle.rs` repiten unas 28 veces `part(n, i, layout, hash, &data[ranges[i as usize].clone()])`, que rustfmt parte en 5 a 13 líneas (por ejemplo `tests/upload.rs:446-458`, `:478-492`, `:512-526`, `:680-693`). `common.rs` ya tiene `part_by(uploader, n, index, &snapshot)` (`common.rs:163-180`). Propuesta: sumar `part_of(n, index, &snapshot) = part_by(member("uploader"), …)`, que es el mismo uploader que usa `part` (`common.rs:123`), y pasar los tests a `let snap = three_part_snapshot(1);`. Para los casos que alteran el hash o los datos (`tests/upload.rs:372-373`, `:408-410`) se puede mutar `snap.2` o `snap.0`, como ya hace `tests/upload.rs:600-601`.
  - **Estado:** ✅ aplicada (paso 3). `part_of(n, index, &snap)` reemplaza todas las llamadas a `part` que cortaban un `three_part_snapshot`; los casos con hash equivocado o envelope dañado mutan `snap.2`/`snap.0`. `part` queda para los tests que arman chunks a mano (`upload_sizes_are_bounded`, `chunk_of_the_wrong_size_is_rejected`).
- **El loop `for index in [0, 1] { relay.stage_chunk(…).await.unwrap(); }`** aparece al menos 6 veces (`tests/upload.rs:374-388`, `:411-425`, `:445-459`, `:478-492`, `:512-526`, `:228-242`). Un helper `async fn upload(relay, n, indices, &snap, bounds)` las deja en 1 línea cada una (~40 líneas menos, ya contadas dentro del rango anterior).
  - **Estado:** ✅ aplicada (paso 3) como `upload_parts(relay, n, indices, &snap, (tail, head))`, que exige que cada chunk se acepte. Los bucles que comprueban el `bool` devuelto (`multipart_upload_out_of_order_assembles_and_stages`) siguen explícitos.
- **`tests/recovery.rs`**: la escritura directa de un snapshot en disco (calcular el hash, armar el nombre y hacer `fs::write`) se repite en `:11-15`, `:19-24`, `:28-33`, `:57-63`, `:91-97` y `:114-120`. Un `fn write_snapshot_file(dir, room, n, data) -> PathBuf` en `common.rs` ahorra ~20–25 líneas.
  - **Estado:** ✅ aplicada (paso 3), más `name_of(path)` para los tests que comparan nombres de archivo. El snapshot cuyo contenido no coincide con el hash del nombre sigue escrito a mano, porque el helper nombra el archivo con el hash real.
- **El future de bounds con compuerta** (`async { rx.await.ok(); Ok(LogBounds { tail_seq: seq(0), head_seq: seq(10) }) }`) aparece en `tests/upload.rs:554-560`, `:730-736` y `tests/lifecycle.rs:106-112`. Un `fn log_bounds(tail, head) -> LogBounds`, que `bounds()` también usaría, ahorra ~10 líneas.
  - **Estado:** ✅ aplicada (paso 3)
- **`assert!(matches!(r, Err(ServerError::RoomNotFound(_))), …)`** aparece 4 veces (`tests/download.rs:29-32`, `:81-84`; `tests/lifecycle.rs:55-58`, `:114-117`). Se puede sumar `assert_room_not_found` junto a `assert_bad_request`/`assert_superseded` (`common.rs:133-145`). Ahorro neto ~6 líneas.
  - **Estado:** ✅ aplicada (paso 3)
- **Aserción redundante:** `src/tests/relay.rs:48-50` comprueba que el archivo viejo no existe justo después de comprobar que `files_in(snapshots)` es exactamente `[nuevo]` (`:44-47`).
  - **Estado:** ⏭️ no aplicada — la aserción está implicada por la anterior, pero el paso pide no quitar aserciones.
- **Lista de secuencias duplicada:** `tests/upload.rs:100` y `:123` repiten la misma lista de 15 secuencias. Conviene una `const SEQS: [u64; 15]` para que las dos no se desincronicen.
  - **Estado:** ✅ aplicada (paso 3) como `CONCURRENT_SEQS`.
- **Menores en `common.rs`:** `open` (`:33`) y `uploads_dir` (`:111`) recalculan `dir.path().join("snapshots")` en vez de usar `snapshots_dir`. `three_part_snapshot` hace un `data.clone()` innecesario (`:160`); alcanza con mover `data` después de calcular `len`. Lo mismo pasa con `SnapshotRelay::new(dir.path().join("snapshots"), …)` en `tests/upload.rs:89` y `:290`.
  - **Estado:** ✅ aplicada (paso 3)
- **Cobertura:** es buena y está orientada a invariantes: concurrencia que nunca retrocede, fallos inyectados de `sync_dir`, de lectura y de borrado, recuperación sin volver a un snapshot más viejo, y purga contra un vecino con prefijo `_`. No vi tests redundantes entre sí.
