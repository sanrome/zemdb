# Auditoría de calidad de código — índice general

**Fecha:** 12 de octubre de 2026 · **Commit auditado:** `7bcb6bb` · **Alcance:** `crates/core`, `crates/storage`, `crates/server`, `crates/client` (código fuente y tests)

Esta auditoría mira **cómo está escrito** el código, no su correctitud. La pregunta es si se puede decir lo mismo en menos líneas y más claro, sin perder buenas prácticas ni comportamiento. También evalúa la cantidad y la calidad de los comentarios. **No se modificó código.** Cada módulo tiene su informe `AUDITORIA_CALIDAD.md` con las sugerencias numeradas (Q1, Q2, …), el `archivo:línea` de cada una, el ahorro estimado, el riesgo y la prioridad.

## Metodología

- 14 auditores en paralelo, uno por módulo, en modo solo lectura y con un límite de 13 minutos cada uno. Todos siguieron las mismas instrucciones: preservar el comportamiento exacto y los principios de `GEMINI.md`, y no proponer rediseños.
- Cada sugerencia se verificó contra el código antes de anotarla. Las que tocan API pública o no se pudieron confirmar sin compilar llevan la marca **"a verificar"**.
- El coordinador verificó en el código 5 de las afirmaciones de mayor impacto o riesgo, y las 5 se confirmaron:
  - el scan duplicado entre `memory` y `disk`;
  - que `seen` en `validate_column_updates` es código muerto;
  - que `impl Display for VerifyError` no se usa;
  - que el `Display` de `DataType` es idéntico a `Debug`;
  - que `FileHeader::compute_crc` es igual a `crc32fast::hash(&encode()[..36])`.
- **Segunda pasada adversarial**: 4 verificadores intentaron refutar las sugerencias de prioridad alta o riesgo medio. Revisaron **48 sugerencias**:
  - **39 confirmadas** y **9 corregidas**, por detalles de implementación o líneas mal citadas;
  - **0 descartadas**.
  - Cada sugerencia revisada lleva en su informe la línea `Verificación (2.ª pasada)` con el motivo.
  - Las correcciones con impacto práctico son tres:
    - `require_table` (schema) tiene que ser `pub(super)`;
    - el `run_blocking` de storage tiene que ser genérico en el tipo de error;
    - la sugerencia Q5 de memory cambia el uso de memoria, así que conviene no aplicarla tal cual.
- No se corrió `cargo`. Las métricas por archivo (líneas de código, doc, comentarios y blancos) salen de un conteo estático.
- Los ahorros son estimaciones con rango, y las sugerencias de cada módulo se pueden aplicar por separado.

## Resultado en una línea

El código está **bien escrito y bien comentado**. No hay grandes excesos en el código fuente, donde el ahorro realista es de **~1 150–1 450 líneas de 18 846 (≈6–8 %)**. El volumen recortable está en los **tests**: **~2 900–3 900 líneas de 25 395 (≈11–15 %)**, casi todo por la falta de módulos `tests/common` y por setup copiado. En total serían **~4 000–5 300 líneas (≈9–12 % del workspace)**, sin perder cobertura ni cambiar comportamiento.

## Métricas globales

| | Total | Código | Doc (`///`, `//!`) | Comentarios (`//`) |
|---|---:|---:|---:|---:|
| Código fuente (sin tests) | 18 846 | 14 100 | 2 552 | 303 |
| Tests (unitarios + integración) | 25 395 | 21 649 | 210 | 942 |

La relación entre comentarios y código fuente es de ~0,20: 1 línea de doc o comentario cada 5 de código. **No sobran comentarios en general.** Casi todo lo que hay explica el *por qué* (durabilidad, orden de fsync, invariantes, concurrencia) y hay que conservarlo. El problema es puntual (ver abajo).

## Informes por módulo

| Módulo | Informe | Líneas auditadas | Ahorro estimado | Oportunidad principal |
|---|---|---:|---:|---|
| core · schema | [`crates/core/src/schema/AUDITORIA_CALIDAD.md`](../../crates/core/src/schema/AUDITORIA_CALIDAD.md) | 1 325 | 140–180 (11–14 %) | Doble indirección de validación (`Schema` → funciones libres → `TableSchema` → funciones libres); `seen` muerto en `validate_column_updates`; helpers `require_table`/`upsert_column` |
| core · value | [`crates/core/src/value/AUDITORIA_CALIDAD.md`](../../crates/core/src/value/AUDITORIA_CALIDAD.md) | 808 | 55–75 (7–9 %) | Macro para los 6 `impl From<entero>`; `parse_uuid` con iteradores; `Display` de `DataType` |
| core · mutation | [`crates/core/src/mutation/AUDITORIA_CALIDAD.md`](../../crates/core/src/mutation/AUDITORIA_CALIDAD.md) | 459 | 40–50 (9–11 %) | `insert/update/delete` que deleguen en `Operation::new`; `#[derive(Serialize)]` en el buffer; doc falsa ("100% stack-allocated") |
| core · protocol | [`crates/core/src/protocol/AUDITORIA_CALIDAD.md`](../../crates/core/src/protocol/AUDITORIA_CALIDAD.md) | 1 303 + 207 tests | 35–46 (~2,5 %) | Ya es compacto; `replay_wal_records` con `loop`/`return`; docs desactualizadas de `TornWrite` |
| core · lib/id/crypto | [`crates/core/src/AUDITORIA_CALIDAD.md`](../../crates/core/src/AUDITORIA_CALIDAD.md) | 464 | 20–35 (4–8 %) | Borrar el alias `pub mod operation`, que no se usa; `crypto.rs` necesita **más** doc |
| core · tests de integración | [`crates/core/tests/AUDITORIA_CALIDAD.md`](../../crates/core/tests/AUDITORIA_CALIDAD.md) | 2 894 | 320–400 (11–14 %) | Helper `assert_roundtrip`; tablas de casos; tests duplicados entre archivos |
| storage · disk | [`crates/storage/src/disk/AUDITORIA_CALIDAD.md`](../../crates/storage/src/disk/AUDITORIA_CALIDAD.md) | 2 170 + 1 699 tests | 290–400 (7–10 %) | Scan copiado de `memory`; helper `fail_on_err` (6 repeticiones); `compute_crc` duplica `encode` |
| storage · memory | [`crates/storage/src/memory/AUDITORIA_CALIDAD.md`](../../crates/storage/src/memory/AUDITORIA_CALIDAD.md) | 872 | ~80 (~9 %) | Scan compartido con `disk` (~80 netas en el crate, contadas una sola vez); helper para el lock envenenado (9 repeticiones) |
| storage · engine/snapshot/options/… | [`crates/storage/src/AUDITORIA_CALIDAD.md`](../../crates/storage/src/AUDITORIA_CALIDAD.md) | 1 083 | ~80 (7–8 %) | `apply_scan_transforms` duplicada por `cfg` wasm; `KeyRange` con `Bound::as_ref`/`RangeBounds::contains` |
| storage · tests de integración | [`crates/storage/tests/AUDITORIA_CALIDAD.md`](../../crates/storage/tests/AUDITORIA_CALIDAD.md) | 3 306 | 700–900 (21–27 %) | No hay `tests/common/mod.rs`: el esquema `users` está copiado 4 veces y hay 40 bloques de fila + operación; los pares memoria/disco pueden usar el patrón genérico `<E: StorageEngine>` que ya tiene `apply_snapshot_tests.rs` |
| server · log | [`crates/server/src/log/AUDITORIA_CALIDAD.md`](../../crates/server/src/log/AUDITORIA_CALIDAD.md) | 2 788 + 1 984 tests | 220–290 (src 4,5–5,5 %) | `read_range`/`read_range_with_mutations` duplicados en `warm_disk`/`cold_disk`; `HotBuffer` guarda a mano `min_seq`/`max_seq`, que se pueden derivar de la cola; comentarios sobre una rotación por edad que no existe |
| server · actor | [`crates/server/src/actor/AUDITORIA_CALIDAD.md`](../../crates/server/src/actor/AUDITORIA_CALIDAD.md) | 2 680 + 3 845 tests | 175–235 (src 4–5,5 %) | Handlers de `room.rs` que devuelvan `Result` y respondan una vez; `RoomCommand::reject` en una línea por brazo |
| server · api | [`crates/server/src/api/AUDITORIA_CALIDAD.md`](../../crates/server/src/api/AUDITORIA_CALIDAD.md) | 2 418 + 969 tests | 135–175 (4–5 %) | 6 de 7 handlers de `data_plane.rs` repiten la misma forma (`ensure_payload` + `respond`); autenticación repetida en `extract.rs` |
| server · relay | [`crates/server/src/relay/AUDITORIA_CALIDAD.md`](../../crates/server/src/relay/AUDITORIA_CALIDAD.md) | 1 344 + 1 413 tests | 205–270 | `promote_and_install` común a `stage_snapshot`/`stage_chunk`; helper `part_of` en tests (28 llamadas) |
| server · config/error/durable/… | [`crates/server/src/AUDITORIA_CALIDAD.md`](../../crates/server/src/AUDITORIA_CALIDAD.md) | 1 369 + 749 tests | 275–325 (13–15 %) | `config.rs`: un `#[serde(default)]` en el struct elimina 23 funciones `default_*` (~110 líneas); helper `set` para los overrides |
| server · tests de integración | [`crates/server/tests/AUDITORIA_CALIDAD.md`](../../crates/server/tests/AUDITORIA_CALIDAD.md) | 7 945 | 1 300–1 800 (16–23 %): parte A 750–1 000 · parte B 550–800 | No existe `tests/common/mod.rs`: el arranque del server se copia en 5+ archivos y la fábrica de salas ~13 veces; ~45 envíos de `RoomCommand` + `oneshot` de 10–12 líneas; `server_integration_tests.rs` no usa su propio helper `post_message` |
| client | [`crates/client/AUDITORIA_CALIDAD.md`](../../crates/client/AUDITORIA_CALIDAD.md) | 10 | — | Es el esqueleto de `cargo new`; declara 5 dependencias sin usar |

## Hallazgos transversales

### 1. Comentarios

- **Sin códigos de auditoría** (`DEF-xx`, `C-xx`, …) en ningún `.rs`: se cumple la regla de `GEMINI.md`. Narrativa histórica casi no hay; quedan restos puntuales en tests ("backdoor eliminated").
- **Lo que sobra:**
  - doc-comments con tono de marketing (handlers de `api/`, `DiskStorageEngine`);
  - docs que repiten la firma ("Constructs a new AppState instance.");
  - comentarios de pasos numerados que narran el código línea a línea (`room.rs`, `apply_batch`, varios tests).
- **Lo que está mal y hay que corregir** (aunque no ahorre líneas):
  - `mutation/op.rs:60` dice "100% stack-allocated, zero heap pointers", pero hay `Vec` y `Box`;
  - `op.rs:47` dice "strictly sorted", pero no se quitan duplicados;
  - la doc de `TornWrite` describe un offset que vale siempre 0;
  - la doc de `SnapshotRelay` dice "Ephemeral", pero los snapshots persisten en disco;
  - `validation.rs:434` menciona HashMaps que ya no existen;
  - `server/src/log/tiered_log.rs:200` y `policy.rs:49` describen una rotación por edad que no existe (solo se rota por cantidad de operaciones o por bytes);
  - en tests, `room_actor_tests.rs:165` dice "Unauthorized" cuando se espera `ClientNotRegistered`, `:822` menciona `u64::MAX` cuando se prueba 999, y `disk_wal_resilience_tests.rs:826` dice "(or 2)" cuando solo se usa la secuencia 1.
- **Lo que falta:**
  - `crypto.rs` no tiene doc en el puerto público, y `NoOpCryptoEngine` no avisa que no cifra;
  - `scalar.rs` no explica por qué `Eq`/`Hash`/`Ord` son manuales (`Float` con `total_cmp` y `-0.0`).

### 2. Duplicación de código

- **Scan por lotes copiado** entre `storage/src/memory/mod.rs:102-193` y `storage/src/disk/mod.rs:298-377`. Una sola función en `engine.rs` ahorra ~80 líneas. Es la duplicación más grande del código fuente.
- **Arranque del server en tests de integración**: `ServerConfig` → `SchemaRegistry` → `SnapshotRelay` → `RoomManager` → `AppState` → listener se copia en 5+ archivos de `crates/server/tests/`, con 14 copias de `SnapshotRelay::new`. Lo mismo pasa con helpers como `assert_binary_error`, el token y `RegisterClient`. Falta un `tests/common/mod.rs`.
- **Patrones de error repetidos**:
  - `map_err(|e| StorageError::Other(...))` aparece 23 veces;
  - "lock poisoned" aparece 9 veces en `memory/mod.rs`;
  - `if let Err(err) { wal.mark_failed(...); return Err(err) }` aparece 6 veces en `disk`;
  - `spawn_blocking(..).await.map_err(join)??` aparece 10 veces.
- **Entre crates** (`fail_point`, `sync_dir`): están duplicados entre `storage` y `server`, pero unificarlos exige un crate común. **No se recomienda**, porque `server` no depende de `storage`.

### 3. API pública sin uso (todas "a verificar": dependen de que no haya consumidores externos)

- `zemdb_core::operation` (alias de compatibilidad).
- `body_crc()`, `valid_bytes_offset`, `with_default_origin` (protocol).
- `COMPRESSION_RAW`/`COMPRESSION_ZSTD`, `StorageError::EngineClosed`, `room_file_path`, `FILE_VERSION`, `WalReader::read_all` (storage).
- `impl Display for VerifyError` (relay, interno: es código muerto).
- Las dependencias declaradas en `crates/client/Cargo.toml`.

### 4. Boilerplate reemplazable

- `server/src/config.rs`: 23 funciones `default_*` más 24 atributos `#[serde(default = ...)]`.
- `RoomCommand::reject`: 13 brazos de 3 líneas.
- Los `impl From<entero>` de `Scalar`.
- `FileHeader::compute_crc`.

### 5. Observaciones fuera del alcance de calidad (anotadas, no son refactors)

Algunos auditores encontraron detalles de comportamiento o de cobertura. No se resuelven con un refactor de redacción, así que quedan para una auditoría funcional:

- `TableSchema::try_new` acepta columnas PK repetidas.
- Un PK con tipo incorrecto da errores distintos según la ruta (`TypeMismatch` vs. `PrimaryKeyTypeMismatch`).
- La limpieza de `.snap.tmp.*` ignora en silencio los errores de `read_dir` (`recovery.rs:338-351`).
- `server/src/tests/error.rs` cubre 9 de 24 variantes.
- `tests/config.rs` no prueba varios overrides de entorno.

## Orden sugerido de aplicación

1. **Riesgo bajo y mecánico.** Corregir las docs erróneas, borrar el código muerto (`seen`, `VerifyError: Display`, `pub mod operation`), aplicar `#[serde(default)]` en `config.rs`, la macro `From`, `compute_crc` y `RoomCommand::reject`. Es el lote con mejor relación entre ahorro y riesgo.
2. **Helpers compartidos en código fuente.** El scan común de storage, `respond`/`ensure_payload` en `data_plane.rs`, `promote_and_install` en relay, los handlers de `room.rs` con `Result`, `fail_on_err` en disk y la indirección de `schema/validation.rs`. Hay que revisar cada uno con los tests existentes, porque tocan rutas con orden de efectos.
3. **Tests.** `tests/common/mod.rs` en `crates/server/tests/` y en `crates/storage/tests/`, tablas de casos y fusión de tests duplicados. Es donde está el mayor ahorro absoluto, y la cobertura adversaria no se toca.
   - Los módulos `common` necesitan `#![allow(dead_code)]`: CI corre `clippy --all-targets -D warnings`, y cada binario de test usa solo una parte de los helpers.
   - En `server/tests` hay que usar funciones libres sobre `mpsc::Sender<RoomCommand>` y no `RoomManager::ask`, que agrega timeout y respawn y cambiaría lo que observan los tests.
4. **API pública sin uso.** Dejarla al final, decidiendo caso por caso si el crate tendrá consumidores externos.
