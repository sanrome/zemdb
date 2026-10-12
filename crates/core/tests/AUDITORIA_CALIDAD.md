# Auditoría de calidad — `crates/core/tests`

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** los 9 archivos `crates/core/tests/*.rs` (2894 líneas)

## Resumen
- La cobertura es buena y la mayoría de los tests prueba comportamiento observable con nombres descriptivos. Se notan dos generaciones de estilo:
  - tests antiguos con prefijo `test_`, asserts sueltos y comentarios "// Test X roundtrip";
  - tests nuevos con helpers (`delete_frame`, `assert_torn`, `commit_with`, `table_json`) y casos en tabla.
- Ahorro estimado: **~320–400 líneas (≈11–14 %)**, sin perder ningún caso cubierto.
- Principales oportunidades:
  1. **Helper `assert_roundtrip`** en `protocol_codec_tests.rs`: 17 bloques encode/decode/assert de 3–4 líneas cada uno.
  2. **Tests como tablas de casos**: los 5 tests de deserialización de `TableSchema` y el de `validate_column_updates` en `schema_validation_tests.rs`, y los pares `RoomId`/`SchemaId`/`ClientId` de `id_validation_tests.rs`.
  3. **Un test redundante entre archivos y varios solapados:**
     - `test_schema_deserialize_rejects_duplicate_table_name` duplica a `duplicate_table_names_are_rejected`;
     - los roundtrips antiguos de WAL quedaron cubiertos por los tests nuevos;
     - el test de bounds de cripto lo garantiza el compilador.
  4. **Que los tests antiguos de `wal_frame_tests.rs` usen los helpers** que el mismo archivo ya define más abajo.
- No hay referencias a códigos de auditoría. La única sigla dudosa, "C-DEREF", viene de las Rust API Guidelines.

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| `crypto_tests.rs` | 25 | 20 | 0 | 0 | 5 | ~6 |
| `id_validation_tests.rs` | 195 | 178 | 0 | 2 | 15 | ~50–60 |
| `protocol_codec_tests.rs` | 694 | 605 | 3 | 32 | 54 | ~65–80 |
| `schema_table_ids_tests.rs` | 222 | 189 | 4 | 2 | 27 | ~0–5 |
| `schema_validation_tests.rs` | 777 | 670 | 2 | 28 | 77 | ~120–150 |
| `snapshot_envelope_tests.rs` | 101 | 90 | 0 | 0 | 11 | ~3 |
| `squashing_rules_tests.rs` | 350 | 298 | 6 | 10 | 36 | ~15–20 |
| `types_memory_tests.rs` | 136 | 109 | 0 | 8 | 19 | ~12–16 |
| `wal_frame_tests.rs` | 394 | 322 | 2 | 12 | 58 | ~50–65 |
| **Total** | **2894** | **2481** | **17** | **94** | **302** | **~320–400** |

## Comentarios

- **Densidad:** baja (≈3 %) y en general útil. Los comentarios más valiosos explican la *razón* de un caso límite y conviene conservarlos:
  - `schema_table_ids_tests.rs:1-4`: los ids de tabla son datos persistidos.
  - `wal_frame_tests.rs:74`: corrupción frente a torn write según lo que sigue.
  - `wal_frame_tests.rs:105`: cola de ceros por `fallocate` tras un corte de luz.
  - `wal_frame_tests.rs:348-349`.
  - `squashing_rules_tests.rs:61`, `128` y `148`.
  - `protocol_codec_tests.rs:481-482`: el log usa enteros de ancho fijo.
  - `protocol_codec_tests.rs:666`: los códigos nuevos van al final.
- **Redundantes (borrar):**
  - `protocol_codec_tests.rs:36, 81, 98, 109, 121, 138, 153, 165, 177, 187, 209, 221` son 12 comentarios "// Test <Variante> roundtrip" que repiten el nombre de la variante escrita debajo. El de la línea 221, "Test **new** ErrorCodes roundtrip", además es narrativa histórica.
  - `protocol_codec_tests.rs:250`: "Append trailing corrupted/garbage bytes".
  - `wal_frame_tests.rs:67`, `84` y `97`: "Tamper with payload byte…" y "Truncate to just partial header".
  - `schema_validation_tests.rs:383`, `386`, `395` y `404` narran el código paso a paso.
- **Mejorables:**
  - `types_memory_tests.rs:12` ("Guarantee PrimaryKey fits in standard 64-byte L1 cache line without split!") y `:14`: tono exclamativo, y acompañan asserts redundantes (Q8).
  - `types_memory_tests.rs:90`: "(C-DEREF compliant, no Deref coercion)". C-DEREF es una regla de las Rust API Guidelines, no un código de auditoría, pero se confunde con el formato que prohíbe GEMINI.md. Alcanza con "sin `Deref`".
  - `schema_validation_tests.rs:235-236` ("NOT alphabetical order…!"): útil, pero sobran las mayúsculas y el signo.
  - `schema_validation_tests.rs:747-748`: el doc-comment de `columns()` describe el límite que prueban los tests, no al helper. Moverlo al test o cambiarlo por `schema::MAX_COLUMNS` (Q4).
- **Nombres:** conviven dos convenciones. Usan el prefijo `test_` `crypto_tests`, `types_memory_tests`, la mitad de `protocol_codec_tests` y `wal_frame_tests` y todo `schema_validation_tests`. Los demás tienen nombres en forma de frase. No ahorra líneas, pero unificar al estilo descriptivo ayuda.
  - Hay nombres que no coinciden con lo que prueban. `test_validate_compact_row_op_unknown_pk_column_returns_error` (`schema_validation_tests.rs:731`) prueba `TableSchema::try_new`.
  - `test_two_pointer_column_merge_linear` (`squashing_rules_tests.rs:254`) no prueba linealidad.

## Sugerencias

### Q1. Helper genérico `assert_roundtrip` en `protocol_codec_tests.rs`
- **Dónde:**
  - `protocol_codec_tests.rs:19-239`: un solo test de 221 líneas;
  - `protocol_codec_tests.rs:262-284` y `370-379`.
- **Problema:** 17 bloques repiten `let enc = encode_message(&m).expect("serialization failed"); let dec: T = decode_message(&enc).expect("deserialization failed"); assert_eq!(m, dec);`, que rustfmt reparte en 3–4 líneas. Hay 35 `expect` con esos dos textos.
- **Propuesta:**
  ```rust
  fn assert_roundtrip<M: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug>(msg: &M) {
      let frame = encode_message(msg).expect("serialization failed");
      assert_eq!(&decode_message::<M>(&frame).expect("deserialization failed"), msg);
  }
  ```
  `decode_message<'a, T: Deserialize<'a>>` admite `DeserializeOwned`. Cada bloque queda en `assert_roundtrip(&msg);`. Además:
  - mover a ese test los roundtrips de `Ack`, `AckConfirmed` y `DeregisterAck` (262-284 y 370-379), o dejarlos con el helper;
  - borrar los 12 comentarios "// Test X roundtrip";
  - reducir el bucle de `ErrorCode` (221-238) a un solo `ServerMessage::Error`, porque `error_codes_keep_their_wire_indexes` (664-694) ya hace el roundtrip de los 20 códigos;
  - opcional: reusar `commit_with` (503) para el `Commit` de las líneas 23-30, y crear `fn room() -> RoomId`, porque `RoomId::new("room-abc").unwrap()` aparece 14 veces.
- **Ahorro estimado:** ~55–65 líneas · **Riesgo:** bajo · **Prioridad:** alta.
- **Verificación (2.ª pasada):** ✅ confirmada — `decode_message<'a, T: Deserialize<'a>>` (`codec.rs:92`) acepta `M: DeserializeOwned`, el helper compila; líneas citadas (19, 221-238, 262-284, 370-379, 503, 664-694) correctas. Matiz: `error_codes_keep_their_wire_indexes` hace el roundtrip del `ErrorCode` suelto, no dentro de `ServerMessage::Error`; por eso conviene conservar un `ServerMessage::Error` como propone.

### Q2. Tabla de casos para la deserialización inválida de `TableSchema`
- **Dónde:** `schema_validation_tests.rs:584-663`: 5 tests casi idénticos de 16 líneas.
- **Problema:** solo cambian `primary_key`, `nullable`/`encrypted` del único campo, una columna extra y el texto esperado. Además, cada test hace `assert!(res.is_err())` seguido de `res.unwrap_err()`, y lo primero sobra porque `unwrap_err` ya falla si no hay error.
- **Propuesta:** un test con la tabla `[(primary_key, columns, "texto esperado"); 5]` que arme el JSON con `serde_json::json!` (como hace `table_json` en `schema_table_ids_tests.rs:10-20`) y compruebe `err.to_string().contains(expected)`.
  - Si se pasa de `from_str` a `from_value`, el mensaje ya no lleva el sufijo de línea y columna. Los `contains` siguen siendo válidos, pero **a verificar** al aplicarlo.
  - Alternativa sin riesgo: seguir usando `from_str` con `json.to_string()`.
- **Ahorro estimado:** ~55–60 líneas · **Riesgo:** bajo · **Prioridad:** alta.
- **Verificación (2.ª pasada):** ✅ confirmada — los 5 tests (`585-663`) solo comparan con `contains`; `TableSchema::deserialize` (`table.rs:135-159`) parsea el helper completo y recién después llama a `try_new`, así que el orden de claves de `json!` (BTreeMap) no cambia qué error sale. Cada caso de la tabla debe incluir `table_id` (sin default y con `deny_unknown_fields`).

### Q3. Borrar el test duplicado de nombres de tabla repetidos
- **Dónde:** `schema_validation_tests.rs:692-714` (`test_schema_deserialize_rejects_duplicate_table_name`).
- **Problema:** es el mismo caso que `schema_table_ids_tests.rs:64-70` (`duplicate_table_names_are_rejected`): JSON con dos tablas `tasks` de ids 0 y 1, y aserción sobre `"already exists"`. La única diferencia es que uno usa JSON literal y el otro el helper. El mismo error también se cubre por builder en `schema_validation_tests.rs:666-690`.
- **Propuesta:** borrar el de `schema_validation_tests.rs`.
- **Ahorro estimado:** ~24 líneas · **Riesgo:** bajo · **Prioridad:** alta.
- **Verificación (2.ª pasada):** ✅ confirmada — mismo JSON (dos `tasks`, ids 0 y 1, mismos campos) y misma aserción `"already exists"`; la única diferencia es `from_str` vs `from_value` (`schema_table_ids_tests.rs:22-24`), irrelevante porque el chequeo está en `Schema::try_from_tables` (`global.rs:96`), tras parsear.

### Q4. Compactar asserts repetidos en `schema_validation_tests.rs`
- **Dónde y propuesta:**
  - **178-196 y 256-278**: `test_compact_row_conversion` y `test_zero_copy_row_conversions` son el mismo test con `to_compact_row`/`from_compact_row` o con `row_into_compact`/`compact_into_row`. Conviene fusionarlos en uno que pruebe las dos variantes sobre la misma fila (~−18).
  - **137-159**: el mismo `PrimaryKeyTypeMismatch` se construye dos veces. Construirlo una vez (`let expected = …;`) y comparar update y delete contra él (~−8).
  - **396-414**: se repiten 4 asserts para `restored` y `restored_zero_copy`. Mejor `for row in [restored, restored_zero_copy] { … }` (~−6).
  - **237-240 y 250-253**: comparar contra listas, con `assert_eq!(names, ["id", "zebra", "alpha", "beta"])` y `assert_eq!(compact, CompactRow::new(vec![…]))`. `CompactRow` deriva `PartialEq` (`value/row.rs:109`) (~−4).
  - **418-511** (`validate_column_updates`, 94 líneas): 7 casos de error en secuencia. Convertirlos en una tabla `[(updates, expected_error)]` (~−20). Los valores de error siguen siendo verbosos, así que el ahorro es moderado.
  - **757, 760 y 770**: cambiar los literales `65_535`/`65_536` por `schema::MAX_COLUMNS`/`+ 1`, que ya usa `protocol_codec_tests.rs:520`. No ahorra líneas, pero aclara.
- **Ahorro estimado:** ~50–55 líneas · **Riesgo:** bajo · **Prioridad:** media.

### Q5. Helpers de aserción en `id_validation_tests.rs`
- **Dónde:** `id_validation_tests.rs:5-11, 20-29, 35-44, 105-114`, con el par `RoomId`+`SchemaId`, y `59-62, 68-73, 126-142, 178-183, 188-193`, con `ClientId`.
- **Problema:** cada aserción con mensaje ocupa 4–5 líneas con rustfmt, y el par Room/Schema se repite 4 veces.
- **Propuesta:**
  ```rust
  fn assert_path_ids(ids: &[&str], valid: bool) {
      for &id in ids {
          assert_eq!(RoomId::new(id).is_ok(), valid, "room id {id:?}");
          assert_eq!(SchemaId::new(id).is_ok(), valid, "schema id {id:?}");
      }
  }
  fn assert_client_id(id: &str, valid: bool) { assert_eq!(ClientId::new(id).is_ok(), valid, "client id {id:?}"); }
  ```
  Además:
  - incluir `"com0"` y `"lpt0"` en la lista de `path_ids_reject_windows_reserved_device_names` (35) y borrar `path_ids_reject_com0_and_lpt0` (103-115). Sigue siendo un test negativo del caso, como pide GEMINI.md;
  - fusionar `client_ids_reject_other_invisible_filler_and_tag_characters` (164-184) en la lista de `client_ids_reject_invisible_format_and_bidi_characters` (117-143);
  - la línea 46 solo comprueba `RoomId`; con el helper se comprobarían también los `SchemaId`. Pasan igual, porque comparten `validate_path_id`.
- **Ahorro estimado:** ~50–60 líneas · **Riesgo:** bajo · **Prioridad:** media.

### Q6. Usar los helpers de `wal_frame_tests.rs` en los tests antiguos
- **Dónde:** `wal_frame_tests.rs:12-115`. Son 6 tests `test_wal_frame_*` escritos antes de los helpers de las líneas 117-156.
- **Propuesta:**
  - **13-58**: `test_wal_frame_roundtrip` y `test_wal_frame_with_mutation_id_roundtrip` son iguales salvo el `mutation_id`. Fusionarlos en un bucle `for mutation_id in [None, Some(MutationId::new([7; 16]))]` con `let WalBatchDecodeResult::Ok { ops, mutation_id: got, bytes_consumed } = … else { panic!(…) };` (~−25).
  - **60-77**: `test_wal_frame_detects_crc_tampering` es el caso "frame válido inmediatamente después" de `checksum_mismatch_before_a_later_valid_frame_is_corruption` (323-331). Unificarlos con `for gap in [0, 9]` (~−14).
  - **79-101**: reescribir con `delete_frame` y `assert_torn`, por ejemplo `assert_torn(decode_wal_batch_from_slice(&delete_frame(1)[..5]))` (~−12). El test de la línea 103 debe quedar con `matches!` porque comprueba `valid_bytes_offset: 0`.
  - **135-137**: borrar `log_of`, que solo envuelve `frames.concat()` y se usa dos veces (~−4).
  - **144-149**: hacer `assert_corruption` genérico (`<T: Debug>(r: Result<T, WalFrameError>)`) para usarlo también con `replay_wal_records` en 279-282 (~−3).
  - Mover todos los helpers arriba del primer test.
- **Ahorro estimado:** ~55 líneas · **Riesgo:** bajo · **Prioridad:** media.

### Q7. Constantes mágicas en `wal_frame_tests.rs` y `protocol_codec_tests.rs`
- **Dónde y propuesta:**
  - `wal_frame_tests.rs:392` usa `Some(16 * 32)`, donde 32 es `BATCH_HEADER_SIZE + 14` (el relleno de la línea 374). Expresarlo así, o con una constante `FAKE_FRAME_LEN`.
  - `wal_frame_tests.rs:240` y `299` usan `[0xBA, 0x7C, 0x01]`, que son `BATCH_MAGIC` más un byte. Escribirlo como `[&BATCH_MAGIC[..], &[0x01]].concat()` o con un comentario.
  - `wal_frame_tests.rs:201-205` y `360-366` construyen un header con CRC válido de la misma forma. Se puede extraer `fn seal_header(h) -> h` (~−3).
  - `protocol_codec_tests.rs:9` y `336`: cambiar `[0x5A, 0x4D, …]` por `protocol::PROTOCOL_MAGIC`, que está reexportado vía `pub use codec::*`.
  - `protocol_codec_tests.rs:394`: `let max_frame = MAX_FRAME_SIZE;` es un alias trivial (~−1).
  - `protocol_codec_tests.rs:598-603` y `618-623` arman el mismo frame. Extraerlo a un helper `delete_commit_frame()` (~−5).
  - `protocol_codec_tests.rs:526-528` y `558-560` generan el mismo `Vec<ColumnUpdate>` de nulos. Extraer `null_deltas(n)` (~−3).
- **Ahorro estimado:** ~10–12 líneas · **Riesgo:** bajo · **Prioridad:** baja.

### Q8. Redundancias en `types_memory_tests.rs` y `crypto_tests.rs`
- **Dónde y propuesta:**
  - **`types_memory_tests.rs:13`, `15-16`**: `<= 64` y `% 8 == 0` se deducen de las igualdades exactas de las líneas 7, 10 y 11 (40, 88 y 96). Borrarlos y dejar, como mucho, un comentario de una línea con la intención: "PrimaryKey cabe en una línea de caché; Operation sin padding de cola" (~−3).
  - **`types_memory_tests.rs:35-42`**: 8 asserts por índice. Definir `expected` en orden y comparar `values` contra esa lista después del `sort()`. Hay que mantener la permutación inicial (~−6).
  - **`types_memory_tests.rs:111-118`**: el roundtrip bincode de `RoomId` y `SchemaId` como string plano repite `id_validation_tests.rs:93-101` (`ids_serialize_as_plain_strings`). Dejar solo el de `SequenceNumber` (~−7).
  - **`crypto_tests.rs:20-25`**: `test_crypto_concurrency_bounds_satisfied` no puede fallar en tiempo de ejecución, y además ya lo garantiza el compilador. `CryptoEngine: CryptoConcurrencyBounds` es supertrait, así que `impl CryptoEngine for NoOpCryptoEngine` en `crypto.rs` no compilaría sin el bound. Borrarlo (~−6).
- **Ahorro estimado:** ~20 líneas · **Riesgo:** bajo · **Prioridad:** baja.

### Q9. Tabla para la deserialización inválida de `TableBuffer`
- **Dónde:** `squashing_rules_tests.rs:286-306` y `340-350`. Son 4 tests de 1–2 asserts sobre `decode_buffer`.
- **Propuesta:** un único `table_buffer_deserialization_rejects_inconsistent_buffers` con los 4 `RawTableBuffer` inválidos en un array y el caso válido `[1, 3]`. Conservar el comentario de la línea 343, que explica el orden de los deltas.
- **Ahorro estimado:** ~12–15 líneas · **Riesgo:** bajo · **Prioridad:** baja.

### Q10. Fixtures comunes entre archivos (opcional)
- **Dónde:** `squashing_rules_tests.rs:3-13` (`users()`) y `schema_validation_tests.rs:4-13` (`sample_schema()`) definen la misma tabla `users` (id, name, age, bio, secret_chat). También se repite el patrón `TableSchema::builder(x).table_id(0).primary_key("id", DataType::Int)` en `schema_table_ids_tests.rs:119-121`, `schema_validation_tests.rs` (varias veces) y `squashing_rules_tests.rs:108-112`.
- **Propuesta:** un `tests/common/mod.rs` con `users_table()` y `table(name)`. El ahorro neto es chico (~10 líneas), porque cada archivo suma `mod common;` y hace falta `#[allow(dead_code)]` en los helpers que algún binario no use. Recomendable solo si crecen los fixtures compartidos.
- **Ahorro estimado:** ~5–10 líneas · **Riesgo:** bajo · **Prioridad:** baja.

## Lo que conviene dejar como está
- **`every_buffer_built_by_apply_deserializes_again`** (`squashing_rules_tests.rs:223-251`): la enumeración exhaustiva de secuencias de 3 operaciones es compacta para lo que cubre.
- **`header_checksum_covers_every_field`** y **`search_finds_frames_across_window_edges`** (`wal_frame_tests.rs:183-197` y `259-269`): ya son tablas de casos.
- **Los tests de corrupción frente a torn write** de `wal_frame_tests.rs:271-356`: parecen similares, pero cada uno fija un caso distinto de la regla "es corrupción solo si sigue un frame válido" (frame anidado, header en cero, frame parcial). Son tests adversarios que pide GEMINI.md.
- **Los helpers de tamaño exacto** `value_with_frame_len` y `op_with_wire_len` (`protocol_codec_tests.rs:381-438`): son largos pero necesarios para probar los límites al byte, y su doc-comment explica la suposición del prefijo de longitud.
- **`error_codes_keep_their_wire_indexes`** (`protocol_codec_tests.rs:664-694`): la lista explícita de 20 códigos es el contrato de compatibilidad del formato. No hay que generarla.
- **`invalid_headers_are_rejected`** (`snapshot_envelope_tests.rs:21-50`): mezcla `assert_eq!` con `matches!` (el `InvalidMagic` lleva payload). Pasarlo a tabla no ahorra.
- **`test_wire_framing_header_and_magic_version_verification`** (`protocol_codec_tests.rs:296-298`): compara los bytes literales `b"ZM"` y `0x01` a propósito, para fijar el formato de cable. No conviene cambiarlos por constantes.
- **`schema_table_ids_tests.rs`**: es el archivo más limpio; el comentario de módulo y los helpers son adecuados.

## Tests
Este informe trata solo de tests. Ningún cambio propuesto reduce la cobertura: las fusiones conservan cada entrada probada, y los tests que se borran (Q3, Q8 crypto, Q6 roundtrip y CRC) tienen un equivalente exacto o los garantiza el compilador.
