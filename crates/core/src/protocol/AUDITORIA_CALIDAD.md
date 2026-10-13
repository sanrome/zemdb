# Auditoría de calidad — `crates/core/src/protocol/`

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `codec.rs`, `limits.rs`, `messages.rs`, `mod.rs`, `snapshot_envelope.rs`, `wal_frame.rs`, `tests/codec.rs`, `tests/messages.rs`

> Restricción de todo el informe: los formatos wire (`DefaultOptions` de bincode: varint, little-endian) y WAL (`bincode::serialize` legacy: enteros de ancho fijo) no cambian. Cada propuesta que toca codificación aclara por qué produce exactamente los mismos bytes.

## Resumen

- El módulo ya es compacto y está bien escrito. Las funciones son cortas, los errores usan `thiserror`, el decodificador del WAL se comparte con los lectores en streaming (`parse_batch_header` / `decode_batch_payload` / `find_valid_frame` / `FrameAnomaly`) y los comentarios explican sobre todo el *por qué*. No hay referencias a códigos de auditoría ni narrativa histórica en comentarios.
- **Ahorro estimado:** ~29–36 líneas sobre 1303 de código fuente (≈2,5 %), más ~6–10 de 207 en tests. Recortar más significaría borrar documentación que vale la pena.
- Principales oportunidades:
  1. `replay_wal_records` (`wal_frame.rs:443-474`): un `loop` con `return` elimina la variable mutable `torn_write_detected` y el brazo `CleanEof`, al que no se puede llegar.
  2. Comentarios desactualizados o engañosos en `wal_frame.rs`: el doc de `TornWrite` describe un offset que vale siempre `0`, al magic se lo llama "little-endian" y hay docs de una línea de más de 100 columnas.
  3. `SnapshotEnvelopeHeader::parse` (`snapshot_envelope.rs:103-126`): se puede escribir con `first_chunk` + desestructuración, igual que `parse_batch_header`.
  4. API sin uso o que confunde: `valid_bytes_offset` siempre vale `0`, el getter `body_crc()` no se usa en ningún lado, y el nombre del alias `with_default_origin` no corresponde a nada del struct.

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| `codec.rs` | 168 | 108 | 41 | 3 | 16 | ~2–5 |
| `limits.rs` | 96 | 47 | 39 | 0 | 10 | 0 |
| `messages.rs` | 332 | 239 | 81 | 0 | 12 | ~4 |
| `mod.rs` | 11 | 10 | 0 | 0 | 1 | 0 |
| `snapshot_envelope.rs` | 222 | 146 | 47 | 0 | 29 | ~8–9 |
| `wal_frame.rs` | 474 | 329 | 92 | 2 | 51 | ~15–18 |
| `tests/codec.rs` | 106 | 92 | 1 | 7 | 6 | ~6–10 |
| `tests/messages.rs` | 101 | 94 | 2 | 2 | 3 | 0 |
| **Total** | **1510** | **1065** | **303** | **14** | **128** | **~35–46** |

## Comentarios

**Densidad y calidad.** El 23 % de las líneas del código fuente (300 de 1303) son doc comments (`///`/`//!`), una proporción razonable para el contrato wire/WAL de un crate público. La mayoría documenta invariantes o decisiones: el formato en disco, el orden de los chequeos y las garantías ante crashes. No hay códigos de auditoría (C-xx, DEF-xx…) ni frases tipo "antes hacíamos".

**Conservar (valiosos):**
- `limits.rs:1-9` (doc del módulo): el invariante "toda operación aceptada entra sola en un registro WAL y en un frame de respuesta" explica por qué existe todo el archivo.
- `limits.rs:43-55` (`MAX_MESSAGE_VALUES`): explica el DoS (24 B en memoria contra 1 B en el wire) y la regla de que un `Value` nuevo fuera de esas colecciones debe cobrarse a mano.
- `codec.rs:89-90` (el header se chequea antes que el tamaño), `codec.rs:114` (presupuesto sincrónico), `codec.rs:130-131` (cómo se codifica la etiqueta del enum).
- `messages.rs:12` ("Encoded by variant index, so new codes are appended at the end"): es un invariante de compatibilidad wire.
- `wal_frame.rs:240-245`, `266-272`, `300-320`: las reglas para distinguir un torn write de corrupción y el límite de chequeos que evita la búsqueda cuadrática. Son el núcleo de la seguridad de la recuperación.
- `snapshot_envelope.rs:1-16`: la tabla del layout del envelope.

**Recortar o corregir:**
- `wal_frame.rs:55-56` (**desactualizado**): "Incomplete batch found at EOF… Indicates the byte count consumed so far before encountering the incomplete batch." `decode_wal_batch_from_slice` decodifica un solo frame y siempre pone `valid_bytes_offset: 0` (`wal_frame.rs:403`). Además, `TornWrite` ya no cubre solo un batch incompleto al final: también cubre un header inválido o un CRC que no coincide sin un frame válido después. Ver Q4.
- `wal_frame.rs:66` (**engañoso**): "`0xBA7C`, little-endian: `[0xBA, 0x7C]`". Leídos como `u16` little-endian, esos bytes dan `0x7CBA`. Son una secuencia de bytes, no un entero. Propuesta: "`magic`: 2 bytes, `[0xBA, 0x7C]`".
- `wal_frame.rs:12-13` repite el layout que ya documenta `encode_wal_batch` (`wal_frame.rs:65-71`). Basta con "Fixed WAL batch header size (18 bytes; layout in [`encode_wal_batch`])."
- Docs de una línea de más de 100 columnas que además repiten los campos: `wal_frame.rs:29` (119), `:47` (123), `:63` (135), `:114` (114); `messages.rs:85` (110), `:121` (103), `:221` (125), `:295` (128); `codec.rs:87` (109). Ejemplos de reescritura:
  - `wal_frame.rs:47` → `/// A decoded batch and the length of its frame.`
  - `wal_frame.rs:114` → `/// [`encode_wal_batch`] for a single operation.`
  - `messages.rs:295` → `/// Registration accepted: the room schema, head and retention tail, and the active snapshot.`
- `wal_frame.rs:443` "Helper to read all valid WAL records…": "Helper to" no aporta. El párrafo `443-447` cabe en 2–3 líneas (ver Q1).
- `messages.rs:76` "Ergonomic alias for tests.": el comentario es correcto, pero el nombre del método no lo es (Q8).
- `snapshot_envelope.rs:128` "Serializes the header." y los docs de los getters (`139-152`, `wal_frame.rs:131-144`) repiten la firma. Son aceptables en API pública (cuestan 1 línea cada uno); si se borra el getter sin uso (Q7), se va su doc con él.
- `messages.rs:59-61` "Bounded strictly to 96 bytes": el dato es correcto (lo verifica `crates/core/tests/types_memory_tests.rs:11`), pero "Bounded strictly" sugiere un límite del wire. Más preciso: "Occupies 96 bytes in memory (8B SequenceNumber + 88B Operation)."
- Opcional (+1 línea): el invariante "la codificación es por índice de variante, así que no se reordena y lo nuevo va al final" está documentado solo en `ErrorCode` (`messages.rs:12`), pero vale igual para `ClientMessage` y `ServerMessage`. `peek_client_message_kind` y `ClientMessageKind` dependen de él.
- **Estado:** ✅ aplicada (paso 1), incluido el invariante opcional en `ClientMessage` y `ServerMessage`. El doc de `TornWrite::valid_bytes_offset` dice que es relativo al slice y por eso siempre `0` (quitar el campo es Q4). `messages.rs:76` queda para Q8 y los docs de getters/`to_bytes` se dejan, como propone el informe.

## Sugerencias

### Q1. `replay_wal_records`: `loop` con `return` en vez de `while` + variable mutable + `break`
- **Dónde:** `wal_frame.rs:443-474`
- **Problema:** `while offset < wal_bytes.len()` garantiza un slice no vacío, así que el brazo `CleanEof => break` nunca se ejecuta y solo está para cubrir la exhaustividad. La variable `torn_write_detected` existe solo para sacar el valor del `match`.
- **Propuesta:**
  ```rust
  /// Decodes every frame of `wal_bytes` in order. Stops at a torn write and returns its
  /// description with the operations and the length of the valid prefix, so the caller can
  /// truncate the damaged tail.
  pub fn replay_wal_records(
      wal_bytes: &[u8],
  ) -> Result<(Vec<SequencedOperation>, usize, Option<String>), WalFrameError> {
      let mut all_ops = Vec::new();
      let mut offset = 0;
      loop {
          match decode_wal_batch_from_slice(&wal_bytes[offset..])? {
              WalBatchDecodeResult::Ok {
                  ops,
                  bytes_consumed,
                  ..
              } => {
                  all_ops.extend(ops);
                  offset += bytes_consumed;
              }
              WalBatchDecodeResult::CleanEof => return Ok((all_ops, offset, None)),
              WalBatchDecodeResult::TornWrite { reason, .. } => {
                  return Ok((all_ops, offset, Some(reason)))
              }
          }
      }
  }
  ```
  El comportamiento es idéntico. Con `offset == len`, el slice vacío da `CleanEof` (`wal_frame.rs:391-393`) y la función devuelve `(ops, len, None)`, igual que hoy. `bytes_consumed` siempre es mayor que 0 (vale `frame_len`, que es al menos 18), así que el bucle termina. Los errores siguen saliendo por `?`.
- **Ahorro estimado:** ~5 líneas (3 de código, 2 de doc) · **Riesgo:** bajo · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — `decode_wal_batch_from_slice` es pura y con slice vacío devuelve `CleanEof` (`391-393`), así que la llamada extra al llegar a `offset == len` da el mismo `(ops, len, None)`; `bytes_consumed = frame_len ≥ BATCH_HEADER_SIZE (18)` y `frame_len ≤ slice.len()` (si no, `TruncatedPayload`), por lo que no hay bucle infinito ni slicing fuera de rango. Los brazos `return` tipan `!` y compilan. Consumidores (`storage/src/disk/format.rs:177`, tests) no ven diferencia.
- **Estado:** ✅ aplicada (paso 1)

### Q2. Comentarios desactualizados o engañosos en `wal_frame.rs`
- **Dónde:** `wal_frame.rs:12-13`, `29`, `47`, `53-56`, `63-66`, `114`, `382-389`
- **Problema:** ver la sección Comentarios. Lo que más importa es `TornWrite` (`55-56`), porque documenta un campo que nunca lleva información, y el "little-endian" del magic (`66`).
- **Propuesta:** corregir `TornWrite` así: "The next frame cannot be read and no valid frame follows it (see [`FrameAnomaly`]): a torn write." Corregir el magic. Recortar el doc de `BATCH_HEADER_SIZE` a una línea con referencia a `encode_wal_batch`. Acortar los docs de `Ok`, `WalBatchPayload`, `encode_wal_batch` (primera línea) y `encode_wal_record`. En `decode_wal_batch_from_slice` (`382-389`), compactar la enumeración de retornos a 5–6 líneas sin perder ningún caso.
- **Ahorro estimado:** ~4–6 líneas · **Riesgo:** bajo (solo comentarios) · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — solo doc. Líneas correctas; el magic es `[0xBA, 0x7C]` (`wal_frame.rs:10`) escrito tal cual con `extend_from_slice` (`:91`), así que "little-endian" en `:66` es engañoso (como `u16` LE serían `[0x7C, 0xBA]`); `TornWrite` `:55-56` describe un offset que siempre es `0` (`:403`).
- **Estado:** ✅ aplicada (paso 1)

### Q3. `SnapshotEnvelopeHeader::parse` con `first_chunk` + desestructuración
- **Dónde:** `snapshot_envelope.rs:103-126`
- **Problema:** índices sueltos (`header[4]`, `header[6..9]`, `header[10..13]`) y un `get(..).and_then(try_into)` que `first_chunk` ya resuelve. `parse_batch_header` (`wal_frame.rs:179`) usa el estilo desestructurado, y además `first_chunk` ya se usa en `wal_frame.rs:408`.
- **Propuesta:**
  ```rust
  let [m0, m1, m2, m3, version, flag, l0, l1, l2, l3, c0, c1, c2, c3] = *bytes
      .first_chunk::<SNAPSHOT_HEADER_LEN>()
      .ok_or(SnapshotEnvelopeError::TooShort)?;
  let magic = [m0, m1, m2, m3];
  if magic != SNAPSHOT_MAGIC {
      return Err(SnapshotEnvelopeError::InvalidMagic(magic));
  }
  if version != SNAPSHOT_VERSION {
      return Err(SnapshotEnvelopeError::UnsupportedVersion(version));
  }
  let compression = SnapshotCompression::from_flag(flag)
      .ok_or(SnapshotEnvelopeError::UnknownCompression(flag))?;
  Ok(Self {
      compression,
      uncompressed_len: u32::from_be_bytes([l0, l1, l2, l3]),
      body_crc: u32::from_be_bytes([c0, c1, c2, c3]),
  })
  ```
  Se mantienen el mismo orden de chequeos y los mismos errores, y se siguen leyendo solo los primeros 14 bytes.
- **Ahorro estimado:** ~4–5 líneas · **Riesgo:** bajo · **Prioridad:** media
- **Estado:** ✅ aplicada (paso 1)

### Q4. `WalBatchDecodeResult::TornWrite::valid_bytes_offset` siempre vale `0`
- **Dónde:** `wal_frame.rs:55-59`, `403`
- **Problema:** el único productor lo fija en `0`. Ningún consumidor lo lee: `storage/src/disk/wal.rs:106` y todos los `matches!` usan `{ reason, .. }` o `{ .. }`. Solo `crates/core/tests/wal_frame_tests.rs:111` lo construye para compararlo. El offset real lo calcula quien llama (`replay_wal_records` con su `offset`).
- **Propuesta:** quitar el campo (`TornWrite { reason: String }`), la línea `valid_bytes_offset: 0,` y la línea de `wal_frame_tests.rs:111`. Si se prefiere no tocar la API, corregir al menos el doc (Q2) para que diga que el offset es relativo al slice y por lo tanto siempre `0`.
- **Ahorro estimado:** ~3 líneas · **Riesgo:** bajo-medio (enum público re-exportado en `lib.rs:31`; **a verificar** que no haya consumidores fuera del workspace) · **Prioridad:** media
- **Verificación (2.ª pasada):** ✅ confirmada — grep en todo `crates/`: el único lector potencial es `storage/src/disk/wal.rs:106` con `{ reason, .. }` (sigue compilando sin el campo), `storage/tests/disk_wal_resilience_tests.rs:367` y `core/tests` usan `{ .. }`; solo `wal_frame_tests.rs:111` lo construye. El enum no se serializa (no afecta bytes en disco/red); solo cambia su salida `Debug`, que ningún test compara.

### Q5. `encode_wal_batch`: sacar los temporales de un solo uso
- **Dónde:** `wal_frame.rs:86-94`
- **Problema:** `batch_len`, `crc` y `ops_count` se usan una sola vez, justo debajo.
- **Propuesta:**
  ```rust
  let mut record = Vec::with_capacity(BATCH_HEADER_SIZE + payload.len());
  record.extend_from_slice(&BATCH_MAGIC);
  record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
  record.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
  record.extend_from_slice(&(ops.len() as u32).to_le_bytes());
  let header_crc = crc32fast::hash(&record);
  ```
  Los bytes son idénticos: mismas expresiones, en el mismo orden y con el mismo truncamiento `as u32`. Bajar `let payload_struct` a un argumento en línea (`&WalBatchPayloadRef { mutation_id, ops }`) ahorra 1 línea más.
- **Ahorro estimado:** ~3–4 líneas · **Riesgo:** bajo · **Prioridad:** baja
- **Estado:** ✅ aplicada (paso 1), incluido el `WalBatchPayloadRef` en línea.

### Q6. `codec.rs`: un helper para las opciones wire
- **Dónde:** `codec.rs:67-70`, `93-97`, `132-136`
- **Problema:** `bincode::DefaultOptions::new().with_limit(MAX_MESSAGE_SIZE)` aparece tres veces. Además, `.reject_trailing_bytes()` en `encode_message` (`:69`) no tiene efecto al serializar (solo se aplica al deserializar), y la regla "el wire es `DefaultOptions` con este límite" no queda escrita en un único lugar.
- **Propuesta:**
  ```rust
  /// Bincode options of the wire format: varint, little-endian, payload up to `MAX_MESSAGE_SIZE`.
  fn wire_options() -> impl Options {
      bincode::DefaultOptions::new().with_limit(MAX_MESSAGE_SIZE)
  }
  ```
  y usar `wire_options().serialize_into(..)`, `wire_options().reject_trailing_bytes().deserialize(..)` y `wire_options().allow_trailing_bytes().deserialize(..)`. Los bytes serializados no cambian, porque ni el límite ni la opción de trailing alteran la codificación. `encoded_len` (`:54-56`) se queda **sin** límite a propósito: `check_operation_size` necesita el tamaño real aunque supere el máximo para reportarlo. Conviene dejarlo dicho en un comentario de una línea.
- **Ahorro estimado:** ~0–2 líneas (es una mejora de claridad, no de longitud) · **Riesgo:** bajo · **Prioridad:** baja

### Q7. Getter `SnapshotEnvelopeHeader::body_crc()` sin uso
- **Dónde:** `snapshot_envelope.rs:149-152`
- **Problema:** no hay ningún uso en el workspace (`grep -rn '\.body_crc()' crates` → 0). `compression()` y `uncompressed_len()` sí se usan (`storage/src/snapshot.rs:87-129`).
- **Propuesta:** eliminarlo. El campo se sigue usando internamente en `verify_digest` y `to_bytes`.
- **Ahorro estimado:** 4 líneas · **Riesgo:** bajo (API pública del crate; **a verificar** si se considera parte del contrato) · **Prioridad:** baja

### Q8. Alias `SequencedOperation::with_default_origin`
- **Dónde:** `messages.rs:76-79`
- **Problema:** es idéntico a `new`. `SequencedOperation` no tiene ningún campo de "origin", así que el nombre no describe nada. Solo lo usan tests (~80 llamadas en otros crates).
- **Propuesta:** eliminarlo y reemplazar las llamadas por `SequencedOperation::new` (cambio mecánico con `sed`, mismo comportamiento).
- **Ahorro estimado:** 4 líneas en el módulo (los tests quedan con la misma cantidad de líneas) · **Riesgo:** bajo, pero toca archivos fuera de este alcance · **Prioridad:** baja

### Q9. `frame_payload` con patrón de slice
- **Dónde:** `codec.rs:145-164`
- **Problema:** `peek_version` (`:81-84`) ya lee el header con un patrón de slice, mientras que `frame_payload` usa índices (`bytes[0..2]`, `bytes[2]`).
- **Propuesta:**
  ```rust
  let [m0, m1, version, _flags, payload @ ..] = bytes else {
      return Err(DecodeError::TooShort { len: bytes.len() });
  };
  if [*m0, *m1] != PROTOCOL_MAGIC {
      return Err(DecodeError::InvalidMagic { got: [*m0, *m1] });
  }
  if *version != PROTOCOL_VERSION { /* igual que hoy */ }
  if bytes.len() > MAX_FRAME_SIZE { /* igual que hoy */ }
  Ok(payload)
  ```
  Se mantienen el mismo orden (TooShort → magic → versión → tamaño) y los mismos errores. La contra es que el patrón fija 4 elementos en lugar de usar `PROTOCOL_HEADER_LEN`. Si se adopta, conviene un `const _: () = assert!(PROTOCOL_HEADER_LEN == 4);` o un comentario.
- **Ahorro estimado:** ~2 líneas · **Riesgo:** bajo · **Prioridad:** baja
- **Estado:** ✅ aplicada (paso 1), con `const _: () = assert!(PROTOCOL_HEADER_LEN == 4);`.

### Q10. `WalBatchPayload` no necesita ser `pub`
- **Dónde:** `wal_frame.rs:29-34`
- **Problema:** solo se usa en `decode_batch_payload` (`:225`). No está en el `pub use` de `lib.rs` ni tiene usos externos, pero el `pub use wal_frame::*` de `mod.rs:11` lo expone igual. Sus derives `Debug, Clone, PartialEq, Serialize` no se usan.
- **Propuesta:** hacerlo privado con `#[derive(serde::Deserialize)]` solamente, dejando `WalBatchPayloadRef` al lado. Los campos van en el mismo orden (`mutation_id`, `ops`), así que la codificación no cambia. Hace falta un comentario de una línea que diga que el orden de campos de ambos structs es el formato en disco, porque hoy esa restricción solo se deduce de "with the same encoding".
- **Ahorro estimado:** ~0 líneas (gana encapsulamiento) · **Riesgo:** bajo (**a verificar** consumidores fuera del workspace) · **Prioridad:** baja

## Lo que conviene dejar como está

- **WAL con `bincode::serialize`/`deserialize` y wire con `DefaultOptions`** (`wal_frame.rs:77`, `111`, `225` frente a `codec.rs`). Parece una inconsistencia que convendría unificar, pero son formatos distintos (ancho fijo frente a varint). Unificarlos cambia los bytes en disco o en la red. `limits.rs:3-5` lo documenta.
- **La triple enumeración `ClientMessage` / `ClientMessage::kind()` / `ClientMessageKind::ALL`** (`messages.rs:163-216`). Sin `strum` ni macros no se reduce, y un `macro_rules!` empeoraría la lectura. Los tests `every_kind_is_listed_in_order_and_read_from_its_frame` y `client_messages_have_as_many_variants_as_listed_kinds` protegen la consistencia.
- **`find_valid_frame` y `payload_matches`** (`wal_frame.rs:297-380`): el código y los comentarios largos están justificados (ventanas con solapamiento de `BATCH_HEADER_SIZE - 1`, el límite `MAX_PAYLOAD_CHECKS`, la memoria acotada). Cada párrafo documenta una decisión de seguridad de recuperación.
- **El closure `anomaly` en `decode_wal_batch_from_slice`** (`wal_frame.rs:395-406`): evita repetir la búsqueda y la clasificación en los cuatro puntos de salida. El `.expect("reads stay within the slice")` está justificado porque un `Cursor` sobre un slice no falla dentro de rango.
- **`check_operation_size`** (`limits.rs:71-96`): los dos `map_err(|e| OperationSizeError::Encoding(e.to_string()))` vienen de tipos de error distintos (`WalFrameError` y `bincode::Error`). Un helper no ahorra nada.
- **`RESPONSE_ENVELOPE_ALLOWANCE`** (`limits.rs:27-37`): la suma explícita término a término, con su doc, es la forma más verificable de ese cálculo.
- **`ServerMessage::compute_snapshot_hash`** (`messages.rs:323-328`): estaría mejor como función libre en `snapshot_envelope.rs`, pero tiene 17 usos externos y moverla es un cambio de API sin ahorro de líneas.
- **El nombre de variante `WalBatchDecodeResult::Ok`**: se confunde con `Result::Ok` en los patrones, pero renombrarlo toca 27 usos externos. No entra en el alcance.
- **`DecodeError::UnsupportedVersion { expected, .. }`**: `expected` siempre es `PROTOCOL_VERSION` y podría ir como argumento de formato, como en `TooLarge`, pero `server/src/tests/error.rs:61` y `core/tests/protocol_codec_tests.rs:305` construyen o comparan el campo.
- **`SnapshotEnvelopeValidator::new()`** junto a `Default`: es idiomático y se usa en `server/src/relay/files.rs:199`.

## Tests

**`tests/messages.rs` (101 líneas):** está bien. `sample` usa un `match` sin comodín, así que agregar un `ClientMessageKind` no compila hasta que tenga su muestra. Los dos tests cubren orden, `kind()`, `peek` y el conteo de variantes. Hay una única fragilidad aceptable: `:97` compara el texto del error de bincode (`"variant index 0 <= i < {count}"`), que depende de la versión de bincode.

**`tests/codec.rs` (106 líneas):**
- **Expresión duplicada:** `(MAX_MESSAGE_VALUES as usize).div_ceil(crate::schema::MAX_COLUMNS) + 1` aparece en `:60` y `:78`, y `crate::schema::MAX_COLUMNS` cuatro veces (`:60`, `:61`, `:78`, `:83`). Con `use crate::schema::MAX_COLUMNS;` y un helper `fn rows_over_budget() -> usize` (1 línea de doc) se ahorran ~2–3 líneas y queda más legible.
- **Mismo patrón de frontera en dos tests:** `a_message_over_the_value_budget_is_rejected` (`:21-32`) y `key_values_and_update_deltas_count_against_the_budget` (`:34-54`) hacen lo mismo: con presupuesto N decodifica, con N-1 devuelve `TooManyValues { max: N-1 }`. Un helper
  ```rust
  fn assert_budget_is_exact<T: DeserializeOwned + PartialEq + Debug>(value: &T, values: u64)
  ```
  que codifique, decodifique con `values`, compare y verifique el error con `values - 1` deja cada test en 3–5 líneas. **Ahorro ~6 líneas.**
- **Redundancia parcial (a verificar):** `server_responses_are_not_budgeted` (`:76-93`) prueba que `decode_message` no aplica presupuesto, algo que ya cubren `:69-73` y `:100-103`. Su único aporte es el nombre (documenta la intención para `ServerMessage`), y materializa más de 1 Mi `Value`s, así que es de los tests más costosos del módulo. Se puede dejar por documentación o borrar (~18 líneas). Me inclino por dejarlo, pero señalo el costo.
- **`a_failed_decode_does_not_leave_its_budget_on_the_thread` (`:95-106`):** conservar. Verifica la liberación RAII del presupuesto (`Drop` de `ValueBudget`), que es un principio de GEMINI.md.
