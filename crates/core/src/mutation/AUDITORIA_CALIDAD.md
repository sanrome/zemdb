# Auditoría de calidad — `crates/core/src/mutation/`

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `mod.rs`, `squash.rs`, `op.rs`, `buffer.rs`

## Resumen
- Módulo compacto y bien estructurado. `squash.rs` ya está casi en su mínimo y `merge_sorted_column_updates` es un buen ejemplo de uso de `Peekable::next_if`. No hay códigos de auditoría ni narrativa histórica.
- La principal fuente de líneas sobrantes está en `op.rs`: `insert`/`update`/`delete` repiten el literal de `Operation` en lugar de delegar en `Operation::new`.
- En `buffer.rs`, el `impl Serialize` manual (con un struct auxiliar interno) se puede sustituir por `#[derive(Serialize)]` + `#[serde(serialize_with)]` con salida idéntica.
- Hay dos comentarios **inexactos** que conviene corregir: `op.rs:60` ("100% stack-allocated, zero heap pointers" es falso) y `op.rs:47-48` (el "strictly sorted" no lo garantizan los constructores).
- **Ahorro estimado:** ~40–50 líneas, un **9–11 %** de las 459 del módulo.

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| `op.rs` | 206 | 152 | 30 | 0 | 24 | ~22–26 |
| `buffer.rs` | 177 | 138 | 25 | 0 | 14 | ~18–24 |
| `squash.rs` | 69 | 51 | 11 | 4 | 3 | 0 |
| `mod.rs` | 7 | 6 | 0 | 0 | 1 | 0 |
| **Total** | **459** | **347** | **66** | **4** | **42** | **~40–50** |

## Comentarios

**Densidad:** media-alta (66 líneas doc/459) y de buena calidad en general. Abundan los comentarios de *por qué* y los invariantes.

**Conservar (valiosos):**
- `squash.rs:41` ("An update cannot resurrect a row with a pending delete") y `squash.rs:59-61` (por qué un delete tras un insert pendiente se envía igual). Son la semántica no obvia del squashing.
- `squash.rs:32-35`: el contrato de equivalencia ("sending the result is equivalent to sending both … the incoming operation always wins"), incluida la salvedad del tamaño.
- `op.rs:62-69`: `Operation` es un tipo de wire no validado, y solo `Schema::validate_operation` lo valida. Es un invariante de dominio importante.
- `buffer.rs:120-125`: "either leaves the buffer unchanged" y "a buffer built by `apply` always deserializes again". Documentan el principio de validar antes de mutar.
- `buffer.rs:30-35`: qué rechaza la deserialización y qué no (no reaplica el squashing).

**Corregir (inexactos):**
- `op.rs:60`: "100% stack-allocated, zero heap pointers, aligned to 8 bytes." es **falso**. `OperationKind::Insert` contiene un `CompactRow` (un `Vec`), `Update` un `Vec<ColumnUpdate>`, `Value::String`/`Bytes` son `Box`, y `PrimaryKey` puede desbordar al heap. Hay que borrar la frase.
- `op.rs:59`: el desglose "2B table_id + 6B padding + 8B timestamp + …" supone un layout que Rust no garantiza (reordena campos). Lo verificable es el total, 88 bytes, que asserta `crates/core/tests/types_memory_tests.rs:10`. Basta con "88 bytes (asserted in `types_memory_tests`)". Lo mismo vale para `op.rs:7` y `op.rs:41`.
- `op.rs:47-48`: "Updates are maintained strictly sorted by `column_idx` ascending". `Operation::new`/`update` ordenan con `sort_by_key`, pero no eliminan duplicados, así que el orden **estricto** solo lo garantiza `UpdateBuilder::set` o la validación (`schema/validation.rs:335`). Propuesta: "Constructors sort deltas by `column_idx`; [`Schema::validate_operation`] rejects duplicates."

**Recortar:**
- `buffer.rs:21-35` (15 líneas): los párrafos 2 y 3 repiten que la clave de cada entrada es la PK de su operación (líneas 27-28 y 30-31). Fusionados quedan en ~11 líneas.
- `buffer.rs:126-128`: el párrafo sobre cuándo se clona la PK es un detalle de implementación. Alcanza con un `//` de una línea junto al brazo `None =>` (`buffer.rs:151`), o con eliminarlo.
- `op.rs:46-48`: el doc de `Update` mezcla la descripción de la variante con dos reglas. Con la corrección anterior queda en 2 líneas.
- `squash.rs:16`: `$O(M+N)$` es notación LaTeX que rustdoc no renderiza; queda mejor `O(M+N)`. Lo mismo pasa en `schema/validation.rs:434`, fuera de alcance.

## Sugerencias

### Q1. Los constructores de `Operation` delegan en `new`
- **Dónde:** `op.rs:79-128`.
- **Problema:** `insert`, `update` y `delete` repiten el literal completo `Self { table_id, timestamp, pk, kind }` (6 líneas cada uno), y `update` repite el `sort_by_key` que ya hace `new`.
- **Propuesta:**
  ```rust
  pub fn new(table_id: u16, pk: PrimaryKey, timestamp: u64, mut kind: OperationKind) -> Self {
      if let OperationKind::Update { updates } = &mut kind {
          updates.sort_by_key(|u| u.column_idx);
      }
      Self { table_id, timestamp, pk, kind }
  }

  pub fn insert(table_id: u16, pk: PrimaryKey, row: CompactRow, timestamp: u64) -> Self {
      Self::new(table_id, pk, timestamp, OperationKind::Insert { row })
  }
  // update(...) y delete(...) igual; update pierde el `mut updates` y el sort propio.
  ```
  El comportamiento es idéntico: `new` solo ordena `Update` (el mismo `sort_by_key` estable), y para `Insert`/`Delete` el resultado es el mismo literal. Hay 159 llamadas a estos constructores en el workspace y ninguna firma cambia.
- **Ahorro estimado:** ~20 líneas · **Riesgo:** bajo · **Prioridad:** alta

### Q2. `Serialize` de `TableBuffer` derivado con `serialize_with`
- **Dónde:** `buffer.rs:42-63` (y el import `SerializeStruct` en `buffer.rs:6`).
- **Problema:** son 22 líneas, con un struct auxiliar `Pending` y un `impl Serialize` anidado, solo para serializar `pending` como secuencia de valores.
- **Propuesta:**
  ```rust
  #[derive(Debug, Clone, PartialEq, Default, Serialize)]
  pub struct TableBuffer {
      table_id: u16,
      /// Serialized as the sequence of operations: each one carries its own key.
      #[serde(serialize_with = "serialize_pending")]
      pending: HashMap<PrimaryKey, Operation>,
  }

  fn serialize_pending<S: Serializer>(
      pending: &HashMap<PrimaryKey, Operation>,
      serializer: S,
  ) -> Result<S::Ok, S::Error> {
      serializer.collect_seq(pending.values())
  }
  ```
  La salida es idéntica: el derive emite `serialize_struct("TableBuffer", 2)` con `table_id` y `pending` en ese orden, y `pending` pasa por `collect_seq(values())`, igual que hoy. El `Deserialize` manual no cambia. Lo cubren los round-trips de `crates/core/tests/squashing_rules_tests.rs:282,333`.
- **Ahorro estimado:** ~12–14 líneas · **Riesgo:** bajo · **Prioridad:** alta

### Q3. Simplificar la validación del `Deserialize` de `TableBuffer`
- **Dónde:** `buffer.rs:85-98`.
- **Problema:** `contains_key` + `insert` hacen dos búsquedas en el hash y ocupan 4 líneas, y la comprobación de orden con `windows(2)` es un `if let` + `if` anidado.
- **Propuesta:**
  ```rust
  if pending.insert(op.pk.clone(), op).is_some() {
      return Err(de::Error::custom("pending operations repeat a primary key"));
  }
  ```
  Ante un error se descarta el mapa entero, así que insertar antes de detectar el duplicado no es observable. El mensaje no cambia. Para el orden, `if let OperationKind::Update { updates } = &op.kind` + `if` se puede colapsar en `matches!`/`let-else`, o usar `!updates.is_sorted_by(|a, b| a.column_idx < b.column_idx)` (**a verificar:** requiere Rust ≥ 1.82, y el repo no fija `rust-version` ni toolchain).
- **Ahorro estimado:** ~2–4 líneas · **Riesgo:** bajo · **Prioridad:** baja
- **Nota:** el `insert(..).is_some()` tiene que ir en el mismo lugar que el `contains_key` actual, es decir, después del chequeo de orden de deltas. Así una operación con los dos problemas (deltas desordenados y PK repetida) sigue informando el error de deltas, como hoy.

### Q4. Corregir y condensar la doc de tamaños y orden
- **Dónde:** `op.rs:5-7`, `op.rs:39-41`, `op.rs:57-61`, `op.rs:47-48`.
- **Propuesta:** ver la sección Comentarios: borrar la frase falsa de `op.rs:60`, reemplazar los desgloses de padding por una referencia al test de tamaños y corregir el "strictly sorted".
- **Ahorro estimado:** ~3–4 líneas · **Riesgo:** nulo · **Prioridad:** **alta** (no por las líneas, sino porque hoy la doc afirma algo falso)

### Q5. Condensar la doc de `TableBuffer` y de `apply`
- **Dónde:** `buffer.rs:21-35` y `buffer.rs:126-128`.
- **Propuesta:** fusionar los párrafos que repiten la relación clave↔PK y reducir la nota sobre el clon de la PK a un `//` junto al brazo `None` (o eliminarla).
- **Ahorro estimado:** ~5–6 líneas · **Riesgo:** nulo · **Prioridad:** media

### Q6. Menores
- `squash.rs:32-35`: `` `check_operation_size` `` puede ser un enlace intra-doc (`[`check_operation_size`](crate::protocol::limits::check_operation_size)`); así rustdoc lo valida si se renombra.
- `op.rs:172-206` (`UpdateBuilder::set`): `retain` + `push` es O(n²) en el número de columnas. Es correcto y n es chico; no hace falta tocarlo, solo lo dejo anotado.
- `buffer.rs:134-141`: el `TableIdMismatch` se construye a mano, igual que en `schema/validation.rs:437-441` (con `expected`/`actual` en roles distintos). Un helper ahorraría 2–3 líneas, pero no lo recomiendo: los dos sitios tienen semánticas diferentes.

## Lo que conviene dejar como está
- **`squash.rs` completo:** `merge_sorted_column_updates` (12 líneas, O(M+N) sin índices) y `squash_operations` ya son mínimos. El `return` dentro del brazo `Delete/Update` (`squash.rs:42-44`) importa: evita actualizar el `timestamp` en el caso incompatible, de modo que la operación pendiente queda intacta, tal como promete `buffer.rs:122-124`. El `resize` de `squash.rs:48-50` también se queda: `squash_operations` es `pub` y no puede asumir operaciones validadas.
- **El `Deserialize` manual de `TableBuffer` (`buffer.rs:65-105`):** valida invariantes (tabla, PK única, deltas ascendentes) que un derive no puede expresar. Un `#[serde(try_from)]` no ahorraría líneas en la práctica.
- **El chequeo de `table_id` en `apply` (`buffer.rs:134-141`) antes de `validate_operation`:** es necesario. `validate_operation` compara la operación con el esquema, no el esquema con el buffer, y el orden "validar todo y después mutar" respeta el principio de GEMINI.md.
- **Los accessors `#[inline]` de `Operation`/`ColumnUpdate`:** son funciones `pub` no genéricas de un crate de biblioteca, y el `#[inline]` permite inlining entre crates sin LTO.
- **`BufferError`:** ya usa `thiserror`. El mensaje de `IncompatibleOperation` no se compara textualmente en los tests (`squashing_rules_tests.rs:105` compara la variante), pero no hay motivo para cambiarlo.

## Tests
Los tests de este módulo no se asignaron a esta auditoría: no hay carpeta `mutation/tests/` y la cobertura está en `crates/core/tests/squashing_rules_tests.rs` (34 funciones). Para Q2/Q3 ya existen las redes de seguridad necesarias: un round-trip `serialize`→`deserialize` de un `TableBuffer` real (`squashing_rules_tests.rs:248`) y decodificaciones de buffers crudos inválidos (`squashing_rules_tests.rs:282-283, 330-343`).
