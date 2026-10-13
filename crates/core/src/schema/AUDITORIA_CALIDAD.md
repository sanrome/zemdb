# Auditoría de calidad — `crates/core/src/schema/`

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `column.rs`, `validation.rs`, `mod.rs`, `table.rs`, `global.rs` (contexto de uso: `crates/core/tests/schema_validation_tests.rs`, `schema_table_ids_tests.rs`)

## Resumen

- En general el módulo está bien: tiene invariantes claros (`TableSchema::try_new` es el único camino de construcción y también lo usa `Deserialize`), errores tipados con `thiserror`, y valida antes de mutar (`insert_table`, `add_column`). Hay pocos comentarios y casi todos son buenos. No hay códigos de auditoría ni narrativa histórica.
- El mayor ahorro está en la **doble indirección de validación**. `Schema::validate_*` llama a `validation::validate_*`, que llama a `TableSchema::validate_*`, que vuelve a llamar a `validation::*`. Así cada operación tiene dos o tres firmas, y en total son 10 funciones libres `pub` que ningún caller del workspace usa (Q2).
- La búsqueda `get_table_by_name(..).ok_or_else(TableNotFound)` se repite 6 veces (Q1). Hay código **inalcanzable**: el `seen` de `validate_column_updates` (Q3) y varias ramas que el invariante de `try_new` ya descarta (Q9).
- Hay duplicación mecánica en `TableBuilder`, donde cuatro métodos repiten el mismo "buscar o agregar" (Q4). También en el chequeo de aridad de `CompactRow` (3 copias, Q5) y en el chequeo de `DataType::Null` (2 copias, Q7). Además hay bucles que se leen mejor como iteradores (Q6).
- **Ahorro estimado: ~140–180 líneas de 1325 (≈11–14 %)**, casi todo en `table.rs` y `validation.rs`. Q2 sola aporta unas 60–75 y es la única con riesgo medio, porque saca funciones `pub` reexportadas por `pub use validation::*`.

## Métricas

| Archivo | Total | Código | Doc (`///`/`//!`) | Coment. (`//`) | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| `column.rs` | 57 | 43 | 6 | 0 | 8 | ~0 |
| `global.rs` | 267 | 204 | 31 | 0 | 32 | ~20–30 |
| `mod.rs` | 9 | 8 | 0 | 0 | 1 | 0 |
| `table.rs` | 539 | 432 | 49 | 2 | 56 | ~70–85 |
| `validation.rs` | 453 | 372 | 12 | 6 | 63 | ~50–70 |
| **Total** | **1325** | **1059** | **98** | **8** | **160** | **~140–180** |

## Comentarios

**Densidad:** baja. Hay 98 líneas de doc y 8 de `//`, o sea un ~8 % del módulo. Ese nivel es adecuado para un módulo de dominio, así que el recorte posible es chico (~12–18 líneas). Lo que sobra es redundancia, no exceso.

**Conservar** (explican el *por qué* o un invariante):
- `column.rs:4-9`: por qué los campos se leen por accessors y quién valida la columna.
- `table.rs:9-10`: por qué `MAX_COLUMNS = u16::MAX`, que es el índice posicional `u16`.
- `table.rs:88-90`: semántica de las columnas cifradas (el servidor solo ve `Value::Bytes`).
- `table.rs:140-141`: por qué `table_id` no tiene default al deserializar.
- `global.rs:11-16` y `global.rs:47-56`: política de asignación de ids, invariantes de los dos mapas y forma serializada. Es la mejor documentación del módulo.
- `global.rs:162` y `validation.rs:399-400`: dicen exactamente qué se verifica.
- `validation.rs:195,212,225`: los pasos `// 1.`, `// 2.` y `// 3.` documentan el orden de los chequeos, que es el orden de precedencia de errores que comparan los tests.

**Recortar o corregir:**
- **Desactualizado o narrativo:** `validation.rs:434` dice "…without reconstructing HashMaps". Compara con una implementación que no existe (el módulo usa `BTreeMap`). Basta con "Validates an Operation against this table."
- **Notación inconsistente y afirmación imprecisa:** `$O(C)$` (estilo LaTeX) aparece en `validation.rs:314,374,399,434`, mientras `table.rs:345,350,359,364` dice "O(C)". En `validate_column_updates` la cota además es imprecisa: es O(U·P) por el `primary_key().contains`. Conviene sacar la complejidad de los wrappers y dejarla, si acaso, en un solo lugar.
- **Docs que repiten la firma:** `table.rs:36` ("Sets an explicit table_id…"), `table.rs:42`, `table.rs:114`, `global.rs:180` ("Number of tables in the schema."). También los de las funciones libres `validation.rs:124,159,193,242,273,286,301` ("Validates an update operation on a Schema."). Se pueden borrar, y desaparecen solos con Q2.
- **Doc duplicado:**
  - `table.rs:391-393` es idéntico a `table.rs:411-413`. El segundo puede quedar en "Consuming variant of [`Self::from_compact_row`]".
  - `table.rs:270-272` (3 líneas) repite lo que ya dice `global.rs:151-152`. Alcanza con una línea: "Appends a nullable column (append-only evolution) and returns its positional index."
  - La regla "sin id por defecto" está tres veces: `table.rs:15-17`, `table.rs:103-104` y `global.rs:13-16`. Se puede dejar en `global.rs` y en `build`.
- **Doc impreciso:** `table.rs:64` dice "Adds a standard non-nullable column." Pero si la columna ya estaba declarada (por ejemplo con `nullable_column`), `column()` solo cambia el tipo y **conserva** `nullable`/`encrypted` (`table.rs:67-68`). Lo mismo pasa con `table.rs:75` y `table.rs:88`. Propuesta: "Declares a column (non-nullable if new); re-declaring only changes its type."
- `validation.rs:252` (`// PK columns cannot be updated directly`) repite el nombre de la variante `CannotUpdatePrimaryKey`. Se puede borrar.
- **Estado:** ✅ aplicada (paso 1). La complejidad quedó solo en los dos docs de `validation.rs` donde es exacta (`validate_compact_row`, `validate_keyed_row`), escrita `O(C)`.

## Sugerencias

### Q1. Helper `Schema::require_table` para la búsqueda por nombre
- **Dónde:** `global.rs:237-239`, `250-252`, `262-264`; `validation.rs:279-281`, `293-295`, `307-309`.
- **Problema:** el bloque de 3 líneas `let t = self.get_table_by_name(table).ok_or_else(|| ValidationError::TableNotFound(table.to_string()))?;` aparece 6 veces, idéntico.
- **Propuesta:**
  ```rust
  fn require_table(&self, name: &str) -> Result<&TableSchema, ValidationError> {
      self.get_table_by_name(name)
          .ok_or_else(|| ValidationError::TableNotFound(name.to_string()))
  }
  // …
  pub fn to_operation_delete(&self, table: &str, pk: PrimaryKey, timestamp: u64) -> Result<Operation, ValidationError> {
      self.require_table(table)?.to_operation_delete(pk, timestamp)
  }
  ```
  `add_column` (`global.rs:154-158`) necesita `get_mut`, así que queda como está. `validate_operation` (`global.rs:225-227`) busca por id con el mensaje `"id:{}"` y también queda igual.
- **Ahorro estimado:** ~9–12 líneas · **Riesgo:** bajo · **Prioridad:** alta
- **Verificación (2.ª pasada):** ⚠️ corregida — las 6 apariciones y líneas son correctas y el error es idéntico, pero un `fn require_table` privado en `impl Schema` (módulo `schema::global`) no es visible desde el módulo hermano `schema::validation`: hay que declararlo `pub(super)` (o `pub(crate)`), salvo que se aplique Q2 y los cuerpos de `validation.rs:273-312` se muden a `global.rs`.
- **Estado:** ✅ aplicada (paso 2): como Q2 llevó los cuerpos a `global.rs`, `require_table` es privada.

### Q2. Eliminar la doble indirección `Schema`/`TableSchema` → funciones libres de `validation.rs`
- **Dónde:** `table.rs:328-367` (7 wrappers: `validate_pk`, `validate_row`, `validate_update`, `validate_compact_row`, `validate_keyed_row`, `validate_column_updates`, `validate_operation`); `global.rs:206-221` (3 wrappers); `validation.rs:160-453` (funciones libres `pub`).
- **Problema:** cada validación tiene dos firmas, y las de `Schema` tienen tres (`Schema::validate_update` → `validation::validate_update` → `TableSchema::validate_pk`/`validate_update` → `validation::validate_table_update`). Además los nombres no coinciden (`validate_table_update` frente a `TableSchema::validate_update`). Las funciones libres son `pub` y salen como `zemdb_core::schema::validate_*` vía `pub use validation::*` (`mod.rs:9`), pero el grep no encuentra ningún uso fuera de `schema/`: los tests y `storage` usan los métodos. `validate_pk` recibe además un closure `get_column` (`validation.rs:163`) cuyo único caller pasa `|col| self.get_column(col)`, una generalidad sin uso.
- **Propuesta:**
  - Convertir las funciones libres de tabla en un bloque `impl TableSchema { … }` **dentro de `validation.rs`** (el archivo sigue siendo "la lógica de validación"), cambiando `table: &TableSchema` por `&self`, y borrar los wrappers de `table.rs:328-367`.
  - `validate_pk` pasa a ser `fn validate_pk(&self, pk)` sin closure.
  - Llevar los cuerpos de `validate_insert/update/delete` (`validation.rs:273-312`) a los métodos de `Schema` en `global.rs`, usando Q1 (cada uno queda en 1–3 líneas).
  - `validate_field_value` pasa a ser privada.
  - El orden de los chequeos y los errores no cambian. Solo se mueve código.
- **Ahorro estimado:** ~60–75 líneas · **Riesgo:** medio. Es superficie pública: hay que confirmar que ningún consumidor fuera del workspace importa `zemdb_core::schema::validate_*`. **A verificar.** · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — grep en todo `crates/` (core, storage, server, client): ninguna llamada a `validate_*` libres ni import de `schema::validate*` fuera de `schema/`; `validate_pk` tiene un único caller con closure (`table.rs:329-334`) y `validate_field_value` no se usa fuera de `validation.rs`. Un `impl TableSchema` en otro módulo del mismo crate compila; usar los accessors (`self.name()`, `self.columns()`) como hoy. Sigue siendo un cambio de API pública.
- **Estado:** ✅ aplicada (paso 2): las validaciones son un `impl TableSchema` en `validation.rs` (`validate_table_update` pasó a ser `TableSchema::validate_update`), sin wrappers en `table.rs`; `validate_pk` ya no recibe closure, `validate_field_value` es privada y los cuerpos de `validate_insert/update/delete` están en los métodos de `Schema`.

### Q3. `seen` en `validate_column_updates` es código muerto
- **Dónde:** `validation.rs:323`, `352-358`.
- **Problema:** el bloque `if let Some(prev)` (`335-349`) ya rechaza `idx == prev` (DuplicateColumn) e `idx < prev` (Unsorted). Por inducción los índices aceptados son estrictamente crecientes, así que `seen[col_idx]` nunca puede ser `true`. El test de duplicado (`schema_validation_tests.rs:477-485`) cae en la primera rama, con la misma variante y la misma columna. Hoy se asigna un `SmallVec` por llamada sin necesidad.
- **Propuesta:** borrar `seen` y, de paso, obtener `col_def` con `get` para no indexar tres veces `table.columns()[col_idx]`:
  ```rust
  let col_def = table.columns().get(u.column_idx as usize).ok_or_else(|| ValidationError::UnknownColumn {
      table: table.name().to_string(),
      column: format!("index {}", u.column_idx),
  })?;
  ```
  El texto `"index {}"` lo compara un test (`schema_validation_tests.rs:459`), así que se mantiene.
- **Ahorro estimado:** ~10–12 líneas · **Riesgo:** bajo · **Prioridad:** alta
- **Verificación (2.ª pasada):** ⚠️ corregida — `seen` es efectivamente inalcanzable (bounds check → `prev` rechaza `==` y `<`, así que los índices aceptados son estrictamente crecientes) y el test de duplicado da la misma variante/columna. Precisiones: el `get(...).ok_or_else` debe quedar **antes** del bloque `if let Some(prev)` (`336-349`, no `335`) para conservar la prioridad de `UnknownColumn`; y hay que borrar también `use smallvec::SmallVec;` (`validation.rs:6`), cuyo único uso es `seen` (si no, warning `unused_imports`).
- **Estado:** ✅ aplicada (paso 1)

### Q4. `TableBuilder`: un helper para "buscar o agregar columna"
- **Dónde:** `table.rs:50-101`.
- **Problema:** `primary_key`, `column`, `nullable_column` y `encrypted_column` repiten el mismo `if let Some(col) = self.columns.iter_mut().find(..) { col.data_type = …; <flag> } else { push(ColumnDef::new(..)<flag>) }`.
- **Propuesta:**
  ```rust
  /// The column `name` with type `data_type`, appended if not declared yet.
  fn upsert_column(&mut self, name: String, data_type: DataType) -> &mut ColumnDef {
      let idx = match self.columns.iter().position(|c| c.name == name) {
          Some(i) => i,
          None => { self.columns.push(ColumnDef::new(name, data_type)); self.columns.len() - 1 }
      };
      let col = &mut self.columns[idx];
      col.data_type = data_type;
      col
  }
  pub fn nullable_column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
      self.upsert_column(name.into(), data_type).nullable = true;
      self
  }
  ```
  Se comporta exactamente igual: las columnas nuevas nacen con `nullable = encrypted = false` y se activa solo el flag del método. `column()` no toca flags. `primary_key` conserva su `push` deduplicado en `self.primary_key` y pone `.nullable = false`.
- **Ahorro estimado:** ~12–15 líneas · **Riesgo:** bajo · **Prioridad:** media
- **Estado:** ✅ aplicada (paso 2)

### Q5. Chequeo de aridad de `CompactRow` triplicado
- **Dónde:** `table.rs:395-401`, `table.rs:415-421`, `validation.rs:379-385`.
- **Problema:** el mismo `if compact.len() > columns.len() { return Err(CompactRowArityMismatch { … }) }`, de 7 líneas, aparece tres veces.
- **Propuesta:** `pub(crate) fn check_compact_arity(&self, len: usize) -> Result<(), ValidationError>` en `TableSchema`, con una llamada de 1 línea en cada sitio.
- **Ahorro estimado:** ~10 líneas · **Riesgo:** bajo · **Prioridad:** media
- **Estado:** ✅ aplicada (paso 2): `TableSchema::check_compact_arity` vive en `validation.rs` con visibilidad `pub(super)`, que basta para `table.rs`.

### Q6. Bucles de acumulación → iteradores
- **Dónde:**
  - `table.rs:311-322` (`extract_pk`)
  - `table.rs:372-376` y `383-387` (`to_compact_row`, `row_into_compact`)
  - `table.rs:402-408` y `422-428` (`from_compact_row`, `compact_into_row`)
  - `table.rs:438-443` (`compact_update_fields`)
  - `table.rs:452-464` (`expand_update_fields`)
  - `validation.rs:388-392`
- **Propuesta (ejemplos):**
  ```rust
  // extract_pk: corta en la primera columna ausente, igual que el match actual
  let values = self.primary_key.iter()
      .map(|c| row.get(c).cloned().ok_or_else(|| ValidationError::MissingPrimaryKeyColumn {
          table: self.name.clone(), column: c.clone() }))
      .collect::<Result<_, _>>()?;           // SmallVec<[Value; 1]>
  let pk = PrimaryKey::from_smallvec(values);

  // to_compact_row
  let values = self.columns.iter().map(|c| row.get(&c.name).cloned().unwrap_or(Value::Null)).collect();

  // validate_compact_row
  let val = compact.get(i).unwrap_or(&Value::Null);

  // expand_update_fields: get() reemplaza el chequeo manual de rango
  let col = self.columns.get(u.column_idx as usize).ok_or_else(|| ValidationError::UnknownColumn { … })?;
  ```
  `Row` es `BTreeMap<String, Value>` (`value/row.rs:318`), así que `from_compact_row` puede hacer `.filter(..).map(..).collect()` directamente. Un `collect()` sobre `slice::Iter().map` reserva la capacidad exacta, igual que el `with_capacity` actual.
- **Ahorro estimado:** ~15–20 líneas · **Riesgo:** bajo · **Prioridad:** media
- **Estado:** ✅ aplicada (paso 1)

### Q7. Chequeo de `DataType::Null` duplicado, con mensaje literal repetido
- **Dónde:** `table.rs:207-214` y `table.rs:295-302`.
- **Problema:** las dos ramas construyen el mismo `InvalidColumnDataType` con el mismo texto largo, que es además el **único** valor de `message` en todo el workspace.
- **Propuesta:** un helper privado `fn check_column_type(table: &str, col: &ColumnDef) -> Result<(), ValidationError>`, llamado en el mismo punto de cada función para conservar la precedencia: en `try_new` antes del duplicado, en `add_column` al final. Opcional (**a verificar**, porque cambia la forma de una variante pública): quitar el campo `message` y poner el texto en el `#[error]`. El `Display` queda idéntico, y el único test (`schema_validation_tests.rs:370`) hace match con `{ .. }`.
- **Ahorro estimado:** ~5–8 líneas · **Riesgo:** bajo · **Prioridad:** media
- **Estado:** ✅ aplicada en parte (paso 2): helper privado `check_column_type` en `table.rs`, llamado en el mismo punto de `try_new` y `add_column`. ⏭️ no aplicado quitar el campo `message`: cambia la forma de una variante pública y no es consecuencia del helper.

### Q8. `impl Serialize for Schema` con newtype interno
- **Dónde:** `global.rs:63-83`.
- **Problema:** declara una struct `Tables<'a>` con su propio `impl Serialize` solo para llamar a `collect_seq`, 21 líneas en total.
- **Propuesta:**
  ```rust
  impl Serialize for Schema {
      fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
          let mut state = serializer.serialize_struct("Schema", 1)?;
          state.serialize_field("tables", &self.tables_by_id.values().collect::<Vec<_>>())?;
          state.end()
      }
  }
  ```
  `Vec<&TableSchema>` produce la misma secuencia en JSON y en bincode (longitud seguida de los elementos). El costo es un `Vec` de referencias por serialización, que es despreciable para un schema. Usar el bound inline `<S: Serializer>` / `<D: Deserializer<'de>>` ahorra 2 líneas más en cada impl manual (`global.rs:86-89`, `table.rs:136-139`). En `table.rs:150-157`, desestructurar el helper (`let TableSchemaHelper { table_id, name, primary_key, columns } = …`) deja la llamada a `try_new` en una línea.
- **Ahorro estimado:** ~15 líneas · **Riesgo:** bajo · **Prioridad:** media
- **Estado:** ✅ aplicada (paso 1)

### Q9. Ramas inalcanzables por el invariante de `try_new`
- **Dónde:** `validation.rs:197-202`, `175-178`, `410-415` (`UnknownColumn` para una columna PK) y `validation.rs:417-422` (`MissingPrimaryKeyColumn` en `validate_keyed_row`).
- **Problema:** `try_new` garantiza que toda columna PK existe, no es nullable y no está cifrada (`table.rs:224-247`). `add_column` solo agrega columnas nullable que no son PK, y los campos son privados.
  - Las tres búsquedas de una columna PK nunca fallan.
  - En `validate_keyed_row`, `validate_compact_row` (`407`) ya devolvió `MissingRequiredColumn` si una columna PK falta o es `Null`, así que `417-422` no se puede alcanzar.
  - `validate_keyed_row` es además el único uso de `column_indices()` (`table.rs:182-185`); `table.column_index(..)` alcanza.
- **Propuesta:** borrar `417-422` y reemplazar las búsquedas por `expect("primary key column exists")`. El módulo ya usa ese estilo en `table.rs:440` (`expect("already validated")`). Si el equipo prefiere conservar el `Err` defensivo, al menos sacar `417-422`, que hoy está precedido por un chequeo que la cubre.
- **Ahorro estimado:** ~12–18 líneas · **Riesgo:** bajo en semántica. Pasar de `Err` a `panic` en un estado imposible es una decisión de estilo. **A verificar** con la convención del equipo. · **Prioridad:** media
- **Estado:** ✅ aplicada en parte (paso 1): se borró el chequeo inalcanzable `MissingPrimaryKeyColumn` de `validate_keyed_row`. ⏭️ no aplicado el cambio de las búsquedas de columna PK de `Err` a `expect`: pasar de error a panic es una decisión de convención que el informe deja a verificar.

### Q10. Menores
- `global.rs:101-106`: `Schema::new` repite el `Default` derivado. Puede ser `Self::default()` (−3).
- `validation.rs:226-236`: en el paso 3 de `validate_row`, el `if let … else { return Err }` se aplana a `let col_def = table.get_column(col_name).ok_or_else(..)?; if col_def.nullable { … }` (−3, mismo orden).
- `validation.rs:336-349`: el `if/else if` sobre `prev` se lee mejor como `match u.column_idx.cmp(&prev)` (sin ahorro, más claro).
- `global.rs:164-167`: `next_table_id` puede usar `self.tables_by_id.last_key_value()`. Es cosmético, opcional.
- `mod.rs:1-4`: los submódulos son `pub` y además se reexportan con `*`, así que cada tipo tiene dos rutas. No hay usos de `schema::{table,global,column,validation}::` fuera del módulo. Podrían ser `mod` privados (0 líneas, API más limpia). **A verificar**: cambia las rutas de rustdoc.
- **Ahorro estimado:** ~6–8 líneas · **Riesgo:** bajo · **Prioridad:** baja
- **Estado:** ✅ aplicada en parte (paso 1): `Schema::new` con `Self::default()`, paso 3 de `validate_row` aplanado y `match … cmp` en `validate_column_updates`. ⏭️ no aplicado `last_key_value` en `next_table_id`: es cosmético y opcional, y `keys().next_back()` ya se lee bien. Los submódulos privados de `mod.rs` cambian la API pública y quedan para el paso 4.

## Lo que conviene dejar como está

- **`ValidationError` (`validation.rs:10-122`, 113 líneas).** Cada variante es distinta, los tests comparan variantes y campos exactos, y ya usa `thiserror`. No conviene fusionar variantes (salvo la opción de `message` en Q7).
- **`TableSchema::try_new` (`table.rs:188-256`).** Su largo está justificado: es el único punto que establece los invariantes de los que depende toda la validación (PK existente, no nullable, no cifrada; sin columnas `Null` ni duplicadas). El orden de los chequeos define la precedencia de errores.
- **Orden de chequeos en `add_column` (`table.rs:273-308`) e `insert_table` (`global.rs:133-149`).** Validan todo antes de mutar (principio de GEMINI.md). El orden nombre→id decide qué error aparece si chocan los dos.
- **Las tres pasadas de `validate_row`.** Unificarlas en una sola pasada cambiaría qué error sale primero cuando hay varios (por ejemplo, `test_schema_insert_missing_pk` espera `MissingPrimaryKeyColumn`).
- **Los `Deserialize` manuales vía `try_new` (`table.rs:135-159`, `global.rs:85-98`).** Un `derive` saltaría la validación estricta, y `deny_unknown_fields` más la ausencia de default de `table_id` son intencionales.
- **Pares prestado/consumidor** (`to_compact_row`/`row_into_compact`, `from_compact_row`/`compact_into_row`). Los dos están en uso, e implementar uno en función del otro agregaría clones.
- **Accessors `#[inline]` de `ColumnDef` y `TableSchema`, y los gemelos que hacen panic** (`SchemaBuilder::table`, `Schema::from_tables`). Son triviales, pero sostienen el encapsulamiento y los tests los usan mucho.

## Tests

No me asignaron tests. El módulo no tiene `src/schema/tests/`: sus pruebas viven en `crates/core/tests/`. Ninguna de las sugerencias Q1–Q10 cambia los mensajes o variantes que comparan esos tests (`"index 99"`, `DuplicateColumn` por `idx == prev`, `MissingPrimaryKeyColumn` antes que `TypeMismatch`).

**Observaciones fuera del alcance de redacción** (no se resuelven con un refactor de calidad; las dejo anotadas):
1. `try_new` no rechaza columnas PK repetidas en `primary_key` (por ejemplo `["id","id"]`) cuando se llama directo o por deserialización. `TableBuilder::primary_key` sí deduplica (`table.rs:52`).
2. Un PK con el tipo incorrecto da errores distintos según el camino. `Schema::validate_insert` devuelve `TypeMismatch` porque `validate_row` corre primero (`validation.rs:209`). `TableSchema::to_operation_insert` devuelve `PrimaryKeyTypeMismatch` porque `extract_pk` corre primero (`table.rs:473`). Además los dos caminos validan el PK dos veces (`validate_row` más `extract_pk`→`validate_pk`).
