# Auditoría de calidad — `crates/core/src/value/`

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `mod.rs`, `data_type.rs`, `decode_budget.rs`, `row.rs`, `scalar.rs`, `tests/decode_budget.rs`, `tests/row.rs`

## Resumen
- Módulo en buen estado: código idiomático, sin referencias a códigos de auditoría ni narrativa histórica, y con los invariantes de seguridad (límite de columnas y presupuesto de valores) bien documentados.
- Casi todo el volumen sobrante está en `scalar.rs`, que repite boilerplate: 6 `impl From<entero>` idénticos, 8 accessors `as_*` y la canonicalización de `-0.0` escrita 5 veces.
- **Ahorro estimado:** ~55–75 líneas, un **7–9 %** de las 808 líneas de código fuente (o un 6–8 % si se cuentan los tests, 945 en total), sin tocar comportamiento.
- Principales oportunidades: macro para los `From` de enteros (Q1), `parse_uuid` con iteradores (Q2), el `Display` de `DataType` delegando en `Debug` (Q3) y los accessors duplicados de `row.rs` y `scalar.rs` (Q4, Q5).
- En comentarios hay más déficit que exceso: `scalar.rs` no explica por qué `Eq`/`Hash`/`Ord` son manuales (Float con `total_cmp` y `-0.0 == 0.0`). Conviene agregar 2–3 líneas de *por qué*.

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| `scalar.rs` | 350 | 304 | 7 | 0 | 39 | ~40–50 |
| `row.rs` | 343 | 254 | 32 | 0 | 57 | ~12–16 |
| `data_type.rs` | 30 | 27 | 1 | 0 | 2 | ~9 |
| `decode_budget.rs` | 75 | 46 | 19 | 0 | 10 | 0 |
| `mod.rs` | 10 | 8 | 0 | 0 | 2 | 0 |
| `tests/row.rs` | 77 | 63 | 1 | 2 | 11 | 0 (+1–2 por claridad) |
| `tests/decode_budget.rs` | 60 | 52 | 0 | 2 | 6 | 0 (+1 por claridad) |
| **Total** | **945** | **754** | **60** | **4** | **127** | **~55–75** |

## Comentarios

**Densidad:** muy desigual. `decode_budget.rs` (19 líneas doc/75) y `row.rs` (32/343) están bien comentados. `scalar.rs` tiene solo 7 líneas doc en 350, y justo ahí está la lógica menos obvia.

**Conservar (valiosos):**
- `decode_budget.rs:1-10`: doc del módulo. Explica por qué existe (un `Null` ocupa 1 byte en el wire y 24 en memoria) y su costo fuera de un presupuesto. Es el *por qué* del diseño.
- `decode_budget.rs:47-49`: el guard restaura el estado anterior también ante un panic, y así el presupuesto no se filtra a otros decodes (principio RAII).
- `row.rs:245-256`: por qué no se usan los `Deserialize` derivados (prealloc de 1 MiB, `SmallVec` confía en el largo declarado) y cómo se cobra el presupuesto. Es un comentario de seguridad.
- `row.rs:258-259`: la justificación medida del `#[inline]` ("about a third slower"). Sin ella alguien lo borraría.
- `row.rs:238-239`: por qué se limita la preasignación.
- `scalar.rs:8-12` y `row.rs:12-13`: los tamaños (24 y 40 bytes) están respaldados por asserts en `crates/core/tests/types_memory_tests.rs:5,7,13`, así que son invariantes y no prosa.

**Recortar:**
- `row.rs:242-259` (18 líneas doc de `deserialize_columns`): el segundo y el tercer párrafo se superponen ("rejected before decoding anything … counted as they arrive" se repite con "the declared length up front … one by one any element beyond it"). Se pueden fusionar en ~13–14 líneas sin perder contenido. Ver Q8.
- `row.rs:12-13`: "optimized with SmallVec to keep scalar keys on the stack while strictly fitting within a single 64-byte L1 cache line (size: 40 bytes)" se puede reducir a una línea, por ejemplo "Inline up to one value (40 bytes, fits a cache line)".
- `scalar.rs:153`: "Creates a Value::Uuid from a UUID hex string (32 or 36 chars with hyphens)." repite la doc de `parse_uuid` (`scalar.rs:122`). Alcanza con "Like [`Value::parse_uuid`], wrapped in `Value::Uuid`."
- **Estado:** ✅ aplicada (paso 1)

**Agregar (falta el *por qué*):**
- `scalar.rs:279-331`: falta un comentario de 1–2 líneas que diga que `PartialEq`/`Hash`/`Ord` son manuales para que `Float` tenga orden total (`total_cmp`) con `-0.0 == 0.0`, y que el `Hash` canonicaliza igual para ser coherente con `Eq`. Hoy hay que deducirlo.
- `scalar.rs:27`: `type_order` es `pub` y define el orden entre tipos (lo usan `Ord` y `Hash`), pero no tiene doc.
- **Estado:** ✅ aplicada (paso 1)

## Sugerencias

### Q1. Macro para los `From<entero>` de `Value`
- **Dónde:** `scalar.rs:169-209` (`i8`, `i16`, `i32`, `u8`, `u16`, `u32`; `i64` aparte en 187-191).
- **Problema:** hay 6 bloques de 5 líneas más un blanco, idénticos salvo el tipo, todos con `v as i64`.
- **Propuesta:**
  ```rust
  macro_rules! from_lossless_int {
      ($($t:ty),*) => {$(
          impl From<$t> for Value {
              fn from(v: $t) -> Self {
                  Value::Int(i64::from(v))
              }
          }
      )*};
  }
  from_lossless_int!(i8, i16, i32, u8, u16, u32);
  ```
  `i64::from` es equivalente a `as i64` para estos tipos (todas son conversiones sin pérdida) y deja explícito que no se trunca.
- **Ahorro estimado:** ~25 líneas · **Riesgo:** bajo · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — líneas 169-209 correctas; para `i8/i16/i32` `as i64` extiende signo y para `u8/u16/u32` extiende con ceros, exactamente igual que `i64::from`. (Opcional: `i64` también entra en la macro, `i64::from(i64)` es la identidad.)
- **Estado:** ✅ aplicada (paso 1), con `i64` incluido en la macro.

### Q2. `parse_uuid` con iteradores en lugar de dos bucles con índices manuales
- **Dónde:** `scalar.rs:123-151`.
- **Problema:** dos ramas (36 y 32 caracteres) repiten el armado `(hi << 4) | lo`. La rama de 36 usa un `while` con `i`/`bi` y los saltos de guion están cableados en dos lugares (126 y 130).
- **Propuesta:**
  ```rust
  pub fn parse_uuid(s: &str) -> Option<[u8; 16]> {
      const HYPHENS: [usize; 4] = [8, 13, 18, 23];
      let b = s.as_bytes();
      let hyphenated = match b.len() {
          32 => false,
          36 if HYPHENS.iter().all(|&i| b[i] == b'-') => true,
          _ => return None,
      };
      let mut digits = (0..b.len()).filter(|i| !(hyphenated && HYPHENS.contains(i))).map(|i| b[i]);
      let mut bytes = [0u8; 16];
      for byte in &mut bytes {
          *byte = (hex_val(digits.next()?)? << 4) | hex_val(digits.next()?)?;
      }
      Some(bytes)
  }
  ```
  Es equivalente: los segmentos 8-4-4-4-12 tienen largo par, así que los pares de dígitos coinciden con los del bucle original. Un guion fuera de posición sigue fallando en `hex_val`. No asigna memoria. Los tests de `crates/core/tests/types_memory_tests.rs:21,48,57` cubren ambos formatos.
- **Ahorro estimado:** ~10 líneas · **Riesgo:** bajo-medio (conviene sumar un caso con guion mal ubicado y uno con mayúsculas antes del cambio) · **Prioridad:** media
- **Verificación (2.ª pasada):** ✅ confirmada — mismos pares de dígitos (el `while` original salta 8/13/18/23 y avanza de a 2: (9,10)…(24,25)…), mismo resultado `None` para largo distinto de 32/36, guiones mal ubicados (36 → `None`; guion en posición de dígito → `hex_val` falla) o hex inválido; el guard `36 if …` solo indexa tras confirmar el largo, así que no hay OOB; `digits.next()?` nunca agota (32 dígitos). Compila: `HYPHENS.contains(i)` recibe `&usize`, `?` dentro del `for` en fn que devuelve `Option`. Tests citados (`:21,48,57`) correctos.
- **Estado:** ✅ aplicada (paso 1), junto con Q9 (`to_digit(16)` en lugar de `hex_val`). La equivalencia con la versión anterior se comprobó fuera del repo sobre ~2,2 millones de entradas (mutaciones de UUID válidos con y sin guiones, mayúsculas, largos 35/37, no ASCII). Los casos borde en `types_memory_tests.rs` quedan para el paso de tests.

### Q3. `Display` de `DataType` delegando en `Debug`
- **Dónde:** `data_type.rs:17-30`.
- **Problema:** son 8 brazos que escriben literalmente el nombre de la variante, exactamente lo que imprime el `Debug` derivado de un enum sin campos.
- **Propuesta:**
  ```rust
  impl fmt::Display for DataType {
      fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
          fmt::Debug::fmt(self, f) // the variant name, e.g. "Int"
      }
  }
  ```
  La salida es idéntica: el `Debug` derivado de variantes unitarias usa `write_str`, que igual que el `write!(f, "Null")` actual ignora el ancho y el relleno. Los mensajes de error que muestran `DataType` no cambian.
- **Ahorro estimado:** ~9 líneas · **Riesgo:** bajo (el `Display` queda acoplado al nombre de la variante; si algún día se renombra una variante, cambia el texto, y el comentario lo deja dicho) · **Prioridad:** media
- **Estado:** ✅ aplicada (paso 1)

### Q4. Accessors duplicados en `PrimaryKey` y `CompactRow`
- **Dónde:** `row.rs:34-40` (`values`/`as_slice` en `PrimaryKey`), `row.rs:136-142` (lo mismo en `CompactRow`), además de `AsRef<[Value]>` (73-78 y 177-182), que hace lo mismo.
- **Problema:** hay tres formas de obtener el mismo `&[Value]`. `as_slice()` solo se usa dentro de `row.rs:69` y `row.rs:163`; ningún otro crate, test ni bench lo llama (grep en todo el repo).
- **Propuesta:** eliminar `as_slice` de ambos tipos y usar `self.0.iter()` / `self.values.iter()` en los `IntoIterator`.
- **Ahorro estimado:** ~8 líneas · **Riesgo:** bajo · **a verificar:** es API `pub` de `zemdb_core`; borrarlo solo es seguro si no hay consumidores fuera del workspace · **Prioridad:** media

### Q5. `to_uuid` como `as_uuid().copied()`
- **Dónde:** `scalar.rs:106-120`.
- **Problema:** dos funciones casi idénticas. `as_uuid` no tiene usos fuera de `scalar.rs` y `to_uuid` tiene uno (`types_memory_tests.rs:53`).
- **Propuesta:** `pub fn to_uuid(&self) -> Option<[u8; 16]> { self.as_uuid().copied() }`. Opcional y a verificar: eliminar una de las dos, porque es API pública.
- **Ahorro estimado:** ~4 líneas · **Riesgo:** bajo · **Prioridad:** baja
- **Estado:** ✅ aplicada en parte (paso 1): `to_uuid` es `as_uuid().copied()`. Borrar una de las dos es API pública y queda para el paso 4.

### Q6. Un único helper para canonicalizar `-0.0`
- **Dónde:** `scalar.rs:213`, `220`, `295`, `320-321` (5 apariciones de `if x == 0.0 { 0.0 } else { x }`).
- **Problema:** la regla "`-0.0` y `0.0` son el mismo valor" vive copiada en 5 sitios. Si se cambia en uno y no en otro, `Hash` y `Eq` quedan incoherentes.
- **Propuesta:** agregar `#[inline] fn canonical(v: f64) -> f64 { if v == 0.0 { 0.0 } else { v } }` con una línea doc ("`-0.0` and `0.0` are the same value; keeps `Hash` consistent with `Eq`"). `From<f32>` pasa a ser `Value::from(f64::from(v))`. Para NaN no cambia nada: `NaN == 0.0` es falso, igual que hoy.
- **Ahorro estimado:** ~0–2 líneas netas (el beneficio es claridad y una sola fuente de verdad) · **Riesgo:** bajo · **Prioridad:** media
- **Estado:** ✅ aplicada (paso 2)

### Q7. `PrimaryKey::single` sin `push` manual
- **Dónde:** `row.rs:20-24`.
- **Propuesta:** `Self(SmallVec::from_buf([value.into()]))`. Sigue inline, y el test `tests/row.rs:57-64` lo cubre. En la misma línea, `row.rs:27`: `.map(|v| v.into())` puede ser `.map(Into::into)`.
- **Ahorro estimado:** ~2 líneas · **Riesgo:** bajo · **Prioridad:** baja
- **Estado:** ✅ aplicada (paso 1)

### Q8. Condensar la doc de `deserialize_columns`
- **Dónde:** `row.rs:242-259`.
- **Problema:** se superponen los párrafos 2 y 3 (ver la sección Comentarios).
- **Propuesta:** fusionarlos. Hay que conservar: el motivo (los derives confían en el largo declarado), las tres defensas (rechazo anticipado, prealloc acotado, conteo incremental), el cobro del presupuesto (largo declarado por adelantado y luego de a uno) y la nota del `#[inline]`.
- **Ahorro estimado:** ~4 líneas · **Riesgo:** nulo · **Prioridad:** baja
- **Estado:** ✅ aplicada (paso 1)

### Q9. `hex_val` con `char::to_digit`
- **Dónde:** `scalar.rs:159-167`.
- **Propuesta:** `(c as char).to_digit(16)` acepta exactamente `0-9a-fA-F`. Combinado con Q2, `hex_val` desaparece (con un cast final `as u8`).
- **Ahorro estimado:** ~6 líneas · **Riesgo:** bajo · **Prioridad:** baja
- **Estado:** ✅ aplicada (paso 1), combinada con Q2.

### Q10. Menores de estilo, sin cambio de líneas
- `scalar.rs:340-344`: el `format!` del UUID con 16 argumentos supera las 100 columnas, y rustfmt no lo formatea. Un bucle con `if matches!(i, 4 | 6 | 8 | 10) { f.write_str("-")?; }` y `write!(f, "{byte:02x}")` se lee mejor, aunque ocupa 1–2 líneas más.
- `scalar.rs:336-347`: `write!(f, "NULL")` puede ser `f.write_str("NULL")`, y `write!(f, "{}", v)` puede ser `write!(f, "{v}")`.
- `scalar.rs:316`: el brazo `(Null, Null) => Equal` es redundante con el fallback por `type_order` de la línea 328. Se puede borrar (1 línea) o dejar por explícito.
- `row.rs:193`: `std::ops::IndexMut` va con ruta completa mientras `Index` se importa arriba (`row.rs:10`). Unificar.
- `row.rs:173`: `self.into_values().into_iter()` puede ser `self.values.into_iter()`.
- `mod.rs:1-4`: los submódulos son `pub` y además se reexportan con `*`. Ningún crate externo usa las rutas `value::scalar::…`, `value::row::…` ni `value::data_type::…` (solo `mutation/op.rs:1` usa `value::row::deserialize_columns`, que es `pub(crate)`). **A verificar** si conviene hacerlos privados: es un cambio de API.
- **Estado:** ✅ aplicada en parte (paso 1): `Display` del UUID con bucle, `write_str`/argumentos en línea, `IndexMut` importado y `self.values.into_iter()`. ⏭️ no aplicado el borrado del brazo `(Null, Null)`: el informe lo deja opcional y se conserva por explícito. Los submódulos privados de `mod.rs` cambian la API y quedan para el paso 4.

## Lo que conviene dejar como está
- **`decode_budget.rs` completo:** el enum de tres estados, el guard RAII con `Drop` y el `#[must_use]` son la forma mínima correcta. No hay nada que recortar.
- **El trait `ColumnSeq` (`row.rs:206-236`):** parece boilerplate, pero `Vec` y `SmallVec` no comparten un trait estándar con `reserve`, y sin él se perdería el prealloc acotado. `Default + Extend` no alcanza (`extend_reserve` es inestable).
- **El visitor de `deserialize_columns` (`row.rs:281-305`):** el contador manual `len` y el orden de los chequeos (primero el límite, después el cobro, después el `push`) implementan "a lo sumo un valor materializado de más". No conviene compactarlo.
- **Los accessors `as_int`/`as_float`/…:** una macro ahorraría ~30 líneas, pero empeora la navegación y la doc generada, y con rustfmt la forma `if let … else` no entra en una línea (supera `single_line_if_else_max_width`). Recomiendo dejarlos.
- **`PartialEq` vía `cmp` y `Hash` manual:** son necesarios por la semántica de `Float`. Solo falta el comentario (ver Comentarios).
- **Los pares `CompactRow::new` / `From<Vec<Value>>` y `RowBuilder`:** son API ergonómica usada en muchos tests de `server` (28 usos de `RowBuilder`).

## Tests
- **`tests/decode_budget.rs`:** cubre bien los casos (ilimitado, exacto, en bloque, anidado, panic). Una observación de claridad: en `values_are_charged_in_bulk` (`tests/decode_budget.rs:34`), el segundo `let budget = ValueBudget::start(10);` *sombrea* al primero sin soltarlo, así que quedan dos guards anidados vivos. El test pasa, pero de casualidad respecto de lo que dice probar. Conviene agregar `drop(budget);` antes, como hace `tests/row.rs:72`.
- **`tests/row.rs:44`:** el literal `"65536"` es `MAX_COLUMNS + 1` (`MAX_COLUMNS = u16::MAX`). Es más robusto usar `contains(&(MAX_COLUMNS + 1).to_string())`.
- **`tests/row.rs:69,74`:** en lugar de `crate::value::ValueBudget`, conviene importarlo una vez (`use crate::value::ValueBudget;`).
- **`tests/row.rs:47-55`:** `items.clone()` seguido de `pop()` se lee bien. Sin cambios.
- **Fuera de alcance:** `scalar.rs` y `data_type.rs` no tienen tests unitarios en `value/tests/`; los cubren los tests de integración (`crates/core/tests/types_memory_tests.rs`). Si se aplica Q2, conviene agregar casos borde de UUID allí (guion mal ubicado, mayúsculas, 35 o 37 caracteres).
