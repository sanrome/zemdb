# Auditoría de calidad — `crates/core/src` (archivos de nivel superior)

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `lib.rs`, `id.rs`, `crypto.rs`

## Resumen
- Los tres archivos están en buen estado: código idiomático, `thiserror` ya en uso, validación de IDs centralizada en un `macro_rules!` y sin referencias a códigos de auditoría.
- Ahorro estimado: **~20–35 líneas de 464 (≈4–8 %)**. Margen chico: la mayor parte del código ya es mínima.
- Lo que más rinde: borrar el módulo de compatibilidad `operation`, que nadie usa (`lib.rs:10-14`), y unificar con una macro los dos newtypes `u64` (`SequenceNumber` y `CorrelationId`, `id.rs:231-268` y `328-361`). Esto último es opcional.
- Comentarios: densidad adecuada en `id.rs`, donde explican el *por qué* (sistemas de archivos que no distinguen mayúsculas, spoofing Unicode). En `crypto.rs` faltan: el puerto público no tiene ni un `///`, y `NoOpCryptoEngine` no avisa que no cifra. Ahí conviene **sumar** 4–6 líneas.

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| `lib.rs` | 40 | 36 | 1 | 0 | 3 | ~6–8 |
| `id.rs` | 361 | 269 | 42 | 3 | 47 | ~15–27 |
| `crypto.rs` | 63 | 57 | 0 | 0 | 6 | 0 (+4–6 de doc recomendadas) |
| **Total** | **464** | **362** | **43** | **3** | **56** | **~20–35** |

## Comentarios

- **`id.rs`: buena calidad, conservar casi todo.**
  - `id.rs:45-49` (`validate_path_id`) explica por qué se prohíben las mayúsculas en macOS y Windows: es una razón que el código no deja ver.
  - `id.rs:91-98` (`is_invisible_or_bidi`) dice qué cubre la lista negra y qué **no** garantiza (sin normalización Unicode; los tokens atan los bytes exactos). Es documentación de seguridad valiosa.
  - `id.rs:4-7` (`InvalidIdError`) documenta el truncado del valor para que el error no crezca con la entrada.
  - `id.rs:111`, `118` y `129` agrupan los rangos Unicode. Son útiles.
- **Redundancia leve en `id.rs`:** la regla de los IDs de ruta aparece tres veces: en `validate_path_id` (`id.rs:47-49`), en `RoomId` (`id.rs:207-208`) y en `SchemaId` (`id.rs:216-217`). Es aceptable porque `validate_path_id` es privada y las otras dos son la doc pública. No la recortaría.
- **`id.rs:148`** (`/// Validates `id` and wraps it.`) y **`lib.rs:10`** (`/// Compatibility alias module for legacy imports.`) son los únicos doc-comments flojos. El de `lib.rs:10` desaparece con Q1.
- **`crypto.rs`: no tiene ningún comentario.**
  - `CryptoConcurrencyBounds` (`crypto.rs:14-22`) existe porque en `wasm32` los futures de WebCrypto no son `Send`. Hoy eso solo se sabe por `ROADMAP.md:210`.
  - `NoOpCryptoEngine` (`crypto.rs:41-42`) devuelve el texto plano tal cual, y nada en el código lo advierte.
  - Ver Q3.

## Sugerencias

### Q1. Borrar el módulo `operation`, alias de compatibilidad que no se usa
- **Dónde:** `lib.rs:10-14`.
- **Problema:** `pub mod operation { pub use crate::mutation::*; pub use crate::protocol::SequencedOperation; }` es un alias "para imports legacy". `grep` no encuentra ningún `zemdb_core::operation` ni `crate::operation` en el workspace (server, storage, client, tests). El proyecto está en 0.1.0 y no tiene consumidores externos. Además, el doc-comment es narrativa histórica.
- **Propuesta:** borrar el bloque y su doc-comment.
- **Ahorro estimado:** ~6 líneas · **Riesgo:** bajo (cambia la API pública; **a verificar** solo si hay consumidores fuera del repo) · **Prioridad:** alta.
- **Verificación (2.ª pasada):** ✅ confirmada — el bloque está en `lib.rs:10-14` (doc + módulo); grep en todo el repo (incluidos imports multilínea `operation::`/`operation,` y `.md`) no encuentra usos de `zemdb_core::operation` ni `crate::operation`. Los mismos ítems siguen exportados por `mutation`, `protocol` y los `pub use` de la raíz.

### Q2. Macro para los newtypes `u64` (opcional)
- **Dónde:** `id.rs:231-268` (`SequenceNumber`) y `id.rs:328-361` (`CorrelationId`).
- **Problema:** los dos bloques son idénticos salvo el nombre y `SequenceNumber::next`: mismo `derive` (que incluye `Default`), `#[serde(transparent)]`, `new`/`get` `const`, `Display` con `write!(f, "{}", self.0)`, `From<u64>` y `From<X> for u64`. El archivo ya usa la misma técnica para los IDs de texto (`validated_string_id!`, `id.rs:140-202`).
- **Propuesta:** crear `macro_rules! u64_id { ($(#[$meta:meta])* $name:ident) => { ... } }` con exactamente esos impls, invocarla dos veces y dejar `impl SequenceNumber { pub const fn next(&self) -> Self { Self(self.0 + 1) } }` fuera de la macro.
  - **Mantener `write!(f, "{}", self.0)`.** Cambiarlo por `fmt::Display::fmt(&self.0, f)` cambiaría el comportamiento con flags de ancho o relleno (`{:>5}`).
  - Ganancia modesta: con solo dos tipos, el código explícito actual también se lee bien. Aplicarla solo si se agrega un tercer ID numérico.
- **Ahorro estimado:** ~15–20 líneas · **Riesgo:** bajo · **Prioridad:** baja.
- **Estado:** ⏭️ no aplicada — el propio informe la recomienda solo cuando haya un tercer ID numérico; con dos tipos el código explícito se lee igual de bien.

### Q3. Documentar el puerto criptográfico (suma líneas, mejora la claridad)
- **Dónde:** `crypto.rs:4-12`, `14-22`, `24-39` y `41-42`.
- **Problema:** es la API pública de un puerto de seguridad y no tiene doc. Sin leer el ROADMAP no se sabe:
  - qué es `aad` (datos autenticados que se ligan al cifrado);
  - por qué `CryptoConcurrencyBounds` tiene dos definiciones según `cfg`;
  - que `NoOpCryptoEngine` **no cifra**.
- **Propuesta:** agregar estos doc-comments de una línea:
  - en `CryptoEngine`: "Cifra y descifra valores de columnas `encrypted`; `aad` se autentica pero no se cifra";
  - en `CryptoConcurrencyBounds`: "`Send + Sync` salvo en `wasm32`, donde los futures de WebCrypto no son `Send`";
  - en `NoOpCryptoEngine`: "Devuelve los datos sin cifrar; solo para tests o despliegues sin E2EE".
- **Ahorro estimado:** −4 a −6 líneas (suma) · **Riesgo:** nulo · **Prioridad:** media.
- **Estado:** ✅ aplicada (paso 1)

### Q4. Quitar `AsRef<[u8; 16]>` de `MutationId` (a verificar)
- **Dónde:** `id.rs:295-299`.
- **Problema:** es redundante con `as_bytes()` (`id.rs:280-282`), que devuelve lo mismo y además es `const`. Su único uso en el workspace es una aserción en `crates/core/tests/types_memory_tests.rs:226`. `AsRef<[u8]>` (`id.rs:289-293`) tampoco tiene usos fuera de ese test, pero es el impl idiomático para APIs genéricas de bytes: conviene dejarlo.
- **Propuesta:** borrar el impl `AsRef<[u8; 16]>` y la aserción del test.
- **Ahorro estimado:** ~5 líneas (+1 en el test) · **Riesgo:** bajo (API pública; **a verificar** si hay consumidores externos) · **Prioridad:** baja.

### Q5. `#![forbid(unsafe_code)]` repite el lint del workspace
- **Dónde:** `lib.rs:1`.
- **Problema:** `Cargo.toml` del workspace define `[workspace.lints.rust] unsafe_code = "forbid"` y `crates/core/Cargo.toml` hereda con `[lints] workspace = true`, así que el atributo es redundante. `crates/storage/src/lib.rs:1` también lo tiene.
- **Propuesta:** borrarlo de `core` y `storage` a la vez, para que quede consistente. También es válido mantenerlo como defensa en profundidad, por si alguien quita `[lints] workspace = true`. Decisión de estilo.
- **Ahorro estimado:** ~2 líneas · **Riesgo:** nulo · **Prioridad:** baja.
- **Estado:** ⏭️ no aplicada — es una decisión de estilo (el atributo también sirve de defensa en profundidad) y, para que quede consistente, habría que tocar `crates/storage`, fuera del crate de este paso.

## Lo que conviene dejar como está
- **`is_invisible_or_bidi` (`id.rs:99-134`):** la lista de rangos es larga por necesidad, porque `char::is_control` solo cubre la categoría Cc. Comprimirla con una dependencia de propiedades Unicode sería un cambio de dependencias, no de redacción.
- **Mensajes con el límite escrito a mano** (`id.rs:56`, "at most 64 characters", e `id.rs:77`, "at most 256 bytes"): repiten `MAX_PATH_ID_LEN` y `MAX_CLIENT_ID_LEN`, pero `reason` es `&'static str` y `concat!` no acepta constantes. Generarlos en tiempo de ejecución cambiaría el tipo del error. Se puede dejar así.
- **Las dos ramas `cfg` de `CryptoConcurrencyBounds`** (`crypto.rs:14-22`) y el par `cfg_attr(async_trait…)` repetido en el trait y en el impl (`crypto.rs:24-25` y `44-45`): `async_trait` lo exige así y no hay una forma más corta.
- **La lista de reexportaciones de `lib.rs:16-40`:** es la superficie pública del crate, y la usan server, storage y los tests (`use zemdb_core::*`).
- **`validated_string_id!`:** `TryFrom<String>`, `TryFrom<&str>` y `FromStr` delegan en `new`. Son los impls estándar y los necesita `#[serde(try_from = "String")]`.
- **`MutationId::from_u128`** junto a `From<u128>`: `from_u128` es `const` y tiene 14 usos en server y tests.

## Tests
Los tests de estos archivos están en `crates/core/tests/` (`id_validation_tests.rs`, `crypto_tests.rs`, `types_memory_tests.rs`) y se auditan en `crates/core/tests/AUDITORIA_CALIDAD.md`.
