# Auditoría de calidad — `crates/client`

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `src/lib.rs` (3 líneas), `tests/client_tests.rs` (7 líneas), `Cargo.toml`

## Resumen
- El crate es el esqueleto que genera `cargo new --lib`. No tiene código propio que acortar: `src/lib.rs:1-3` es `pub fn add(left, right)` y `tests/client_tests.rs:1-7` es el test `it_works`, que comprueba `add(2, 2) == 4`. No tiene comentarios.
- Ahorro de líneas: **0**. No hay nada que reescribir.
- **Dependencias sin uso:** `Cargo.toml` declara `zemdb-core`, `serde`, `bincode`, `thiserror` y `bytes`, y `src/lib.rs` no usa ninguna. Además, `src/lib.rs` no tiene `#![forbid(unsafe_code)]` como `core` y `storage`, aunque el lint del workspace igual se aplica por `[lints] workspace = true`.
- **Sugerencia (prioridad baja, riesgo nulo):** mientras no empiece la Fase 4 (`ROADMAP.md:444`), reemplazar `add` e `it_works` por un `//!` de una línea que diga que el crate está reservado para el SDK cliente (`ZemdbClient` → `RoomHandle` → `TableHandle`), y quitar las dependencias que no se usan. Así se evita que el ROADMAP cuente ese test placeholder como cobertura del cliente: `ROADMAP.md:96` dice "1 en `zemdb-client`". Si se prefiere mantener el placeholder hasta implementar el SDK, no hay nada más que objetar.
  - **Estado:** ✅ aplicada en parte (paso 1): se quitaron las cinco dependencias sin uso de `Cargo.toml` (y sus entradas en `Cargo.lock`); el build wasm de CI (`core`, `storage`, `client`) y `cargo check --workspace` siguen pasando. ⏭️ no aplicado el reemplazo de `add`/`it_works` por un `//!`: borra el único test del crate y cambia el conteo del ROADMAP, fuera del alcance de este paso (sin cambios en tests); queda a decisión del equipo.
