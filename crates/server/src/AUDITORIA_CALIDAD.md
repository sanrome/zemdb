# Auditoría de calidad — `crates/server/src/` (archivos de nivel superior)

**Fecha:** 12 de octubre de 2026 · **Commit:** `7bcb6bb` · **Alcance:** `config.rs`, `error.rs`, `durable.rs`, `dedup.rs`, `schema_registry.rs`, `blocking.rs`, `fail_point.rs`, `lib.rs`, `main.rs` y sus tests unitarios `tests/{config,durable,error,schema_registry,blocking}.rs` (no `tests/relay.rs`).

## Resumen

- Código en buen estado: errores con `thiserror`, durabilidad correcta (tmp + `sync_all` + `rename` + `sync_dir`, `create_dir_all_synced` sincroniza cada padre), RAII para el lock de escritura de schemas y comentarios que explican el *por qué*. No hay referencias a códigos de auditoría ni narrativa histórica en el alcance.
- La mayor oportunidad está en **`config.rs`** (565 líneas): 23 funciones `default_*` de una línea + 24 atributos `#[serde(default…)]` + un `impl Default` que las vuelve a enumerar (Q1), y 24 bloques `if let Some(x) = …? { self.f = x; }` casi idénticos en `apply_overrides_from` (Q2). Entre las dos se van ~150 líneas sin cambiar comportamiento.
- Resto del código fuente: recortes chicos y seguros (bloque `cfg(not(unix))` muerto en `durable.rs`, `Path::ancestors`, aplanar el escaneo de `SchemaRegistry::new`, inlinear `add_column_locked`).
- Tests: la tabla de `tests/error.rs` ocupa 5 líneas por caso y cubre 9 de 24 variantes; en `tests/config.rs` hay 4 tests de secretos y 2 de rangos de lifecycle que caben en las tablas que ya existen; en `tests/schema_registry.rs` el setup "tempdir + registry + schema `todo`" se repite 5 veces.
- **Ahorro estimado:** ~180–205 líneas en código fuente (≈13–15 % de 1 369) y ~95–120 en tests (≈13–16 % de 749): **~275–325 líneas en total (≈13–15 % del alcance)**.

## Métricas

| Archivo | Total | Código | Doc | Coment. | Blancos | Ahorro estimado |
|---|---:|---:|---:|---:|---:|---:|
| `config.rs` | 565 | 405 | 87 | 2 | 71 | ~150–170 |
| `error.rs` | 215 | 163 | 19 | 0 | 33 | ~0–2 |
| `schema_registry.rs` | 178 | 127 | 24 | 7 | 20 | ~12–15 |
| `main.rs` | 113 | 96 | 4 | 2 | 11 | ~2–4 |
| `durable.rs` | 95 | 67 | 18 | 1 | 9 | ~10–12 |
| `fail_point.rs` | 79 | 55 | 16 | 0 | 8 | 0 |
| `dedup.rs` | 62 | 38 | 15 | 1 | 8 | ~0–2 |
| `blocking.rs` | 31 | 12 | 16 | 0 | 3 | ~3 |
| `lib.rs` | 31 | 30 | 0 | 0 | 1 | 0 |
| **Fuente** | **1 369** | | | | | **~180–205** |
| `tests/config.rs` | 371 | 343 | 1 | 1 | 26 | ~50–65 |
| `tests/schema_registry.rs` | 189 | 163 | 0 | 6 | 20 | ~15–20 |
| `tests/error.rs` | 89 | 84 | 0 | 0 | 5 | ~30 (sin ampliar cobertura) |
| `tests/durable.rs` | 56 | 42 | 0 | 0 | 14 | 0 |
| `tests/blocking.rs` | 44 | 36 | 0 | 3 | 5 | 0 |
| **Tests** | **749** | | | | | **~95–120** |

## Comentarios

**Densidad y calidad.** Hay pocos comentarios y casi todos aportan: explican decisiones (por qué un solo lock global en `SchemaRegistry`, por qué se ignora el poison, por qué `LruCache::unbounded`, por qué `sync_dir` no hace nada fuera de Unix, por qué `block_in_place` y no `spawn_blocking`). No hay códigos de auditoría (C-xx, DEF-xx, M-xx) ni narrativa del tipo "antes hacíamos X" (lo verifiqué con grep). Lo que sí hay:

- **Doc-comments que repiten la firma** (recorte opcional, 0–1 línea cada uno): `error.rs:124` ("Maps the ServerError to the protocol ErrorCode."), `error.rs:154`, `dedup.rs:48` y `:53` (`len`/`is_empty`), `schema_registry.rs:132` ("Retrieves an Arc reference…"), `schema_registry.rs:170` ("Lists all registered SchemaIds."), `config.rs:384` y `:389` (`from_toml_str`/`from_file`). Son de una línea; si se dejan no molestan, pero no agregan nada.
- **Doc desactualizado o incompleto:** `config.rs:497` ("Load configuration with optional TOML file and automatic environment variable overrides.") no menciona que `load_with_env` también **valida** (`config.validate()?` en `:504`), que es lo más importante de esa función. Propuesta: "Loads the TOML file (or the defaults), applies the environment overrides and validates the result."
- **Doc demasiado largo:** `config.rs:314-320` (7 líneas) enumera las seis constantes por nombre; los intra-doc links ya están en cada constante. Cabe en 3: "Checks that `max_snapshot_bytes`, the HTTP limits and every server default of the room lifecycle policy lie within their ranges (the `*_RANGE` constants and the ranges room overrides must meet). The error names the setting and its environment variable." (~-4).
- **Duplicado entre dos docs:** "An empty variable counts as unset" aparece en `config.rs:398` (`apply_env_overrides`) y otra vez, con el motivo, en `:413-414` (`apply_overrides_from`). Además la línea `:399` mide 130 columnas (rustfmt no reformatea comentarios). Dejar el motivo en un solo lugar y partir la línea (~-1/-2).
- **Docs de campos que se solapan:** `body_read_timeout_secs` (`config.rs:107-110`) y `body_min_rate_bytes_per_sec` (`:114-117`) explican los dos el mecanismo "un segundo más por cada N bytes". Se puede explicar una vez en `body_read_timeout_secs` y dejar en el otro "Minimum body rate that extends the deadline (bytes/s, default 32 KiB/s); see `body_read_timeout_secs`." (~-2).
- **Docs que listan llamadores y pueden quedar viejos:** `durable.rs:70-71` ("the room actor saving its roster or `log_meta.json`, the admin API"). Basta con "Callers are often async, so the blocking I/O runs through [`blocking_io`]." (~-1).
- `config.rs:191` "/// Seven days, in seconds." sobre `SEVEN_DAYS_SECS` repite el nombre; desaparece con Q1.
- `blocking.rs:3-7`: el primer párrafo del doc de módulo enumera cinco tipos de I/O; se puede resumir en dos líneas ("most of what an actor does is synchronous file I/O, and every commit ends in an `fsync`; run on a worker thread it stalls the other tasks of that worker"). El segundo párrafo (`:9-15`, `block_in_place` vs `spawn_blocking` y el pánico en current-thread) **hay que conservarlo** (~-3).

**A conservar sin cambios:** `schema_registry.rs:15-21` (por qué un solo lock), `:37-38`, `:48-50` (qué archivos se saltean y por qué uno corrupto sí falla), `:87-89` (poison ignorado), `:98-101` (por qué no se reemplazan schemas), `:107`; `durable.rs:10-16` (Windows/NTFS), `:50`, `:66-69`; `dedup.rs:6-7` y `:18`; `error.rs:39`, `:56-86` (semántica de cada variante, contrato con el cliente); `config.rs:68-70`, `:101-103`, `:121-124` (`ulimit -n`), `:283-287` (por qué se rechazan los secretos de desarrollo), `:375-376`; `main.rs:56-57`, `:66-67`, `:106-107`; todo `fail_point.rs:1-12`.

## Sugerencias

### Q1. `config.rs`: un solo `#[serde(default)]` a nivel de struct en lugar de 23 funciones `default_*`

- **Dónde:** `config.rs:13-125` (24 atributos), `:129-226` (funciones `default_*` y `SEVEN_DAYS_SECS`), `:228-257` (`impl Default`).
- **Problema:** cada valor por defecto se escribe tres veces: la función, el atributo que la nombra y el campo de `impl Default` que la vuelve a llamar. Las 11 funciones de lifecycle construyen `RoomLifecyclePolicy::default()` una vez cada una.
- **Propuesta:** `#[serde(default)]` en el struct (serde toma los campos que faltan de `ServerConfig::default()`), borrar los atributos por campo y las funciones, y escribir los valores directamente en `Default`. Los dos secretos de desarrollo pasan a constantes porque `validate_secrets` los compara:
  ```rust
  /// Built-in development secrets. They are public, so `validate_secrets` rejects them.
  const DEV_AUTH_SECRET: &str = "default_auth_secret_dev_32bytes!";
  const DEV_ADMIN_SECRET: &str = "default_admin_secret_dev_32bytes!";

  #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
  #[serde(default)]
  pub struct ServerConfig { /* campos y docs sin atributos */ }

  impl Default for ServerConfig {
      fn default() -> Self {
          let policy = RoomLifecyclePolicy::default();
          Self {
              host: "127.0.0.1".to_string(),
              port: 8080,
              data_dir: PathBuf::from("./data"),
              auth_secret: DEV_AUTH_SECRET.to_string(),
              admin_secret: DEV_ADMIN_SECRET.to_string(),
              ram_max_ops: policy.ram_max_ops,
              ram_ttl_secs: policy.ram_ttl.as_secs(),
              // … resto de lifecycle igual …
              dormant_after_secs: None,
              idle_timeout_secs: policy.idle_timeout.map_or(0, |idle| idle.as_secs()),
              dedup_lru_capacity: 10_000,
              snapshot_ttl_secs: 7 * 24 * 60 * 60,
              max_snapshot_bytes: 512 * 1024 * 1024,
              header_read_timeout_secs: 10,
              body_read_timeout_secs: 60,
              body_min_rate_bytes_per_sec: 32 * 1024,
              max_connections: 10_000,
          }
      }
  }
  ```
  `check_secret(…, &self.auth_secret, DEV_AUTH_SECRET)` en `:289-298`. Los valores son exactamente los mismos, incluido `dormant_after_secs: None` (hoy `#[serde(default)]` = `None` en `:71` y `None` en `:244`). En `tests/config.rs:18,21` cambia `&default_admin_secret()` por `DEV_ADMIN_SECRET` (y lo mismo con auth). Grep: ninguna función `default_*` se usa fuera de `config.rs` y sus tests (`tests/auth_tests.rs:36` usa el literal, no la función).
- **Ahorro estimado:** ~110–115 líneas · **Riesgo:** bajo (es el idioma estándar de serde; el TOML vacío sigue dando `ServerConfig::default()`, lo cubre `tests/config.rs:323`) · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — los 24 campos ya tienen default hoy (23 `default = "…"` + `dormant_after_secs` con `#[serde(default)]`), no hay `deny_unknown_fields`, así que ningún TOML pasa de rechazado a aceptado; `impl Default` llama a las mismas funciones, valores idénticos. Matiz: los usos en tests están en `src/tests/config.rs:18,21` (y `:323`), no en `crates/server/tests/`.

### Q2. `config.rs`: un helper `set` para los 24 overrides de entorno

- **Dónde:** `config.rs:420-493`.
- **Problema:** 24 bloques de 3 líneas con la misma forma `if let Some(v) = …? { self.field = v; }` (20 con `parse_var` y 4 con `var`). El nombre de la variable temporal (`cap`, `secs`, `bytes`, `max`, `rate`…) no aporta.
- **Propuesta:**
  ```rust
  /// Replaces `slot` with `value`, if set.
  fn set<T>(slot: &mut T, value: Option<T>) {
      if let Some(value) = value {
          *slot = value;
      }
  }
  // …
  set(&mut self.host, var("ZEMDB_HOST")?);
  set(&mut self.port, parse_var(&var, "ZEMDB_PORT")?);
  set(&mut self.data_dir, var("ZEMDB_DATA_DIR")?.map(PathBuf::from));
  // …
  set(&mut self.ram_ttl_secs, parse_var(&var, RamTtl.env_var())?);
  set(&mut self.dormant_after_secs, parse_var(&var, DormantAfter.env_var())?.map(Some));
  ```
  Se mantiene el orden de lectura y el primer `?` que falla sigue cortando en la misma variable (importa para qué error se informa). Algunas líneas largas (`ZEMDB_BODY_MIN_RATE_BYTES_PER_SEC`) las va a partir rustfmt; ya está contemplado en el ahorro.
- **Ahorro estimado:** ~35–45 líneas · **Riesgo:** bajo · **Prioridad:** alta
- **Verificación (2.ª pasada):** ✅ confirmada — `parse_var` es genérico en `T: FromStr`; con `.map(Some)` infiere `u64` para `dormant_after_secs`; se conserva el orden y el corte en el primer `?`. Compila (el `&mut self.campo` no choca con el closure `var`).

### Q3. `durable.rs`: bloque `cfg(not(unix))` muerto y `Path::ancestors`

- **Dónde:** `durable.rs:28-31` y `:38-46`.
- **Problema 1:** `#[cfg(not(unix))] { let _ = dir_path; }` evita un warning de parámetro sin usar que no puede darse: `dir_path` ya se usa en todas las plataformas en `injected("sync_dir", dir_path)?` (`:18`).
- **Problema 2:** el `while` con `match current.parent()` recorre a mano lo que hace `Path::ancestors` (que es justamente `parent()` repetido).
- **Propuesta:**
  ```rust
  let missing: Vec<&Path> = path
      .ancestors()
      .take_while(|dir| !dir.as_os_str().is_empty() && !dir.is_dir())
      .collect();
  ```
  Equivalente: para `a/b` da `a/b`, `a` y se corta en `""`; para una ruta absoluta se corta en el primer ancestro que existe y, si llega a `/`, `ancestors` termina igual que el `None => break` de hoy. Opcional: las dos llamadas `sync_dir(x.parent().unwrap_or_else(|| Path::new("")))` (`:54`, `:85`) pueden ir a un `fn sync_parent(path: &Path)`; ahorra poco pero nombra la intención.
- **Ahorro estimado:** ~10 líneas · **Riesgo:** bajo · **Prioridad:** media

### Q4. `schema_registry.rs`: aplanar el escaneo de `SchemaRegistry::new`

- **Dónde:** `schema_registry.rs:39-73`.
- **Problema:** se llama dos veces a `path.is_file()` y a `path.extension().and_then(..)`, y el caso normal queda a 5 niveles de indentación (`for` → `if` → `if let` → `match`).
- **Propuesta:**
  ```rust
  for entry in fs::read_dir(&dir)? {
      let path = entry?.path();
      if !path.is_file() {
          continue;
      }
      match path.extension().and_then(|s| s.to_str()) {
          Some("tmp") => {
              fs::remove_file(&path)?;
              continue;
          }
          Some("json") => {}
          _ => continue,
      }
      let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
          continue;
      };
      // (comentario de :48-50)
      let schema_id = match SchemaId::new(stem) { /* igual que hoy */ };
      let schema: Schema = serde_json::from_str(&fs::read_to_string(&path)?).map_err(|e| {
          ServerError::Serialization(format!("Failed to parse schema JSON from {}: {e}", path.display()))
      })?;
      schemas.insert(schema_id, Arc::new(schema));
  }
  ```
  Mismos efectos en el mismo orden (borrar `.tmp`, saltear nombres inválidos con `warn!`, fallar con `Serialization` si el JSON está corrupto) y el mismo texto de error.
- **Ahorro estimado:** ~5–7 líneas, y dos niveles menos de indentación · **Riesgo:** bajo · **Prioridad:** media

### Q5. `schema_registry.rs`: inlinear `add_column_locked`

- **Dónde:** `schema_registry.rs:138-168`.
- **Problema:** `add_column_locked` tiene un único llamador y existe solo para que el closure de `blocking_io` sea corto. `register_schema` (`:108-114`) ya pone su lógica dentro del closure, así que las dos funciones públicas quedan con estilos distintos.
- **Propuesta:** mover el cuerpo dentro del closure: `let _write = self.lock_writes();` y después `get_schema` → `clone` → `fail_point::hook` → `add_column` → `persist`, en el mismo orden. El `hook` sigue después de leer y con el lock tomado, que es lo que necesita el test `add_column_waits_for_a_concurrent_add_column_on_the_same_schema`.
- **Ahorro estimado:** ~7 líneas · **Riesgo:** bajo · **Prioridad:** baja

### Q6. `config.rs`: recortar docs (ver "Comentarios")

- **Dónde:** `config.rs:107-117`, `:314-320`, `:396-401`/`:412-414`, `:497`.
- **Problema:** docs largos, duplicados o incompletos (detalle en "Comentarios").
- **Propuesta:** las cuatro reescrituras de la sección de comentarios; la de `:497` corrige un doc incompleto.
- **Ahorro estimado:** ~8 líneas · **Riesgo:** nulo · **Prioridad:** media

### Q7. `config.rs`: `max_snapshot_bytes` con `check_range` (**a verificar**, cambia el texto del error)

- **Dónde:** `config.rs:322-328` y constantes `:259-263`.
- **Problema:** es la única comprobación de rango escrita a mano; las otras cuatro usan `check_range`.
- **Propuesta:** `pub const MAX_SNAPSHOT_BYTES_RANGE: RangeInclusive<u64> = 1 << 20..=64 << 30;` y `check_range("max_snapshot_bytes (ZEMDB_MAX_SNAPSHOT_BYTES)", self.max_snapshot_bytes, MAX_SNAPSHOT_BYTES_RANGE)?`. **Cambia el mensaje**: se pierden "(1 MiB)" y "(64 GiB)". El único test (`tests/config.rs:78`) busca solo `max_snapshot_bytes` y grep no encuentra "must be between" fuera de `config.rs`, pero hay que decidir si el mensaje cuenta como contrato. Además `MIN_MAX_SNAPSHOT_BYTES`/`MAX_MAX_SNAPSHOT_BYTES` son `pub` en un módulo `pub`: reemplazarlas cambia la API (no tienen usos fuera de `config.rs`).
- **Ahorro estimado:** ~5 líneas · **Riesgo:** medio (mensaje y API) · **Prioridad:** baja
- **Verificación (2.ª pasada):** ✅ confirmada — `1 << 20..=64 << 30` tiene la precedencia correcta y entra en `u64`; el mensaje pierde "(1 MiB)"/"(64 GiB)" tal como se advierte; ningún test compara esos textos; las constantes solo se usan en `config.rs`.

### Q8. `main.rs`: temporales innecesarios y log duplicado

- **Dónde:** `main.rs:18-21`, `:43-51`.
- **Problema:** `schemas_dir`/`snapshots_dir` se usan una sola vez, y el mensaje de arranque sale dos veces (log y stdout).
- **Propuesta:** `SchemaRegistry::new(config.data_dir.join("schemas"))?` y `SnapshotRelay::new(config.data_dir.join("snapshots"), …)` sin `schemas_dir`/`snapshots_dir` (~-2). Usar `{addr}` en lugar de `"{}", addr` en `:47-48` y `:51`. El `println!` de `:51` repite el `info!` de `:43-49`; grep no encontró ningún script ni test que lea esa línea de stdout, pero quitarlo cambia la salida → **a verificar** (o dejar un comentario que explique para qué está).
- **Ahorro estimado:** ~2–4 líneas · **Riesgo:** bajo · **Prioridad:** baja

### Q9. Visibilidad `pub` sin usos externos (**a verificar**, API pública)

- **Dónde:** `error.rs:13` (`RETRY_AFTER_SECS`), `config.rs:259-276` (constantes de rango), `config.rs:10` (`Serialize` en `ServerConfig`: no encontré ningún `to_string`/`to_string_pretty` de la config en el workspace).
- **Problema:** constantes y derives públicos que nadie usa fuera del módulo amplían la API sin necesidad.
- **Propuesta:** pasar las constantes a `pub(crate)` y evaluar si `Serialize` hace falta. No ahorra líneas; reduce superficie pública. Como `config` y `error` son módulos `pub` del crate, puede haber consumidores externos que no se ven con grep.
- **Ahorro estimado:** 0 · **Riesgo:** medio (API) · **Prioridad:** baja
- **Verificación (2.ª pasada):** ✅ confirmada — `RETRY_AFTER_SECS` solo se usa en `error.rs:189`; las constantes de rango solo en `config.rs`; no hay serialización de `ServerConfig` en el workspace. El riesgo de API externa sigue siendo la única duda.

## Lo que conviene dejar como está

- **`error.rs` entero.** Ya usa `thiserror`; los mensajes `#[error(..)]` y los dos `match` (`to_error_code`, `to_status_code`) son contrato observable (las integraciones comparan textos y códigos HTTP). Se podría derivar el status desde `ErrorCode` y ahorrar ~4 líneas en el grupo `Io | Wal | … => 500`, pero `ErrorCode` tiene `TableAlreadyExists`, que ningún `ServerError` produce, así que habría que agregar un brazo sin sentido. Dos `match` exhaustivos sobre `ServerError` son más claros y el compilador obliga a mapear cada variante nueva. El `From<InvalidIdError>` manual hace falta (convierte a `BadRequest(String)`, no envuelve el error).
- **Los cuatro `check_range` de `config.rs:329-348`.** Mezclan `u64` y `usize`, así que una tabla necesitaría conversiones o trait objects; las 20 líneas se leen bien.
- **`config.rs:356-371` (`lifecycle_settings`) y `:374-382` (`default_lifecycle_policy`).** Reutilizan la validación y la mezcla de overrides de la política de rooms en vez de duplicarlas; el comentario de `:375-376` explica una sutileza real.
- **`parse_var` y `check_secret`.** Cortos y claros. El orden de `check_secret` (vacío → valor de desarrollo → largo) da el mensaje más útil primero, y ningún mensaje incluye el secreto (`tests/config.rs:52`).
- **`fail_point.rs`.** `check` y `hook` repiten el "buscar `(name, path)` y sacarlo" sobre dos `Vec` de tipos distintos; un helper genérico ahorraría ~2 líneas a cambio de más tipos. Los `#[cfg(not(test))] let _ = (name, path);` sí hacen falta (en builds sin test los parámetros no se usan).
- **`blocking.rs`.** 8 líneas de código y un doc de módulo largo pero justificado: es el único punto de entrada a I/O bloqueante desde código async y el doc explica el pánico de `block_in_place` en current-thread (solo se puede recortar el primer párrafo, ver "Comentarios").
- **`dedup.rs`.** `is_empty` lo exige clippy (`len_without_is_empty`) para un tipo `pub` con `len`; el campo `capacity` hace falta porque `LruCache::unbounded` no limita.
- **`durable.rs::write_atomic_blocking`.** El bloque `{ … }` que cierra `tmp_file` antes del rename y el orden write → `sync_all` → fail point → rename → `sync_dir` son la garantía de crash-safety; no se tocan.
- **`main.rs::os_shutdown_signal`/`ctrl_c`.** Las ramas por plataforma y el `pending()` cuando no se puede instalar el handler son comportamiento deliberado y están documentados.
- **`lib.rs`.** Son solo módulos y re-exports.

## Tests

**`tests/config.rs` (371 líneas, ~50–65 de ahorro):**
- `:15-49`: cuatro tests (`built_in_development_secrets_are_rejected`, `empty_and_short_secrets_are_rejected`, `identical_auth_and_admin_secrets_are_rejected`, `strong_distinct_secrets_are_accepted`) con el mismo `assert!(config_with(a, b).validate_secrets().is_err())` partido por rustfmt en 3 líneas. Se pueden juntar en una tabla `for (auth, admin) in [(DEV_AUTH_SECRET, STRONG_ADMIN), ("", STRONG_ADMIN), (&short, STRONG_ADMIN), (STRONG_AUTH, STRONG_AUTH), …] { assert!(…is_err(), "{auth:?}/{admin:?}") }` más el caso aceptado (~-17). De paso convendría comprobar **qué** error sale (hoy cualquier `Err` pasa, incluso uno equivocado).
- `:112-156`: `snapshot_demand_ttl_below_a_minute_is_rejected` y `zero_dormancy_timeout_is_rejected` repiten el patrón de la tabla `lifecycle_defaults_outside_their_range_fail_naming_the_variable` (`:249-284`), que ya tiene un caso de `dormant_after_secs`. Los rechazos se pueden pasar a esa tabla y los valores aceptados a un solo test "límites aceptados" (~-20/-25).
- `:97-99` sobra: `:323` ya comprueba `from_toml_str("") == ServerConfig::default()` completo (~-4).
- `strong_config()` (`:108`) se define después de usar `config_with` y lejos de ella; conviene ponerla junto a `config_with` (`:4`).
- `7 * 24 * 60 * 60` aparece 4 veces (`:94,95,99,127`) y los límites de `max_snapshot_bytes` están como literales en `:76,80`: usar una constante local y `MIN_MAX_SNAPSHOT_BYTES ± 1`.
- **Huecos de cobertura:** la lista de `empty_environment_values_count_as_unset` (`:289-311`) no incluye `ZEMDB_RAM_MAX_BYTES`, `ZEMDB_SEGMENT_MAX_BYTES` ni `ZEMDB_FORGET_DORMANT_AFTER_SECS`, y `unparsable_environment_values_fail_naming_the_variable` (`:171-190`) tampoco los tiene. Los tests unitarios no comprueban que se apliquen `ZEMDB_HOST`, `ZEMDB_PORT`, `ZEMDB_DATA_DIR`, los dos secretos, `ZEMDB_DEDUP_LRU_CAPACITY`, `ZEMDB_SNAPSHOT_TTL_SECS` ni `ZEMDB_MAX_SNAPSHOT_BYTES`. Con Q2 un error de copiar y pegar en esas líneas (asignar al campo equivocado) no lo detectaría ningún test unitario.
- `load_with_max_snapshot_bytes` (`:61-72`) llama a `load_with_env`, que lee el entorno real del proceso: si CI exporta alguna `ZEMDB_*`, el test cambia de resultado. Es el único test de `from_file`/`load_with_env`, así que se queda, pero conviene saberlo.

**`tests/error.rs` (89 líneas, ~30 de ahorro):** cada caso de la tabla ocupa 5 líneas (`:6-52`). Con un helper `let e = |variant: fn(String) -> ServerError| variant(String::new());` cada caso cabe en una línea: `(e(ServerError::Forbidden), ErrorCode::Forbidden, StatusCode::FORBIDDEN),`. Con ese formato conviene **completar la tabla con las 24 variantes**. Hoy cubre 9, y faltan justamente varias del contrato HTTP (`RoomNotFound`→404, `BehindCompaction`→410, `RoomLocked`→423, `RateLimited`→429, `SnapshotSuperseded`→409, `InvalidSequence`→400). Aun completa queda más corta que ahora.

**`tests/schema_registry.rs` (189 líneas, ~15–20 de ahorro):**
- Setup repetido: `tempdir()` + `SchemaId::new("todo").unwrap()` + `SchemaRegistry::new(..)` + `register_schema(id.clone(), test_schema())` aparece en `:25-28`, `:45-50`, `:73-78`, `:93-96` y `:153-156`, y `dir.path().join("todo.json")` 4 veces. Un fixture `fn todo_registry() -> (TempDir, SchemaId, SchemaRegistry)` más `fn todo_path(dir: &TempDir) -> PathBuf` ahorra ~10–12 líneas.
- `ColumnDef::new(name, DataType::Int).nullable(true)` aparece 4 veces (`:34`, `:116`, `:131`, `:168`) → `fn nullable_int(name) -> ColumnDef`.
- `has_priority` (`:15-21`) se puede generalizar a `has_column(schema, name)` y reusar en `:144-146` y `:180-185`.
- Los dos tests de concurrencia (`:91-148`, `:150-189`) son largos pero se justifican: uno fuerza el entrelazado exacto con el hook y el otro es una prueba de carga con barrera. Ninguno es redundante.

**`tests/durable.rs`:** compacto. Detalle de nombre: `create_dir_all_synced_syncs_the_parent_of_each_new_component` (`:49-56`) solo comprueba el primer componente; un nombre más preciso sería `..._fails_when_syncing_a_new_parent_fails`.

**`tests/blocking.rs`:** bien; los tres tests cubren los tres caminos de `blocking_io` (sin runtime, current-thread, multi-thread con un solo worker) y los comentarios explican qué se prueba. No tocar.
