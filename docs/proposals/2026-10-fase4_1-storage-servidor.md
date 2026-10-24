# Storage y servidor para el cliente — Plan de la sub-fase 4.1

**Fecha:** 23 de Octubre de 2026
**Tipo:** Plan de sub-fase (`docs/METODOLOGIA.md` §1). Documento vivo: lotes y seguimiento.
**Estado:** En curso: Lotes 0 a 3 cerrados; el próximo es el 4.
**Plan de la fase:** [`2026-10-fase4-plan.md`](2026-10-fase4-plan.md), §4.1. **Metodología:** [`docs/METODOLOGIA.md`](../METODOLOGIA.md).
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) y [`ARCHITECTURE.md`](../../ARCHITECTURE.md). Reglas de código: [`GEMINI.md`](../../GEMINI.md).

---

## 0. Resumen

- **Qué.** Lo que el SDK necesita de `zemdb-core`, `zemdb-storage` y `zemdb-server` antes de escribir su núcleo (4.2): el dueño como prefijo del `ClientId` (DC15), `Schema::evolves_from` y `reload_schema` (DC5), el `schema_id` en las respuestas (DC5), los plazos del servidor con el reloj de Tokio (DC16), la variante de error de un snapshot que no cumple las restricciones y los errores de E/S al abrir (DC12), Zstd en wasm32 con `ruzstd` (DC9) y la capa de archivos del motor en disco (DC8, DC17).
- **Dónde.** Todo en código que ya está en el alcance auditado (`INVARIANTES.md` §1.4): cada lote tiene su revisión de delta, y la sub-fase cierra con una revisión de cierre (plan de la fase, §5.1).
- **Qué no.** Nada de `zemdb-client`: el núcleo es 4.2. El motor del navegador sobre el OPFS es 4.5; acá solo la capa de archivos que lo hace posible y su implementación nativa.
- **Decisión nueva:** DC17 (la forma de la capa de archivos), con el spike S3, adoptada el 23 de Octubre de 2026.

---

## 1. Decisiones

Las de la fase que implementa esta sub-fase, todas adoptadas: DC5, DC8, DC9, DC12, DC15 y DC16 (§3 del plan de la fase). Esta sub-fase agrega una:

| ID | Decisión | Lotes | Estado |
|---|---|---|---|
| **DC17** | **La capa de archivos del motor en disco.** Ver abajo. | 7 a 9 | Adoptada (23 de Octubre de 2026), con renombrar un archivo abierto como operación del trait |

**DC17 · La capa de archivos.** Un trait asíncrono de `zemdb-storage`, `RoomFiles` (nombre provisorio), que representa el directorio de datos del motor y es lo único del motor en disco que toca el sistema de archivos. El motor (`DiskStorageEngine`) pasa a ser genérico sobre él, con la implementación nativa por defecto. `DiskStorageEngine::new(options)` queda en el `impl` de la implementación nativa (un parámetro de tipo por defecto no participa de la inferencia, así que un `new` genérico no compilaría en las llamadas de hoy), y otro constructor recibe la capa. Quien usa el motor con `DiskStorageOptions` no cambia nada; `WalWriter::write_batch` y `write_record`, públicos, cambian de firma (dejan de tomar un `tokio::fs::File`), lo que antes del primer release no requiere compatibilidad (`GEMINI.md`).
- **Operaciones del directorio**, una por cada forma en que el motor abre o crea un archivo hoy:
  - abrir con un lock exclusivo, creándolo si falta (el `.lock` y el WAL de la sala al abrirla);
  - crear con un lock exclusivo un archivo que no existe, y fallar si existe (el WAL nuevo de la rotación, que nunca adopta un archivo con ese nombre, y los `.snap.tmp.*`, que se bloquean al crearlos);
  - crear truncando (`.wal.compacting.tmp`, que hoy se crea con `File::create`);
  - abrir uno existente para leer (el `.snap`, `.wal.compacting`);
  - saber si existe un archivo; listar los nombres con un prefijo (los `.snap.tmp.*` que deja una compactación interrumpida); renombrar reemplazando el destino; borrar; crear el directorio de datos; y sincronizar el directorio (no-op donde no existe, como hoy en Windows).
- **Operaciones de un archivo abierto:** leer y escribir en un offset (`read_at`, `write_at`), su largo, truncarlo (`set_len`), `sync_data` y `sync_all`. E/S posicional, sin cursor: es la forma del `FileSystemSyncAccessHandle` (`{ at }`) y evita la posición compartida de los handles clonados que hoy usa la recuperación para buscar frames.
- **Por qué asíncrono:** en el OPFS, obtener un handle, renombrar (`move()`), borrar y listar son promesas; solo leer, escribir, truncar y `flush()` son síncronos. En nativo, cada operación corre en el pool bloqueante (`run_blocking`), como hoy `tokio::fs`, pero devuelve su error en la misma llamada: desaparece la sutileza de `write_all` de Tokio, que entrega los bytes al pool y reporta el error recién en el próximo `flush` (hoy, `sync_file` y `WalWriter::write_flushed`).
- **Una operación soltada no aterriza después de reabrir (INV-S1, INV-L7).** `apply_batch` no corre en una tarea propia: si se suelta su future con una escritura en el pool bloqueante, `PendingWrite` marca la sala fallida, y quien la cierra y la reabre corre contra esa escritura. Hoy lo impiden el lock del handle del WAL, que retiene la operación en vuelo hasta que termina (una reapertura da `RoomLocked`), y el `flush` del `tokio::fs::File` en `close_room`, que la espera. La implementación nativa de la capa conserva los dos mecanismos: cada operación lleva al pool bloqueante un `Arc` del archivo, con su lock, y un permiso del handle, así que el lock se suelta recién cuando termina la última operación, aunque nadie la espere; y cerrar el handle (o el `sync` de `close_room`) espera ese permiso. El permiso guarda el error de la operación que nadie esperó y lo devuelve el próximo `sync` o el cierre del handle, como hoy el `flush` de Tokio: `close_room` sigue devolviendo el error de una escritura de un `apply_batch` soltado. El Lote 7 lo prueba.
- **Renombrar un archivo abierto (S3).** En el OPFS, `move()` y `removeEntry()` fallan con `NoModificationAllowedError` si el origen o el destino tienen un `FileSystemSyncAccessHandle` abierto. Hoy el motor renombra un solo archivo con su handle abierto, el WAL, en dos lugares y con el mismo handle: la rotación de la compactación pasa `room_{id}.wal` a `.wal.compacting` y sigue con el handle viejo, que apunta al archivo renombrado; y si abrir el WAL nuevo falla, la vuelta atrás (`roll_back_rotation`) renombra `.wal.compacting` de vuelta, con ese handle todavía abierto. Todo lo demás (los snapshots en staging, la absorción, los borrados de `.wal.compacting` y de los temporales, la instalación de un snapshot) ya se hace sin handles abiertos en el origen ni en el destino (`compactor.rs`, `recovery.rs`). Por eso el trait tiene dos formas de renombrar:
  - **renombrar por ruta,** del directorio, sobre archivos sin handles abiertos ni en el origen ni en el destino, que es como el motor ya renombra y borra todo lo que no es el WAL;
  - **renombrar un archivo abierto,** sobre el handle, que lo sigue después del rename: la rotación y su vuelta atrás, que pasa a recibir el handle del WAL en lugar de solo las rutas. En nativo es el `rename` de hoy. En el OPFS, la implementación cierra el handle de acceso, hace `move()` y lo reabre sobre el mismo `FileSystemFileHandle`, que sigue siendo válido después de su `move()` (S3, pregunta 4); el lock de la sala impide que otra pestaña tome el archivo en el medio.

  Así la rotación y su vuelta atrás tienen la misma lógica en las dos plataformas, y la capa no cambia ningún comportamiento del motor en nativo. Lo que difiere es el error: en nativo, `rename` es todo o nada y el handle sigue válido, por eso la rotación propaga el error de su rename sin marcar la sala fallida (la marca si después falla la vuelta atrás o el sync del directorio); en el OPFS, cerrar, mover y reabrir tiene estados intermedios (el `move()` hecho y la reapertura fallida, o al revés), que pueden dejar un handle muerto. Es un requisito de la implementación del OPFS, en 4.5: un handle muerto hace fallar toda operación siguiente y nunca escribe en otro archivo, así que la próxima operación que escriba en el WAL (un `apply_batch`, una compactación o el truncado de un `apply_snapshot`) marca la sala fallida y la recuperación pliega `.wal.compacting`.
- **Lo que no cambia:** el formato (`ZEM1`, frames `0xBA7C` con CRC32, `ZMSN`), el protocolo de instalación de snapshots (temporal, sync, rename, sync del directorio, purga), la recuperación y sus decisiones, los locks del motor (`compaction`, `wal`, `state`) y el de la sala, las tareas propias de `open_room`, `apply_snapshot` y la compactación, y los fail points, que siguen identificados por ruta (el de `read_only_handle`, que hoy reproduce el error diferido de un `tokio::fs::File`, conserva su nombre y su paso, pero no su mecanismo: el Lote 7 lo adapta). No cambia ningún comportamiento: lo único que se reimplementa es el mecanismo del lock del handle del WAL (y el error diferido de una escritura que nadie esperó), con el mismo efecto.
- **Lo que queda para 4.5:** la implementación sobre el OPFS, con el renombrar de un archivo abierto (cerrar, `move()` y reabrir); cómo corren en wasm32 las tareas propias del motor (hoy `tokio::spawn`) y el trabajo de CPU que hoy va al pool bloqueante; compilar en wasm32 el motor en disco, con la compresión del `.snap` (que desde el Lote 9 pasa por la función del Lote 6, y en wasm32 es `ruzstd` `Fastest`, DC9) y la validación de `zstd_level`, que hoy usa `zstd::compression_level_range()`; y los supuestos del OPFS en `INVARIANTES.md` §2 (`flush()` vale como `fsync`, ya en DC11; que `move()` sobre un destino existente sea atómico ante un corte, sin especificar, como `flush()`). En el Lote 9, la compactación comprime con la función de compresión del Lote 6 en lugar de llamar a `zstd::encode_all`, para que en 4.5 no haya otro camino.
- **Alternativas descartadas:** que la rotación cierre el handle antes de renombrar y lo reabra (la primera versión de esta decisión), que cambiaba la rotación y su vuelta atrás en nativo sin necesidad, porque en el OPFS la misma operación se puede resolver dentro de la implementación; un trait síncrono, que no puede abrir ni renombrar en el OPFS sin bloquear el Worker sobre una promesa; y un motor del navegador escrito aparte, que duplicaría la recuperación y la compactación, la parte con más casos de crash del storage.

**S3 (spike de DC17, 23 de Octubre de 2026).** Preguntas, desde un Worker dedicado y sin Rust:
1. ¿`FileSystemFileHandle.move()` sobre un archivo existente lo reemplaza, con la forma `move(nombre)` y con `move(directorio, nombre)`?
2. ¿Se puede mover un archivo que tiene un `FileSystemSyncAccessHandle` abierto? ¿Y sobre un destino que lo tiene?
3. ¿Se puede borrar (`removeEntry()`) un archivo con un handle abierto?
4. ¿Un `FileSystemFileHandle` sigue siendo válido después de su `move()`?

Respuestas en Chromium 153 headless sobre Linux (Playwright 1.63):
1. Sí, con las dos formas: el destino tiene el contenido nuevo y el origen desaparece.
2. No: `move()` falla enseguida con `NoModificationAllowedError` si el origen tiene un handle abierto ("cannot be moved while it is locked"), y también si lo tiene el destino ("cannot be moved to a destination which is locked"); en los dos casos no cambia nada.
3. No: falla con `NoModificationAllowedError` y no lo borra.
4. Sí: apunta al archivo con su nombre nuevo.

No se probó qué deja `move()` si el navegador se corta en el medio. **Pendiente:** S3 en Safari y Firefox, con las preguntas 1 a 3, antes de cerrar 4.5 (con S1 en Firefox, DC8). El código del spike no se mergea.

---

## 2. Lotes

| Orden | Lote | Crates | Decisiones | Invariantes | Progreso |
|---|---|---|---|---|---|
| 0 | Contrato | Documentos | DC5, DC9, DC12, DC15, DC16, DC17 | Nueva: S7. Ajustes: S4, S5, S6, C1, C2, E2, W1, W2, A5, K3, K6, K12 | ✅ |
| 1 | El dueño como prefijo del `ClientId` y `Schema::evolves_from` | core, server | DC15, DC5 | A5, K6, C1 | ✅ |
| 2 | El `schema_id` en `CommitAck`, `SyncBatch` y `HeartbeatAck` | core, server | DC5 | E2, W1, W2, B1, K6 | ✅ |
| 3 | Los plazos del servidor con el reloj de Tokio | server | DC16 | B2, R3, L1 | ⬜ |
| 4 | Los errores de la apertura y de `apply_snapshot` | storage | DC12 | S4, S5, C2, W1, K3 | ⬜ |
| 5 | `reload_schema` | storage | DC5 | S7, C1, C2, C3, S6, K6 | ⬜ |
| 6 | Zstd en wasm32 con `ruzstd` | storage | DC9 | W1, W2, S5, K12 | ⬜ |
| 7 | Capa de archivos: el trait y su implementación nativa | storage | DC8, DC17 | S1 a S6, L7 | ⬜ |
| 8 | Capa de archivos: la recuperación | storage (y core, si cambia `find_valid_frame`) | DC17 | S3, S4, S6 | ⬜ |
| 9 | Capa de archivos: la compactación y `apply_snapshot` | storage | DC17 | S1, S3, S5 | ⬜ |
| 10 | Cierre | Documentos | Todas | Todas las tocadas | ⬜ |

**Orden.**
- El Lote 0 va primero: solo documentos, con su revisión cruzada (`docs/METODOLOGIA.md` §2).
- Los Lotes 1 a 3 tocan `core` y `server`; el 4 en adelante, `storage`. El 1 va antes del 5, que usa `evolves_from`, y el 4 antes del 5, que en disco usa la variante de DC12. El 2 y el 3 son independientes del resto.
- El 6 es independiente de los demás, pero va antes de la capa de archivos para no mezclar su delta con ella.
- Los Lotes 7 a 9 dependen de DC17 y van en ese orden: tocan los mismos archivos de `crates/storage/src/disk/`. Son un refactor sin cambio de comportamiento: unas 105 líneas de `disk/` tocan hoy el sistema de archivos, en unas 20 funciones, así que el cambio es de unas 200 a 250 líneas nuevas (el trait y la implementación nativa) y unas 150 a 250 cambiadas. El corte es tentativo: lo fija el Lote 7 con la forma final del trait, y si el 8 y el 9 juntos entran en ~300 líneas, van en un solo lote.
- El 10 va al final: la revisión de cierre de la sub-fase (plan de la fase, §5.1).

**Checklist propio.** Los Lotes 1, 4, 5 y 6 tocan cómo decide o qué acepta una réplica: el revisor de delta recorre además el checklist de determinismo de la Fase 3.11 (§5.4 de su [propuesta](2026-10-fase3_11-restricciones-proposal.md)). Los Lotes 7 a 9 recorren, en su lugar, este checklist de crash-safety:
- ninguna operación de archivo cambia de orden respecto de las demás, ni de lugar respecto de los locks;
- una operación soltada nunca aterriza después de que la sala se cierra o se reabre (INV-S1, INV-L7);
- cada `sync` que había sigue, sobre el mismo archivo o directorio, y ningún resultado de E/S se descarta (`GEMINI.md`, principio 3);
- cada fail point sigue en el mismo paso, con el mismo nombre;
- las imágenes de crash (`fail_point::arm_crash_plan`) y la simulación del disco (`disk/tests/simulation.rs`) siguen pasando sin cambios en sus expectativas.

### Lote 0 · Contrato · ✅ Hecho
**Objetivo:** el contrato contra el que se revisa cada lote de 4.1. Solo documentos.

**Qué se hace:**
- **Este plan,** con DC17 como *Propuesta* y la respuesta de S3.
- **`INVARIANTES.md`, versión 7:**
  - **INV-S7, nueva** (§3.2), 🆕 (4.1, Lote 5): `reload_schema` solo cambia a un esquema que evoluciona del de la sala, y lo hace de una vez o no cambia nada.
  - **INV-S4, INV-S5 e INV-C2:** un snapshot cuyas filas no cumplen las restricciones es la variante propia de DC12, no `SnapshotCorruption`, y un error de E/S al leer los archivos de la sala al abrirla es `Io` (🆕, Lote 4).
  - **INV-W1:** la variante de DC12 en lugar de `SnapshotCorruption`; el decodificador `ruzstd` de wasm32, con el mismo tope que el de `zstd` (🆕, Lotes 4 y 6).
  - **INV-W2:** lo que codifica `ruzstd` lo decodifica `zstd` nativo a los mismos bytes, y al revés (🆕, Lote 6).
  - **INV-E2:** las respuestas llevan además el `schema_id` (🆕, Lote 2).
  - **INV-C1:** en "Sostenida por", la réplica solo recarga un esquema que evoluciona del suyo (`reload_schema`, INV-S7), y una sala en disco abierta con un esquema más angosto que el de su log queda fuera de su premisa.
  - **INV-S6:** la excepción de una sala en disco recargada después de abrirse con un esquema más angosto que el de su log (🆕, Lote 5).
  - **INV-A5, INV-K3, INV-K6 e INV-K12:** el lote de 4.1 que implementa cada parte.
  - **§1.6:** las decisiones de la Fase 4 llegan a DC17.
  - **§4:** DEF-34 y DEF-54 con el lote que los cierra.
- **El plan de la fase:** DC17 en la tabla de decisiones, como *Propuesta*, y 4.1 en curso.
- **`ROADMAP.md`:** "Lo que sigue" pasa a 4.1, con el enlace a este plan.
- `ARCHITECTURE.md` no cambia: describe comportamiento implementado.

**INV-S7** (Medio si se rompe): su enunciado, cómo se sostiene y cómo intentar romperla están en `INVARIANTES.md` §3.2, que es la única versión.

**Entrega:** `docs: plan Fase 4.1 with its batches and the file layer proposal, and adjust the contract for it (Fase 4.1, Lote 0)`.

**Hecho.**
- **Documentos:** este plan, con DC17 *Propuesta* y S3; `INVARIANTES.md` en la versión 7 (INV-S7 nueva; ajustes de INV-S4, S5, S6, C1, C2, E2, W1, W2, A5, K3, K6 y K12, y DEF-34 y DEF-54 en §4 con su lote); DC17 en la tabla del plan de la fase y una precisión en DC5; `ROADMAP.md` con 4.1 en curso.
- **Revisión cruzada** (agentes aparte, de contexto fresco y solo lectura), en cinco rondas, cada una acotada a las correcciones de la anterior:
  1. **2 bloqueantes, 13 ajustes y 5 menores.** DC17 reemplazaba el lock del handle del WAL por esperar cada operación, que no cubre una escritura de un `apply_batch` soltado (INV-S1, INV-L7): ahora cada operación nativa retiene el archivo, su lock y un permiso hasta terminar. El Lote 5 construía los índices fuera del lock de la sala en memoria (INV-C3). Los ajustes: la API pública que cambia (`WalWriter`, el `new` de un motor genérico), las formas de abrir y crear archivos que faltaban en DC17, la vuelta atrás de la rotación, `find_valid_frame` en `zemdb-core`, el tope de una operación en el Lote 2, los tests que citaba el Lote 4, la reserva de 64 MiB, la medición pendiente en Safari, la fila de DC17 en el plan de la fase, los documentos de los Lotes 7 a 9, lo que queda para 4.5 y el fail point `read_only_handle`.
  2. **1 bloqueante, 1 ajuste y 4 menores.** Una sala en disco abierta con un esquema más angosto conserva valores en las columnas "nuevas", así que los índices de las restricciones nuevas no nacen vacíos: el Lote 5 se parte por motor (en memoria se agregan vacíos; en disco se construyen y se verifican desde las filas, bajo el mutex del WAL). También: los tests y fuzzers del Lote 4, el error de una operación soltada que guarda el permiso de DC17, la función de compresión del Lote 6.
  3. **1 bloqueante, 2 ajustes y 3 menores.** En disco, después de reabrir con un esquema más angosto, verificar el estado final no restituye las decisiones del replay: la equivalencia de INV-S7 vale solo si la sala nunca se abrió así (el SDK abre siempre con el último esquema guardado, DC5), y si no, garantiza INV-C2 e INV-C3; DC5 anota la precisión; la recarga en disco publica solo el esquema y los datos (la fase 3 de la compactación escribe el estado sin el mutex del WAL).
  4. **2 bloqueantes, 1 ajuste y 2 menores,** de redacción: INV-S6 necesitaba la excepción de esa recarga, y la premisa tiene que hablar del log, también lo compactado en el `.snap`; los tests fuerzan una compactación antes de la reapertura angosta.
  5. **Verde,** con 4 menores de redacción, corregidos.
- **Desvíos:** INV-S6 y la precisión de DC5 (adoptada) no estaban en la lista inicial del lote; las dos salieron de la revisión cruzada.

### Lote 1 · El dueño como prefijo del `ClientId` y `Schema::evolves_from` (`zemdb-core`, `zemdb-server`) · ✅ Hecho
**Objetivo:** las dos reglas de `zemdb-core` que el resto de 4.1 y el SDK usan: quién es el dueño de un cliente (DC15) y cuándo un esquema puede reemplazar a otro en una réplica (DC5).

**Archivos:** `crates/core/src/id.rs`, `crates/core/src/schema/global.rs` y `table.rs`; tests de `zemdb-core` y de `zemdb-server` (el commit con un dueño). `ARCHITECTURE.md` §3.1.

**Qué se hace:**
- **`ClientId::owner(&self) -> &str`:** la parte anterior al primer `#`, o el id entero si no tiene `#` o si alguna de las dos partes queda vacía (`#x` y `x#` son su propio dueño). `Schema::check_owner` compara el dueño declarado con `client.owner()`, byte a byte, en lugar de con el id entero. Nada más en el servidor cambia (roster, cursor, lease, dedup, designación, subidas y SSE siguen por `ClientId`).
- **`Schema::evolves_from(&self, older: &Schema) -> bool`:** `self.extends(older)` y, para cada tabla de `older`:
  - las `UNIQUE` y las FK de `older` son un prefijo de las de la tabla nueva, iguales y sobre las mismas posiciones;
  - el dueño (columna e identidad) y la inmutabilidad no cambian;
  - toda restricción después de ese prefijo incluye alguna columna nueva de la tabla (de índice mayor o igual que la cantidad de columnas de `older`).

  Las tablas nuevas pueden traer cualquier restricción válida (todas sus columnas son nuevas), también FK hacia tablas viejas. `referenced_by` no se compara: puede crecer. Es reflexiva y transitiva.
- `evolves_from` no reemplaza a la regla de la reasignación del servidor (`extends` y `has_same_constraints`, Lote 6b de la Fase 3.11), que es más estricta: esa es del servidor sobre su log, y esta, de una réplica sobre su estado.

**Tests exigidos.**
- **Dueño:** `owner()` de `alice`, `alice#phone`, `bob#y#z` (dueño `bob`), `#x`, `x#`, `#` y un id con caracteres no ASCII antes del `#`. En el servidor, un dispositivo `alice#phone` commitea con dueño `alice` (se acepta) y con dueño `alice#phone` (`SchemaViolation`); `bob#alice` no puede declarar `alice`; un id sin `#` sigue como antes.
- **`evolves_from`, un negativo por regla:** una columna o una tabla menos, o con otro tipo (falla `extends`); una `UNIQUE` o una FK quitada, cambiada o en otra posición; otra columna dueño u otra identidad; la inmutabilidad cambiada en cualquier sentido; una `UNIQUE` nueva o una FK nueva solo sobre columnas viejas.
- **Positivos:** el mismo esquema; otra versión sin cambios; una columna nueva con `UNIQUE` y con FK que la incluyen (`add_column_with_constraints`); una tabla nueva con restricciones y una FK hacia una tabla vieja; una FK nueva que entra a una tabla vieja.
- **Transitividad:** con el generador de esquemas de `crates/core/tests/common`, cadenas aleatorias de `add_column`, `add_column_with_constraints` y tablas nuevas: cada esquema evoluciona de todos los anteriores. Y la regla de la reasignación implica `evolves_from`.

**Interacciones a revisar:**
- `Schema::check_owner` en el commit (`RoomActor`), también en un reintento deduplicado: el dedup sigue por `(ClientId, MutationId)`.
- `apply_operation` en las réplicas compara el dueño de la fila con el que declara la operación: no cambia (INV-A5).
- Los tests y fixtures del servidor que usan un `ClientId` con `#`.
- `has_same_constraints`, `extends` y `same_constraints`: `evolves_from` se apoya en ellos sin cambiarlos.

**Documentos:** `ARCHITECTURE.md` §3.1 (el dueño como prefijo, y `evolves_from` junto a la evolución del esquema); `INVARIANTES.md`: INV-A5 sin 🆕, y en INV-C1 e INV-K6 la parte de `evolves_from`.

**Hecho.**
- **API.** `ClientId::owner` (`split_once('#')`, con las dos partes no vacías; si no, el id entero), y `Schema::check_owner` compara con él. `Schema::evolves_from` y `TableSchema::evolves_from` (`pub(crate)`): `extends`, las `UNIQUE` y las FK de la tabla vieja como prefijo (`starts_with`; la igualdad incluye nombre, columnas, `references` y `on_delete`), el mismo `OwnerDef` y la misma inmutabilidad, y toda restricción posterior con alguna posición mayor o igual que la cantidad de columnas viejas.
- **Tests agregados:**
  - `crates/core/tests/schema_owner_tests.rs`: `a_device_writes_as_the_person_before_the_first_hash` (dispositivos de `alice`, `bob#alice`, `bob#y#z`, `#x`, `x#`, `#`, NFC contra NFD en la parte de persona).
  - `crates/core/tests/schema_replica_evolution_tests.rs`, nuevo: un negativo por regla (tabla o columna quitada, columna con otro tipo, `UNIQUE` quitada, sobre otras columnas, renombrada o en otro orden, FK quitada, en otro orden o sobre otras columnas, otro dueño, dueño quitado o agregado, inmutabilidad en los dos sentidos, `UNIQUE` y FK nuevas solo sobre columnas viejas, también al lado de una columna nueva); positivos (el mismo esquema, `add_column` y `add_column_with_constraints`, una tabla nueva con FK hacia las viejas, que agranda `foreign_keys_referencing`); y cadenas aleatorias de evoluciones: cada esquema evoluciona de todos los anteriores, ninguno vuelve atrás, y la regla de la reasignación implica `evolves_from` (300 casos; 20.000 en la versión `#[ignore]`).
  - `crates/server/tests/owner_commit_tests.rs`: `the_devices_of_a_person_write_as_the_person_before_the_first_hash` y `the_devices_of_a_person_are_deduplicated_apart`.
  - Contra el esqueleto (`owner` devolvía el id entero y `evolves_from` era `extends`) fallaban por comportamiento los dos del servidor, el de dispositivos de core y los dos negativos de restricciones; los positivos y las cadenas pasaban, como corresponde a un esqueleto que ya cumple `extends`.
- **Mutaciones verificadas** (todas detectadas): `extends` en `true`; sacar el prefijo de las `UNIQUE`, el de las FK, la igualdad del dueño, la de la inmutabilidad y cada una de las dos reglas de "columna nueva"; comparar las `UNIQUE` sin orden (la detectó el negativo de `UNIQUE` en otro orden, agregado por la revisión de delta); cada una de las dos condiciones de no vacío de `owner()`; y `check_owner` con el id entero. La revisión de delta probó además, también detectadas: las FK sin orden, `>` en lugar de `>=`, contra el largo de la clave, saltear las tablas viejas sin restricciones, aceptar también el id entero y el dedup por dueño.
- **Interacciones revisadas:**
  - `check_owner` en el commit del actor: el dedup por `(ClientId, MutationId)` va antes, así que un reintento reconocido se confirma sin volver a pasar por el chequeo, como antes, y sin secuenciar nada; dos dispositivos son dos clientes para el dedup (test).
  - `apply_operation` en las réplicas compara el dueño de la fila con el que declara la operación: no cambia (`cargo test --workspace` en verde).
  - Tokens y autenticación atan el `ClientId` entero: no cambian.
  - Roster, cursor, lease, designación, subidas, el tope de dos streams SSE y la parte del presupuesto de respuestas siguen por `ClientId`, así que son por dispositivo y una persona los suma: DC15 lo acepta (INV-B5), sin cambio de código.
  - Los tests y fixtures del servidor con un `ClientId` con `#`: no había ninguno fuera de los nuevos.
  - `extends`, `has_same_constraints` y `same_constraints`: `evolves_from` se apoya en `extends` sin cambiarlo, y la implicación desde la regla de la reasignación está probada en las cadenas. Las posiciones no se comparan: bajo `extends`, las columnas viejas están en las mismas posiciones, así que restricciones y dueños iguales por nombre son iguales por posición (una mutación sobre esa comparación no se podía detectar, y se sacó).
  - `evolves_from` no tiene todavía un consumidor de producción (el Lote 5 y el SDK).
  - `OwnerIdentity::ClientId` conserva su nombre; sus docstrings, las de `OwnerDef` y `try_with_owner`, el mensaje de `OwnerMismatch` y el comentario del commit en el actor dicen ahora que es el dueño del cliente.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura): sin hallazgos Altos ni Medios. Se atendieron los Bajos (el negativo de `UNIQUE` en otro orden; los textos viejos sobre el dueño) y las notas (el SDK "will check" en `ARCHITECTURE.md`; el paso 5 de los documentos; las interacciones de arriba). Queda como nota que un id `alice #x` tiene el dueño `alice ` con el espacio: lo cuida el backend.
- **Chequeos:** fmt, clippy, test, doc y el build de wasm32 en verde; `cargo test --release -- --ignored` de `zemdb-core` y de `zemdb-server`.
- **Tamaño:** unas 60 líneas de producción, más docstrings.
- **Desvíos:** ninguno.

### Lote 2 · El `schema_id` en `CommitAck`, `SyncBatch` y `HeartbeatAck` (`zemdb-core`, `zemdb-server`) · ✅ Hecho
**Objetivo:** que el cliente se entere de una reasignación por cualquier respuesta, no solo por el registro (DC5). Las versiones son por esquema, y dos esquemas pueden tener la misma.

**Archivos:** `crates/core/src/protocol/messages.rs` y `limits.rs`; `crates/server/src/actor/command.rs`, `room.rs` y `api/data_plane.rs`; los tests de codificación y los fuzzers de mensajes.

**Qué se hace:**
- `schema_id: SchemaId` al final de cada una de las tres variantes (bincode es posicional; el orden de las variantes no cambia). `PROTOCOL_VERSION` sigue en `0x01` (`GEMINI.md`).
- El actor lo pone en `CommitResponse`, `SyncBatchResponse` y `HeartbeatResponse` con el `schema_id` de la sala, el mismo que da a `Registered` y `Schema`.
- `RESPONSE_ENVELOPE_ALLOWANCE` suma el `schema_id` (su largo y hasta `MAX_PATH_ID_LEN` bytes), para que un `CommitAck` o un `SyncBatch` al tope de operaciones siga entrando en un frame. Como de ese margen sale `MAX_RESPONSE_OPS_BYTES`, que es también el tope con que `check_operation_size` acepta una operación en un commit, ese tope baja en lo mismo (73 bytes), para el servidor y para el pre-chequeo del cliente.
- Los comentarios de `schema_version` dicen que un id o una versión distintos de los del cliente son un cambio de esquema.

**Tests exigidos.**
- Ida y vuelta de las tres variantes con `schema_id` (`protocol_codec_tests`), y su lugar en los fuzzers de mensajes (INV-W1).
- Un `CommitAck` y un `SyncBatch` con operaciones al tope de `MAX_RESPONSE_OPS_BYTES` entran en `MAX_MESSAGE_SIZE` con todos los demás campos al máximo: un id de sala y un `schema_id` de 64 bytes, y `correlation_id`, secuencias y versión en `u64::MAX`. Negativo: sin el cambio en `RESPONSE_ENVELOPE_ALLOWANCE`, ese mensaje no entra (con campos chicos entraría igual, porque el margen ya sobreestima la cuenta de operaciones).
- `check_operation_size` rechaza una operación de `MAX_RESPONSE_OPS_BYTES` más un byte con el tope nuevo.
- En el servidor: después de reasignar una sala a otro esquema con la misma versión, las tres respuestas traen el `schema_id` nuevo; después de un `add_column`, el mismo id con la versión nueva.

**Interacciones a revisar:**
- El presupuesto de respuestas (`response_budget`) y el tamaño de cada frame.
- `check_operation_size` y el tope de una operación en un commit, que baja con el margen.
- `Registered` y `Schema`, que ya llevan el id: los cinco mensajes dicen lo mismo en un mismo estado del actor.
- La recarga de esquema del Lote 6c de la Fase 3.11, que no cambia el id de la sala.
- INV-E2: la versión sigue llegando por las mismas respuestas.

**Documentos:** `ARCHITECTURE.md` §7.2 (los campos de las tres respuestas y el tope de una operación) y §3.1 (*Schema Version*); `INVARIANTES.md`: INV-E2 sin 🆕, y en INV-K6 la parte del `schema_id`.

**Hecho.**
- **Wire.** `schema_id: SchemaId` al final de `CommitAck`, `SyncBatch` y `HeartbeatAck`, con docstrings que dicen que las versiones son por esquema y que un id o una versión distintos de los del cliente son un cambio de esquema. `RESPONSE_ENVELOPE_ALLOWANCE` suma el largo y los 64 bytes del `schema_id` (de 132 a 205 bytes), así que `MAX_RESPONSE_OPS_BYTES`, y con él el tope de `check_operation_size`, baja 73 bytes.
- **Servidor.** `CommitResponse` (el commit nuevo y el reintento deduplicado), `SyncBatchResponse` y `HeartbeatResponse` llevan el `schema_id` del actor, el mismo que da a `Registered` y `Schema`; `api/data_plane.rs` lo copia a los mensajes.
- **Tests agregados o ampliados:**
  - `crates/core/tests/protocol_codec_tests.rs`: las idas y vueltas de las tres variantes con `schema_id`; `response_envelope_allowance_covers_every_field_but_the_operations` (un `CommitAck` y un `SyncBatch` sin operaciones y con todo lo demás al máximo, ids de 64 bytes incluidos, codifican en a lo sumo el margen); `largest_accepted_operation_fits_alone_in_any_response`, con un id de sala y un `schema_id` de 64 bytes y `correlation_id`, secuencias y versión en `u64::MAX`, y el negativo: con una operación del tamaño que permitiría el margen sin el `schema_id`, ninguna de las dos respuestas entra; una operación de `MAX_RESPONSE_OPS_BYTES` más un byte sigue rechazada; y `a_response_with_an_invalid_schema_id_is_malformed`.
  - Los generadores de mensajes de `crates/core/tests/common` traen un `schema_id` aleatorio, así que los fuzzers del decoder y las idas y vueltas aleatorias lo cubren (INV-W1, INV-W2); el test de largos declarados de `decoder_fuzz_tests.rs` ajusta su offset.
  - `crates/server/src/actor/tests/manager.rs`: `every_response_reports_the_schema_a_room_was_reassigned_to`, una sala reasignada entre dos esquemas con la misma versión: el registro, el commit, su reintento, el sync y el heartbeat dan el id nuevo.
  - `crates/server/tests/server_integration_tests.rs`: `schema_evolution_cascades_to_active_room` comprueba por HTTP, después de un `add_column`, el mismo id con la versión nueva en `Registered`, `Schema`, `HeartbeatAck`, `SyncBatch` y `CommitAck`.
  - Contra el esqueleto (los campos sin el cambio del margen, y el actor con un id fijo) fallaban por comportamiento los dos tests de tamaño de core y los dos del servidor.
- **Mutaciones verificadas** (detectadas): el margen sin el término del `schema_id`; cada uno de los cuatro lugares del actor (commit nuevo, reintento, sync, heartbeat) con otro id; cada una de las tres copias de `data_plane.rs` con otro id. Sobrevive sacar solo los 9 bytes del largo (dejando los 64): el margen sobreestima los varints (el tag de la variante, el largo de los dos ids y la cuenta de operaciones), así que el sobre más grande real mide 177 bytes contra 205; la revisión de delta midió que una rebaja de hasta 28 bytes, de cualquier término, sobrevive, y de 29 la detectan los dos tests de tamaño. Esa holgura no se puede detectar sin fijar la constante.
- **Interacciones revisadas:**
  - El presupuesto de respuestas: cada respuesta presupuestada reserva `MAX_FRAME_SIZE`, y la más grande sigue entrando (test de tamaño). `HeartbeatAck` no se presupuesta: crece a lo sumo 65 bytes.
  - `check_operation_size` y el tope de una operación en un commit, que baja 73 bytes; el presupuesto de lectura de `tiered_log` y el catch-up del actor usan `MAX_RESPONSE_OPS_BYTES` y siguen en verde, también los tests de respuestas grandes de `http_edge_tests.rs`.
  - `Registered` y `Schema`: el `schema_id` del actor no cambia mientras vive (una reasignación pasa con el actor detenido, en `RoomManager::get_or_spawn`), así que los cinco mensajes dicen lo mismo (tests del manager y de integración).
  - La recarga del Lote 6c de la Fase 3.11: ignora la de otro esquema y no cambia el id; después de un `add_column` el id es el mismo con la versión nueva (test de integración).
  - INV-E2: la versión sigue llegando por las mismas respuestas.
  - Decodificar una respuesta: `SchemaId` se valida al deserializarse, así que un id inválido es `Malformed` (test).
  - El reintento de un commit secuenciado antes de una reasignación devuelve el `assigned_seq` original con el id nuevo: es lo correcto (el cliente se entera del cambio), y lo arma el mismo código del reintento que cubre el test del manager.
  - Los logs escritos antes de este cambio: con el tope anterior se pudo aceptar una operación de entre `MAX_MESSAGE_SIZE − 205` y `MAX_MESSAGE_SIZE − 132` bytes de wire, y ninguna lectura vuelve a pasar `check_operation_size`. Con ids de sala y de esquema de 64 bytes y secuencias grandes, esa operación ya no entra sola en una respuesta, y el sync de esa secuencia no se podría codificar. Antes del primer release no hay logs que conservar (`GEMINI.md`), así que no lleva código.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura): en verde, sin hallazgos Altos ni Medios. Probó además mutaciones propias, todas detectadas (el id del reintento y el del `HeartbeatAck` de `data_plane.rs`, y el margen 44 y 46 bytes de más o 29 de menos). Su Bajo (los logs anteriores al cambio) y sus notas (el reintento a través de una reasignación; la holgura de 28 bytes) quedan en esta nota.
- **Chequeos:** fmt, clippy, test, doc y el build de wasm32 en verde; `cargo test --release -- --ignored` de `zemdb-core` y de `zemdb-server`.
- **Tamaño:** unas 15 líneas de producción, más docstrings.
- **Desvíos:** ninguno.

### Lote 3 · Los plazos del servidor con el reloj de Tokio (`zemdb-server`) · ✅
**Objetivo:** que el arnés de 4.2 controle todos los plazos del servidor con el reloj pausado (DC16, plan de la fase §5.3).

**Archivos:** `crates/server/src/actor/lease.rs`, `snapshot_demand.rs` y `room.rs`; `crates/server/src/log/hot_buffer.rs` y `tiered_log.rs`.

**Qué se hace:** `std::time::Instant` pasa a `tokio::time::Instant` en el lease del roster, la demanda y la designación de snapshots, y la retención de la HotBuffer (los únicos usos que quedan en el servidor; el actor, el relay y los límites HTTP ya usan el de Tokio). La conversión del roster entre `Instant` y hora de pared para persistirlo (`instant_before`, la restauración de la demanda) se hace con el mismo reloj. Lo que mide la hora de pared (TTL del log, tokens, el roster y la demanda persistidos, la recuperación del relay) no cambia.

**Tests exigidos.** Con el reloj pausado (`start_paused`) y `tokio::time::advance`, sin dormir de verdad: un cliente sin heartbeat pasa a `Disconnected` al vencer el lease; la designación de un snapshot vence a los 60 s y pasa a otro cliente; la HotBuffer suelta lo que pasó su ventana. Los tres fallan con `std::time::Instant` (el reloj pausado no lo mueve). Negativos: un instante antes de cada plazo, el cliente sigue `Connected`, la designación sigue y la HotBuffer conserva lo suyo.

**Interacciones a revisar:**
- La persistencia del roster y de la demanda (`meta_room.json`): un `Instant` restaurado desde una edad, también una edad mayor que lo que el reloj puede representar hacia atrás.
- `apply_sliding_window_at` y los tests que ya pasan un `Instant` a mano.
- Los tests del servidor que ya usan el reloj pausado.
- En producción, `tokio::time::Instant` es el `Instant` del sistema: ningún plazo cambia.

**Documentos:** ninguno, salvo que `ARCHITECTURE.md` §5 nombre el reloj de algún plazo.

**Hecho.**
- **Servidor.** `tokio::time::Instant` en lugar de `std::time::Instant` en `lease.rs` (`ClientEntry::last_heartbeat`, el registro, la actividad, los timeouts y la conversión del roster entre `Instant` y hora de pared al guardarlo y al leerlo), `snapshot_demand.rs`, `room.rs` (la renovación, la restauración y el vencimiento de la demanda, y la designación), `hot_buffer.rs` (la llegada de cada operación y la ventana que aplica cada append) y `tiered_log.rs` (`MaintenanceClock::mono`, la ventana que aplica el mantenimiento). En `crates/server/src` no queda ningún plazo con `std::time::Instant`: los únicos usos son cronómetros de tests que miden tiempo real. Nada de lo que mide la hora de pared cambia. `ClientEntry::last_heartbeat` es público y cambia de tipo, lo que antes del primer release no requiere compatibilidad (`GEMINI.md`).
- **Tests agregados**, todos con el reloj pausado y `tokio::time::advance`, sin dormir de verdad. Cada uno empieza adelantando el reloj de Tokio una hora, así que un plazo medido con el reloj del sistema vencería enseguida o nunca:
  - `crates/server/src/actor/tests/room.rs`, sobre un actor abierto sin correr su loop, cuyos ticks corre el test: `a_client_without_heartbeats_disconnects_when_its_lease_expires_on_the_tokio_clock` (el lease cuenta desde el último heartbeat: el cliente sigue `Connected` al cumplirse y pasa a `Disconnected` 1 ms después); `a_designation_expires_after_60_seconds_on_the_tokio_clock_and_passes_to_another_client` (1 ms antes de los 60 s sigue el mismo designado; a los 60 s, otro); `a_snapshot_demand_expires_on_the_tokio_clock_also_after_the_room_reopens` (vence a su TTL y no 1 ms antes, y una demanda restaurada al reabrir vence a lo que le quedaba y no 1 s antes).
  - `crates/server/src/actor/tests/lease.rs`: `inactivity_survives_a_save_and_a_reopen_on_the_tokio_clock` (60 s de inactividad guardados y restaurados: el lease de 90 s no vence a los 89 s y vence a los 91 s).
  - `crates/server/src/log/tests/tiered_log.rs`: `the_ram_window_expires_on_the_tokio_clock` (un append y el mantenimiento conservan lo que estuvo en RAM 1 ms menos que la ventana y sueltan lo que estuvo la ventana entera).
  - Contra el esqueleto (el código anterior), los cinco fallaban por comportamiento, en su parte positiva; los negativos pasaban.
- **Mutaciones verificadas** (todas detectadas): cada uno de los doce usos, de a uno, devuelto al reloj del sistema (`Instant::from_std(std::time::Instant::now())`): el guardado, el registro, la actividad, los timeouts y la lectura del roster; la renovación, la restauración y el vencimiento de la demanda, y la designación; la llegada a la HotBuffer, la ventana del append y la del mantenimiento. El default de serde de `last_heartbeat` no se puede observar (`LoadedClient::into_entry` lo pisa), así que no lleva test.
- **Interacciones revisadas:**
  - La persistencia del roster: el guardado y la lectura convierten con el mismo reloj (test del roster). `instant_before`, con una edad mayor que lo que el reloj puede representar hacia atrás, sigue saturando: el `checked_sub` de Tokio delega en el de `std` (`activity_before_the_clock_can_reach_saturates`).
  - La restauración de la demanda: el plazo se calcula hacia adelante desde el `now` de Tokio (test de la demanda).
  - `apply_sliding_window_at`, `MaintenanceClock::after` y los tests que arman un `Instant` a mano: fuera de un runtime, el `now` de Tokio es el del sistema, y siguen en verde.
  - Los tests del servidor que ya usaban el reloj pausado: avanzarlo ahora mueve también los leases, la designación, la demanda y la ventana de RAM, y todos siguen en verde, también los de integración que avanzan el reloj con `sleep`. La simulación de onboarding tenía la demanda y la designación en 100 ms de tiempo real, con un `sleep` real de 5 ms por tick: pasan a 40 s del reloj de Tokio (diez ticks) y el `sleep` se va. Su evento que vence el snapshot del relay (`advance` de 2000 s) ahora vence también esos plazos; la simulación sigue cubriendo descargas, huecos y podas por TTL, y la revisión de delta comprobó que dos corridas dan las mismas estadísticas.
  - El relay y la designación, ahora en el mismo reloj: la inactividad y el tope de las subidas y el TTL del snapshot ya usaban el de Tokio, y la designación, que no cambia mientras el designado sube (INV-B2), ahora también.
  - El trabajo bloqueante (`blocking_io`, `spawn_blocking`) lee el mismo reloj que el runtime, porque el de Tokio lo busca por `Handle::try_current`; `RoomActor::open` corre dentro de `blocking_io`.
  - `pick_uploader` compara actividades, todas del mismo reloj.
  - En producción, sin el reloj pausado, el `Instant` de Tokio es el del sistema (`test-util` es una feature de las dev-dependencies, y el resolver 2 no la lleva al binario): ningún plazo cambia.
- **Para el arnés de 4.2** (§5.3 del plan de la fase, corregido en este lote): del roster, solo el tiempo entre que se guarda y la sala se reabre cuenta en hora de pared; la demanda se guarda como la hora de su última renovación, así que con el reloj pausado vuelve de una reapertura con casi todo su TTL. Y mientras corre una tarea de `spawn_blocking`, el reloj pausado no avanza solo: Tokio 1.53 inhibe el avance automático (`runtime/blocking/schedule.rs`), lo que responde la duda que §5.3 dejaba para el comienzo de 4.2.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura, con sus propias corridas en una copia): en verde, sin hallazgos Altos ni Medios. Probó además mutaciones propias en la restauración de la demanda y en los timeouts, detectadas. Su Bajo (dos comentarios de la simulación que quedaban cortos) está corregido; sus notas quedan en esta nota.
- **Chequeos:** fmt, clippy, test, doc y el build de wasm32 en verde; `cargo test --release -- --ignored` de `zemdb-server`, con las cuatro simulaciones de onboarding.
- **Tamaño:** unas 15 líneas de producción, más docstrings.
- **Desvíos:** ninguno, salvo la corrección de §5.3 del plan de la fase, que el lote no listaba entre sus documentos.

### Lote 4 · Los errores de la apertura y de `apply_snapshot` (`zemdb-storage`) · ⬜
**Objetivo:** que el SDK pueda decidir por el error si descarta la réplica (DC12, INV-K3): un snapshot que no cumple las restricciones y un error de E/S no son un daño.

**Archivos:** `crates/storage/src/error.rs`, `memory/state.rs`, `disk/recovery.rs`, `engine.rs` (la documentación del trait) y `fail_point.rs`.

**Qué se hace:**
- **`StorageError::SnapshotConstraints { table_id: u16, constraint: String }`:** la que devuelve la verificación de las filas de un snapshot contra las restricciones (`RoomData::build_checked`), en `open_room` (el `.snap` local) y en `apply_snapshot` (el de otro miembro), en los dos motores. El resto de los errores de un snapshot no cambia: una tabla fuera del esquema o una fila inválida siguen siendo `SnapshotCorruption`.
- **`Io` al abrir:** en `load_snapshot`, un error al leer el header o el cuerpo del `.snap` es `Io` (el largo ya se verificó antes de leer, bajo el lock de la sala, así que un error de lectura no es un archivo corto). Hoy sale como `WalCorruption` o `SnapshotCorruption`. Los errores del WAL ya salen como `Io`. Un fail point nuevo en esa lectura, solo en tests.

**Tests exigidos.**
- En los dos motores, `apply_snapshot` de un snapshot con una `UNIQUE` repetida y con una FK colgante: `SnapshotConstraints` con la tabla y la restricción, y la sala sin cambios.
- `open_room` de un `.snap` local que no cumple las restricciones del esquema con que se abre: `SnapshotConstraints`, y los archivos intactos.
- Negativos: una fila inválida y una tabla fuera del esquema siguen siendo `SnapshotCorruption`; un bit invertido en el `.snap`, `SnapshotCorruption` o `WalCorruption` como hoy.
- Con el fail point, un error de lectura del header y otro del cuerpo del `.snap`: `Io`, y los archivos intactos.

**Interacciones a revisar:**
- Los tests que hoy esperan `SnapshotCorruption` por las restricciones: `crates/storage/src/memory/tests/index.rs`, `crates/storage/tests/disk_wal_resilience_tests.rs` y `check_unchecked_states` en `crates/storage/tests/randomized_engine_tests.rs` (los de `apply_snapshot_tests.rs` lo esperan por tablas o filas inválidas, y siguen igual). Los fuzzers de snapshots de `randomized_engine_tests.rs` aceptan una lista cerrada de errores y corren con un esquema con restricciones: la lista suma `SnapshotConstraints`.
- El servidor no abre salas del storage (solo valida snapshots en el relay por su envelope): verificarlo.
- El checklist de determinismo: la restricción nombrada es la misma en los dos motores (tablas por id, filas por clave).

**Documentos:** `ARCHITECTURE.md` §8 (*apply_snapshot Rules*, *Opening Requires a Schema That Covers the Room's Data*) y §3.1 (*Enforcement*); `INVARIANTES.md`: INV-S4, S5, C2 y W1 sin 🆕 en lo de este lote, y en INV-K3 la parte de 4.1.

### Lote 5 · `reload_schema` (`zemdb-storage`) · ⬜
**Objetivo:** cambiar el esquema de una sala abierta sin cerrarla (DC5, DEF-34), con INV-S7.

**Archivos:** `crates/storage/src/engine.rs`, `error.rs`, `memory/mod.rs`, `memory/state.rs`, `memory/index.rs` (los campos de `TableIndexes` son privados: agregar índices vacíos necesita un método ahí) y `disk/mod.rs`.

**Qué se hace:**
- **`StorageEngine::reload_schema(&self, room_id: &RoomId, schema: Schema) -> Result<(), StorageError>`,** en los dos motores.
- **Validar primero:** la sala abierta (`RoomNotFound`), no fallida (`RoomFailed`, en disco) y `schema.evolves_from(&actual)`; si no, un error propio (`StorageError::SchemaNotEvolving { room_id }`), sin cambiar nada. El mismo esquema es un `Ok` sin cambios.
- **Ningún lote en el medio.** La recarga se hace entera bajo los locks de los escritores, así que ningún lote se aplica con dos esquemas ni entre construir y publicar. No escribe en disco. Cada motor lo hace a su modo, porque sus filas no cumplen lo mismo:
  - **En memoria, sin construir desde las filas.** Las filas de una sala en memoria nunca son más anchas que su esquema (`validate_batch` valida cada operación, y `apply_snapshot` cada fila). Un índice tiene una entrada por fila cuya clave para la restricción no tiene `NULL` (`memory/index.rs`), y una fila corta lee `NULL` en las columnas que le faltan. Con `evolves_from`, toda restricción nueva de una tabla vieja incluye una columna nueva, que vale `NULL` en todas sus filas, y las tablas nuevas están vacías: los índices de las restricciones nuevas nacen vacíos, y los de las viejas, que son un prefijo en el mismo orden, no cambian. La recarga conserva los índices vivos y agrega vacíos los de las restricciones nuevas y las tablas nuevas, en O(tablas y restricciones), bajo el lock de escritura de la sala (un `std::sync::RwLock`, que no puede cruzar un `.await`, así que construir desde las filas en el pool bloqueante dejaría que un lote se aplique entre construir y publicar).
  - **En disco, construyendo y verificando desde las filas.** Una sala en disco abierta con un esquema más angosto que el que escribió sus archivos conserva las filas con sus valores (las columnas no se verifican al abrir; el replay no valida, y un `Update` ensancha la fila): si se cierra con v2, se reabre con v1 y se recarga v2, una restricción nueva puede encontrar valores, y hasta filas que la violan. La recarga construye desde las filas los índices de las restricciones nuevas (los de las viejas no cambian: sus columnas son las mismas) y verifica las filas contra ellas, en el pool bloqueante, bajo el mutex del WAL (un `tokio::sync::Mutex`, que sí cruza un `.await`; los lectores siguen leyendo el estado viejo). Publica bajo el lock de escritura del estado, y solo el esquema y los datos: la fase 3 de la compactación escribe `snapshot_seq` y `snapshot_len` bajo ese lock sin el mutex del WAL, y no hay que pisarlos. Si las filas no cumplen el esquema nuevo, es `SnapshotConstraints` (Lote 4) y no cambia nada. Mientras construye, las escrituras de la sala esperan: segundos en una sala de un millón de filas con muchas restricciones nuevas (S2), que sigue siendo menos que cerrar y reabrir (DC5).
  - **Qué garantiza en disco** (INV-S7): INV-C2 e INV-C3 siempre; y la equivalencia con abrir con el esquema nuevo, solo si la sala nunca se abrió con un esquema bajo el que no vale toda operación de su log, también las compactadas en su `.snap` (uno más angosto que el que las escribió; el SDK abre siempre con el último esquema que guardó, DC5). Una sala abierta así ya decidió con ese esquema, fuera de la premisa de INV-C1, como pasa hoy al abrirla, y es la excepción de INV-S6.

**Tests exigidos** (en los dos motores, con un mismo helper):
- Un negativo por cada regla de `evolves_from` que llega al storage (una restricción sobre columnas viejas, una restricción cambiada, otro dueño, una columna menos): `SchemaNotEvolving` y la sala igual (esquema, filas e índices).
- En disco, el caso de las filas más anchas: abrir con v1, recargar v2 (v1 con una columna `c` y `UNIQUE(c)`), insertar una fila con `c = 1`, cerrar, reabrir con v1 y recargar v2: el índice de `UNIQUE(c)` tiene la fila (INV-C3), y un insert con `c = 1` se rechaza (`Unique`). Y si las filas de la sala violan la restricción nueva (escritas bajo otra extensión de v1), la recarga es `SnapshotConstraints` y la sala queda igual.
- En una sala fallida: `RoomFailed`, sin cambios.
- Positivos: una columna nueva con `UNIQUE` y FK, y después un lote que la usa y choca (`Unique`, `ForeignKey`), igual que en una sala abierta con el esquema nuevo; una tabla nueva, con escrituras; el mismo esquema dos veces.
- **Equivalencia (INV-S6, INV-C3):** recargar en un punto al azar de un log aleatorio y aplicar el resto da el mismo estado, los mismos reportes y los mismos índices que abrir con el esquema nuevo y aplicar todo; justo después de recargar, los índices son iguales a los que se construyen desde las tablas (`RoomData::build`, en un test unitario de cada motor); y reabrir después de recargar, con el esquema nuevo, da lo mismo. Sin reaperturas con esquemas más angostos (con el log en el WAL o compactado en el `.snap`), que no tienen esta equivalencia.
- **Reaperturas más angostas, en disco:** un log aleatorio con reaperturas con un esquema más angosto, con y sin una compactación antes (para que las filas anchas vengan del `.snap` y no solo del replay), y recargas en puntos al azar: después de cada recarga, el estado cumple las restricciones (INV-C2) y los índices son los que se construyen desde las tablas (INV-C3).
- **Concurrencia:** recargas y lotes en tareas que compiten, en los dos motores: después de cada paso, los índices son los que se construyen desde las tablas, y cada lote se aplica con un solo esquema (un lote que escribe la columna nueva se aplica entero o se rechaza por esquema entero). También con scans, una compactación y un `apply_snapshot` en curso.

**Interacciones a revisar:**
- La compactación: su fase 1 toma el estado bajo el mutex del WAL (la recarga la excluye), la fase 2 no lo lee, y la fase 3 escribe `snapshot_seq` y `snapshot_len` bajo el lock de escritura del estado sin el mutex del WAL, el único escritor del estado que ese mutex no excluye, así que la recarga publica solo el esquema y los datos. Un snapshot escrito con las tablas de antes de la recarga se abre igual, porque la apertura completa las tablas que faltan.
- `apply_snapshot` reemplaza el esquema por el que recibe: una recarga y un snapshot concurrentes quedan en el orden del lock.
- `scan` y `get` resuelven la tabla con el esquema del estado: un scan empezado antes de la recarga sigue con su clon.
- `get_head_seq` y el resto de los lectores.
- El checklist de determinismo de la Fase 3.11: la decisión de cada operación después de recargar es la de una sala abierta con ese esquema, en los dos motores.
- Abrir una sala en disco con un esquema más angosto que el de sus filas (`ARCHITECTURE.md` §8: las columnas no se verifican al abrir).

**Documentos:** `ARCHITECTURE.md` §8 (*Active Storage Schema Validation* y una entrada para la recarga); `INVARIANTES.md`: INV-S7 sin 🆕, la excepción de INV-S6 sin 🆕, en INV-C1 e INV-K6 la parte de la recarga, y DEF-34 cerrado en §4.

### Lote 6 · Zstd en wasm32 con `ruzstd` (`zemdb-storage`) · ⬜
**Objetivo:** que un navegador restaure los snapshots que suben los clientes nativos, y que comprima los suyos (DC9, DEF-54).

**Archivos:** `crates/storage/Cargo.toml`, `src/snapshot.rs` y sus tests.

**Qué se hace:**
- `ruzstd` 0.8 como dependencia de wasm32 y de desarrollo (para probar su camino en nativo desde los tests unitarios); en nativo sigue `zstd`.
- **Decodificador:** en wasm32, los snapshots `Zstd` se decodifican con el `StreamingDecoder` de `ruzstd`, por el mismo camino acotado que hoy usa `zstd` (`decompress_bounded` y `deserialize_zstd` con el `PayloadReader`): se detiene un byte después del largo declarado y, por lo que declara el envelope, nunca reserva más que una constante (`decompress_bounded` reserva hasta 64 MiB por adelantado).
- **Codificador:** una función de compresión de `snapshot.rs` (por ejemplo `compress_zstd(payload, level)`): en nativo, `zstd` con el nivel; en wasm32, el nivel `Fastest` de `ruzstd` (el nivel se ignora). `encode_snapshot_envelope` con compresión la usa y marca el envelope `Zstd`; hoy, en wasm32, escribe `Raw`. El Lote 9 hace que la compactación comprima el `.snap` (`ZEM1`, sin envelope) con la misma función.
- **Medición pendiente de DC9:** el codificador en Chromium y cuánto suma al wasm, anotados en DC9. La de Safari la corre el dueño del proyecto con el mismo programa, y queda como pendiente de DC9 que el Lote 10 revisa.

**Tests exigidos** (unitarios, sobre el camino de `ruzstd`, compilado también en nativo para los tests):
- Ida y vuelta `ruzstd` → `ruzstd`, `ruzstd` → `zstd` y `zstd` → `ruzstd`, con los mismos bytes (INV-W2).
- Negativos, como los del decodificador de `zstd`: un cuerpo que produce más que el largo declarado, uno que produce menos, uno truncado, uno con basura y un largo declarado mayor que el tope: error tipado, sin reservar de más (INV-W1).
- Un fuzzer del camino de `ruzstd`, en un test unitario (`src/tests/snapshot.rs`, porque desde los tests de integración en nativo no se llega a ese camino): envelopes `Zstd` válidos comprimidos con `ruzstd` y mutados al azar, decodificados con `ruzstd`: nunca un pánico, y todo error es `SnapshotCorruption`.
- `cargo build --target wasm32-unknown-unknown` de `zemdb-storage` con el codificador y el decodificador.

**Interacciones a revisar:**
- `DEFAULT_MAX_SNAPSHOT_UNCOMPRESSED_BYTES` y `max_snapshot_uncompressed_bytes`: el mismo tope en los dos decodificadores.
- El relay del servidor solo mira el envelope, no la compresión: verificarlo.
- El BLAKE3 de una subida se calcula sobre los bytes del envelope: un snapshot comprimido en el navegador tiene otros bytes que el mismo estado comprimido en nativo, como ya pasa entre niveles.

**Documentos:** `ARCHITECTURE.md` §8 (Zstd en wasm32); `INVARIANTES.md`: INV-W1, W2 y K12 sin 🆕 en lo de este lote, y DEF-54 cerrado en §4; la medición en DC9.

### Lotes 7 a 9 · La capa de archivos (`zemdb-storage`) · ⬜
**Requisito para empezar:** DC17, adoptada el 23 de Octubre de 2026.

**Objetivo:** que ninguna parte de `crates/storage/src/disk/` fuera de la implementación nativa del trait use `std::fs`, `tokio::fs` ni `fs2`, sin cambiar el comportamiento del motor en disco.

**Archivos:** un módulo nuevo de la capa en `crates/storage/src/` (el trait y la implementación nativa); `disk/mod.rs`, `disk/wal.rs`, `disk/recovery.rs`, `disk/compactor.rs`, `fail_point.rs` y `sys.rs`; los tests del motor en disco que toman un `tokio::fs::File` (`disk_wal_resilience_tests.rs` usa `WalWriter`).

**Corte tentativo:**
- **Lote 7 · El trait y su implementación nativa.** El trait de DC17, su implementación sobre `std::fs` y `fs2` en el pool bloqueante (con el `Arc` del archivo y el permiso del handle de DC17), y el motor genérico sobre él (`DiskStorageEngine<F = …>`, con `new` en el `impl` de la nativa). Pasan a la capa: abrir y bloquear los archivos de la sala, el append del WAL en `apply_batch` (hoy `wal.file.write_all` y `sync_file`; `WalWriter::write_batch` y `write_record` dejan de tomar un `tokio::fs::File`), `close_room`, las operaciones de directorio (crear el directorio, `sync_dir`, borrar, renombrar y listar) y el fail point `read_only_handle`.
- **Lote 8 · La recuperación.** `load_snapshot`, el replay del WAL y de `.wal.compacting` (con `read_at` en lugar de un `BufReader` con posición), la limpieza de temporales y la escritura del snapshot vacío. La búsqueda de un frame válido después de una anomalía usa `find_valid_frame` de `zemdb-core`, que toma un `Read + Seek` síncrono y hoy corre en el pool bloqueante sobre un clon del handle: o un adaptador en `zemdb-storage`, o `find_valid_frame` pasa a un lector posicional en `zemdb-core` (y el lote toca `core`). Lo decide el lote, con su revisión de delta.
- **Lote 9 · La compactación y `apply_snapshot`.** La rotación del WAL y su vuelta atrás, con el renombrar de un archivo abierto (sin cambiar su orden ni su lógica), la absorción, el staging y la instalación de snapshots, y la compresión del `.snap` con la función del Lote 6.

**Tests exigidos** (los fija cada lote; como mínimo):
- toda la suite del storage sin cambios en sus expectativas, incluidas las imágenes de crash, la simulación del disco y las versiones largas (`cargo test -p zemdb-storage --release -- --ignored`);
- un test de que renombrar por ruta y borrar no se hacen con handles abiertos: una implementación de prueba del trait que falla como el OPFS (S3), también por los handles de solo lectura (`.snap`, `.wal.compacting`), y que renombra un archivo abierto cerrando, moviendo y reabriendo, corre la compactación con y sin absorción (con sus dos vueltas atrás), la instalación de un snapshot y la recuperación de una compactación interrumpida;
- un test por cada error de E/S que la capa ahora devuelve en la misma llamada (el caso que hoy cubre `sync_file`);
- en el Lote 7, el caso de INV-S1: soltar el future de `apply_batch` con su escritura retenida en el pool bloqueante (un fail point que pausa), cerrar la sala o soltar el motor, y reabrir: la reapertura da `RoomLocked` hasta que la escritura termina, o `close_room` la espera, y nunca aterriza después de la recuperación (negativo: sin el `Arc` del archivo en la operación, aterriza después);
- en el Lote 9, las vueltas atrás de la rotación de hoy (abrir el WAL nuevo falla; renombrar de vuelta falla), sin cambios en sus expectativas.

**Interacciones a revisar:** INV-S1 e INV-L7 (una escritura de `apply_batch` soltada, el lock del handle del WAL y el `flush` de `close_room`, que DC17 reimplementa); D86 y D89 (ninguna operación de archivo de `open_room`, `apply_snapshot` ni la compactación aterriza después de reabrir); D93; `PendingWrite` y `RoomFailed`; los fail points por ruta; `WalWriter` y `WalReader`, públicos; Windows (un test no lee un archivo que la sala tiene abierto).

**Documentos:** `ARCHITECTURE.md` §8 (la capa de archivos, *Room Lock and WAL Lock* y la rotación); `INVARIANTES.md`: el "Sostenida por" de INV-S1, que nombra el lock del handle del WAL y el `flush` de `close_room`, y el de INV-L7, que suma el mecanismo de DC17; el plan de la fase, DC8 y DC17.

### Lote 10 · Cierre · ⬜
- La revisión de cierre de 4.1 (`docs/METODOLOGIA.md` §6), con las tres condiciones de `INVARIANTES.md` §1.4, sobre todo lo que tocó la sub-fase y los mecanismos con que interactúa. Si no sale en verde, sus rupturas se arreglan en lotes nuevos de 4.1 antes de abrir 4.2.
- `INVARIANTES.md`: el estado de cada invariante tocada. El plan de la fase: 4.1 cerrada. `ROADMAP.md`: "Lo que sigue", 4.2.
- Los pendientes que pasan de 4.1 a 4.5, anotados en sus decisiones: la medición del codificador `ruzstd` en Safari (DC9), y S3 en Safari y Firefox (DC17).

---

## 3. Seguimiento

**Avance:** Lote 0 cerrado el 23 de Octubre de 2026, con la revisión cruzada en verde en su quinta ronda; Lote 1 cerrado el mismo día. DC17 adoptada el 23 de Octubre, con renombrar un archivo abierto como operación del trait (el refactor de los Lotes 7 a 9 queda sin cambio de comportamiento). Lote 2 cerrado el 23 de Octubre. Lote 3 cerrado el 24 de Octubre. El próximo es el Lote 4.
