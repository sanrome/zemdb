# Auditoría amplia del SDK (cierre de 4.2)

**Fecha:** 30 de Octubre de 2026
**Commit auditado:** `9de2c1b` (`main`), al cierre del Lote 11 del [plan de 4.2](../../proposals/2026-10-fase4_2-nucleo-replica.md), con la CI en verde
**Contrato:** [`docs/INVARIANTES.md`](../../INVARIANTES.md), versión 9, y [`ARCHITECTURE.md`](../../../ARCHITECTURE.md) §11
**Alcance:** el de DC3 y §3 del plan de 4.2:
- las invariantes del cliente (§3.11) en la parte que implementa 4.2;
- INV-S8;
- los ajustes de 4.2 a INV-R6, W1, W2 y C1.

Las partes 🆕 de 4.3 a 4.6, y K8, K9, K10, K13 y K14, quedan para la revisión de cierre de la sub-fase que las implementa.

**Resultado: en verde** (`INVARIANTES.md` §1.4). Ninguna invariante se rompe, con el criterio de §2 que el dueño del proyecto adoptó el 31 de Octubre, a partir de la revisión de delta del Lote 12: una respuesta que decodifica, responde al pedido y no viola nada que el cliente verifique cuenta como del servidor, en cualquier campo. Quedan:
- Bajos de documentos, corregidos en el Lote 12;
- ocho Bajos de tests y de código, al backlog (DEF-189 a DEF-196);
- tres propuestas de invariantes, que decide el dueño del proyecto.

---

## 1. Método

Es la auditoría amplia por invariantes de `INVARIANTES.md` §1, con tres auditores en paralelo. Cada uno fue un agente de contexto fresco, de solo lectura sobre el repo y con sus corridas en su propia copia de `9de2c1b`, y respondió solo con veredictos:

| Auditor | Invariantes | Foco |
|---|---|---|
| Réplica y log | K1 ★, K4 ★, K5, K6 ★, C1 ★ en el cliente, W1 ★ y W2 de los metadatos del cliente | El aplicador, la época, el registro, los metadatos y el cambio de esquema |
| Escrituras | K2 ★, K11, K15 | El pipeline de escritura, los reintentos, lo retenido y el cierre |
| Recuperación y robustez | K3 ★, K7, K12 ★, S8, R6 | El descarte y `delete_room`, los snapshots y el relay, y la validación de las respuestas |

Cada auditor recibió:
- `INVARIANTES.md` (§1, §2, §3.11, §4, el resumen, y las secciones de las otras invariantes de su grupo: S8 en §3.2, R6 en §3.4, W1 y W2 en §3.9, C1 en §3.10);
- `ARCHITECTURE.md` §11 y las secciones de su grupo;
- el plan de la fase (DC1 a DC17), y el de 4.2 (DC18 a DC23, los lotes y sus notas **Hecho**);
- `GEMINI.md`;
- el arnés de simulación de los Lotes 10 y 11.

El origen (a) es el rango de 4.2, `9df0de7..9de2c1b`.

**Desvíos:**
- **Respecto de §3 del plan:** W1 y W2 de los metadatos del cliente (Lote 3) se sumaron al auditor de la réplica. El resumen de `INVARIANTES.md` los marcaba sin auditar, y su foco incluye los metadatos.
- **Respecto de §1.6:** las decisiones D1 a D102 y DR1 a DR15 no se les dieron aparte; las citan los documentos que recibieron, y estaban en el repo.

**Verificación del coordinador.** Los tres parches se aplicaron juntos sobre `9de2c1b`:
- de `zemdb-client`, 164 tests pasan y el de DEF-189 falla, como se espera;
- el test de `zemdb-storage` pasa.

**Corridas de los auditores**, todas en verde:
- **Réplica y log:**
  - 30 semillas de los tres tests no ignorados de la simulación;
  - dos mutaciones inversas detectadas: sin el chequeo de época al aplicar un lote, y un ack con su head más uno.
- **Escrituras:**
  - 300 semillas de cada simulación sin fuzzing, y 100 de cada esquema con fuzzing;
  - una mutación inversa detectada: sin el chequeo de la entrada de la escritura después de su espera.
- **Recuperación y robustez:**
  - las suites del cliente, del storage (`delete_room`) y del relay;
  - cortes en cada punto de un sync con cambio de esquema y compactación (23), de la instalación del snapshot de otro miembro (12) y de un descarte (35);
  - 30 semillas de fuzzing de todo menos los registros, y 32 rondas en 4 semillas de bajadas de dos chunks fuzzeadas;
  - cuatro mutaciones inversas detectadas: guardar los metadatos sin temporal, descartar sin `delete_room`, descartar ante un `RoomReset` a un sync, y el relay sin comparar la encarnación.

---

## 2. Resultado por invariante

| Invariante | Veredicto | Lo que la sostiene | Tests de la auditoría |
|---|---|---|---|
| **INV-K1 ★** | Se sostiene | Un solo aplicador, en una tarea propia, para todo cambio del storage. Un lote se aplica solo en la época del pedido, sin hueco, después de descartar lo ya aplicado. Cada respuesta se valida antes de usarse: la correlación, la sala, la continuidad desde el cursor y el catch-up hasta la secuencia. La época cambia solo con un descarte, así que misma época es misma encarnación. | `audit_a_batch_held_across_a_discard_for_damaged_metadata_is_not_applied` |
| **INV-K4 ★** | Se sostiene | K1, K6 y C1, sobre INV-Y2 e Y8. El snapshot del designado es el de su réplica: `create_snapshot` corre bajo el aplicador y en la época de la subida. | `audit_a_column_added_while_calls_are_in_flight_leaves_every_replica_as_the_log` (72 combinaciones de demoras, llamadas a la vez y un crash), `audit_a_snapshot_uploaded_while_a_column_is_added_converges_with_the_log` |
| **INV-K5** | Se sostiene | El cursor es el head del storage, leído bajo el aplicador con la época y la encarnación. Un `Registered` para un cursor mayor que el head no registra. | `audit_no_request_presents_a_cursor_above_the_head_of_the_replica` (un transporte que verifica cada pedido al salir) |
| **INV-K6 ★** | Se sostiene | El esquema sale solo de `Registered`, decidido bajo el aplicador con `evolves_from`. Los metadatos se guardan antes de `reload_schema`. Una sala que falla cerrada no aplica, no instala, no sube y no se registra. Una respuesta demorada con una versión anterior se aplica con el esquema local, como da por hecho INV-E3, todavía 🆕 (su test es DEF-186). | `audit_a_failed_sync_of_a_saved_schema_is_made_durable_by_the_next_open_of_the_room` |
| **INV-C1 ★** (cliente) | Se sostiene | Ninguna operación se aplica con un esquema más angosto que el suyo: el último esquema guardado es el de la apertura, y `SchemaValidation` atrapa una aridad mayor. Como en K6, se apoya en INV-E3. Fuera de la configuración del contrato, DEF-189. | los de K4 y K6 |
| **INV-W1 ★** (metadatos) | Se sostiene | Sin `unwrap` ni pánicos. Se reserva el tamaño del archivo; un archivo que no verifica cuenta como que falta, y un error de E/S es un error. | `audit_room_metadata_with_random_schemas_survive_a_save_and_damage_counts_as_missing` |
| **INV-W2** (metadatos) | Se sostiene | La ida y vuelta es exacta. Todo byte está bajo el CRC32 o es la línea fija del CRC, y el temporal nunca se lee. | el mismo |
| **INV-K2 ★** | Se sostiene | Un `MutationId` por escritura. `seen` y la entrada se toman bajo el aplicador antes del primer intento. Todo movimiento del head pasa por lo que se retiene. La incertidumbre es pegajosa, y cerrar, descartar o fallar cerrada terminan la escritura. Del lado del servidor, ningún código "sin efecto" sale después de secuenciar. | los 5 `audit_k2_*` de `escrituras.patch`: fallar cerrada con la secuencia esperada, un reinicio del servidor, un intento incierto y la sala recreada, el `CommitAck` después de cerrar y reabrir, un intento cortado en el plazo que se secuencia después |
| **INV-K11** | Se sostiene | `close_room` y `shutdown` cierran bajo el aplicador, después de los cambios en curso, y el cierre del motor espera sus tareas. Después de `shutdown` no se abre ninguna sala. | `audit_k11_*` (2) |
| **INV-K15** | Se sostiene | Una entrada por escritura en vuelo, quitada por un guard. Lo retenido es solo lo mayor que el menor `seen`, y se poda y se vacía. Una tarea de cambio y una bajada por sala, hasta el tope. | `audit_k15_*` (2) |
| **INV-K3 ★** | Se sostiene | Solo `Local::discard` borra. Se llega por `open`: sin metadatos que verifiquen, o con datos dañados. O por un registro con cursor que recibe `RoomReset` o `InvalidSequence`, con la correlación y la sala del pedido si las trae, en la época del pedido. Nada más descarta. | `audit_k3_*` (4): cortes en cada punto de tres cambios, y fuzzing de todo menos los registros |
| **INV-K7** | Se sostiene (sin el `SyncWorker` de 4.3) | Cada espera de un snapshot termina con un anuncio, con un registro dentro del log o con una reapertura. Los ciclos están acotados. El designado sube el snapshot de su réplica. | `audit_r6_*`, `audit_k12_fuzzed_chunks_*` |
| **INV-K12 ★** | Se sostiene | `decode_message` (W1) y un `match` tipado con guardas. Ninguna aritmética desborda. En la bajada, el tope se mira antes de reservar y la reserva no aborta (`Config`). `apply_snapshot` cumple S5. Lo que los filtros del fuzzing del Lote 11 no fabrican queda fuera de alcance: un servidor que miente, y DEF-81 (§4). | `audit_k12_fuzzed_chunks_of_a_download_in_two_chunks_do_not_panic_or_move_the_replica` |
| **INV-S8** | Se sostiene | `RoomAlreadyOpen` bajo el lock de las salas, y una tarea que retiene los locks. Se borra del extremo más nuevo al más viejo, con un `sync_dir` por archivo, y los temporales nunca se leen. | `audit_s8_crash_at_any_point_of_delete_room_after_a_cut_snapshot_install_leaves_a_prefix` |
| **INV-R6** | Se sostiene la parte de 4.2 (sigue ❌ Bajo por DEF-188) | La encarnación se compara en el chunk 0 sin ancla y en cada chunk de subida. `create_room` purga antes de escribir los metadatos. | `audit_r6_the_upload_of_a_client_of_the_old_incarnation_never_reaches_the_new_room` |

**El pendiente del Lote 10.** El caso (c) de INV-K2 y la sala que falla cerrada no están entre las causas de un resultado incierto en `check_write` del arnés. K2 se sostiene igual: el arnés no produce ninguno de los dos casos, porque no inyecta fallas del storage y verifica que ninguna sala falle cerrada, y los dos caminos tienen tests propios. Si un arnés futuro los produce, `check_write` tiene que sumarlos.

---

## 3. Bajos

**Corregidos en el Lote 12** (documentos):
- **`INVARIANTES.md`:**
  - **§2:** una respuesta que decodifica, responde al pedido (tipo, y correlación y sala si las trae) y no viola nada que el cliente verifique cuenta como del servidor, en cualquier campo, también los heads y la cola del log; INV-K12 vale igual. Si llega dañada así, protegerla es de TLS, y queda fuera de alcance como un servidor malicioso. Lo decidió el dueño, como precisión de DC11.
  - **INV-K2:** una escritura cuya sala se cierra antes de que llegue su `CommitAck` termina como en (d), aunque la sala se vuelva a abrir y su secuencia se aplique después.
  - **INV-K2 y K5**, *Límite aceptado*: el cursor de un sync, un ack o un commit se lee bajo el aplicador y se manda después de soltarlo, así que un pedido leído antes de un descarte puede salir después.
  - **La introducción de §3.11** y el *Sostenida por* de INV-K4, que decían que todo estaba por implementar o auditar.
  - **INV-K12:** las respuestas se decodifican sin presupuesto de valores, así que reservan hasta unas 24 veces su tamaño, la constante de una legítima.
  - **INV-K15:** la entrada de cada sala que el cliente abrió, que vive lo que el cliente.
- **`ARCHITECTURE.md` §11:**
  - *Schema*: un guardado de los metadatos cuyo `sync_dir` falló se vuelve durable con la próxima apertura de la sala, porque el motor en disco sincroniza su directorio al abrirla;
  - *Attempts*: los finales de una escritura con un error del cliente.
- **Rustdoc:**
  - `ClientError::Config` no nombraba el backoff, el plazo ni los snapshots;
  - `Client::write` no nombraba los finales con un error del cliente, ni el `CommitAck` que llega después de cerrar la sala.

**Al backlog** (en la nota del Lote 12 del plan de 4.2, con sus fixes): DEF-189 a DEF-196.

| DEF | Invariantes | Qué | Test |
|---|---|---|---|
| DEF-189 | C1, K6 | Con `FileMetadata` fuera del directorio del motor, que §11 excluye pero la API permite, un guardado con el `sync_dir` fallido y un crash dejan la réplica con un esquema más angosto que su log, y diverge sin aviso. | `replica.patch` (falla a propósito) |
| DEF-190 | K7, K15 | Una réplica detrás del log, cuyo único snapshot pasa el tope, pide su primer chunk en cada heartbeat: 251 veces en 600 s. | `escrituras.patch` |
| DEF-191 | K12, K7 | Un chunk anclado que no sigue al primero y anuncia más que el tope es `Config` y no `Network`. | `recuperacion.patch` |
| DEF-192 | K2 | Un `HeartbeatAck` con un head más allá del servidor sube para siempre el head visto de la época, y toda escritura posterior de la sala termina incierta hasta cerrarla. Por el criterio de §2, queda fuera de alcance, y falla del lado seguro: es un endurecimiento opcional. | `recuperacion.patch` |
| DEF-193 | K3 | Ningún test de la suite corta un cambio de la réplica a la mitad, y el arnés no mira los descartes con fuzzing. | `recuperacion.patch` |
| DEF-194 | K1, K5, K6 | El arnés no cruza los eventos de esquema y de sala con pedidos en vuelo, ni verifica los cursores. | `replica.patch` |
| DEF-195 | K12 | Una respuesta reserva hasta unas 24 veces su tamaño, y en wasm32 una reserva que falla aborta. A revisar con cada transporte, en 4.3 y 4.5. | — |
| DEF-196 | K7 | Un `Registered` con una cola que deja la réplica detrás del log (su head, menor que `tail_seq - 1`) la deja esperando un snapshot que nadie anuncia, hasta reabrir la sala. Fuera de alcance por el criterio de §2, como DEF-192: es un endurecimiento opcional. | `review.patch` |

**Notas, sin defecto:**
- **`RoomMetadata` no nombra su sala.** El archivo de otra sala copiado en su lugar verifica y falla del lado seguro: `RoomReset`, un descarte y una bajada nueva; con otro esquema, `SchemaMismatch`. Copiar el archivo está fuera del modelo (D92). Test: `audit_the_metadata_of_another_room_get_a_reset_and_the_replica_is_downloaded_again`.
- **Un `Registered` con un head menor que el cursor presentado se acepta.** No trae operaciones, y el sync siguiente lo detecta.
- **Un head ilegible al llegar un `CommitAck` da `OutcomeUnknown`.** Falla del lado seguro, y ninguno de los dos motores lo produce con la sala abierta. Test: `audit_bajo_a_commit_ack_with_an_unreadable_head_is_unknown_though_its_sequence_applies`.
- **Un `shutdown` cortado por su llamador deja abiertas las salas que no llegó a cerrar.** La llamada no terminó, y otro `shutdown` las cierra.
- **Un lote demorado a través de `close_room` y `open_room`, en la misma época, se aplica.** Es de la misma encarnación y pasa por el chequeo de continuidad.

---

## 4. Propuestas de invariantes (§1.2)

Agregarlas es una decisión del dueño del proyecto.

1. **Del auditor de la réplica:** "Lo que el SDK lee de sus metadatos para abrir una sala o aplicar operaciones ya es durable: un guardado que falló no se usa hasta que su efecto sea durable."
   - Es INV-L3 ★ del lado del cliente.
   - INV-C1 en el cliente depende de esto, y hoy lo sostiene un efecto del motor (DEF-189). En 4.5, con el OPFS, tiene que ser explícito.
2. **Del auditor de las escrituras:** "Ningún mensaje con efecto del cliente (commit, sync, ack) se acepta en otra encarnación que la de la réplica que lo mandó."
   - Cerraría los dos límites aceptados que vienen de la misma causa: el de INV-K2 (un commit leído antes de un descarte se secuencia en la sala recreada) y el de INV-K5 (un sync, un ack o un commit anterior al descarte deja adelantado el cursor del servidor).
   - Cuesta la encarnación en `Commit`, `Sync` y `Ack`, como DC23 en los chunks.
3. **Del auditor de recuperación:** "Ningún byte de una respuesta lleva a un error que el SDK o su worker tratan como permanente para la sala (`Config`, `FailedClosed`): esos errores salen solo de la configuración o de lo que anuncia el primer chunk validado de una bajada."
   - La liveness de INV-K7 con el `SyncWorker` de 4.3 depende de cómo clasifique los errores, y DEF-191 ya es un caso.
   - `FailedClosed` por un esquema que no evoluciona sale de un `Registered`, que §2 deja fuera de alcance si miente.

---

## 5. Los tests

Son las modificaciones que cada auditor hizo en su copia, guardadas como parches contra `9de2c1b`. Se aplican, en una copia o un worktree en ese commit, con:

```bash
git apply docs/audits/2026-10-fase4_2-audit/<parche>.patch
```

Los cuatro juntos no se pisan.

| Parche | Auditor | Archivos | Tests |
|---|---|---|---|
| `replica.patch` | Réplica y log | `metadata_tests.rs`, `room_tests.rs`, `schema_tests.rs`, `snapshot_tests.rs`, `sync_tests.rs`, en `crates/client/tests/` | 8 |
| `escrituras.patch` | Escrituras | `crates/client/tests/audit_writes_tests.rs` | 11 |
| `recuperacion.patch` | Recuperación y robustez | `crates/client/tests/audit_recuperacion_tests.rs` (8) y `crates/storage/src/disk/tests/delete.rs` (1) | 9 |
| `review.patch` | La revisión de delta del Lote 12 | `crates/client/tests/review_tail_tests.rs` (1) y `review_error_tests.rs` (1) | 2 |

**Cómo se corren:**
- `cargo test -p zemdb-client --test <archivo>`;
- `cargo test -p zemdb-storage --lib audit_s8`;
- `AUDIT_SEED=<n>` cambia la semilla de los dos fuzz de `audit_recuperacion_tests.rs`.

Todos pasan en `9de2c1b` (los de `review.patch` muestran DEF-196, y un error sin correlación ni sala que el criterio de §2 deja fuera de alcance), salvo `audit_metadata_apart_from_the_engine_diverge_after_a_failed_sync_and_a_crash`, que muestra DEF-189 y falla a propósito.

**Para traer un test a la suite** (como piden DEF-193 y DEF-194):
- renombrarlo: los nombres llevan `audit_` y algunos, códigos como `k2` o `bajo`;
- sacar los códigos de los comentarios (`GEMINI.md`);
- darles a los de muchos puntos o semillas una versión corta, con la larga en `#[ignore]` (`docs/METODOLOGIA.md` §4, regla 5).

`replica.patch` declara con `#[path]` los generadores de `zemdb-core` y el oráculo del storage, en `metadata_tests.rs` y `snapshot_tests.rs`.
