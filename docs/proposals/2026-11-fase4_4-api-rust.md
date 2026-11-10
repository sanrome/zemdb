# API de Rust — Plan de la sub-fase 4.4

**Fecha:** 6 de Noviembre de 2026
**Tipo:** Plan de sub-fase (`docs/METODOLOGIA.md` §1). Documento vivo: lotes y seguimiento.
**Estado:** En curso: Lote 0 (el contrato) cerrado el 6 de Noviembre de 2026, con la revisión cruzada en verde en su tercera ronda. El dueño del proyecto adoptó DC32 a DC37 el 7 de Noviembre de 2026, sin cambios. El Lote 1 (`check_operation`) cerró el 9 de Noviembre de 2026, con la revisión de delta en verde en su primera ronda, y el Lote 2 (la fachada y las lecturas) el 10 de Noviembre de 2026, también en su primera ronda. Lo que sigue es el Lote 3.
**Plan de la fase:** [`2026-10-fase4-plan.md`](2026-10-fase4-plan.md), §4.4. **Metodología:** [`docs/METODOLOGIA.md`](../METODOLOGIA.md).
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) y [`ARCHITECTURE.md`](../../ARCHITECTURE.md). Reglas de código: [`GEMINI.md`](../../GEMINI.md).
**Parte de:** el núcleo de la réplica de 4.2 ([plan](2026-10-fase4_2-nucleo-replica.md)), y la API que la sub-fase 4.3 agrega al núcleo ([plan](2026-11-fase4_3-transporte-worker.md)): el `TokenProvider` (DC30), el worker con `start_worker` y `set_freshness`, y el estado de la conexión (DC28). 4.3 corre en paralelo: los lotes de 4.4 que usan algo de ella van después del lote de 4.3 que lo trae (§4.4 del plan de la fase).

---

## 0. Resumen

- **Qué.** La API de Rust del SDK: `ZemdbClient`, `RoomHandle` y `TableHandle` sobre el núcleo de 4.2, con filas y claves por nombre de columna, lecturas locales (`get`, `scan`), escrituras (`insert`, `update`, `delete`) con el dueño completado por el SDK y el pre-chequeo local, el cifrado de columnas con el `CryptoEngine` de la aplicación, `watch` y `ChangeStream`, el descarte a pedido de la aplicación, el modo `live` atado a los `watch`, `Offline`, y la app de referencia nativa como criterio de aceptación.
- **Dónde.** En `zemdb-client`, en módulos nuevos de la fachada, para tocar lo menos posible `src/client.rs`, que 4.3 cambia en paralelo. El núcleo suma solo lo que la fachada necesita de adentro: el esquema de la réplica abierta y el motor, el estado de la conexión, el aviso de los cambios para `watch`, la cuenta de suscripciones en `status` y el descarte a pedido. Un lote en `zemdb-storage`, `StorageEngine::check_operation`, para el pre-chequeo (DC33), con el checklist de determinismo de la Fase 3.11. Todo es código del alcance auditado (el SDK lo está desde el cierre de 4.2), así que cada lote lleva su revisión de delta. El cambio de §2 de `INVARIANTES.md` precisa supuestos del cifrado que el modelo ya tenía (la aplicación y sus claves son de confianza): no cambia el modelo de amenaza y no pide una auditoría amplia (§1.4).
- **Qué no.** El transporte, el worker y la renovación del token (4.3; la verificación del `ClientId` de un token renovado pasó a 4.3, DC30); el navegador, el runtime del Worker y las claves de cifrado en el Worker (4.5); la API de JavaScript y los errores de rango de DC14 (4.6); escaneos en streaming y consultas por índices secundarios (después de la v0.1, `ROADMAP.md` §3). La API de bajo nivel del núcleo (`Client::write`, DC18) sigue mandando la operación tal como la arma quien la llama: el dueño, el cifrado y la validación son de la fachada.
- **Decisiones nuevas:** DC32 a DC37 (§1), adoptadas por el dueño del proyecto el 7 de Noviembre de 2026. DC36 precisa DC24 (adoptada): qué hace un `watch` con el modo que eligió la aplicación. DC32 precisa DC14: la clave de una escritura puede traer la columna dueño con el dueño del cliente. DC37 le suma un método público al núcleo de DC18, y precisa DC22 y DC29: descartar la sala no saca a una cuyo snapshot pasa el tope del cliente.
- **Cómo cierra:** con una revisión de cierre (`docs/METODOLOGIA.md` §6), con dos revisores (§3), y con la app de referencia nativa corriendo sus escenarios contra un servidor local (§6 del plan de la fase).

---

## 1. Decisiones

Las de la fase que implementa esta sub-fase: DC1, DC4 (la app de referencia), DC12 (el descarte a pedido), DC14 (los principios de la API: datos serializables, `watch` por id en el límite del Worker), DC15 (el dueño y el id de dispositivo), DC18 (la fachada llama a los métodos del núcleo y no expone `heartbeat`), DC24 (el modo `live` atado a los `watch`) y DC28 (el worker que arranca la fachada), con DC30 para el token. Esta sub-fase agrega las siguientes, que el dueño del proyecto adoptó el 7 de Noviembre de 2026, sin cambios:

| ID | Decisión | Lotes | Estado |
|---|---|---|---|
| **DC32** | La forma de la fachada: los handles, las filas y las claves por nombre, las lecturas locales, el orden de una escritura y los errores | 2, 3 (y los errores de 4, 5 y 8) | Adoptada (7 de Noviembre de 2026) |
| **DC33** | El dueño completado por el SDK y el pre-chequeo, con `StorageEngine::check_operation` | 1, 3, 4 | Adoptada (7 de Noviembre de 2026) |
| **DC34** | El cifrado de columnas: los datos adicionales con la sala, `NULL` cifrado y el texto descifrado validado | 5 | Adoptada (7 de Noviembre de 2026) |
| **DC35** | `watch`: el aviso del aplicador, una marca por suscripción y el valor recalculado al leerse | 6 | Adoptada (7 de Noviembre de 2026) |
| **DC36** | La fachada con el worker: el modo de frescura, `live` mientras haya un `watch`, `sync`, `Offline` y el primer token (precisa DC24) | 8 | Adoptada (7 de Noviembre de 2026) |
| **DC37** | Descartar a pedido de la aplicación: `discard_room`, en cualquier estado de la sala | 7 | Adoptada (7 de Noviembre de 2026) |

### DC32 · La forma de la fachada
- **Dónde.** Módulos nuevos (`src/api/`: el cliente, la sala, la tabla, las filas y los errores locales), sobre el `Client` del núcleo, que no cambia de forma: sus métodos siguen públicos como API de bajo nivel (DC18), y `Client::write` manda la operación tal como la arma quien la llama, sin el dueño, el cifrado ni la validación de la fachada (el servidor verifica igual el dueño y los tipos, INV-A5). El núcleo suma, `pub(crate)`, lo que la fachada lee de adentro: el esquema de una réplica abierta, el motor y el estado de la conexión (DC36).
- **`ZemdbClient`.** `open` abre el `Client` del núcleo con lo mismo que él (la configuración, el motor, los metadatos y el transporte; desde el Lote 3 de 4.3, también el `TokenProvider`, DC30) y, además, el `CryptoEngine` de la aplicación, si tiene (DC34). `client_id()` expone el `ClientId` (DC15), que la aplicación manda a su backend para pedir los tokens. `room(room_id, token)` abre la sala (`open_room`) y devuelve su `RoomHandle`; desde el Lote 8, `room(room_id)` toma el primer token del `TokenProvider` y arranca el worker (DC36). Si `open_room` termina en `FailedClosed` con la réplica abierta (un esquema que no evoluciona del local), `room` devuelve igual el handle, para leerla y descartarla (DC37); `status` dice que falla cerrada, y sus escrituras dan `FailedClosed`. Una sala sin metadatos necesita al servidor (DC18): sin una respuesta válida, `room` devuelve el error de `open_room` (`Network`), sin handle ni worker. `shutdown` cierra todo, como el del núcleo. Un `ZemdbClient` y sus handles se clonan barato y comparten el cliente.
- **`RoomHandle`.** `table(name)` devuelve el `TableHandle` de una tabla del esquema vigente (si no está, `InvalidOperation`); `status`, `close` (el `close_room` del núcleo) y, en sus lotes, `discard` (DC37), `sync` y `set_freshness` (DC36). `heartbeat` no se expone (DC18).
- **`TableHandle`.** Guarda el nombre de la tabla y la resuelve contra el esquema de la réplica en cada llamada: el esquema solo crece (DC5), y una tabla conserva su id y su nombre.
- **Filas y claves por nombre** (como pide DC14 para JavaScript, y para que el runtime del Worker de 4.5 no tenga que traducir): `Row`, columnas por nombre con su `Value`, y `Key`, las columnas de la clave primaria por nombre, los dos serializables con `serde`.
  - **Al escribir:** una columna que falta en la fila de un `insert` vale `NULL` si es nullable, y si no, es `InvalidOperation`; una columna que no es del esquema, un valor que no es del tipo real de su columna (también en una columna cifrada) o un `NULL` en una columna que no lo admite, también. La columna dueño no va en la fila ni en los cambios; la clave de una escritura la puede omitir (la completa el SDK) o traerla con el dueño del cliente (DC33).
  - **Al leer:** una fila trae todas las columnas del esquema con que se leyó (las que una fila corta no tiene, en `NULL`), o solo las que pide el `scan`. La clave de una lectura lleva la columna dueño, para poder leer las filas de otros.
- **Escrituras** (Lote 3). `insert(row)` es el `Insert` del protocolo: si la clave existe, reemplaza la fila entera (`ARCHITECTURE.md` §4), salvo en una tabla inmutable, donde la réplica la rechaza (`Duplicate`); devuelve la clave de la fila, con la columna dueño, que sirve igual para `get`, `update` y `delete`. `update(key, changes)` manda solo las columnas que cambian, que no pueden ser de la clave. `delete(key)`. El `timestamp` de la operación es el de la hora de pared, en milisegundos, y es informativo: el orden lo da la secuencia (§10 de `ARCHITECTURE.md`). La fachada llama una vez a `write` del núcleo, que reintenta con el mismo `MutationId` (DC6), y nunca repite una escritura: sería otro `MutationId`, otra mutación para el dedup (INV-K2, INV-Y4).
- **El orden de una escritura,** en el que cada paso que falla la termina sin mandar nada (INV-K2), salvo el pre-chequeo sin decisión (DC33):
  1. la sala tiene la réplica abierta (si no, `RoomClosed`, sin esperar: una réplica vacía, después de un descarte por `RoomReset`, no tiene esquema contra el cual validar, y el registro siguiente del worker o de un `sync` la abre; y una escritura que llega durante una reapertura de la sala del storage o un descarte es `RoomClosed`, como una lectura);
  2. la validación propia de la fachada, por nombre, contra el esquema de la réplica: la tabla, las columnas, el tipo real de cada valor y su nulabilidad, la clave (todas sus columnas, salvo la columna dueño, que el paso 3 completa si falta), la columna dueño y unos cambios vacíos en un `update` (`InvalidOperation`);
  3. el dueño (DC33);
  4. `Offline` (DC36, desde el Lote 8);
  5. el cifrado (DC34, desde el Lote 5; hasta entonces, una escritura que toca una columna cifrada es `Crypto`);
  6. sobre la operación que se manda: `Schema::validate_operation` (la misma regla del servidor y del storage, que pide `Bytes` en una columna cifrada y el dueño declarado), `check_operation_size` (el tope de una operación, `ARCHITECTURE.md` §7.2, sobre el texto cifrado, que es más largo, con la secuencia del peor caso y un `MutationId`, que ocupa lo mismo sea cual sea, como lo usa el servidor) y `Schema::check_owner` con el `ClientId` del cliente (INV-A5). Una falla en este paso es `InvalidOperation`;
  7. el pre-chequeo (DC33, desde el Lote 4);
  8. una llamada a `write`.
- **Lecturas locales** (Lote 2): `get(key)` da la fila o nada; `scan(scan)` da un `Vec<Row>` con el rango de claves, la dirección, el límite y las columnas (por nombre) que pide, sobre el `scan` del storage. Nunca mandan nada, también sin red, y leen la réplica abierta también si falla cerrada (DC20). Una sala cuya réplica no está abierta (cerrada, o vacía después de un descarte por `RoomReset` hasta que llegue `Registered`) es `RoomClosed`. No toman el aplicador: leen el esquema de la réplica y después el storage, así que durante la reapertura de la sala del storage después de un error (DC21) pueden dar `RoomClosed`, y una fila escrita después de una recarga del esquema que llegó en el medio se lee con las columnas del esquema leído (las demás, que el esquema siguiente agregó al final, no aparecen). Hasta el Lote 5, una lectura que devuelve una columna cifrada es `Crypto` (nunca el texto cifrado como si fuera el valor). Sin streams en la v0.1: un `scan` trae su resultado entero, y el límite lo acota.
- **Datos serializables** (DC14, DC18): los argumentos y los resultados de los métodos de la fachada son datos propios (`Row`, `Key`, el `scan`, `RoomStatus`, `ClientError`). Los handles y los `ChangeStream` no lo son: el runtime del Worker de 4.5 los guarda de su lado, por id.
- **`ClientError`,** completo para la v0.1 (§A.1 del plan de la fase), con variantes serializables, cada una con un código para JavaScript en 4.6:
  - `InvalidOperation` (Lote 2): la operación o la lectura no son válidas para el esquema local; nada se mandó. Distinta de `Validation`, que sigue siendo el rechazo del servidor.
  - `Crypto` (Lote 2, como guarda de las columnas cifradas hasta el Lote 5, que le da su sentido final: sin `CryptoEngine`, o cifrar falló), `Decryption` (Lote 5), `PrecheckFailed` (Lote 4) y `Offline` (Lote 8), en sus decisiones.
  - Los mensajes que arma la fachada (`InvalidOperation`, `Crypto`, `Decryption`, `PrecheckFailed`, `Offline`) nombran tablas, columnas y tipos, nunca valores: un valor puede ser un dato personal, o el claro de una columna cifrada (INV-K9). Los del núcleo, que la fachada devuelve tal cual (por ejemplo, el de una respuesta inválida a un registro), pueden llevar valores de columnas no cifradas, nunca el claro de una cifrada, que el núcleo no ve.
  - `OutOfRange` (DC14) llega con 4.6.
- **Alternativas descartadas:** filas posicionales (`CompactRow`), que obligan a la aplicación a conocer el orden de las columnas y no sirven a JavaScript; un `scan` que devuelve un stream del storage, que toma prestado el motor (en la v0.1, con el límite alcanza); leer bajo el aplicador (una lectura esperaría la instalación de un snapshot entero, segundos en una sala grande); el dueño, el cifrado y la validación dentro de `Client::write` (el núcleo es la API de bajo nivel, DC18, y no tiene el `CryptoEngine`).

### DC33 · El dueño y el pre-chequeo
- **El dueño lo completa el SDK** (§A.4 del plan de la fase, INV-K8), en las tablas con dueño (`OwnerIdentity::ClientId`; `zemdb-core` rechaza `Pseudonym`), con la parte de dueño de su `ClientId` (`ClientId::owner`, la persona, DC15), como `Value::String`:
  - en un `insert`, en la columna dueño de la fila (y por ella en la clave, si es parte de ella);
  - en un `update` y un `delete`, en la clave si la columna es parte de ella; si no, como un delta más del `update` (la operación lo exige, `MissingOwner`) y como el `owner` del `delete`.

  Si la aplicación pone la columna dueño en la fila o en los cambios, o en la clave de una escritura con otro valor que el dueño del cliente, la llamada es `InvalidOperation` y no se manda nada: el SDK no la pisa en silencio. En la clave puede ir con el dueño del cliente (es la clave como se lee, la que devuelve `insert`). Una tabla sin dueño no lleva nada. Antes de mandar, `Schema::check_owner` verifica la operación armada (paso 6 de DC32).
- **El pre-chequeo** (§A.4): antes de mandar, la fachada pregunta al storage qué decidiría la réplica sobre la operación en su estado actual, y si la rechazaría, la escritura termina con `PrecheckFailed`, con el motivo (el mismo `Violation` de `ConstraintViolation`, o la fila que falta de `RowMissing`), sin mandar nada. Distinto de `ConstraintViolation`, que es la decisión de la réplica sobre una secuencia.
  - **`StorageEngine::check_operation(room_id, op) -> Result<Option<RejectReason>, StorageError>`,** nuevo (Lote 1), en los dos motores. Verifica la operación como `apply_batch` verifica una (una tabla que la sala no tiene es `TableNotFound`; una operación que no valida con el esquema de la sala, `SchemaValidation`) y devuelve la decisión que `apply_batch` tomaría sobre ella en el estado publicado de la sala, el que tiene la réplica: `None` si la aplicaría; el `DELETE` de una fila que no existe no cambia nada y no se reporta, así que también es `None`. No cambia nada: ni filas, ni índices, ni el head. Una sala cerrada es `RoomNotFound`. La decisión sale de la misma función que usan `apply_batch` y la recuperación del motor en disco: se separa de `apply_operation` la parte que decide, que ya corre antes de tocar la fila (INV-C3), y `apply_operation` la llama. En disco, bajo el lock de lectura del estado de la sala, sin el mutex del WAL (que esperaría el `fsync` en curso), así que no mira si la sala está marcada como fallida: ese error lo da el `apply_batch` siguiente, como hasta ahora.
  - **Sobre la operación que se manda:** después del dueño y del cifrado (DC34), justo antes de `write`. Las columnas cifradas no pueden estar en una restricción, en la clave ni en la columna dueño (`ARCHITECTURE.md` §3.1), así que la decisión sobre el texto cifrado es la misma que sobre el claro.
  - **Es un consejo:** no toma el aplicador, y la réplica puede estar atrasada o cambiar antes de que el servidor secuencie la escritura. Puede frenar una escritura que el log habría aceptado (la réplica atrasada), y dejar pasar una que el log rechaza, cuya decisión llega igual de la réplica (`ConstraintViolation`). Activado por defecto; `ClientConfig::precheck` lo apaga.
  - **Sus errores:** `TableNotFound` y `SchemaValidation` (el esquema de la sala cambió entre la validación local y el pre-chequeo) son `InvalidOperation`, y `RoomNotFound` (la sala del storage se cerró en el medio), `RoomClosed`: en esos, no se manda nada. Cualquier otro error del storage deja el pre-chequeo sin decisión, y la escritura sigue: es un consejo, y frenarla dejaría a una sala del motor en disco sin escrituras hasta que algo la reabra (la reapertura de DC21 la dispara el `apply_batch` del catch-up de esa escritura).
- **Alternativas descartadas:** un pre-chequeo armado en el cliente con `get` y `scan` (sin los índices de las restricciones, un `UNIQUE` pediría recorrer la tabla, y sería una segunda implementación de las reglas, que puede apartarse de la del storage); el pre-chequeo dentro de `write` del núcleo (el núcleo es la API de bajo nivel, DC18, y no sabe del dueño ni del cifrado).

### DC34 · El cifrado de columnas
- **El `CryptoEngine` es de la aplicación** (`INVARIANTES.md` §2): lo da al abrir el `ZemdbClient`, si tiene columnas cifradas. Sin él, una escritura que toca una columna cifrada es `Crypto`, sin mandar nada, y una lectura que la devuelve, también. Como `NULL` también se cifra, todo `insert` en una tabla con columnas cifradas lo necesita, aunque no las nombre. Las garantías de los datos adicionales suponen un `CryptoEngine` que los autentica (un AEAD); con `NoOpCryptoEngine`, que la aplicación puede dar (es solo para tests o despliegues sin E2EE, `zemdb-core`), no hay cifrado ni autenticación, y es su decisión.
- **Qué se cifra:** el valor con su tipo real, codificado como en el protocolo (`bincode` de `Value`), también `NULL`: así el servidor no ve cuáles celdas están vacías. Lo que viaja es `Value::Bytes` (lo único que el servidor acepta en una columna cifrada, salvo `NULL`). Se cifra en la fila de un `insert` y en cada delta de un `update`, después de la validación por nombre y del dueño, y antes de la validación de la operación que se manda y del pre-chequeo (DC32); ni la clave ni la columna dueño se cifran nunca (`zemdb-core` lo prohíbe).
- **Los datos adicionales** (INV-K9): una codificación canónica y versionada de `(room_id, table_id, pk, column_idx)`, armada por el SDK, con la sala adentro, y con los `Float` de la clave canonicalizados como los compara `Value` (`-0.0` como `0.0`), para que un `update` que nombra la fila con una clave igual pero con otros bits dé los mismos datos adicionales. La sala va además como argumento del `CryptoEngine`, que puede derivar de ella la clave. Con la sala adentro, un `CryptoEngine` que no deriva una clave por sala igual rechaza el texto cifrado de otra sala con la misma tabla, clave y columna. `ARCHITECTURE.md` §3.1, que hoy dice `(table_id, pk, column_idx)`, cambia en el Lote 5.
- **Al leer** (`get`, `scan` y cada valor de `watch`): se descifra cada columna cifrada de cada fila que se devuelve. El texto descifrado se decodifica como un único `Value`, con el tope de su largo (nunca una reserva mayor que lo que llegó, INV-K12) y sin bytes de sobra, y tiene que ser del tipo de la columna (o `NULL`, si es nullable). Si no descifra, o no decodifica así, la lectura es `Decryption`, con la tabla y la columna: nunca un valor inventado. Un `NULL` guardado se lee como `NULL` (las filas cortas de una columna agregada después, o una celda que otro cliente dejó en `NULL`).
- **Una fila que no descifra hace fallar la lectura entera.** Cualquier miembro puede escribir cualquier `Bytes` en una columna cifrada de las filas que puede escribir, y con eso hace fallar los `scan` que piden esa columna. La aplicación puede leer el resto con un `scan` que no la pide (por ejemplo, la tabla de posiciones de la app de referencia no lee `notes`).
- **Límite aceptado:** un texto cifrado anterior de la misma celda, o de una encarnación anterior de la sala con el mismo id, descifra bien: los datos adicionales no llevan la versión de la fila ni la encarnación. Quien puede escribir esa fila puede volver a poner un valor viejo, como podría escribir cualquier otro.
- **Lo que guarda el storage** es el texto cifrado, así que el snapshot que sube el designado también (INV-K9).
- **Alternativas descartadas:** la sala solo como argumento del `CryptoEngine` (uno que no la use aceptaría el texto cifrado de otra sala); `NULL` en claro (el servidor ve qué celdas están vacías); un error por celda en la fila (o un valor que lo marca, que sería inventado, o una `Row` con errores adentro).

### DC35 · `watch`
- **Las suscripciones.** `TableHandle::watch(key) -> ChangeStream<Option<Row>>` y `watch_scan(scan) -> ChangeStream<Vec<Row>>`. Un `ChangeStream` es un `Stream` cuyos elementos son `Result<T, ClientError>`: su primer elemento es el valor actual, y después uno cada vez que el valor cambia. Un error al leer (`Decryption`, `Storage`) es un elemento, el stream sigue, y no cambia el último valor emitido, con el que se compara el siguiente. La suscripción se registra antes de leer el valor inicial, así que un cambio que llega en el medio la marca. Sobre una réplica vacía (después de un descarte por `RoomReset`), el primer elemento llega cuando se abre.
- **El aviso del aplicador.** Después de cada cambio de la réplica, dentro de la tarea del aplicador y cuando ya es visible (y durable, con un motor persistente: `apply_batch` vuelve después del `fsync`), el núcleo avisa a las suscripciones de la sala:
  - con las claves que cambiaron, al aplicar un lote: las de sus operaciones aplicadas, sin las rechazadas que reporta el `ApplyReport`. El aviso va en el punto común por el que todo lote entra al storage (`apply_batch` dentro del aplicador, DC7), así que cubre un sync, el catch-up de un commit y, con 4.3, el evento `ops`;
  - con "todo", al instalar un snapshot, al reabrir la sala del storage después de un error, al recargar el esquema, al abrir la réplica (también la de `Registered` después de un descarte) y al descartarla.

  El aviso solo marca (bajo un lock síncrono, sin `.await` ni E/S): cada suscripción tiene una marca, que se prende si el aviso la toca (su clave, o su tabla para un `watch_scan`, o "todo").
- **El valor se recalcula al leerse.** Cuando la aplicación pide el siguiente elemento, si la marca está prendida, el stream la apaga, lee el valor (con la lectura de DC32, y DC34 si descifra) y lo emite solo si es distinto del último que emitió; si no, espera el próximo aviso, sin perder uno que llegue mientras decide. Así un consumidor lento no acumula nada: saltea los estados intermedios y recibe el actual (INV-K10), y una operación rechazada nunca emite, porque no cambia el valor. Una lectura que da `RoomClosed` en medio de un cambio (una reapertura, un descarte) no emite ni termina el stream: espera el próximo aviso.
- **El fin.** El stream termina (devuelve `None`) cuando la sala queda cerrada al terminar un cambio (`close_room`, `shutdown`, el descarte a pedido de DC37, o un error que sacó la sala del cliente), no en el cierre intermedio de una reapertura. Mientras la réplica está vacía (después de un descarte por `RoomReset` o D78), el stream no emite, y recalcula cuando se abre otra vez. Soltar el stream quita su suscripción.
- **Memoria** (INV-K15): por suscripción, su filtro, su marca y su último valor (en un `watch_scan`, las filas de su rango, descifradas: proporcional al rango que eligió la aplicación); nada por cambio aplicado, sin colas. El registro de las suscripciones vive en la sala del núcleo, y `status` cuenta cuántas tiene (un campo público más de `RoomStatus`).
- **Con 4.3,** la fachada cuenta las suscripciones activas de cada sala para el modo `live` (DC36).
- **Alternativas descartadas:** un canal por sala con las claves de cada cambio (memoria por cambio, y un consumidor atrasado que pierde avisos); emitir las operaciones (la aplicación tendría que aplicarlas y filtrar las rechazadas); releer por tiempo.

### DC36 · La fachada con el worker (precisa DC24)
Depende de los Lotes 3, 4 y 7 de 4.3 (el `TokenProvider`, el worker y su estado, y el modo `live`).
- **El worker de cada sala.** `room` arranca el worker de la sala (`start_worker`, DC28), en el modo que la aplicación eligió para ella (por defecto, `ClientConfig::freshness`), y `close` lo detiene con `close_room`. Un segundo `room` de la misma sala no arranca otro.
- **El modo que elige la aplicación** (`RoomHandle::set_freshness`) es el modo base de la sala, y el modo que la fachada le pide al worker sale de él y de las suscripciones de la sala:
  - con `periodic`, `live` mientras la sala tenga al menos un `watch`, y `periodic` cuando no queda ninguno (DC24);
  - con `manual` o `live`, ese mismo modo, haya o no `watch`: una aplicación que eligió `manual` no quiere syncs automáticos (por ejemplo, por la batería), y un `watch` muestra lo que traen sus escrituras y sus `sync`; una que eligió `live` lo quiere siempre.

  Leer el modo base y la cuenta de suscripciones y llamar a `set_freshness` van bajo un lock asíncrono por sala, así que dos cambios no se intercalan: el último que toma el lock deja el modo que corresponde al estado de ese momento. Soltar un stream pide el cambio en una tarea, porque `Drop` no puede esperar: si no hay un runtime de Tokio (`Handle::try_current`), o el cliente ya se soltó, no pide nada. Si el servidor tiene el SSE apagado, el worker cae a `periodic` (DC24), y `status` lo dice.
- **`RoomHandle::sync`** llama al `sync` del núcleo, para ponerse al día cuando la aplicación quiera (en `manual`, o al volver a la pestaña; la precisión de DC18 de 4.3).
- **`Offline`** (§A.4): una escritura en una sala cuyo último pedido no tuvo respuesta termina con `Offline`, en el paso 4 de DC32 (antes de cifrar y de mandar nada). El estado de la conexión lo lee la fachada del núcleo por un acceso `pub(crate)`, no por `status`, que da `RoomClosed` en medio de un cambio de la réplica. Sin worker, o antes del primer pedido de la sala, no hay estado, y la escritura sigue como en 4.2. Las lecturas nunca son `Offline`.
- **El primer token.** `ZemdbClient::open` recibe el `TokenProvider` y se lo pasa al núcleo (DC30); `room(room_id)` le pide el primer token de la sala, con el plazo de DC30, y verifica su `ClientId` como DC30 verifica uno renovado. Si el proveedor falla o no responde en el plazo (por ejemplo, sin red), `room` abre igual, sin token, una sala con metadatos, para leerla: el registro recibe `Unauthorized`, y la renovación de DC30 consigue el token después. Una sala sin metadatos necesita al servidor (DC32): `room` devuelve el error, sin handle ni worker. Un token de otro `ClientId` es un error de la aplicación: `room` termina con `Config`, sin mandar nada con él.

### DC37 · Descartar a pedido de la aplicación
- **Para qué** (DC12, INV-K3): la salida de los casos que fallan cerrado (un esquema que no evoluciona del local, DC20), y de una sala que no abre por `SnapshotConstraints` o `SchemaMismatch`. No es la salida de una sala cuyo snapshot pasa el tope de bajada (INV-K7 la exceptúa): la réplica vacía se registra sin cursor y necesita el mismo snapshot, salvo que el log de la sala todavía esté entero; la salida es subir `max_snapshot_bytes`. Es el único camino por el que la aplicación descarta una réplica.
- **`Client::discard_room(room_id)`,** en el núcleo (datos serializables, DC18; un método público más de la forma del núcleo), y `RoomHandle::discard` y `ZemdbClient::discard_room` en la fachada (el segundo, para una sala sin handle, porque su apertura falló). Vale en cualquier estado de la sala: abierta, fallando cerrada, cerrada después de una apertura que falló, o que este cliente nunca abrió (con archivos de una ejecución anterior; la sala suma su entrada al cliente, como al abrirla). Después de `shutdown`, es `RoomClosed`.
- **Qué hace:** detiene el worker de la sala (con 4.3: `stop_worker` manda el último `Ack` si el head se movió, con el head de esa encarnación, DC28), y bajo el lock de los registros y el aplicador (después de un registro, un sync, una bajada o una subida en vuelo, que el cambio de época deja sin efecto, DC7) termina sus escrituras en vuelo como un descarte (INV-K2, b: incierta con `CommitAck`, `Network` sin él, o incierta después de un intento incierto) y sus `watch` (DC35), y descarta en el orden de DC19: borra los metadatos de la sala de forma durable (con el temporal de un guardado cortado), cierra la sala del storage si está abierta, la borra (`delete_room`) y suma uno a la época. La sala queda cerrada: el próximo `open_room` la abre como nueva (sin metadatos, borra otra vez, aunque no quede nada, y suma uno más a la época), se registra sin cursor y se pone al día desde el servidor, con su esquema actual.
- **Un error en un paso lo detiene** (DC19), y la sala queda cerrada: sin metadatos, la próxima apertura vuelve a borrar (por ejemplo, después de `RoomLocked`). Aparte del último `Ack` del worker, no manda nada al servidor: la entrada del roster vence con su lease, como la de un cliente que se fue, o la reemplaza el registro siguiente.

### 1.1. Lo que hereda

| # | De dónde | Qué | Cómo se resuelve | Lote |
|---|---|---|---|---|
| 1 | Plan de la fase, §4.4 y §A.3 | `ZemdbClient`, `RoomHandle` y `TableHandle`: CRUD y escaneos | DC32 | 2, 3 |
| 2 | §4.4 y §A.7 | `watch` y `ChangeStream` | DC35 | 6 |
| 3 | §4.4 y §A.4 | El dueño completado por el SDK y el pre-chequeo local | DC33 | 1, 3, 4 |
| 4 | §4.4 (DC24) | El modo `live` atado a los `watch`, la API del modo de frescura y `sync` | DC36 | 8 |
| 5 | §4.4 y §A.10 | El cifrado de columnas con `CryptoEngine` | DC34 | 5 |
| 6 | §4.4 y §A.1 | `ClientError` | DC32 | 2 a 5, 8 |
| 7 | §4.4 (DC12, INV-K3) | Descartar a pedido de la aplicación | DC37 | 7 |
| 8 | §4.4 (DC15) | Exponer el id de dispositivo | `client_id()` (DC32) | 2 |
| 9 | §4.4, *Lo que 4.4 toma de 4.3* | El `TokenProvider`, `start_worker` y `set_freshness`, el estado de la conexión para `Offline`, y `sync` público | DC36, después de los Lotes 3, 4 y 7 de 4.3 | 8 |
| 10 | DC18 (plan de 4.2) | `write` suma, antes, el dueño y el pre-chequeo; las lecturas y `watch`; el `Offline` inmediato | DC32, DC33, DC35 y DC36: en la fachada, no en `Client::write` | 2 a 4, 6, 8 |
| 11 | DC14 | Datos serializables, y `watch` por id en el límite del Worker | DC32 y DC35 | 2, 6 |
| 12 | `ARCHITECTURE.md` §3.1 | Los datos adicionales del cifrado sin la sala, contra §A.10 e INV-K9, que la nombran | DC34 | 5 |
| 13 | Plan de la fase, §6 (DC4) | La app de referencia nativa, como criterio de aceptación | Lote 10 | 10 |
| 14 | DC22 y DC29 (INV-K17) | "Subir el tope o descartar la sala" ante un snapshot mayor que `max_snapshot_bytes`: descartar no alcanza | DC37; INV-K17 corregida en el Lote 0, y §11 de `ARCHITECTURE.md` en el Lote 7 | 0, 7 |

---

## 2. Lotes

| Orden | Lote | Crates | Decisiones | Invariantes | Progreso |
|---|---|---|---|---|---|
| 0 | Contrato | Documentos | DC32 a DC37 | Ajustes: C3, K2, K3, K4, K7, K8, K9, K10, K12, K15, K17 y §2 | ✅ |
| 1 | `check_operation` | storage | DC33 | C3 | ✅ |
| 2 | La fachada y las lecturas | client | DC32 | K17 | ✅ |
| 3 | Las escrituras y el dueño | client | DC32, DC33 | K2, K8 | ⬜ |
| 4 | El pre-chequeo | client | DC33 | K2, K8, K17 | ⬜ |
| 5 | El cifrado de columnas | client | DC34 | K9, K12, K2 | ⬜ |
| 6 | `watch` | client | DC35 | K10, K15 | ⬜ |
| 7 | Descartar a pedido | client | DC37 | K3, K2, K10, K15, K17 | ⬜ |
| 8 | La fachada con el worker: el modo de frescura, `sync`, `Offline` y el primer token | client | DC36 | K2, K7, K17 | ⬜ |
| 9 | La simulación con la fachada | client (tests) | — | K8, K9, K10, K15 | ⬜ |
| 10 | La app de referencia nativa | client (ejemplo y tests) | DC4 | K4, K9, K10 | ⬜ |
| 11 | Revisión de cierre y cierre | Documentos | Todas | Las de §3.11 que toca 4.4, y C3 | ⬜ |

**Orden.**
- El Lote 0 va primero: solo documentos, con su revisión cruzada (`docs/METODOLOGIA.md` §2). Las *Propuestas* se confirman antes del Lote 1.
- El 1 toca `zemdb-storage` y va antes del 4, que lo usa. El 2 va antes del 3 al 8. El 3 antes del 4 y del 5 (las escrituras), el 5 antes del 6 (`watch` descifra) y el 6 antes del 7 y del 8 (el descarte termina los `watch`, y el modo `live` los cuenta).
- **Con 4.3 en paralelo:**
  - el 8 va después de los Lotes 3, 4 y 7 de 4.3;
  - el 7 detiene el worker en `discard_room` si el Lote 4 de 4.3 ya está; si no, lo suma el 8, con su test;
  - el aviso de `watch` (Lote 6) cubre el evento `ops` porque va en el punto común del aplicador; si el Lote 8 de 4.3 ya está, el 6 prueba que un `watch` emite por un `ops`, y si no, lo prueba el 9;
  - el 9 va después del Lote 9 de 4.3, porque cambian el mismo arnés de simulación;
  - el 10, después del Lote 1 de 4.3 (el transporte HTTP) y del 8.

  Las dos sub-fases cambian `src/client.rs`, `src/error.rs`, `src/config.rs`, `src/lib.rs` y `RoomStatus`: `git pull` antes de cada commit, y los cambios de 4.4 en el núcleo, chicos y `pub(crate)`, salvo `discard_room` y la cuenta de suscripciones de `RoomStatus`.
- El 9 y el 10 son pruebas y ejemplo, sin producción (salvo arreglos de lo que encuentren, que van como desvíos en su nota o como lotes nuevos).
- El 11 cierra con la revisión de cierre (§3); sus rupturas se arreglan en lotes nuevos de 4.4.

**Tamaño.** Unas 60 líneas de producción el Lote 1, 280 el 2, 170 el 3, 50 el 4, 200 el 5, 250 el 6, 70 el 7 y 170 el 8; el ejemplo del 10, unas 300, que no son producción.

**Pruebas.**
- **La fachada,** contra el servidor real en proceso y con el transporte de prueba de `crates/client/tests/common/`, como en 4.2: el transporte registra cada pedido, así que un error local se prueba viendo que no salió ningún `Commit`, y el dueño y el cifrado, mirando la operación de cada `Commit` que salió. Un `CryptoEngine` de prueba, en `tests/common/`, que autentica los datos adicionales como un AEAD (falla si no son los del cifrado). Los errores del storage, con el motor de prueba que envuelve a uno real (`TestEngine`, que suma `check_operation`). Se observa sin hacer `pub` nada interno: por la API, el transporte, `status` y el motor, que el test lee por la API de `zemdb-storage`.
- **Lo que no se puede forzar desde afuera** (una lectura durante una recarga del esquema, entre leer el esquema y leer el storage), con tests unitarios de la fachada en `src/api/tests/`.
- **`check_operation` (Lote 1),** con los tests del storage y su oráculo (`crates/storage/tests/common/oracle.rs`): sobre estados y operaciones al azar, en los dos motores, su decisión es la de `apply_batch` y la del oráculo, y no cambia nada. Una versión corta en la suite y una larga con `#[ignore]`.
- **El decodificador del texto descifrado (Lote 5),** con tests unitarios en `src/api/tests/` y fuzzing (bytes al azar por un `CryptoEngine` que los devuelve tal cual): corto en la suite y largo con `#[ignore]`.
- **`watch` (Lote 6),** con lotes al azar y un consumidor que lee a destiempo: al quedar quieta la réplica, el último valor de cada suscripción es el que da una lectura directa.
- **El claro en un snapshot o un chunk** se busca en el snapshot decodificado (su sobre, sin Zstd), no en sus bytes: el del motor en disco va comprimido.

**Checklist propio.** Los Lotes 2 a 8 recorren este checklist, además del del cliente de 4.2 (§2 de su [plan](2026-10-fase4_2-nucleo-replica.md)) para lo que cambie del núcleo. El 1, que cambia cómo decide el storage, recorre el de determinismo de la Fase 3.11 (§5.4 de su [propuesta](2026-10-fase3_11-restricciones-proposal.md)).
- Cada escritura sigue el orden de DC32, y todo error local sale antes de llamar a `write` del núcleo, sin mandar nada (INV-K2): el test lo verifica con el transporte.
- La fachada llama a `write` una vez por escritura y nunca la repite (INV-K2, INV-Y4).
- Ningún valor de una columna cifrada sale de la fachada en claro: ni en la operación, ni en un mensaje de error, ni en el storage (INV-K9).
- Las lecturas no mandan nada, y no esperan al aplicador.
- El aviso del aplicador solo marca: sin `.await`, sin E/S y sin lock que cruce un `.await`; ningún aviso se pierde entre mirar la marca y esperar.
- Nada crece por cambio aplicado: por suscripción, su filtro, su marca y su último valor (INV-K15).
- Los argumentos y los resultados de los métodos públicos son datos propios y serializables (DC14, DC32); los handles y los streams, no.
- Los cambios del núcleo son los de su lote y `pub(crate)` (salvo `discard_room` y la cuenta de suscripciones de `RoomStatus`), porque 4.3 cambia los mismos archivos en paralelo.

### Lote 0 · Contrato · ✅ Hecho
**Objetivo:** el contrato contra el que se revisa cada lote de 4.4. Solo documentos.

**Qué se hace:**
- **Este plan,** con DC32 a DC37 como *Propuestas*, lo que hereda (§1.1) y la revisión de cierre (§3).
- **`INVARIANTES.md`, versión 18** (la 17 la tomó el Lote 6 de la Fase 3.12, en paralelo):
  - **INV-C3:** verificar una operación sin aplicarla (`check_operation`, DC33) no cambia nada y da la decisión que tomaría `apply_batch` en ese estado, 🆕 (Lote 1).
  - **INV-K2:** el orden de una escritura en la fachada, con sus errores locales (`RoomClosed` sin la réplica abierta, `InvalidOperation`, la columna dueño, `Offline`, `Crypto`, `PrecheckFailed`) antes de llamar a `write`, una sola llamada a `write` por escritura, y el descarte a pedido, que termina las escrituras como un descarte, 🆕 (Lotes 3 a 5, 7 y 8).
  - **INV-K3:** el descarte a pedido de la aplicación (DC37), 🆕 (Lote 7).
  - **INV-K4:** la app de referencia nativa, en *Automatizable*, 🆕 (Lote 10).
  - **INV-K7:** la fachada arranca el worker de cada sala que abre (DC36), 🆕 (Lote 8).
  - **INV-K8:** el enunciado, de la fachada (la API de bajo nivel manda la operación como la arma quien la llama); el dueño en cada operación y la columna dueño escrita por la aplicación (DC33), 🆕 (Lotes 3 y 4).
  - **INV-K9:** el enunciado, de la fachada; los datos adicionales con la sala adentro, `NULL` cifrado, el texto descifrado validado contra el tipo de la columna, `Crypto` sin `CryptoEngine`, el claro que nunca va en un mensaje de error, el supuesto de un `CryptoEngine` que autentica los datos adicionales y el límite aceptado de un texto cifrado anterior (DC34), 🆕 (Lote 5).
  - **INV-K10:** el enunciado precisa el último valor (cuando la réplica deja de cambiar, con la réplica abierta, el que entrega es su estado), que no repite un valor y que termina al cerrar la sala; el aviso del aplicador y el valor recalculado al leerse (DC35), 🆕 (Lotes 6 y 7).
  - **INV-K12:** el texto descifrado de una columna cifrada que escribió otro miembro (DC34), 🆕 (Lote 5).
  - **INV-K15:** qué retiene cada suscripción de `watch`, el último valor de un `watch` de un `scan` entre los usos que no acota una constante, y la entrada de una sala descartada (DC35, DC37), 🆕 (Lotes 6 y 7).
  - **INV-K17:** un snapshot mayor que `max_snapshot_bytes` se repite hasta que la aplicación suba el tope (descartar la sala no alcanza, DC37); la fachada no agrega errores que detengan una sala: el handle de una sala que falla cerrada, el pre-chequeo sin decisión, el descarte a pedido y el worker (DC32, DC33, DC36, DC37), 🆕 (Lotes 2, 4, 7 y 8).
  - **§2:** el `CryptoEngine` es de la aplicación y se supone que autentica los datos adicionales; cualquier miembro puede escribir cualquier `Bytes` en una columna cifrada de las filas que puede escribir, y el SDK lo rechaza al leer si no descifra o no decodifica (DC34).
  - El resumen, con el lote de 4.4 que implementa cada parte; §1.6, con las decisiones hasta DC37.
- **El plan de la fase:** DC32 a DC37 en la tabla de decisiones, como *Propuestas*; las notas de DC36 en DC24 y DC28, y de DC37 en DC18 y DC29; en §4.4, el estado de la conexión que DC36 lee sin `status`; en §A.4, la clave de una escritura que puede traer el dueño del cliente; 4.4 en curso, con el enlace a este plan (encabezado, §4, §4.4 y §8); la frase de §4 sobre 4.3 y 4.4, que ya comparten archivos; en §A.10, los datos adicionales con la sala adentro y `NULL` cifrado (DC34).
- **`ROADMAP.md`:** 4.4 en curso, con el enlace a este plan.
- `ARCHITECTURE.md` no cambia: describe comportamiento implementado. Desde el Lote 1, cada lote suma lo suyo (§8 para `check_operation`, §3.1 para los datos adicionales, y §11 para la fachada).

**Entrega:** `docs: plan Fase 4.4 with its batches and the Rust API decisions, and adjust the client contract for it (Fase 4.4, Lote 0)`.

**Hecho.**
- **Documentos:** este plan, con DC32 a DC37 *Propuestas*, lo que hereda (§1.1) y la revisión de cierre con dos revisores (§3); `INVARIANTES.md` en la versión 18 (ajustes de C3, K2, K3, K4, K7, K8, K9, K10, K12, K15, K17 y §2, el resumen y §1.6); el plan de la fase con DC32 a DC37, las notas en DC14, DC18, DC24, DC28 y DC29, la frase de §4 sobre 4.3 y 4.4, §4.4, §A.4, §A.10 y 4.4 en curso; `ROADMAP.md` con 4.4 en curso.
- **Revisión cruzada** (agentes aparte, de contexto fresco y solo lectura), en tres rondas, cada una acotada a las correcciones de la anterior:
  1. **2 bloqueantes, 10 ajustes y menores.**
     - **Los bloqueantes:**
       - el orden de una escritura validaba con `Schema::validate_operation` antes del dueño y del cifrado, y eso falla siempre: exige `Bytes` en una columna cifrada y el dueño declarado, y `check_operation_size` sobre el claro no acota la operación cifrada. Ahora es el orden de DC32;
       - INV-K8 e INV-K9 hablaban de "el SDK", pero `Client::write` es público y no valida nada: ahora son de la fachada, y la API de bajo nivel manda la operación como la arma quien la llama.
     - **Los ajustes:** el handle de una sala que falla cerrada al abrirse; `Offline` sin `status`, que da `RoomClosed` en medio de un cambio, y la escritura con la réplica vacía; la carrera entre dos cambios del modo `live` (un lock por sala); el primer token sin red; el último `Ack` del worker en el descarte, y quién conecta `stop_worker` si el Lote 4 de 4.3 llega después; el aviso de `watch` en el punto común de `apply_batch`, y quién prueba el evento `ops`; descartar la sala no es la salida de un snapshot mayor que el tope (también en INV-K17); los mensajes del núcleo que llevan valores; las listas de lotes e invariantes; y tres tests que no detectaban lo que decían (el claro en un snapshot comprimido, el negativo del Lote 1, las columnas cifradas antes del Lote 5).
     - **Los menores:** la frase de §4 del plan de la fase; la cuenta de suscripciones en `RoomStatus`, pública; las notas en DC18 y DC24; que §2 no cambia el modelo de amenaza; `TableNotFound` en `check_operation`; el supuesto de un AEAD, el texto cifrado anterior y la clave canónica con `-0.0`; los detalles de `watch` (registrar antes de leer, la réplica vacía, el error que no cambia el último valor); la época del descarte, que sube dos veces; la clave que devuelve `insert`; `Drop` fuera de un runtime; el motor de prueba y el test unitario de la recarga.
  2. **3 ajustes y 8 menores.** Una escritura durante una reapertura de la sala del storage es `RoomClosed`, como una lectura; un `RoomFailed` en el pre-chequeo frenaba las escrituras de una sala en disco que la compactación marcó como fallida, hasta que algo la reabriera (ahora un error del storage deja el pre-chequeo sin decisión, y `check_operation` lee el estado publicado, sin el mutex del WAL); y correcciones que no habían llegado al plan de la fase. Los menores: un `Bytes` en `notes` es `InvalidOperation`; el reenvío de `check_operation` en el motor de prueba; K15 en el Lote 7 y K2 sin el Lote 2; la precisión de DC22 y DC29; `check_operation_size` con el peor caso; `room` en una sala sin metadatos; "con la réplica abierta" en INV-K10; y §0.
  3. **Verde,** con 7 menores, corregidos: las excepciones del pre-chequeo en los resúmenes; los lotes de INV-K17 (2, 4, 7 y 8); el primer token sin red, solo para una sala con metadatos; los mensajes que arma la fachada en INV-K9; la clave con el dueño del cliente en §A.4 y en DC14; las entradas *Automatizable*, con un mismo criterio, y el test de una escritura durante el descarte; y un `update` sin cambios.
- **Desvíos:**
  - la versión 18 de `INVARIANTES.md`: la 17 la tomó el Lote 6 de la Fase 3.12, en paralelo;
  - los ajustes de INV-K4, K7 y K17 y las notas en DC14, DC22, DC28 y DC29 no estaban en la lista inicial: salieron de la revisión.
- **Chequeos:** el lote solo toca documentos, así que los de código no cambian; `git diff --check` limpio, y las listas de lotes e invariantes, verificadas con un script.
- **Las Propuestas:** el dueño del proyecto adoptó DC32 a DC37 el 7 de Noviembre de 2026, sin cambios.

### Lote 1 · `check_operation` (`zemdb-storage`) · ✅ Hecho
**Objetivo:** que el pre-chequeo del SDK pregunte al storage qué decidiría la réplica, con la misma regla que aplica el log (DC33).

**Archivos:** `crates/storage/src/engine.rs` (el método del trait), `memory/state.rs` (la decisión, separada de `apply_operation`), `memory/mod.rs`, `disk/mod.rs`, y sus tests (los unitarios de `memory/tests/` y de integración con el oráculo); en `crates/client/tests/common/`, el reenvío de `check_operation` en el motor de prueba (`TestEngine`), para que la suite del cliente siga compilando. `ARCHITECTURE.md` §8.

**Qué se hace:** separar de `apply_operation` la parte que decide (las reglas de §4 de `ARCHITECTURE.md`, que ya corren antes de tocar la fila y sus índices) en una función que no muta, que `apply_operation` llama antes de aplicar; `StorageEngine::check_operation`, en los dos motores, que verifica la operación como `apply_batch` (la tabla y el esquema) y devuelve esa decisión sobre el estado publicado, sin cambiar nada (en disco, bajo el lock de lectura del estado, sin el mutex del WAL ni sus puntos de falla).

**Tests exigidos.**
- Para cada motivo (`MissingRow`, `Duplicate`, `NotOwner`, `Unique`, `ForeignKey`, `Restrict`), `check_operation` lo da en un estado donde `apply_batch` lo da, con el mismo nombre de restricción, y `None` donde la aplicaría; el `DELETE` de una fila que no existe, `None`. En los dos motores.
- No cambia nada: el head, las filas (un `scan` igual antes y después) y los índices (después de un `check_operation` que decidiría aplicarla o rechazarla, un `apply_batch` decide lo mismo que sin él).
- Una tabla que la sala no tiene es `TableNotFound`; una operación que no valida con el esquema de la sala, `SchemaValidation`; una sala cerrada, `RoomNotFound`. En disco, una sala marcada como fallida decide sobre su estado publicado, y el `apply_batch` siguiente da `RoomFailed`, como hoy.
- Después de `reload_schema`, decide con las restricciones nuevas; en disco, después de reabrir la sala y durante una compactación.
- Propiedad, con el oráculo: estados al azar (los generadores de los tests del storage) y una operación al azar; su decisión es la de `apply_batch` sobre ese estado y la del oráculo, en los dos motores (INV-S6).
- La mutación inversa del lote, con mutaciones reales: un `check_operation` que saltea las claves foráneas, y uno que aplica la operación en lugar de solo decidirla; con cada una, algún test falla.

**Interacciones a revisar:** `apply_batch` y la recuperación del motor en disco, que comparten la función (el replay decide igual: INV-S3, INV-C1); los índices evolucionados de `reload_schema` (INV-S7); el estado que reemplaza la compactación y la instalación de un snapshot; los puntos de falla de `room_data.build` y del WAL, que `check_operation` no recorre; la compilación a wasm32 (el motor en memoria).

**Documentos:** `ARCHITECTURE.md` §8 (`check_operation`); `INVARIANTES.md`: C3.

**Entrega:** `feat(storage): check an operation without applying it, for the precheck of the client (Fase 4.4, Lote 1)`.

**Hecho** (9 de Noviembre de 2026).
- **Qué entró.**
  - **La decisión, separada** (`memory/state.rs`): `decide(data, schema, op, old)` decide sin mutar nada y devuelve el cambio (la fila que queda bajo la clave, o ninguna en un `DELETE`), `Ok(None)` si no cambia nada, o el motivo del rechazo. Es el mismo código que tenía `apply_operation`, con las reglas en el mismo orden, el mismo ensanchado de un `UPDATE` y los mismos motivos. `apply_operation` la llama y aplica el cambio a las filas y los índices, también en la recuperación del motor en disco, que no valida otra vez. Toma la operación por valor, así que el camino de `apply_batch` arma la fila con los valores de la operación sin copiarlos, como antes.
  - **`check_operation(room_id, schema, data, op)`** (`state.rs`): valida la operación como un lote (`validate_operation`, que ahora usa también `validate_batch`: `TableNotFound` y `SchemaValidation`) y devuelve el motivo de `decide`.
  - **`StorageEngine::check_operation(room_id, &op)`**, en los dos motores. La operación se clona antes del lock, y la decisión corre bajo el lock de lectura de la sala en memoria, o del estado de la sala en disco, sin el mutex del WAL: en disco no espera el `fsync` de un lote, una rotación del WAL ni una compactación, no mira si la sala está marcada como fallida y no recorre los puntos de falla del WAL. En memoria, el lock de lectura espera a un lote que se está aplicando, sin E/S, como `get` y `scan`. Una sala cerrada es `RoomNotFound`.
  - **El reenvío en `TestEngine`** (`crates/client/tests/common/`), que puede fallar a pedido como los demás métodos.
- **Tests.**
  - **De integración** (`crates/storage/tests/check_operation_tests.rs`), en los dos motores:
    - cada motivo (`MissingRow`, `Duplicate`, `NotOwner`, también en un `DELETE`, `Unique`, `ForeignKey` y `Restrict`), con el nombre de la restricción, donde `apply_batch` lo da después, y `None` donde la aplica; el `DELETE` de una fila que no existe, `None`;
    - no cambia nada: el head y las filas, y después de verificar un `INSERT` que se aplicaría y un `DELETE` que se rechazaría, `apply_batch` decide como sin ellos;
    - `TableNotFound`, `SchemaValidation`, y `RoomNotFound` con una sala que nunca se abrió y con una cerrada;
    - después de `reload_schema`, decide con las restricciones nuevas (`posts_tag` y `children_alt`), donde antes era `SchemaValidation`;
    - después de `apply_snapshot`, decide sobre las filas del snapshot, con su esquema, y el lote siguiente decide igual;
    - una sala en disco reabierta desde un snapshot y el WAL decide sobre lo que recupera;
    - la propiedad con el oráculo: un log al azar con una recarga del esquema en un punto al azar, compactaciones y reaperturas del motor en disco; antes de cada lote, operaciones al azar que cada motor decide como el oráculo (`Oracle::decide`, que aplica sobre un clon del oráculo, sin código de los motores), y la primera del lote, que `apply_batch` decide igual después. Una versión corta (12 logs) y una larga con `#[ignore]` (400).
  - **Unitarios:**
    - `memory/tests/state.rs`: sobre estados al azar que ya rompen alguna regla (filas al azar sin verificar), la decisión de `check_operation` es la de `apply_operation`;
    - un escenario compartido por los dos motores (`memory/tests/index.rs`): las verificaciones de un log al azar, con una recarga a la mitad, dan la decisión del oráculo y no cambian las filas, los índices (entrada por entrada) ni el head; la primera operación de cada lote se decide como la decide `apply_batch`; aparecen los seis motivos;
    - en disco (`src/tests/disk.rs`): una verificación no espera a un lote en su `fsync` (decide sobre el estado anterior, que no es durable todavía) ni a una rotación del WAL; durante una compactación decide sobre los lotes que entraron mientras tanto, y lo mismo después de reabrir; una sala marcada como fallida verifica sobre el estado que publica, y el lote siguiente da `RoomFailed`.
- **Mutaciones inversas: las 8 las mató algún test.**
  - Un `check_operation` que saltea las claves foráneas, y otro que saltea la regla del dueño.
  - Uno que ignora la fila existente.
  - Uno que no valida la operación.
  - Uno que aplica la operación en lugar de solo decidirla, en memoria y en disco.
  - En disco, uno que toma el mutex del WAL, y otro que rechaza una sala marcada como fallida.
- **El checklist de determinismo de la Fase 3.11** (§5.4 de su propuesta), punto por punto. La decisión depende solo de las filas y los índices anteriores, de la operación y del esquema (`decide`, con la fila existente tomada de los mismos datos), y en su camino no se itera un `HashMap`: solo hay búsquedas. La igualdad es la de `Value`. Aplicar, reaplicar el WAL y verificar usan la misma función, en los dos motores. Se decide con el esquema publicado, el que usaría `apply_batch` en ese momento. El motivo es el mismo, y verificar no cambia nada ni avanza el head.
- **Interacciones revisadas.**
  - `apply_batch` y la recuperación del motor en disco comparten `decide` por `apply_operation`, sin cambios en su camino (INV-S3, INV-C1).
  - Los índices evolucionados de `reload_schema` (INV-S7), y el estado que reemplazan la compactación y la instalación de un snapshot: `check_operation` lee el esquema y los datos bajo el mismo lock con que se publican juntos.
  - Los puntos de falla de `room_data.build` y del WAL, que `check_operation` no recorre.
  - La compilación a wasm32 del motor en memoria, con clippy.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura, que probó en su propia copia, con sus propias mutaciones y los tests largos en release), **primera ronda: verde.** Ningún hallazgo Alto ni Medio; recorrió el checklist de §5.4 punto por punto. Sus cuatro Bajos se arreglaron en el lote:
  - el rustdoc del trait decía que no espera a un lote en curso, y eso vale solo en disco: el lock de lectura en memoria espera a un lote que se está aplicando, sin E/S; y el estado sobre el que decide puede venir también de una recarga del esquema o de un snapshot;
  - `ARCHITECTURE.md` decía que el cliente ya lo pregunta antes de mandar una escritura, y eso llega con el Lote 4;
  - faltaba un test después de `apply_snapshot`, que ahora está, en los dos motores;
  - este plan, que faltaba actualizar.
- **Desvíos.**
  - La versión 21 de `INVARIANTES.md`: la 19 la tomó el Lote 1 de 4.3, y la 20, el Lote 6b de la Fase 3.12, en paralelo.
  - `apply_operation` busca la tabla del esquema dos veces, una dentro de `decide` y otra para actualizar los índices: es un costo despreciable.
- **Chequeos.**
  - fmt, clippy del workspace y de wasm32 con `-D warnings`, la suite del workspace (1309 tests, sin fallas; después de los arreglos de la revisión, la del storage otra vez, con el test de `apply_snapshot`), doc con `-D warnings` y el build a wasm32.
  - Los `--ignored` del storage en release: 22, sin fallas.

### Lote 2 · La fachada y las lecturas (`zemdb-client`) · ✅ Hecho
**Objetivo:** los handles de DC32 y las lecturas locales, sin escrituras todavía.

**Archivos:** `src/api/` (nuevo: el cliente, la sala, la tabla, las filas, las claves y el `scan`, con sus tests unitarios en `src/api/tests/`), `src/lib.rs`, `src/client.rs` (el esquema de una réplica abierta y el motor, `pub(crate)`), `src/error.rs` (`InvalidOperation` y `Crypto`); `tests/facade_tests.rs`. `ARCHITECTURE.md` §11 (*Facade*).

**Qué se hace:** `ZemdbClient` (`open`, con el `CryptoEngine` opcional, `client_id`, `room`, `shutdown`), `RoomHandle` (`table`, `status`, `close`) y `TableHandle` (`get`, `scan`); `Row`, `Key` y el `scan` con `serde`; la conversión entre nombres y posiciones con el esquema de la réplica; la validación de las claves y del `scan` contra el esquema (`InvalidOperation`); el handle de una sala que falla cerrada al abrirse. Hasta el Lote 5, una lectura que devuelve una columna cifrada es `Crypto`.

**Tests exigidos.**
- Las filas de una sala al día con el servidor en proceso, por nombre: `get` de una fila de otro miembro (con la columna dueño en la clave), y `scan` con un rango, hacia atrás, con límite y con columnas; una fila corta, escrita antes de agregar una columna, la trae en `NULL`.
- Sin red (el transporte de prueba no responde), las lecturas devuelven lo que tiene la réplica y no mandan nada.
- Una sala que falla cerrada al abrirse (un esquema que no evoluciona): `room` devuelve el handle, `status` dice que falla cerrada, y la sala se lee.
- `RoomClosed` para una sala que no se abrió, una cerrada, una vacía después de un descarte por `RoomReset` (antes de `Registered`) y después de `shutdown`.
- Negativos: una tabla, una columna o una clave que no son del esquema (una columna de más o de menos en la clave, un valor de otro tipo) son `InvalidOperation`; una lectura que devuelve una columna cifrada es `Crypto`.
- Ida y vuelta con `serde` de `Row`, `Key`, el `scan` y cada `ClientError`.
- Unitario: una lectura con una recarga del esquema entre leer el esquema y leer el storage se lee con las columnas del esquema que leyó.

**Interacciones a revisar:** el aplicador (las lecturas no lo toman: durante una reapertura, `RoomClosed`); el cambio de esquema entre leer el esquema y leer el storage (DC20); el descarte por `RoomReset` (la réplica vacía); `open_room` sobre una sala abierta (otro handle, la misma sala) y sobre una que falla cerrada (`FailedClosed` con la réplica abierta); los cambios de 4.3 en `client.rs` (solo accesos `pub(crate)`); el runtime del Worker de 4.5 (datos serializables).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K17 (el handle de una sala que falla cerrada).

**Entrega:** `feat(client): the Rust facade, its handles and the local reads (Fase 4.4, Lote 2)`.

**Hecho** (10 de Noviembre de 2026).
- **Qué entró.**
  - **Los handles** (`src/api/`): `ZemdbClient` (`open`, con el `CryptoEngine` opcional como `Option<Arc<dyn CryptoEngine>>`, `client_id`, `room` y `shutdown`), `RoomHandle` (`room_id`, `table`, `status` y `close`) y `TableHandle` (`name`, `room`, `get` y `scan`). Se clonan barato: comparten el cliente por un `Arc`. Un `RoomHandle` nombra su sala por el id, así que el handle de una sala cerrada y abierta otra vez la alcanza otra vez. `room` devuelve el handle también cuando `open_room` termina en `FailedClosed` con la réplica abierta, y el error de `open_room` en cualquier otro caso (una sala sin metadatos y sin respuesta, `Network`).
  - **Las filas, las claves y el `scan`** (`row.rs`, `scan.rs`): `Row` y `Key`, mapas de columnas por nombre (`serde` transparente sobre un `BTreeMap<String, Value>`), y `Scan` (los dos límites del rango, cada uno una clave entera, la `Direction`, el límite y las columnas), con `serde` y métodos para armarlos.
  - **Las lecturas** (`table.rs`): una lectura se planea contra el esquema de la réplica, con su época, bajo el lock síncrono de la sala y sin el aplicador (`Read::get` y `Read::scan`: la tabla, la clave, el rango y las columnas, por nombre), y después lee el storage por el id de la tabla (`get_by_id` y `scan_by_id`). Las filas toman los nombres de las columnas leídas: una fila corta tiene las que le faltan en `NULL`, y los valores que una versión posterior del esquema agregó al final quedan afuera. Después de leer, si la réplica ya no es de la misma época (un descarte en el medio, después del cual la sala del storage puede tener otra encarnación, con otro esquema), la lectura es `RoomClosed`, igual que si la sala del storage estaba cerrada cuando la lectura llegó a ella (`RoomNotFound`); una sala cerrada y abierta otra vez antes de que la lectura llegue se lee, como abrió.
  - **Los errores** (`error.rs`): `InvalidOperation` (una tabla, una columna o una clave que no son del esquema, un valor de otro tipo o un `NULL` en la clave, una columna pedida dos veces) y `Crypto` (una lectura que daría una columna cifrada, con o sin `CryptoEngine`, después de validar la lectura, como una escritura valida por nombre antes de cifrar). Sus mensajes nombran tablas, columnas y tipos, nunca valores.
  - **Del núcleo, `pub(crate)`** (`client.rs`): `read_schema` (lo que la fachada lee del esquema de una réplica abierta, con su época), `is_of_epoch` (si la réplica sigue en esa época) y `storage` (el motor).
- **Tests.**
  - **De integración** (`tests/facade_tests.rs`), contra el servidor en proceso, con una tabla con el dueño en la clave (`members`) y otra con una columna cifrada (`rounds`); la réplica se pone al día antes con el `Client` del núcleo, porque la fachada no sincroniza hasta el Lote 8:
    - las filas por nombre: `get` de una fila de otro miembro, con la columna dueño en la clave, y una fila corta, escrita antes de agregar una columna, con ella en `NULL`; `scan` con rangos incluidos, excluidos y abiertos, hacia atrás, con límite (también 0), con columnas, y un rango invertido, que no da filas; otro handle de la sala lee lo mismo; nada se manda;
    - sin red, una sala con metadatos abre sin registrarse y se lee, sin mandar nada;
    - una sala que falla cerrada al abrirse tiene su handle, su `status` lo dice, y se lee, también desde otro handle;
    - otro error de la apertura que fallar cerrada (`Storage`, al leer el head para el registro) no da el handle, aunque la réplica quede abierta;
    - `RoomClosed`: cerrada, con su apertura fallida, con la sala del storage cerrada durante una lectura (`RoomNotFound` del motor de prueba; otro error del motor es `Storage`), vacía después de un descarte por `RoomReset` hasta `Registered`, y después de `shutdown`; una sala sin metadatos y sin red no tiene handle (`Network`);
    - una lectura no espera a un cambio de la réplica en curso: con la recarga del esquema retenida bajo el aplicador (el motor de prueba suma `hold_next`), lee con el esquema de antes;
    - un descarte mientras una lectura está en el storage la hace `RoomClosed`;
    - los negativos: una tabla, una columna o una clave que no son del esquema (una columna de más o de menos, otro tipo, `NULL`), en `get` y en los límites del `scan`, y una columna pedida dos veces, son `InvalidOperation`, sin nombrar los valores; `get` y `scan` de `rounds` que dan `notes` son `Crypto`, con y sin `CryptoEngine`, y un `scan` que no la pide lee la ronda que bob abrió con texto cifrado en ella;
    - la ida y vuelta por JSON de `Row` (con cada tipo de `Value`), `Key`, `Scan`, `Direction`, `RoomStatus` y cada `ClientError`.
  - **Unitarios** (`src/api/tests/table.rs`), con un servidor de mentira que solo responde registros:
    - una lectura planeada antes de que la sala del storage recargue el esquema y tome filas con la columna nueva da las columnas del esquema que leyó;
    - una lectura planeada antes de cerrar y abrir la sala en la misma época lee; una planeada antes de un descarte por una sala creada otra vez, cuya tabla con el mismo id es otra con una fila de la misma clave (un entero), es `RoomClosed`, en `get` y en `scan`;
    - una fila del storage, corta o con valores de más, toma los nombres de las columnas de la lectura.
- **Mutaciones inversas: las 17 las mató algún test.**
  - Una lectura que no verifica la época, que la verifica antes de leer el storage en lugar de después, o una verificación que ignora la época.
  - Una lectura que se niega mientras un cambio de la réplica está en curso (en lugar de leer sin el aplicador).
  - Una sala del storage cerrada en el medio que da `Storage` en lugar de `RoomClosed`.
  - Una fila corta que no trae en `NULL` las columnas que le faltan.
  - Una clave que acepta otra columna, o cualquier valor.
  - Un `scan` que acepta una columna dos veces, que ignora sus columnas, su dirección o su límite.
  - Un `get` o un `scan` que dan columnas cifradas, y un `get` que da `Crypto` antes de validar la clave.
  - `room` sin el handle de una sala que falla cerrada, y `room` con el handle después de cualquier error con la réplica abierta.
- **El checklist propio** (§2), en lo que toca a un lote de lecturas:
  - las lecturas no mandan nada (el transporte de prueba registra también lo que se manda sin red) y no esperan al aplicador (el test con la recarga retenida);
  - los argumentos y los resultados son datos de la fachada, serializables (la ida y vuelta por JSON); los handles no;
  - los cambios del núcleo son tres accesos `pub(crate)`, que usa la producción;
  - ningún mensaje de la fachada lleva valores (los tests de los negativos buscan el valor en el mensaje).
  - Del checklist del cliente de 4.2, para lo que cambia del núcleo: los accesos leen el estado de la sala bajo su lock síncrono, sin `.await` con él tomado, y no cambian nada.
- **Interacciones revisadas.**
  - El aplicador: las lecturas no lo toman. Durante la reapertura de la sala del storage (DC21) o un cierre, la lectura que llega a la sala cerrada es `RoomClosed`; con el motor en disco, una lectura espera mientras el motor abre o borra una sala del storage (de cualquier sala), porque eso toma su mapa de salas.
  - El cambio de esquema entre leer el esquema y leer el storage (DC20): el storage recarga antes de que la réplica tome el esquema, y el esquema solo crece, así que los ids de las tablas y las posiciones de las columnas leídas siguen valiendo.
  - El descarte por `RoomReset`: la réplica vacía es `RoomClosed`, y un descarte en el medio de una lectura también, por la época.
  - `open_room` sobre una sala abierta (otro handle, la misma sala) y sobre una que falla cerrada (el handle, también en un segundo `room`).
  - Los cambios de 4.3 en `client.rs`: solo los tres accesos, al lado de `room`.
  - El runtime del Worker de 4.5: `Row`, `Key`, `Scan`, `RoomStatus` y `ClientError` pasan por `serde`; `ZemdbClient`, `RoomHandle` y `TableHandle` son `Send + Sync`.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura, que probó en su propia copia con sus propias mutaciones), **primera ronda: verde.** Ningún hallazgo Alto ni Medio. Sus seis Bajos se arreglaron en el lote:
  - `ARCHITECTURE.md` y el rustdoc de `get` prometían `RoomClosed` para una sala cerrada y abierta otra vez durante la lectura, y la lectura la lee si la sala ya abrió cuando llega a ella: se precisaron, y la verificación quedó solo en la época (`is_of_epoch`), que es lo que impide leer otra encarnación;
  - con el motor en disco, una lectura espera a la apertura o al borrado de una sala del storage, que toman el mapa de salas del motor: se documentó (§11 de `ARCHITECTURE.md` y las interacciones de arriba);
  - ningún test fijaba que `room` devuelve otro error de `open_room` con la réplica abierta: ahora uno lo hace, y su mutación está arriba;
  - el unitario del descarte buscaba una clave de otro tipo que la fila de la otra tabla: ahora es la misma, y el `get` sin la verificación de la época leería esa fila;
  - el `scan` de `rounds` que no pide `notes` leía una tabla vacía: ahora lee una ronda con texto cifrado;
  - el encabezado de este plan, que faltaba actualizar.
- **Desvíos.**
  - El motor de prueba de `tests/common/` suma `hold_next` (retener la próxima llamada a `reload_schema` o `get_by_id`), para los dos tests de lecturas durante un cambio, que el plan no listaba.
  - La guarda de `Crypto` va después de la validación por nombre: una lectura inválida para el esquema es `InvalidOperation` aunque toque una columna cifrada, como una escritura valida por nombre antes de cifrar (DC32).
- **Chequeos.**
  - fmt, clippy del workspace y de wasm32 con `-D warnings`, doc con `-D warnings` y el build a wasm32.
  - La suite del workspace (1364 tests, sin fallas, antes de los arreglos de la revisión); después de ellos, otra vez fmt, clippy, wasm32 y doc, la suite del cliente (213 tests, sin fallas) y las mutaciones.
  - Los `--ignored` del cliente en release: 3, sin fallas.

### Lote 3 · Las escrituras y el dueño (`zemdb-client`) · ⬜
**Objetivo:** `insert`, `update` y `delete` con el dueño completado por el SDK (DC32, DC33).

**Archivos:** `src/api/` (la tabla y el dueño); `tests/facade_write_tests.rs`. `ARCHITECTURE.md` §11.

**Qué se hace:** el orden de DC32 hasta el paso 8, sin `Offline`, el cifrado ni el pre-chequeo, que llegan en sus lotes: la sala con la réplica abierta, la validación por nombre, el dueño, la validación de la operación que se manda (`Schema::validate_operation`, `check_operation_size` y `Schema::check_owner`) y una llamada a `write`. Hasta el Lote 5, una escritura que toca una columna cifrada con un valor de su tipo real o con `NULL` (también por omitir una nullable) es `Crypto`, sin mandar nada; con otro tipo (un `Bytes` en una columna `String`), `InvalidOperation` en el paso 2. `insert` devuelve la clave, con la columna dueño.

**Tests exigidos.**
- En una tabla con el dueño en la clave (`members` de la app de referencia) y en una con el dueño fuera de ella: `insert`, `update` y `delete` salen con el dueño de la persona (en la operación de cada `Commit` que registra el transporte, `TableSchema::declared_owner` es `ClientId::owner`) y el servidor los acepta; una tabla sin dueño no lleva nada. La clave que devuelve `insert` sirve para `get`, `update` y `delete`.
- La columna dueño en la fila o en los cambios, o con otro dueño en la clave de una escritura, es `InvalidOperation`, y no sale ningún `Commit`; también una columna de la clave en los cambios de un `update`, un `update` sin cambios, una columna obligatoria que falta, una operación más grande que el tope, y una sala con la réplica vacía o en medio de una reapertura de la sala del storage (`RoomClosed`).
- Hasta el Lote 5: una escritura que toca `notes` (una columna `String` cifrada) con un `String` o con `NULL`, o que la omite, es `Crypto`; con un `Bytes`, `InvalidOperation`. En los dos casos no sale ningún `Commit`.
- Las decisiones de la réplica llegan como en 4.2: `Ok`, `ConstraintViolation` (`Unique`, `ForeignKey`, `Restrict`, `Duplicate` en una tabla inmutable, `NotOwner` al cambiar una fila ajena con el dueño fuera de la clave) y `RowMissing`.
- Dos dispositivos de una persona (DC15): uno cambia la fila que escribió el otro, y se acepta.
- Una escritura incierta (una respuesta perdida, con el transporte de prueba) termina `OutcomeUnknown`, y todos sus intentos llevan el mismo `MutationId`: la fachada no la repite.

**Interacciones a revisar:** `write` del núcleo (DC6, DC21), que no cambia; la verificación del dueño en el servidor (`Schema::check_owner`, INV-A5) y en la réplica (`NotOwner`); `MissingOwner` y `UnexpectedOwner` de `validate_operation`; `ClientId::owner` con dispositivos (DC15); una tabla inmutable (`Insert` solamente); una sala que falla cerrada (`FailedClosed` del núcleo).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K2, K8.

**Entrega:** `feat(client): writes through the facade, with the owner filled by the client (Fase 4.4, Lote 3)`.

### Lote 4 · El pre-chequeo (`zemdb-client`) · ⬜
**Objetivo:** que una escritura que la réplica local ya rechazaría no salga (DC33).

**Archivos:** `src/api/` (la tabla), `src/config.rs` (`precheck`), `src/error.rs` (`PrecheckFailed`); `tests/common/` (que el motor de prueba haga fallar `check_operation` a pedido) y `tests/precheck_tests.rs`. `ARCHITECTURE.md` §11.

**Qué se hace:** el paso 7 de DC32: `check_operation` sobre la operación que se manda, justo antes de `write`; `PrecheckFailed` con el motivo; `ClientConfig::precheck`, activado por defecto.

**Tests exigidos.**
- Cada motivo (`Unique`, `ForeignKey`, `Restrict`, `Duplicate`, `NotOwner`, `RowMissing`) es `PrecheckFailed` con ese motivo, y no sale ningún `Commit`.
- Con el pre-chequeo apagado, la misma escritura sale y la réplica decide (`ConstraintViolation`).
- Una réplica atrasada frena una escritura que el log habría aceptado, y después de un `sync` pasa (el comportamiento documentado).
- Una escritura que pasa el pre-chequeo y que otra, secuenciada antes, hace rechazar, termina con la decisión de la réplica (`ConstraintViolation`).
- Un `SchemaValidation` o un `TableNotFound` en el pre-chequeo (con el motor de prueba) terminan la escritura con `InvalidOperation`, y un `RoomNotFound` con `RoomClosed`, sin mandar nada; cualquier otro error (`Io`, `RoomFailed`) deja el pre-chequeo sin decisión, la escritura sale y la réplica decide.

**Interacciones a revisar:** `check_operation` y el aplicador (no lo toma); una sala del motor en disco marcada como fallida por la compactación, que se reabre con el `apply_batch` del catch-up (DC21); el cifrado del Lote 5 (la decisión sobre el texto cifrado, porque las restricciones no incluyen columnas cifradas); `RoomNotFound` durante una reapertura; INV-K2.

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K2, K8, K17 (el pre-chequeo sin decisión).

**Entrega:** `feat(client): the local precheck of a write (Fase 4.4, Lote 4)`.

### Lote 5 · El cifrado de columnas (`zemdb-client`) · ⬜
**Objetivo:** que una columna cifrada nunca salga de la fachada ni se guarde en claro, y que lo que no descifra sea un error (DC34).

**Archivos:** `src/api/` (el cifrado, con sus tests unitarios en `src/api/tests/`), `src/error.rs` (`Decryption`, y el sentido final de `Crypto`); `tests/common/` (el `CryptoEngine` de prueba) y `tests/crypto_tests.rs`. `ARCHITECTURE.md` §3.1 (los datos adicionales) y §11.

**Qué se hace:** el paso 5 de DC32: cifrar los valores de las columnas cifradas de un `insert` y de un `update`, también `NULL`, con los datos adicionales de DC34; descifrar al leer, con el texto decodificado con el tope de su largo y validado contra el tipo de la columna; `Crypto` sin `CryptoEngine` o si cifrar falla, y `Decryption` si no descifra.

**Tests exigidos.**
- Un `insert` y un `update` de `notes` (la app de referencia): la operación del `Commit` lleva `Value::Bytes` que no contienen el texto en claro, el storage guarda el texto cifrado (leído por la API de `zemdb-storage`), y el snapshot que sube el designado, decodificado, tampoco lo contiene; `get` y `scan` devuelven el valor con su tipo real. Un `NULL`, también el de una columna nullable que el `insert` no nombra, sale cifrado y se lee `NULL`.
- Un texto cifrado copiado a otra fila, a otra columna o a otra sala (con el mismo `CryptoEngine`) es `Decryption`; un `scan` que no pide esa columna lee el resto.
- Un `update` que nombra la fila con `-0.0` en un `Float` de la clave, guardada con `0.0`, se descifra.
- Sin `CryptoEngine`, o con uno que falla al cifrar, una escritura que toca una columna cifrada es `Crypto`, y no sale ningún `Commit`; una lectura, `Crypto`.
- Un texto descifrado que es otro tipo, que tiene bytes de sobra o que declara un largo enorme es `Decryption`, sin pánico ni una reserva de ese largo; fuzzing con bytes al azar (corto en la suite, largo con `#[ignore]`).
- Una columna cifrada agregada después: las filas cortas la leen `NULL`.
- Ningún mensaje de error de la fachada lleva un valor (un `InvalidOperation` y un `Decryption` sobre `notes`).

**Interacciones a revisar:** el pre-chequeo (sobre el texto cifrado); el dueño y la clave (nunca cifrados); la validación del servidor (solo `Bytes` o `NULL` en una columna cifrada) y `check_operation_size` sobre el texto cifrado; el cambio de esquema (las columnas agregadas al final); la subida del snapshot del designado (el estado del storage, cifrado); los mensajes del núcleo, que solo ven el texto cifrado; `watch` del Lote 6, que descifra en cada valor.

**Documentos:** `ARCHITECTURE.md` §3.1 y §11; `INVARIANTES.md`: K9, K12, K2.

**Entrega:** `feat(client): end-to-end encrypted columns through the facade (Fase 4.4, Lote 5)`.

### Lote 6 · `watch` (`zemdb-client`) · ⬜
**Objetivo:** suscripciones que reflejan la réplica, con memoria constante por suscripción (DC35).

**Archivos:** `src/api/` (`ChangeStream`), `src/client.rs` (el registro de las suscripciones en la sala, el aviso en las tareas del aplicador, y su cuenta en `RoomStatus`); `tests/watch_tests.rs`. `ARCHITECTURE.md` §11 (*Watch*).

**Qué se hace:** `watch` y `watch_scan`; el aviso del aplicador en el punto común de `apply_batch` (las claves de las operaciones aplicadas) y al instalar un snapshot, reabrir la sala del storage, recargar el esquema, abrir la réplica y descartarla ("todo"); la marca, el valor recalculado al leerse y la comparación con el último emitido; el fin del stream cuando la sala queda cerrada; la cuenta de suscripciones en `status`.

**Tests exigidos.**
- El primer elemento es el valor actual; después emite por una escritura propia (su catch-up) y por la de otro cliente que aplica un sync.
- No emite por una operación rechazada (el `insert` de otro cliente que viola un `UNIQUE` sobre la clave que mira), ni por un cambio en otra clave, ni, en un `watch_scan`, por un cambio de la tabla fuera de su rango.
- Un `watch_scan` de `votes` actualiza la tabla de posiciones de la app de referencia mientras llegan votos.
- Después de instalar un snapshot, de una reapertura por un error del storage (con el motor de prueba), de agregar una columna (aparece en `NULL`) y de un descarte por `RoomReset` (no emite con la réplica vacía, y después emite el estado nuevo), el valor es el de una lectura directa; un `watch` creado sobre la réplica vacía da su primer elemento al abrirse.
- Un consumidor lento: muchos lotes sin leer, y el siguiente elemento es el valor actual, sin los intermedios; `status` cuenta una suscripción por stream, sin importar cuántos lotes pasaron.
- El stream termina al cerrar la sala y en `shutdown`, no en una reapertura; soltarlo quita la suscripción (`status`); soltarlo en cada `.await` de una lectura no deja nada.
- Propiedad: lotes al azar con lecturas a destiempo; al quedar quieta la réplica, el último valor de cada suscripción es el de una lectura directa.
- Un error al leer (`Decryption`, con el `CryptoEngine` de prueba) es un elemento, el stream sigue, y el valor siguiente se compara con el último valor emitido.
- Si el Lote 8 de 4.3 ya está: un `watch` emite por un evento `ops`.

**Interacciones a revisar:** cada cambio del aplicador (DC21: aplicar, instalar, reabrir, el esquema, abrir, descartar, cerrar), incluido lo que 4.3 hace pasar por él (el sync del worker y el evento `ops`); las escrituras en vuelo, que el aviso no toca; la época (un descarte por D78 deja la misma encarnación); los cortes (soltar un stream en cada `.await`); los cambios de 4.3 en `client.rs` y en `RoomStatus`.

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K10, K15.

**Entrega:** `feat(client): watch and change streams over the replica (Fase 4.4, Lote 6)`.

### Lote 7 · Descartar a pedido (`zemdb-client`) · ⬜
**Objetivo:** la salida de los casos que fallan cerrado (DC37).

**Archivos:** `src/client.rs` (`discard_room`), `src/api/` (`RoomHandle::discard`, `ZemdbClient::discard_room`); `tests/discard_tests.rs`. `ARCHITECTURE.md` §11 (*Discarding*, y la frase de *Download* que da descartar la sala como salida de un snapshot mayor que el tope).

**Qué se hace:** `Client::discard_room`, en cualquier estado de la sala, con los pasos de DC19 y lo que DC37 termina (escrituras, `watch` y, si el Lote 4 de 4.3 ya está, el worker), dejando la sala cerrada; los dos métodos de la fachada.

**Tests exigidos.**
- Una sala que falla cerrada (un esquema que no evoluciona) se descarta, y el `open_room` siguiente la abre con el esquema del servidor y el estado del servidor.
- Una sala cuya apertura falla con `SnapshotConstraints` o `SchemaMismatch`, y una que este cliente nunca abrió (con archivos de una ejecución anterior), se descartan.
- Las escrituras en vuelo terminan como en un descarte (incierta con `CommitAck`, `Network` sin él) y los `watch` terminan; la época sube, y sube otra vez en la apertura siguiente.
- Un registro, un sync y una bajada en vuelo al descartar no cambian la réplica nueva (la época).
- El orden de los pasos, con la capa de archivos de prueba (los metadatos antes que los datos); un error en `delete_room` (`RoomLocked`) lo detiene, y la próxima apertura vuelve a borrar.
- Una escritura que llega durante el descarte es `RoomClosed`, sin mandar nada; después de `shutdown`, `discard_room` es `RoomClosed`.
- No se manda nada al servidor (el transporte de prueba), salvo, con el worker de 4.3, su último `Ack`.

**Interacciones a revisar:** el lock de los registros y el aplicador (un registro, un sync, una bajada o una subida en vuelo); el descarte automático de 4.2 (DC19), que comparte los pasos; `stop_worker` de 4.3 (DC28), que manda el último `Ack` con el head de esa encarnación; `watch` (el fin del stream); la entrada de la sala en el cliente (INV-K15).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K3, K2, K10, K15 (la entrada de una sala descartada), K17 (la salida de una sala que falla cerrada).

**Entrega:** `feat(client): discard the local replica of a room on request (Fase 4.4, Lote 7)`.

### Lote 8 · La fachada con el worker: el modo de frescura, `sync`, `Offline` y el primer token (`zemdb-client`) · ⬜
**Objetivo:** que la fachada maneje el worker de cada sala y encienda `live` con los `watch` (DC36). Después de los Lotes 3, 4 y 7 de 4.3.

**Archivos:** `src/api/` (el cliente, la sala y `ChangeStream`), `src/client.rs` (el estado de la conexión, `pub(crate)`, y, si el Lote 7 se hizo antes del Lote 4 de 4.3, detener el worker en `discard_room`), `src/error.rs` (`Offline`); `tests/facade_worker_tests.rs`. `ARCHITECTURE.md` §11.

**Qué se hace:** `room` toma el primer token del `TokenProvider` y arranca el worker; `set_freshness` como modo base; el modo efectivo por las suscripciones, bajo un lock por sala; `sync`; `Offline` por el estado de la conexión.

**Tests exigidos.**
- `room` arranca el worker (`status`), un segundo `room` de la misma sala no arranca otro, y `close` lo detiene.
- Con `periodic`, el primer `watch` enciende `live` (el stream de eventos abierto, en `status`) y soltar el último vuelve a `periodic`; con `manual` y con `live`, los `watch` no cambian el modo. Muchos streams que se abren y se sueltan a la vez, desde varias tareas, terminan en el modo que corresponde a la cuenta final.
- Soltar un stream después de `shutdown`, o fuera de un runtime de Tokio, no entra en pánico.
- Con el SSE apagado, un `watch` deja la sala en `periodic`, y `status` lo dice.
- En `manual`, `sync` pone la réplica al día.
- Sin red, después de un pedido sin respuesta, una escritura es `Offline` sin mandar nada, y las lecturas siguen; con la red de vuelta y un pedido con respuesta, la escritura sale.
- `room` toma el primer token del proveedor; un token de otro `ClientId` es `Config`, sin mandar nada con él; con el proveedor que falla o no responde (sin red), `room` abre una sala con metadatos y se lee, y el token llega con la renovación cuando vuelve la red; una sala sin metadatos da `Network`, sin handle, y no arranca el worker.
- Si el Lote 7 se hizo antes del Lote 4 de 4.3: `discard_room` detiene el worker.

**Interacciones a revisar:** `set_freshness`, `start_worker` y el estado de la conexión de 4.3 (DC28); el `Drop` de un stream después de `shutdown` o con el cliente soltado; la renovación del token de DC30 sobre una sala abierta sin token; INV-K17 (la fachada no agrega errores que detengan una sala); `Offline` y DC6 (una red que se cae después de la verificación).

**Documentos:** `ARCHITECTURE.md` §11; `INVARIANTES.md`: K2 (`Offline`), K7, K17.

**Entrega:** `feat(client): the facade runs the worker of each room, with live mode while it is watched (Fase 4.4, Lote 8)`.

### Lote 9 · La simulación con la fachada (`zemdb-client`, tests) · ⬜
**Objetivo:** las invariantes de la fachada bajo fallas, en el arnés de simulación (§5.3 del plan de la fase). Después del Lote 9 de 4.3.

**Qué se hace:** clientes que escriben por la fachada (algunos con el `CryptoEngine` de prueba, sobre el esquema de la app de referencia con `notes` cifrada), con `watch` sobre claves y rangos al azar, abiertos y soltados durante la carga, y con el worker y el SSE de 4.3.

**Verificaciones:**
- cada operación que sale declara el dueño de su cliente (INV-K8);
- ningún pedido lleva el texto en claro de una columna cifrada, ni el snapshot de una subida, decodificado (el transporte busca una marca que el generador pone en cada valor cifrado) (INV-K9);
- al final de cada paso, con los streams leídos hasta quedar quietos, el último valor de cada `watch` es el de una lectura directa, también con lotes que llegan por eventos `ops` (INV-K10), y `status` cuenta solo las suscripciones vivas (INV-K15);
- ningún pánico.

Una versión corta en la suite y una larga con `#[ignore]`.

**Documentos:** `INVARIANTES.md`: *Automatizable* de K8, K9, K10 y K15.

**Entrega:** `test(client): the facade in the simulation, with encrypted columns and watches (Fase 4.4, Lote 9)`.

### Lote 10 · La app de referencia nativa (`zemdb-client`, ejemplo y tests) · ⬜
**Objetivo:** el criterio de aceptación de 4.4 (§6 del plan de la fase). Después del Lote 1 de 4.3 y del Lote 8.

**Archivos:** `crates/client/examples/` (el ejemplo, con sus escenarios en un módulo propio), `tests/ranking_app_tests.rs` (los mismos escenarios, incluidos con `#[path]`, para que corran en la CI).

**Qué se hace:** el esquema de §6, con `notes` cifrada; un servidor local en proceso, en un socket de `127.0.0.1`, con el transporte HTTP de 4.3 y clientes en disco; los ocho escenarios de §6:
- las réplicas llegan a la misma tabla de posiciones;
- los rechazos `Duplicate`, `ForeignKey` y `Unique`;
- la misma persona desde dos dispositivos;
- `Restrict` entre dueños distintos;
- un miembro nuevo que entra con la historia podada y se pone al día desde un snapshot que sube otro;
- sin red, la tabla de posiciones se lee y votar es `Offline`;
- `watch` de la tabla de posiciones mientras llegan votos;
- `notes` nunca en claro: no está en las operaciones que sirve el servidor (las lee un sync del archivista) ni en el snapshot, decodificado.

**Documentos:** `INVARIANTES.md`: *Automatizable* de K4, K9 y K10.

**Entrega:** `feat(client): the native reference app, the ranking by votes (Fase 4.4, Lote 10)`.

### Lote 11 · Revisión de cierre y cierre · ⬜
- La revisión de cierre (§3), con la app de referencia como criterio de aceptación. Si no sale en verde, sus rupturas se arreglan en lotes nuevos de 4.4, cada uno con su revisión de delta.
- `INVARIANTES.md`: el estado de cada invariante revisada. El plan de la fase: 4.4 cerrada. `ROADMAP.md`: "Lo que sigue".

---

## 3. Revisión de cierre

Al cerrar 4.4, sobre el commit del Lote 10, con dos revisores en paralelo, de contexto fresco y solo lectura (`docs/METODOLOGIA.md` §6), cada uno con sus corridas en una copia del repo:

| Revisor | Invariantes | Foco |
|---|---|---|
| Escrituras y cifrado | K2, K8, K9, K12 y C3 | La fachada de escritura y su orden, el dueño, el pre-chequeo y `check_operation`, el cifrado y el texto descifrado |
| Réplica y reactividad | K3, K4, K7, K10, K15 y K17 | `watch` y el aviso del aplicador, el descarte a pedido, la fachada con el worker (el modo de frescura y `Offline`) y la app de referencia |

Cada uno recibe `INVARIANTES.md`, `ARCHITECTURE.md`, este plan, el de la fase (decisiones DC1 a DC37) y los arneses, y responde con veredictos sobre lo que tocó 4.4 y los mecanismos con los que interactúa, con las tres condiciones de `INVARIANTES.md` §1.4. Lo de 4.5 y 4.6 (las claves en el Worker, los errores de rango) sigue 🆕.

---

## 4. Seguimiento

**Avance:** plan escrito el 6 de Noviembre de 2026, con DC32 a DC37 *Propuestas*. Lote 0 (el contrato) cerrado el 6 de Noviembre, con la revisión cruzada en verde en su tercera ronda. El dueño del proyecto adoptó DC32 a DC37 el 7 de Noviembre, sin cambios. Lote 1 (`check_operation`) cerrado el 9 de Noviembre, con la revisión de delta en verde en su primera ronda. Lote 2 (la fachada y las lecturas) cerrado el 10 de Noviembre, con la revisión de delta en verde en su primera ronda. Lo que sigue es el Lote 3.
