# Restricciones de integridad verificadas por las réplicas — Fase 3.11

**Fecha:** 19 de Octubre de 2026
**Tipo:** Feature, no remediación. Documento vivo: propuesta, plan por lotes y seguimiento.
**Estado:** Cerrada (21 de Octubre de 2026). Las decisiones de §3 se adoptaron en el Lote 0 (19 de Octubre de 2026), salvo DR15, que se difirió. Se ejecutaron los Lotes 0 a 6, más el 6b, que pidió la revisión de cierre, y el 6c, que pidió la revisión de delta del 6b; el 2b quedó diferido.
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) y [`ARCHITECTURE.md`](../../ARCHITECTURE.md).
**Lugar en el roadmap:** después del cierre de la [Fase 3.10](2026-10-fase3_10-remediation-plan.md) y antes de la Fase 4 (SDK del cliente).

---

## 0. Resumen

- **Qué.** Los esquemas pueden declarar:
  - restricciones `UNIQUE` de una o varias columnas;
  - claves foráneas (`FOREIGN KEY`) hacia la clave primaria de una tabla del mismo esquema, incluida la propia, con `ON DELETE RESTRICT`;
  - un **dueño de fila** (`owner`): una columna que identifica al cliente dueño de la fila, con su `ClientId` o con un seudónimo; solo el dueño puede insertarla, modificarla o borrarla;
  - **tablas inmutables** (`immutable`): una fila se inserta una vez y nunca se reemplaza, modifica ni borra.
- **Quién las verifica.**
  - `UNIQUE`, `FOREIGN KEY` y la regla de inmutabilidad sobre un `Insert` dependen del estado: las verifica cada réplica al aplicar el log, en orden de secuencia. Una operación que viola una de ellas se secuencia y queda en el log como cualquier otra. Cada réplica la convierte en un NOP: no cambia nada y se informa como rechazada.
  - Que el dueño que declara una operación sea quien la envía, y que una tabla inmutable no reciba `Update` ni `Delete`, no dependen del estado. Los verifica el servidor al recibir el commit, como hoy verifica tipos y nulabilidad.
  - Que ese dueño sea también el de la fila existente sí depende del estado. Lo verifican las réplicas, salvo cuando el dueño está en la clave primaria: ahí no hace falta.
- **El servidor.** Guarda y distribuye las restricciones como parte del esquema y valida su definición. No mira filas: sus validaciones siguen siendo livianas y sin estado.
- **Por qué funciona.** Aplicar el log ya es una función determinista del estado y la operación (INV-Y8), y el mecanismo ya existe: un `Update` sobre una fila inexistente se secuencia y cada réplica lo ignora. Las restricciones agregan condiciones a esa función.
- **El riesgo.** Hoy el servidor es la única autoridad sobre qué es válido. Con esto, cada réplica tiene que tomar exactamente la misma decisión, o los datos divergen sin aviso. Casi todas las decisiones de §3 existen para eso. La más importante es DR3: una restricción solo puede nacer con el esquema, o con una columna nueva que la incluya, porque hoy un cambio de esquema no tiene posición en el log.
- **Cuánto cambia.**
  - Servidor: poco (la API de administración y la verificación del dueño).
  - Core: el esquema.
  - Storage: es el grueso. Índices en memoria, la verificación y el resultado por operación de `apply_batch`.
  - Cliente: todavía no existe; se diseña con esto desde el principio (§7).
- **Plan.** Siete lotes, más uno opcional (§6): contrato, core, servidor (con un 2b opcional de seudónimos), tres de storage y cierre. La revisión de cierre agregó un 6b, sobre la reasignación de salas, y la revisión de delta del 6b, un 6c, sobre la recarga del esquema. La metodología está en §5.

---

## 1. Motivación

### 1.1. Lo que hay hoy
- La única garantía relacional es la clave primaria. `ARCHITECTURE.md` §3.1 (*Soft Foreign Keys*) declara que las referencias colgantes están permitidas.
- El servidor no puede verificar `UNIQUE` ni `FOREIGN KEY`: no guarda filas (`ARCHITECTURE.md` §1 y §5.1). Darle estado para eso contradice el objetivo del proyecto: un servidor liviano que coordina muchas salas por proceso.
- Ningún cliente puede impedir que otro miembro de la sala modifique o borre sus filas.
- El plan de la Fase 4 (`ROADMAP.md` §4.4.4) dice que "el servidor valida esquema y restricciones" y responde `SchemaViolation`. Con este servidor, eso no es implementable para las restricciones que dependen del estado.

### 1.2. La idea
Una réplica es una máquina de estados determinista cuya entrada es el log. Si la decisión de aplicar o rechazar una operación depende solo del estado anterior, de la operación y de las restricciones del esquema, todas las réplicas honestas que aplican el mismo log deciden lo mismo, sin coordinarse. El servidor no necesita conocer la decisión: solo ordena.

Ejemplo con dos clientes concurrentes y una FK `tasks.project_id → projects`. El cliente A borra el proyecto `p` y el cliente B inserta una tarea de `p`:
- **El servidor secuencia primero el borrado:** toda réplica borra `p` y después rechaza la tarea, porque no tiene padre.
- **El servidor secuencia primero la tarea:** toda réplica la inserta y después rechaza el borrado, porque `p` tiene hijos (`RESTRICT`).

En los dos casos todas las réplicas terminan iguales, y cada cliente se entera del resultado de su propia operación.

### 1.3. Qué se gana y qué cuesta
**Se gana:**
- Integridad relacional sin darle estado al servidor ni agregar coordinación.
- El rechazo llega en el mismo viaje de ida y vuelta del modelo *write-through* de la Fase 4 (§7).
- Resistencia a un miembro hostil. Puede secuenciar operaciones que violan las restricciones, pero las réplicas honestas las rechazan igual. Con el dueño de fila, ni siquiera puede escribir filas ajenas.

**Cuesta:**
- La corrección pasa a depender de que todas las réplicas decidan igual. Una diferencia diverge sin aviso. Puede venir de un error de implementación, de dos versiones del SDK con semánticas distintas o de una réplica que aplica una operación con otro esquema.
- Las operaciones rechazadas ocupan el log, la retención y la ventana de dedup como cualquier otra.
- Índices secundarios en la memoria del cliente, uno por restricción.
- La equivalencia del *squashing* (`ARCHITECTURE.md` §4.1) deja de valer con restricciones (§7). No afecta a la v0.1, que no tiene outbox offline.

---

## 2. Alcance

### 2.1. Entra
- `UNIQUE` sobre una o varias columnas de una tabla.
- `FOREIGN KEY` de una o varias columnas hacia la clave primaria completa de una tabla del mismo esquema, incluida la propia (árboles, jerarquías). `ON DELETE RESTRICT` es la única acción al borrar un padre.
- **Dueño de fila:** una columna cuyo valor identifica al cliente dueño (DR10). Puede ser su `ClientId`, o un seudónimo que no lo revela a los demás miembros (DR15, opcional). Solo el dueño puede insertar, modificar o borrar la fila, esté la columna en la clave primaria o no.
- **Tabla inmutable:** solo admite `Insert`, y un `Insert` sobre una clave primaria que ya existe se rechaza (DR11).
- Restricciones declaradas al crear el esquema. `UNIQUE` y `FOREIGN KEY` también se pueden declarar al agregar una columna, si la incluyen (DR3).
- El resultado de cada operación (aplicada o rechazada, y por qué), observable a través de `apply_batch`.

### 2.2. No entra

| Qué | Por qué | ¿Se puede agregar después? |
|---|---|---|
| `ON DELETE CASCADE` y `SET NULL` | Producen efectos que no están en el log (borrados o updates derivados). El reporte y el `ChangeStream` tendrían que propagarlos, y un borrado puede tocar muchas filas. | Sí, como acción nueva. Usa el mismo índice inverso que `RESTRICT`. |
| `CHECK` | Los de una sola columna no dependen del estado: los podría validar el servidor, como hoy valida tipos. Los de varias columnas necesitan la fila entera, que un `Update` parcial no trae. | Sí. Los de una columna, en el servidor. |
| Restricciones sobre columnas que ya existen | Se aplicarían también a operaciones secuenciadas antes de crearlas. Hace falta anclar el cambio de esquema en el log (DR3). | Sí, anclando el esquema en el log (§2.3). Queda para después de la v0.1. |
| Anonimato frente al operador del servidor | El seudónimo (DR15) oculta el `ClientId` a los demás miembros, pero quien tiene el secreto del servidor puede recalcularlo para cada cliente. | No con este modelo. Haría falta criptografía como firmas ciegas o pruebas de conocimiento cero. |
| Restricciones diferidas y transacciones de varias operaciones | Cada commit lleva una sola operación (`ClientMessage::Commit`). | Requiere commits de varias operaciones. |
| Collations, comparación sin distinguir mayúsculas, normalización Unicode | El resultado depende de la versión de Unicode de cada cliente: no es determinista entre réplicas. | No con este modelo. La aplicación puede guardar una columna ya normalizada. |
| Restricciones sobre columnas cifradas | El texto cifrado es aleatorio, y ni el storage ni el servidor tienen las claves. | No. |
| Consultas por índice secundario | Los índices de esta fase son internos. | Sí (backlog 7 de `ROADMAP.md` §7). |
| Detección de divergencia entre réplicas | Ver DR13. | Sí, con un digest del estado por secuencia. |

### 2.3. Trabajo futuro: el esquema anclado en el log
Se decidió dejarlo para después de la v0.1: la v0.1 no va a correr en producción con esquemas que evolucionan, y hacerlo ahora retrasa el cliente una fase entera. Queda acá el análisis para retomarlo.

**Qué resuelve.** Hoy un cambio de esquema no tiene posición en el log (§3.2). Si la tuviera, cada operación se validaría y aplicaría con la versión vigente en su secuencia. Eso permitiría:
- agregar o quitar restricciones sobre columnas existentes;
- DDL más rico: borrar o renombrar columnas, o agregar `CHECK` después;
- reemplazar DR3 por "una restricción rige desde la secuencia en que se agrega", y dejar de depender de que el cliente baje el esquema antes de aplicar una respuesta (INV-E2).

**Dos formas:**
1. **Entradas de esquema en el log.** El log pasa de operaciones a un enum: operación o cambio de esquema (por ejemplo, el `add_column` entero). Snapshot y log reconstruyen datos y esquema sin nada más.
2. **Un puntero por sala.** El log no cambia. Cada sala guarda una tabla durable `(versión, desde_seq)`, escrita antes de aceptar commits con la versión nueva. Los clientes la reciben, y el storage la guarda para el replay.

**Qué hay que resolver:**
- **Alcance:** el log del servidor (buffer en RAM, segmentos, sidecars, retención, catch-up), el sync, el WAL del storage y su replay, los snapshots y el relay. Implica volver a revisar las invariantes de durabilidad, entrega y retención.
- **El registry** guarda hoy solo la última versión de cada esquema. Necesita el historial, o el log tiene que llevar los cambios.
- **Un snapshot** tiene que decir con qué versión se tomó.
- **El storage** no recarga el esquema de una sala abierta (DEF-34). Pasaría a cambiarlo en medio de un batch.
- **Los datos existentes.** El esquema es una plantilla compartida por muchas salas, y el servidor no ve los datos. Una restricción agregada sobre una columna existente puede encontrar violaciones en una sala. Hay que elegir entre dos semánticas, con o sin log:
  - que valga solo para filas nuevas (como `NOT VALID` en Postgres), lo que debilita INV-C2;
  - que quede inactiva en esa sala: es determinista, pero el administrador no se entera.

**Compatibilidad.** DR3 es compatible hacia adelante: cuando el esquema se ancle en el log, DR3 solo se relaja, y ningún esquema creado bajo DR3 queda inválido. El costo de diferirlo aparece si se publica un release antes: entonces cambiar el formato del log requiere subir `PROTOCOL_VERSION` y migrar logs y WAL (`GEMINI.md`).

---

## 3. Decisiones de diseño

Se numeran DR1, DR2, etc. para no chocar con la numeración D de los planes de remediación, que la Fase 3.10 sigue usando. Se adoptaron en el Lote 0, salvo DR15, que se difirió a después de la v0.1.

| ID | Decisión | Lote | Estado |
|---|---|---|---|
| **DR1** | **`UNIQUE`, `FOREIGN KEY` e inmutabilidad: las verifica cada réplica.** Las restricciones son parte del esquema. El servidor valida su definición y las guarda, distribuye y recarga como hoy. No verifica filas ni rechaza commits por ellas, así que no hay `ErrorCode` nuevo para estas tres. Cada réplica decide al aplicar cada operación en orden de secuencia, en `apply_batch` y en el replay del WAL, en los dos motores. Una operación que viola una restricción se rechaza entera, nunca a medias: no cambia tablas ni índices, y `head_seq` avanza igual porque la operación se consumió. Una operación inválida para el esquema (tipo, aridad) sigue siendo un error de `apply_batch`: es un log dañado, no un rechazo. | 1–5 | Adoptada |
| **DR2** | **Alcance.** El de §2.1. Lo de §2.2 queda afuera. | 0 | Adoptada |
| **DR3** | **Una restricción nunca cambia la decisión sobre el pasado.** Una restricción nace con el esquema (`POST /admin/schemas`), o con la columna nueva que agrega `add_column`. En ese caso tiene que incluirla: entre sus columnas para `UNIQUE`, y entre las columnas de la tabla hija para una FK. Nunca se agrega sobre columnas existentes, ni se quita ni modifica. El dueño y la inmutabilidad se declaran solo al crear el esquema. Una sala existente solo se reasigna a otro esquema (`RoomManager::get_or_spawn` con esquema) si ese esquema extiende el suyo (las mismas tablas, claves y columnas, más columnas nullable y tablas nuevas; Lote 6b) y tiene las mismas restricciones, el mismo dueño y la misma inmutabilidad. El porqué está en §3.2. **Alternativa diferida a después de la v0.1:** anclar el esquema en el log (§2.3). Permitiría restricciones sobre columnas existentes, a cambio de tocar el log, el sync y el replay. | 0, 1, 2, 6b | Adoptada |
| **DR4** | **Semántica de la verificación.** Ver §3.1. En resumen:<br>- Se verifican las restricciones cuyas columnas toca la operación (todas, en un `Insert`).<br>- La igualdad es la de `Value`.<br>- Una clave con algún `NULL` no se verifica.<br>- Las columnas que faltan en una fila corta son `NULL`.<br>- El motivo informado es el de la primera restricción que falla, en un orden fijo. | 4 | Adoptada |
| **DR5** | **El resultado se informa.** `apply_batch` devuelve `ApplyReport { head_seq, rejected }`, con un elemento `(seq, motivo)` por cada operación no aplicada.<br>- **Motivos:** `Unique`, `ForeignKey` y `Restrict`, con el nombre de la restricción; `Duplicate` (un `Insert` en una tabla inmutable sobre una clave existente); `NotOwner` (la fila existente es de otro dueño); y `MissingRow` (un `Update` sobre una fila que no existe, que hoy se ignora sin aviso).<br>- Solo se listan las excepciones, para no reservar un resultado por operación.<br>- En la recuperación, el reporte se descarta.<br>Los nombres son tentativos. | 3, 4 | Adoptada |
| **DR6** | **Índices derivados en memoria, sin cambio de formato en disco.**<br>- Un índice por restricción: para `UNIQUE`, clave → PK; para una FK, clave foránea → PK hija. Los dos son multimapas ordenados y persistentes (`imbl`, con clon O(1) como `Table`), para que el motor de disco los clone y los publique junto con las tablas.<br>- Solo se indexan claves sin `NULL`.<br>- No se persisten. Se construyen al abrir (desde el snapshot, antes del replay) y al aplicar un snapshot, y se actualizan con cada operación aplicada.<br>- El `.snap`, el WAL y el envelope `ZMSN` no cambian. | 3 | Adoptada |
| **DR7** | **El estado siempre cumple las restricciones.**<br>- `apply_snapshot` verifica que el snapshot cumpla las restricciones del esquema con que se aplica. Lo hace al construir los índices, en el pool bloqueante. Si no las cumple, responde `SnapshotCorruption` y la sala no cambia (INV-S5).<br>- `open_room` hace lo mismo con el snapshot local. Una violación es un error y no se descarta nada (INV-S4). | 5 | Adoptada |
| **DR8** | **Formato estricto y semántica congelada.**<br>- Las restricciones, el dueño y la inmutabilidad viven en `TableSchema`, que ya rechaza campos desconocidos.<br>- `ColumnDef` y los cuerpos de `POST /admin/schemas` y `.../columns` pasan a rechazarlos. Hoy un `"unique": true` dentro de una columna se ignoraría sin aviso.<br>- Los campos nuevos se serializan siempre, porque bincode es posicional. En JSON tienen default vacío.<br>- `PROTOCOL_VERSION` queda en `0x01` (`GEMINI.md`).<br>- La semántica de un tipo de restricción no cambia nunca: otro comportamiento es otro tipo. Un cliente viejo no puede decodificar un tipo nuevo, así que falla cerrado en vez de aplicar el log sin la regla. | 1, 2 | Adoptada |
| **DR9** | **Límites.**<br>- Un tope de restricciones por tabla y de columnas por restricción, para acotar el costo de cada operación (INV-W1). Adoptado: 32 y 16.<br>- Los nombres de restricción son únicos en el esquema. | 1 | Adoptada |
| **DR10** | **Dueño de fila: el servidor verifica quién declara, las réplicas de quién es la fila.**<br>- Una tabla declara `owner: { column, identity }`. La columna no está cifrada. Es `String` si la identidad es el `ClientId`, y `Bytes` si es un seudónimo (DR15).<br>- **Toda operación declara el dueño:**<br>  - **Columna en la clave primaria:** la clave ya lo trae, en `Insert`, `Update` y `Delete`.<br>  - **Columna fuera de la clave:** el `Insert` lo trae en la fila; un `Update` tiene que incluir un delta de esa columna con el dueño; un `Delete` lo lleva en un campo nuevo, `Delete { owner }` (hoy `Delete` no lleva datos).<br>  - `validate_operation` rechaza una operación que no lo declara, y una que lo declara donde no corresponde.<br>- **Servidor:** al recibir el commit, después de `validate_operation` y antes de secuenciar, compara el dueño declarado con el del cliente autenticado (INV-A1), byte a byte. Si no coincide, responde `SchemaViolation`, permanente para esa mutación, y no queda rastro (INV-Y5). No responde `Forbidden`, que el SDK interpreta como un problema del token.<br>- **Réplicas:** si la columna está fuera de la clave y la fila existe, su dueño tiene que ser el declarado; si no, rechazan la operación (`NotOwner`). Así nadie modifica, borra ni reemplaza una fila ajena, y un `Update` no puede transferir una fila.<br>- El servidor no modifica la operación ni guarda estado, y el log no necesita registrar quién escribió cada operación. Ver §3.4. | 2, 4 | Adoptada |
| **DR11** | **Tabla inmutable.** `immutable: true` en la tabla.<br>- `validate_operation` rechaza `Update` y `Delete` sobre ella, sin estado: el servidor responde `SchemaViolation` y el storage la trata como cualquier operación inválida.<br>- Las réplicas rechazan un `Insert` cuya clave primaria ya existe (motivo `Duplicate`), en vez del upsert habitual. | 2, 4 | Adoptada |
| **DR12** | **Requisitos para la Fase 4.** Ver §7.<br>- El cliente resuelve cada escritura cuando su réplica aplicó hasta la secuencia asignada, con el resultado de DR5.<br>- Hay un pre-chequeo local opcional.<br>- El `ChangeStream` no emite nada por operaciones rechazadas.<br>- `ROADMAP.md` §4.4.4 se reescribe. | 0, 6 | Adoptada |
| **DR13** | **Sin detección de divergencia en esta fase.** Es un riesgo aceptado (`INVARIANTES.md` §4): una réplica que decide distinto diverge sin aviso.<br>- **Mitigación:** el oráculo diferencial y los tests de convergencia (§5.3).<br>- **Backlog:** un digest del estado por secuencia, que las réplicas comparen a través del servidor (por ejemplo, en el heartbeat). | 0 | Adoptada |
| **DR14** | **Cierre por revisión de delta, sin auditoría amplia.** Core, storage y servidor están dentro del alcance auditado, y el modelo de amenaza no cambia en lo que importa a D90: un miembro hostil sigue pudiendo secuenciar operaciones válidas, y las réplicas honestas las rechazan. Cada lote cierra con su revisión de delta (§5.2, paso 4), y la fase con una revisión de cierre del delta completo. Esa revisión también audita por primera vez las invariantes nuevas. | 6 | Adoptada |
| **DR15** | **Dueño seudónimo (opcional).** Con `identity: "Pseudonym"`, la columna dueño guarda un seudónimo de 32 bytes en vez del `ClientId`.<br>- **Derivación:** el servidor deriva una clave de dueño con BLAKE3 en modo con clave. Las entradas son un secreto propio (`ZEMDB_OWNER_SECRET`, distinto de los otros dos), la sala y el cliente. Se la da al cliente en `Registered`. El seudónimo de una tabla es el hash con clave de su `table_id` bajo la clave de dueño: el cliente lo calcula, y el servidor lo recalcula para verificar.<br>- **El cliente escribe el seudónimo y el servidor lo verifica,** en vez de que el servidor lo inserte al confirmar la operación. El servidor no modifica operaciones (`ARCHITECTURE.md` §4.1), la copia local del cliente es igual a la del log, los reintentos deduplicados no cambian, y el cliente necesita su seudónimo igual, para reconocer sus propias filas.<br>- **Lo que garantiza:** los demás miembros no ven el `ClientId`, ni pueden vincular las filas de un cliente entre tablas distintas. Conocer el seudónimo de otro no permite escribir en su nombre: el servidor lo recalcula desde el cliente autenticado.<br>- **Lo que no garantiza:** quien tiene el secreto puede recalcular el seudónimo de cada cliente (§2.2); las filas de un cliente en la misma tabla comparten seudónimo; y el orden y el momento de los commits pueden delatar a quien escribe.<br>- **El secreto no se rota:** cambiarlo deja todas las filas sin un dueño que pueda modificarlas. Sin el secreto configurado, se rechaza un esquema que use seudónimos, y el servidor no arranca si alguno ya registrado los usa. | 2b | ⏸ Diferida (post-v0.1) |

### 3.1. Reglas de verificación en las réplicas (DR4)

**Notación:**
- `T` es la tabla de la operación y `p` su clave primaria.
- `r` es la fila resultante:
  - en un `Insert`, la fila nueva;
  - en un `Update`, la existente ensanchada al ancho del esquema, con los deltas aplicados.
- La **clave** de una restricción es la tupla de valores de sus columnas en `r`.

| Operación | Qué se verifica | Se rechaza si |
|---|---|---|
| `Insert` (upsert) | Todas las `UNIQUE` y FK de `T`. Si `T` es inmutable, también que `p` no exista. Si `T` tiene el dueño fuera de la clave y `p` existe, que la fila existente sea del dueño declarado. | **Inmutable:** ya existe una fila con PK `p` (`Duplicate`).<br>**Dueño:** la fila existente con PK `p` es de otro dueño (`NotOwner`).<br>**`UNIQUE`:** otra fila, con PK distinta de `p`, tiene la misma clave.<br>**FK:** no existe en la tabla referida una fila cuya PK sea la clave. La excepción es una FK de `T` a `T` cuya clave es `p`: una fila puede apuntarse a sí misma. |
| `Update` | Si `T` tiene el dueño fuera de la clave, que la fila existente sea del dueño declarado. Las `UNIQUE` y FK de `T` con alguna columna entre las que el `Update` cambia. | Si la fila `p` no existe, no se aplica y se informa `MissingRow`, antes de cualquier otra verificación.<br>**Dueño:** la fila es de otro dueño (`NotOwner`).<br>Después, las mismas condiciones que en `Insert`, sobre `r`. |
| `Delete` | Si `T` tiene el dueño fuera de la clave, que la fila sea del dueño declarado. Las FK de cualquier tabla, `T` incluida, que apuntan a `T`. | Si la fila `p` no existe, no cambia nada y no se informa: el resultado es el mismo.<br>**Dueño:** la fila es de otro dueño (`NotOwner`).<br>**`RESTRICT`:** alguna fila hija tiene `p` como clave foránea. Si es la misma tabla, no cuenta la propia fila `p`. |

Además:
- **Claves con `NULL`:** si alguna columna de la clave es `NULL`, la restricción no aplica a esa fila. `UNIQUE` admite varias, y la FK no se verifica; es el `MATCH SIMPLE` de SQL. Las columnas que faltan en una fila corta (escrita antes de agregarse la columna) leen `NULL`.
- **Igualdad:** es la de `Value`, cuyos `Eq` y `Ord` son totales. `-0.0` y `0.0` son iguales, los NaN se comparan por bits, y `String` y `Bytes` se comparan byte a byte.
- **`RESTRICT` solo se evalúa en `Delete`.** Un `Insert` que reemplaza una fila conserva su PK, así que no afecta a las filas que la referencian. Un `Update` no puede cambiar la PK (ya se valida).
- **Qué columnas "toca" un `Update`:** las de sus deltas, aunque el valor nuevo sea igual al anterior. Se define así, y no comparando contra el estado, para que la regla sea simple. Mientras el estado cumpla las restricciones (INV-C2), el resultado es el mismo que comparando; si no las cumpliera por un error, sigue siendo determinista.
- **Todo o nada:** si alguna verificación falla, no se aplica nada de la operación.
- **Orden del motivo:**
  1. `MissingRow`;
  2. `Duplicate`;
  3. `NotOwner`;
  4. las `UNIQUE`, en orden de declaración;
  5. las FK de `T`, en orden de declaración.

  En un `Delete`, después de `NotOwner`, las FK que apuntan a `T` se recorren por id de la tabla hija y, dentro de cada una, en orden de declaración.
- **Dentro de un batch:** cada operación se verifica contra el estado que dejó la anterior, incluidas las del mismo batch.

### 3.2. Por qué DR3 alcanza
Hoy un cambio de esquema no tiene posición en el log:
- `add_column` recarga el esquema en el actor de cada sala (`ReloadSchema`) sin marcar desde qué secuencia rige.
- El cliente que ve otra `schema_version` baja el esquema antes de aplicar las operaciones de esa respuesta, aunque algunas se hayan secuenciado antes del cambio.
- El storage reaplica su WAL con el esquema que recibe `open_room`, y `apply_snapshot` puede reemplazar el esquema.

La misma operación, entonces, puede aplicarse con versiones distintas del esquema según la réplica y el momento. Para una columna nueva no importa, porque toda operación válida sigue siéndolo (INV-E1). Para una restricción sobre una columna existente, sí:
1. Se secuencia la op 100: un `Insert` con un `email` repetido.
2. El cliente A la aplica bajo la v2, sin `UNIQUE`, y la fila entra.
3. El administrador agrega `UNIQUE(email)` y el esquema pasa a v3.
4. El cliente B estaba atrasado: baja la v3 y aplica la misma op 100, que con la v3 es un NOP.
5. A y B quedan con datos distintos.

Con DR3, toda restricción agregada después incluye una columna nueva `c`:
- **Las operaciones secuenciadas antes de `c` no pudieron escribirla:** el servidor las habría rechazado contra el esquema de entonces.
- **Por eso la restricción no aplica a esas operaciones.** En el estado que ven y en las filas que escriben, `c` es `NULL` o falta, y una clave con `NULL` no se verifica (§3.1). `RESTRICT` tampoco aplica, porque ninguna fila hija tiene todavía un valor en `c`.
- **Las operaciones posteriores que escriben `c` llegan en una respuesta con la versión nueva,** y el cliente baja ese esquema antes de aplicarlas (INV-E2).

La decisión sobre cada operación es, entonces, la misma bajo cualquier versión del esquema desde la que esa operación es válida. Eso es INV-C1 (§6, Lote 0).

**La reasignación** (revisión de cierre, Lote 6b). El argumento supone que el esquema de la sala desciende, por `add_column`, del esquema bajo el que se secuenció su log. Una reasignación a un esquema con las mismas restricciones pero con otras columnas lo rompe: los valores de las operaciones viejas caen en otras columnas, y una restricción agregada después sobre una de ellas las juzga distinto que antes. Por eso la reasignación exige, además, que el esquema nuevo extienda al viejo.

El dueño y la inmutabilidad no tienen este problema. Solo se declaran al crear el esquema, así que rigen desde la primera operación de cada sala. Además, `add_column` no puede agregar columnas a la clave primaria.

### 3.3. Formato

Las tablas se referencian por nombre, como las columnas de la clave primaria, y se resuelven a su id al construir el esquema. Los nombres de tabla no cambian nunca.

```json
{
  "version": 1,
  "tables": [
    {
      "table_id": 0, "name": "polls", "primary_key": ["id"],
      "columns": [
        {"name": "id", "data_type": "Uuid", "nullable": false, "encrypted": false},
        {"name": "slug", "data_type": "String", "nullable": false, "encrypted": false}
      ],
      "unique": [{"name": "polls_slug", "columns": ["slug"]}],
      "foreign_keys": []
    },
    {
      "table_id": 1, "name": "votes", "primary_key": ["voter", "poll_id"],
      "columns": [
        {"name": "voter", "data_type": "String", "nullable": false, "encrypted": false},
        {"name": "poll_id", "data_type": "Uuid", "nullable": false, "encrypted": false},
        {"name": "choice", "data_type": "Int", "nullable": false, "encrypted": false}
      ],
      "owner": {"column": "voter", "identity": "ClientId"},
      "immutable": true,
      "unique": [],
      "foreign_keys": [
        {"name": "votes_poll", "columns": ["poll_id"], "references": "polls", "on_delete": "Restrict"}
      ]
    }
  ]
}
```

Con `"identity": "Pseudonym"`, la columna `voter` es `Bytes` y guarda el seudónimo del cliente (DR15).

Agregar una columna con una restricción que la incluye:
```json
{
  "table_name": "polls",
  "column": {"name": "code", "data_type": "String", "nullable": true, "encrypted": false},
  "unique": [{"name": "polls_code", "columns": ["code"]}]
}
```
Una restricción que no incluye la columna nueva, como `{"columns": ["slug"]}`, es `BadRequest` y no cambia nada.

### 3.4. Dueño de fila e inmutabilidad: el caso de los votos

**Objetivo:** cada cliente vota una sola vez por encuesta, y nadie vota a nombre de otro.

**El dueño se declara y se verifica en dos partes (DR10).** El servidor sabe quién envía cada commit, porque el token liga el `ClientId`, pero no sabe de quién es una fila, porque no guarda filas. Por eso:
- **Toda operación declara su dueño.** El servidor verifica, sin estado, que el declarado sea quien la envía.
- **Las réplicas verifican** que la fila existente sea de ese dueño. Tienen la fila, así que no necesitan saber quién envió la operación: les alcanza con el dueño declarado, que el servidor ya verificó.

Con la columna dueño en la clave primaria, la segunda parte sobra: la clave de la operación ya identifica la fila y su dueño, y el servidor rechaza cualquier operación de B sobre una clave de A. Fuera de la clave, `Update` y `Delete` tienen que declarar el dueño (un delta de la columna dueño, y el campo nuevo de `Delete`). Las réplicas rechazan con `NotOwner` la operación sobre una fila ajena.

**Por qué no hace falta registrar el autor en el log.** Sería la alternativa: que el log guarde quién envió cada operación, y que las réplicas comparen. Pero hoy el log guarda solo un hash del cliente y la mutación, para el dedup. Agregar el autor cambia el log, el WAL, el `SyncBatch` y el storage del cliente, y expone a la sala quién escribió cada operación. Con el dueño declarado, la información viaja en la operación misma y el servidor no la modifica.

**Cómo se modela.**
- **`votes(voter, poll_id, choice)`, con clave `(voter, poll_id)` y `owner` en `voter`:** un voto por cliente y encuesta, y nadie escribe el voto de otro. No hace falta `UNIQUE`.
  - Sin `immutable`, el dueño puede cambiar o retirar su voto: un segundo `Insert` reemplaza al primero, y puede hacer `Update` y `Delete`.
  - Con `immutable`, el voto es definitivo. El segundo se rechaza en todas las réplicas (`Duplicate`), y `Update` y `Delete` los rechaza el servidor.
- **Una clave propia del voto (`vote_id`), con `UNIQUE(voter, poll_id)` y `owner` en `voter`:** también funciona, y el dueño puede modificar o borrar su voto declarándose dueño.

**Votos anónimos (DR15, opcional).** Con `identity: "Pseudonym"`, la columna dueño guarda un seudónimo en vez del `ClientId`:
- el cliente lo recibe en `Registered` y lo escribe;
- el servidor lo recalcula desde el cliente autenticado para verificarlo;
- las réplicas comparan valores como siempre.

Los demás miembros ven que un mismo seudónimo votó, pero no quién es. El costo es chico: un secreto más, un campo en `Registered` y el cálculo en el commit, sin estado en el servidor.

**Lo que la garantía es y no es.**
- **Es por `ClientId`, no por persona.** Si el backend emite un id por dispositivo, una persona con dos dispositivos vota dos veces. Con un id por usuario, es un voto por usuario.
- **Sin seudónimo, los `ClientId` quedan visibles para toda la sala,** porque están en las filas.
- **Con seudónimo, es seudonimato frente a los demás miembros, no anonimato.** El operador del servidor puede recalcular el seudónimo de cada cliente. Las filas de un cliente en la misma tabla comparten seudónimo. El orden y el momento de los commits pueden delatar a quien vota.
- **No protege contra un snapshot falso** que un miembro hostil suba con filas ajenas (riesgo aceptado DEF-81).

**Nombre.** Se propone `owner` y no `signed`: la garantía viene de la autenticación del servidor, no de una firma criptográfica que una réplica pueda verificar sola.

---

## 4. Cambios por componente

| Componente | Cambio | Lote | Producción estimada |
|---|---|---|---|
| `zemdb-core` | Tipos de `UNIQUE` y FK, validación estructural, evolución con `add_column`, serde estricta | 1 | ~250 líneas |
| `zemdb-core` + `zemdb-server` | Dueño e inmutabilidad (definición, dueño declarado y `Delete { owner }`, `validate_operation`, verificación del dueño en el commit), API de administración, cuerpos estrictos, reasignación | 2 | ~200 líneas |
| `zemdb-core` + `zemdb-server` | Dueño seudónimo (secreto, clave de dueño en `Registered`, verificación) | 2b (opcional) | ~100 líneas |
| `zemdb-storage` | Índices derivados y `ApplyReport` | 3 | ~250 líneas |
| `zemdb-storage` | Verificación en las réplicas (`UNIQUE`, FK, `RESTRICT`, `Duplicate`, `NotOwner`) | 4 | ~230 líneas |
| `zemdb-storage` | Snapshots recibidos, apertura y evolución | 5 | ~120 líneas |
| `zemdb-client` | Nada en esta fase. Deja requisitos para la Fase 4 (§7) | — | — |
| Documentos | `INVARIANTES.md` y `ROADMAP.md` en el Lote 0; `ARCHITECTURE.md` en cada lote, junto con el comportamiento | 0–6 | — |

**Lo que no cambia:**
- los mensajes del protocolo, salvo el contenido del esquema que llevan, `Delete { owner }` y, con el Lote 2b, la clave de dueño en `Registered`;
- la estructura del log del servidor y de los frames del WAL: `Delete { owner }` viaja dentro de la operación, que ya se serializa entera;
- el camino del commit, salvo la verificación del dueño;
- los formatos de archivo del storage (`.snap`, WAL, `ZMSN`), con la misma salvedad de `Delete`;
- el relay de snapshots.

---

## 5. Metodología

### 5.1. Reglas de trabajo
1. **Tamaño:** un lote es una unidad revisable de **~300 líneas de producción como máximo**; los tests no cuentan. Si se pasa, se parte. Idealmente, cada lote es una sesión propia.
2. **Tests:** siguen la estructura de dos niveles de `GEMINI.md`. Cada regla de comportamiento nueva tiene al menos un test negativo.
3. **Sin códigos** (`DR`, `INV-`, `DEF-`) en código, tests ni docstrings (`GEMINI.md`).
4. **Chequeos de cierre:** cada lote cierra con:
   - `cargo fmt --all -- --check`;
   - `cargo clippy --workspace --all-targets -- -D warnings`;
   - `cargo test --workspace`;
   - `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.

   Si el lote toca `core` o `storage`, también `cargo build -p zemdb-core -p zemdb-storage -p zemdb-client --target wasm32-unknown-unknown`.
5. **Revisiones:** cada lote cierra con su revisión de delta, y la fase con la revisión de cierre (DR14).
6. **Documentos:**
   - `ARCHITECTURE.md` se actualiza en el mismo commit que el comportamiento que describe.
   - `INVARIANTES.md` se actualiza cuando cambia una garantía.
   - Este plan se actualiza al cerrar cada lote.
7. **Alcance:** lo que no está en el lote no se hace en el lote (§5.2, paso 6).

### 5.2. Procedimiento por lote
Este procedimiento, junto con `GEMINI.md`, alcanza para retomar el trabajo sin el contexto de sesiones anteriores.

**Orden:**
- **El Lote 0** solo toca documentos y se puede adelantar.
- **El Lote 1** arranca cuando cierra la Fase 3.10, para no mezclar su delta con la revisión de cierre de esa fase (D90).
- **El Lote 2** depende del 1 y es independiente del 3 al 5.
- **El Lote 2b** está diferido a después de la v0.1.
- **Los Lotes 3, 4 y 5** van en ese orden: tocan los mismos archivos del storage.
- **El Lote 6** va al final.

**0. Arranque.**
- **El árbol:** `git pull`, con el árbol de trabajo limpio y la CI de `main` en verde en las tres plataformas.
- **Qué leer:**
  - la sección del lote en este plan;
  - las decisiones que cita (§3);
  - las invariantes que toca en `INVARIANTES.md`;
  - `GEMINI.md`;
  - en los lotes de storage, `ARCHITECTURE.md` §8.
- **Decisiones en estado *Propuesta*:** se confirman con el dueño del proyecto antes de escribir código.

**1. Primero los tests exigidos.**
- Se escriben contra la API nueva, con un esqueleto que compila y conserva el comportamiento viejo (sin `todo!()`).
- Se comprueba que fallan por el comportamiento, no solo por no compilar. Un test que pasa sin la implementación no prueba nada y se reescribe.

**2. La implementación mínima.** Lo que piden las decisiones del lote, sin adelantar el siguiente. Siguiendo `GEMINI.md`: validar antes de mutar (la decisión se calcula antes de tocar tablas o índices), nada de I/O bloqueante ni trabajo de CPU largo en el runtime, y tipos deserializados validados antes de usarse.

**3. Verificación.**
- **Chequeos:** los de la regla 4.
- **Mutación inversa:** deshacer cada regla o cada actualización de índice que agrega el lote, una por vez, y confirmar que algún test falla. Después, restaurarla.
- **Versiones largas, una sola vez al cerrar el lote:** `cargo test -p <crate> --release -- --ignored` de los crates tocados. Nunca se agregan a la CI de cada push.
- **Windows:** un test no lee un archivo que la sala tiene abierto.

**4. Revisión de delta.**
- **Quien implementa** lista, en la nota **Hecho** del lote, las interacciones del cambio: los mecanismos que ya existían y que leen o escriben el mismo estado. Cada lote trae una lista inicial, que no es exhaustiva. Para cada interacción va un test que la ejercite, o el argumento de por qué no hace falta.
- **El revisor** es independiente: un agente aparte, de contexto fresco y en modo solo lectura. Recibe el diff, las decisiones del lote, las invariantes que toca, los tests exigidos y la lista de interacciones. Responde cinco cosas:
  1. si el lote implementa lo que dicen sus decisiones, y nada más;
  2. si deshacer cada regla hace fallar un test;
  3. si la lista de interacciones está completa, y cuál falta;
  4. si el diff rompe alguna invariante, existente o nueva;
  5. en los lotes de storage, si se cumple el checklist de determinismo (§5.4).

Como en las auditorías, sin un test que lo reproduzca no hay hallazgo Alto ni Medio.

**5. Documentos, en el mismo cambio.**
- **`ARCHITECTURE.md`:** la parte del comportamiento que el lote implementa.
- **`INVARIANTES.md`:** el estado de las invariantes que el lote implementa.
- **Este plan:** el lote pasa a ✅, con una nota **Hecho** que lista los tests agregados, las mutaciones verificadas, las interacciones revisadas y cualquier desvío respecto de lo planeado.

**6. Alcance.** Lo que aparezca fuera del lote no se arregla en el momento:
- si afecta a esta feature, se agrega a este plan, en el lote que corresponda;
- si es un defecto preexistente, va al backlog con la severidad que le corresponda por `INVARIANTES.md`.

**7. Entrega.**
- Los cambios se dejan sin commitear para que el dueño revise el diff.
- Con su visto bueno, se arma un commit por lote, o pocos, con el estilo `feat(storage): … (Fase 3.11, Lote N)`, y se pushea a `main`.
- El lote queda cerrado recién con la CI de `main` en verde en las tres plataformas.

### 5.3. Estrategia de pruebas
Las réplicas tienen que decidir igual, y nadie lo verifica en producción (DR13). Por eso las pruebas tienen cuatro capas:

1. **Reglas.** Un test por cada fila y cada nota de §3.1, positivo y negativo, corrido sobre los dos motores con un mismo helper.
2. **Oráculo diferencial.** Es la defensa principal contra la divergencia.
   - **Qué es:** un modelo de referencia en `crates/storage/tests/common/`, que es la especificación de §3.1 en forma ejecutable. Guarda las filas en un `BTreeMap` y verifica todo recorriendo tablas completas, sin índices.
   - **Independencia:** no comparte código con la implementación, ni siquiera helpers del storage; solo usa tipos de `zemdb-core`. Así un error de la implementación no se repite en el oráculo.
   - **Uso:** logs aleatorios aplicados al oráculo y a los dos motores tienen que dar el mismo reporte y el mismo estado final.
   - **El generador:** usa dominios chicos (por ejemplo, 4 valores por columna), para que las colisiones sean frecuentes. Incluye `NULL`, auto-referencias, filas cortas, claves compuestas, `-0.0` y NaN.
   - Tiene una versión larga `#[ignore]`.
3. **Convergencia y recuperación.**
   - Se extienden `replica_convergence_tests.rs` y la simulación del disco (`disk/tests/simulation.rs`) a esquemas con restricciones. Aplicar todo, o restaurar un snapshot tomado en un punto aleatorio y aplicar el resto, da el mismo estado y los mismos reportes, en los dos motores y después de una compactación y una reapertura.
   - Un crash en cualquier paso, seguido de una reapertura, da el estado del oráculo.
   - **Evolución:** el mismo log aplicado bajo distintas versiones del esquema da lo mismo (INV-C1).
4. **Robustez.**
   - **El verificador nunca entra en pánico.** Un pánico después de escribir el WAL deja la sala fallida, y el replay lo repite: sería una operación veneno que no deja abrir la sala nunca más.
   - Los fuzzers existentes (de operaciones y de snapshots) se corren también con esquemas con restricciones.

### 5.4. Checklist de determinismo
El revisor lo recorre en cada lote de storage:
- La decisión depende solo de las tablas y los índices antes de la operación, de la operación y de las restricciones del esquema.
- En el camino de la decisión o del motivo no se itera un `HashMap`, ni nada sin orden definido.
- La igualdad es la de `Value`: sin normalización, sin collation y sin hashes con semilla.
- Ni el reloj, ni el azar, ni el tamaño del batch, ni el motor (memoria o disco), ni el camino (aplicar o reaplicar el WAL) cambian el resultado.
- El resultado no depende de cuándo la réplica conoció la versión del esquema (DR3).
- El motivo informado también es determinista.
- Un rechazo no tiene efectos (ni en tablas ni en índices), y `head_seq` avanza.

---

## 6. Lotes

| Orden | Lote | Crates | Decisiones | Invariantes | Progreso |
|---|---|---|---|---|---|
| 0 | Contrato | Documentos | Todas | Nuevas: C1, C2, C3, A5. Ajuste: Y8 | ✅ |
| 1 | `UNIQUE` y FK en el esquema | core | DR3, DR8, DR9 | E1, W1, W2 | ✅ |
| 2 | Servidor: dueño, inmutabilidad y API de administración | core, server | DR3, DR8, DR10, DR11 | A1, A5, Y5, E1, E2, D9, W1, W2 | ✅ |
| 2b | Dueño seudónimo (diferido) | core, server | DR15 | A1, A5, Y5 | ⏸ |
| 3 | Índices derivados y `ApplyReport` | storage | DR5, DR6 | S1, S3, S6, C3 | ✅ |
| 4 | Verificación en las réplicas | storage | DR1, DR4, DR5, DR10, DR11 | Y8, S1, A5, C2, C3, W1 | ✅ |
| 5 | Snapshots, apertura y evolución | storage | DR3, DR7 | S4, S5, C1, C2, W1 | ✅ |
| 6 | Cierre | Documentos | DR12, DR14 | Todas las tocadas | ✅ |
| 6b | Reasignación solo a un esquema que extiende el suyo (de la revisión de cierre) | core, server | DR3 | C1, E1 | ✅ |
| 6c | Recarga del esquema solo a las salas que siguen en él (de la revisión de delta del 6b) | server | DR3, DR10 | C1, A5, E2 | ✅ |

**Requisito para empezar la Fase 4:** Lotes 0 a 6 cerrados, con la revisión de cierre en verde. El 2b está diferido y no es requisito. Cumplido el 21 de Octubre de 2026: la revisión de cierre rompió INV-C1 y la revisión de delta del 6b, INV-C1 e INV-A5; los Lotes 6b y 6c las arreglaron, y la fase cerró con todas sus invariantes sostenidas (nota **Hecho** del Lote 6). El diseño del cliente, no su código, puede empezar cuando cierre el Lote 3, que fija el contrato de `apply_batch`.

### Lote 0 · Contrato · ✅ Hecho
**Objetivo:** dejar el contrato contra el que se revisa cada lote. Solo documentos.

**Qué se hace:**
- Confirmar las decisiones con el dueño del proyecto. Las preguntas abiertas están en §8.
- **`INVARIANTES.md`.** Pasa a la versión 4.
  - **Grupo nuevo, §3.10 Restricciones:** INV-C1 a C3 (enunciados abajo), con estado 🆕 y el lote que las implementa.
  - **En §3.5:** INV-A5.
  - **Ajuste del enunciado de INV-Y8:** dos réplicas honestas que aplican el mismo prefijo terminan con el mismo estado y toman la misma decisión sobre cada operación (aplicada o rechazada, con el mismo motivo). Aplicar una operación es una función determinista del estado, de la operación y de las restricciones del esquema.
  - **En §2, "No se confía en":** un miembro con token válido puede secuenciar operaciones que violan las restricciones; las réplicas honestas las rechazan (INV-C2). No puede escribir filas ajenas en una tabla con dueño (INV-A5).
  - **En §4, riesgos aceptados:**
    - una réplica que decide distinto (por un error o por otra semántica) diverge sin aviso (DR13);
    - las operaciones rechazadas ocupan el log, la retención y la ventana de dedup;
    - el dueño de fila es por `ClientId`, y queda visible para toda la sala salvo con seudónimo;
    - el seudónimo lo puede recalcular quien tiene el secreto del servidor, y vincula las filas de un cliente dentro de la misma tabla (§3.4).
- **`ROADMAP.md`:**
  - insertar la Fase 3.11 en §4 y en el cronograma de §5;
  - agregar el requisito de su cierre para empezar la Fase 4;
  - reescribir el paso 2 de §4.4.4 y los errores del cliente según DR12;
  - anotar en el backlog 7 (índices secundarios) que los índices de las restricciones existen pero son internos;
  - agregar al backlog el esquema anclado en el log (§2.3), con un enlace a este análisis.
- `ARCHITECTURE.md` no cambia en este lote: describe comportamiento implementado. Cada lote lo actualiza con lo suyo.

**Enunciados propuestos:**
- **INV-C1 ★ · Ninguna versión posterior del esquema cambia la decisión sobre una operación ya secuenciada.**
  - **Enunciado:** aplicar el log de una sala con cualquier versión del esquema bajo la que cada operación es válida da el mismo estado y la misma decisión (aplicada o rechazada, con el mismo motivo) para cada operación. Vale al recibirla, al reabrir y al reaplicar el WAL.
  - **Sostenida por:** DR3, INV-E1 e INV-E2.
  - **Cómo intentar romperla:** agregar una columna con `UNIQUE` o FK en medio de un log; réplicas que conocen la versión nueva en momentos distintos; reabrir con la versión nueva un WAL escrito con la vieja; reasignar la sala a otro esquema.
- **INV-C2 ★ · El estado de una réplica cumple las restricciones de su esquema.**
  - **Enunciado:** después de cada `apply_batch`, de abrir una sala y de aplicar un snapshot:
    - ninguna clave `UNIQUE` sin `NULL` se repite;
    - toda clave foránea sin `NULL` tiene su fila padre;
    - ninguna fila de una tabla inmutable fue reemplazada.

    Un snapshot (local o de otro peer) que no las cumple no se abre ni se instala.
  - **Sostenida por:** DR4, DR7 y DR11.
  - **Cómo intentar romperla:** violaciones concurrentes en los dos órdenes; un padre y su hijo en el mismo batch, en los dos órdenes; un snapshot con duplicados o con FK colgantes; una fila que se apunta a sí misma.
- **INV-C3 · Un rechazo no deja rastro, y los índices son función de las tablas.**
  - **Enunciado:** una operación rechazada no cambia tablas ni índices, y avanza `head_seq`. El estado queda igual que si se aplicara el log sin ella. En todo estado publicado, los índices son exactamente los que se construirían desde las tablas, en los dos motores.
  - **Sostenida por:** DR1, DR6 y "validar primero, mutar después" (`GEMINI.md`).
- **INV-A5 ★ · Nadie escribe una fila ajena.**
  - **Enunciado:** en una tabla con dueño:
    - toda operación secuenciada declara como dueño al cliente que la envió (su `ClientId`, o su seudónimo);
    - ninguna operación aplicada inserta, reemplaza, modifica ni borra una fila cuyo dueño es otro.
  - **Sostenida por:** la verificación del servidor en el commit (DR10, DR15), la de las réplicas (DR4, DR10) e INV-A1.
  - **Cómo intentar romperla:**
    - un `Insert`, `Update` o `Delete` de B que declara a A como dueño;
    - uno de B que se declara dueño sobre una fila de A, con el dueño dentro y fuera de la clave;
    - un `Update` que intenta transferir la fila;
    - un reintento deduplicado;
    - un seudónimo de A copiado por B.
  - **Fuera de alcance:** un snapshot falso (DEF-81).

**Entrega:** `docs(plan): adopt the integrity constraints decisions and contract (Fase 3.11, Lote 0)`.

**Hecho.**
- **Decisiones:** DR1 a DR14 adoptadas; DR15 (y el Lote 2b) diferida a después de la v0.1. Respuestas a §8: `RESTRICT`; se informa `MissingRow`; topes de 32 restricciones por tabla y 16 columnas por restricción; reasignación de esquema solo con las mismas restricciones, dueño e inmutabilidad; nombres como se propusieron (Fase 3.11, `owner`, `identity`, `immutable`).
- **`INVARIANTES.md` (versión 4):** grupo §3.10 con INV-C1 a C3 y la INV-A5 en §3.5, todas 🆕 con el lote que las implementa; enunciado, sostén y casos de INV-Y8 ampliados; §2 con lo que puede y no puede hacer un miembro hostil frente a las restricciones y el dueño; tres riesgos aceptados nuevos en §4 (divergencia sin detección, rechazos que ocupan el log, dueño por `ClientId` y visible); §1.6 con las decisiones DR.
- **`ROADMAP.md`:** la fase en §2, en §4.3.C y en el cronograma de §5; su cierre como requisito de la Fase 4; §4.4.4 con la validación liviana del servidor y el resultado decidido por la réplica; errores del cliente (`ConstraintViolation`, `RowMissing`); nota en el backlog 7 y backlog nuevo 11 (esquema anclado en el log), 12 (dueño seudónimo) y 13 (detección de divergencia).
- **Desvío:** ninguno. `ARCHITECTURE.md` no cambia en este lote, como estaba previsto.

### Lote 1 · `UNIQUE` y FK en el esquema (`zemdb-core`) · ✅ Hecho
**Objetivo:** que un esquema pueda declarar `UNIQUE` y FK, validadas estructuralmente, serializadas de forma estricta y evolucionables solo según DR3.

**Archivos:** `crates/core/src/schema/` (`table.rs`, `global.rs`, `column.rs`, `validation.rs`, un `constraint.rs` nuevo y `mod.rs`) y los re-exports de `lib.rs`.

**Qué se hace:**
- **Tipos nuevos.** `UniqueConstraint { name, columns }` y `ForeignKey { name, columns, references, on_delete }`, con `OnDelete::Restrict` como única variante.
  - `TableSchema` los guarda (`unique`, `foreign_keys`) y precalcula los índices de columna, como hoy `column_indices`.
  - `Schema` precalcula, para cada tabla, la lista de FK que la apuntan, en el orden de §3.1. Se marca `#[serde(skip)]` y se reconstruye al deserializar, como `id_by_name`.
- **Validación de cada tabla** (en `TableSchema::try_new`):
  - las columnas existen y no se repiten dentro de una restricción;
  - ninguna está cifrada;
  - una `UNIQUE` no es exactamente la clave primaria;
  - se respetan los topes de DR9.
- **Validación cruzada:**
  - la tabla referida existe;
  - las columnas de la FK coinciden en cantidad y tipo, en orden, con la clave primaria referida;
  - los nombres de restricción son únicos en el esquema.

  Corre con el esquema completo (`try_from_tables`, `SchemaBuilder::build`, `Deserialize`), no tabla por tabla, para que una FK pueda apuntar a una tabla de id mayor.
- **Evolución.** Un método nuevo de `Schema` agrega una columna junto con restricciones que la incluyen (DR3), y sube la versión una sola vez. `add_column` sin restricciones queda como está.
- **Serde.**
  - Los campos nuevos tienen `#[serde(default)]` y se serializan siempre, sin `skip_serializing_if`, porque bincode es posicional.
  - `ColumnDef` pasa a `deny_unknown_fields` (DR8).
- **Errores:** variantes nuevas de `ValidationError`.

**Tests exigidos.** Negativos, uno por regla:
- columna inexistente;
- columna repetida en una restricción;
- columna cifrada;
- `UNIQUE` igual a la PK;
- FK a una tabla inexistente;
- FK con otra cantidad o con otros tipos de columnas;
- nombre de restricción repetido;
- topes de DR9;
- agregar una columna con una restricción que no la incluye: error, y versión y esquema sin cambios;
- un campo desconocido dentro de una columna en JSON: error.

Positivos:
- una FK a sí misma;
- una FK a una tabla de id mayor;
- una columna nueva con `UNIQUE` y con FK: la versión sube en uno;
- roundtrip JSON y bincode de un esquema con restricciones (`format_roundtrip_tests`, `protocol_codec_tests` con `Registered`);
- un esquema sin los campos nuevos sigue leyéndose.

**Interacciones a revisar:**
- `Schema::insert_table` es público y lo usa la deserialización. La validación cruzada no puede ir ahí si las tablas llegan en orden de id y una FK apunta a una de id mayor.
- Los archivos `schemas/{id}.json` ya escritos tienen que seguir leyéndose (default vacío) y conservar su checksum (INV-D9).
- `TableBuffer::apply` valida con el esquema, y no tiene que cambiar.
- `PartialEq` y `Hash` de `Schema`, si existen: las restricciones son parte de su identidad, y los datos precalculados no.
- Los fuzzers de decodificación de `zemdb-core` (INV-W1).

**Documentos:** `ARCHITECTURE.md` §3.1, donde *Soft Foreign Keys* pasa a describir las restricciones declarables. Se aclara que la verificación llega en los lotes de storage.

**Hecho.**
- **API.** `constraint.rs` con `UniqueConstraint`, `ForeignKey`, `OnDelete::Restrict` y los topes `MAX_CONSTRAINTS_PER_TABLE` (32, `UNIQUE` y FK juntas) y `MAX_CONSTRAINT_COLUMNS` (16). `TableSchema` guarda `unique` y `foreign_keys` y precalcula `unique_column_indices` y `foreign_key_column_indices`; `TableBuilder::unique` y `::foreign_key`. `Schema` precalcula `referenced_by` y lo expone con `foreign_keys_referencing(table_id)`, en el orden de §3.1 (id de la tabla hija y declaración). `Schema::add_column_with_constraints` agrega la columna y sus restricciones y sube la versión una vez. Doce variantes nuevas de `ValidationError`.
- **Tests agregados** (`crates/core/tests/schema_constraint_tests.rs`, 24): un negativo por regla (columna inexistente, repetida y cifrada; `UNIQUE` igual a la PK, en cualquier orden; FK a una tabla inexistente; FK con otra cantidad y con otros tipos, también en otro orden; nombre repetido en la tabla, entre `UNIQUE` y FK y entre tablas; los dos topes; restricción sin columnas; restricción agregada que no incluye la columna nueva, con versión y esquema intactos; otros rechazos de `add_column_with_constraints` sin cambios; el tope contando las restricciones existentes; campo desconocido en una columna, en una `UNIQUE` y en una FK; `on_delete` faltante o desconocido) y los positivos (FK a sí misma; FK a una tabla de id mayor y ciclo entre dos tablas, por builder, `try_from_tables` y JSON; columna nueva con `UNIQUE` y con FK; el JSON de §3.3 ida y vuelta; esquema sin los campos nuevos; roundtrip binario; posiciones precalculadas y `foreign_keys_referencing`; identidad). Antes de implementar, contra un esqueleto que compilaba, fallaban 22 de 24 por comportamiento; los dos que pasaban son positivos que el esqueleto ya cumplía (leer un esquema sin los campos nuevos e identidad).
- **Tests extendidos:** el generador aleatorio de esquemas (`tests/common`) declara `UNIQUE` y FK, también al agregar columnas, y `random_schemas_round_trip_in_json_and_binary` exige que aparezcan; `sample_schema` (fuzzer de JSON de esquema y mensajes `Registered`/`Schema` del fuzzer de mensajes) tiene restricciones, una FK a una tabla de id mayor, una a sí misma y una columna agregada con restricciones; `protocol_codec_tests` con `Registered` de un esquema con restricciones; el formato esperado de `schema_table_ids_tests` incluye las dos listas vacías.
- **Mutaciones verificadas** (todas ejecutadas, cada una detectada por algún test): cada regla de la tabla (vacía, tope de columnas, columna inexistente, repetida y cifrada, tope de restricciones, `UNIQUE` = PK, la columna nueva incluida), las posiciones precalculadas, cada regla cruzada (nombre repetido, tabla referida, cantidad, tipo), la validación cruzada en `insert_table`, `try_from_tables` y `try_build`, la asignación de `referenced_by` en esos tres caminos y en `add_column_with_constraints`, la tabla candidata en la validación cruzada, validar antes de mutar en `add_column_with_constraints`, el orden de las `UNIQUE` agregadas después de las existentes, la subida de versión (y que sea una sola aunque lleguen `UNIQUE` y FK juntas), `deny_unknown_fields` de `ColumnDef`, `UniqueConstraint` y `ForeignKey`, y los dos `#[serde(default)]`.
- **Interacciones revisadas:**
  - `Schema::insert_table`: la deserialización, `try_from_tables` y `SchemaBuilder` insertan sin la validación cruzada y la corren una vez, con todas las tablas (así una FK puede apuntar a una tabla de id mayor o formar un ciclo). `insert_table`, público, ahora también la corre después de agregar su tabla y la saca si falla: una tabla suelta entra solo después de las que referencia. Tests: `a_foreign_key_to_a_missing_table_is_rejected`, `a_foreign_key_may_reference_a_table_with_a_larger_id`, `a_constraint_name_is_unique_in_the_whole_schema`.
  - `schemas/{id}.json` ya escritos: el CRC32 se verifica sobre los bytes tal como se escribieron (`durable::verified_json`) antes de parsear, y las listas faltantes leen vacías. Test: `a_schema_without_constraint_fields_still_reads`. El registry solo reescribe en `add_column`, con un checksum nuevo.
  - `TableBuffer::apply` y `validate_operation`: no cambian ni miran las restricciones (`cargo test --workspace` en verde).
  - `PartialEq` y `Hash`: siguen derivados. Los datos precalculados (`column_indices`, las posiciones, `id_by_name`, `referenced_by`) son función de los campos serializados, así que no cambian la igualdad; las restricciones sí son parte de la identidad. Tests: `constraints_are_part_of_the_schema_identity` y los roundtrips, que comparan también lo reconstruido.
  - Fuzzers de decodificación (INV-W1): el de JSON de esquema reconstruye cada tabla con `try_with_constraints`, y los de mensajes llevan esquemas con restricciones. Los topes se chequean antes de recorrer cada restricción, y la validación cruzada es O(restricciones · log).
  - `SchemaBuilder::build` ahora entra en pánico si la validación cruzada falla, como ya lo hacía `table`; `try_build` es la variante que devuelve el error. Los usos de servidor, storage y cliente no declaran restricciones.
  - **Servidor, sin cambio de código:** `POST /admin/schemas` ya acepta restricciones, porque deserializa `Schema` con su validación (una inválida es 400), y la columna de `POST .../columns` rechaza campos desconocidos porque es un `ColumnDef`. `.../columns` sigue usando `add_column`, sin restricciones. Los tests de la API y `§7.1` quedan para el Lote 2.
  - Storage: recibe esquemas con restricciones y todavía las ignora (Lotes 3 a 5). El `.snap` no guarda el esquema, y la apertura solo verifica que existan las tablas.
  - **Ventana hasta el Lote 2** (revisión de delta, L1): `AddColumnRequest` no rechaza campos desconocidos, así que `POST .../columns` con el cuerpo de §3.3 responde 200 y agrega la columna **sin** la restricción, en silencio, y por DR3 ya no se le puede agregar después. `ARCHITECTURE.md` §3.1 lo aclara: hasta el Lote 2, las restricciones solo se declaran al crear el esquema. El Lote 2 lo cierra (`deny_unknown_fields` y restricciones en `AddColumnRequest`); conviene no publicar un release entre los dos lotes.
  - **Reasignación de una sala** (`RoomManager::get_or_spawn` con esquema): hoy acepta un esquema con otras restricciones. Es inocuo mientras el storage no las verifica, y lo cierra el Lote 2.
  - **`Registered`, `Schema` y `ReloadSchema`** llevan esquemas más grandes: acotados por los topes (32 restricciones de hasta 16 columnas por tabla). El tamaño frente a la reserva de 16 MiB por respuesta se revisa en el Lote 2, como ya estaba previsto.
- **Revisión de delta** (agente aparte, contexto fresco, solo lectura): sin hallazgos Altos ni Medios. Se atendieron:
  - **L2 y L3:** el test de evolución agrega en una sola llamada una `UNIQUE` y una FK sobre una tabla que ya tenía una `UNIQUE`, y verifica la versión, el orden y las posiciones. Las dos mutaciones que sobrevivían ahora se detectan.
  - **N4:** `insert_table` y `add_column_with_constraints` mutaban y revertían ante un error. Ahora la validación cruzada no muta: valida el esquema con la tabla candidata y devuelve el `referenced_by` nuevo, y recién entonces se escribe.
  - **L1, N2 y N3:** en las interacciones.
  - **N1:** el estado de INV-C1 en `INVARIANTES.md` pasa de "Lotes 1 y 5" a "Lote 5" (ver desvíos).
- **Chequeos:** fmt, clippy, test, doc y el build wasm32 de §5.1 en verde, y `cargo test -p zemdb-core --release -- --ignored`.
- **Desvíos:**
  - **Regla agregada:** una restricción sin columnas es un error (`EmptyConstraint`). Su clave sería la tupla vacía, nunca `NULL`, así que todas las filas chocarían.
  - **`TableSchema::try_new` conserva su firma** (tabla sin restricciones) y la validación vive en `TableSchema::try_with_constraints`, al que delega. Se agregó `SchemaBuilder::try_build`.
  - **`insert_table` valida lo cruzado** (ver interacciones). El plan solo pedía no ponerlo en el camino de la deserialización.
  - **`INVARIANTES.md`:** el plan no lo listaba para este lote. INV-C1 deja de esperar al Lote 1, porque la regla de DR3 en la que se apoya ya está, y su "Sostenida por" en INV-E1 suma `add_column_with_constraints`. Sigue 🆕 hasta el Lote 5 y la revisión de cierre.
  - **Tamaño:** ~470 líneas de producción sin comentarios, por encima del tope de ~300 de §5.1. La mayor parte son las doce variantes de error y los literales que expande rustfmt. No se partió el lote.
- **Fuera del lote (backlog):** `TableSchema::try_new` acepta una columna repetida en la clave primaria (por ejemplo, `"primary_key": ["id", "id"]` en JSON; `TableBuilder` la deduplica). Es preexistente y de severidad baja: la clave queda con aridad 2 y se valida igual en todos lados. Con el Lote 1, además, una `UNIQUE` `["id"]` sobre esa tabla no se reconoce como igual a la PK.

### Lote 2 · Servidor: dueño, inmutabilidad y API de administración · ✅ Hecho
**Objetivo:** lo que verifica el servidor (DR10 y DR11) y la administración de los esquemas con restricciones.

**Archivos:**
- `crates/core/src/schema/` (dueño e inmutabilidad);
- `crates/core/src/mutation/` (`Delete { owner }` y el squashing);
- `crates/server/src/api/control_plane.rs`;
- `crates/server/src/schema_registry.rs`;
- `crates/server/src/actor/room.rs` (commit);
- `crates/server/src/actor/manager.rs` (reasignación).

**Qué se hace:**
- **Core.**
  - `owner: Option<OwnerDef>` (columna e identidad) e `immutable: bool` en `TableSchema`, con su validación estructural:
    - la columna existe y no está cifrada;
    - es `String` para `ClientId` y `Bytes` para `Pseudonym`;
    - el seudónimo se rechaza hasta el Lote 2b.
  - `OperationKind::Delete` pasa a llevar `owner: Option<Value>`.
  - `validate_operation`:
    - rechaza `Update` y `Delete` en una tabla inmutable;
    - en una tabla con el dueño fuera de la clave, exige el dueño declarado (un delta de la columna dueño en el `Update`, `owner` en el `Delete`);
    - en cualquier otra tabla, rechaza un `Delete` con `owner`.
  - Una función de core devuelve el dueño que declara una operación, y otra lo compara con un `ClientId`.
- **Commit.** El actor verifica el dueño después de `validate_operation` y antes de secuenciar. Si no coincide, responde `SchemaViolation`.
- **API de administración.**
  - `AddColumnRequest` acepta las restricciones de la columna nueva.
  - `CreateSchemaRequest` y `AddColumnRequest` pasan a `deny_unknown_fields`.
- **Reasignación.** `RoomManager::get_or_spawn` con esquema, sobre una sala existente, se rechaza si el esquema nuevo no tiene las mismas restricciones. Se verifica antes de escribir `meta_room.json`.

**Tests exigidos:**
- **Dueño:**
  - B no puede insertar, actualizar ni borrar declarando a A como dueño, con el dueño dentro o fuera de la clave. Responde `SchemaViolation`, sin secuencia nueva, sin evento SSE y sin cambio en el head (INV-Y5).
  - A puede hacer las tres cosas.
  - Con el dueño fuera de la clave, un `Update` sin el delta de la columna dueño, o un `Delete` sin `owner`, es `SchemaViolation`.
  - Un `Delete` con `owner` en una tabla sin dueño fuera de la clave, también.
  - B que se declara dueño sobre una fila de A se secuencia: lo rechazan las réplicas (Lote 4).
  - Un reintento deduplicado de un commit aceptado sigue respondiendo su secuencia original.
- **Formato:**
  - roundtrip de `Delete { owner }` en los mensajes, en los frames WAL y en `TableBuffer`;
  - el tamaño de `Operation` en `types_memory_tests` (debería seguir en 88 bytes);
  - squashing: un `Delete` que reemplaza una operación pendiente conserva su `owner`, y un `Update` consolidado conserva el delta del dueño.
- **Inmutable:**
  - `Update` y `Delete` dan `SchemaViolation`.
  - Dos `Insert` con la misma clave se secuencian los dos: el servidor no lo decide.
- **Que el servidor no decide `UNIQUE` ni FK:** dos commits con la misma clave `UNIQUE` se secuencian los dos (DR1).
- **API:**
  - crear un esquema con restricciones: 201, y el GET las devuelve;
  - una definición inválida es 400 y no escribe nada;
  - `add_column` con una restricción que incluye la columna: 200, la versión sube y las salas abiertas reciben el esquema;
  - una restricción que no la incluye es 400, y la versión no cambia;
  - un campo desconocido en el cuerpo es 400.
- **Reasignación:** con otras restricciones, error y `meta_room.json` intacto.

**Interacciones a revisar:**
- El orden de las verificaciones del commit. El dedup va primero (un reintento siempre se reconoce), después la retención, `validate_operation` y el dueño.
- El lock del registry y el `.tmp` de `add_column`.
- `reload_schema_for_rooms` y `ReloadSchema`.
- `Registered` y `GetSchema` con esquemas más grandes, frente a la reserva de 16 MiB por respuesta.
- `ClientId` contra `Value::String`: la comparación es byte a byte, sin normalizar.
- `Delete { owner }` toca todo lo que codifica operaciones:
  - el codec del protocolo y sus fuzzers;
  - los frames WAL del servidor y del storage;
  - `check_operation_size`;
  - las reglas de squashing de `ARCHITECTURE.md` §4.1, incluida la anti-zombi.
- El storage valida cada operación con `validate_operation`, así que exige lo mismo que el servidor. Un `Delete` sin `owner` en una tabla que lo pide es un log dañado.

**Documentos:** `ARCHITECTURE.md`:
- §3.1: dueño e inmutabilidad;
- §4: `Delete { owner }` y el dueño declarado;
- §5.1: el servidor verifica el dueño;
- §7.1: los cuerpos de `/admin/schemas` y `.../columns`, y el rechazo de campos desconocidos;
- §7.2: `SchemaViolation` por dueño o inmutabilidad.

**Hecho.**
- **API.**
  - **Core.** `OwnerDef { column, identity }` y `OwnerIdentity { ClientId, Pseudonym }` en `constraint.rs`. `TableSchema` guarda `owner` e `immutable` y precalcula `owner_column_index`; se validan en `TableSchema::try_with_owner` (y `with_immutable`), que usan el builder (`TableBuilder::owner`, `::immutable`), la deserialización y `with_added_column`. `OperationKind::Delete { owner: Option<Value> }`, con `Operation::delete_with_owner`. `TableSchema::declared_owner` devuelve el dueño que declara una operación, y `Schema::check_owner` lo compara byte a byte con un `ClientId`. `Schema::has_same_constraints` compara restricciones, dueño e inmutabilidad tabla por tabla. Seis variantes nuevas de `ValidationError`.
  - **Servidor.** El actor corre `check_owner` después de `validate_operation` y antes de secuenciar (`SchemaViolation`). `CreateSchemaRequest` y `AddColumnRequest` rechazan campos desconocidos; `AddColumnRequest` lleva `unique` y `foreign_keys`, y el endpoint usa `SchemaRegistry::add_column_with_constraints` (nuevo; `add_column` delega en él). `RoomManager::check_reassignment` rechaza la reasignación antes de escribir `meta_room.json`.
- **Tests agregados** (38):
  - `crates/core/tests/schema_owner_tests.rs` (15): la columna dueño inexistente, cifrada, nullable o de otro tipo; el seudónimo; el dueño inválido o con campos desconocidos en JSON; el JSON de §3.3 ida y vuelta, en JSON y binario; una tabla sin los campos nuevos; la identidad; `add_column` y `add_column_with_constraints` conservan dueño e inmutabilidad; `validate_operation` (tabla inmutable; `Update` sin el delta del dueño; `Delete` sin `owner`, con un `owner` nulo o de otro tipo, y con `owner` donde no corresponde), también a través de `to_operation_update` y `to_operation_delete`; el dueño declarado en cada caso; `check_owner` (otro cliente, otra capitalización, NFC contra NFD, prefijo y sufijo); y `has_same_constraints` (iguales con columnas y tablas sin restricciones agregadas; distintos con otra `UNIQUE`, otra FK, otro dueño, otra inmutabilidad, otra tabla referida con el mismo nombre y otra posición de columna).
  - Formato (`Delete { owner }`): `protocol_codec_tests` (mensajes, también con el presupuesto de valores, y el tamaño), `wal_frame_tests`, `squashing_rules_tests` (2: el `Delete` que reemplaza una operación pendiente conserva su `owner`, el `Update` consolidado conserva el delta del dueño, un `Update` sin el dueño no se encola, y el `TableBuffer` ida y vuelta en binario y JSON) y el test unitario del presupuesto de valores (`protocol/tests/codec.rs`). `types_memory_tests` sigue en 88 bytes sin cambios.
  - `crates/server/tests/owner_commit_tests.rs` (8): B no inserta, actualiza ni borra declarando a A, con el dueño dentro y fuera de la clave (`SchemaViolation` sin rastro: sin secuencia, sin evento, el cursor igual y el `mutation_id` sin registrar); A hace las tres cosas; el dueño no declarado; B declarándose dueño de la fila de A se secuencia; el reintento deduplicado (también con otra operación, porque el dedup va primero); la tabla inmutable; dos `UNIQUE` iguales se secuencian; y por HTTP es `SchemaViolation`, no `Forbidden`.
  - `admin_schema_http_tests` (6): crear con restricciones, dueño e inmutabilidad (201 y GET); definiciones inválidas (400 sin escribir); `add_column` con una restricción que incluye la columna (200, versión 2, la sala abierta recibe el esquema; antes, una FK a una tabla inexistente es 400 sin cambios); una que no la incluye (400, el GET igual); campos desconocidos en los dos cuerpos (400); y un esquema al tope de restricciones llega entero en `Registered` y `Schema`.
  - Reasignación: `actor/tests/manager.rs` (1), en las dos direcciones, con `meta_room.json` byte a byte igual y la sala abriéndose con su esquema; una sala en marcha no se reasigna.
  - `op_bytes` del log del servidor cuenta el dueño (`log/tests/budget.rs`, 1), y `memory_engine_tests` (2): en los dos motores, un batch con una operación que el servidor rechaza falla entero; en el de disco, además, un `Delete` con dueño se relee del WAL al reabrir.
  - **Extendidos:** el generador de operaciones aleatorias (`tests/common`) produce `Delete` con y sin dueño (fuzzers de decodificación y roundtrips de mensajes y WAL); el de esquemas declara dueños y tablas inmutables, y `random_schemas_round_trip_in_json_and_binary` exige que aparezcan; `sample_schema` (fuzzer de JSON de esquema, que ahora reconstruye cada tabla también con su dueño e inmutabilidad, y mensajes `Registered`/`Schema`) suma una tabla inmutable con el dueño fuera de la clave; `sample_ops` suma un `Delete` con dueño (fuzzers de mensajes y de frames WAL); el formato esperado de `schema_table_ids_tests` y el JSON documentado de `schema_constraint_tests` incluyen `owner` e `immutable`; `room_actor_tests` espera `BadRequest` del registry.
  - **Contra el esqueleto** (compilaba y conservaba el comportamiento viejo): de los 31 escritos antes de implementar, fallaban 23 por comportamiento. Los 8 que pasaban son positivos o de formato que el esqueleto ya cumplía (roundtrips, identidad, una tabla sin los campos nuevos, el dueño que escribe lo suyo, la operación sobre una fila ajena que se secuencia, el reintento, `UNIQUE` sin decidir y el esquema al tope). Los de `op_bytes` y storage se agregaron durante la implementación; los cubren las mutaciones.
- **Mutaciones verificadas** (50, todas ejecutadas una por vez y restauradas después; cada una la detecta algún test): cada regla de la columna dueño (que exista, sea `String`, no sea nullable ni cifrada, y el seudónimo) y su posición precalculada; que el builder, la deserialización y `with_added_column` conserven dueño e inmutabilidad; que `to_operation_update`, `to_operation_delete`, `Schema::validate_update` y `Schema::validate_delete` validen la operación entera; cada regla de `validate_operation` (la tabla inmutable, el delta del dueño en el `Update`, el `owner` del `Delete` y su tipo, el `owner` donde no corresponde, y que el dueño esté fuera de la clave); cada fuente del dueño declarado (clave, fila, delta y `Delete`); la comparación de `check_owner` y su salida para una tabla sin dueño; cada campo que compara `same_constraints`, cada una de las cuatro condiciones de `has_constraints`, y la cantidad y `referenced_by` en `has_same_constraints`; el cobro del dueño en los dos presupuestos; `check_owner` en el commit; `deny_unknown_fields` en los dos cuerpos; que el registry agregue las restricciones y responda `BadRequest`; y la verificación de la reasignación.
  - **Sobrevivientes de la primera pasada:** cinco, todas en la comparación de esquemas: no comparar el `table_id`, las `UNIQUE` declaradas, las FK declaradas o sus posiciones en `same_constraints`, y `referenced_by` en `has_same_constraints`. Se agregaron casos a `schemas_with_the_same_constraints_owners_and_immutability_are_interchangeable`: la misma restricción en otra tabla, una `UNIQUE` y una FK renombradas, una FK sobre otra posición, la columna dueño renombrada y en otra posición, y la tabla referida, sin restricciones propias, con otro id. Las cinco pasaron a detectarse.
- **Interacciones revisadas:**
  - **El orden del commit:** registro, `last_ack_seq`, dedup, retención, `validate_operation`, `check_owner` y el tamaño. Un reintento se reconoce antes de mirar la operación, y un rechazo por dueño o inmutabilidad no deja rastro (INV-Y5). Tests: `a_retried_commit_is_acknowledged_with_its_original_sequence`, `nobody_writes_in_the_name_of_another_owner`.
  - **El lock del registry y el `.tmp`:** `add_column_with_constraints` corre en el mismo `blocking_io`, bajo `lock_writes`, y valida sobre una copia antes de `persist`, así que un rechazo no escribe nada. `add_column` delega en él, de modo que los tests de concurrencia y de fallas del registry (`schema_registry.rs`, `concurrent_column_additions_leave_every_room_with_the_latest_schema`) recorren el camino nuevo. Test: `a_constraint_that_omits_the_new_column_is_bad_request_and_changes_nothing`.
  - **`reload_schema_for_rooms` y `ReloadSchema`:** sin cambios; manda el `Arc` del registry. Test: `a_column_added_with_constraints_that_include_it_reaches_the_open_rooms`.
  - **`Registered`, `Schema` y `ReloadSchema` frente a la reserva de 16 MiB:** `ReloadSchema` pasa un `Arc`, sin copiar ni codificar. `Registered` y `Schema` se arman después de reservar un frame del presupuesto de respuestas, y `encode_message` nunca produce más de un frame, así que la reserva siempre alcanza. Un esquema llega por el JSON de un cuerpo de hasta un frame, y su codificación wire es menor (cada nombre de campo, comilla y nombre de tipo del JSON es un varint o un tag): 40 tablas al tope de restricciones dan 224 KB en el wire contra 341 KB de JSON. Test: `a_schema_at_the_constraint_limits_reaches_clients_in_one_frame`. Ver el backlog sobre `add_column` repetidos.
  - **`ClientId` contra `Value::String`:** comparación byte a byte, sin normalizar. Test: `only_the_declared_owner_may_send_an_operation`.
  - **`Delete { owner }` en lo que codifica operaciones:** el codec y sus fuzzers (el generador aleatorio produce el dueño), los frames WAL (el storage usa el mismo `encode_wal_batch` de core), `check_operation_size` (mide la operación codificada, dueño incluido), el presupuesto de valores de un mensaje de cliente (el dueño se cobra al decodificarse, como un valor de columna) y el de lectura del log (`op_bytes`). Squashing: la regla anti-zombi sigue (`Delete { .. }` seguido de `Update` es `Incompatible`) y el dueño se conserva. `ARCHITECTURE.md` §4.1 anota que la equivalencia del squashing deja de valer con restricciones (dos `Insert` de una clave inmutable), como dice §7.
  - **Storage:** `validate_batch` usa `validate_operation`, así que un batch con un `Update` o `Delete` de una tabla inmutable, o sin el dueño requerido, es un log dañado y falla entero (INV-S1). Tests: `memory_rejects_a_batch_with_an_operation_the_server_rejects` y `disk_rejects_a_batch_with_an_operation_the_server_rejects`.
  - **El formato de `TableBuffer` en JSON:** el `Delete` pasa de `"Delete"` a `{"Delete": {"owner": null}}`. No hace falta compatibilidad (`GEMINI.md`).
  - **`schemas/{id}.json` ya escritos:** leen sin dueño ni inmutabilidad (default) y conservan su checksum (INV-D9). Test: `a_table_without_owner_or_immutability_still_reads`.
  - **La sala en marcha:** `get_or_spawn` con un esquema devuelve el actor vivo sin reasignarlo (como antes); la reasignación, y su verificación, solo ocurren al abrir la sala. Test: `a_room_moves_only_to_a_schema_with_the_same_constraints_owners_and_immutability`. Si la reasignación se rechaza, `slot` saca la sala de `room_meta`, lo que es inocuo porque no está viva.
  - **Los validadores por nombre** (`Schema::validate_update` y `validate_delete`, sin llamadores en el workspace) validaban solo la clave y los campos: ahora validan la operación que construyen `to_operation_update` y `to_operation_delete` (ver desvíos).
  - **`TableBuffer::apply` no conoce al cliente,** así que no puede correr `check_owner`: una outbox puede fusionar una operación con un dueño ajeno dentro de una válida, y el servidor rechaza la fusionada entera. Se agregó a la nota de §4.1 de `ARCHITECTURE.md`; lo resuelve el diseño de la outbox (DR12; la outbox está diferida para después de la v0.1).
  - **Los generadores del storage** (oráculo, motores aleatorios, convergencia) todavía no producen tablas con dueño ni inmutables, ni `Delete` con dueño; solo un test de disco relee uno del WAL. Se agregó a los tests del Lote 4.
- **Revisión de delta** (agente aparte, contexto fresco, solo lectura): sin hallazgos Altos ni Medios. Se atendieron:
  - **B1:** la condición de las FK en `has_constraints` no tenía test (una tabla con solo una FK quedaba fuera de la comparación). Se agregó el caso, y su mutación y la de las `UNIQUE` se detectan. La nota decía "cada condición" y solo se habían mutado dos: ahora son las cuatro.
  - **B2:** `Schema::validate_update` y `Schema::validate_delete` aceptaban un `Update` o `Delete` de una tabla inmutable, o sin el dueño requerido. Ahora validan la operación entera, con tests y mutaciones.
  - **N1 y N2:** el nombre truncado de un test en esta nota, y la redacción de §5.1 de `ARCHITECTURE.md`, que leía la inmutabilidad como si dependiera del dueño.
  - **N3:** el `#[serde(default)]` de `owner` es redundante (serde lee un `Option` faltante como `None`), y su mutación es equivalente. Se deja, por simetría con `immutable`.
  - **Interacciones** que faltaban: la sala en marcha, los validadores por nombre, `TableBuffer` frente al dueño ajeno y los generadores del storage (arriba); las preexistentes, al backlog.
  - El revisor corrió, además, 14 mutaciones propias (la del atajo `from == to` y la de N3 son equivalentes) y las 40 a 45 por su cuenta.
- **Chequeos:** fmt, clippy, test (863 tests) y doc de §5.1, y el build y clippy wasm32, en verde; los dos jobs MSRV (`cargo +1.88.0 test --workspace` y `cargo +1.85.0 build --workspace --lib --bins`, con un `CARGO_TARGET_DIR` aparte); y `cargo test -p <crate> --release -- --ignored` de core, server y storage (el de core, repetido después de los cambios de la revisión).
- **Desvíos:**
  - **Regla agregada:** la columna dueño no puede ser nullable. Un dueño `NULL` no declara a nadie: así un `Insert`, un delta o un `owner` nulos se rechazan con la validación de siempre.
  - **`add_column` responde `BadRequest`,** no `SchemaViolation` (los dos son 400), para cualquier error de validación, como dice §3.3 para las restricciones y como `POST /admin/schemas`.
  - **`to_operation_update` y `to_operation_delete` validan la operación entera,** para no construir una que el esquema rechaza.
  - **El dueño de un `Delete` se cobra** en el presupuesto de valores de un mensaje de cliente y en `op_bytes`. El plan no lo listaba.
  - **`SchemaRegistry::add_column_with_constraints`** se agregó al lado de `add_column`, que delega en él, en vez de cambiar su firma.
  - **`OwnerIdentity::Pseudonym` existe y se rechaza,** para que el Lote 2b solo lo acepte.
  - **La reasignación rechazada es `BadRequest`.** Además, como se verifica antes de escribir, una reasignación a un esquema que no existe (o desde uno que falta del registry) falla con `SchemaNotFound` sin reescribir `meta_room.json`; antes se reescribía y fallaba después.
  - **Los validadores por nombre** (`Schema::validate_update` y `validate_delete`) delegan en `to_operation_update` y `to_operation_delete` (revisión de delta, B2).
  - **`INVARIANTES.md`:** INV-A5 pasa de "Lotes 2 y 4" a "Lote 4", y su "Sostenida por" nombra `validate_operation` y `check_owner`. Sigue 🆕.
  - **`ARCHITECTURE.md`:** además de §3.1, §4, §5.1, §7.1 y §7.2, una nota en §4.1 (squashing) y una frase en §8 (el storage rechaza lo que el servidor no secuencia).
  - **Tamaño:** ~310 líneas de producción agregadas sin comentarios ni blancos (unas 420 con la documentación), en el tope de §5.1.
- **Fuera del lote (backlog):**
  - **`add_column` repetidos pueden hacer crecer un esquema sin tope** (columnas de nombres largos, con o sin restricciones), hasta que `Registered` y `Schema` no entran en un frame y fallan al codificarse para todos los clientes de sus salas. Es preexistente (alcanza con columnas) y solo lo puede provocar el administrador, que es de confianza: severidad baja. Lo cerraría que el registry rechace un cambio cuyo esquema no entra en una respuesta.
  - **Una sala sin `meta_room.json` pero con log:** `get_or_spawn` con un esquema la trata como nueva, sin verificar la reasignación, y crea otra encarnación sobre el log viejo. No diverge, porque los clientes se reinician por la encarnación, pero ese log pasa a decidirse con otras restricciones. Es preexistente y solo lo alcanza la API de la librería: severidad baja.
  - **La versión del esquema después de una reasignación:** el cliente se entera del cambio por el número de versión, que puede coincidir en el esquema destino; ahora las restricciones son las mismas, pero las columnas pueden no serlo. Preexistente (INV-E1 e INV-E2 frente a la reasignación): severidad baja.
  - **La copia del esquema por respuesta:** `Registered` y `Schema` clonan el `Schema` para codificarlo. Esa memoria es transitoria y no la cuenta el presupuesto de respuestas, aunque la acota la cantidad de reservas concurrentes. Con restricciones, un esquema ocupa en memoria varias veces lo que mide en el wire (cada columna de una restricción es un `String`). Baja; lo cerraría que el mensaje lleve el `Arc`.

### Lote 2b · Dueño seudónimo · ⏸ Diferido
**Objetivo:** DR15. Se difirió a después de la v0.1 (los votos anónimos no son un requisito); no afecta al resto de la fase. Queda especificado para retomarlo.

**Archivos:**
- `crates/server/src/config.rs` (el secreto);
- `crates/server/src/actor/room.rs` (verificación y `Registered`);
- `crates/core/src/protocol/messages.rs` (`Registered`);
- `crates/core/src/schema/` (aceptar `Pseudonym`);
- una función de core que deriva el seudónimo de una tabla desde la clave de dueño, compartida por el servidor y el SDK.

**Qué se hace:**
- **El secreto.** `ZEMDB_OWNER_SECRET`: opcional, de al menos 32 bytes, distinto de los otros dos y validado al arrancar como ellos.
  - Sin él, `POST /admin/schemas` rechaza un esquema con seudónimos (`BadRequest`).
  - Si ya hay uno registrado que los usa, el servidor no arranca.
- **La clave de dueño.**
  - Se deriva con BLAKE3 en modo con clave, a partir del secreto, la sala y el cliente.
  - `Registered` la lleva, siempre que el servidor tenga el secreto.
- **El commit.** En una tabla con seudónimo, el dueño declarado se compara con el seudónimo recalculado desde el cliente autenticado.

**Tests exigidos:**
- B no puede escribir con el seudónimo de A, aunque lo copie de una fila.
- El seudónimo de un cliente es el mismo después de reiniciar el servidor y después de reabrir la sala.
- Es distinto para otro cliente, para otra sala y para otra tabla.
- Sin el secreto: el esquema con seudónimos es 400, y el arranque con uno ya registrado falla con un error que nombra la variable.
- Registro sin cursor y con cursor: los dos devuelven la misma clave.

**Interacciones a revisar:**
- **La encarnación de la sala.** Si una sala recreada debe dar otros seudónimos, la encarnación entra en la derivación.
- **Los logs (`tracing`):** nunca imprimen la clave de dueño.
- **El orden de las verificaciones del commit**, igual que en el Lote 2.

**Documentos:**
- `ARCHITECTURE.md`: §3.1, §6 (`Registered`) y §7.2.
- `README.md`: la variable nueva.

### Lote 3 · Índices derivados y `ApplyReport` (`zemdb-storage`) · ✅ Hecho
**Objetivo:** que todo camino que cambia tablas mantenga los índices de las restricciones, y que `apply_batch` informe su resultado. Todavía no se rechaza nada por restricciones.

**Archivos:**
- `crates/storage/src/memory/state.rs` (`RoomState`, `apply_operation`);
- un `memory/index.rs` nuevo;
- `memory/mod.rs`;
- `disk/mod.rs`;
- `disk/recovery.rs`;
- `engine.rs` (contrato y rustdoc);
- los re-exports de `lib.rs`.

**Qué se hace:**
- **Índices.** `RoomState` suma los índices de DR6. Como son multimapas, se mantienen consistentes aunque todavía nada impida duplicados.
- **Dónde se mantienen:**
  - en `apply_operation`;
  - en la apertura: se construyen desde el snapshot, antes del replay;
  - en el replay de `.wal.compacting` y del WAL activo;
  - en `apply_snapshot`: se construyen en el pool bloqueante y se publican junto con las tablas.
- **Motor de disco:** `apply_batch` clona tablas e índices (O(1)), aplica y publica todo junto.
- **Contrato:** `apply_batch` devuelve `ApplyReport`. En este lote, el único motivo que puede aparecer es `MissingRow`.

**Tests exigidos.** Unitarios, porque los índices son internos:
- **Propiedad central:** después de batches aleatorios sobre un esquema con restricciones, en los dos motores, los índices son iguales a los reconstruidos desde las tablas. Vale también después de reabrir, de una compactación seguida de reapertura, y de `apply_snapshot`.
- **Reporte:** `MissingRow` aparece para un `Update` sobre una fila inexistente o borrada, y para nada más. Es igual en los dos motores.
- **INV-S1:** un batch rechazado por hueco o por esquema no cambia nada, índices incluidos.

**Interacciones a revisar:**
- **El clon y la publicación del motor de disco:** un lector nunca ve tablas e índices de momentos distintos. También el punto de pánico `apply_batch.apply`.
- **La compactación:** captura solo tablas, y no publica tablas sin índices.
- **`apply_snapshot`:**
  - en el head, no cambia nada e ignora el esquema;
  - adelante, reemplaza esquema, tablas e índices de una vez.
- **La apertura:** `SchemaMismatch` sigue saliendo antes de construir nada, y el orden snapshot → `.wal.compacting` → WAL activo se mantiene.
- **Scans y `get`:** no cambian.
- **INV-S6:** memoria igual a disco ahora incluye índices y reportes.
- **La memoria:** una entrada por fila y por restricción, solo con clave sin `NULL`.
- **wasm32:** `imbl` ya compila ahí, y la construcción corre inline.

**Documentos:** `ARCHITECTURE.md` §8: el contrato de `apply_batch` y los índices derivados.

**Hecho.**
- **API.**
  - **Contrato** (`engine.rs`, reexportado en `lib.rs`): `apply_batch` devuelve `ApplyReport` (campos privados; `head_seq()` y `rejected()`, una lista de `(SequenceNumber, RejectReason)` en orden de secuencia, solo con las operaciones no aplicadas). `RejectReason` tiene una sola variante, `MissingRow`; las del Lote 4 se agregan ahí. El rustdoc de `apply_batch` describe el contrato: cada operación se decide sobre el estado que dejó la anterior, un rechazo no cambia nada y avanza `head_seq`, un `Delete` de una fila inexistente no se informa, y el replay toma las mismas decisiones y las descarta.
  - **Índices** (`memory/index.rs`, nuevo): `ConstraintIndex = imbl::OrdSet<(clave, PK)>`, un multiconjunto ordenado y persistente con una entrada por fila cuya clave no tiene `NULL` (una columna que le falta a una fila corta lee `NULL`). `TableIndexes` guarda uno por `UNIQUE` y uno por FK, en orden de declaración, y sabe construirse desde las filas (`build`) y reemplazar las entradas de una fila vieja por las de la nueva (`update`).
  - **`RoomData`** (`memory/state.rs`): las tablas y sus índices en un solo valor, con campos privados. Se construye desde las tablas (`build`, O(N log N) por restricción) o vacío (`empty`), se lee con `tables()`, y solo `apply_operation` lo cambia. `RoomState` y `DiskRoomState` guardan un `RoomData` en lugar de las tablas, así que tablas e índices se clonan, cambian y reemplazan juntos.
  - **`apply_operation`** recibe el `RoomData` y devuelve `Option<RejectReason>`: decide antes de tocar nada (un `Update` sin fila es `MissingRow`), y después actualiza índices y tabla con la fila vieja y la nueva. Un `Delete` sin fila no crea la tabla. `TableIndexes::update` verifica en debug que tenga un índice por restricción del esquema.
  - **Dónde se construyen:** al abrir una sala de disco, desde el snapshot y en el pool bloqueante, después de `SchemaMismatch` y antes del replay (`load_snapshot`); al aplicar un snapshot, dentro de `decode_received_snapshot`, que corre en el pool bloqueante (inline en wasm32) y ahora devuelve `(head_seq, RoomData)`; y vacíos al abrir una sala de memoria o una de disco sin snapshot.
  - **Motor de disco:** `apply_batch` clona el `RoomData` (O(1) por tabla y por restricción), aplica, junta los rechazos y publica todo bajo el lock de estado. La compactación captura solo las tablas.
- **Tests agregados** (18):
  - `crates/storage/src/memory/tests/index.rs` (3 unitarios): solo se indexan claves sin `NULL` (también la columna que falta en una fila corta y la clave compuesta con un solo `NULL`), y una clave compuesta sigue el orden en que la restricción declara sus columnas; filas con la misma clave tienen cada una su entrada; un `update` mueve, saca y agrega las entradas de una fila. Además, los helpers que comparten los tests de los dos motores: el esquema con restricciones (dos `UNIQUE`, una compuesta con un `Float`; una FK a otra tabla y una a la misma tabla; una tabla sin restricciones; las compuestas nombran sus columnas en otro orden que la tabla) y su versión con una columna agregada con `UNIQUE`; `RandomLog`, un log aleatorio de dominios chicos (claves de 4 valores, `NULL`, filas cortas, -0.0 y dos NaN) que lleva qué filas existen y da el reporte esperado de cada batch; `RoomContents`, que lee esquema y `RoomData` de cualquiera de los dos motores; la comparación de los índices contra los recalculados desde las filas sin el código de producción (columnas buscadas por nombre); y los escenarios genéricos de batch rechazado, de snapshots y de construcción fuera del runtime.
  - `src/tests/memory.rs` (4): la propiedad central sobre 60 batches aleatorios; los batches rechazados por hueco y por esquema no cambian ni filas ni índices; los snapshots en el head (con otro esquema, que se ignora) y adelante (con el esquema evolucionado, cuyos índices se construyen) y los batches posteriores; los índices de un snapshot se construyen fuera del runtime.
  - `src/tests/disk.rs` (8 y uno `#[ignore]`): la propiedad central con una réplica en memoria del mismo log (reportes, filas e índices iguales después de cada batch) a través de reaperturas: desde el WAL solo, desde un snapshot compactado y el WAL posterior, con una versión posterior del esquema (las filas viejas son cortas para la columna nueva) y con el `.wal.compacting` que deja una compactación fallida; los batches rechazados, también después de reabrir; los snapshots y la reapertura posterior; un pánico al aplicar no publica ni filas ni índices, y la reapertura los rehace; la construcción al abrir y al aplicar un snapshot no corre en el runtime (en un runtime de un hilo, el punto de hilo `room_data.build` nunca se alcanza desde el hilo del test); un `.wal.compacting` cuyo único registro vivo es un `Update` rechazado se pliega y el head no retrocede; `apply_batch` y `apply_snapshot` soltados después de cualquier cantidad de suspensiones dejan índices que siguen a las filas, en memoria y al reabrir, y el snapshot queda entero o no queda; y la versión larga, con 200 logs reabiertos y compactados en puntos aleatorios.
  - `tests/apply_report_tests.rs` (2, integración): `MissingRow` para un `Update` de una fila nunca insertada, borrada en el mismo batch y en uno anterior, y nada para inserts, updates de filas existentes, el `Delete` de una fila inexistente ni el batch vacío; el head avanza aunque el último del batch se rechace; el motor de disco informa lo mismo que el de memoria, y al reabrir rehace las mismas decisiones.
  - **Contra el esqueleto** (compilaba y conservaba el comportamiento viejo: índices sin mantener y reporte siempre vacío): fallaban por aserción los 13 escritos antes de implementar (10 unitarios, la versión larga y los 2 de integración). Un decimocuarto, que contaba los índices por tabla, pasaba contra el esqueleto y se quitó: lo cubren los demás. Los otros cinco se agregaron después: los dos de construcción fuera del runtime, por mutaciones sobrevivientes; el de `.wal.compacting` con solo rechazos y los dos de cancelación, de la revisión de delta.
  - **Adaptados:** los llamadores de `apply_batch` en los tests existentes (`head_seq()` del reporte), los que leían `tables` del estado interno, y `support.rs`, que reexporta el `Rng` de los tests de índices (clippy rechaza cargar `rng.rs` dos veces como módulo).
- **Mutaciones verificadas** (32 propias y 15 del revisor, en copias del repo, una por vez; todas detectadas salvo una equivalente):
  - **En `TableIndexes`:** construir sin las filas; no sacar la entrada vieja; no agregar la nueva; saltear las FK o las `UNIQUE`; indexar `NULL` como un valor; indexar una clave con algún `NULL` sin él; leer como valor la columna que falta; no crear un índice por restricción; armar la clave en el orden de las columnas de la tabla en vez del de la declaración.
  - **En `apply_operation`:** no informar `MissingRow`; no pasar a los índices la fila vieja, la nueva o ninguna; un `Delete` que no saca la fila.
  - **En `RoomData`:** construir sin las filas; crear la sala sin índices; construir el snapshot recibido y el de la apertura sin los índices del esquema.
  - **Reportes:** descartar los rechazos en los dos motores; un `head_seq` equivocado en el reporte de los dos motores y en el del batch vacío del de disco.
  - **Publicación:** no publicar el `RoomData` del batch ni el del snapshot (disco y memoria); publicar las filas nuevas con los índices de antes del batch.
  - **Fuera del runtime:** construir en el runtime los índices de la apertura y los del snapshot recibido, en los dos motores.
  - **Las del revisor:** contar en `applied_count` solo los registros aplicados; que los `Insert`, los `Update` o los `Delete` no toquen los índices; que un rechazo no avance el head (en disco, en memoria y en el replay); conservar el esquema viejo en `apply_snapshot` (en los dos motores); informar el `Delete` de una fila inexistente; listar los rechazos al revés; que un `Update` rechazado igual escriba una fila.
  - **Equivalente:** sacar el atajo `if old_key == new_key { continue; }` de `update`, que borra e inserta la misma entrada. El índice queda igual por `Eq`, aunque no bit a bit (ver -0.0 en las interacciones).
  - **Sobrevivientes, y cómo se cerraron:**
    - *Construir en el runtime los índices de la apertura* (primera pasada). Primero se agregó un punto de pausa en el closure de la apertura, como el de `apply_snapshot.decode`. La revisión mostró que solo detectaba la mutación si la pausa se movía junto con la construcción, y que lo mismo pasaba con el snapshot recibido. Se reemplazó por el punto de hilo `room_data.build` y los tests `indexes_are_built_off_the_runtime` (disco: apertura y snapshot) y `snapshot_indexes_are_built_without_blocking_the_runtime` (memoria).
    - *La clave en el orden de la tabla* (revisión, Bajo 1): el esquema de los tests declaraba las restricciones compuestas en el orden de sus columnas en la tabla. Ahora las declara en otro, y la detectan los tests aleatorios y el unitario.
- **Interacciones revisadas:**
  - **El clon y la publicación del motor de disco:** tablas e índices son un solo valor con campos privados, así que no hay forma de publicar unos sin otros. Ante un pánico en `apply_batch.apply` no se publica nada, la sala queda fallida y la reapertura rehace los índices. Tests: `panic_while_applying_a_batch_publishes_neither_rows_nor_indexes`; mutaciones de publicación.
  - **La compactación:** clona solo `data.tables()` y no escribe `data`; la reapertura posterior construye desde su snapshot. Test: `indexes_follow_the_rows_through_reopens_and_compactions`.
  - **`apply_snapshot`:** los índices se construyen con el esquema que llega con el snapshot, en el mismo closure del pool bloqueante que lo decodifica y lo verifica (el del punto `apply_snapshot.decode`). En el head no cambia nada e ignora el esquema; adelante reemplaza esquema, tablas e índices de una vez. Tests: `snapshots_replace_rows_and_indexes_together` en los dos motores, y en disco la reapertura posterior.
  - **La apertura:** `SchemaMismatch` sale antes de construir (los tests existentes siguen en verde); el orden snapshot → `.wal.compacting` → WAL activo no cambia, y el replay usa `apply_operation`, con las mismas decisiones y el reporte descartado. `applied_count` sigue contando los registros rechazados, que se consumen: un `.wal.compacting` cuyos registros vivos se rechazaron todos igual se pliega, porque el head avanzó. Tests: `compacting_wal_with_only_rejected_records_keeps_the_head` y la simulación `crash_at_any_point_recovers_a_confirmed_state`.
  - **Scans y `get`:** leen `data.tables()`, sin cambios de comportamiento.
  - **INV-S6:** memoria igual a disco incluye índices y reportes: el test de disco compara, después de cada batch y de cada reapertura, reportes, filas e índices con una réplica en memoria.
  - **La memoria:** ver abajo.
  - **wasm32:** `imbl` compila y `run_blocking` corre inline; build y clippy wasm32 en verde.
  - **El batch vacío del motor de disco:** sale antes del WAL con el reporte del head actual. Test: `disk_reports_the_same_and_replays_the_same_decisions`.
  - **`create_snapshot` y el formato:** serializan `data.tables()` como antes; los tests de portabilidad y de formato siguen en verde.
  - **El esquema de una sala abierta no se recarga** (DEF-34): los índices se construyen siempre con el esquema con que se aplica, en la apertura y en `apply_snapshot`, así que nunca tienen otra cantidad de restricciones que el esquema vigente.
  - **El costo por operación:** O(restricciones · log n), inline como la escritura de la fila, acotado por los topes de DR9.
  - **Pánicos:** el código de índices no indexa por posición (usa `get`, `zip` y `get_mut`), así que no agrega una operación veneno. El `debug_assert` de `update` solo existe en builds de debug, y lo dispararía un estado que hoy no se puede alcanzar (índices construidos con otro esquema).
  - **El orden de las columnas de una clave** es el de la declaración, no el de la tabla: el Lote 4 compara la clave de una FK con la PK del padre. Lo fijan el esquema de los tests y `only_keys_without_null_are_indexed`.
  - **Cancelación** de `apply_batch` y `apply_snapshot` con restricciones: un batch soltado deja la sala antes o después del batch, o fallida, con índices que siguen a las filas; un snapshot soltado queda entero o no queda. Tests: los dos `..._dropped_at_any_suspension_point_keeps_the_indexes_on_the_rows`.
  - **El reporte de un batch durable que nadie recibió** (un pánico, un future soltado o un crash después del `fsync`): la reapertura aplica el batch y toma sus decisiones, pero el reporte se perdió, reenviar el batch es `SequenceMismatch` y un sync no lo vuelve a entregar. Se agregó a §7.
  - **La simulación y los crash plans:** el punto de hilo no cuenta para los crash plans (no es un punto de falla), y la simulación sigue cubriendo la recuperación de cada paso, sobre un esquema sin restricciones; extenderla a uno con restricciones es del Lote 4.
  - **El `RoomData` reemplazado se libera en el runtime** (el de un batch o un snapshot aplicado, bajo el lock de la sala; el de un snapshot descartado o una apertura fallida, sin él): liberar un árbol no compartido es O(n), como ya pasaba con las tablas, y ahora cuesta más o menos el doble. Va al backlog (abajo).
  - **El replay del WAL corre en el runtime,** como antes, y ahora también mantiene los índices: O(restricciones · log n) más por registro.
  - **Un snapshot descartado o rechazado** (`SnapshotBehind`, o alcanzado por los writers) tira su `RoomData` ya construido sin publicarlo.
  - **La memoria de un snapshot de otro peer** crece con las restricciones: hasta 32 entradas por fila, además de la fila. Está acotada por DR9 y es lo mismo que en una sala legítima; se agregó a las interacciones del Lote 5 y a §8 de `ARCHITECTURE.md`.
  - **-0.0 y 0.0:** por el atajo de clave igual, un `Update` que cambia una clave entre valores iguales para `Value` deja en el índice los bits viejos. El índice es igual por `Eq` al reconstruido, y las búsquedas del Lote 4 usan `Ord`: la decisión es la misma.
  - **La API pública:** `apply_batch` deja de devolver `SequenceNumber`; no hay llamadores en server ni client, y no hace falta compatibilidad (`GEMINI.md`).
- **Memoria** (§8): medida con un binario aparte y un allocator que cuenta los bytes vivos, sobre una sala de memoria con 100.000 filas de cuatro columnas (entero, string de 23 bytes, entero, entero) insertadas en orden de clave. La fila sola cuesta 264 B. Cada índice suma por fila unos 167 B con una clave entera (FK), 213 B con el string (`UNIQUE`) y 221 B con una clave de dos enteros (`UNIQUE` compuesta), y se suman: con los tres, 601 B más por fila. Cada entrada guarda dos `PrimaryKey` (80 bytes en línea) más lo que la clave tenga en el heap; el resto es la parte del árbol.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura, que probó en su propia copia): sin hallazgos Altos ni Medios. El lote implementa DR5 y DR6 dentro de su alcance y nada más; no rompe ninguna invariante (S1, S2, S3, S5, S6, C3, Y8, W1); el checklist de §5.4 se cumple punto por punto. Se atendieron:
  - **Bajo 1** (la clave en el orden de la tabla sobrevivía a los tests) y **Bajo 2** (que la construcción no corriera en el runtime estaba protegido solo por la estructura del código): ver mutaciones.
  - **N2:** `apply_operation` creaba la tabla antes de decidir. Ahora decide primero, y un `Delete` sin fila no la crea.
  - **N3:** un `debug_assert` en `TableIndexes::update` si los índices no tienen uno por restricción del esquema.
  - **N1, N4, N5, N7 y N9:** en las interacciones, el backlog, el Lote 5 y los desvíos. **N6:** en §7. **N8:** las cifras de memoria de `ARCHITECTURE.md` y la cita de los tests de `applied_count`.
  - **Interacciones que faltaban:** el reporte perdido, la cancelación, la simulación, la liberación en el runtime, el replay, los snapshots descartados, la memoria de un peer, el orden de las columnas y la tabla creada antes de decidir. Están arriba, y se incorporaron sus tres tests de disco (el de `.wal.compacting` con solo rechazos y los dos de cancelación).
- **Chequeos** (después de atender la revisión): fmt, clippy, test (880 tests) y doc de §5.1, y el build y clippy wasm32, en verde; los dos jobs MSRV (`cargo +1.88.0 test --workspace` y `cargo +1.85.0 build --workspace --lib --bins`, con un `CARGO_TARGET_DIR` aparte); y `cargo test -p zemdb-storage --release -- --ignored` (13, con la versión larga nueva).
- **Desvíos:**
  - **Tablas e índices en un solo valor (`RoomData`)** en lugar de dos campos del estado: la publicación conjunta queda garantizada por el tipo.
  - **El multimapa es un `OrdSet` de pares `(clave, PK)`**: una entrada por fila y por restricción, sin un conjunto por clave.
  - **Punto de hilo nuevo** en `fail_point.rs` (`thread_point`, que solo en los tests registra qué hilo lo alcanzó) en `RoomData::build`, para los tests de construcción fuera del runtime. Reemplazó a un punto de pausa en la apertura (ver mutaciones). Por eso `RoomData::empty` ya no pasa por `build`: corre en el runtime y no construye nada.
  - **Estructura de los tests:** el módulo de tests de `memory/index.rs` es `pub(crate)`, y en él viven los helpers y escenarios que usan los tests de los dos motores; `disk/tests/support.rs` reexporta su `Rng` (clippy rechaza cargar `rng.rs` dos veces).
  - **`ARCHITECTURE.md`:** además de §8 (el contrato y una sección nueva de índices derivados, y los ajustes de *apply_snapshot Rules*, *WAL Mutex* y la apertura), una frase en §3.1 (*Enforcement*): los índices ya existen, pero todavía no se rechaza nada.
  - **`INVARIANTES.md`:** INV-S1, S3 y S6 incluyen los índices (y S6, los reportes) en su enunciado o su "Sostenida por"; INV-C3 pasa a "Lote 4; los índices, Lote 3". Siguen ✅ las tres primeras y 🆕 la última.
  - **Tamaño:** unas 275 líneas de producción agregadas sin comentarios ni blancos (89 quitadas), unas 20 de ellas de infraestructura de tests en `fail_point.rs`; dentro del tope de §5.1.
- **Fuera del lote (backlog):**
  - **Liberar el estado reemplazado fuera del runtime.** Un batch o un snapshot sueltan el `RoomData` que reemplazan en el runtime, bajo el lock de la sala, y una apertura fallida o un snapshot descartado sueltan el suyo en el runtime: O(n) si el árbol no está compartido. Es preexistente para las tablas (los índices lo duplican, más o menos) y Bajo: se podría mandar el valor viejo al pool bloqueante.

### Lote 4 · Verificación en las réplicas (`zemdb-storage`) · ✅ Hecho
**Objetivo:** las reglas de §3.1.

**Archivos:** `memory/state.rs` (la verificación antes de aplicar), `memory/index.rs`, el rustdoc de `engine.rs`, y tests:
- un `crates/storage/tests/constraint_tests.rs` nuevo;
- el oráculo en `crates/storage/tests/common/`;
- `replica_convergence_tests.rs`;
- `disk/tests/simulation.rs`.

**Qué se hace:**
- Primero, el oráculo (§5.3) y los tests de reglas, que tienen que fallar.
- Después, la verificación:
  - se calcula la decisión antes de tocar tablas o índices;
  - en caso de rechazo, se informa el motivo y avanza `head_seq`;
  - el replay usa la misma función y descarta el reporte.

**Tests exigidos:**
- **Reglas:** uno por fila y por nota de §3.1. Entre ellos:
  - `UNIQUE` con un `Insert` duplicado, con un upsert de la misma fila, con un `Update` que choca y con claves con `NULL`;
  - clave compuesta con un solo `NULL`;
  - fila corta;
  - FK sin padre, con `NULL`, y apuntando a sí misma;
  - `RESTRICT` con hijos, y con la propia fila como único hijo;
  - padre e hijo en el mismo batch, en los dos órdenes;
  - el ejemplo concurrente de §1.2, en los dos órdenes;
  - `Duplicate` en una tabla inmutable;
  - `NotOwner`: B se declara dueño y hace `Insert` sobre la clave de una fila de A (dueño fuera de la clave), `Update` o `Delete` sobre ella; A sobre su propia fila sí puede;
  - el orden del motivo cuando fallan dos restricciones.
- **INV-C3:** el estado con una operación rechazada es igual al estado sin esa operación.
- **Oráculo diferencial** y su versión larga.
- **Convergencia:** con restricciones, entre motores, con snapshot en un punto aleatorio y con reapertura después de una compactación. Tanto los reportes del resto como el estado son iguales.
- **Crash:** un crash en cada paso, con reapertura, coincide con el oráculo.
- **Sin pánicos:** operaciones aleatorias, incluidas las que violan restricciones, nunca hacen entrar en pánico al verificador.
- **Generadores con dueño e inmutabilidad** (pendiente del Lote 2): el oráculo, los motores aleatorios y la convergencia producen tablas con dueño (dentro y fuera de la clave) e inmutables, y `Delete` con dueño.

**Mutación inversa:** sacar, una por vez, `UNIQUE`, FK, `RESTRICT`, `Duplicate`, `NotOwner`, el salto por `NULL` y la excepción de la auto-referencia. En cada caso, algún test falla.

**Interacciones a revisar:**
- **El WAL ya está escrito cuando se decide.** El replay vuelve a decidir con la misma función y, por DR3, con un esquema que da lo mismo.
- **Un pánico del verificador después del WAL es una operación veneno:** la sala queda fallida y el replay lo repite.
- **INV-S1:** un rechazo no es un error, y el batch sigue siendo atómico.
- **`Update` sobre una fila corta:** la fila se ensancha antes de calcular la clave.
- **Ciclos entre tablas** (`A → B` y `B → A`): el orden de inserción decide, y es determinista.
- **`validate_batch`** (hueco y esquema) sigue antes que todo.
- **El checklist de §5.4.**

**Documentos:**
- `ARCHITECTURE.md`:
  - §4: la semántica de cada operación con restricciones;
  - §8: la verificación en `apply_batch`;
  - §3.1: *Soft Foreign Keys* desaparece.
- `INVARIANTES.md`: INV-C2 e INV-C3 pierden el lote pendiente en su estado. Siguen 🆕 hasta la revisión de cierre.

**Hecho.**
- **API.**
  - **`RejectReason`** (`engine.rs`) suma `Duplicate`, `NotOwner`, y `Unique`, `ForeignKey` y `Restrict` con el nombre de la restricción (`{ constraint: String }`); los nombres de DR5 quedaron como estaban. El rustdoc de `apply_batch` describe las reglas y el orden del motivo.
  - **`apply_operation`** (`memory/state.rs`) decide antes de tocar tablas o índices, en el orden de §3.1: `MissingRow` (y el `Delete` de una fila inexistente, que sale sin informar), después `check_existing_row` (`Duplicate`, `NotOwner`), y sobre la fila que deja la operación, `check_row` (las `UNIQUE` y después las FK de la tabla, en orden de declaración; un `Update` solo las que tienen alguna columna entre sus deltas) o, para un `Delete`, `check_references` (`Restrict`, por `Schema::foreign_keys_referencing`: id de la tabla hija y orden de declaración). Un rechazo sale antes de la actualización de índices y tabla. El replay usa la misma función y descarta el resultado, como en el Lote 3.
  - **Búsquedas** (`memory/index.rs`): `TableIndexes::has_other_row` (una `UNIQUE`, salteando la propia fila), `is_referenced` (una FK, salteando la propia fila si la tabla se referencia a sí misma) y `rows_with_key`, un rango del `OrdSet` desde `(clave, PK vacía)`, que ninguna PK precede. La FK busca su clave entre las PK de la tabla referida (`contains_key`). Todo por `Ord`/`Eq` de `Value`.
- **Tests agregados** (32, dos de ellos `#[ignore]`, más la reescritura de los helpers del Lote 3):
  - **El oráculo** (`tests/common/oracle.rs`): un modelo de referencia que solo usa tipos de `zemdb-core`, guarda las filas en `BTreeMap` y verifica cada regla recorriendo tablas enteras, con las columnas buscadas por nombre y el dueño declarado y las tablas referidas calculados por su cuenta. Trae el generador (dominios de 4 valores, enteros desde -1, `NULL`, filas cortas, auto-referencias, -0.0 y dos NaN, dos dueños, solo `Insert` en las tablas inmutables, `Delete` con dueño) y dos esquemas: siete tablas con cada clase de regla (`UNIQUE` simple y compuesta con un float, FK simple, compuesta y a la propia tabla con las columnas en otro orden, un ciclo entre dos tablas con una PK float, una tabla inmutable con el dueño en la clave y otra con el dueño fuera, una mutable con el dueño fuera, y una cuya PK es también FK hacia otra tabla), y su evolución con una `UNIQUE` y una FK sobre columnas agregadas. Lo cargan por `#[path]` los tests de integración y los unitarios (`memory/tests/index.rs`, como `rng.rs`), con el pegamento a `RejectReason` en cada lado.
  - **`tests/constraint_tests.rs`** (25 y uno `#[ignore]`), con un helper que aplica los batches en los dos motores, exige el mismo reporte y las mismas filas, y vuelve a exigirlas al reabrir el disco (el replay): `UNIQUE` duplicada, la propia fila por upsert y por `Update`, el `Update` que choca, claves con `NULL` (también compuestas con un solo `NULL`), la compuesta con -0.0/0.0 y NaN de iguales y distintos bits, filas cortas ensanchadas antes de verificar (con la `UNIQUE` de una columna agregada); FK sin padre (insert y update), con `NULL` (y compuesta con un solo `NULL`), auto-referencia (insert y update, y el orden de las columnas compuestas), claves float por valor; `RESTRICT` con hijos, con la propia fila como único hijo, por valor y un upsert del padre que se aplica; búsquedas con PK negativas; una PK que también es FK hacia otra tabla (no es una auto-referencia); padre e hijo en el mismo batch en los dos órdenes; el ejemplo de §1.2 en los dos órdenes; el ciclo entre tablas; `Duplicate`; `NotOwner` (insert, update, delete y la transferencia por `Update`; el dueño hace las tres; con el dueño en la clave no hay choque); el orden del motivo en 14 casos (`MissingRow`/`Duplicate`/`NotOwner` entre sí y contra `UNIQUE`, FK y `RESTRICT`, `UNIQUE` antes de FK aunque la FK se declare antes, cada clase en orden de declaración, `RESTRICT` por id de la tabla hija y por declaración dentro de ella); INV-C3 (sacar una operación rechazada por una restricción y renumerar da las mismas filas y los mismos rechazos posteriores, en los dos motores); el diferencial contra el oráculo (12 logs con el cambio de esquema en un punto aleatorio, compactaciones y reaperturas; la versión larga, 400); y que los logs aleatorios rompan cada regla por cada restricción del esquema evolucionado.
  - **`src/memory/tests/state.rs`** (3): las columnas que toca un `Update`, sobre un estado que ya viola (construido sin verificar); sin pánicos y un rechazo sin efectos sobre tablas e índices, en 40 estados aleatorios que violan y 12.000 operaciones; sin pánicos con operaciones que el esquema rechaza (claves de otro ancho o tipo, filas de cualquier ancho, deltas fuera de rango, dueños de otro tipo, una tabla que no existe), porque el replay no revalida.
  - **Convergencia** (`replica_convergence_tests.rs`): `replicas_converge` se generalizó a un esquema y un generador y compara también los reportes (completo, del disco con compactación, y del resto después de un snapshot en cada combinación de motores) y el modelo; caso nuevo con el esquema del oráculo (4 logs; la versión larga, 100).
  - **Simulación** (`disk/tests/simulation.rs`): el esquema suma las siete tablas del oráculo y la mitad de las operaciones son del generador; el estado esperado de cada candidato lo da el oráculo, y cada snapshot lleva el estado del modelo que contiene. Corren con restricciones el crash en cada punto, la falla en cada punto, los lectores, el WAL dañado y las operaciones soltadas.
  - **Generadores con dueño e inmutabilidad** (pendiente del Lote 2): el del oráculo (y por él los motores aleatorios de los unitarios y la convergencia) y el de `randomized_engine_tests.rs` (dueño en la clave, fuera de ella o ninguno, tablas inmutables, `Update` con el delta del dueño y `Delete` con dueño), con `random_schemas_have_owners_and_immutable_tables`.
  - **Adaptados:** `RandomLog` (`memory/tests/index.rs`) usa el generador y el modelo del oráculo para el reporte esperado, en vez de predecir solo `MissingRow`; con eso, los tests de índices de los dos motores del Lote 3 pasan a ejercitar rechazos de todas las clases. Se quitaron sus generadores propios.
  - **Contra el esqueleto** (las variantes nuevas y las búsquedas, sin usarlas): fallaban por aserción 37: 18 de 23 en `constraint_tests`, 1 de convergencia, y 18 de la lib (2 unitarios nuevos, las 5 simulaciones y 11 tests de índices del Lote 3 con el reporte del oráculo). Pasaban 6, positivos o de cobertura: `a_row_keeps_its_own_unique_key`, `an_upsert_of_a_referenced_row_is_applied`, los dos de `NULL` y los dos de cobertura de los generadores. INV-C3 y el de pánicos pasaban al principio (solo veían `MissingRow`) y se reescribieron para exigir rechazos por restricciones. Después se agregaron `lookups_find_rows_with_any_primary_key` (mutación sobreviviente) y el de operaciones inválidas, que el esqueleto también cumple.
- **Mutaciones verificadas** (26, en una copia del repo, una por vez y restauradas; todas detectadas salvo una equivalente): `UNIQUE` que nunca choca y que cuenta la propia fila; FK que nunca falta, sin la excepción de auto-referencia, que busca en su propia tabla y que verifica claves con `NULL`; `RESTRICT` que nunca encuentra hijos, que cuenta la propia fila y que recorre las tablas hijas al revés; sin `Duplicate`; sin `NotOwner`; el orden del motivo (`NotOwner` antes de `Duplicate`, las restricciones antes que la fila existente, FK antes que `UNIQUE`, `UNIQUE` y FK en orden inverso de declaración); un `Update` que verifica todas las restricciones; las claves del `Update` calculadas sobre la fila sin los deltas (el ensanchado: la fila corta lee `NULL`); sin ensanchar al ancho del esquema; un rechazo que igual actualiza los índices; un rechazo informado como aplicado; el `Delete` de una fila inexistente informado; y el rango de búsqueda que empieza en `(clave, 0)`.
  - **Equivalente:** que `UNIQUE` verifique también claves con `NULL`. El índice no guarda claves con `NULL`, así que una clave así nunca encuentra otra fila; el salto es una optimización.
  - **Sobreviviente, y cómo se cerró:** el rango desde `(clave, 0)` saltea las filas cuya PK es menor (negativa, o de un tipo anterior). Ningún test usaba PK menores que 0. Se agregó `lookups_find_rows_with_any_primary_key` y el dominio entero del generador pasó a empezar en -1.
  - **De la revisión:** la mutación de `RESTRICT` que nunca encuentra hijos no compilaba en la primera pasada; corregida, la detectan 16 tests. Dos más del revisor sobrevivían (ver revisión): la excepción de auto-referencia aplicada a una FK hacia otra tabla, y `RESTRICT` salteando la propia fila en otra tabla. Se cerraron con la tabla `profiles` del oráculo y `a_key_that_is_also_a_foreign_key_to_another_table_needs_its_parent`; total, 26 mutaciones.
- **Interacciones revisadas:**
  - **El WAL ya está escrito cuando se decide:** el replay (`disk/recovery.rs`) llama a la misma `apply_operation` y descarta el resultado; con un esquema posterior decide igual por DR3. Tests: el helper de `constraint_tests` (reapertura después de cada caso), el diferencial (reaperturas y el cambio de esquema a mitad del log en el disco) y la convergencia.
  - **Operación veneno:** el replay no corre `validate_operation`, así que el verificador no debe entrar en pánico ni con operaciones inválidas. Lee filas y restricciones con `get`, `zip` y búsquedas que devuelven `Option`, y escribe un delta después de ensanchar la fila. Tests: los dos de pánicos de `state.rs`.
  - **INV-S1:** un rechazo no es un error; el batch se escribe y se aplica entero. El helper exige `Ok` y el head en cada batch.
  - **`Update` sobre una fila corta:** se ensancha antes de calcular las claves. Test: `short_rows_read_null_and_are_widened_before_the_check` y su mutación.
  - **Ciclos entre tablas:** decide el orden. Test: `a_cycle_between_tables_is_decided_by_order`.
  - **`validate_batch`** (hueco y esquema) sigue antes que todo: los tests de batches rechazados del Lote 3 corren ahora con rechazos de todas las clases.
  - **Snapshots sin verificar hasta el Lote 5:** se puede instalar un estado que viola; las decisiones siguen siendo deterministas (`an_update_checks_only_the_constraints_of_the_columns_it_sets` decide sobre uno).
  - **Motor de disco:** decide sobre el clon del `RoomData` y lo publica igual; un rechazo deja el clon intacto. **Motor de memoria:** decide y muta en el lugar, bajo el lock de la sala; un rechazo no muta porque decide antes. Test: `the_verifier_never_panics_and_a_rejection_changes_nothing` compara el `RoomData` antes y después de cada rechazo.
  - **Costo:** O(restricciones · log n) por operación, inline como la escritura de la fila; un `Vec` con las columnas de los deltas por `Update` y un `String` por rechazo, acotados por el batch.
  - **El esquema de una sala abierta no se recarga** (DEF-34); `apply_snapshot` reemplaza el esquema, y desde ahí se decide con el nuevo.
  - **-0.0 y 0.0 en los índices** (los bits viejos tras el atajo de clave igual del Lote 3): las búsquedas usan `Ord`. Tests: `composite_unique_keys_compare_every_value_as_a_value`, `restrict_compares_keys_as_values`.
  - **`check_row` sin el índice de la tabla:** todas las tablas del esquema tienen sus índices (`RoomData::empty` y `build`). Si faltara uno, las `UNIQUE` no se verificarían; las FK no dependen de él (revisión, Bajo 3).
  - **`applied_count` del replay** cuenta las operaciones consumidas, rechazadas incluidas: el head avanza y un `.wal.compacting` se pliega igual (test del Lote 3).
  - **`NotOwner` se compara en toda tabla con dueño,** no solo con el dueño fuera de la clave; es equivalente (ver Lote 5, interacciones). La mutación a la forma literal sobrevive, como corresponde a una equivalente.
  - **Restricciones entre dueños distintos:** un miembro puede ocupar una clave `UNIQUE` o impedir el borrado de una fila ajena referenciándola. Se agregó a §7 y a §3.1 de `ARCHITECTURE.md`.
  - **El reporte perdido ante un crash** y el squashing ya estaban en §7 y en §4.1 de `ARCHITECTURE.md`.
  - **`RejectReason`** deja de ser trivial (lleva un `String`); no tiene llamadores en server ni client.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura, que probó en su propia copia y corrió en release las tres versiones largas nuevas): sin errores de decisión. El lote implementa §3.1 y nada más; no rompe ninguna invariante (S1, S6, Y8, C3, W1, A5; C2 espera al Lote 5 para los snapshots); el checklist de §5.4 se cumple punto por punto; el oráculo no comparte código con la implementación. Se atendieron:
  - **Medio 1:** las mutaciones M2 y M3 sobrevivían, porque ninguna FK del oráculo hacia otra tabla podía valer lo mismo que la PK de su fila. Se agregó `profiles (id)` con `id → parents` al esquema del oráculo (y con eso a los tests aleatorios) y un test de reglas; las dos se detectan.
  - **Bajo 2:** la mutación de `RESTRICT` que no compilaba (arriba).
  - **Bajo 3:** `check_row` ya no sale si falta el índice de la tabla; las FK no lo usan.
  - **Notas:** la interacción entre dueños (§7 y `ARCHITECTURE.md`); las interacciones del motor de memoria, `applied_count` y el `.snap` local (arriba y en el Lote 5); el "Automatizable" de INV-S3, que nombraba un `MemoryStorageEngine` como oráculo; y la frase de §8 de `ARCHITECTURE.md` sobre no indexar por posición. Se dejan: `reject_reason` duplicado entre `tests/common/mod.rs` y `memory/tests/index.rs` (el oráculo no puede conocer `RejectReason`, y cada nivel de tests tiene su pegamento) y la carga del oráculo por `#[path]` en los unitarios, como `rng.rs`.
- **Chequeos** (después de atender la revisión): fmt, clippy, test y doc de §5.1, y el build y clippy wasm32, en verde; los dos jobs MSRV (`cargo +1.88.0 test --workspace` y `cargo +1.85.0 build --workspace --lib --bins`, con un `CARGO_TARGET_DIR` aparte); y `cargo test -p zemdb-storage --release -- --ignored`.
- **Desvíos:**
  - ***Soft Foreign Keys*** ya no existía en `ARCHITECTURE.md` §3.1 (el Lote 1 la reemplazó por *Integrity Constraints*): se reescribió *Enforcement* y *Who verifies what*.
  - **`NotOwner` en toda tabla con dueño** (equivalente; ver interacciones).
  - **La simulación del disco** compara contra el oráculo, no contra un motor de memoria, y su esquema suma las tablas del oráculo; cada snapshot de la simulación lleva el estado del modelo.
  - **`RandomLog` y los escenarios del Lote 3** usan el oráculo por `#[path]` (generador y reporte esperado).
  - **El esquema del oráculo** cambió `UNIQUE (score, code)` por `UNIQUE (rank, score)`: la primera nunca podía ser el motivo, porque una clave repetida repite `code`.
  - **`ARCHITECTURE.md`:** además de §3.1, §4 y §8, la frase sobre los índices en *Derived Constraint Indexes*. **`INVARIANTES.md`:** INV-A5 y C3 quedan 🆕 sin lote pendiente, C2 en "Lote 5", Y8 sin lote; los "Sostenida por" de Y8, A5, C2 y C3, y el "Automatizable" de INV-S3.
  - **La simulación del disco** movió `users` e `items` a los ids 8 y 9 para hacer lugar a las tablas del oráculo, y el batch inválido de la simulación, que borraba de la tabla 9 como tabla inexistente, pasó a ser válido. Solo lo sortean las versiones largas, que fallaron en la primera corrida ("unexpected Ok" para el batch inválido); ahora usa una constante `UNKNOWN_TABLE` (99), y las 15 versiones largas pasan.
  - **Tamaño:** unas 140 líneas de producción agregadas sin comentarios ni blancos (9 quitadas), por debajo de las ~230 estimadas.
- **Fuera del lote:** ninguno nuevo para el backlog; la interacción entre dueños quedó como requisito de la Fase 4 (§7).

### Lote 5 · Snapshots, apertura y evolución (`zemdb-storage`) · ✅ Hecho
**Objetivo:** DR7 e INV-C1.

**Archivos:**
- `memory/state.rs` (`decode_received_snapshot`, que verifica al construir los índices);
- `disk/recovery.rs` (la verificación del snapshot local);
- tests en `apply_snapshot_tests.rs` y `randomized_engine_tests.rs`.

**Qué se hace:**
- `apply_snapshot` verifica `UNIQUE`, FK e inmutabilidad (sin claves repetidas) del snapshot recibido, en el pool bloqueante.
- `open_room` verifica el snapshot local al construir los índices.

**Tests exigidos:**
- **`apply_snapshot` inválido.** Un snapshot con una clave `UNIQUE` repetida, o con una FK colgante, da `SnapshotCorruption` y deja la sala sin cambios, en los dos motores.
- **`apply_snapshot` válido.** Un snapshot con claves `NULL`, o con una fila que se apunta a sí misma, se instala.
- **Apertura de un `.snap` local que viola una restricción** (fabricado en disco con un CRC válido): falla, y los archivos quedan byte a byte iguales.
- **Evolución, aleatorio:**
  - **El montaje:** un log con columnas agregadas, con restricciones, en puntos aleatorios.
  - **Las dos réplicas:** una aplica cada operación con la versión que conocía al recibirla; la otra aplica todo con la última.
  - **El resultado esperado:** mismo estado y mismos reportes. Lo mismo al reabrir con la última versión un WAL y un snapshot escritos con versiones viejas (INV-C1).
- **Fuzzers de snapshots** con un esquema con restricciones: nunca un pánico.

**Interacciones a revisar:**
- **`NotOwner` con el dueño en la clave** (Lote 4): se compara siempre, y es equivalente a compararlo solo fuera de la clave porque una fila guarda en sus columnas de clave su propia clave. Lo garantiza `validate_keyed_row` en las operaciones y en los snapshots recibidos; el `.snap` local no lo verifica al abrir (lo escribió el motor).
- **El oráculo no lee snapshots:** la simulación lleva el estado del modelo junto con cada snapshot. Los tests de este lote que necesiten un snapshot que viola tienen que fabricarlo sin el oráculo.
- **Un snapshot de otro peer escrito con una versión vieja,** con filas cortas, se verifica bajo la nueva y pasa (INV-C1).
- **La verificación de la apertura sale antes de truncar, absorber o reescribir nada,** como `SchemaMismatch` (INV-S4).
- **El relay:** un snapshot hostil que viola restricciones lo rechazan los clientes honestos. Esto mejora DEF-81, pero no lo cierra.
- **El costo:** O(N log N) por restricción en el pool bloqueante, e inline en wasm32.
- **La memoria:** los índices de un snapshot de otro peer suman, por fila, una entrada por restricción de su tabla (hasta 32, de 170 a 220 bytes cada una según lo medido en el Lote 3), además de la fila. Está acotada por DR9 y es lo mismo que en una sala legítima, pero conviene decirlo en INV-S5 e INV-W1.

**Documentos:**
- `ARCHITECTURE.md` §8, en las reglas de `apply_snapshot` y en la apertura.
- `INVARIANTES.md`:
  - "Sostenida por" de INV-S4 e INV-S5;
  - INV-C1 pierde el lote pendiente en su estado, y sigue 🆕 hasta la revisión de cierre.

**Hecho.**
- **API.**
  - **`RoomData::build_checked`** (`memory/state.rs`): construye los índices (`RoomData::build`) y enseguida verifica las filas con `RoomData::check` (privada). Una violación es `SnapshotCorruption`, con el nombre de la tabla y el motivo (`Unique` o `ForeignKey` con la restricción).
  - **`check`** verifica cada fila con `check_row(.., None)`, es decir con las reglas de un `Insert` de esa fila sobre las filas del snapshot. Así hereda el salto por `NULL`, la lectura como `NULL` de las columnas que faltan en una fila corta, la igualdad de `Value` y la auto-referencia. Recorre las tablas en orden de id (`Schema::tables`, un `BTreeMap`) y las filas en orden de clave (`OrdMap`), así que la restricción nombrada es siempre la misma. Alcanza el punto de hilo `room_data.build`, como la construcción.
  - **Dónde corre:** `decode_received_snapshot` (los dos motores, dentro del closure de `run_blocking`, inline en wasm32) y `load_snapshot` (la apertura del disco, en el pool bloqueante), en lugar de `RoomData::build`. En la apertura sale en el mismo punto que `SchemaMismatch`: antes de limpiar temporales, reaplicar, truncar, absorber o reescribir nada.
  - **Contrato:** el rustdoc de `open_room` y `apply_snapshot` (`engine.rs`) lo describe.
  - **Inmutabilidad:** no hay nada que verificar. Que una fila de una tabla inmutable no se reemplazó es una propiedad de la historia, no del estado, y un snapshot guarda una fila por clave primaria. Lo que sí puede repetir una clave es el payload bincode: el decoder se queda con la última, en forma determinista. Eso no se cambió en el lote (ver desvíos y backlog).
- **Tests agregados** (14, cuatro de ellos `#[ignore]`):
  - **Unitarios, en los dos motores** (escenarios en `memory/tests/index.rs`, corridos desde `src/tests/memory.rs` y `src/tests/disk.rs`):
    - `a_snapshot_that_breaks_a_constraint_changes_nothing`: snapshots adelante que repiten una clave `UNIQUE` (de una columna de la tabla y de una agregada después) o tienen una FK colgante (a otra tabla y a la propia, compuesta) dan `SnapshotCorruption` que nombra esa restricción y ninguna otra. Dejan head, esquema, filas e índices como estaban; en el disco, también al reabrir. Dos casos fijan el motivo cuando hay varias violaciones: la primera tabla por id y, dentro de ella, la primera fila por clave.
    - `a_snapshot_that_meets_the_constraints_is_installed`: se instalan claves con `NULL` (repetidas, sin padre y compuestas con un solo `NULL`), una fila que se apunta a sí misma por una FK compuesta declarada en otro orden, y filas cortas escritas con `constrained_schema` e instaladas con `evolved_schema`, cuya `UNIQUE (tag)` lee `NULL`. En el disco, también al reabrir.
    - Los snapshots que violan se fabrican con un motor abierto con las mismas tablas sin restricciones; ni el oráculo ni la implementación intervienen.
  - **Apertura** (`disk_wal_resilience_tests.rs`, 2): un `.snap` con una `UNIQUE` repetida y otro con una FK colgante, escritos por el motor sin restricciones (CRC válido), con un `.wal.compacting` y un WAL activo con un torn write al final. Abrir con las restricciones falla nombrando la restricción, los tres archivos quedan byte a byte iguales, y la sala abre después con el esquema original, con todos sus datos.
  - **Evolución** (`schema_evolution_tests.rs`, nuevo; 1 y uno `#[ignore]` con 300 logs):
    - Cada log agrega de 1 a 4 columnas en puntos aleatorios de su primera mitad, con `oracle::add_random_column`: ninguna restricción, `UNIQUE` de la columna sola o con otra, una FK a una tabla de clave entera, o una FK compuesta a `children`.
    - Las réplicas que conocen cada versión desde el primer batch que la necesita: una de memoria, que pasa a la versión nueva con un snapshot de sí misma tomado con la vieja; una de disco, que reabre con la nueva; y el modelo.
    - Las réplicas que aplican todo con la última versión: una de memoria y una de disco, que compacta y reabre al azar.
    - Una réplica tardía instala, con la última versión, un snapshot de la réplica de disco tomado con una versión vieja, y aplica el resto.
    - Todas dan los mismos reportes en cada batch y terminan con las mismas filas, también las de disco reabiertas con la última versión sobre WAL y snapshots escritos con versiones viejas.
    - Exige rechazos por `Unique` y `ForeignKey` de las restricciones agregadas; la versión larga, también por `Restrict`.
  - **Fuzzers** (`randomized_engine_tests.rs`; 3 y tres `#[ignore]`):
    - los de snapshots dañados y de archivos de sala dañados, parametrizados por esquema, con `constrained_sample_schema` (una `UNIQUE`, una FK y una `UNIQUE` sobre una columna agregada). Un estado aceptado o abierto tiene que cumplir las restricciones, según un chequeo ingenuo propio del test (`broken_constraints`, que recorre tablas enteras);
    - uno diferencial nuevo, `unchecked_states_are_installed_only_if_they_meet_the_constraints`: estados aleatorios escritos sin restricciones sobre dominios chicos, aplicados con ellas a los dos motores, se instalan si y solo si `broken_constraints` no encuentra nada; si no, son `SnapshotCorruption` que nombra una restricción rota y el head no se mueve.
  - **Contra el esqueleto** (`build_checked` y `check` existían y se llamaban, pero `check` no verificaba nada): fallaban por aserción 6. Eran los dos de snapshots que violan (memoria y disco), los dos de apertura, el diferencial y el fuzzer de snapshots dañados con restricciones; este último no siempre (ver backlog). Pasaban 4:
    - los dos de snapshots válidos, que son positivos y detectan las mutaciones del salto por `NULL` y de la auto-referencia;
    - el de evolución, que es una propiedad que DR3 ya sostenía y que este lote no debía romper; detecta, por ejemplo, que una columna que falta lea un valor (mutación del revisor);
    - el fuzzer de archivos dañados con restricciones, cuyo objetivo es "nunca un pánico".

    Los dos casos del motivo con varias violaciones y la aserción de restricciones del fuzzer de archivos se agregaron por la revisión.
  - **Generador:** `oracle::add_random_column` (solo tipos de `zemdb-core`) excluye columnas de la clave en la FK compuesta. Si las incluyera, la auto-referencia del generador pisaría la clave, y la versión larga lo encontró (`PrimaryKeyMismatch`). Cada operación generada se valida con su versión, como en `random_log`.
- **Mutaciones verificadas** (14 propias, en una copia del repo, una por vez y restauradas; todas detectadas, ninguna dejó de compilar):
  - **M1 y M2:** sin verificación en el snapshot recibido (4 tests) y en el local (los 2 de apertura).
  - **M3 y M4:** `check` ignora `UNIQUE` y las FK. Cada una la detectan 4 tests, en los dos caminos: los escenarios de los dos motores, el diferencial y el test de apertura correspondiente.
  - **M5a y M5b:** la verificación no saltea `NULL` en las FK (16 tests) ni en las `UNIQUE` (14).
  - **M6a:** sacar la excepción de auto-referencia de `check_row`, compartida con `apply_operation`. Solo la detectan tests del Lote 4 (11): en un snapshot es equivalente, porque la fila ya está en su tabla y la búsqueda del padre la encuentra.
  - **M6b:** la variante realista para la verificación, que rechaza una fila que se apunta a sí misma: 6 tests, entre ellos los de snapshots válidos.
  - **M7:** la verificación de la apertura al final de `recover_room`, después de truncar y plegar: los 2 de apertura y el del punto de hilo.
  - **M8a, M8b y M8c:** la verificación en el runtime, en la apertura y en el snapshot recibido de cada motor: `indexes_are_built_off_the_runtime` y `snapshot_indexes_are_built_without_blocking_the_runtime`.
  - **Sobrevivientes, y cómo se cerraron:** M9 y M10, recorrer las tablas o las filas en orden inverso, sobrevivían porque cada caso violaba una sola restricción (revisión, Bajo 2). Se agregaron los dos casos con varias violaciones, y los escenarios exigen que el mensaje nombre solo la restricción esperada.
  - **Las del revisor** (16, en su copia): coinciden con las de arriba. Además, `check` solo en la primera tabla, `check` que rechaza filas cortas, y una columna que falta que lee `Int(0)`, que el test de evolución detecta por sí solo. Una más es equivalente: verificar después del replay y antes de truncar, que solo detecta el punto de hilo, porque el replay no escribe archivos.
- **Interacciones revisadas:**
  - **`NotOwner` con el dueño en la clave:** sin cambios. `validate_keyed_row` sigue corriendo sobre los snapshots recibidos, y el `.snap` local no se valida fila por fila (lo escribió el motor). El lote no verifica dueños en snapshots: un dueño fuera de la clave puede ser cualquier valor.
  - **El oráculo no lee snapshots:** los que violan se fabrican con motores sin restricciones, y los fuzzers usan su propio chequeo ingenuo. El oráculo solo suma `add_random_column`.
  - **Un snapshot de otro peer escrito con una versión vieja** pasa bajo la nueva: el caso de filas cortas, la réplica de memoria y la tardía del test de evolución.
  - **La apertura** sale antes de tocar archivos: los tests byte a byte y M7.
  - **La compactación y el pliegue** escriben snapshots que la apertura siguiente verifica (revisión, 3.a). Pasan porque son estados que `apply_operation` produjo desde una base verificada, bajo el mismo esquema o una versión posterior (DR3). Tests: `indexes_follow_the_rows_through_reopens_and_compactions`, las reaperturas del test de evolución y la simulación. Un corolario: una sala de disco que recibió `apply_snapshot` con un esquema de otras restricciones puede no abrir después con el registrado, y elegir el esquema sigue a cargo del llamador.
  - **El WAL no se verifica:** el replay decide cada operación con el esquema con que se abre, como en el Lote 4.
  - **El relay:** los clientes honestos rechazan un snapshot hostil que viola las restricciones. Mejora DEF-81 sin cerrarlo, porque un snapshot falso que las cumple se sigue instalando. Se agregó a §6 de `ARCHITECTURE.md` (revisión, Bajo 4).
  - **Snapshots en el head y atrasados** (revisión, 3.b): el snapshot se decodifica y verifica antes de compararlo con el head. Uno que viola es `SnapshotCorruption` en lugar del no-op del head o de `SnapshotBehind`, como ya pasaba con una fila inválida. No tiene llamadores hoy.
  - **El error en la apertura** (revisión, Bajo 3): es `SnapshotCorruption`, la misma variante que un CRC roto, aunque es un problema de esquema. Se agregó a §7.
  - **El costo:** O(N log N) por restricción, como la construcción, en el pool bloqueante; inline en wasm32 (build y clippy en verde).
  - **La memoria:** la de los índices de un snapshot de otro peer quedó en INV-S5 e INV-W1.
  - **Liberación:** el `RoomData` de un snapshot rechazado se suelta dentro del closure del pool bloqueante.
  - **El motivo del error** es determinista (M9 y M10), aunque no es una decisión.
  - **`an_update_checks_only_the_constraints_of_the_columns_it_sets`** arma su estado con `RoomData::build`, que no verifica, y sigue en verde.
  - **Pánicos:** `check` lee con `get` y búsquedas que devuelven `Option`, como `check_row`. Los fuzzers con restricciones corrieron en versión larga.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura, que probó en su propia copia y corrió en release las cuatro versiones largas nuevas): sin hallazgos Altos ni Medios. El lote implementa DR7 y el test de INV-C1 y nada más; no rompe ninguna invariante (S3, S4, S5, S6, C1, C2, C3, W1); el checklist de §5.4 se cumple punto por punto; el oráculo no comparte código con la implementación. Se atendieron:
  - **Bajo 1:** la interpretación de "sin claves repetidas", que figura en los desvíos.
  - **Bajo 2:** el motivo sin test (M9 y M10, arriba).
  - **Bajo 3:** el error de la apertura, en §7.
  - **Bajo 4:** §6 de `ARCHITECTURE.md`.
  - **Notas:** el encabezado de este lote, que faltaba desde el Lote 4; el enunciado de INV-S5, que ahora nombra las restricciones; la aserción de restricciones en el fuzzer de archivos dañados; el rustdoc de `load_snapshot`; un comentario cortado en el test de evolución. Las interacciones 3.a y 3.b están arriba.
- **Chequeos** (después de atender la revisión): fmt, clippy, test y doc de §5.1, y el build y clippy wasm32, en verde; los dos jobs MSRV (`cargo +1.88.0 test --workspace` y `cargo +1.85.0 build --workspace --lib --bins`, con un `CARGO_TARGET_DIR` aparte); y `cargo test -p zemdb-storage --release -- --ignored` (19, con las cuatro largas nuevas).
- **Desvíos:**
  - **"Inmutabilidad (sin claves repetidas)"** se interpretó como "nada que verificar en el estado". Rechazar un payload que repite una clave sería barato (`RowsAsRead::visit_map`), pero no cambia ninguna decisión y vale para todas las tablas, así que va al backlog. A confirmar con el dueño del proyecto.
  - **Dónde están los tests:** los de `apply_snapshot` son unitarios (escenarios en `memory/tests/index.rs`) y no van en `apply_snapshot_tests.rs`, porque comparan índices y esquema. Los de apertura están en `disk_wal_resilience_tests.rs`, junto a los de `SchemaMismatch`. El de evolución tiene un archivo propio, porque usa el modelo y el generador del oráculo.
  - **Un fuzzer diferencial nuevo**, porque las mutaciones de bytes casi nunca producen un snapshot bien formado que viole una restricción.
  - **`dump`** de `randomized_engine_tests.rs` acepta `?Sized`, para recorrer los dos motores como `&dyn StorageEngine`.
  - **Tamaño:** unas 27 líneas de producción agregadas sin comentarios ni blancos (4 quitadas), por debajo de las ~120 estimadas.
- **Fuera del lote (backlog):**
  - **Los bytes de un snapshot dependen del orden de un `HashMap`.** `RoomSnapshotRef` serializa `&HashMap<u16, Table>`, así que dos réplicas con el mismo estado (o la misma, en dos procesos) producen snapshots con distintos bytes. Los fuzzers de snapshots, entonces, no se reproducen por semilla. No cambia ninguna decisión. Es preexistente y Bajo: se podría serializar las tablas en orden de id.
  - **Un payload de snapshot que repite una clave primaria** (o un id de tabla) se decodifica quedándose con la última. Es determinista, y no le da a un peer hostil nada que no pueda mandar con un snapshot bien formado. Es preexistente y Bajo: se podría rechazar en el decoder, como `SnapshotCorruption`.

### Lote 6 · Cierre · ✅ Hecho
**Objetivo:** que los documentos describan lo que quedó, y la revisión de cierre (DR14).

**Qué se hace:**
- **`ARCHITECTURE.md`:** una pasada de consistencia de §3.1, §4, §5.1, §7 y §8.
- **`README.md`:** una fila de la Fase 3.11 en la tabla de estado.
- **`ROADMAP.md`:** la fase completa, y la Fase 4 alineada con §7.
- **Revisión de cierre.** La hace un agente aparte, de contexto fresco y en modo solo lectura.
  - **Qué revisa:** el delta completo de la fase y los mecanismos con los que interactúa.
  - **Contra qué:** INV-C1, C2, C3 y A5, auditadas por primera vez, más Y8, Y5, E1, E2, A1, S1, S3, S4, S5, S6, D9, W1 y W2, y el checklist de §5.4.
  - **El criterio:** el de `INVARIANTES.md` §1.4.
- **Lo que encuentre:**
  - una ★ rota o una ruptura Alta se arregla antes de la Fase 4, en un lote 6b;
  - una Media o Baja de origen (b) o (c) va al backlog.
- **`INVARIANTES.md`:** las invariantes nuevas pasan a ✅ si la revisión las sostiene.

**Hecho.**
- **Documentos.**
  - **`ARCHITECTURE.md`**, pasada de consistencia de §3.1, §4, §5.1, §7 y §8 contra `main` (los nombres de tipos y funciones coinciden con el código):
    - §3.1: `validate_operation` corre en el servidor antes de secuenciar y en los motores antes de escribir el WAL, y el replay no revalida (revisión, N2); un `Update` ensancha la fila al ancho del esquema con que se aplica (N4); el dueño seudónimo, "diferido después de la v0.1" en lugar de "until it is supported"; la interacción entre dueños, sin "This phase"; *Reassignment* y *Schema Version*, con la regla del Lote 6b.
    - §4.1: la regla anti-zombi (las réplicas rechazan ese `Update` como `MissingRow`; decía que el servidor lo ignoraba) y *Deletes Are Always Sent* (la fila puede existir en las réplicas; decía "en el servidor").
    - §6: el relay ya no dice que el cliente "debe" validar el snapshot en la Fase 4: lo hace `apply_snapshot`.
    - §8: el header `ZEM1` no tiene "schema fingerprint" (N3); el costo de un `Delete`, que mira las FK de todo el esquema que apuntan a su tabla; `SnapshotBehind` y el no-op del head, que se evalúan después de verificar el snapshot; las cifras de memoria de los índices con claves anchas (N1).
  - **`README.md`:** la fila de la Fase 3.11 en la tabla de estado.
  - **`ROADMAP.md`:**
    - fecha y estado, §2 y §4.3.C con la fase cerrada;
    - §4.4.4 reescrito según §7: la validación liviana del servidor (tipos, dueño declarado, inmutabilidad sobre `Update` y `Delete`); `UNIQUE`, FK, `Duplicate` y `NotOwner` que decide la réplica; el resultado de cada escritura desde el `ApplyReport` del lote de su secuencia; el `ChangeStream`, que no emite nada por rechazos; el dueño que completa el SDK; el pre-chequeo local; y los requisitos abiertos (el reporte perdido ante un crash, las restricciones entre dueños distintos, el `.snap` local que no cumple las restricciones y los borradores offline);
    - §4.4.7, el cronograma (Lotes 1 a 6 y el 6b, y un ítem en el checklist de la Fase 4) y la matriz de trazabilidad;
    - en el backlog, una nota de squashing en el punto 8 y un punto 14 nuevo con los Bajos de la fase.
  - **`INVARIANTES.md`** (versión 5): C1, C2, C3 y A5 pasan a ✅; Y8 confirma su ampliación; §1.4 registra la revisión de cierre y los dos lotes que pidió; C1, A5, E1 y E2 suman las reglas del 6b y el 6c a "Sostenida por" y "Cómo intentar romperla"; S5 y W1, las cifras de memoria.
- **Revisión de cierre** (agente aparte, de contexto fresco y solo lectura, que probó en su propia copia de `77ca57b`):
  - **Qué revisó:** el delta `ef2fed0..77ca57b` (el código desde `449aa73`) y los mecanismos con los que interactúa (el commit y el log del servidor, el sync, el WAL y el replay del storage, los snapshots y su relay, el registry de esquemas y la reasignación de salas, el squashing), contra C1, C2, C3 y A5, auditadas por primera vez, Y8, Y5, E1, E2, A1, S1, S3, S4, S5, S6, D9, W1 y W2, y el checklist de §5.4, con las interacciones de las notas de los Lotes 1 a 5.
  - **Veredictos:** se sostienen C2, C3, A5, Y8 (con un mismo esquema), Y5, E1 (salvo la reasignación), E2, A1, S1, S3, S4, S5, S6, D9, W1 y W2, cada una con su evidencia en archivo y línea y sus tests. **Se rompe C1** (H1). El checklist de §5.4 se cumple, salvo el punto 5 después de una reasignación (H1). No hay códigos `DR`, `INV-` ni `DEF-` en `crates/`.
  - **Corridas en su copia:** `cargo test --workspace` (920) y las versiones largas de core (13), storage (19) y server (13), en verde. Dos tests de ataque no rompieron nada: un `Update` con dos deltas del dueño para transferir la fila (lo rechaza `validate_operation`, por deltas repetidos o desordenados) y la reapertura de un WAL bajo un dueño, una inmutabilidad y una `UNIQUE` con las que no se escribió (sin pánicos y con decisiones deterministas).
  - **H1 · INV-C1 ★ · se rompe.** `check_reassignment` solo miraba `has_same_constraints`, así que una sala se movía a un esquema con las mismas restricciones pero con otras columnas. Una restricción agregada después sobre una columna nueva del destino juzgaba entonces las operaciones viejas, cuyos valores habían caído en ella: una réplica nueva diverge sin aviso, y una de disco que reabre pierde una fila (sin compactar) o no abre (`SnapshotCorruption`, compactada). Lo reproducían tres tests en la copia del revisor (dos de storage y uno de servidor), y los de storage, también en una copia propia. El revisor propuso Media, porque solo se llega por la API de librería y con dos acciones del operador; se trató como ★ rota, porque el chequeo de la reasignación aceptaba los dos pasos, el resultado es divergencia o pérdida sin aviso y no es un doble disparador raro (§1.4). Origen: (b), la reasignación con otras columnas es anterior a la fase, y el chequeo del Lote 2 la dejó pasar. **Se arregló en el Lote 6b**, por decisión del dueño del proyecto. La revisión de delta del 6b encontró otro camino, preexistente, que rompía C1 y A5 (F1: una recarga del esquema llegaba a una sala reasignada o recreada con otro), y lo arregló el **Lote 6c**.
  - **Bajos y notas:**
    - **N1 (a):** las cifras de memoria de los índices no cubrían claves anchas: el revisor midió unos 620 bytes por entrada con una clave de 16 enteros, y unos 19,9 KB de índices por fila con una tabla al tope (32 `UNIQUE` de 16 columnas), unas 85 veces su fila en un snapshot. Se corrigieron `ARCHITECTURE.md` §8, INV-S5, INV-W1 y §8 de este plan; el dimensionamiento va al backlog.
    - **N2, N3 y N4 (b):** frases de `ARCHITECTURE.md` que no coincidían con el código. Corregidas (arriba).
    - **N5 (b):** `create_room` también llega al caso de la sala sin `meta_room.json`. Se sumó a ese ítem del backlog.
    - **N6 (b):** el mensaje de `SnapshotCorruption` por una fila inválida nombra una tabla según el orden de un `HashMap`. Backlog, Bajo.
    - **N7 (a):** una línea de comentario de 129 columnas en `fail_point.rs`. Backlog: no entró al 6b, que solo arregla H1.
    - **N8:** la versión del esquema después de una reasignación. Con el 6b el destino solo agrega columnas y tablas, y un cliente que no baja el esquema falla cerrado; sigue Bajo en el backlog. *Reassignment* de `ARCHITECTURE.md` se corrigió con el 6b.
- **Backlog** (`ROADMAP.md` §7, punto 14): los Bajos que dejaron los Lotes 1 a 5 en sus notas y los de esta revisión, cada uno con su severidad, su origen y el lote que lo encontró.
- **Chequeos:** fmt, clippy, test (928) y doc de §5.1, en verde con los documentos, el 6b y el 6c; los de cada uno, en su nota.
- **Desvíos:**
  - **Hubo código en el cierre:** los Lotes 6b y 6c, por las ★ rotas, cada uno con su propio procedimiento y su revisión de delta.
  - **`ARCHITECTURE.md` §6** también se tocó (una frase en futuro sobre la validación de un snapshot), aunque no estaba entre las secciones de la pasada.
  - **Los Bajos de los lotes** se juntaron en el backlog de `ROADMAP.md`; antes solo estaban en la nota de cada lote.

### Lote 6b · Reasignación solo a un esquema que extiende el suyo (`zemdb-core`, `zemdb-server`) · ✅ Hecho
**Objetivo:** cerrar H1 de la revisión de cierre (INV-C1 ★).

**Decisión** (del dueño del proyecto, entre esta, prohibir la reasignación de una sala con log, o mandar H1 al backlog como Media): la reasignación exige, además de las mismas restricciones, dueño e inmutabilidad, que el esquema nuevo extienda al viejo. Cada tabla del viejo existe en el nuevo con el mismo id, nombre y clave primaria; sus columnas son las primeras de la tabla nueva, con el mismo nombre, tipo, nulabilidad y cifrado; y las de más son nullable. Se permiten tablas nuevas. DR3 y §3.2 lo dicen.

**Hecho.**
- **API.** `Schema::extends(&self, older)` (`crates/core/src/schema/global.rs`) busca cada tabla de `older` por id y delega en `TableSchema::extends` (`table.rs`, `pub(crate)`): el mismo nombre y la misma PK, `columns.starts_with(older.columns)` (la igualdad de `ColumnDef` cubre nombre, tipo, nulabilidad y cifrado) y el resto nullable. `RoomManager::check_reassignment` (`crates/server/src/actor/manager.rs`) exige `extends`, con su propio mensaje de `BadRequest`, y después `has_same_constraints`, antes de escribir `meta_room.json`, como antes.
- **Tests agregados** (2, y dos aserciones en un test aleatorio):
  - `a_schema_extends_another_only_with_nullable_columns_and_tables_added` (`crates/core/tests/schema_evolution_tests.rs`): un esquema se extiende a sí mismo, y lo extienden él mismo con columnas agregadas por `add_column` y `add_column_with_constraints` (una cifrada) y con una tabla más. Uno más angosto es extendido por él, pero no lo extiende. No lo extienden: una columna quitada (al final y en el medio), renombrada, de otro tipo, que pasa a nullable, a requerida o a cifrada, columnas reordenadas, una columna insertada antes de las viejas, una requerida agregada después, una requerida seguida de una nullable, otra PK (compuesta, y del mismo ancho sobre otra columna), la tabla renombrada, la tabla en otro id y una tabla faltante.
  - `operations_stay_valid_under_every_later_schema_version` (y su versión larga): cada versión posterior extiende a la anterior, y no al revés.
  - `a_room_moves_only_to_a_schema_that_extends_its_own` (`crates/server/src/actor/tests/manager.rs`): reasignar a un esquema con una columna menos, renombrada, de otro tipo, nullable, cifrada, con una requerida de más, con otra PK (compuesta), con la tabla renombrada o en otro id, y el caso de H1 (de un esquema con `note` a uno sin ella), es `BadRequest` con `meta_room.json` igual byte a byte. Después, las salas abren con su esquema, y una se mueve a uno que agrega una columna nullable y una tabla.
  - **Contra el esqueleto** (`extends` siempre `true`, ya llamado en la reasignación): fallaban por aserción los tres.
- **Mutaciones verificadas** (10, una por vez y restauradas; todas detectadas): sin el nombre de la tabla; sin la PK; la PK comparada solo por largo; columnas comparadas solo por largo; solo por nombre; sin la nulabilidad de las agregadas; nullable solo la última agregada; una tabla faltante aceptada; sin `extends` en la reasignación; y `extends` al revés. **Equivalente, y se quitó:** comparar también el `table_id` en `TableSchema::extends`, porque la tabla ya se busca por id. **Sobrevivientes, y cómo se cerraron:** la PK solo por largo y la nulabilidad solo de la última agregada (revisión, B1 y B2): se agregaron una PK del mismo ancho sobre otra columna y una columna requerida seguida de una nullable.
- **Interacciones revisadas:**
  - **El momento de la reasignación:** solo al abrir una sala cuyo actor no corre, y antes de escribir `meta_room.json`; `from == to` sale antes y un esquema faltante sigue siendo `SchemaNotFound`. Test: el nuevo y `a_room_moves_only_to_a_schema_with_the_same_constraints_owners_and_immutability`.
  - **Los tests existentes** reasignan entre esquemas idénticos (`first` y `second`) y siguen en verde.
  - **El orden:** primero `extends` y después `has_same_constraints`. Con el prefijo, las posiciones que compara `has_same_constraints` son las mismas columnas.
  - **Un `add_column` posterior sobre el destino** solo lo extiende, y DR3 vuelve a valer: las operaciones viejas no escribieron más allá del ancho del origen.
  - **El storage del cliente:** con `extends`, todas las tablas del origen están en el destino (no hay `SchemaMismatch` al abrir) y, con las mismas restricciones, el `.snap` local verifica igual.
  - **La versión del esquema después de una reasignación** (backlog del Lote 2): si el número coincide y el destino tiene más columnas, el cliente no baja el esquema, y una operación que escriba una columna nueva falla su validación local: falla cerrado. Sigue Bajo.
  - **`create_room` y una sala sin `meta_room.json`** no son reasignaciones (backlog).
  - **wasm32:** core compila (build y clippy en verde).
  - **Un snapshot del relay escrito bajo el esquema de origen** y aplicado con el destino (revisión): con `extends` y las mismas restricciones equivale a una versión posterior, que DR7 y el test de evolución del Lote 5 ya cubren.
  - **Reasignaciones encadenadas o de ida y vuelta** (revisión): `extends` es transitivo, y se compara contra la versión actual del origen, que extiende a todas las anteriores.
  - **Una consecuencia conservadora** (revisión): `has_same_constraints` rechaza un destino con una tabla nueva con restricciones, o con una `UNIQUE` sobre una columna nueva de una tabla vieja, aunque DR3 lo haría seguro. Es lo que dice la respuesta 4 de §8.
  - **La recarga del esquema de origen** en curso mientras la sala se reasigna (revisión): no estaba en la lista y rompía; ver la revisión y el Lote 6c.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura, que probó en su propia copia): sin hallazgos Altos ni Medios en el delta. El lote implementa la decisión y nada más; las 8 mutaciones listadas entonces se detectan, y de 12 propias del revisor sobrevivían 2 (B1 y B2, arriba); el escenario de H1 ahora es `BadRequest` sin reescribir `meta_room.json`, y sobre `77ca57b` se aceptaba. Se atendieron:
  - **B1 y B2:** los dos casos que faltaban (arriba), y la nota, que decía "otra PK" para el caso compuesto del servidor.
  - **Interacciones** que faltaban: el snapshot del relay, las reasignaciones encadenadas, la consecuencia conservadora y la recarga (arriba).
  - **F1 · INV-C1 ★ e INV-A5 ★ · Alto, origen (b):** una recarga del esquema viejo llegaba a una sala que ya estaba en otro. Lo reproducían tres tests en la copia del revisor, y también en una copia propia; es preexistente (pasa igual en `77ca57b`) y el 6b no lo empeora. Se arregló en el **Lote 6c**, por decisión del dueño del proyecto.
  - **`open_room` y `apply_snapshot` con otro esquema** (Bajo, preexistente): solo por la API del storage, que controla el SDK. Se agregó a §7 como requisito de la Fase 4.
- **Chequeos:** fmt, clippy, test (922 tests) y doc de §5.1, y el build y clippy wasm32, en verde; los dos jobs MSRV (`cargo +1.88.0 test --workspace` y `cargo +1.85.0 build --workspace --lib --bins`, con un `CARGO_TARGET_DIR` aparte); y `cargo test -p <crate> --release -- --ignored` de core y server.
- **Desvíos:**
  - **DR3 y §3.2 cambiaron su texto** (la reasignación) y la respuesta 4 de §8 suma la condición. La reasignación sigue permitida, como se decidió en el Lote 0.
  - **Tamaño:** unas 20 líneas de producción sin comentarios ni blancos.

### Lote 6c · Recarga del esquema solo a las salas que siguen en él (`zemdb-server`) · ✅ Hecho
**Objetivo:** cerrar F1 de la revisión de delta del 6b (INV-C1 ★ e INV-A5 ★).

**El problema.** `reload_schema_for_rooms` lista primero las salas que usan el esquema y después, sala por sala, toma su spawn lock y le manda la versión actual, sin volver a mirar si la sala sigue en ese esquema; el actor la instalaba sin comparar ids, y el commit valida con ella. La recarga corre en su propia tarea y espera a cada actor sin timeout: mientras está trabada en una sala, otra de la lista puede reasignarse, o borrarse y recrearse con otro esquema (solo con la API HTTP de administración), y recibir el esquema viejo. Esa sala sirve y valida sus commits con un esquema que no es el suyo hasta que su actor reinicia: juzga su log con restricciones que no tiene (C1) y puede secuenciar una operación que declara dueño a otro cliente (A5). Lo abre solo el operador, pero el efecto es silencioso y queda en el log, y no falla cerrado.

**Decisión** (del dueño del proyecto, entre esta, la misma sin la segunda defensa, o mandar F1 al backlog como Media): con el spawn lock tomado, la recarga vuelve a comprobar que la sala siga en el esquema y, si no, la saltea; y `RoomCommand::ReloadSchema` lleva el `schema_id`, y el actor ignora la recarga de un esquema que no es el suyo.

**Hecho.**
- **API.** `reload_schema_for_rooms` (`crates/server/src/actor/manager.rs`) compara, bajo el spawn lock, el `schema_id` de `room_meta` de la sala con el de la recarga antes de mandarla, y saltea la sala si no coincide o no tiene metadatos. `RoomCommand::ReloadSchema { schema_id, schema, reply }` (`command.rs`); el actor (`room.rs`) solo instala el esquema, y emite `SchemaReloaded`, si `schema_id` es el suyo, y responde `Ok` en los dos casos.
- **Tests agregados** (6):
  - `a_reload_does_not_reach_a_room_that_moved_to_another_schema` (`crates/server/src/actor/tests/manager.rs`): una recarga de `first` lista dos salas y queda trabada en una (con sus spawn locks tomados, como `schema_reload_racing_a_respawn_reaches_the_new_actor`); la otra se reasigna a un esquema que extiende `first`, y `first` suma una columna con `UNIQUE` en la posición de una columna del destino. La recarga llega solo a la sala que sigue en `first`, que recibe su última versión, y la reasignada sirve exactamente su esquema.
  - `a_reload_does_not_reach_a_room_created_again_with_another_schema`: lo mismo con una sala borrada y creada de nuevo con otro esquema, solo con las operaciones de administración.
  - `a_room_created_again_with_an_owner_keeps_checking_it_after_a_reload_of_its_old_schema`: la sala recreada con dueño rechaza con `SchemaViolation` un `Insert` de un cliente que declara dueño a otro, y no secuencia nada.
  - `a_room_reloads_only_its_own_schema` (`crates/server/src/actor/tests/room.rs`): el actor ignora una recarga de otro esquema, sin emitir `SchemaReloaded`, y aplica una del suyo, emitiéndolo.
  - `a_reload_waiting_for_a_rooms_lock_skips_it_if_it_moves_meanwhile` (revisión, B2): la recarga espera el spawn lock de la sala mientras, con ese lock, la sala se mueve a otro esquema; la saltea, porque vuelve a mirar con el lock tomado.
  - `a_room_created_again_with_the_same_schema_gets_its_newest_version` (revisión): una sala borrada y creada de nuevo con el mismo esquema, mientras la recarga está trabada, la recibe con la versión que el registry tiene al mandarla, nunca una anterior.
  - **Contra el esqueleto** (`ReloadSchema` ya llevaba el `schema_id`, pero ni el manager ni el actor lo usaban): fallaban por aserción los cuatro primeros; los dos últimos y la aserción del evento se agregaron por la revisión.
- **Mutaciones verificadas** (8, una por vez y restauradas): sin el re-chequeo en el manager (lo detectan los dos primeros, porque la sala aparece entre las recargadas aunque el actor la ignore); el re-chequeo al revés (cuatro tests, también los dos de recarga que ya existían); el actor que aplica cualquier recarga (el del actor); el actor al revés (tres tests); el re-chequeo antes de tomar el lock (el de la sala movida bajo su lock); el evento emitido aunque se ignore la recarga (el del actor); y, de la regla previa, la versión leída antes de recorrer las salas (dos tests). **Sobrevivientes, y cómo se cerraron:** el re-chequeo antes del lock y el evento (revisión, B1 y B2): los dos tests y la aserción nuevos. **Equivalente:** aceptar en el re-chequeo una sala sin metadatos. El actor saca su entrada de `room_meta` justo antes de salir, así que una sala sin metadatos no corre o se está cerrando, y el chequeo del actor cubre igual ese caso; se dejó la forma conservadora.
- **Interacciones revisadas:**
  - **La recarga y el respawn** (`schema_reload_racing_a_respawn_reaches_the_new_actor`): el respawn resuelve los metadatos antes de que la recarga tome el lock, así que la sala sigue listada y recibe la versión nueva. Sigue en verde.
  - **La creación concurrente** (`schema_reload_does_not_block_concurrent_room_creation`): sin cambios; el re-chequeo es una lectura de `room_meta` sin retener la guarda.
  - **La lista que devuelve la recarga** ya no incluye una sala salteada; el handler de `add_column` no la usa (solo espera a la tarea).
  - **`SchemaReloaded`:** solo se emite si el actor instaló el esquema.
  - **El orden de los comandos:** el actor aplica los comandos en orden, así que una recarga de su esquema que llega después de otra más nueva no lo atrasa (como antes: cada recarga lee el registry al mandarse).
  - **Una sala recreada con el mismo esquema** (revisión): sigue en la lista y recibe la versión que el registry tiene al mandarse; el registry solo avanza. Test: `a_room_created_again_with_the_same_schema_gets_its_newest_version`.
  - **No hay ventana entre el re-chequeo y el envío** (revisión): los dos van bajo el mismo spawn lock, que toman `delete_room`, `create_room` y la reasignación y el respawn (`slot`); la única que escribe `room_meta` sin él es la tarea del actor al salir, y solo saca su entrada. Test: `a_reload_waiting_for_a_rooms_lock_skips_it_if_it_moves_meanwhile`.
  - **No hay otro emisor del esquema a un actor vivo** (revisión): `ReloadSchema` solo lo arma la recarga; `get_or_spawn` con un esquema sobre una sala viva devuelve el actor sin reasignarlo, y `create_room` sobre ella es `RoomAlreadyExists`. El rechazo de comandos al cerrar (`command.rs`) no cambia.
- **Revisión de delta** (agente aparte, de contexto fresco y solo lectura, que probó en su propia copia): en verde, sin hallazgos Altos ni Medios. El lote implementa la decisión y nada más; no rompe C1, A5, E2, Y5, L1, L2, D5 ni A1; F1 ya no se reproduce (sin el delta, los tres tests de la revisión del 6b reproducen el defecto; con él, fallan en su aserción del defecto); `cargo test --workspace` (926) y las largas de server, en verde. Se atendieron:
  - **B1 y B2:** sobrevivían el evento emitido aunque se ignore la recarga y el re-chequeo antes del lock. Se agregaron la aserción y el test (arriba).
  - **B3:** `ARCHITECTURE.md` §7.1 decía que `SchemaReloaded` se emite "across all active rooms"; ahora, en las que usan el esquema.
  - **Nota:** el docstring de `reload_schema_for_rooms` decía que devuelve las salas que aplicaron el esquema; ahora, las salas a las que se mandó.
  - **Interacciones** que faltaban: la sala recreada con el mismo esquema, la ventana y los otros emisores (arriba).
- **Chequeos** (después de atender la revisión): fmt, clippy, test (928 tests) y doc de §5.1, y el build y clippy wasm32, en verde; los dos jobs MSRV (`cargo +1.88.0 test --workspace` y `cargo +1.85.0 build --workspace --lib --bins`, con un `CARGO_TARGET_DIR` aparte); y `cargo test -p <crate> --release -- --ignored` de core y server.
- **Documentos:** `ARCHITECTURE.md` §7.1 (la recarga solo llega a las salas que siguen en el esquema); `INVARIANTES.md`: C1 y A5 en "Sostenida por", y C1, A5 y E2 en "Cómo intentar romperla".
- **Desvíos:** ninguno respecto de la decisión. **Tamaño:** unas 20 líneas de producción sin comentarios ni blancos.

---

## 7. Requisitos para la Fase 4 (DR12)
- **Resultado de cada escritura.**
  1. El cliente hace el commit y recibe `CommitAck` con la secuencia asignada.
  2. Aplica el catch-up. Si hay `has_more`, o el acuse llegó sin catch-up por presupuesto, sigue con `/sync` hasta la secuencia asignada.
  3. Toma el resultado de esa secuencia del `ApplyReport`.

  `insert`, `update` y `delete` devuelven `Ok`, o un error con el motivo: `ConstraintViolation { constraint, kind }` o `RowMissing`. El nombre `ConflictError` del plan actual se reemplaza.
- **Rechazos del servidor.** Un dueño declarado que no es el del cliente, y un `Update` o `Delete` sobre una tabla inmutable, llegan como `SchemaViolation` en el commit, como cualquier otra validación del servidor.
- **El dueño lo completa el SDK.** La aplicación no lo escribe:
  - el SDK pone el `ClientId`, o el seudónimo derivado de la clave de dueño de `Registered` (Lote 2b), en la columna dueño;
  - con el dueño fuera de la clave, agrega el delta del dueño a cada `Update` y el `owner` a cada `Delete`;
  - con seudónimo, el SDK también lo usa para reconocer las filas propias;
  - si el dueño está en la clave primaria, la aplicación da el resto de la clave y el SDK completa la columna dueño;
  - si la aplicación escribe la columna dueño a mano, el SDK rechaza la llamada localmente con un error claro, sin mandar nada. No la pisa en silencio: un valor distinto del propio es un error de la aplicación, y el servidor lo rechazaría igual (`SchemaViolation`).
- **Pre-chequeo local.** Opcional y activado por defecto: verifica contra el estado local antes de mandar. Es solo un aviso temprano, porque la réplica local puede estar atrasada. La decisión es la de la aplicación en orden.
- **`ChangeStream` y `watch`:** no emiten nada por operaciones rechazadas.
- **Crash entre el commit y la aplicación.** El cliente conoce el resultado al volver a sincronizar solo si guardó la secuencia asignada de su mutación pendiente; si no, lo ve solo en el estado. Lo decide el diseño del SDK de la Fase 4, como DEF-85 (la outbox está diferida para después de la v0.1).
  - Guardar la secuencia no alcanza si el crash (o un pánico, o un future soltado) llega después de que la réplica hizo durable el batch y antes de que entregara el `ApplyReport`: la reapertura aplica el batch y decide igual, pero el reporte se descarta, reenviar el batch es `SequenceMismatch` y un sync no lo vuelve a entregar (revisión del Lote 3). El SDK necesita otra fuente para esa decisión, por ejemplo guardar los rechazos en el storage junto con el batch, o recalcularla.
- **`ROADMAP.md` §4.4.4.** El paso 2 deja de decir que el servidor valida las restricciones. El servidor valida lo liviano, incluido el dueño declarado. La réplica decide `UNIQUE`, FK, `Duplicate` y `NotOwner`.
- **Restricciones entre dueños distintos** (revisión del Lote 4). Las reglas valen igual para todos los miembros: uno puede ocupar primero una clave `UNIQUE`, y una fila propia que referencia la fila de otro (que ese otro no puede borrar, `NotOwner`) le impide borrar la suya (`RESTRICT`). No rompe INV-A5, que habla de escribir filas ajenas, pero limita lo que §1.1 promete. El diseño de la Fase 4 decide si alcanza con documentarlo para quien escribe el esquema (por ejemplo, que una FK hacia una tabla con dueño incluya la columna dueño) o si hace falta una regla nueva.
- **Un snapshot local que no cumple las restricciones** (revisión del Lote 5). `open_room` lo rechaza con `SnapshotCorruption`, la misma variante que un CRC roto o un archivo truncado, pero no es daño en el archivo: solo pasa si la sala se abre con restricciones con las que sus datos no se escribieron, y vuelve a abrir con el esquema con que se escribieron. El SDK no debe reaccionar a ese error descartando la réplica local para resincronizar, porque perdería datos intactos y escrituras pendientes. El diseño de la Fase 4 decide si alcanza con distinguirlo por el mensaje o si hace falta una variante propia del error.
- **El esquema con que el SDK abre y aplica** (revisión de delta del Lote 6b). `open_room` y `apply_snapshot` aceptan cualquier esquema que tenga las tablas de la sala: las columnas no se verifican. El SDK abre la sala solo con el esquema que le sirve el servidor y, ante un cambio (una versión nueva o una reasignación), exige que el nuevo extienda al viejo con las mismas restricciones (`Schema::extends` y `Schema::has_same_constraints`), y si no, falla cerrado. Opcionalmente, `apply_snapshot` podría rechazar un esquema que no extienda el actual.
- **Borradores offline (post-v0.1).** Con restricciones, mandar lo consolidado deja de ser equivalente a mandar en orden. Por ejemplo, `Insert(u = 1)` seguido de `Update(u = 2)` puede dar otro resultado que un único `Insert(u = 2)` si una de las dos claves choca. El gestor de borradores tiene que elegir entre no consolidar las columnas con restricciones o aceptar esa diferencia, y documentarla.

---

## 8. Riesgos y preguntas abiertas

**Preguntas para el Lote 0:**
Todas resueltas en el Lote 0:
1. ¿`RESTRICT`, o que el borrado de un padre deje a los hijos colgando? Resuelta: `RESTRICT`.
2. ¿Se informa `MissingRow`? Resuelta: sí.
3. Los topes de DR9. Resuelta: 32 restricciones por tabla y 16 columnas por restricción.
4. Reasignación de esquema: ¿solo con las mismas restricciones, o prohibida para una sala con log? Resuelta: solo con las mismas restricciones, dueño e inmutabilidad; desde el Lote 6b, además, solo a un esquema que extiende el suyo.
5. ~~Seudónimos (Lote 2b): ¿entran en esta fase o se difieren?~~ Resuelta: se difieren a después de la v0.1.
6. Alcance del seudónimo: ¿por tabla, o también por fila? Por fila sería, por ejemplo, incluir `poll_id` en la derivación, para que los votos de un cliente en encuestas distintas no se puedan vincular. El servidor lo podría calcular sin estado, porque esos valores vienen en la operación. Diferida con el Lote 2b; la propuesta es por tabla.
7. ¿La encarnación de la sala entra en la derivación del seudónimo? Diferida con el Lote 2b; la propuesta es que sí, porque una sala recreada es otra sala.
8. El nombre de la fase y de los campos. Resuelta: Fase 3.11, `owner`, `identity` e `immutable`.

**Riesgos:**
- **Divergencia por un error de implementación** (DR13): la mitigan el oráculo, la convergencia y la revisión de cierre, y la detectaría un digest de estado (backlog).
- **Operación veneno:** un pánico del verificador después de escribir el WAL deja la sala sin poder abrir (§5.3, capa 4).
- **Memoria:** cada restricción indexa, por fila, su clave y la PK. Medido en el Lote 3: entre 167 y 221 bytes por fila y por restricción, contra 264 de una fila de cuatro columnas (nota **Hecho** del Lote 3). La revisión de cierre midió unos 620 bytes con una clave de 16 columnas: una tabla al tope (32 `UNIQUE` de 16 columnas) suma unos 19,9 KB de índices por fila, unas 85 veces lo que ocupa su fila en un snapshot. Con filas como las medidas, una sala de 100 MB de filas y tres restricciones por tabla ocupa unos 330 MB en RAM. Si resultara mucho para el cliente, queda buscar una representación más compacta.
- **Costo de abrir una sala:** construir los índices es O(N log N) por restricción, en el pool bloqueante.
