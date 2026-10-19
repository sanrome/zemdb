# Restricciones de integridad verificadas por las réplicas — Fase 3.11

**Fecha:** 19 de Octubre de 2026
**Tipo:** Feature, no remediación. Documento vivo: propuesta, plan por lotes y seguimiento.
**Estado:** Propuesta. Las decisiones de §3 se confirman en el Lote 0.
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
- **Plan.** Siete lotes, más uno opcional (§6): contrato, core, servidor (con un 2b opcional de seudónimos), tres de storage y cierre. La metodología está en §5.

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
| Restricciones sobre columnas que ya existen | Se aplicarían también a operaciones secuenciadas antes de crearlas. Hace falta anclar el cambio de esquema en el log (DR3). | Sí, con un marcador de cambio de esquema en el log de cada sala. Es el cambio caro. |
| Anonimato frente al operador del servidor | El seudónimo (DR15) oculta el `ClientId` a los demás miembros, pero quien tiene el secreto del servidor puede recalcularlo para cada cliente. | No con este modelo. Haría falta criptografía como firmas ciegas o pruebas de conocimiento cero. |
| Restricciones diferidas y transacciones de varias operaciones | Cada commit lleva una sola operación (`ClientMessage::Commit`). | Requiere commits de varias operaciones. |
| Collations, comparación sin distinguir mayúsculas, normalización Unicode | El resultado depende de la versión de Unicode de cada cliente: no es determinista entre réplicas. | No con este modelo. La aplicación puede guardar una columna ya normalizada. |
| Restricciones sobre columnas cifradas | El texto cifrado es aleatorio, y ni el storage ni el servidor tienen las claves. | No. |
| Consultas por índice secundario | Los índices de esta fase son internos. | Sí (backlog 7 de `ROADMAP.md` §7). |
| Detección de divergencia entre réplicas | Ver DR13. | Sí, con un digest del estado por secuencia. |

---

## 3. Decisiones de diseño

Se numeran DR1, DR2, etc. para no chocar con la numeración D de los planes de remediación, que la Fase 3.10 sigue usando. Todas quedan como *Propuesta* hasta el Lote 0.

| ID | Decisión | Lote | Estado |
|---|---|---|---|
| **DR1** | **`UNIQUE`, `FOREIGN KEY` e inmutabilidad: las verifica cada réplica.** Las restricciones son parte del esquema. El servidor valida su definición y las guarda, distribuye y recarga como hoy. No verifica filas ni rechaza commits por ellas, así que no hay `ErrorCode` nuevo para estas tres. Cada réplica decide al aplicar cada operación en orden de secuencia, en `apply_batch` y en el replay del WAL, en los dos motores. Una operación que viola una restricción se rechaza entera, nunca a medias: no cambia tablas ni índices, y `head_seq` avanza igual porque la operación se consumió. Una operación inválida para el esquema (tipo, aridad) sigue siendo un error de `apply_batch`: es un log dañado, no un rechazo. | 1–5 | Propuesta |
| **DR2** | **Alcance.** El de §2.1. Lo de §2.2 queda afuera. | 0 | Propuesta |
| **DR3** | **Una restricción nunca cambia la decisión sobre el pasado.** Una restricción nace con el esquema (`POST /admin/schemas`), o con la columna nueva que agrega `add_column`. En ese caso tiene que incluirla: entre sus columnas para `UNIQUE`, y entre las columnas de la tabla hija para una FK. Nunca se agrega sobre columnas existentes, ni se quita ni modifica. El dueño y la inmutabilidad se declaran solo al crear el esquema. Una sala existente solo se reasigna a otro esquema (`RoomManager::get_or_spawn` con esquema) si ese esquema tiene las mismas restricciones. El porqué está en §3.2. **Alternativa descartada para esta fase:** un marcador de cambio de esquema en el log de cada sala. Permitiría restricciones sobre columnas existentes, a cambio de tocar el log, el sync y el replay. | 0, 1, 2 | Propuesta |
| **DR4** | **Semántica de la verificación.** Ver §3.1. En resumen:<br>- Se verifican las restricciones cuyas columnas toca la operación (todas, en un `Insert`).<br>- La igualdad es la de `Value`.<br>- Una clave con algún `NULL` no se verifica.<br>- Las columnas que faltan en una fila corta son `NULL`.<br>- El motivo informado es el de la primera restricción que falla, en un orden fijo. | 4 | Propuesta |
| **DR5** | **El resultado se informa.** `apply_batch` devuelve `ApplyReport { head_seq, rejected }`, con un elemento `(seq, motivo)` por cada operación no aplicada.<br>- **Motivos:** `Unique`, `ForeignKey` y `Restrict`, con el nombre de la restricción; `Duplicate` (un `Insert` en una tabla inmutable sobre una clave existente); `NotOwner` (la fila existente es de otro dueño); y `MissingRow` (un `Update` sobre una fila que no existe, que hoy se ignora sin aviso).<br>- Solo se listan las excepciones, para no reservar un resultado por operación.<br>- En la recuperación, el reporte se descarta.<br>Los nombres son tentativos. | 3, 4 | Propuesta |
| **DR6** | **Índices derivados en memoria, sin cambio de formato en disco.**<br>- Un índice por restricción: para `UNIQUE`, clave → PK; para una FK, clave foránea → PK hija. Los dos son multimapas ordenados y persistentes (`imbl`, con clon O(1) como `Table`), para que el motor de disco los clone y los publique junto con las tablas.<br>- Solo se indexan claves sin `NULL`.<br>- No se persisten. Se construyen al abrir (desde el snapshot, antes del replay) y al aplicar un snapshot, y se actualizan con cada operación aplicada.<br>- El `.snap`, el WAL y el envelope `ZMSN` no cambian. | 3 | Propuesta |
| **DR7** | **El estado siempre cumple las restricciones.**<br>- `apply_snapshot` verifica que el snapshot cumpla las restricciones del esquema con que se aplica. Lo hace al construir los índices, en el pool bloqueante. Si no las cumple, responde `SnapshotCorruption` y la sala no cambia (INV-S5).<br>- `open_room` hace lo mismo con el snapshot local. Una violación es un error y no se descarta nada (INV-S4). | 5 | Propuesta |
| **DR8** | **Formato estricto y semántica congelada.**<br>- Las restricciones, el dueño y la inmutabilidad viven en `TableSchema`, que ya rechaza campos desconocidos.<br>- `ColumnDef` y los cuerpos de `POST /admin/schemas` y `.../columns` pasan a rechazarlos. Hoy un `"unique": true` dentro de una columna se ignoraría sin aviso.<br>- Los campos nuevos se serializan siempre, porque bincode es posicional. En JSON tienen default vacío.<br>- `PROTOCOL_VERSION` queda en `0x01` (`GEMINI.md`).<br>- La semántica de un tipo de restricción no cambia nunca: otro comportamiento es otro tipo. Un cliente viejo no puede decodificar un tipo nuevo, así que falla cerrado en vez de aplicar el log sin la regla. | 1, 2 | Propuesta |
| **DR9** | **Límites.**<br>- Un tope de restricciones por tabla y de columnas por restricción, para acotar el costo de cada operación (INV-W1). Propuesta: 32 y 16.<br>- Los nombres de restricción son únicos en el esquema. | 1 | Propuesta |
| **DR10** | **Dueño de fila: el servidor verifica quién declara, las réplicas de quién es la fila.**<br>- Una tabla declara `owner: { column, identity }`. La columna no está cifrada. Es `String` si la identidad es el `ClientId`, y `Bytes` si es un seudónimo (DR15).<br>- **Toda operación declara el dueño:**<br>  - **Columna en la clave primaria:** la clave ya lo trae, en `Insert`, `Update` y `Delete`.<br>  - **Columna fuera de la clave:** el `Insert` lo trae en la fila; un `Update` tiene que incluir un delta de esa columna con el dueño; un `Delete` lo lleva en un campo nuevo, `Delete { owner }` (hoy `Delete` no lleva datos).<br>  - `validate_operation` rechaza una operación que no lo declara, y una que lo declara donde no corresponde.<br>- **Servidor:** al recibir el commit, después de `validate_operation` y antes de secuenciar, compara el dueño declarado con el del cliente autenticado (INV-A1), byte a byte. Si no coincide, responde `SchemaViolation`, permanente para esa mutación, y no queda rastro (INV-Y5). No responde `Forbidden`, que el SDK interpreta como un problema del token.<br>- **Réplicas:** si la columna está fuera de la clave y la fila existe, su dueño tiene que ser el declarado; si no, rechazan la operación (`NotOwner`). Así nadie modifica, borra ni reemplaza una fila ajena, y un `Update` no puede transferir una fila.<br>- El servidor no modifica la operación ni guarda estado, y el log no necesita registrar quién escribió cada operación. Ver §3.4. | 2, 4 | Propuesta |
| **DR11** | **Tabla inmutable.** `immutable: true` en la tabla.<br>- `validate_operation` rechaza `Update` y `Delete` sobre ella, sin estado: el servidor responde `SchemaViolation` y el storage la trata como cualquier operación inválida.<br>- Las réplicas rechazan un `Insert` cuya clave primaria ya existe (motivo `Duplicate`), en vez del upsert habitual. | 2, 4 | Propuesta |
| **DR12** | **Requisitos para la Fase 4.** Ver §7.<br>- El cliente resuelve cada escritura cuando su réplica aplicó hasta la secuencia asignada, con el resultado de DR5.<br>- Hay un pre-chequeo local opcional.<br>- El `ChangeStream` no emite nada por operaciones rechazadas.<br>- `ROADMAP.md` §4.4.4 se reescribe. | 0, 6 | Propuesta |
| **DR13** | **Sin detección de divergencia en esta fase.** Es un riesgo aceptado (`INVARIANTES.md` §4): una réplica que decide distinto diverge sin aviso.<br>- **Mitigación:** el oráculo diferencial y los tests de convergencia (§5.3).<br>- **Backlog:** un digest del estado por secuencia, que las réplicas comparen a través del servidor (por ejemplo, en el heartbeat). | 0 | Propuesta |
| **DR14** | **Cierre por revisión de delta, sin auditoría amplia.** Core, storage y servidor están dentro del alcance auditado, y el modelo de amenaza no cambia en lo que importa a D90: un miembro hostil sigue pudiendo secuenciar operaciones válidas, y las réplicas honestas las rechazan. Cada lote cierra con su revisión de delta (§5.2, paso 4), y la fase con una revisión de cierre del delta completo. Esa revisión también audita por primera vez las invariantes nuevas. | 6 | Propuesta |
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
| 0 | Contrato | Documentos | Todas | Nuevas: C1, C2, C3, A5. Ajuste: Y8 | ⬜ |
| 1 | `UNIQUE` y FK en el esquema | core | DR3, DR8, DR9 | E1, W1, W2 | ⬜ |
| 2 | Servidor: dueño, inmutabilidad y API de administración | core, server | DR3, DR8, DR10, DR11 | A1, A5, Y5, E1, E2, D9, W1, W2 | ⬜ |
| 2b | Dueño seudónimo (diferido) | core, server | DR15 | A1, A5, Y5 | ⏸ |
| 3 | Índices derivados y `ApplyReport` | storage | DR5, DR6 | S1, S3, S6, C3 | ⬜ |
| 4 | Verificación en las réplicas | storage | DR1, DR4, DR5, DR10, DR11 | Y8, S1, A5, C2, C3, W1 | ⬜ |
| 5 | Snapshots, apertura y evolución | storage | DR3, DR7 | S4, S5, C1, C2, W1 | ⬜ |
| 6 | Cierre | Documentos | DR12, DR14 | Todas las tocadas | ⬜ |

**Requisito para empezar la Fase 4:** Lotes 0 a 6 cerrados, con la revisión de cierre en verde. El 2b está diferido y no es requisito. El diseño del cliente, no su código, puede empezar cuando cierre el Lote 3, que fija el contrato de `apply_batch`.

### Lote 0 · Contrato · ⬜ Pendiente
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
  - anotar en el backlog 7 (índices secundarios) que los índices de las restricciones existen pero son internos.
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

### Lote 1 · `UNIQUE` y FK en el esquema (`zemdb-core`) · ⬜ Pendiente
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

### Lote 2 · Servidor: dueño, inmutabilidad y API de administración · ⬜ Pendiente
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

### Lote 3 · Índices derivados y `ApplyReport` (`zemdb-storage`) · ⬜ Pendiente
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

### Lote 4 · Verificación en las réplicas (`zemdb-storage`) · ⬜ Pendiente
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

### Lote 5 · Snapshots, apertura y evolución (`zemdb-storage`) · ⬜ Pendiente
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
- **Un snapshot de otro peer escrito con una versión vieja,** con filas cortas, se verifica bajo la nueva y pasa (INV-C1).
- **La verificación de la apertura sale antes de truncar, absorber o reescribir nada,** como `SchemaMismatch` (INV-S4).
- **El relay:** un snapshot hostil que viola restricciones lo rechazan los clientes honestos. Esto mejora DEF-81, pero no lo cierra.
- **El costo:** O(N log N) por restricción en el pool bloqueante, e inline en wasm32.

**Documentos:**
- `ARCHITECTURE.md` §8, en las reglas de `apply_snapshot` y en la apertura.
- `INVARIANTES.md`:
  - "Sostenida por" de INV-S4 e INV-S5;
  - INV-C1 pierde el lote pendiente en su estado, y sigue 🆕 hasta la revisión de cierre.

### Lote 6 · Cierre · ⬜ Pendiente
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
  - con seudónimo, el SDK también lo usa para reconocer las filas propias.
- **Pre-chequeo local.** Opcional y activado por defecto: verifica contra el estado local antes de mandar. Es solo un aviso temprano, porque la réplica local puede estar atrasada. La decisión es la de la aplicación en orden.
- **`ChangeStream` y `watch`:** no emiten nada por operaciones rechazadas.
- **Crash entre el commit y la aplicación.** El cliente conoce el resultado al volver a sincronizar solo si guardó la secuencia asignada de su mutación pendiente; si no, lo ve solo en el estado. Lo decide el diseño de la outbox, como DEF-85.
- **`ROADMAP.md` §4.4.4.** El paso 2 deja de decir que el servidor valida las restricciones. El servidor valida lo liviano, incluido el dueño declarado. La réplica decide `UNIQUE`, FK, `Duplicate` y `NotOwner`.
- **Borradores offline (post-v0.1).** Con restricciones, mandar lo consolidado deja de ser equivalente a mandar en orden. Por ejemplo, `Insert(u = 1)` seguido de `Update(u = 2)` puede dar otro resultado que un único `Insert(u = 2)` si una de las dos claves choca. El gestor de borradores tiene que elegir entre no consolidar las columnas con restricciones o aceptar esa diferencia, y documentarla.

---

## 8. Riesgos y preguntas abiertas

**Preguntas para el Lote 0:**
1. ¿`RESTRICT`, o que el borrado de un padre deje a los hijos colgando? `RESTRICT` necesita el índice inverso; la otra opción es más barata pero más débil. Propuesta: `RESTRICT`.
2. ¿Se informa `MissingRow`? Propuesta: sí; hoy el `Update` se pierde sin aviso.
3. Los topes de DR9 (32 restricciones por tabla y 16 columnas por restricción).
4. Reasignación de esquema: ¿solo con las mismas restricciones, o prohibida para una sala con log? Propuesta: con las mismas restricciones.
5. ~~Seudónimos (Lote 2b): ¿entran en esta fase o se difieren?~~ Resuelta: se difieren a después de la v0.1.
6. Alcance del seudónimo: ¿por tabla, o también por fila? Por fila sería, por ejemplo, incluir `poll_id` en la derivación, para que los votos de un cliente en encuestas distintas no se puedan vincular. El servidor lo podría calcular sin estado, porque esos valores vienen en la operación. Propuesta: por tabla en esta fase.
7. ¿La encarnación de la sala entra en la derivación del seudónimo? Propuesta: sí; una sala recreada es otra sala.
8. El nombre de la fase (3.11) y de los campos (`owner`, `identity`, `immutable`).

**Riesgos:**
- **Divergencia por un error de implementación** (DR13): la mitigan el oráculo, la convergencia y la revisión de cierre, y la detectaría un digest de estado (backlog).
- **Operación veneno:** un pánico del verificador después de escribir el WAL deja la sala sin poder abrir (§5.3, capa 4).
- **Memoria:** cada restricción indexa, por fila, su clave y la PK. En el rango de salas del proyecto (5 a 100 MB) entra en RAM, pero hay que medirlo en el Lote 3.
- **Costo de abrir una sala:** construir los índices es O(N log N) por restricción, en el pool bloqueante.
