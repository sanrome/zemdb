# Invariantes de ZemDB

**Estado:** Vigente. Versión 3 (18 de Octubre de 2026): incorpora las decisiones de la [Fase 3.10](proposals/2026-10-fase3_10-remediation-plan.md) (D90–D97) tomadas sobre la [2ª auditoría por invariantes](audits/2026-10-invariantes-audit-2.md). La versión 2 (16 de Octubre) incorporó las de la [Fase 3.9](proposals/2026-10-fase3_9-remediation-plan.md) (D70–D72, D77–D79) y es la que auditó la 2ª ronda; la versión 1 (15 de Octubre, sobre `6485f46`), la que auditó la [primera](audits/2026-10-invariantes-audit.md).
**Uso:** base de las auditorías por invariantes y de los tests de propiedades. Reemplaza a las auditorías abiertas por perspectiva ("buscá errores desde X").
**Contrato de referencia:** [`ARCHITECTURE.md`](../ARCHITECTURE.md). Las invariantes se enuncian sobre el comportamiento observable y remiten a la sección que lo describe; no citan líneas de código, para que el documento no envejezca con cada refactor.

---

## 1. Para qué sirve y cómo se usa

Una invariante es una garantía que el sistema promete **en todos los caminos**: con crashes en cualquier punto, cancelaciones en cualquier `.await`, concurrencia arbitraria y clientes hostiles dentro del modelo de amenaza (§2). La lista es **cerrada**: una auditoría termina cuando cada invariante tiene un veredicto.

### 1.1. Veredictos

Cada invariante auditada recibe exactamente uno:

| Veredicto | Qué tiene que traer |
|---|---|
| **Se sostiene** | El argumento: qué mecanismos la garantizan, por qué cubren todos los caminos que llegan a ella, y qué tests la ejercitan. Si un camino no tiene test, se dice. |
| **Se rompe** | Un test que la rompe (en una copia del repo), el escenario en una línea, el origen (§1.3) y la severidad (la de la invariante, salvo que se justifique otra). |
| **Indeterminada** | Qué faltó para decidir (un camino que no se pudo recorrer, un comportamiento del SO o de una dependencia que no se pudo comprobar). No es un hallazgo; pasa a la revisión de cierre o de delta que la toque (D90). |

Un "se rompe" sin test que lo reproduzca no se acepta como hallazgo Alto ni Medio.

### 1.2. Lo que no es una invariante

- **Hallazgos fuera de la lista** (un doc desactualizado, un test débil que no protege ninguna invariante, una mejora de rendimiento sin cota violada): se anotan como **Bajo en el backlog**, no entran a un plan de remediación por lotes.
- **Una invariante que falta:** si un auditor encuentra una garantía que el sistema necesita y que no está acá, la **propone** (enunciado + por qué) en lugar de reportar un defecto. Agregarla es una decisión del dueño del proyecto, y se audita en la revisión de cierre o de delta que la toque (D90).
- **Riesgos aceptados** (§4): no se vuelven a reportar.

### 1.3. Origen de cada ruptura

| Origen | Significado | Qué indica si domina |
|---|---|---|
| **(a) Introducida** | La introdujo la última ronda de cambios (`git blame` / bisect sobre el rango de la ronda). | El proceso de fix: lotes demasiado grandes o tests exigidos insuficientes. |
| **(b) Preexistente** | Existía antes de la última ronda y nadie la había mirado así. | Normal; tiene que bajar ronda a ronda. |
| **(c) Disparador raro** | Solo se dispara con hardware o SO fallando, o con una secuencia hostil improbable. | Se prioriza por costo del fix, no por cantidad. |

### 1.4. Criterio de corte

Una auditoría por invariantes está **en verde** cuando:
1. Ninguna invariante **núcleo** (marcada ★) se rompe.
2. Ninguna ruptura es Alta.
3. Las rupturas Medias que quedan tienen origen (b) o (c) y un fix acotado.

**Doble disparador raro (D91).** Una ruptura que necesita dos disparadores (c) independientes a la vez y falla cerrada es Media como máximo, y no cuenta como ★ rota para la condición 1. "Falla cerrada" quiere decir que la sala no abre o se responde un error o `BehindCompaction`, y que nada se pierde ni se inventa sin aviso. Si algo se pierde o se inventa sin aviso, la ruptura conserva la severidad de la invariante.

**Después de la 2ª auditoría (D90).** No hay más rondas amplias periódicas:
- La Fase 3.10 cierra con una **revisión de cierre** de su delta: las mismas tres condiciones, sobre lo que la fase tocó y los mecanismos con los que eso interactúa. En verde, el código auditado se considera estable.
- Desde entonces, cada cambio se revisa por su delta contra esta lista y contra los mecanismos con los que interactúa. Lo que aparezca de origen (b) o (c), Medio o Bajo, va al backlog, no a un plan nuevo.
- Una auditoría amplia vuelve a hacer falta solo si cambia el modelo de amenaza (§2), o si entra código fuera del alcance actual, como el SDK de la Fase 4. En ese caso se audita una vez, con este método, contra las invariantes nuevas de ese código.

### 1.5. Estado de cada invariante

La columna **Estado** del resumen (§3) dice cómo quedó cada invariante en la última auditoría y qué lote del plan vigente la cierra:
- ✅ se sostuvo;
- ✅ (Dnn): la auditoría la rompió, pero la decisión Dnn la deja sostenida sin código (un supuesto de §2, un riesgo aceptado de §4 o un enunciado ajustado);
- ❌ se rompió, y entre paréntesis va el lote que la arregla;
- ❌ Bajo (backlog): todas sus rupturas abiertas son Bajas y no entran en ningún lote; no bloquea el corte (§1.4);
- 🆕 es nueva y todavía no se auditó, y entre paréntesis va el lote que la implementa, si hace falta código.

Cada lote que cierra una ruptura actualiza esta columna en el mismo commit (§5).

### 1.6. Reparto sugerido

Un auditor por grupo (§3.1 a §3.9). Cada uno recibe este documento, `ARCHITECTURE.md`, la lista de riesgos aceptados (§4) y las decisiones de los planes de remediación (D1 a D102), y responde solo con veredictos.

---

## 2. Modelo de amenaza y supuestos

Las invariantes valen **bajo estos supuestos**. Una ruptura que solo existe violando uno de ellos no es un hallazgo (pero sí puede ser una propuesta para cambiar el supuesto).

**Se confía en:**
- **El sistema de archivos honra `fsync`**: lo sincronizado sobrevive a un corte de energía. Puede dejar escrituras rotas (*torn writes*), colas llenas de ceros y renames no persistidos si no se sincronizó el directorio. En Windows, el sync de directorios es un no-op (ver §5.2 de `ARCHITECTURE.md`).
- **El disco no se edita de forma maliciosa**: puede tener corrupción accidental (bytes dañados, archivos truncados o faltantes), pero nadie fabrica frames con CRC válidos.
- **Los nombres de las entradas de directorio** (D92): la corrupción accidental afecta el contenido de los archivos, no los nombres con que figuran en su directorio (ext4 con `metadata_csum`, XFS v5, btrfs y ZFS verifican los bloques de directorio y devuelven EIO antes que un nombre alterado). Por eso el rango de un segmento, o la sala y la secuencia de un snapshot, se leen de su nombre sin checksum.
- **El poseedor del admin secret** es de confianza total.
- **El proceso es el único dueño de su `data_dir`**; los locks lo hacen cumplir, y que lo cumplan es parte de las invariantes.

**No se confía en:**
- **La red y quien no tiene token:** puede mandar cualquier byte, a cualquier ritmo, abrir y abandonar conexiones.
- **Un miembro con token válido de una sala:** puede ser hostil dentro de su sala (mandar mensajes válidos en cualquier orden, no leer respuestas, reintentar, abrir streams). No puede afectar a otras salas ni actuar como otro cliente. Que suba un snapshot bien formado pero falso está fuera de alcance (§4).
- **El reloj de pared:** puede saltar hacia atrás o adelante. El orden lo define solo `SequenceNumber`.

**Fuera de alcance de la v0.1:** TLS (lo termina el proxy), revocación de tokens, defensa contra snapshots envenenados, el SDK del cliente (Fase 4) salvo `zemdb-storage` y `zemdb-core`.

---

## 3. Invariantes

### Resumen

Estado en `b4cb390`, según la [2ª auditoría por invariantes](audits/2026-10-invariantes-audit-2.md) (ver §1.5), con los lotes del [plan de la Fase 3.10](proposals/2026-10-fase3_10-remediation-plan.md) y las decisiones D92 a D97.

| ID | Invariante | ★ | Si se rompe | Estado |
|---|---|---|---|---|
| **INV-D1** | Un commit confirmado sobrevive a un crash | ★ | Alto | ❌ (Lote 1) |
| **INV-D2** | Un número de secuencia nunca se reutiliza | ★ | Alto | ✅ |
| **INV-D3** | La recuperación nunca descarta datos durables ni acepta basura | ★ | Alto | ❌ (Lote 1) |
| **INV-D4** | Los metadatos se reemplazan atómicamente | | Medio | ✅ |
| **INV-D5** | Una sala tiene un solo dueño | ★ | Alto | ✅ |
| **INV-D6** | Un solo proceso por `data_dir` | ★ | Alto | ❌ (Lote 3) |
| **INV-D7** | Nada es visible antes de ser durable en este proceso | ★ | Alto | ❌ (Lote 1) |
| **INV-D8** | El piso de secuencia vive fuera del log | ★ | Alto | ✅ |
| **INV-D9** | Los metadatos se verifican antes de usarse | ★ | Alto | ✅ (D92) |
| **INV-D10** | Cada archivo del log es contiguo por construcción | | Medio | 🆕 (Lote 1) |
| **INV-S1** | `apply_batch`: éxito durable o ningún efecto | ★ | Alto | ✅ (D93) |
| **INV-S2** | Los lectores ven siempre un prefijo del WAL | | Medio | ✅ |
| **INV-S3** | Un crash en cualquier paso del storage se recupera al mismo estado | ★ | Alto | ✅ (D93) |
| **INV-S4** | Abrir una sala nunca descarta datos | ★ | Alto | ✅ |
| **INV-S5** | `apply_snapshot`: valida todo y reemplaza atómicamente, o no cambia nada | | Medio | ✅ |
| **INV-S6** | Memoria igual a disco | | Medio | ✅ |
| **INV-Y1** | Orden total contiguo por sala | ★ | Alto | ✅ |
| **INV-Y2** | Entrega exacta: ni huecos, ni duplicados, ni cambios | ★ | Alto | ✅ |
| **INV-Y3** | Quedarse atrás siempre se señala | ★ | Alto | ✅ |
| **INV-Y4** | Idempotencia por cliente dentro de la ventana | | Medio | ✅ |
| **INV-Y5** | Un commit rechazado no deja rastro | | Medio | ✅ |
| **INV-Y6** | Los cursores solo avanzan por lo que el cliente confirmó | | Medio | ✅ |
| **INV-Y7** | Un registro con cursor de otra encarnación nunca se acepta | | Medio | ✅ |
| **INV-Y8** | Réplicas con el mismo prefijo convergen | ★ | Alto | ✅ |
| **INV-Y9** | La ventana de dedup es función del log | | Medio | ✅ (D94) |
| **INV-R1** | La poda por cursores nunca borra lo que alguien necesita | ★ | Alto | ✅ |
| **INV-R2** | El rango retenido es contiguo y su cola no retrocede | | Medio | ✅ |
| **INV-R3** | Perder estado volátil del roster solo cuesta retención o re-registro | | Medio | ✅ |
| **INV-R4** | Todo cliente que se queda atrás termina pudiendo ponerse al día | | Medio | ❌ (Lote 4; DEF-181 al backlog) |
| **INV-R5** | Solo se anuncian y sirven snapshots usables, y una descarga es de un solo snapshot | | Medio | ❌ (Lote 4) |
| **INV-R6** | Un snapshot solo se ofrece a la encarnación que lo aceptó | | Medio | ✅ |
| **INV-R7** | Una subida confirmada es usable | | Bajo | ❌ Bajo (backlog) |
| **INV-A1** | Nadie actúa sobre otra sala ni como otro cliente | ★ | Alto | ✅ |
| **INV-A2** | Nada se decodifica antes de autenticar, salvo lo acotado de `/register` | | Alto | ✅ |
| **INV-A3** | Los efectos de un miembro quedan confinados a su sala | | Medio | ❌ (Lote 4; DEF-181 al backlog) |
| **INV-A4** | Ningún identificador produce rutas fuera de `data_dir` | ★ | Alto | ✅ |
| **INV-B1** | La memoria por request está acotada | ★ | Alto | ❌ (Lote 2) |
| **INV-B2** | Nada se retiene sin plazo | | Medio | ❌ (Lote 4) |
| **INV-B3** | La memoria, el disco y los FDs del servidor están acotados por la configuración | | Medio | ✅ |
| **INV-B4** | El costo por request del actor no depende de la historia | | Medio | ✅ |
| **INV-B5** | Ningún cliente acapara un recurso global | | Medio | ✅ |
| **INV-L1** | Toda request recibe una respuesta, en tiempo acotado | | Medio | ✅ |
| **INV-L2** | Cancelar una request nunca deja estado a medias | | Medio | ❌ (Lote 3) |
| **INV-L3** | Un I/O incierto nunca se confirma como éxito | ★ | Alto | ✅ |
| **INV-L4** | Reabrir una sala no cambia ningún estado confirmado | | Medio | ✅ |
| **INV-L5** | El apagado no pierde nada confirmado | | Medio | ✅ |
| **INV-L6** | Borrar una sala es atómico | | Medio | ✅ |
| **INV-L7** | Ningún efecto en disco sobrevive al lock que lo serializa | | Medio | 🆕 (Lote 3) |
| **INV-E1** | El esquema solo crece, y toda operación del log sigue siendo válida | ★ | Alto | ✅ |
| **INV-E2** | Un cliente con el esquema viejo siempre puede enterarse | | Bajo | ✅ |
| **INV-W1** | Ninguna entrada hace entrar en pánico, abortar ni reservar de más | ★ | Alto | ✅ |
| **INV-W2** | Codificar y decodificar es la identidad, y todo byte persistido está bajo un checksum | | Medio | ✅ (D92) |

---

### 3.1. Durabilidad del servidor

#### INV-D1 ★ · Un commit confirmado sobrevive a un crash
**Enunciado.** Si el servidor envió un `CommitAck` con la secuencia `s`, después de un crash o corte de energía en cualquier punto, al reabrir la sala la operación está en el log con la secuencia `s` y el mismo contenido, o `s < tail_seq` porque una poda legítima la borró (INV-R1).
**Sostenida por.** `sync_data` de `active.wal` antes de responder; sync del directorio al crear `active.wal` y al sellar segmentos; compresión a cold con tmp, sync, rename, sync y verificación antes de borrar el warm (`ARCHITECTURE.md` §5.2). *Pendiente (Lote 1):* que un commit nunca se escriba detrás de un hueco de `active.wal` (INV-D10, D98).
**Cómo intentar romperla.** Crash (fail point) entre cada paso de: append, rotación, escritura del sidecar `.ids`, compresión, borrado del warm, poda, escritura de `log_meta.json`. Una sala que se apaga por inactividad durante la rotación. Un `fsync` que falla y una request reintentada (ver INV-D7).
**Automatizable.** Simulación con fail points en cada paso y comparación contra el conjunto de `CommitAck` enviados.

#### INV-D2 ★ · Un número de secuencia nunca se reutiliza
**Enunciado.** Una secuencia que la sala asignó (y que pudo haberse entregado) nunca se asigna a otra operación: tampoco después de podar todo, de reiniciar, de perder o dañar segmentos, ni de recrear el `active.wal`. Nunca se asigna `u64::MAX`.
**Sostenida por.** `head_seq` sale del máximo entre los nombres de segmento, `active.wal` y `log_meta.json`; `log_meta.json` se escribe antes de borrar; `checked_add` y rechazo de `u64::MAX` (D66) (§5.1, §5.2 *Durable Sequence Floor*, *Damaged Segments*); el piso de secuencia de INV-D8; INV-D7 para lo que se confirma al reabrir.
**Excepción (D78).** Si se daña el último frame de `active.wal` y no hay ningún frame válido después, no se distingue de una escritura rota (INV-D3): se trunca, y su secuencia puede reutilizarse si no aparece en ningún otro archivo del log (INV-D8). La lista de archivos es cerrada (D95): el riesgo vale aunque el roster (`last_ack_seq`) o el relay (la secuencia del snapshot activo) tengan esa secuencia (§4).
**Cómo intentar romperla.** Podar todos los segmentos y reiniciar; perder el último segmento sellado; `active.wal` con cola rota; error de `stat` en `active.wal` (D67); crash entre el rename de rotación y el sync del directorio; un `fsync` fallido seguido de un corte de energía.

#### INV-D3 ★ · La recuperación nunca descarta datos durables ni acepta basura
**Enunciado.** Al abrir un log (servidor o storage), un daño seguido de un frame válido es corrupción y no se trunca nada; un daño sin frames válidos después es *torn write* y se trunca solo desde el daño. Ningún frame cuyo header o payload no pase su CRC se interpreta como operación.
**Sostenida por.** Header con CRC propio (D46), `find_valid_frame`, clasificación compartida en `zemdb-core` (§8 *Physical WAL Framing*). *Pendiente (Lote 1):* que la apertura del servidor nunca borre una copia cold verificada para quedarse con su warm (DEF-168).
**Excepción (D95).** En storage, `.wal.compacting` se escribe entero y sincronizado antes de tomar su nombre (en la rotación y en la absorción, D85), así que un daño en su último frame solo puede ser accidental. Igual se trata como escritura rota: si el WAL activo no tiene lotes posteriores, el lote de ese frame se pierde sin aviso; si los tiene, la apertura falla por el hueco (§4).
**Cómo intentar romperla.** Daño en cada byte del header y del payload, cola de ceros, frame válido embebido como valor dentro de un payload (un frame embebido puede convertir una escritura rota en corrupción: es un riesgo aceptado, §4), más de 16 payloads dañados seguidos, lectores con buffers que cortan en cualquier punto, daño en `.wal.compacting`. Un `log_meta.json` dañado que haga borrar segmentos al abrir (ver INV-D9).
**Automatizable.** Logs aleatorios con un corte o un daño en una posición aleatoria; oráculo = el prefijo de frames anteriores al daño.

#### INV-D4 · Los metadatos se reemplazan atómicamente
**Enunciado.** Después de un crash, `meta_room.json`, el roster, `schemas/{id}.json`, `log_meta.json` y los sidecars `.ids` están en su versión anterior completa o en la nueva completa; nunca mezclados ni vacíos. Un archivo ilegible hace fallar la apertura (o, en el roster y los sidecars, se descarta con las consecuencias documentadas), nunca se lo toma por vacío o inexistente.
**Sostenida por.** `write_atomic` (tmp, sync, rename, sync del padre); limpieza de `.tmp` al abrir; `try_exists` (D67) (§5.2 *Atomic Server Metadata*).
**Cómo intentar romperla.** Crash en cada paso de `write_atomic`; `.tmp` huérfanos con nombres de otro archivo; un error de `stat`.

#### INV-D5 ★ · Una sala tiene un solo dueño
**Enunciado.** En ningún momento dos actores, dos procesos ni dos instancias del storage escriben los archivos de la misma sala. Un respawn empieza recién cuando el actor anterior soltó los archivos. Cubre los archivos de la sala, es decir, su directorio: los del relay los serializan el lock del slot y `server.lock` (INV-D6, INV-L7).
**Sostenida por.** `log.lock` y el lock de `active.wal` (servidor), `room_{id}.lock` y el lock del WAL (storage), spawn lock por sala y aviso de fin del actor (§5.2 *Room Log Lock*, §5.4, §8 *Room Lock*).
**Cómo intentar romperla.** Respawn mientras el actor viejo drena su cola o comprime al apagarse; `delete_room` concurrente con `get_or_spawn`; un `tokio::fs` en vuelo después de soltar la sala (ver INV-L2); la ventana de la rotación de compactación; un segundo proceso que borra la sala (ver INV-D6).

#### INV-D6 ★ · Un solo proceso por `data_dir`
**Enunciado.** Un segundo proceso servidor sobre un `data_dir` que ya usa otro se niega a arrancar antes de tocar ningún archivo (salas, relay, esquemas). Ningún proceso borra ni recrea los archivos de una sala cuyo log tiene abierto otro. Un proceso retiene el lock hasta después de su última operación de archivo, incluidas las del pool bloqueante (INV-L7).
**Sostenida por.** D83: lock exclusivo de `data_dir/server.lock` al arrancar, antes de tocar cualquier otro archivo; `delete_room` toma el `log.lock` de la sala antes de purgar el relay o borrar nada (§5.2 *Room Log Lock*, *Data Directory Lock*). *Pendiente (Lote 3):* soltar `server.lock` recién después de soltar el runtime (D100, DEF-172).
**Cómo intentar romperla.** Dos servidores sobre el mismo directorio, con arranques solapados; borrado y recreación de una sala desde el segundo proceso.

#### INV-D7 ★ · Nada es visible antes de ser durable en este proceso
**Enunciado.** Ningún dato leído al abrir (un registro de `active.wal`, una entrada de directorio, el WAL del storage) se sirve, se deduplica ni se confirma antes de que lo cubra un `fsync` exitoso hecho por este proceso, aunque lo haya escrito una encarnación anterior del actor o del proceso. Para un archivo que solo toma su nombre definitivo después de un `fsync` exitoso de quien lo escribió (segmentos sellados, sidecars, colds, JSON, `.snap`), alcanza con que este proceso sincronice su directorio antes de usarlo.
**Sostenida por.** D77: `sync_all` de `active.wal` y sync de `segments/`, del directorio de la sala, de `rooms/` y de `data_dir` al abrir el log, y de `data_dir` al arrancar; en `open_room` del storage, sync de `data_dir` y su padre y `sync_all` del WAL activo (lo de `.wal.compacting` se vuelve durable en el snapshot donde se pliega) (§5.2 *Opening a Room*, §8 *Options and Data Directory*). *Pendiente (Lote 1):* sync de `schemas/` al arrancar y de `snapshots/` al recuperar el relay (DEF-169). Riesgo aceptado: el EIO de Linux (§4).
**Cómo intentar romperla.** Un `fsync` que falla, seguido de reapertura y reintento del mismo commit; un sync de directorio que falla y un intento posterior que adopta el archivo; el corte de energía se simula truncando cada archivo a su último largo sincronizado.

#### INV-D8 ★ · El piso de secuencia vive fuera del log
**Enunciado.** Al abrir, `head_seq` nunca es menor que una secuencia que conste como asignada en cualquier archivo del log: nombres de segmentos y de sidecars, `active.wal` y `log_meta.json` (que guarda el fin del último segmento sellado y el de la última poda). Perder un segmento deja un hueco, que se reporta como `BehindCompaction` (INV-Y3), nunca secuencias reutilizables. La lista es cerrada (D95): el roster y el relay también registran secuencias asignadas, pero no entran en el piso (§4, fila D78).
**Sostenida por.** D81: `log_meta.json` guarda `pruned_through_seq` (antes de borrar) y `sealed_through_seq` (al sellar, antes del rename de `active.wal`); `head_seq` es el máximo de los dos, los nombres de segmento y `active.wal` (§5.2 *Durable Sequence Floor*). Los nombres de los sidecars no se leen: como `sealed_through_seq` se escribe antes del rename, ningún segmento ni sidecar termina por encima.
**Cómo intentar romperla.** Perder el segmento sellado más nuevo justo después de una rotación (sin `active.wal`); perder segmentos después de una poda total; una rotación interrumpida entre el sellado y la escritura de `log_meta.json`.

#### INV-D9 ★ · Los metadatos se verifican antes de usarse
**Enunciado.** Todo JSON persistido que decide algo (`log_meta.json`, `meta_room.json`, `schemas/{id}.json`, el roster) se verifica con un checksum antes de usarse. Si no verifica, cuenta como ilegible: falla la apertura, o el roster se descarta con un warning. Abrir un log nunca borra un archivo a partir de un valor que no verificó, ni de uno verificado que se derivó de otro sin verificar. Los nombres de archivo no cuentan como valores sin verificar: se confía en ellos (§2, D92).
**Sostenida por.** D82: los cuatro JSON llevan una última línea `crc32:<hex>` con el CRC32 del JSON, que se verifica antes de parsear (§5.2 *Atomic Server Metadata*). Los sidecars, los frames del WAL, el `.snap` y el envelope `ZMSN` tienen su propio CRC.
**Cómo intentar romperla.** Un bit invertido en cada campo de cada JSON que lo deje válido; un archivo truncado; la línea del checksum faltante o con otro valor.

#### INV-D10 · Cada archivo del log es contiguo por construcción
**Enunciado.** Ni `active.wal`, ni un segmento sellado, ni su copia cold tienen huecos ni operaciones fuera del rango que indican su nombre o el inicio de `active.wal`. Un registro que no continúa el último de `active.wal` nunca se escribe.
**Sostenida por.** La compresión contrasta el nombre del segmento con su contenido (§5.2). *Pendiente (Lote 1):* sellar `active.wal` al abrir si quedó por debajo del piso, y que `append` rechace un registro que no continúa `active.wal` (D98, DEF-167).
**Cómo intentar romperla.** Un crash entre la escritura de `log_meta.json` y el rename de la rotación, más un daño en el último frame de `active.wal`; un commit después de truncar una escritura rota.

### 3.2. Durabilidad del storage (cliente)

#### INV-S1 ★ · `apply_batch`: éxito durable o ningún efecto
**Enunciado.** Si `apply_batch` devuelve `Ok`, el batch está sincronizado en el WAL y visible en memoria. Si devuelve `Err`, no cambió nada ni en memoria ni en disco, o la sala quedó marcada como fallida (`RoomFailed`) y reabrirla reconstruye el estado desde el disco. Un hueco de secuencia se reporta como `SequenceMismatch` antes que cualquier error de contenido (D63).
**Sostenida por.** `sync_file` con flush previo (D43); `PendingWrite`; validación del batch entero antes de escribir; `open_room`, `apply_snapshot` y la compactación corren en su propia tarea, así que, mientras el runtime siga sondeando esa tarea, ninguna de sus operaciones de archivo aterriza después de reabrir la sala (D86, D89) (§8 *Active Storage Schema Validation*, *WAL Mutex*, *Failed Room State*). `apply_batch` no corre en una tarea: lo que impide que su E/S en vuelo aterrice después de reabrir es el lock del handle del WAL más el `flush` de `close_room`.
**Riesgo aceptado (D93).** Apagar el runtime con un `open_room`, un `apply_snapshot` o una compactación en vuelo y reabrir la sala en el mismo proceso antes de que termine su hilo bloqueante (§4).
**Cómo intentar romperla.** Falla de escritura de fondo, cancelación del future en cada `.await`, pánico al aplicar, batch con hueco interno, batch al borde de `u64::MAX`.

#### INV-S2 · Los lectores ven siempre un prefijo del WAL
**Enunciado.** Un `get` o un `scan` ve el estado después de un batch completo, nunca medio batch, y nunca un batch que no esté sincronizado. Un `scan` largo no ve estados de batches distintos mezclados en una misma tabla.
**Sostenida por.** Clones O(1) de las tablas y publicación bajo el lock de estado (§8 *WAL Mutex*, *Non-Blocking Chunked Scan Streams*).
**Cómo intentar romperla.** Escrituras concurrentes con scans paginados; compactación y `apply_snapshot` durante un scan.

#### INV-S3 ★ · Un crash en cualquier paso del storage se recupera al mismo estado
**Enunciado.** Un crash en cualquier punto de la compactación (fases 1 a 3), de la absorción de `.wal.compacting`, de `apply_snapshot` o de la propia recuperación deja archivos que, al reabrir, dan el estado del último batch confirmado (o el del snapshot, si `apply_snapshot` devolvió `Ok`, falló después del rename o se soltó con su tarea en curso).
**Sostenida por.** §8 *CoW Compaction*, *Crash-Safe apply_snapshot*, *Crash-Safe Recovery of an Interrupted Compaction*; la absorción reemplaza `.wal.compacting` entero y nunca lo escribe en el lugar (D85).
**Riesgo aceptado (D93).** El mismo de INV-S1: apagar el runtime con una tarea del storage en vuelo y reabrir la sala en el mismo proceso (§4).
**Cómo intentar romperla.** Fail points en cada paso; crash durante la recuperación de un crash anterior; `.wal.compacting` dañado.
**Automatizable.** Simulación: secuencia aleatoria de batches, compactaciones y snapshots con un crash aleatorio; oráculo = un `MemoryStorageEngine` con las mismas operaciones confirmadas.

#### INV-S4 ★ · Abrir una sala nunca descarta datos
**Enunciado.** `open_room` con un esquema que no cubre los datos de la sala falla con `SchemaMismatch` antes de modificar cualquier archivo que tenga datos de la sala (antes del chequeo puede crear el lock y un WAL o `.snap` vacíos, y borrar temporales `.snap.tmp.*` y `.wal.compacting.tmp`). Ninguna operación del WAL ni fila del snapshot se descarta en silencio.
**Sostenida por.** D55 (§8 *Opening Requires a Schema That Covers the Room's Data*).
**Cómo intentar romperla.** Tabla presente solo en `.wal.compacting`; tabla vacía en el snapshot; esquema con la tabla pero con otro id; torn write en el mismo WAL que trae la tabla desconocida.

#### INV-S5 · `apply_snapshot` valida todo y reemplaza atómicamente, o no cambia nada
**Enunciado.** Un snapshot con otra `head_seq` que la anunciada, una tabla fuera del esquema o una fila inválida se rechaza sin tocar la sala. Uno aceptado reemplaza esquema, head y tablas de una vez. Si la E/S falla después del rename, la llamada devuelve error, la sala queda `RoomFailed` y al reabrirla aparece el snapshot nuevo. La sala nunca retrocede. Ninguna declaración interna del snapshot reserva más memoria que una constante más lo que ocupan sus bytes.
**Sostenida por.** D48 (§8 *apply_snapshot Rules*, *Bounded Snapshot Decompression*).
**Cómo intentar romperla.** Snapshots fabricados (longitudes declaradas enormes, cuentas de filas falsas, filas con clave distinta de la columna PK, compresión que no termina en la longitud declarada); cierre de la sala durante la decodificación.

#### INV-S6 · Memoria igual a disco
**Enunciado.** Mientras una sala del storage no esté `RoomFailed`, lo que devuelven `get`, `scan` y `get_head_seq` es exactamente lo que daría cerrarla y reabrirla con el mismo esquema (módulo los `Null` al final de las filas, INV-Y8). Vale en los puntos sin una operación en curso: mientras corre la tarea de una operación soltada, la memoria puede ir detrás del disco, y `close_room` espera a esa tarea.
**Sostenida por.** Solo se aplican batches sincronizados; el camino vivo y el replay usan el mismo `apply_operation`; ninguna operación de archivo de `open_room`, `apply_snapshot` ni la compactación aterriza después de reabrir (D86, D89) (§8).
**Cómo intentar romperla.** Comparar después de cada paso de una secuencia aleatoria de batches, compactaciones, snapshots, cancelaciones y reaperturas.
**Automatizable.** Las simulaciones de storage del Lote 0 de la Fase 3.9 ya lo comprueban en cada paso.

### 3.3. Secuencia y entrega

#### INV-Y1 ★ · Orden total contiguo por sala
**Enunciado.** Las operaciones aceptadas en una sala reciben `1, 2, 3, …` en el orden en que el actor las acepta, sin huecos ni repeticiones. El log retenido en cualquier momento es exactamente `[tail_seq, head_seq]` de ese orden.
**Sostenida por.** El actor serializa los commits; `TieredLog::append` (§5.1, §10.1).
**Cómo intentar romperla.** Commits concurrentes desde muchas conexiones; fallo de append seguido de reintento; respawn entre commits; rotación fallida después del fsync.

#### INV-Y2 ★ · Entrega exacta: ni huecos, ni duplicados, ni cambios
**Enunciado.** Para un cursor `c` con `tail_seq - 1 ≤ c ≤ head_seq`, la concatenación de las respuestas que el cliente obtiene siguiendo el protocolo (`/sync` o el catch-up de `CommitAck`, encadenados por `has_more`) entrega exactamente las operaciones `(c, head_seq]`, en orden, idénticas a las aceptadas. Si el log no puede servir el rango, la respuesta es un error explícito (`BehindCompaction` o `Unavailable`), nunca un batch vacío por debajo del head ni uno que saltee operaciones.
**Sostenida por.** Lectura por tiers con presupuesto, `DeltaBatch` contiguo, huecos como `BehindCompaction` (§5.2 *Streaming reads*, *Bounded responses*, *Accurate Physical tail_seq*).
**Cómo intentar romperla.** Cursores en los bordes de cada tier y segmento; operaciones más grandes que el presupuesto; eviction del HotBuffer durante la lectura; compresión o poda entre páginas; segmento dañado en el medio; presupuesto de bytes en vuelo agotado.
**Automatizable.** Simulación: oráculo = vector de operaciones aceptadas; cualquier cursor válido reconstruye su sufijo exacto.

#### INV-Y3 ★ · Quedarse atrás siempre se señala
**Enunciado.** Si el cursor de un cliente ya no se puede servir desde el log (`c < tail_seq - 1`, o un hueco en el rango), la próxima operación del cliente que lleva ese cursor recibe `BehindCompaction`, o una respuesta que lo obliga a un sync que lo recibe: el registro (que informa `tail_seq`), un reintento deduplicado (catch-up vacío con `has_more`) y un ack o commit con un cursor válido y un hueco después (`head_seq` mayor que el cursor, o `has_more`). Ninguna poda, TTL, cuota ni daño deja a un cliente atrás sin que se entere.
**Sostenida por.** `log::retention`, chequeos en sync, ack y commit (§7.2 *Retention Contract*).
**Cómo intentar romperla.** Poda por TTL entre el registro y el primer sync; cliente `Dormant` que vuelve; ack con un cursor más viejo que el guardado; heartbeat (que no falla por diseño: verificar que el cliente igual se entere en su próxima operación con cursor).

#### INV-Y4 · Idempotencia por cliente dentro de la ventana
**Enunciado.** Un reintento del mismo `(client_id, mutation_id)` mientras su clave esté en la ventana de dedup devuelve la secuencia original y no se secuencia otra vez, también después de un respawn o reinicio (ventana hidratada desde `active.wal` y los sidecars; con un segmento sellado dañado, solo si su sidecar se puede leer). El mismo `mutation_id` de otro cliente es una mutación nueva.
**Sostenida por.** D59, D57 (§5.1 punto 4, §5.2 *Opening a Room*).
**Cómo intentar romperla.** Reintento después de un fallo de append, de un respawn, con un sidecar dañado o faltante, con un segmento sellado dañado; reintento concurrente con el original.
**Límite aceptado.** Fuera de la ventana un reintento se aplica de nuevo (DEF-85, §4). Qué es la ventana lo define INV-Y9.

#### INV-Y9 · La ventana de dedup es función del log
**Enunciado.** La ventana de dedup que una sala arma al abrirse son las claves de las últimas `dedup_lru_capacity` mutaciones distintas del log retenido cuyos ids se pueden leer (de `active.wal`, de los sidecars o, si el sidecar falta o está dañado, del segmento), por orden de secuencia. Consultarla no la cambia. Mientras la sala sigue abierta, la ventana en memoria puede además reconocer claves de operaciones que se podaron o cuyo segmento quedó ilegible (D94): un reintento de una de ellas recibe su secuencia original hasta el próximo respawn, y después se comporta como uno fuera de la ventana (§4, DEF-85).
**Sostenida por.** Hidratación por orden del log (§5.2 *Opening a Room*), y una consulta que no cambia el orden de la ventana: reconocer un reintento no lo deja más tiempo en ella (§5.1 punto 4).
**Cómo intentar romperla.** Reintentos de mutaciones viejas con la ventana llena; un respawn entre reintentos; un sidecar faltante o un segmento dañado.

#### INV-Y5 · Un commit rechazado no deja rastro
**Enunciado.** Un commit que recibe cualquier error que no sea "el actor se detuvo después de escribir" (`InvalidSequence`, `BehindCompaction`, `SchemaViolation`, `BadRequest`, `ClientNotRegistered`) no cambia el log, el head, el cursor del cliente ni el dedup, y no emite `head_advanced`. Un rechazo por `BehindCompaction` puede dejar al cliente `Bootstrapping` y hacer que la sala pida un snapshot (con el evento `snapshot_wanted`): eso no es rastro de la mutación.
**Sostenida por.** El orden de chequeos de `handle_commit` (§7.2 *Ordering*, *Size of a single mutation*).
**Cómo intentar romperla.** Cada causa de rechazo combinada con un `mutation_id` repetido; operación justo por encima de cada límite de tamaño.

#### INV-Y6 · Los cursores solo avanzan por lo que el cliente confirmó
**Enunciado.** Entre dos registros, el cursor de un cliente nunca retrocede, nunca supera `head_seq` y solo avanza hasta un valor que el cliente reportó (`ack_seq`, el `from_seq` de un sync servido, el `last_ack_seq` de un commit aceptado o de un reintento deduplicado). Un heartbeat no lo mueve. Un registro lo redefine al `current_seq` que reporta el cliente (o lo deja sin cursor): un valor más bajo solo hace retener más log. Un registro que recibe `Unavailable` por falta de presupuesto ya redefinió el cursor; es inocuo, porque el reintento es idempotente.
**Sostenida por.** `ClientLeaseTracker::observe` (§5.2 *Client Lifecycle States*).
**Cómo intentar romperla.** Acks desordenados o repetidos; sync rechazado; commit rechazado; registro con cursor sobre un cliente existente.

#### INV-Y7 · Un registro con cursor de otra encarnación nunca se acepta
**Enunciado.** Un registro con `current_seq` y una encarnación distinta (o ausente) recibe `RoomReset` sin tocar el roster. La encarnación sobrevive a reinicios, reaperturas y reasignaciones de esquema, y una sala recreada tiene otra. La garantía cubre el registro (D70). Que un cliente que se registró sin cursor no use después un cursor guardado (en sync, ack o commit) es una regla del SDK: arranca de 0 o de un snapshot.
**Sostenida por.** D58, D70 (§6 *Room incarnation*).
**Cómo intentar romperla.** Sala sin `meta_room.json` creada por `get_or_spawn`; delete y create concurrentes; registros con y sin cursor, con encarnaciones viejas, nuevas y ausentes.

#### INV-Y8 ★ · Réplicas con el mismo prefijo convergen
**Enunciado.** Dos réplicas honestas que aplican el mismo prefijo del log, en cualquier combinación de "aplicar operaciones" y "restaurar un snapshot tomado en `S` y aplicar desde `S`", terminan con el mismo estado. "Mismo estado" quiere decir las mismas filas módulo los `Null` al final (D71): un `Update` ensancha la fila hasta el ancho del esquema con que se aplica, y `compact_into_row` completa con `Null`. Aplicar una operación es una función determinista del estado y la operación (upsert del `Insert`, merge por campo del `Update`, tombstone del `Delete`).
**Sostenida por.** `apply_operation` en storage, `Schema::validate_operation` (§4, §10.1).
**Cómo intentar romperla.** `Update` sobre una fila inexistente o borrada; `Update` de una columna agregada después (filas cortas); snapshot de un motor aplicado en el otro; -0.0 y NaN en claves.
**Automatizable.** Log aleatorio; oráculo = aplicar todo desde cero contra snapshot en un punto aleatorio + resto, en los dos motores.
**Fuera de alcance.** Un snapshot falso de un miembro hostil (DEF-81).

### 3.4. Retención y onboarding

#### INV-R1 ★ · La poda por cursores nunca borra lo que alguien necesita
**Enunciado.** La poda proactiva nunca borra una operación con secuencia mayor que el cursor de un cliente `Connected` o `Disconnected` con cursor dentro del log, ni las operaciones `(S, head]` de un snapshot usable, ni las de una subida en curso. La poda por TTL o cuota puede borrar cualquiera de esas, pero solo como prefijo y queda señalada por INV-Y3.
**Sostenida por.** Ancla de retención con `upload_seq` (D61), roster persistido antes de borrar, exclusión de `Bootstrapping` y `Dormant` (§5.2 *Tier 4*, *Snapshot Retention Anchor*).
**Cómo intentar romperla.** Cliente que se registra mientras corre una poda; snapshot que deja de ser usable por un hueco (D62); subida que se vuelve inactiva; un cliente `Bootstrapping` que pasa a `Connected` con un cursor por debajo del piso.

#### INV-R2 · El rango retenido es contiguo y su cola no retrocede
**Enunciado.** Ninguna poda (cursores, TTL, cuota) deja huecos en `[tail_seq, head_seq]`: siempre borra un prefijo. `tail_seq` nunca retrocede, tampoco al reabrir (un warm que sobrevivió a su cold se descarta).
**Sostenida por.** `expire` corta en el primer segmento conservado; `scan` descarta lo podado según `log_meta.json` (§5.2 *TTL & Quota Compaction*, Tier 3).
**Cómo intentar romperla.** Warm que no se pudo comprimir delante de colds vencidos; borrado de warm fallido; segmentos con rangos solapados en disco.

#### INV-R3 · Perder estado volátil del roster solo cuesta retención o re-registro
**Enunciado.** Un crash o un pánico pierde como mucho ~1 s de cambios del roster (cursores, registros y estados; la última actividad, `last_seen`, puede perder más, ver §5.2 *Client Roster Persistence*), y eso solo puede causar más retención, un `ClientNotRegistered` seguido de un re-registro, o un snapshot de más. Nunca una entrega incorrecta ni la poda de algo que un cliente `Connected` necesitaba.
**Sostenida por.** D56, persistencia antes de cualquier poda que borre (§5.2 *Client Roster Persistence*).
**Cómo intentar romperla.** Crash entre un registro y el tick; crash entre un ack y la poda que dispara; roster ilegible al abrir.

#### INV-R4 · Todo cliente que se queda atrás termina pudiendo ponerse al día
**Enunciado.** Mientras exista al menos un cliente `Connected` que siga el protocolo y cuyo estado esté más allá de todo hueco del log, todo cliente `Bootstrapping` termina viendo un snapshot usable anunciado, puede descargarlo y ponerse al día desde él. No hay estados sin salida: snapshot por debajo de un hueco, subida que falla al completarse, designado que se va, snapshot vencido, demanda vencida.
**Sostenida por.** Demanda de snapshot y designación (§5.2 *Snapshot Demand*), D61, D62. El relay solo descarta un snapshot activo si no es usable, si venció o si su sala se borró o se recreó (D96). *Pendiente (Lote 4):* que reiniciar una subida al vencer su tope no conserve la designación (D102, DEF-174), y que el chunk 0 no descarte un snapshot usable comparándolo con un rango leído antes de elegirlo (DEF-176).
**Ruptura Baja abierta.** Las subidas chicas fallidas de un miembro pueden congelar la designación (DEF-181, backlog).
**Cómo intentar romperla.** Cada transición de la demanda combinada con reinicios; el único `Connected` es el designado y se desconecta; la subida completa justo cuando TTL mueve la cola; hueco que aparece después de anunciar el snapshot.
**Automatizable.** Simulación con muchos clientes y crashes: propiedad de liveness acotada ("en N ticks"). Está en el repo desde el Lote 0 de la Fase 3.9.
**Límite aceptado.** La cuota tiene que retener lo que la sala escribe mientras se sube y se restaura un snapshot: como desde D69 también poda warm recientes, con menos cada snapshot puede salir del rango retenido antes de que alguien se ponga al día desde él (§5.2 *TTL & Quota Compaction*).

#### INV-R5 · Solo se anuncian y sirven snapshots usables, y una descarga es de un solo snapshot
**Enunciado.** `Registered`, `HeartbeatAck`, `snapshot_available` y el chunk 0 sin ancla (uno anclado se sirve sin mirar el rango) solo ofrecen un snapshot con `tail_seq - 1 ≤ S ≤ head_seq` que no esté por debajo de un hueco conocido (en o por debajo de un cursor desde el que un sync encontró un hueco). Una descarga anclada por hash recibe chunks de un único snapshot, o `SnapshotSuperseded`; nunca bytes mezclados de dos. Mientras un snapshot está activo, el relay no lo reemplaza por uno de secuencia menor (después de que vence, si al arrancar su archivo es ilegible, o después de la purga de `delete_room` o `create_room`, puede activarse otro menor).
**Sostenida por.** §6 *Multipart Chunked Snapshot Transfer*, *Snapshot Relay*; la marca de hueco que el actor le informa al relay: el mayor cursor desde el que un sync encontró un hueco (§5.2 *Damaged Segments*). *Pendiente (Lote 4):* que el chunk 0 compare con un rango leído después de elegir el snapshot (DEF-176).
**Cómo intentar romperla.** Reemplazo del snapshot durante una descarga; reemplazo con la misma secuencia y otro contenido; reinicio con archivos `.snap` de distintas secuencias, vencidos o ilegibles; borrado y recreación de la sala durante una descarga; un snapshot marcado por un hueco que se pide desde el chunk 0.

#### INV-R6 · Un snapshot solo se ofrece a la encarnación que lo aceptó
**Enunciado.** Un snapshot que el relay aceptó para una encarnación de una sala nunca se anuncia, ancla ni sirve en otra encarnación con el mismo id, aunque la sala se haya recreado sin pasar por `delete_room`.
**Sostenida por.** `delete_room` y `create_room` purgan el relay (D84, §6 *Room deletion*).
**Cómo intentar romperla.** Recrear una sala cuyo directorio se perdió, con un snapshot viejo todavía en el relay; reinicio del servidor entre medio.

#### INV-R7 · Una subida confirmada es usable
**Enunciado.** Si el relay confirma una subida (`SnapshotStaged` o `staged: true`), el snapshot es usable en ese momento (INV-R5). Si el rango retenido se movió y ya no lo es, la respuesta es `SnapshotSuperseded`.
**Sostenida por.** D61 para las subidas de varios chunks; las de un solo chunk anclan la poda mientras se lee el rango, como las de un solo request (§5.2 *Snapshot Retention Anchor*). Contra la poda por TTL o cuota, que ignora el ancla, el piso que el actor publica al abrirse, después de cada pasada de mantenimiento y al encontrar un hueco: el relay lo compara justo antes de instalar (§6 *Acceptance*).
**Cómo intentar romperla.** Podas por cursores, TTL y cuota entre la consulta del rango y la instalación, en subidas de un request, de un chunk y de varios.
**Límite aceptado.** Una pasada de TTL o cuota publica el piso después de borrar los segmentos, así que una instalación que cae en esos milisegundos puede confirmar un snapshot que ya no es usable. Tampoco se mira el piso al reenviar un chunk de una subida ya instalada, que responde `staged: true`. En los dos casos el snapshot no se anuncia ni se sirve (INV-R5, desde el Lote 4 de la Fase 3.10: hasta entonces el chunk 0 puede servirlo, DEF-176); el costo es que la demanda sigue y se pide otra subida.
**Rupturas Bajas abiertas.** El piso se olvida cuando el actor se detiene, y una subida en vuelo se confirma sin chequearlo (DEF-177); el reintento de una subida en un solo request no mira el piso (DEF-178). Las dos, en el backlog.

### 3.5. Aislamiento y autorización

#### INV-A1 ★ · Nadie actúa sobre otra sala ni como otro cliente
**Enunciado.** Ninguna request del data plane tiene efecto sobre una sala sin un token válido de esa sala (o el admin secret en el relay). Ninguna request actúa sobre el estado de un `client_id` distinto del del token. El admin secret no sirve como token de cliente fuera del relay.
**Sostenida por.** Extractores, validación de 3 vías, comparaciones en tiempo constante (§7.2 *Universal Security*).
**Cómo intentar romperla.** Cada endpoint con token de otra sala, de otro cliente, vencido, mal formado, en query (solo `/events` lo acepta); payload con otra sala u otro cliente; un mensaje de otro tipo en cada endpoint.

#### INV-A2 · Nada se decodifica antes de autenticar, salvo lo acotado de `/register`
**Enunciado.** Ninguna request sin autenticar provoca la lectura de un body de más de 64 KiB ni la decodificación de filas. `/register` solo mira la variante antes de decodificar `RegisterClient`.
**Sostenida por.** Orden de extractores, `MAX_REGISTER_BODY_SIZE`, `peek_client_message_kind` (§7.2, §7.3).
**Cómo intentar romperla.** Body grande en cada endpoint sin token; variantes no `RegisterClient` en `/register`; rutas y métodos inexistentes.

#### INV-A3 · Los efectos de un miembro quedan confinados a su sala
**Enunciado.** Un miembro hostil de una sala no puede degradar otras salas más allá de lo que acotan los límites globales (INV-B1 a INV-B3), ni alterar el estado de otro cliente de su sala (cursor, lease, registro, subida en curso ajena con la misma secuencia).
**Sostenida por.** Dedup por cliente (D59), reglas del relay sobre quién reemplaza una subida (las mismas para un chunk y para una subida en un solo request), topes de SSE (D54) (§6, §7.2, §7.3). *Pendiente (Lote 4):* que un chunk de otro miembro no entre en una subida ajena con los mismos parámetros (DEF-180).
**Cómo intentar romperla.** Commits con `mutation_id` ajenos; subidas que reemplazan la de otro; muchos streams SSE; registro y deregistro en bucle.
**Límite aceptado.** Reemplazar una subida con una secuencia mayor, o subir un snapshot falso (DEF-81, §4). La subida de un miembro que no es el designado ancla la poda pero no congela la designación (D76). Los commits de un miembro pueden sacar claves ajenas de la ventana compartida (DEF-85) y, con la cuota (D69), dejar atrás a otros clientes: las dos cosas pasan por el log de la sala, no por el estado del otro cliente.
**Ruptura Baja abierta.** Las subidas chicas fallidas de un miembro pueden congelar la designación (DEF-181, backlog).

#### INV-A4 ★ · Ningún identificador produce rutas fuera de `data_dir`
**Enunciado.** Ningún `RoomId`, `SchemaId`, `ClientId`, secuencia ni hash produce una ruta fuera de `data_dir`, ni dos identificadores distintos producen el mismo archivo (incluidos sistemas de archivos que no distinguen mayúsculas).
**Sostenida por.** Validación por construcción de los ids (§3.1), nombres de archivo inyectivos del relay.
**Cómo intentar romperla.** Ids por URL, por payload binario, por JSON admin y por nombres de archivo encontrados en disco; nombres reservados de Windows.

### 3.6. Recursos acotados

#### INV-B1 ★ · La memoria por request está acotada
**Enunciado.** Ninguna request reserva más memoria que una cota fija por request: un body de hasta `MAX_FRAME_SIZE`, hasta `MAX_MESSAGE_VALUES` valores decodificados y una respuesta de hasta `MAX_FRAME_SIZE`. La apertura de una sala que dispara una request retiene como mucho la ventana de RAM (`ram_max_ops`, `ram_max_bytes`) más un frame, además de los ids de la ventana de dedup. Ninguna respuesta materializa más de `MAX_MESSAGE_VALUES` valores, y lo que retienen las lecturas (presupuesto de lectura, HotBuffer) se cuenta por lo que ocupa en memoria, no por su tamaño de wire. La memoria de todas las respuestas retenidas en el servidor, incluidas `Schema` y `Registered` y los buffers de HTTP/2, está acotada por `max_inflight_response_bytes`.
**Sostenida por.** D44, D53, D73: las lecturas y la HotBuffer cobran `max(wire, 24 × valores)`; cada respuesta presupuestada reserva un frame antes de materializarse (el sync y el catch-up, cuando el actor los atiende) y se queda con su codificación; `Schema` y `Registered` también; `max_send_buf_size` de un frame (§5.2 *Bounded responses*, §7.2 *Defensive Bounding*, §7.3). `max(wire, 24 × valores)` es una estimación: con `Text` o `Bytes` cortos la memoria real llega a 2 o 2,5 veces eso, y las copias de hasta 64 KiB que se entregan a h2 quedan fuera del presupuesto. *Pendiente (Lote 2):* la apertura acotada a la ventana de RAM (D99, DEF-170).
**Cómo intentar romperla.** Longitudes declaradas enormes; cuerpos que se descomprimen; muchos streams HTTP/2 con respuestas grandes sin leer; commits cuyo catch-up se queda sin presupuesto; tablas de miles de columnas con filas de `Null`; respuestas `/schema` sin leer.
**Automatizable.** Fuzzing de los decoders (en el repo desde el Lote 0 de la Fase 3.9) y conteo de los valores que materializa cada respuesta.

#### INV-B2 · Nada se retiene sin plazo
**Enunciado.** Ninguna conexión, body, respuesta, permiso de conexión, subida ni designación se retiene más que un plazo configurado sin progreso del cliente: headers, body por tasa mínima, *stall* de respuesta, inactividad de subidas, designación de 60 s. En una subida, solo un chunk que todavía no se había recibido cuenta como progreso, y además tiene un tope total de duración por uploader (D74, D102): reiniciar la subida al vencer no lo renueva. La única excepción es un stream SSE sano, acotado en cantidad por cliente y en total.
**Sostenida por.** D45, D53, D54 (§7.3); D74, el progreso y el tope de las subidas (`snapshot_upload_max_secs`), y D76, la designación que solo congela la subida del designado o del worker (§6). *Pendiente (Lote 4):* el tope por uploader (D102, DEF-174).
**Cómo intentar romperla.** Cliente que lee un byte por plazo; ventana HTTP/2 que se achica; reenviar un chunk ya recibido; subir un chunk por plazo; stream SSE semiabierto (residuo aceptado, §4).

#### INV-B3 · La memoria, el disco y los FDs del servidor están acotados por la configuración
**Enunciado.** Con salas, clientes y tiempo ilimitados, la memoria del servidor está acotada por la configuración y la cantidad de salas abiertas: HotBuffer por lo que ocupa en memoria (INV-B1), dedup por capacidad, roster que olvida `Dormant`, `room_meta` y spawn locks solo de salas vivas, relay con un snapshot por sala, subidas pendientes y SSE con tope. El disco del log de una sala está acotado por la cuota (que cuenta los sidecars) más `active.wal` y lo escrito durante una pasada de mantenimiento; el relay suma el snapshot activo y, durante una subida, el archivo parcial. Los FDs, por las conexiones más dos por sala abierta más los transitorios del pool bloqueante, que también están acotados.
**Sostenida por.** D47, D50 (§5.2, §5.3, §7.3); D69, la cuota que cuenta los sidecars y poda warm y cold (§5.2 *TTL & Quota Compaction*); D73, la HotBuffer por lo que ocupa.
**Cómo intentar romperla.** Miles de salas abiertas y cerradas; clientes que nunca se deregistran; cientos de miles de `mutation_id` distintos; subidas abandonadas.

#### INV-B4 · El costo por request del actor no depende de la historia
**Enunciado.** Ninguna request (commit, ack, registro, heartbeat, sync) hace trabajo O(historia retenida) ni O(clientes) × fsync en el actor; recorrer el roster en memoria (O(clientes) sin E/S) está permitido. La compresión corre fuera del worker. La apertura cuesta O(`dedup_lru_capacity` + `active.wal` + roster) más un listado de los segmentos, que la cuota acota (INV-B3). Un `Ack` que poda hace un unlink por segmento podado dentro del actor: es trabajo amortizado (cada segmento se borra una vez), proporcional a lo que se poda, que en una sola request puede ser casi todo lo retenido.
**Sostenida por.** D56, D57, D47 (§5.2 *Opening a Room*, *Blocking I/O in the Actor*, *Client Roster Persistence*); el sync busca su primer segmento en el índice, sin recorrerlo desde el inicio.
**Cómo intentar romperla.** Medir con 10⁵ operaciones retenidas, 10³ clientes y operaciones de 20 KB: tormenta de registros, acks dentro de `active.wal`, reaperturas por heartbeat, decenas de miles de segmentos.

#### INV-B5 · Ningún cliente acapara un recurso global
**Enunciado.** Ningún cliente (un par sala y `client_id`, o el admin secret en el relay) retiene más que una parte configurada de un recurso que comparten todas las salas: el presupuesto de respuestas en vuelo y los streams SSE. Lo que excede su parte se le rechaza a él (`Unavailable`, `RateLimited`), no a las demás salas.
**Sostenida por.** D54 para SSE; D75, el tope por cliente del presupuesto de respuestas (`max_inflight_response_bytes_per_client`, §7.3).
**Cómo intentar romperla.** Un miembro de una sala que no lee sus respuestas en muchos streams y conexiones, mientras otra sala hace `/sync`; muchos streams SSE del mismo cliente.
**Límite aceptado.** El tope es por cliente: con la parte por defecto (2 frames sobre unos 16 en total), 8 clientes que cooperan agotan el total.

### 3.7. Disponibilidad y ciclo de vida

#### INV-L1 · Toda request recibe una respuesta, en tiempo acotado
**Enunciado.** Toda request del data plane recibe exactamente una respuesta (salvo un stream SSE abierto), del código documentado, en tiempo acotado: nunca se cuelga, ni siquiera si el actor entra en pánico, se detiene o se reinicia en el medio. La cota es `ACTOR_TIMEOUT` más `ACTOR_EXIT_WAIT` por cada pedido al actor (la suscripción SSE hace hasta tres), más la apertura de la sala cuando la dispara la request, más la E/S del relay y el tiempo de escritura de la respuesta.
**Sostenida por.** `RoomManager::ask` con timeout, `catch_unwind`, cierre del mailbox antes de responder (§5.4, §7.2 *Error Handling*).
**Cómo intentar romperla.** Pánico en cada handler; actor que se apaga por inactividad con la cola llena; append fallido con comandos detrás; spawn que falla.

#### INV-L2 · Cancelar una request nunca deja estado a medias
**Enunciado.** Si el cliente corta una request en cualquier `.await`, el efecto es "no ocurrió" o "ocurrió entero". Las operaciones de varios pasos (borrado y creación de sala, escrituras del relay, `add_column`, compactación y escrituras del storage) corren en su propia tarea, retienen el lock que las serializa hasta su última operación de archivo (INV-L7), o marcan la sala como fallida.
**Sostenida por.** `run_to_completion`, `tokio::spawn` en `delete_room` y `add_column`, `open_room`, `compact_room` y `apply_snapshot` del storage en su propia tarea (D86, D89), `PendingWrite` (§7.1, §8). El apagado no espera esas tareas (D87 se descartó). Cortar una equivale a un crash en lo que hace la tarea, pero lo que ya está en el pool bloqueante sigue corriendo hasta terminar: por eso los locks que serializan esas operaciones viven hasta su última operación de archivo (INV-L7). El borrado con lápida (D84) y los archivos atómicos del relay dejan solo lo que el próximo arranque completa o descarta (§7.2 *Graceful Shutdown*). *Pendiente (Lote 3):* la purga del relay de una creación de sala cortada (D101, DEF-175). Riesgo aceptado en el storage: D93 (§4).
**Cómo intentar romperla.** Descartar el future del handler en cada punto de suspensión (el patrón del test de DEF-125); cancelar `compact_room` y `apply_snapshot` del storage y reabrir la sala mientras una operación de archivo sigue en vuelo; apagar el servidor con una tarea de varios pasos en curso.

#### INV-L3 ★ · Un I/O incierto nunca se confirma como éxito
**Enunciado.** Si un write, un fsync o un rename de un archivo durable falla, nunca se responde éxito por una operación que dependía de él. En el servidor, la sala se detiene y se reabre desde disco; en storage, la sala queda `RoomFailed` hasta reabrirse. Un error de I/O nunca se toma por "el archivo no existe".
**Sostenida por.** D43, D67 (§5.2 *Failed append stops the room*, §8 *Failed Room State*).
**Cómo intentar romperla.** Fail points en cada write, sync, rename y `stat`; un handle de solo lectura (escritura de fondo que falla).

#### INV-L4 · Reabrir una sala no cambia ningún estado confirmado
**Enunciado.** Un respawn (por inactividad, pánico, fallo o reinicio) conserva `head_seq`, la encarnación, el esquema vigente, los cursores persistidos y la demanda de snapshot; `tail_seq` no retrocede; el dedup reconoce lo que estaba en la ventana que la sala arma al abrirse (INV-Y9). Lo que se pierde es solo lo documentado como volátil (designación, marca de hueco, ≤1 s del roster y las claves de operaciones podadas o ilegibles que la memoria todavía reconocía, D94).
**Sostenida por.** §5.2 *Opening a Room*, §5.4.
**Cómo intentar romperla.** Reapertura con `idle_timeout` menor que el tick lento; reapertura durante una subida; reapertura con un sidecar faltante y un segmento dañado.

#### INV-L5 · El apagado no pierde nada confirmado
**Enunciado.** Un apagado ordenado (o uno que vence la gracia de 10 s) no pierde ningún commit confirmado ni deja archivos que la próxima apertura no pueda leer. Lo que puede perder es solo el roster no persistido.
**Sostenida por.** §7.2 *Graceful Shutdown*.
**Cómo intentar romperla.** Señal durante una compresión, una subida o un `add_column`; tareas del relay que reabren una sala después de que `shutdown_all` la enumeró.

#### INV-L6 · Borrar una sala es atómico
**Enunciado.** Después de cualquier interrupción (crash, apagado, cancelación) durante el borrado de una sala, la sala existe entera o no existe, y una sala recreada con el mismo id empieza vacía: sin log, roster ni snapshot de la anterior.
**Sostenida por.** El borrado con lápida (D84): el directorio de la sala se renombra a un nombre que ninguna sala puede tener, y `rooms/` se sincroniza, antes de tocar el relay o borrar un archivo; el arranque borra las lápidas que quedaron, y `create_room` purga el relay (§6 *Room deletion*).
**Cómo intentar romperla.** Crash en cada paso del borrado (después del purge del relay, a mitad de `remove_dir_all`); recrear la sala después de cada uno.
**Límite aceptado.** Si la purga del relay falla después del rename durable, el relay ya olvidó el snapshot en memoria, pero su archivo queda en `snapshots/`: después de un reinicio se vuelve a activar hasta su TTL, y una descarga anclada lo recibe. No se anuncia ni se ofrece a otra encarnación (INV-R6), y `create_room` lo purga.

#### INV-L7 · Ningún efecto en disco sobrevive al lock que lo serializa
**Enunciado.** Una operación de archivo lanzada bajo un lock (el spawn lock de una sala, el lock del slot del relay, `server.lock`, el lock de una sala del storage) termina antes de que el lock se suelte, o corre en una tarea que lo retiene hasta su última operación, aunque quien la lanzó se cancele.
**Sostenida por.** D86 y D89 en el storage; `delete_room` en su propia tarea (D84). *Pendiente (Lote 3):* `server.lock` hasta después de soltar el runtime (D100, DEF-172), y la purga del relay y `drop_stale` con el lock del slot (D101, DEF-175).
**Riesgo aceptado (D93).** Si el runtime se apaga con una tarea del storage en curso, la tarea suelta su referencia a la sala, y cuando el motor también la suelta (se lo descarta, o un `close_room` posterior ya no espera), el lock de la sala se libera con la operación de archivo todavía en el pool bloqueante (§4).
**Cómo intentar romperla.** Cortar la request, o apagar el runtime, con una operación en el pool bloqueante, y tomar el mismo lock enseguida.

### 3.8. Esquema

#### INV-E1 ★ · El esquema solo crece, y toda operación del log sigue siendo válida
**Enunciado.** Un esquema registrado nunca pierde, renumera ni cambia el tipo de una tabla o columna. Las columnas nuevas son nullable, y la versión sube con cada cambio. Por eso toda operación aceptada en una sala sigue siendo válida bajo cualquier versión posterior de su esquema, y una réplica siempre puede aplicar el log.
**Sostenida por.** `Schema::add_column`, `SchemaAlreadyExists`, deserialización estricta (§3.1, §7.1); el checksum de `schemas/{id}.json` (D82), el lock del registry y el orden de la recarga.
**Cómo intentar romperla.** `add_column` concurrentes; reinicio a mitad de una escritura del registry; JSON de esquema con ids repetidos, faltantes o campos desconocidos; filas cortas de antes de una columna nueva; un bit invertido en `schemas/{id}.json` (ver INV-D9).

#### INV-E2 · Un cliente con el esquema viejo siempre puede enterarse
**Enunciado.** Después de un `add_column`, toda respuesta de `Registered`, `HeartbeatAck`, `SyncBatch` y `CommitAck` de una sala con ese esquema lleva la versión nueva, sin depender de SSE.
**Sostenida por.** D60 (§7.2).
**Cómo intentar romperla.** Sala abierta durante la recarga; respawn concurrente con la recarga; reinicio del registry.

### 3.9. Formatos y decodificación

#### INV-W1 ★ · Ninguna entrada hace entrar en pánico, abortar ni reservar de más
**Enunciado.** Ningún byte de red, de disco (WAL, segmentos, sidecars, snapshots, JSON) ni de un snapshot de otro peer provoca un pánico, un abort o una reserva de memoria desproporcionada a su tamaño. Todo error es tipado.
**Sostenida por.** `decode_budget`, `Deserialize` propios, `PayloadReader`, CRC antes de decodificar (§7.2 *Defensive Bounding*, §8 *Bounded Snapshot Decompression*).
**Automatizable.** Fuzzing con generadores propios (sin `cargo fuzz`, D88) de `decode_client_message`, `decode_message`, el decoder de frames WAL, el de sidecars, `SnapshotEnvelopeHeader::parse` y `apply_snapshot`. En el repo desde el Lote 0 de la Fase 3.9.

#### INV-W2 · Codificar y decodificar es la identidad, y todo byte persistido está bajo un checksum
**Enunciado.** Para todo mensaje, frame WAL, sidecar, snapshot y esquema válidos, decodificar lo codificado devuelve el mismo valor (con -0.0 canonicalizado). Todo byte del contenido de un archivo persistido que se usa para decidir algo (operaciones, ids de dedup, longitudes, headers) está cubierto por un CRC o un hash antes de usarse. Los nombres de archivo no: se confía en ellos (§2, D92).
**Sostenida por.** Opciones de bincode únicas, CRC de header y payload, CRC de sidecars, envelope `ZMSN`, BLAKE3 del relay, CRC32 de los JSON persistidos (INV-D9).
**Automatizable.** Roundtrip con generadores aleatorios para cada formato. En el repo desde el Lote 0 de la Fase 3.9.

---

## 4. Riesgos aceptados (no se reportan)

| Riesgo | Por qué se acepta | Referencia |
|---|---|---|
| Un reintento fuera de la ventana de dedup se aplica de nuevo (también el de una operación podada, después de un respawn: INV-Y9), y un cliente no puede reconocer su propio commit en un sync. | Ventana acotada por diseño; se decide con la outbox de la Fase 4. | DEF-85 |
| Un miembro puede subir un snapshot bien formado pero falso, o reemplazar una subida con una secuencia mayor. | Defensa fuera de alcance de la v0.1. | DEF-81, §6 *What the server does not check* |
| Un cliente wasm32 no puede restaurar snapshots Zstd. | Diferido a la Fase 4. | DEF-54 |
| `zemdb-storage` no recarga el esquema de una sala abierta. | Diferido; D55 al menos impide perder datos. | DEF-34 |
| Una conexión SSE semiabierta retiene su permiso hasta que TCP falla. | El keep-alive tarda en llenar el buffer; D54 la acota a 2 por cliente y sala. | Residuo de DEF-95 |
| La poda por TTL y cuota ignora el ancla de retención. | Sin eso un cliente abandonado retendría el log para siempre; se señala con `BehindCompaction`. | §5.2 *Snapshot Retention Anchor* |
| Un cliente `Disconnected` sin `dormant_after_secs` frena la poda proactiva hasta el TTL. | Decisión de configuración. | §5.2 *Dormant* |
| Los tokens no se pueden revocar y valen para una sala recreada con el mismo id. | Tokens stateless; la encarnación (D58) protege los cursores. | §6 |
| El token se acepta en la query de `/events`. | `EventSource` no puede mandar headers. | D15, §7.2 |
| Sin TLS propio. | Lo termina el reverse proxy. | D45, §7.3 |
| Un fsync por commit. | Durabilidad antes que throughput. | D4 |
| Un crash pierde hasta ~1 s del roster. | Solo cuesta retención, re-registros o snapshots de más (INV-R3). | D56 |
| La designación del uploader y la marca de hueco viven en memoria. | Se recalculan al reabrir. | §5.2 *Damaged Segments*, *Snapshot Demand* |
| En Windows el sync de directorios es un no-op. | NTFS registra los cambios de directorio en su journal. | §5.2 *Durable directory entries* |
| Un stream SSE sigue recibiendo eventos después de que vence su token, y mientras tanto mantiene la sala abierta. | SSE solo notifica; cualquier dato se pide con un token vigente. | D72 |
| En Linux, después de un EIO en un `fsync`, el kernel puede marcar las páginas como limpias sin haberlas escrito, y un `fsync` desde un descriptor nuevo devuelve éxito igual: INV-D7 no cubre ese caso. | La solución completa (reescribir `active.wal` cuando el actor anterior se detuvo por una escritura fallida) queda para el futuro. | D77 |
| Un valor escrito por un miembro que embebe un frame WAL válido puede hacer que una escritura rota se clasifique como corrupción: la sala no abre, pero no se pierde ni se inventa nada. | Evitarlo exige rediseñar el framing. La absorción atómica (D85) quita el caso más expuesto. | D79, `ARCHITECTURE.md` §8 |
| Un daño en el último frame de `active.wal`, sin frames válidos después, se trunca como una escritura rota y su secuencia puede reutilizarse, aunque el roster (`last_ack_seq`) o el relay (la secuencia del snapshot activo) la tengan: un snapshot en esa secuencia vuelve a ser usable con el efecto viejo, y un cliente con ese cursor no recibe la operación nueva. | No se distingue de una escritura rota. INV-D8 lo cubre cuando la secuencia consta en otro archivo del log, y su lista de archivos es cerrada. | D78, D95 |
| En storage, un daño en el último frame de `.wal.compacting` se trunca como una escritura rota: si el WAL activo no tiene lotes posteriores, ese lote se pierde sin aviso (si los tiene, la apertura falla por el hueco). | Como `.wal.compacting` toma su nombre después de sincronizarse, solo puede venir de un daño accidental en un lugar preciso. Es el gemelo de D78 en el storage. | D95 |
| Si el runtime de Tokio se apaga con un `open_room`, un `apply_snapshot` o una compactación del storage en vuelo, y la misma sala se reabre en el mismo proceso antes de que termine su hilo bloqueante, ese hilo puede pisar archivos de la sala reabierta y perder lotes confirmados sin aviso. No pasa en wasm32, ni al salir del proceso (es un crash), ni al soltar un future (D86, D89). | Quien maneja el runtime cierra las salas con `close_room`, que espera a esas tareas, antes de apagarlo. Es un requisito de diseño del SDK de la Fase 4. | D93, `ARCHITECTURE.md` §8 *Room Lock* |
| En storage, si se pierde el `.snap` después de una compactación que vació el WAL, la sala abre vacía en la secuencia 0. | El cliente vuelve a bajar todo del servidor; cómo detecta la pérdida lo decide el SDK de la Fase 4. | D96 (propuesta S7 de la 2ª auditoría) |

---

## 5. Mantenimiento de este documento

- **Cuándo cambia:** cuando una decisión de diseño agrega, quita o debilita una garantía, se actualiza en el mismo commit que la implementa, igual que `ARCHITECTURE.md`.
- **Quién lo cambia:** un auditor puede proponer una invariante nueva (§1.2), pero agregarla es una decisión del dueño del proyecto.
- **Tests de propiedades:** las invariantes marcadas *Automatizable* son candidatas a proptest, fuzzing o simulación. Una invariante cubierta por una de esas redes puede auditarse leyendo la red en lugar de recorrer el código entero.
