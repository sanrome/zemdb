# ROADMAP.md: Hoja de Ruta de ZemDB

**Proyecto:** `ZemDB` (Motor de Base de Datos Distribuida Local-First)  
**Fecha de Actualización:** 27 de Octubre de 2026  
**Documentos Relacionados:** [README.md](README.md) | [ARCHITECTURE.md](ARCHITECTURE.md) | [Invariantes](docs/INVARIANTES.md) | [Metodología](docs/METODOLOGIA.md) | [Plan Fase 4](docs/proposals/2026-10-fase4-plan.md)  
**Estado:** Fases 1 a 3.11 Completadas / Fase 4 En curso (sub-fases 4.0 y 4.1 cerradas, 4.2 en curso) / Fase 5 Pendiente

Este documento es el índice de fases y el backlog para después de la v0.1. Lo que construye cada fase, en qué orden y cómo se audita vive en su plan (`docs/METODOLOGIA.md` §1); el estado vigente del sistema, en [`ARCHITECTURE.md`](ARCHITECTURE.md), y sus garantías, en [`docs/INVARIANTES.md`](docs/INVARIANTES.md). La historia de cada cambio está en los planes y en `git log`.

---

## 1. Fases

| Fase | Qué | Estado | Plan o propuesta |
|---|---|---|---|
| 1 y 1.5 | `zemdb-core`: tipos, esquemas, operaciones, protocolo binario; workspace unificado | ✅ Completada | — |
| 2 (2A y 2B) | `zemdb-storage`: contrato `StorageEngine`, motor en memoria y motor en disco con WAL y snapshots Zstd | ✅ Completada | [Auditoría](docs/audits/2026-09-fase2-audit.md) |
| 2.5 y 2.8 | Reestructuración modular y contratos de core y storage | ✅ Completada | [2.5](docs/proposals/2026-09-fase2_5-hardening-proposal.md), [2.8 y 3](docs/proposals/2026-09-fase3-proposal.md) |
| 3 | `zemdb-server`: actores por sala, secuenciador, log de 4 niveles, HTTP y SSE | ✅ Completada | [Propuesta](docs/proposals/2026-09-fase3-proposal.md), [bitácora](docs/FASE3_TRACKING.md) |
| 3.5 | Blindaje posterior a la Fase 3: catch-up 1-RTT, autenticación, WAL único, relay multipart | ✅ Completada | [Post-Fase 3](docs/proposals/2026-09-post-fase3-proposal.md), [3.5](docs/proposals/2026-09-fase3_5-proposal.md) |
| 3.6 a 3.8 | Remediación de auditorías: ciclo de vida por cursor, límites HTTP, contrato del protocolo | ✅ Completada | [3.6](docs/proposals/2026-10-fase3_6-remediation-plan.md), [3.7](docs/proposals/2026-10-fase3_7-remediation-plan.md), [3.8](docs/proposals/2026-10-fase3_8-remediation-plan.md) |
| 3.9 | Primera auditoría por invariantes y su remediación | ✅ Completada | [Plan](docs/proposals/2026-10-fase3_9-remediation-plan.md) |
| 3.10 | Segunda auditoría por invariantes; cierre por revisión del delta (D90) | ✅ Completada | [Plan](docs/proposals/2026-10-fase3_10-remediation-plan.md) |
| 3.11 | Restricciones de integridad verificadas por las réplicas y dueño de fila | ✅ Completada | [Propuesta](docs/proposals/2026-10-fase3_11-restricciones-proposal.md) |
| 4 | `zemdb-client`: SDK nativo en Rust y API de JavaScript para el navegador | ⏳ En curso | [Plan](docs/proposals/2026-10-fase4-plan.md), [4.1](docs/proposals/2026-10-fase4_1-storage-servidor.md), [4.2](docs/proposals/2026-10-fase4_2-nucleo-replica.md) |
| 5 | Pruebas de integración de extremo a extremo (§2) | ⏳ Pendiente | — |

**Lo que sigue:** la sub-fase 4.2 del [plan de la Fase 4](docs/proposals/2026-10-fase4-plan.md), el núcleo de la réplica contra un transporte abstracto, en curso con su [plan por lotes](docs/proposals/2026-10-fase4_2-nucleo-replica.md); cierra con la auditoría amplia del SDK (DC3). La 4.1, lo que el SDK necesita de `zemdb-core`, `zemdb-storage` y `zemdb-server`, cerró el 25 de Octubre de 2026 ([plan](docs/proposals/2026-10-fase4_1-storage-servidor.md)).

**Fuera de las fases:** las [evaluaciones de performance](docs/proposals/2026-10-evaluaciones-performance.md) del servidor, en paralelo a la Fase 4. La primera mide el commit con los clientes al día.

---

## 2. Fase 5: Pruebas de Integración de Extremo a Extremo

Tendrá su propio plan, que se escribe al terminar la Fase 4. Lo que se espera de ella:
* **Suite de integración cliente-servidor:** varias instancias del cliente contra un `zemdb-server`, sobre la app de referencia de la Fase 4 (§6 de su plan).
* **Tolerancia a particiones y modo offline:** clientes que operan sin red en solo lectura, rechazo controlado de escrituras offline (`ClientError::Offline`), reconexión y recepción ordenada de lo acumulado sin desincronización.
* **Estrés y límites de carga:** la barrera de 16 MB con paquetes maliciosos, paginación con deltas masivos y retención bajo saturación.

---

## 3. Backlog para Después de la v0.1

Para optimizar la velocidad de desarrollo y evitar sobre-ingeniería prematura en el MVP, los siguientes elementos identificados durante la evaluación quedan formalmente diferidos para versiones posteriores a la v0.1:

1. **`DataType::Decimal` / `Value::Decimal`:**
   - *Razón de diferimiento:* Su incorporación es 100% aditiva. Las necesidades numéricas actuales quedan cubiertas con `Int(i64)` y `Float(f64)`. Se sumará como nueva variante del enum cuando surjan casos de uso financieros o contables.
2. **Red P2P Pura y Relay Descentralizado de Snapshots:**
   - *Razón de diferimiento:* Mecanismo complejo de sincronización ad-hoc entre clientes sin pasar por servidor. Para la arquitectura de salas de ZemDB, el servidor coordinador actúa como intermediario eficiente y seguro, respondiendo limpiamente con `ErrorCode::BehindCompaction` y canalizando snapshots base vía HTTP/2 multipart.
3. **`Buffer Pool` y Paginación en Disco (*Slotted-Pages*):**
   - *Razón de diferimiento:* Las salas de ZemDB manejan de 2 a 50 participantes (5 a 100 MB de datos promedio), entrando holgadamente en la memoria RAM de dispositivos modernos. El índice en RAM con WAL append-only en disco es la solución idónea para esta escala.
4. **Integridad Criptográfica Adicional con BLAKE3:**
   - *Razón de diferimiento:* La biblioteca Zstandard (`zstd`) ya incluye sumas de verificación de integridad (checksums) nativas por bloque y frame en la descompresión, y el WAL implementa CRC32 por lote, haciendo redundante una tercera capa criptográfica en esta etapa.
5. **Codificación Memcomparable para Claves Primarias:**
   - *Razón de diferimiento:* Solo necesaria para motores LSM o B-Trees que ordenen bytes crudos directamente en páginas de disco sin deserializar. En ZemDB, las búsquedas e índices residen en memoria RAM y comparan en $O(1)$ sin alocaciones con `type_order`.
6. **CRDT Celular (`FieldMutation` con timestamps por columna):**
   - *Razón de diferimiento:* La arquitectura adopta formalmente la autoridad del `sequence_id` del servidor como único árbitro determinista para orden total y resolución Last-Write-Wins (LWW) a nivel de mutación.
7. **Índices Secundarios No Primarios (`BTreeMap<Value, BTreeSet<PrimaryKey>>`):**
   - *Razón de diferimiento:* Para la v0.1 el almacenamiento indexa eficientemente por clave primaria (`PrimaryKey`) mediante `BTreeMap<PrimaryKey, CompactRow>` directo por tabla, soportando búsquedas en $O(1)$ amortizado y escaneos de rangos (`KeyRange`). La gestión de índices secundarios en memoria y su mantenimiento atómico durante compactación y replay de WAL es 100% aditiva y se abordará tras estabilizar la sincronización cliente-servidor. *Nota (Fase 3.11): los índices de las restricciones `UNIQUE` y de las claves foráneas ya existen en el storage, pero son internos; exponer consultas por esos índices es lo que queda en el backlog.*
8. **Escrituras Offline: Outbox o Borradores Explícitos (Opción B: Staged Drafts):**
   - *Razón de diferimiento:* Para la v0.1 se garantiza la integridad estricta del motor de almacenamiento mediante el modelo *Server-Authoritative Write-Through con Sync en 1 RTT*: toda mutación se valida y secuencia primero en el servidor antes de ingresar al almacenamiento canónico (`StorageEngine`). Esto erradica de raíz el riesgo de inconsistencias locales por validaciones fallidas o estados huérfanos. El soporte para retener borradores no confirmados mientras el cliente esté offline es 100% aditivo y se incorporará como un gestor de borradores segregado (`DraftManager` o tabla de staging `__zemdb_drafts`), sin alterar el motor de persistencia canónico ni el protocolo del servidor. El squashing que esa capa usaría para consolidar sus escrituras pendientes antes de enviarlas (`TableBuffer`, `ARCHITECTURE.md` §4.1) ya está en `zemdb-core`, sin consumidor en la v0.1. *Nota (Fase 3.11): con restricciones, mandar lo consolidado deja de ser equivalente a mandar en orden; por ejemplo, `Insert(u = 1)` seguido de `Update(u = 2)` puede dar otro resultado que un único `Insert(u = 2)` si una de las dos claves choca. El gestor de borradores tiene que elegir entre no consolidar las columnas con restricciones o aceptar esa diferencia, y documentarla (`ARCHITECTURE.md` §4.1).*
9. **SDK de Administración Dedicado para Clientes (Admin SDK):**
   - *Razón de diferimiento:* El Control Plane opera sobre HTTP REST estándar con payloads JSON (`/admin/...`), permitiendo aprovisionar esquemas y salas desde cualquier herramienta (`curl`, scripts bash, Postman, o backends en Node/Python/Go/Rust). Si el backend de aplicación está escrito en Rust, puede utilizar directamente la biblioteca del servidor (`zemdb_server::lib`) en modo embebido sin costo de red. Construir un SDK cliente tipado dedicado es un wrapper de conveniencia que se implementará de forma aditiva una vez estabilizados los endpoints HTTP.
10. **Reescritura de `active.wal` después de un EIO (riesgo aceptado, D77):**
    - *Razón de diferimiento:* Al abrir una sala, el servidor hace `fsync` de `active.wal` y de los directorios antes de servir nada, y el storage hace lo mismo con su WAL. En Linux, después de un EIO, el kernel puede marcar las páginas como limpias sin haberlas escrito, y un `fsync` desde un descriptor nuevo devuelve éxito igual: un commit cuyo `fsync` falló podría confirmarse al reintentarlo después de reabrir la sala. La solución completa es reescribir `active.wal` (copiarlo a un temporal, sincronizarlo y renombrarlo) cuando el actor anterior se detuvo por una escritura fallida. Requiere un disco que ya devolvió errores, y está en los riesgos aceptados de `docs/INVARIANTES.md` §4.
11. **Esquema anclado en el log (Fase 3.11, §2.3 de la [propuesta](docs/proposals/2026-10-fase3_11-restricciones-proposal.md)):**
    - *Razón de diferimiento:* Darle a cada cambio de esquema una posición en el log de la sala permitiría agregar restricciones sobre columnas existentes y DDL más rico (borrar o renombrar columnas). Toca el log del servidor, el sync, el WAL del storage, los snapshots y el relay, y retrasaría el cliente una fase entera; la v0.1 no corre en producción con esquemas que evolucionan. DR3 es compatible hacia adelante. Antes de un release es gratis; después, requiere subir `PROTOCOL_VERSION` y migrar logs y WAL.
12. **Dueño de fila seudónimo (Fase 3.11, DR15 y Lote 2b de la [propuesta](docs/proposals/2026-10-fase3_11-restricciones-proposal.md)):**
    - *Razón de diferimiento:* Los votos anónimos no son un requisito de la v0.1. Está especificado: un seudónimo por tabla derivado de un secreto del servidor, que el cliente escribe y el servidor verifica. Es seudonimato frente a los demás miembros, no anonimato frente al operador.
13. **Detección de divergencia entre réplicas (Fase 3.11, DR13):**
    - *Razón de diferimiento:* Con restricciones verificadas por las réplicas, una réplica que decide distinto diverge sin aviso (riesgo aceptado en `docs/INVARIANTES.md` §4). Un digest del estado por secuencia, comparado a través del servidor (por ejemplo, en el heartbeat), lo detectaría.
14. **Bajos pendientes de la Fase 3.11** (de las notas **Hecho** de sus lotes y de su revisión de cierre, en la [propuesta](docs/proposals/2026-10-fase3_11-restricciones-proposal.md); ninguno cambia una decisión de las réplicas):
    - **Columna repetida en la clave primaria** (Bajo, preexistente; Lote 1): `TableSchema::try_new` acepta `"primary_key": ["id", "id"]` en JSON (el builder la deduplica). La clave queda con aridad 2 y se valida igual en todos lados, pero una `UNIQUE ["id"]` sobre esa tabla no se reconoce como igual a la PK.
    - **`add_column` repetidos hacen crecer un esquema sin tope** (Bajo, preexistente; Lote 2), hasta que `Registered` y `Schema` no entran en un frame y fallan para todos los clientes de sus salas. Solo lo provoca el administrador; lo cerraría que el registry rechace un cambio cuyo esquema no entra en una respuesta.
    - **Una sala sin `meta_room.json` pero con log** (Bajo, preexistente; Lote 2 y revisión de cierre): `get_or_spawn` con un esquema, y también `create_room` por HTTP, la tratan como nueva, sin verificar la reasignación, y crean otra encarnación sobre el log viejo, que pasa a decidirse con otro esquema. No diverge, porque los clientes se reinician por la encarnación.
    - **La versión del esquema después de una reasignación** (Bajo, preexistente; Lote 2 y revisión de cierre): el número puede coincidir en el esquema destino. Desde el Lote 6b el destino solo agrega columnas y tablas, así que un cliente que no baja el esquema falla cerrado al validar una operación que escribe una columna nueva.
    - **La copia del esquema por respuesta** (Bajo, preexistente; Lote 2): `Registered` y `Schema` clonan el `Schema` para codificarlo, y esa memoria no la cuenta el presupuesto de respuestas. Lo cerraría que el mensaje lleve el `Arc`.
    - **Liberar fuera del runtime el estado reemplazado** (Bajo, preexistente; Lote 3): un batch o un snapshot aplicados, una apertura fallida o un snapshot descartado sueltan su `RoomData` en el runtime, O(n) si el árbol no está compartido. Se podría mandar el valor viejo al pool bloqueante.
    - **Los bytes de un snapshot dependen del orden de un `HashMap`** (Bajo, preexistente; Lote 5): `RoomSnapshotRef` serializa `&HashMap<u16, Table>`, así que los fuzzers de snapshots no se reproducen por semilla. Se podrían serializar las tablas en orden de id.
    - **Un payload de snapshot que repite una clave primaria o un id de tabla** (Bajo, preexistente; Lote 5) se decodifica quedándose con la última, en forma determinista. Se podría rechazar como `SnapshotCorruption`.
    - **El mensaje de `SnapshotCorruption` por una fila inválida** (Bajo, preexistente; revisión de cierre) nombra una tabla según el orden de un `HashMap` (`decode_received_snapshot` recorre `payload.tables`). Se podría recorrer por id.
    - **Dimensionamiento de los índices** (Bajo, introducido; revisión de cierre): con claves anchas, una entrada de índice ocupa unos 620 bytes, y una tabla al tope (32 `UNIQUE` de 16 columnas) suma unos 19,9 KB por fila, unas 85 veces lo que ocupa su fila en un snapshot: con el tope por defecto de 2 GiB por snapshot, unos 180 GB. Es la constante de una sala legítima, que fija quien escribe el esquema. Queda evaluar un tope de snapshot menor, o documentarlo como límite de dimensionamiento.
    - **Una línea de comentario de 129 columnas** en `crates/storage/src/fail_point.rs` (cosmético, introducido; revisión de cierre).

15. **Tipos de TypeScript generados a partir del esquema (Fase 4, §2.2 del [plan](docs/proposals/2026-10-fase4-plan.md)):**
    - *Razón de diferimiento:* En la v0.1 la aplicación declara a mano los tipos de sus tablas. Generarlos desde el esquema es una herramienta aditiva sobre la API de JavaScript.
16. **Bindings para apps móviles nativas (Fase 4, §2.2 del [plan](docs/proposals/2026-10-fase4-plan.md)):**
    - *Razón de diferimiento:* React Native, Swift y Kotlin no corren el wasm del navegador con OPFS. Necesitarían otra capa sobre el SDK de Rust, con su propio storage.
17. **Replicación del servidor por sala y durabilidad relajada (§3.8 del [plan de evaluaciones de performance](docs/proposals/2026-10-evaluaciones-performance.md)):**
    - *Qué:* hoy cada commit se confirma después de su `fsync`. Hay dos formas de no esperar al disco. (a) **Replicación**, al estilo de Kafka: el líder de una sala confirma cuando otra réplica (otro servidor) tiene el commit en memoria, y el paso a disco queda a cargo del sistema operativo o de un intervalo configurable (por ejemplo, 20 ms), que solo importa si caen todas las réplicas a la vez. Las salas son independientes, así que se reparten naturalmente entre servidores. (b) **Durabilidad relajada** en un solo servidor: confirmar antes del `fsync` y sincronizar cada tanto, aceptando perder esa ventana de commits confirmados en un corte.
    - *Lo que exige:* (a) un líder por sala que asigne las secuencias, elegido por consenso (del estilo de Raft) para que dos servidores nunca secuencien la misma sala a la vez, y que la réplica que toma su lugar tenga todo lo confirmado. La confirmación pasa a esperar un viaje por la red entre servidores, de décimas de milisegundo en un mismo datacenter, comparable al `fsync` de un buen disco: su ventaja principal es la disponibilidad (seguir andando si cae un servidor), más que la velocidad. (b) Que el servidor sepa si se apagó limpio y, si no, cambie la encarnación de las salas que tuvieron escrituras en la ventana, para que todas las réplicas lo detecten y se descarten (INV-K3) en lugar de divergir sin aviso (el riesgo aceptado de INV-K1). Un rollback en el cliente, en lugar del descarte, pediría deshacer operaciones ya aplicadas, y las escrituras perdidas, que el usuario vio confirmadas, podrían fallar al reenviarse (por ejemplo, por una restricción `UNIQUE`).
    - *Razón de diferimiento:* con los números de la Evaluación 1, en el caso típico el `fsync` de cada sala no limita (una sala con 10 commits por segundo usa menos del 1% de su techo), y el *group commit* resolvería las salas con mucha carga sin perder la garantía. Se revisa con los resultados de la Evaluación 2, si el total de `fsync` por segundo del disco resulta ser el límite.
