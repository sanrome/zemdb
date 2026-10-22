# SDK del cliente (`zemdb-client`) — Plan de la Fase 4

**Fecha:** 21 de Octubre de 2026
**Tipo:** Feature. Documento vivo: plan de la fase (plan maestro) y seguimiento. Cada sub-fase tiene su propio plan por lotes, que se escribe justo antes de arrancarla.
**Estado:** Propuesta. Ninguna sub-fase empezada.
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) y [`ARCHITECTURE.md`](../../ARCHITECTURE.md).
**Metodología:** [`docs/METODOLOGIA.md`](../METODOLOGIA.md).
**Catálogo de lo que hay que construir:** el Apéndice A. **Índice de fases y backlog:** [`ROADMAP.md`](../../ROADMAP.md).
**Lugar en el roadmap:** después del cierre de la [Fase 3.11](2026-10-fase3_11-restricciones-proposal.md) y antes de la Fase 5 (pruebas E2E).

---

## 0. Resumen

- **Qué.** El SDK del cliente: una réplica local completa de cada sala, con escrituras *write-through* (el servidor secuencia y la réplica decide, §A.4), lectura local y sin red, reactividad (`watch`), onboarding desde snapshots y subida de snapshots cuando el servidor lo designa. Corre nativo, con una API de Rust, y en el navegador, con una API de JavaScript con tipos de TypeScript sobre el mismo SDK compilado a wasm (DC14). Persiste en los dos.
- **Qué no.** Escrituras offline (outbox o borradores), transacciones de varias filas, el SDK de administración, el dueño seudónimo, el esquema anclado en el log y los bindings para apps móviles nativas: todo eso queda para después de la v0.1 (§2.2).
- **Cómo se organiza.** Ocho sub-fases (§4): el contrato primero (4.0), lo que el storage necesita (4.1), el núcleo de la réplica sin red real (4.2), el transporte nativo (4.3), la API de Rust (4.4), el navegador del lado de Rust (4.5), la API de JavaScript (4.6) y el cierre (4.7).
- **Cómo se audita.** Revisión de delta en cada lote, revisión de cierre en cada sub-fase y una auditoría amplia por invariantes del SDK al cerrar el núcleo (4.2), no al final de la fase (DC3, §5).
- **Cómo se acepta.** Una app de referencia, el ranking por votos (§6), que se define en 4.0, se usa como escenario de la simulación desde 4.2, es el criterio de aceptación de la API de Rust en 4.4 y de la de JavaScript en 4.6, y es la base de las pruebas E2E de la Fase 5.

---

## 1. Punto de partida

- **Lo que existe.** `zemdb-core` (tipos, esquema con restricciones, protocolo), `zemdb-storage` (motores en memoria y en disco, `ApplyReport`, verificación de restricciones) y `zemdb-server` están cerrados y dentro del alcance auditado. `zemdb-client` es un stub.
- **Los requisitos heredados:**
  - **D93 (Fase 3.10):** cerrar las salas del storage antes de apagar el runtime de Tokio.
  - **§7 de la Fase 3.11:** el resultado de cada escritura sale del `ApplyReport` de su secuencia; el SDK completa el dueño de fila; pre-chequeo local; `watch` no emite por operaciones rechazadas; y cuatro requisitos de diseño abiertos (el reporte perdido ante un crash, las restricciones entre dueños distintos, un snapshot local que no cumple las restricciones y el esquema con que el SDK abre y aplica).
- **Los pendientes que se deciden o se cierran acá:** DEF-34 (storage no recarga el esquema de una sala abierta), DEF-54 (Zstd en wasm32), DEF-85 (resultado incierto de un commit fuera de la ventana de dedup) y D96 (detectar la pérdida del `.snap`), todos en `INVARIANTES.md` §4.

---

## 2. Alcance

### 2.1. Entra
Todo lo que lista el Apéndice A: fachada pública, CRUD y escaneos, pipeline de escritura con resultado, `SyncWorker`, onboarding y subida de snapshots, metadatos durables, `watch`, E2EE, transporte nativo y del navegador, persistencia en el navegador, la API de JavaScript, y las invariantes y la auditoría del SDK.

### 2.2. No entra
- **Escrituras offline** (outbox o borradores, `ROADMAP.md` §3, punto 8). Sin red, el cliente solo lee.
- **Transacciones de varias filas:** el protocolo commitea una operación por mensaje.
- **El SDK de administración, el dueño seudónimo y el esquema anclado en el log** (`ROADMAP.md` §3).
- **La detección de divergencia entre réplicas** (digest de estado, backlog de la Fase 3.11).
- **Bindings para apps móviles nativas** (React Native, Swift, Kotlin): no corren el wasm con OPFS y necesitarían otra capa.
- **Tipos de TypeScript generados a partir del esquema.** En la v0.1 la aplicación los declara a mano.

---

## 3. Decisiones

Prefijo **DC**. Las *Propuestas* se confirman con el dueño del proyecto antes del código que depende de ellas, la mayoría en 4.0.

| ID | Decisión | Sub-fase | Estado |
|---|---|---|---|
| **DC1** | **Write-through sin outbox en la v0.1.** El storage local guarda solo operaciones secuenciadas por el servidor; sin red el cliente solo lee. | — | Adoptada (Apéndice A.4) |
| **DC2** | **Dos niveles de plan.** Este plan maestro y un plan por sub-fase, escrito justo antes de arrancarla, con la metodología común de `docs/METODOLOGIA.md`. | — | Adoptada (21 de Octubre de 2026) |
| **DC3** | **Auditoría amplia al cerrar el núcleo.** El SDK entra al alcance auditado con una auditoría amplia por invariantes al cerrar 4.2 (`INVARIANTES.md` §1.4), porque el transporte, las APIs y el navegador se construyen encima del núcleo. Las invariantes que 4.2 todavía no implementa quedan 🆕 y las audita la revisión de cierre que las toque. 4.7 es una revisión de cierre del delta completo de la fase, no otra auditoría amplia. | — | Adoptada (21 de Octubre de 2026) |
| **DC4** | **App de referencia: el ranking por votos** (§6), el formato de la app de rankings entre amigos en la que se originó el proyecto. El esquema y los escenarios se fijan en 4.0. | 4.0 | Adoptada (21 de Octubre de 2026); el esquema, Propuesta |
| **DC5** | **Cambio de esquema en una sala abierta (DEF-34).** `reload_schema` en el storage: verifica que el esquema nuevo extienda al actual con las mismas restricciones y arma los índices de las restricciones que nacen con las columnas nuevas, sin cerrar la sala. Alternativa: cerrar y reabrir la sala, que no toca el storage pero corta lecturas y `watch` y reconstruye todos los índices. | 4.1 | Propuesta |
| **DC6** | **Resultado incierto de un commit (DEF-85).** En la v0.1, el SDK reintenta con el mismo `MutationId` durante un plazo acotado y, si no confirma, devuelve un error de resultado incierto; nunca reenvía con un id nuevo, y el estado final llega por sync. Que las respuestas de sync lleven la clave de dedup de cada operación, para que el cliente reconozca su propio commit, queda para la outbox. | 4.2 | Propuesta |
| **DC7** | **Un único aplicador por sala.** Todo lote (catch-up de un commit, sync, snapshot) entra al storage por un solo camino, que descarta el prefijo ya aplicado, entrega el `ApplyReport` a la escritura pendiente que lo espera (venga el lote por donde venga) y conserva los rechazos recientes. Si los rechazos tienen que sobrevivir a un crash (el reporte perdido, §7 de la Fase 3.11), se guardan en el storage junto con el batch, y eso entra en 4.1. | 4.1 / 4.2 | Propuesta |
| **DC8** | **Persistencia en el navegador.** Un motor sobre el *Origin Private File System* con `FileSystemSyncAccessHandle`, dentro de un Web Worker dedicado, reusando el formato de archivos del motor en disco detrás de una capa de archivos. Alternativa: IndexedDB. Sujeta al spike S1. | 4.0 / 4.1 / 4.5 | Propuesta |
| **DC9** | **Zstd en wasm32 (DEF-54).** Un decodificador en Rust puro (`ruzstd`) para restaurar snapshots nativos; el navegador sigue subiendo snapshots sin comprimir. Sujeta al spike S2. | 4.0 / 4.1 | Propuesta |
| **DC10** | **Invariantes del SDK** en un grupo nuevo de `INVARIANTES.md` (§3.11, *Cliente*), con prefijo `INV-K` (`C` ya es de las restricciones). | 4.0 | Propuesta |
| **DC11** | **Modelo de amenaza del cliente.** Se confía en la aplicación que usa el SDK, en el disco local (los supuestos de `INVARIANTES.md` §2) y en el servidor como secuenciador. No se confía en los demás miembros (sus operaciones y sus snapshots), en la red (respuestas perdidas, cortadas o demoradas, eventos SSE perdidos) ni en el reloj. Un snapshot falso pero bien formado sigue fuera de alcance (DEF-81). | 4.0 | Propuesta |
| **DC12** | **Un snapshot local que no cumple las restricciones** (§7 de la Fase 3.11). Una variante propia de `StorageError`, distinta de `SnapshotCorruption`, para que el SDK falle cerrado sin descartar la réplica. | 4.1 | Propuesta |
| **DC13** | **Restricciones entre dueños distintos** (§7 de la Fase 3.11). Documentarlo para quien escribe el esquema (por ejemplo, que una FK hacia una tabla con dueño incluya la columna dueño), sin regla nueva en la v0.1. | 4.0 | Propuesta |
| **DC14** | **API de JavaScript sobre un Worker.** En el navegador, el SDK compilado a wasm corre en un Web Worker dedicado (donde el OPFS da archivos síncronos) y la página lo usa a través de un paquete con tipos de TypeScript que le habla por mensajes. El paquete vive en este repo y se versiona con el resto. En 4.0 se fijan sus principios: el núcleo (4.2) y la API de Rust (4.4) se expresan como comandos y respuestas serializables, y las suscripciones (`watch`) por id, sin referencias que crucen el límite; las filas son objetos con nombres de columna; `Bytes` es `Uint8Array` y `Uuid` un string; los errores llevan un código que mapea `ClientError`; `watch` se puede usar desde React (`useSyncExternalStore`). Queda por decidir cómo se representa `Int(i64)`: `bigint`, o `number` con verificación de rango (por encima de 2⁵³ pierde precisión). | 4.0 / 4.5 / 4.6 | Propuesta |

---

## 4. Sub-fases

| Orden | Sub-fase | Qué entrega | Depende de | Cierra con | Estado |
|---|---|---|---|---|---|
| 4.0 | Contrato y decisiones | Invariantes del SDK, modelo de amenaza, decisiones, spikes, diseño del arnés de simulación, app de referencia | — | Revisión cruzada del contrato | ⬜ |
| 4.1 | Storage para el cliente | Lo que el SDK necesita del storage | 4.0 | Revisión de cierre | ⬜ |
| 4.2 | Núcleo de la réplica | La réplica por sala contra un transporte abstracto | 4.1 | **Auditoría amplia** (DC3) | ⬜ |
| 4.3 | Transporte nativo y `SyncWorker` | HTTP, SSE, reconexión, heartbeat | 4.2 | Revisión de cierre | ⬜ |
| 4.4 | API de Rust | Fachada, `watch`, dueño, pre-chequeo, E2EE, errores | 4.2 | Revisión de cierre y app de referencia nativa | ⬜ |
| 4.5 | Navegador (Rust) | Motor del navegador, transporte del navegador, runtime del Worker, pestañas | 4.3, 4.4 | Revisión de cierre | ⬜ |
| 4.6 | API de JavaScript | Paquete con tipos de TypeScript sobre el Worker | 4.5 | Revisión de cierre y app de referencia en el navegador | ⬜ |
| 4.7 | Cierre | Revisión de cierre de la fase y documentos | 4.6 | Revisión de cierre del delta completo | ⬜ |

4.3 y 4.4 dependen solo de 4.2 y no tocan los mismos archivos: pueden ir en paralelo. El motor del navegador (4.5) depende de 4.1 y del spike S1, no del resto, así que se puede adelantar si 4.2 lo permite.

### 4.0 · Contrato y decisiones
Solo documentos y spikes (`docs/METODOLOGIA.md` §2 y §3).
- **Invariantes del SDK (DC10).** Candidatas, a precisar con enunciado, núcleo (★), severidad, cómo romperlas y si son automatizables:
  - ★ la réplica local es siempre un prefijo del log de la sala: nunca aplica fuera de orden ni algo que el servidor no secuenció;
  - ★ cada escritura se resuelve una sola vez, con la decisión de la réplica en su secuencia, o con un error que dice que el resultado es incierto (DC6);
  - ★ el SDK nunca descarta una réplica local sana: solo ante `RoomReset` o un daño verificado (DC12);
  - ★ las réplicas de clientes que siguen el protocolo convergen (INV-Y8 de punta a punta);
  - un cursor nunca se presenta con otra encarnación;
  - un cliente que se queda atrás termina al día, también cuando el designado para subir el snapshot es él;
  - el SDK nunca manda una operación con un dueño ajeno ni una columna dueño escrita por la aplicación;
  - una columna cifrada nunca sale en claro;
  - cerrar o apagar el cliente no pierde nada confirmado (D93);
  - ninguna respuesta del servidor ni snapshot de un par hace entrar en pánico al SDK;
  - la API de JavaScript no cambia ningún valor al cruzar el límite del Worker (DC14).
- **Modelo de amenaza (DC11)** en `INVARIANTES.md` §2, que deja de decir que el SDK está fuera de alcance.
- **Decisiones DC4 a DC14** confirmadas.
- **Spikes:**
  - **S1:** desde una página armada con Vite, cargar un Worker con Rust compilado a wasm32 y, desde ahí, abrir un archivo del OPFS con `FileSystemSyncAccessHandle`, escribir, `flush`, `truncate` y reabrir; qué pasa con dos pestañas sobre el mismo archivo; qué da `navigator.storage.persist()`; cuánto pesa el wasm. En Chrome, Firefox y Safari.
  - **S2:** restaurar en wasm32 con `ruzstd` un snapshot hecho por `DiskStorageEngine`, y cuánto tarda con uno grande.
- **Arnés de simulación:** el diseño (no el código) de las pruebas del §5.2: varios clientes contra el servidor real en proceso, con un transporte que inyecta fallas y el reloj pausado.
- **App de referencia (DC4):** esquema y escenarios (§6).

### 4.1 · Storage para el cliente
En `zemdb-storage`, dentro del alcance auditado.
- `reload_schema` (DC5), con los índices de las restricciones nuevas.
- Los rechazos que sobreviven a un crash, si DC7 lo pide.
- La variante de error de DC12.
- El decodificador Zstd para wasm32 (DC9).
- La capa de archivos que separa el formato del motor en disco de `std::fs`, si DC8 elige reusarlo.

### 4.2 · Núcleo de la réplica
En `zemdb-client`, contra el trait `TransportClient`, sin red real, con la forma de comandos y respuestas de DC14.
- Registro, encarnación y metadatos durables (§A.9), con la detección de un `.snap` perdido (D96).
- El aplicador único (DC7) y el pipeline de escritura con su resultado (§A.4), con los reintentos de DC6.
- Onboarding y bajada de snapshots (§A.6) y subida como designado (§A.8).
- Cambio de esquema con las reglas de §7 de la Fase 3.11 (extiende al anterior, mismas restricciones; si no, falla cerrado).
- Cierre ordenado de las salas antes de apagar el runtime (D93).
- Se prueba contra el servidor real en proceso (`zemdb-server` como dev-dependency) y con un transporte que inyecta fallas.

### 4.3 · Transporte nativo y `SyncWorker`
- El adaptador `reqwest` sobre HTTP/2.
- El `SyncWorker`: SSE con la reacción a cada evento, pull paginado, `Ack` del último lote, heartbeat y reconexión con *backoff* exponencial y *full jitter* (§A.5).
- La renovación del token ante `Unauthorized`.

### 4.4 · API de Rust
- `ZemdbClient`, `RoomHandle` y `TableHandle`: CRUD, escaneos y `watch` / `ChangeStream` (§A.3 y §A.7).
- Dueño de fila completado por el SDK y pre-chequeo local (§A.4).
- Cifrado de columnas con `CryptoEngine` (§A.10).
- `ClientError` (§A.1).
- **Criterio de aceptación:** la app de referencia nativa, en `crates/client/examples/`, corre sus escenarios contra un servidor local.

### 4.5 · Navegador (Rust)
- La feature `wasm` del cliente y su chequeo en la CI (`cargo check --target wasm32-unknown-unknown -p zemdb-client --no-default-features --features wasm`).
- El transporte del navegador (fetch y `EventSource` con `?token=`).
- El motor de DC8.
- El runtime del Worker: recibe los comandos de DC14, los ejecuta sobre la API de 4.4 y publica las respuestas y los eventos de `watch`.
- La coordinación entre pestañas: una sola abre cada sala.
- **Criterio de aceptación:** tests en un navegador headless (por ejemplo, con `wasm-bindgen-test`) en los que la réplica sobrevive a recargar la página.

### 4.6 · API de JavaScript
- El paquete con tipos de TypeScript: abrir el cliente, salas y tablas, CRUD, escaneos y `watch`, con las representaciones de DC14 y errores con código. Dónde vive dentro del repo y cómo se arma (`wasm-bindgen`, `wasm-pack`) lo decide el plan de la sub-fase.
- Que funcione con un bundler común (Vite), incluida la carga del Worker y del wasm.
- Tests del paquete en navegadores headless (por ejemplo, con Playwright) y su paso en la CI.
- **Criterio de aceptación:** la app de referencia, en TypeScript, corre sus escenarios en el navegador, y su réplica sobrevive a recargar la página.

### 4.7 · Cierre
- La revisión de cierre del delta completo de la fase contra todas las invariantes que tocó.
- Los documentos: una sección del cliente en `ARCHITECTURE.md`, `INVARIANTES.md` con el estado de cada invariante del SDK, `ROADMAP.md` y `README.md`.
- El traspaso a la Fase 5: lo que la app de referencia deja listo para las pruebas E2E.

---

## 5. Cómo se audita y cómo se prueba

### 5.1. Revisiones
Las tres de `docs/METODOLOGIA.md` §6:

| Qué | Cuándo |
|---|---|
| Revisión de delta | Cada lote de cada sub-fase |
| Revisión cruzada del contrato | Cierre de 4.0 |
| Revisión de cierre | Cierre de 4.1, 4.3, 4.4, 4.5, 4.6 y de la fase (4.7) |
| Auditoría amplia por invariantes del SDK | Cierre de 4.2 (DC3) |

Si la auditoría de 4.2 no sale en verde, sus rupturas se arreglan en lotes nuevos de 4.2 antes de abrir 4.3 y 4.4.

### 5.2. Redes de prueba del SDK
Las cuatro capas de `docs/METODOLOGIA.md` §7, aplicadas al cliente:
1. **Reglas.** Cada transición de la réplica (registro, `RoomReset`, `BehindCompaction`, snapshot, commit, reintento, cambio de esquema), positiva y negativa.
2. **Modelo de referencia.** El servidor real en proceso hace de referencia del log, y el oráculo del storage (`crates/storage/tests/common/oracle.rs`) de referencia del estado y de las decisiones de cada réplica.
3. **Simulación.** Varios clientes contra el servidor real, con cortes en cada `.await` del cliente, respuestas perdidas o cortadas, eventos SSE perdidos, poda y reinicios del servidor. Al final: todas las réplicas al mismo estado en la misma secuencia, cada escritura resuelta una sola vez con la decisión de la réplica, y ninguna réplica sana descartada. La app de referencia es uno de sus esquemas.
4. **Robustez.** Fuzzing de las respuestas del servidor y de los snapshots recibidos: nunca un pánico, nunca un lote fuera de orden. En el límite del Worker (4.5 y 4.6), ida y vuelta de cada tipo de valor sin cambios.

Las versiones largas van con `#[ignore]` (`docs/METODOLOGIA.md` §4, regla 5).

---

## 6. App de referencia: el ranking por votos (DC4)

Es el formato de la app de rankings entre amigos en el que se originó el proyecto: en cada ronda, cada miembro vota y los votos suman puntos al ranking. Extiende el caso de los votos de la Fase 3.11 (§3.4 de su propuesta) para que ejercite todo lo que el SDK tiene que hacer, incluida una agregación sobre muchas filas (los puntos) que la réplica calcula localmente y `watch` actualiza mientras llegan votos.

Esquema propuesto, a confirmar en 4.0:
- **`rankings(ranking_id, creator, name)`:** dueño en `creator`.
- **`members(ranking_id, member_id, display_name)`:** clave `(ranking_id, member_id)`, dueño en `member_id` (cada uno se suma a sí mismo) y FK `ranking_id → rankings`.
- **`rounds(ranking_id, round_id, opened_by, title, notes)`:** clave `(ranking_id, round_id)`, dueño en `opened_by`, FK `ranking_id → rankings`, `UNIQUE(ranking_id, title)` y `notes` cifrada.
- **`votes(voter, ranking_id, round_id, candidate)`:** clave `(voter, ranking_id, round_id)`, dueño en `voter`, FK `(ranking_id, round_id) → rounds` y `(ranking_id, candidate) → members`, tabla inmutable. Las dos FK comparten `ranking_id`, así que un voto no puede mezclar la ronda de un ranking con un candidato de otro.

Escenarios:
- varios miembros votan en una ronda y todas las réplicas llegan a la misma tabla de posiciones;
- un segundo voto en la misma ronda se rechaza (`Duplicate`), un voto a alguien que no es miembro también (`ForeignKey`), y dos rondas con el mismo título (`Unique`);
- los miembros que se sumaron le impiden al creador borrar el ranking (`Restrict` entre dueños distintos, el caso de DC13);
- un miembro nuevo entra a un ranking con la historia podada y se pone al día desde un snapshot que sube otro miembro;
- sin red, se sigue viendo la tabla de posiciones y no se puede votar;
- `watch` de la tabla de posiciones mientras llegan votos;
- `notes` nunca viaja en claro.

| Sub-fase | Uso |
|---|---|
| 4.0 | Se fijan el esquema y los escenarios |
| 4.2 | Es uno de los esquemas de la simulación |
| 4.4 | Criterio de aceptación de la API de Rust, como ejemplo nativo |
| 4.6 | Criterio de aceptación de la API de JavaScript, en TypeScript en el navegador |
| Fase 5 | Base de las pruebas E2E |

**La app real, aparte.** La app de rankings se rehace en TypeScript sobre la API de JavaScript, en su propio repo, y es el primer usuario real del SDK. Necesita su propio backend, que emita los tokens de cliente y cree los esquemas y salas por el Control Plane. Se valida contra ella en dos hitos (después de 4.4, con ese backend en Rust, y después de 4.6), sin que bloquee el cierre de ninguna sub-fase: lo que aparezca entra a este plan o al backlog con la regla de alcance (`docs/METODOLOGIA.md` §5, paso 6). Depende de este repo por git, fijada a un commit (o por ruta mientras se desarrollan juntos), y sigue de cerca a `main`, porque el protocolo no se versiona antes del primer release (`GEMINI.md`).

---

## 7. Riesgos

- **La auditoría de 4.2 puede pedir rediseño del núcleo.** Es la razón de DC3: encontrarlo antes de construir encima.
- **El navegador.** Las garantías de `flush()` en el OPFS no están especificadas más allá del estándar; el navegador puede borrar el almacenamiento si le falta espacio (salvo `persist()`), y entonces el cliente vuelve a bajar todo; varias pestañas compiten por los mismos archivos. S1 tiene que responder lo que se pueda antes de DC8.
- **Herramientas de JavaScript.** El paquete suma `wasm-bindgen`, npm, un bundler y tests en navegadores a un repo que hasta ahora es solo Rust; el tamaño del wasm y la carga del Worker dependen del bundler, y los tests en navegador pueden ser inestables en la CI.
- **Memoria.** Los índices de las restricciones viven en memoria también en el navegador: una sala de 100 MB de filas con tres restricciones por tabla ocupa unos 330 MB (§8 de la propuesta de la Fase 3.11).
- **Alcance.** El catálogo del Apéndice A es grande. Lo que no esté en un lote no se hace en el lote (`docs/METODOLOGIA.md` §5, paso 6).

---

## 8. Seguimiento

**Avance:** ninguna sub-fase empezada. La próxima es 4.0, cuyo plan es este documento más el Lote 0 de `docs/METODOLOGIA.md` §2.

---

## Apéndice A. Catálogo del SDK

Qué tiene que construir la Fase 4. Hasta el 22 de Octubre de 2026 vivía en `ROADMAP.md` §4.4; las sub-fases del §4 dicen en qué orden se construye.

### A.1. Modularización Física del Crate `zemdb-client`
* **Organización Modular:**
  - `src/lib.rs`: Punto de entrada público del SDK, re-exportando facades ergonómicas y tipos esenciales.
  - `src/config.rs`: `ClientConfig` con builders tipados para endpoints de servidor, intervals de heartbeat, estrategias de retry y credenciales.
  - `src/error.rs`: Enum tipado `ClientError` (ValidationError, NetworkError, StorageError, ConstraintViolation, RowMissing, OfflineError). `ConstraintViolation` y `RowMissing` son el resultado de una escritura que la réplica rechaza (§A.4). Los `ErrorCode` del servidor (tabla de `ARCHITECTURE.md` §7.2) se agrupan por la reacción que les corresponde: rechazo permanente de la operación (`SchemaViolation`, `BadRequest`, en `ValidationError`), credenciales (`Unauthorized`: pedir un token nuevo; `Forbidden`: permanente para ese token) y reintentables (`Unavailable`, `Timeout`, `RequestTimeout`, `RateLimited` y fallas de red, en `NetworkError`). Los códigos que el SDK resuelve solo (`ClientNotRegistered`, `BehindCompaction`, `RoomReset`, `SnapshotSuperseded`) no llegan a la aplicación salvo que fallen sus recuperaciones.
  - `src/api/`: Facade pública de alto nivel:
    - `client.rs`: `ZemdbClient` (gestor global de conexión y catálogo de salas abiertas).
    - `room.rs`: `RoomHandle` (ámbito de sala activa, schemas y despacho de mutaciones).
    - `table.rs`: `TableHandle` (operaciones declarativas CRUD, escaneos con pushdowns y suscripciones reactivas).
  - `src/sync/`: Núcleo de sincronización canónica y consistencia:
    - `syncer.rs`: Cliente de sincronización unificado (Commit & Sync en 1 RTT).
    - `worker.rs`: `SyncWorker` (tarea Tokio en background con consumo de eventos SSE, pulls `/sync` y heartbeats).
    - `reactive.rs`: `ChangeStream` y bus reactivo a tuplas en memoria.
    - `snapshot.rs`: descarga por chunks del snapshot de onboarding (§A.6) y subida del snapshot cuando el servidor designa a este cliente (§A.8).
    - `state.rs`: metadatos durables del cliente por sala (§A.9).
  - `src/transport/`: Abstracción de red desacoplada:
    - `client.rs`: `trait TransportClient`.
    - `native.rs`: Adaptador HTTP/2 persistente basado en `reqwest`.
    - `wasm.rs`: Adaptador WebFetch basado en `web-sys` y `gloo-net` para navegadores.
  - **Paquete de JavaScript** (§A.12): la API del navegador, con tipos de TypeScript, sobre el SDK compilado a wasm. Su ubicación en el repo la decide el plan de la sub-fase 4.6.

### A.2. Feature Flags y Universalidad Multiplataforma
* **Configuración en `Cargo.toml` del cliente:**
  - `default = ["native"]`: Incluye `tokio` (full) y `reqwest`.
  - `wasm = ["web-sys", "wasm-bindgen-futures", "gloo-net"]`: Permite compilar el cliente hacia `wasm32-unknown-unknown` para su ejecución en navegadores sin arrastrar llamadas bloqueantes de sistema operativo.
  - `zemdb-storage` no tiene features: elige su código con `cfg(target_arch = "wasm32")`. En wasm32 compila solo `MemoryStorageEngine`.
* **Persistencia en el navegador (pendiente):** hoy un cliente wasm32 no persiste nada: `MemoryStorageEngine` vive en memoria y `DiskStorageEngine` usa `std::fs`, `flock` y el pool bloqueante de Tokio. La Fase 4 necesita un motor persistente para el navegador. El candidato es el *Origin Private File System* (OPFS) con `FileSystemSyncAccessHandle`, disponible en Chrome, Firefox y Safari: da archivos con escritura, `truncate` y `flush` síncronos, un lock exclusivo por archivo, y funciona solo dentro de un Web Worker dedicado. La alternativa es IndexedDB. El backend se elige en el plan de la fase. Los índices de las restricciones de la Fase 3.11 viven en memoria también en el navegador: una sala de 100 MB de filas con tres restricciones por tabla ocupa unos 330 MB de RAM (§8 de la [propuesta](2026-10-fase3_11-restricciones-proposal.md)).
* **Zstd en wasm32 (DEF-54, pendiente):** wasm32 escribe snapshots sin comprimir y rechaza los Zstd (`ARCHITECTURE.md` §8), así que un navegador no puede restaurar un snapshot subido por un cliente nativo. Hace falta un decodificador Zstd en Rust puro para wasm32 (por ejemplo `ruzstd`).

### A.3. Fachada Pública Ergonómica del SDK
API idiomática, tipada y declarativa para aplicaciones en Rust:
```rust
let client = ZemdbClient::open(config, storage_engine).await?;
let room = client.room(RoomId::new("engineering_team")).await?;
let tasks = room.table("tasks")?;

// 1. Inserción con validación previa en servidor y sync en 1 RTT (Write-Through)
let pk = tasks.insert(row).await?;

// 2. Consulta de ultra-baja latencia sobre almacenamiento canónico local (<1ms)
let task = tasks.get(&pk).await?;

// 3. Escaneo filtrado con pushdowns
let stream = tasks.scan(ScanOptions::default().limit(50)).await?;

// 4. Suscripción reactiva en vivo
let mut live_stream = tasks.watch(&pk).await?;
tokio::spawn(async move {
    while let Some(updated_row) = live_stream.next().await {
        println!("Registro actualizado: {:?}", updated_row);
    }
});
```

En el navegador, la misma API se expone en JavaScript con tipos de TypeScript (§A.12).

Cada `insert`, `update` o `delete` es un commit de una sola operación (`ClientMessage::Commit` lleva una `Operation`): la v0.1 no tiene transacciones de varias filas.

### A.4. Modelo Server-Authoritative con Sincronización en 1 RTT (Write-Through)
* **Persistencia Canónica Estricta en `StorageEngine`:**
  - El motor de persistencia local (`zemdb-storage`) almacena **única y exclusivamente operaciones secuenciadas (`SequencedOperation`)** validadas y ordenadas por el servidor.
  - Esto erradica de raíz el riesgo de inconsistencias locales, tuplas zombi o estados corruptos ante rechazos de validación (aridad, esquemas o tipos). Una operación que viola una restricción de integridad también queda en el log canónico, y cada réplica la rechaza igual al aplicarlo (Fase 3.11), así que tampoco hace divergir a las réplicas.
* **Pipeline de Escritura y Sincronización en 1 RTT:**
  1. **Envío con Cursor Actual:** Al invocar `insert`, `update` o `delete`, el cliente envía la mutación al endpoint `/commit` del servidor junto con su identificador `MutationId` y su cursor actual `last_ack_seq`. En una tabla con dueño, el SDK ya completó el dueño (ver abajo).
  2. **Validación Server-Side (liviana):** El servidor valida lo que no depende del estado: esquema, tipos, nulabilidad, que el dueño que declara la operación sea el cliente autenticado y que una tabla inmutable no reciba `Update` ni `Delete`. Si alguna validación falla, retorna `ErrorCode::SchemaViolation` (permanente para esa mutación; no `Forbidden`, porque el token es válido), no se secuencia nada y el almacenamiento local **permanece intacto y consistente**. El servidor no guarda filas, así que no valida `UNIQUE`, las claves foráneas (`RESTRICT` incluido), el segundo `Insert` de una clave de una tabla inmutable (`Duplicate`) ni el dueño de la fila existente (`NotOwner`): los decide la réplica (paso 4).
  3. **Secuenciación y Catch-Up Atómico:** Si la mutación es válida, el servidor asigna el `SequenceNumber` canónico y devuelve `CommitAck` conteniendo dicho número junto con cualquier delta remoto pendiente acumulado desde `last_ack_seq`.
  4. **Aplicación en Storage y Resultado de la Escritura:** El cliente aplica los deltas canónicos con `StorageEngine::apply_batch`, que devuelve un `ApplyReport`: el `head_seq` de la sala y las operaciones que no aplicó, con su motivo. La escritura se resuelve cuando la réplica aplicó hasta la secuencia asignada: si el catch-up no llegó hasta ahí (`has_more`, o un acuse sin catch-up por falta de presupuesto), el cliente sigue con `/sync`. El resultado sale del `ApplyReport` del lote que contiene esa secuencia: `Ok`, `ConstraintViolation { constraint, kind }` (por `Unique`, `ForeignKey`, `Restrict`, `Duplicate` o `NotOwner`) o `RowMissing` (un `Update` de una fila que no existe).
  5. **Notificación Reactiva:** Se notifican a los streams activos (`watch`) los cambios aplicados; el `ChangeStream` no emite nada por una operación rechazada.
* **El Dueño de Fila lo Completa el SDK:** La aplicación no escribe el dueño. El SDK pone el `ClientId` del cliente en la columna dueño: en la fila del `Insert` o, si la columna es parte de la clave primaria, en la clave, de la que la aplicación da el resto. Con el dueño fuera de la clave, agrega el delta del dueño a cada `Update` y el `owner` a cada `Delete`. Si la aplicación escribe la columna dueño a mano, el SDK rechaza la llamada localmente con un error claro, sin mandar nada: no la pisa en silencio.
* **Pre-Chequeo Local:** Opcional y activado por defecto: antes de mandar, verifica la escritura contra el estado local. Es solo un aviso temprano, porque la réplica local puede estar atrasada; la decisión es la de la réplica al aplicar el log en orden.
* **Requisitos de Diseño Abiertos (§7 de la [propuesta de la Fase 3.11](2026-10-fase3_11-restricciones-proposal.md)):**
  - **El reporte perdido ante un crash:** el cliente conoce el resultado de una escritura al volver a sincronizar solo si guardó la secuencia asignada de su mutación pendiente, y ni siquiera así si el crash (o un pánico, o un future soltado) llega después de que la réplica hizo durable el batch y antes de que entregara el `ApplyReport`: la reapertura decide igual pero descarta el reporte, reenviar el batch es `SequenceMismatch` y un sync no lo vuelve a entregar. El SDK necesita otra fuente para esa decisión, por ejemplo guardar los rechazos en el storage junto con el batch, o recalcularla.
  - **Restricciones entre dueños distintos:** las reglas valen igual para todos los miembros. Uno puede ocupar primero una clave `UNIQUE`, y una fila propia que referencia la fila de otro (que ese otro no puede borrar, `NotOwner`) le impide borrar la suya (`RESTRICT`). El diseño decide si alcanza con documentarlo para quien escribe el esquema (por ejemplo, que una FK hacia una tabla con dueño incluya la columna dueño) o si hace falta una regla nueva.
  - **Un snapshot local que no cumple las restricciones:** `open_room` lo rechaza con `SnapshotCorruption`, la misma variante que un archivo dañado, pero no es daño: solo pasa si la sala se abre con restricciones con las que sus datos no se escribieron. El SDK no debe reaccionar descartando la réplica local para resincronizar, porque perdería datos intactos y escrituras pendientes; el diseño decide si alcanza con distinguirlo por el mensaje o hace falta una variante propia.
  - **El esquema con que el SDK abre y aplica:** `open_room` y `apply_snapshot` aceptan cualquier esquema que tenga las tablas de la sala. El SDK abre solo con el esquema que le sirve el servidor y, ante un cambio, exige que el nuevo extienda al viejo con las mismas restricciones (`Schema::extends` y `Schema::has_same_constraints`), y si no, falla cerrado.
  - **Borradores offline (post-v0.1):** con restricciones, mandar lo consolidado deja de ser equivalente a mandar en orden (ver `ROADMAP.md` §3, punto 8).
* **Cero Complejidad de Rebase / Rollback:**
  - Al no aplicar escrituras optimistas sin confirmar sobre las tablas canónicas, se elimina la necesidad de motores de rollback/rebase en el cliente.
* **Comportamiento Offline (Modo Solo Lectura):**
  - En ausencia de conexión a red, el cliente opera en modo **Solo Lectura** de alta velocidad sobre su réplica local en `StorageEngine`. Cualquier intento de escritura sin red retorna inmediatamente `Err(ClientError::Offline)`.
  - *(Nota: la v0.1 no tiene outbox. La capacidad de almacenar escrituras offline no confirmadas de forma segregada queda formalmente diferida para post-v0.1, ver `ROADMAP.md` §3).*

### A.5. Worker de Sincronización en Segundo Plano con Jitter (`SyncWorker`)
* **Escucha Reactiva de SSE (opcional):**
  - Mantiene una conexión Server-Sent Events hacia `GET /rooms/{room_id}/events`, con el token en `Authorization: Bearer` o, en el navegador, en `?token=` (`EventSource` no puede mandar headers). Cada evento tiene su reacción:
    - `head_advanced` (dato: el nuevo head): pull paginado a `/sync`.
    - `schema_reloaded` (dato: el id del esquema): `GetSchema` y cambio del esquema local, con las reglas de §A.4 (*El esquema con que el SDK abre y aplica*).
    - `snapshot_wanted` (sin dato): un heartbeat, cuyo `snapshot_wanted` dice si este cliente es el designado (§A.8).
    - `snapshot_available` (dato: su secuencia): si el cliente espera un snapshot, empieza la descarga (§A.6).
    - `lagged`: el stream perdió eventos; el worker hace un `/sync`.
  - SSE solo acelera: las respuestas a heartbeats, syncs y commits son la fuente de verdad, y el worker funciona igual sin stream. Un cliente tiene como mucho 2 streams por sala: un tercero cierra el más viejo.
* **Avance del cursor:**
  - El cursor del cliente en el servidor avanza solo con lo que el cliente confirma: el `from_seq` de cada `/sync` servido, el `last_ack_seq` de cada commit aceptado y `ClientMessage::Ack`. Después de aplicar el último lote (`has_more: false`), el worker manda `Ack` con su head local, para que la poda del servidor no espere a su próximo sync.
* **Reconexión Resiliente:**
  - Si la conexión se interrumpe, el worker utiliza un algoritmo de reconexión con *Exponential Backoff* y *Full Jitter*, mitigando el riesgo de tormentas de reintentos (*thundering herd*) contra el servidor.
* **Heartbeat Continuo:**
  - Emite periódicamente `ClientMessage::Heartbeat`, con un intervalo menor que el `lease_timeout` de la sala (90 s por defecto), para renovar su lease e impedir que el servidor lo pase a `Disconnected`. El heartbeat no lleva cursor ni lo mueve. Su respuesta, `HeartbeatAck`, informa `current_head_seq`, `snapshot_wanted`, `active_snapshot_seq` y `schema_version`.

### A.6. Onboarding de Salas y Descarga Multipart de Snapshots
El camino lo decide el registro (`ARCHITECTURE.md` §6):
1. **Registro.** El cliente manda `RegisterClient` con su cursor (`current_seq`) y la encarnación a la que pertenece (`known_incarnation`) si tiene estado local de la sala, o sin ninguno de los dos si no tiene.
2. **`RoomReset` (409).** La sala se borró y se volvió a crear con el mismo id. El cliente descarta su estado local de la sala y se registra sin cursor, como uno nuevo.
3. **`Registered`.** Trae `head_seq`, `tail_seq`, `active_snapshot_seq`, el esquema y `room_incarnation`, que el cliente guarda junto con su cursor (§A.9); abre la sala con ese esquema (§A.4). Si su cursor (0 sin estado local) es mayor o igual que `tail_seq - 1`, sincroniza deltas con `/sync` desde el cursor. Si no, está `Bootstrapping` y necesita un snapshot.
4. **Snapshot.** Si `active_snapshot_seq` es `Some(S)`, lo descarga, siempre por chunks (`ClientMessage::RequestSnapshotChunk`, de 64 KiB a 4 MiB; el primero sin `snapshot_hash` y los demás con el hash de la primera respuesta). Ante `SnapshotSuperseded` (409) descarta lo recibido y vuelve al chunk 0. Al completar la descarga verifica el BLAKE3, aplica el snapshot con `StorageEngine::apply_snapshot` pasando `S` como secuencia esperada (que también verifica las restricciones) y sincroniza desde `S`. Si no hay snapshot usable, sigue con heartbeats hasta que `HeartbeatAck` (o el evento `snapshot_available`) anuncie uno.
5. **`BehindCompaction` (410) más adelante.** Un sync, commit o ack con un cursor que quedó detrás del log (por ejemplo, por una poda por TTL entre el registro y el primer sync) lleva al paso 4.
6. **`ClientNotRegistered` (409).** El servidor no lo tiene en su roster: vuelve al paso 1 con su cursor.

### A.7. Bus de Suscripciones Reactivas en Vivo (`ChangeStream`)
* Abstracción que permite a componentes de interfaz de usuario observar cambios sobre registros específicos o rangos (`table.watch(&pk)`).
* El stream asíncrono emite notificaciones inmediatas una vez que los deltas canónicos son confirmados y aplicados en el almacenamiento local.
* Una operación que la réplica rechaza (ver el `ApplyReport` en §A.4) no emite nada.

### A.8. Subida del Snapshot por el Cliente Designado
* Cuando la sala necesita un snapshot (un cliente quedó `Bootstrapping` y no hay uno usable), el servidor designa a un cliente `Connected` y se lo dice con `snapshot_wanted: true` en `HeartbeatAck`, `SyncBatch` o `CommitAck`.
* El designado genera el snapshot con `StorageEngine::create_snapshot` en su head local `S` y lo sube, en un request (`POST /rooms/:id/snapshot/upload`, hasta 16 MiB) o por chunks (`ClientMessage::UploadSnapshotChunk`). El relay lo acepta si `tail_seq - 1 <= S <= head_seq` y `S` es mayor que la del snapshot activo. Si no empieza a subir dentro de los 60 s, el servidor designa a otro.
* El repo no tiene un worker de snapshots: sin esta subida, un cliente nuevo en una sala podada no puede incorporarse (INV-R4 supone un cliente `Connected` que sigue el protocolo).

### A.9. Metadatos Durables del Cliente
* Por sala, el cliente guarda de forma durable su `client_id`, la `room_incarnation` y su cursor, que es el head de su storage. El cursor y la encarnación van siempre juntos: después de registrarse sin cursor, el cliente arranca de la secuencia 0 o de un snapshot, nunca de un cursor guardado (`ARCHITECTURE.md` §6, *Room incarnation*).
* Si storage abre la sala por detrás de lo que dicen los metadatos (por ejemplo, vacía en la secuencia 0 después de perder el `.snap`, riesgo D96 de `docs/INVARIANTES.md` §4), el cliente lo detecta y vuelve a bajar el estado del servidor.
* El token lo emite el backend de la aplicación. Ante `Unauthorized` el SDK pide uno nuevo a la aplicación.

### A.10. Columnas Cifradas (E2EE)
* Las columnas con `encrypted: true` viajan como `Value::Bytes` opacos (`ARCHITECTURE.md` §3.1). El SDK las cifra al escribir y las descifra al leer con el `CryptoEngine` de `zemdb-core`, usando como datos adicionales `(table_id, pk, column_idx)`.

### A.11. Invariantes y Auditoría del SDK
* El SDK está fuera del alcance actual de `docs/INVARIANTES.md` (§2). Sus invariantes se agregan al contrato junto con su código, y el SDK se audita una vez contra ellas, con el método de §1 (§1.4).

### A.12. API de JavaScript para el Navegador
* En el navegador, el SDK compilado a wasm corre en un Web Worker dedicado, donde el OPFS da archivos síncronos (§A.2), y la página lo usa a través de un paquete con tipos de TypeScript que le habla por mensajes (DC14).
* Las filas son objetos con nombres de columna; `Bytes` es `Uint8Array` y `Uuid` un string; los errores llevan un código que mapea `ClientError`, y `watch` se puede usar desde React (`useSyncExternalStore`). Cómo se representa `Int(i64)` (`bigint`, o `number` con verificación de rango) se decide en la sub-fase 4.0.
* Los tipos de TypeScript generados a partir del esquema y los bindings para apps móviles nativas quedan fuera de la v0.1.
