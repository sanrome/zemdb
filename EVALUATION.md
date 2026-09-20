# EVALUATION.md: Informe Consolidado de Evaluación Multidimensional

**Proyecto:** `RimDB` (Motor de Base de Datos Distribuida Local-First)  
**Fecha:** 20 de Septiembre de 2026  
**Equipo Evaluador:** 4 Subagentes Especialistas Autónomos  
- 🦀 **Especialista en Código y Rust**
- 🗄️ **Especialista en Motores de Base de Datos**
- 🌐 **Especialista en Sistemas Distribuidos y Protocolos**
- 🏛️ **Especialista en Arquitectura y Estructura de Software**

---

## Índice General

1. [Resumen Ejecutivo y Estado General](#1-resumen-ejecutivo-y-estado-general)
2. [Perspectiva 1: Código y Rust Idiomático](#2-perspectiva-1-código-y-rust-idiomático)
3. [Perspectiva 2: Motores de Base de Datos y Procesamiento de Datos](#3-perspectiva-2-motores-de-base-de-datos-y-procesamiento-de-datos)
4. [Perspectiva 3: Sistemas Distribuidos y Protocolos de Comunicación](#4-perspectiva-3-sistemas-distribuidos-y-protocolos-de-comunicación)
5. [Perspectiva 4: Arquitectura de Software y Estructura del Proyecto](#5-perspectiva-4-arquitectura-de-software-y-estructura-del-proyecto)
6. [Matriz Comparativa de Hallazgos Críticos y Riesgos](#6-matriz-comparativa-de-hallazgos-críticos-y-riesgos)
7. [Matriz de Buenas Prácticas (Actuales vs. Requeridas)](#7-matriz-de-buenas-prácticas-actuales-vs-requeridas)
8. [Conclusiones del Diagnóstico](#8-conclusiones-del-diagnóstico)

---

## 1. Resumen Ejecutivo y Estado General

El proyecto **RimDB** (`rimdb`) tiene como meta la construcción de una base de datos distribuida con paradigma **Local-First**, orientada a grupos de colaboración aislados (**Rooms**) de 2 a 50 clientes. La premisa central del diseño es que el 100% del almacenamiento histórico y persistente reside en los dispositivos cliente, mientras que el servidor actúa exclusivamente como un **coordinador liviano**: secuenciador monótono por sala, validador de tipos y esquemas, y buffer efímero de mutaciones en memoria con compactación en vuelo (*squashing*).

La auditoría técnica independiente realizada por los 4 especialistas confirma que:
* **La visión de diseño es excelente:** La separación en salas independientes, el modelo de tipado fuerte con validación previa, el soporte de E2EE sin revelar datos en tránsito y el protocolo pull-based sobre HTTP/2 son decisiones conceptuales sobresalientes.
* **Existen fallos de lógica y consistencia de severidad crítica:**
  1. **Bug de resurrección de registros zombi en `squash_operations`:** un `Delete` seguido de un `Update` parcial reemplaza el borrado y revive una tupla rota.
  2. **Asimetría de timestamps en mutaciones:** `Operation::Insert` no tiene marca de tiempo, rompiendo la resolución determinista Last-Write-Wins (LWW).
  3. **Anti-patrón de rendimiento severo en `Ord for Value`:** alocación de strings en heap mediante `format!("{:?}", tag)` en comparaciones heterogéneas.
  4. **Ineficiencia extrema de memoria:** `Row = BTreeMap<String, Value>` duplica strings de columnas en cada tupla en el heap.
  5. **Ausencia total de idempotencia y correlación en red:** no existen `mutation_id` ni `correlation_id`, provocando mutaciones duplicadas ante reintentos de red.
  6. **Amnesia tras caídas del servidor:** el secuenciador en memoria reinicia el contador de secuencia a cero si el servidor cae.
  7. **Barrera para WebAssembly (WASM):** dependencias de `tokio` nativo y `zstd` en C impiden compilar el cliente para la web.
  8. **Inconsistencias en el repositorio:** existen subdirectorios `.git` independientes y anidados dentro de `crates/core`, `crates/server` y `crates/client`.

---

## 2. Perspectiva 1: Código y Rust Idiomático

### 2.1. Diagnóstico del Estado Actual del Código

#### Aciertos y Fortalezas
1. **Cargo Workspace Centralizado:** Empleo de `resolver = "2"` y unificación de dependencias en `[workspace.dependencies]`, asegurando paridad de versiones entre crates.
2. **Derivación Consistente de Traits Estándar:** Implementaciones correctas de `Debug`, `Clone`, `PartialEq`, `Serialize`, `Deserialize`, y `Eq`/`Hash` para tipos clave.
3. **Manejo de Errores con `thiserror`:** `ValidationError` en `crates/core/src/schema.rs` provee mensajes claros y tipados sin código redundante.
4. **Patrón Fluent Builder:** Constructores declarativos (`TableBuilder`, `SchemaBuilder`, `RowBuilder`, `UpdateBuilder`) con aceptación ergonómica de parámetros mediante `impl Into<String>`.
5. **Aislamiento de Cifrado E2EE en Tipos:** La semántica de `encrypted: bool` en `ColumnDef` permite que el validador fuerce `Value::Bytes` en tránsito preservando el tipo subyacente para el cliente.

#### Fallas Técnicas, Code Smells y Anti-Patrones

##### 1. Anti-patrón Crítico en `Ord for Value` (Asignaciones en Heap)
En `crates/core/src/value.rs` (líneas 153–158):
```rust
(a, b) => {
    let tag_a = std::mem::discriminant(a);
    let tag_b = std::mem::discriminant(b);
    format!("{:?}", tag_a).cmp(&format!("{:?}", tag_b))
}
```
* **Impacto:** En cada comparación heterogénea dentro de un `BTreeMap`, ordenamiento o búsqueda de claves compuestas, se asignan dos `String`s dinámicos en el heap. Además, la representación textual de `std::mem::Discriminant` no está garantizada como estable entre versiones del compilador de Rust.

##### 2. Ineficiencia en `PrimaryKey(pub Vec<Value>)`
* Encapsula un `Vec<Value>` directamente expuesto. En más del 95% de los modelos relacionales, la clave primaria es un único campo escalar (`i64` o `UUID`). Cada llamada a `PrimaryKey::single(...)` fuerza una alocación en heap innecesaria.

##### 3. Vulnerabilidad DoS en `decode_message`
En `crates/core/src/protocol.rs`:
```rust
pub fn decode_message<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, bincode::Error> {
    bincode::deserialize(bytes)
}
```
* `bincode::deserialize` sin límite de tamaño (`bincode::Options::with_limit`) permite que un payload corrupto o malicioso con un encabezado de longitud adulterado (`u64::MAX`) intente reservar gigabytes de memoria, provocando pánico por OOM.

##### 4. Ausencia del Newtype Pattern
Uso indiscriminado de tipos primitivos (`String` y `u64`) para `room_id`, `client_id` y `seq`. Esto permite que se intercambien accidentalmente argumentos en llamadas de función sin que el compilador alerte.

##### 5. Omisión de Validaciones de Tipos en Claves Primarias
En `Schema::validate_update` y `Schema::validate_delete`: solo se verifica la aridad (`pk.values().len() != t.primary_key.len()`), pero **no se valida que los tipos de datos de los valores del PK coincidan con el esquema**. Un string en una clave entera pasa la validación sin error.

##### 6. Abuso Semántico en Errores
En `crates/core/src/schema.rs`: se usa `ValidationError::MissingPrimaryKeyColumn` para reportar discrepancias en el número de componentes de una clave compuesta, formateando una oración en inglés en el campo `column`.

---

### 2.2. Buenas Prácticas (Rust)

* **Seguidas actualmente:** Workspace resolver v2, derive macros estándar, builders fluidos, errores tipados con `thiserror`.
* **Nuevas a adoptar:**
  - Sustitución de comparaciones `format!` por ordenamiento mediante discriminante entero constante (`type_order(&self) -> u8`).
  - Representación de `PrimaryKey` con `smallvec::SmallVec<[Value; 2]>` o enum `Single(Value)` / `Composite(SmallVec)`.
  - Newtypes fuertemente tipados: `RoomId(String)`, `ClientId(String)`, `SequenceNumber(u64)`, `Timestamp(u64)`.
  - Configuración de lints en `[workspace.lints.clippy]` y `[workspace.lints.rust]` (`unsafe_code = "forbid"`).
  - Límite defensivo en deserialización con `bincode::DefaultOptions::new().with_limit(...)`.
  - Uso de `bytes::Bytes` para payloads binarios y cero-copia.

---

## 3. Perspectiva 2: Motores de Base de Datos y Procesamiento de Datos

### 3.1. Diagnóstico del Estado Actual (Motor DB)

#### Aciertos y Fortalezas
1. **Esquema Centralizado por Sala:** Previene la corrupción de datos y la entropía de tipos característica de esquemas no regulados en sistemas distribuidos.
2. **Upsert por Defecto en Inserciones:** Semántica idónea para replicación eventualmente consistente, tolerando recepciones repetidas de inserciones.
3. **Validación Zero-Knowledge en Servidor:** El servidor valida la presencia y el tipo de columnas cifradas sin tener acceso al descifrado de la información.
4. **Aislamiento Físico File-per-Room:** Simplifica el mantenimiento, la copia de seguridad, el particionamiento y la eliminación segura de datos.

#### Fallas Críticas y Limitaciones Detectadas

##### 1. Bug Crítico de Resurrección de Tuplas Zombi
En `crates/core/src/operation.rs` (líneas 176–180):
```rust
// Rule: DELETE followed by UPDATE -> replaces with update
(target, incoming @ Operation::Update { .. }) => {
    *target = incoming;
    SquashOutcome::Replaced
}
```
* **Gravedad:** Si una fila se elimina y luego llega un `Update` parcial (con 1 o 2 columnas modificadas), el buffer descarta el `Delete` y coloca el `Update` como registro nuevo.
* **Resultado:** Resucita una entidad rota con campos obligatorios vacíos o inconsistentes. Un `Update` sobre un registro eliminado debe descartarse como no-op.

##### 2. Falta de Timestamp en `Operation::Insert`
* `Update` y `Delete` tienen `timestamp: u64`, pero `Insert` no tiene marca temporal.
* Si un cliente genera un `Insert` mientras estuvo desconectado con timestamp lógico $T=100$, y llega al servidor después de un `Delete` con $T=200$, la regla de inserción ciega sobreescribe el `Delete`, resucitando datos antiguos.

##### 3. Dispersión de Memoria y Overhead de `Row = BTreeMap<String, Value>`
* Cada fila guarda los nombres de sus columnas como `String`s independientes en memoria heap.
* Para 100.000 tuplas en una tabla de 10 columnas, se mantienen **1.000.000 de strings redundantes**.
* Búsquedas dentro de la fila requieren comparaciones de cadenas $O(\log C)$ en lugar de acceso posicional directo $O(1)$.

##### 4. Carencias en el Sistema de Tipos de Datos
* `Value` no posee variante `Null`, a pesar de que `ColumnDef` tiene `nullable: bool`.
* La ausencia de una columna obligatoria en `validate_row` no es detectada si la columna no forma parte del PK.
* Faltan tipos esenciales de base de datos: `Decimal` (cálculos financieros sin error IEEE 754), `Timestamp` y `UUID`.

##### 5. Inmutabilidad del Log vs. Compactación Destructiva
* Si el servidor compacta `seq = 1` y `seq = 2` fusionándolos o purgándolos cuando el Cliente A ya leyó hasta `seq = 1`, el Cliente A quedará desincronizado para siempre. Un log secuenciado no puede reescribir eventos ya observados sin un mecanismo de compensación.

##### 6. Ausencia Total de Motor de Almacenamiento Local
* `crates/client` es un stub vacío. No posee Write-Ahead Log (WAL), páginas en disco, aislamiento transaccional ni índice primario.

---

### 3.2. Buenas Prácticas (Base de Datos)

* **Seguidas actualmente:** Esquemas declarativos estrictos, validación de integridad referencial soft, compresión Zstandard propuesta en especificación.
* **Nuevas a adoptar:**
  - **Formato Posicional de Tupla (`CompactRow`):** Vector indexado por posición de columna en el esquema + Null Bitmap para evitar duplicación de strings.
  - **Write-Ahead Logging (WAL) en Cliente:** Registro append-only con CRC32 antes de alterar tablas locales, garantizando durabilidad ante fallos de energía.
  - **Hybrid Logical Clocks (HLC) por Columna:** Timestamps a nivel de celda para resolución LWW granular sin depender de sincronización perfecta de reloj.
  - **Codificación Memcomparable para Claves:** Serialización binaria de claves ordenables lexicográficamente sin deserialización previa.
  - **Buffer Pool y Paginación (Slotted-Page):** Formato de archivo estructurado para salas que superen la memoria disponible.

---

## 4. Perspectiva 3: Sistemas Distribuidos y Protocolos de Comunicación

### 4.1. Diagnóstico del Estado Actual (Sistemas Distribuidos)

#### Aciertos y Fortalezas
1. **Sharding Natural por Room (Shared-Nothing):** No requiere transacciones distribuidas globales (2PC / Paxos cross-node), permitiendo paralelismo total entre salas.
2. **Secuenciador Monótono Determinista:** Garantiza linealización estricta por sala sin la complejidad de matrices de estado de CRDTs pesados.
3. **Protocolo Pull-Based sobre HTTP/2:** Evita retener miles de WebSockets ociosos, optimizando la huella de memoria del servidor y simplificando el paso por proxies.

#### Fallas Distribuidas y Vulnerabilidades de Contrato

##### 1. Ausencia de Idempotencia en Commits (Vulnerabilidad Mayor)
* `ClientMessage::Commit` carece de `mutation_id` (UUID).
* Ante una desconexión de red donde el servidor procesa el commit pero el ACK se pierde, el cliente reintenta la mutación. El servidor le asigna una nueva secuencia y duplica la operación en el log.

##### 2. Falta de Identificador de Correlación (`correlation_id`)
* Ni peticiones ni respuestas correlacionan transacciones. En streams multiplexados HTTP/2 o canales asíncronos, el cliente no puede emparejar cuál respuesta corresponde a cuál petición en vuelo.

##### 3. Clock Skew Severo en LWW
* Los timestamps son generados por los clientes con relojes de pared locales (`SystemTime`).
* Desfases horarios de minutos u horas provocan que clientes con relojes adelantados dominen permanentemente sobre los demás nodos.

##### 4. Amnesia tras Crash del Servidor (SPOF)
* El servidor carece de persistencia de metadatos. Si cae o se reinicia, pierde el último `sequence_id` y el registro de clientes. Al reiniciar en `seq = 1`, corrompe irreversiblemente el estado de las salas activas.

##### 5. Riesgo de Estancamiento (*Offline Stall*) por `BehindCompaction`
* Si un cliente estuvo desconectado más allá del TTL de retención del log, el servidor responde con `BehindCompaction`.
* Como el servidor no guarda el estado completo histórico, el cliente depende exclusivamente de que **otro cliente activo esté conectado simultáneamente** para solicitar un snapshot. Si no hay pares online, el cliente queda bloqueado indefinidamente.

##### 6. Falta de Paginación en `SyncBatch`
* `Sync` solicita todas las operaciones acumuladas sin límite superior. Un delta de 50.000 operaciones genera un bloque binario gigante susceptible a provocar OOM o timeouts de transporte.

---

### 4.2. Buenas Prácticas (Sistemas Distribuidos)

* **Seguidas actualmente:** Aislamiento estricto de particiones, orden total linealizado, serialización binaria en wire format.
* **Nuevas a adoptar:**
  - **Idempotency Keys con Cache Dedup:** Cada commit incluye `mutation_id: [u8; 16]`; el servidor mantiene un cache LRU para responder con la secuencia original sin re-ejecutar.
  - **Hybrid Logical Clocks (HLC):** Monotonicidad garantizada combinando tiempo físico y contador lógico.
  - **Paginación y Flow Control:** Parámetros `max_batch_size` y flag `has_more: bool` en `SyncBatch`.
  - **Micro-WAL de Metadatos en Servidor:** Almacenamiento append-only mínimo para persistir el último `sequence_id` emitido por sala.
  - **Snapshot Pinning / Lease:** Bloqueo temporal del truncado de log mientras un nuevo cliente descarga un snapshot.
  - **Integridad Criptográfica con BLAKE3:** Checksum de snapshots antes de su aplicación en el cliente.
  - **Rebase Local Optimista:** Algoritmo en cliente para deshacer mutaciones pendientes, aplicar el lote remoto y reaplicar las locales resolviendo conflictos.

---

## 5. Perspectiva 4: Arquitectura de Software y Estructura del Proyecto

### 5.1. Diagnóstico del Estado Actual (Arquitectura)

#### Aciertos y Fortalezas
1. **Claridad Conceptual en `ARCHITECTURE.md`:** Documento de diseño bien estructurado con delimitación rigurosa del alcance Local-First.
2. **Desacoplamiento Inicial en 3 Crates:** División conceptual básica entre tipos comunes (`core`), servidor (`server`) y cliente (`client`).

#### Fallas de Arquitectura y Acoplamiento

##### 1. Riesgo de "God Crate" en `crates/core`
`rimdb-core` agrupa simultáneamente:
- Entidades de dominio (`Value`, `PrimaryKey`, `Row`).
- Esquema y reglas de validación (`Schema`, `TableSchema`).
- Semántica de mutaciones (`Operation`).
- Lógica de compactación de memoria del servidor (`squash_operations`).
- Mensajes de red y transporte binario (`ClientMessage`, `ServerMessage`, `bincode`).
*Violación de Principio de Responsabilidad Única (SRP) e Inversión de Dependencias (DIP).*

##### 2. Ausencia de Puertos y Adaptadores (Traits / Interfaces)
No existen abstracciones (`traits`) para:
- Motor de persistencia (`trait StorageEngine`).
- Transporte de red (`trait TransportClient`).
- Criptografía de extremo a extremo (`trait CryptoEngine`).
El código está rígidamente acoplado a implementaciones concretas.

##### 3. Barrera de Compatibilidad con WebAssembly (WASM)
El objetivo de compilar el cliente para navegadores web está bloqueado:
- `rimdb-client` depende de `tokio` con `"full"` (hilos nativos, epoll/kqueue incompatibles con WASM).
- Dependencia directa de `zstd` con enlaces C nativos.
- Falta de *feature flags* (`native` vs `wasm`).

##### 4. Defecto en el Control de Versiones del Workspace
Existen directorios `.git` anidados dentro de `crates/core`, `crates/server` y `crates/client`, en lugar de un repositorio unificado en la raíz.

---

### 5.2. Buenas Prácticas (Arquitectura)

* **Seguidas actualmente:** Workspace Cargo unificado, diseño centrado en el dominio.
* **Nuevas a adoptar:**
  - **Arquitectura Hexagonal (Ports & Adapters):** Desacoplar el motor de base de datos de los detalles de I/O, almacenamiento y red.
  - **Feature Flags para Multi-Target:** Configuración en Cargo de perfiles `native` (tokio, FS, reqwest) y `wasm` (gloo-timers, IndexedDB, web-sys fetch).
  - **Submodularización Estricta con Visibilidad `pub(crate)`:** Encapsulamiento de módulos internos para evitar fugas hacia la API pública.

---

## 6. Matriz Comparativa de Hallazgos Críticos y Riesgos

| ID | Hallazgo Crítico | Especialidad que lo Detectó | Severidad | Impacto en el Sistema |
| :--- | :--- | :--- | :--- | :--- |
| **H-01** | **Resurrección Zombi:** `Delete` seguido de `Update` en `squash_operations` revive filas eliminadas de forma corrupta. | BD, Rust, Distribuidos | 🔴 **Crítica** | Corrupción y pérdida de consistencia en el estado de las salas. |
| **H-02** | **Asignaciones en Heap en `Ord for Value`:** `format!("{:?}", tag)` en cada comparación heterogénea. | Rust, BD | 🔴 **Crítica** | Caída masiva de throughput en ordenamientos, índices y árboles. |
| **H-03** | **Falta de Idempotencia en Commits:** Ausencia de `mutation_id` en peticiones de commit. | Distribuidos | 🔴 **Crítica** | Duplicación silenciosa de mutaciones ante reintentos de red. |
| **H-04** | **Amnesia tras Crash del Servidor:** Pérdida del último `sequence_id` si el servidor se reinicia. | Distribuidos | 🔴 **Crítica** | Desincronización irreversible de clientes activos. |
| **H-05** | **Ineficiencia en Memoria (`Row = BTreeMap<String, Value>`):** Millones de strings redundantes en heap. | BD, Rust, Arq. | 🟠 **Alta** | Uso desmedido de RAM y baja localidad de caché en CPU. |
| **H-06** | **Vulnerabilidad DoS en `decode_message`:** Deserialización `bincode` sin límites de tamaño. | Rust | 🟠 **Alta** | Servidor vulnerable a pánicos por Out-Of-Memory. |
| **H-07** | **Clock Skew en LWW:** Timestamps generados por clientes provocan pérdida de escrituras legítimas. | Distribuidos, BD | 🟠 **Alta** | Inconsistencias temporales en actualizaciones concurrentes. |
| **H-08** | **Omisión de Tipos en Claves Primarias:** `validate_update` y `validate_delete` no chequean tipos del PK. | Rust, BD | 🟠 **Alta** | Inserción de mutaciones con tipos de clave incompatibles. |
| **H-09** | **Falta de Soporte WASM:** Dependencia rígida de `tokio/full` y enlaces C de `zstd` en cliente. | Arq., Rust | 🟡 **Media** | Imposibilidad de ejecutar el cliente en navegadores web. |
| **H-10** | **Repositorios Git Anidados:** Subcarpetas `.git` en cada crate rompen el control de versiones raíz. | Arq., Rust | 🟡 **Media** | Conflictos en CI y fragmentación del código. |

---

## 7. Matriz de Buenas Prácticas (Actuales vs. Requeridas)

| Especialidad | Buenas Prácticas Actuales | Nuevas Buenas Prácticas Requeridas |
| :--- | :--- | :--- |
| **Rust & Código** | Cargo Workspace resolver v2; `thiserror` en errores; fluent builders ergonómicos; derivación idiomática de traits estándar. | Discriminantes enteros en `Ord`; Newtype pattern en identificadores; `SmallVec` en claves primarias; `bincode::Options::with_limit`; lints de Clippy estrictos. |
| **Base de Datos** | Esquemas relacionales con tipado estricto; validación Zero-Knowledge de E2EE; semántica upsert en inserción; topología file-per-room. | Layout de tupla posicional (`CompactRow`); Write-Ahead Logging (WAL) local; Hybrid Logical Clocks; claves memcomparables; paginación y Buffer Pool. |
| **Sistemas Distribuidos** | Sharding natural por sala; orden total monótono linealizado; protocolo binario pull-based sobre HTTP/2. | Claves de idempotencia (`mutation_id`); correlación de peticiones (`correlation_id`); micro-WAL de secuencias en servidor; paginación en `SyncBatch`; snapshot pinning con BLAKE3. |
| **Arquitectura** | Clara delimitación de responsabilidades en la especificación; separación básica en workspace de 3 crates. | Arquitectura Hexagonal (Ports & Adapters); separación de capas de dominio, protocolo y storage; feature flags para WASM/Native; eliminación de sub-repos `.git`. |

---

## 8. Conclusiones del Diagnóstico

El análisis concurrente de los cuatro especialistas demuestra que el proyecto cuenta con un fundamento arquitectónico de gran valor, pero adolece de **vicios iniciales que comprometen su viabilidad en producción** si no se corrigen de inmediato. 

El modelo de squashing actual genera registros zombi, la estructura de tuplas consume memoria de forma excesiva, las comparaciones de tipos degradan la CPU, y el protocolo de red carece de los mecanismos básicos de idempotencia y tolerancia a fallos exigidos por un sistema distribuido.

En el documento complementario **`PROPOSAL.md`** se presenta la solución de ingeniería unificada y compatible, estableciendo la nueva arquitectura de módulos, contratos de red corregidos y el diseño detallado del motor de almacenamiento y coordinación.
