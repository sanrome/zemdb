No retrocompatibility is needed as this is the first version of the proyect so changes shouldn´t be stopped for this cause. 

No incluir identificadores ni referencias a códigos de auditoría (por ejemplo, C-01, C-03, M-04, A-01, B-01, DEF-01, etc.) en los comentarios del código fuente, tests ni docstrings. El código debe explicar la lógica y los motivos técnicos de manera limpia, profesional y autosuficiente, reservando los códigos de defectos exclusivamente para los documentos de auditoría (`docs/audits/`) y propuestas (`docs/proposals/`).

## Estructura de Tests

Los tests nunca se escriben dentro de los archivos de código funcional. Hay dos niveles:

- **Tests unitarios** (pueden acceder a elementos privados): van en un archivo separado `src/.../<modulo>/tests.rs`. El archivo fuente solo contiene, al final, la declaración:
  ```rust
  #[cfg(test)]
  mod tests;
  ```
  Se usan para invariantes internos, estados intermedios e inyección de fallos.
- **Tests de integración** (solo API pública): van en `crates/<crate>/tests/`. Se usan para el comportamiento observable desde fuera del crate.

No se hacen `pub` elementos internos únicamente para poder testearlos; si un test necesita acceder a internals, es un test unitario.

## Principios de Ingeniería y Robustez para Corrección de Errores

1. **Principio "Validar primero, mutar después" (Check-then-Act atómico):**
   - Ninguna función debe mutar estado observable en memoria (secuenciadores, cachés, rosters de clientes) ni persistir en disco antes de haber validado todas las precondiciones (retención, cuotas, autenticación y límites de secuencia).
   - Ante una validación fallida, el estado del sistema debe permanecer inalterado (cero efectos secundarios).
   - Una falla de I/O a mitad de una escritura durable (por ejemplo, `fsync` fallido después de un `write` exitoso) deja el disco en estado incierto y no puede deshacerse en código: no se reintenta ni se ignora. La sala o el motor afectado se marca como inválido y se recupera desde disco.
   - Una vez que una operación es durable, la respuesta al cliente nunca es un error. Si la información accesoria (por ejemplo, el catch-up) no puede construirse, se responde éxito indicando explícitamente que hay datos pendientes (`has_more: true`).

2. **Gestión de Estados Transitorios mediante RAII:**
   - Prohibido el uso de banderas booleanas manuales en memoria para controlar estados críticos (por ejemplo, `is_compacting = true ... is_compacting = false`). Si la función puede salir anticipadamente con el operador `?` o fallar, se debe encapsular el estado en un guard RAII con implementación del trait `Drop`, garantizando que el estado se restablezca aun ante errores o pánicos.
   - Como `Drop` no puede hacer `.await`, el estado controlado por un guard no debe vivir detrás de un lock asíncrono (usar tipos atómicos, por ejemplo `AtomicBool`).
   - El guard solo libera el estado transitorio; la consistencia en disco ante una salida anticipada se garantiza revirtiendo el cambio parcial o mediante la recuperación.

3. **Crash-Safety y Persistencia Atómica en Disco:**
   - Queda estrictamente prohibido borrar o sobrescribir in-place archivos de datos, WAL o snapshots antes de que los nuevos datos estén físicamente confirmados en disco.
   - El protocolo de **reemplazo** o fusión de archivos en disco debe seguir el patrón ARIES / POSIX seguro:
     1) Escribir en archivo temporal (`.tmp`),
     2) Ejecutar `sync_all()`,
     3) `rename()` atómico sobre el destino,
     4) Sincronizar el directorio padre (`sync_dir`),
     5) Únicamente tras confirmar lo anterior, purgar archivos residuales.
   - El **append** al WAL no usa archivo temporal: se escribe al final, se ejecuta `sync_data()` antes de confirmar, y la recuperación trunca la cola incompleta (torn write).
   - Crear, renombrar o borrar archivos también requiere `sync_dir` del directorio padre para que la entrada de directorio sea durable.
   - Tras un `rename` sobre un archivo que el proceso tiene abierto o bloqueado (`flock`), se debe reabrir y volver a bloquear el handle; de lo contrario se escribe sobre un inodo ya desvinculado.
   - La recuperación al arrancar debe tolerar y limpiar archivos `.tmp` residuales y estados intermedios de cualquier paso anterior.
   - Prohibido descartar resultados de I/O (`let _ = ...`) en rutas de durabilidad.

4. **Invariantes Estrictos en Log y Contigüidad:**
   - Cualquier lectura en logs multicapa (`HotBuffer`, `active.wal`, `sealed.wal`, `cold.wal`) debe garantizar de forma innegociable la contigüidad monótona estricta ($S_{i+1} = S_i + 1$). Prohibido omitir deltas o inventar secuencias.
   - Prohibido devolver una respuesta que el cliente pueda interpretar como "estás al día" cuando no lo está. Una respuesta vacía solo es válida acompañada de `has_more: true` o del error explícito (por ejemplo, `BehindCompaction`); nunca se suprime un error de retención con un valor por defecto.

5. **Encapsulamiento del Dominio:**
   - No exponer campos públicos mutables en structs de dominio (`core` y `storage`). Las mutaciones y transformaciones de esquemas, operaciones y tuplas deben ocurrir a través de métodos que validen los invariantes en tiempo de construcción/mutación.
   - Los tipos que llegan por deserialización (wire, JSON de administración, archivos en disco) no pasan por los constructores: deben validarse explícitamente (o mediante `#[serde(try_from = ...)]`) antes de usarse.

6. **Obligatoriedad de Tests Adversarios (Unhappy Path):**
   - Al corregir cualquier defecto, es obligatorio incluir al menos un test negativo que reproduzca el modo de fallo: cortes a mitad de escritura, fallas de I/O, reconexión de clientes en estados degradados (`Dormant`), o consultas con huecos de secuencia.
   - El test debe fallar con el código anterior a la corrección; si no falla, no está reproduciendo el defecto.
   - Los cortes a mitad de escritura se reproducen fabricando en disco cada estado intermedio posible; las fallas de I/O, con puntos de fallo compilados solo en tests (`#[cfg(test)]`) desde tests unitarios.

7. **Entrada No Confiable:**
   - Todo lo que proviene de un cliente (identificadores, tamaños, cantidades, números de secuencia, tokens) se valida en el borde antes de usarse. Los identificadores que terminan en rutas de archivo se restringen a un charset seguro.
   - Toda colección en memoria indexada o dimensionada por datos del cliente tiene un tope y un TTL.

8. **Concurrencia Asíncrona:**
   - Prohibido ejecutar I/O bloqueante o trabajo intensivo de CPU (por ejemplo, Zstd) directamente en un worker de Tokio; usar `tokio::task::spawn_blocking` o `tokio::task::block_in_place`.
   - Prohibido mantener guards de locks síncronos (`std::sync`, iteradores o referencias de `DashMap`) a través de un `.await`.

9. **Documentación Sincronizada:**
   - Si un cambio altera el comportamiento descrito en `ARCHITECTURE.md`, el documento se actualiza en el mismo commit.
