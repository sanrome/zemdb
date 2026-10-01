No retrocompatibility is needed as this is the first version of the proyect so changes shouldn´t be stopped for this cause. 

No incluir identificadores ni referencias a códigos de auditoría (por ejemplo, C-01, C-03, M-04, A-01, B-01, etc.) en los comentarios del código fuente, tests ni docstrings. El código debe explicar la lógica y los motivos técnicos de manera limpia, profesional y autosuficiente, reservando los códigos de defectos exclusivamente para los documentos de auditoría (`docs/audits/`) y propuestas (`docs/proposals/`).

Mantener siempre la estructura de tests establecida en el workspace: los tests deben ubicarse exclusivamente en archivos de prueba dentro de los directorios `tests/` de cada crate (tests de integración y unitarios externos). No incluir módulos de pruebas (`#[cfg(test)]`) ni funciones de test dentro de los archivos fuente funcionales (`src/`).

## Principios de Ingeniería y Robustez para Corrección de Errores

1. **Principio "Validar primero, mutar después" (Check-then-Act atómico):**
   - Ninguna función debe mutar estado observable en memoria (secuenciadores, cachés, rosters de clientes) ni persistir en disco antes de haber validado todas las precondiciones (retención, cuotas, autenticación y límites de secuencia).
   - Ante cualquier error o validación fallida, el estado del sistema debe permanecer inalterado (cero efectos secundarios).

2. **Gestión de Estados Transitorios mediante RAII:**
   - Prohibido el uso de banderas booleanas manuales en memoria para controlar estados críticos (por ejemplo, `is_compacting = true ... is_compacting = false`). Si la función puede salir anticipadamente con el operador `?` o fallar, se debe encapsular el estado en un guard RAII con implementación del trait `Drop`, garantizando que el estado se restablezca aun ante errores o pánicos.

3. **Crash-Safety y Persistencia Atómica en Disco:**
   - Queda estrictamente prohibido borrar o sobrescribir in-place archivos de datos, WAL o snapshots antes de que los nuevos datos estén físicamente confirmados en disco.
   - El protocolo de reemplazo o fusión de archivos en disco debe seguir el patrón ARIES / POSIX seguro: 
     1) Escribir en archivo temporal (`.tmp`), 
     2) Ejecutar `sync_all()`, 
     3) `rename()` atómico sobre el destino, 
     4) Sincronizar el directorio padre (`sync_dir`), 
     5) Únicamente tras confirmar lo anterior, purgar archivos residuales.

4. **Invariantes Estrictos en Log y Contigüidad:**
   - Cualquier lectura en logs multicapa (`HotBuffer`, `active.wal`, `sealed.wal`, `cold.wal`) debe garantizar de forma innegociable la contigüidad monótona estricta ($S_{i+1} = S_i + 1$). Prohibido omitir deltas, inventar secuencias o suprimir silenciosamente errores de retención (`BehindCompaction`) con fallbacks a listas vacías o valores por defecto.

5. **Encapsulamiento del Dominio:**
   - No exponer campos públicos mutables en structs de dominio (`core` y `storage`). Las mutaciones y transformaciones de esquemas, operaciones y tuplas deben ocurrir a través de métodos que validen los invariantes en tiempo de construcción/mutación.

6. **Obligatoriedad de Tests Adversarios (Unhappy Path):**
   - Al corregir cualquier defecto en los directorios `tests/`, es obligatorio incluir al menos un test negativo que reproduzca el modo de fallo: cortes a mitad de escritura, fallas de I/O, reconexión de clientes en estados degradados (`Dormant`), o consultas con huecos de secuencia.