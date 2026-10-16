# Plan de Remediación — Fase 3.9

**Fecha:** 16 de Octubre de 2026
**Origen:** [`docs/audits/2026-10-invariantes-audit.md`](../audits/2026-10-invariantes-audit.md) (DEF-144 a DEF-158, más las invariantes nuevas adoptadas de su §6)
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md)
**Tipo:** Documento vivo (plan + seguimiento). El detalle de cada defecto (dónde, escenario, evidencia) está en la auditoría; acá van el orden, las decisiones, la solución y el test exigido.

---

## 0. Cómo usar este documento

- Los defectos están agrupados en **lotes** ordenados por prioridad de ejecución. Cada lote es una unidad de trabajo revisable (uno o pocos commits).
- Cada ítem tiene: severidad, estado, solución y test exigido.
- Al cerrar un ítem se actualiza su estado aquí y se anota el commit.

**Estados:** ⬜ Pendiente · 🟡 Parcial · ✅ Hecho · ⏸ Diferido · ❌ Descartado

**Reglas de trabajo** (las de las fases anteriores, ver `GEMINI.md`, más dos nuevas):
1. Tests según la estructura de dos niveles de `GEMINI.md`, con al menos un test negativo que reproduzca el fallo y que falle sin el fix. La auditoría ya dejó ese test para casi todos los ítems. Está en el parche de su grupo en [`docs/audits/2026-10-invariantes-audit/`](../audits/2026-10-invariantes-audit/README.md); cuando este plan dice `inv/<grupo>`, se refiere a `<grupo>.patch`. Se trae y se adapta (sin los nombres `audit_*`/`inv_*` ni códigos).
2. Sin códigos de auditoría (`DEF-xx`) en código, tests ni docstrings.
3. Cada lote cierra con `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` y `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` en verde.
4. **Nueva:** cada lote cierra con una **revisión de delta** contra las invariantes que toca (`INVARIANTES.md` §1.1): para cada fix, la mutación que lo deshace tiene que hacer fallar un test, y el diff no puede romper otra invariante. No se hace una auditoría completa por lote.
5. **Nueva:** un cambio que agrega, quita o debilita una garantía actualiza `INVARIANTES.md` en el mismo commit (§5 de ese documento).

### 0.1. Procedimiento por lote

Cada lote se ejecuta con el mismo ciclo, idealmente en una sesión propia, siguiendo el orden de §3. Este procedimiento es lo único que hace falta para retomar el trabajo sin el contexto de sesiones anteriores.

**Excepción al orden:** los Lotes 1 (solo servidor) y 3 (solo storage) no se pisan y pueden hacerse en paralelo. El Lote 2 va solo, porque cambia formatos en disco sobre los que se apoyan los demás.

**0. Arranque.**
- **El árbol:** `git pull`, con el árbol de trabajo limpio y la CI de `main` en verde.
- **Qué leer:** la sección del lote en este plan, las decisiones que cita (§2), las invariantes que toca en `INVARIANTES.md` y `GEMINI.md`.
- **Decisiones en estado *Propuesta*:** si el lote tiene alguna, se confirman con el dueño del proyecto antes de escribir código. Al confirmarse pasan a *Adoptada*, con los cambios que hayan surgido.

**1. Primero el test que falla.** Traer el test de ruptura del ítem desde su parche en `docs/audits/2026-10-invariantes-audit/` (ver la regla 1) y adaptarlo a `GEMINI.md`. Después, **comprobar que falla en el `main` actual**: si no falla, no reproduce el defecto y hay que reescribirlo. Para un ítem sin test en los parches, se escribe uno nuevo con la misma exigencia.

**2. El fix mínimo.** Implementar lo que pide la decisión, sin cambios de alcance. Seguir los principios de `GEMINI.md`: validar antes de mutar, guards RAII, persistencia atómica, nada de I/O bloqueante en el runtime.

**3. Verificación.**
- **Chequeos de la regla 3:** fmt, clippy, tests y doc. Si el lote toca `core` o `storage`, también `cargo build -p zemdb-core -p zemdb-storage -p zemdb-client --target wasm32-unknown-unknown`.
- **Mutación inversa:** deshacer el fix (o sacar la guarda que agrega) y confirmar que el test nuevo falla. Después, restaurarlo.
- **Versiones largas, una sola vez al cerrar el lote:** `cargo test -p <crate> --release -- --ignored` de los crates tocados. Nunca se agregan a la CI de cada push (ver `.github/workflows/randomized.yml`).

**4. Revisión de delta (regla 4).** Sobre el diff completo del lote, con un revisor independiente de contexto fresco y en modo solo lectura (un agente aparte); en un lote chico (por ejemplo, el Lote 1) la puede hacer quien lo implementó. El revisor recibe el diff, las invariantes que toca y los "test exigido". Responde tres cosas:
- si cada fix cierra el escenario de la auditoría;
- si deshacerlo hace fallar un test;
- si el diff rompe alguna otra invariante.

Como en la auditoría, sin un test que lo reproduzca no hay hallazgo Alto ni Medio.

**5. Documentos, en el mismo cambio.**
- **`ARCHITECTURE.md`:** si cambió un comportamiento descrito ahí.
- **`INVARIANTES.md`:** la columna Estado pasa de ❌ o 🆕 a ✅, y se quita el *Pendiente* de "Sostenida por".
- **Este plan:** el ítem pasa a ✅, con una nota **Hecho** que lista los tests agregados, la mutación verificada y cualquier desvío respecto de la solución planeada. Se actualizan el resumen (§3) y el avance total.

**6. Alcance.** Lo que aparezca fuera del lote **no se arregla en el momento**:
- si rompe una invariante, se agrega a este plan como ítem nuevo (DEF-164 en adelante), con su test, en el lote que corresponda;
- si no rompe ninguna, va al backlog Bajo (`INVARIANTES.md` §1.2).

La única excepción son los ítems del backlog marcados **(barato)** que estén en los mismos archivos que toca el lote.

**7. Entrega.**
- Los cambios se dejan sin commitear para que el dueño revise el diff.
- Con su visto bueno, se arma un commit por lote, o pocos (estilo `fix(server): … (Lote N)`), sin trailers de co-autoría, y se pushea a `main`.

**Después del Lote 6.** Una auditoría por invariantes acotada a las invariantes cuyo código cambió más las nuevas, con el método de `INVARIANTES.md` §1. Si sale en verde según §1.4, se cumple el requisito para la Fase 4 (§1).

---

## 1. Criterios del orden

1. **Primero, las redes de prueba (Lote 0).** Las simulaciones, el fuzzer y los proptests de la auditoría viven en copias temporales que se pueden perder. Una vez en el repo, cada lote siguiente se verifica contra las invariantes de forma automática. Ese mismo lote actualiza `INVARIANTES.md` con las decisiones de abajo, porque es el contrato contra el que se revisa cada lote.
2. **Después, lo más probable y barato (Lote 1).** DEF-146 es una aplicación doble en el camino normal, y se arregla con una línea.
3. **Los cambios de formato en disco, juntos y antes de la Fase 4 (Lotes 2 y 3).** El checksum de los JSON y el piso de secuencia cambian archivos del servidor; la absorción atómica cambia cómo storage escribe `.wal.compacting`. Contexto V1: sin compatibilidad con archivos anteriores.
4. **Recursos y ciclo de vida, después (Lotes 4 y 5).** Tienen disparadores hostiles o raros, y ninguno pierde commits.
5. **La documentación, al final (Lote 6).**

**Requisito para empezar la Fase 4:** Lotes 0, 2 y 3 cerrados, y una auditoría por invariantes en verde (`INVARIANTES.md` §1.4), limitada a las invariantes cuyo código cambió más las nuevas.

---

## 2. Decisiones de diseño

Se continúa la numeración del plan de la Fase 3.8 (que termina en D68). Las decisiones D69 a D80 son las que se tomaron sobre la auditoría (el número entre paréntesis es el de la conversación). Las D81 a D87 son de implementación y están **propuestas**: se confirman o cambian antes de arrancar el lote correspondiente. D89 salió de la revisión de delta del Lote 3.

| ID | Decisión | Lote | Estado |
|---|---|---|---|
| **D69** | **La cuota acota el disco de la sala (1a).** (a) `disk_bytes` suma los sidecars `.ids` (24 bytes por operación más su header, calculables por rango sin leerlos). (b) Si la sala está por encima de `max_room_disk_bytes`, la compresión no espera `warm_disk_ttl`: los warm más viejos son elegibles enseguida. El tope de 4 compresiones por pasada se mantiene, porque levantarlo vuelve a congelar al actor (DEF-99). (c) La poda por cuota borra el prefijo más viejo del log, warm o cold, mientras se esté por encima de la cuota; la poda por TTL sigue siendo solo de cold. Así el disco queda acotado por la cuota más `active.wal` más lo escrito en una pasada. Para el cliente, el único efecto es que si estuvo mucho tiempo offline puede recibir `BehindCompaction` antes, y esa consecuencia de la cuota ya está documentada. Estimado: menos de 100 líneas de producción en `warm_segments_due`, `expire` y `disk_bytes`. **Si al implementarlo supera ~300 líneas de producción, se difiere y se aplica la opción (c):** reformular INV-B3 como "cuota + ventana warm + sidecars". | 4 | Adoptada |
| **D70** | **La encarnación se chequea solo en el registro (2a).** Regla del SDK, documentada en ARCHITECTURE §6 y en INV-Y7: después de registrarse sin cursor, el cliente arranca de 0 o de un snapshot; nunca reusa un cursor guardado. El SDK guarda cursor y encarnación juntos. | 0 | Adoptada |
| **D71** | **"Mismo estado" es igualdad módulo `Null` al final de las filas (3a).** Un `Update` sigue ensanchando la fila hasta el ancho del esquema con que se aplica; `compact_into_row` ya completa con `Null`. Se corrige el enunciado de INV-Y8. | 0 | Adoptada |
| **D72** | **Un stream SSE vive más allá del vencimiento de su token: riesgo aceptado (4a).** SSE solo notifica, y cualquier dato se pide con un token vigente. Se documenta en `INVARIANTES.md` §4. | 0 | Adoptada |
| **D73** | **La memoria de las respuestas se cobra por lo que ocupan, no por su tamaño de wire (5a).** (a) `ReadBudget` y la HotBuffer cobran `max(encoded_len, 24 × valores)` por operación: con tablas anchas se pagina más (`has_more`), lo que el contrato ya permite. Una operación sola sigue entrando siempre: tiene como mucho `MAX_COLUMNS` × 2 valores, unos 3 MiB. (b) El presupuesto de bytes en vuelo se pide **antes** de materializar y codificar, con una estimación del tamaño a partir del presupuesto de lectura. (c) Las respuestas `Schema` y `Registered` también toman presupuesto. Su tamaño depende del esquema, y con el tope por cliente (D75) dejan de poder acumularse. Sin presupuesto disponible responden `Unavailable` (503), que el cliente ya tiene que tolerar en cualquier request. Esto reemplaza la idea de "codificar el esquema una vez": cada respuesta lleva campos propios del request (`correlation_id`) y el frame no se puede compartir sin codificar a mano. (d) `max_send_buf_size` de hyper para HTTP/2 se fija en el tamaño de frame del cuerpo (64 KiB), para que se cumpla lo que dice §7.3 sobre los buffers por stream (hoy es de 400 KiB). (e) Los chunks del relay se leen después de tomar el presupuesto. | 4 | Adoptada |
| **D74** | **Una subida tiene que progresar, y tiene un tope total (6a).** (a) Solo un chunk que todavía no se había recibido renueva el plazo de inactividad de 2 minutos; reenviar uno ya recibido se responde igual que hoy, pero no renueva nada. (b) Tope total `snapshot_upload_max_secs` (`ZEMDB_SNAPSHOT_UPLOAD_MAX_SECS`, 7.200 s por defecto, de 60 s a 1 día): pasado ese tiempo desde el primer chunk, la subida se descarta como una inactiva. 512 MiB a 1 Mbit/s tardan ~72 min. | 4 | Adoptada |
| **D75** | **El presupuesto de respuestas tiene un tope por cliente (7a).** `max_inflight_response_bytes_per_client` (`ZEMDB_MAX_INFLIGHT_RESPONSE_BYTES_PER_CLIENT`, por defecto 2 × `MAX_FRAME_SIZE`, de `MAX_FRAME_SIZE` al global). Se cuenta por `(sala, client_id)` dentro del semáforo global. Superado el tope, `Unavailable`. Las respuestas al admin secret del relay cuentan como un cliente más. Un miembro que no lee sus respuestas ya no deja sin presupuesto a las demás salas. | 4 | Adoptada |
| **D76** | **Solo la subida del designado o del worker congela la designación (8a).** La subida de otro miembro sigue anclando la poda (D61), pero no impide reemplazar al designado al vencer su plazo de 60 s. | 4 | Adoptada |
| **D77** | **`fsync` al abrir; el hueco de Linux queda como riesgo (9b).** Servidor: al abrir el log, `sync_all` de `active.wal` y `sync_dir` de `segments/`, del directorio de la sala y de `rooms/`, antes de servir o deduplicar. Storage: en `open_room`, `sync_all` del WAL activo (y de `.wal.compacting` si existe) y sync de `data_dir` y su padre. Riesgo aceptado en `INVARIANTES.md` §4: en Linux, después de un EIO, el kernel puede marcar las páginas como limpias sin haberlas escrito, y un `fsync` desde un descriptor nuevo devuelve éxito igual. La solución completa (reescribir `active.wal` cuando el actor anterior se detuvo por una escritura fallida) queda anotada para el futuro. | 2 | Adoptada |
| **D78** | **El último frame dañado de `active.wal` es una excepción de INV-D2 (10a).** Un daño en el último frame sin ningún frame válido después no se distingue de una escritura rota: se trunca y su secuencia puede reutilizarse. El piso de secuencia (D81) lo cubre cuando la secuencia aparece en otro archivo durable. Se documenta en INV-D2. | 0 | Adoptada |
| **D79** | **Invariantes nuevas y ajustes de enunciado (11, 12, 13).** Se agregan a `INVARIANTES.md`: P1 (un proceso por `data_dir`), P2 (nada visible antes de ser durable en este proceso), P3 (piso de secuencia fuera del log), P4 (metadatos verificados), P5 (ventana de dedup como función del log), P6 (ningún cliente acapara un recurso global), P7 (borrado de sala atómico), P9 (memoria igual a disco en storage), P10 (snapshot atado a la encarnación) y P11 (subida confirmada usable). P12 y P13 se incorporan a los enunciados de INV-B2 e INV-B1. P8 no se adopta: pasa a riesgo aceptado en §4 (un valor que embebe un frame válido puede convertir una escritura rota en corrupción; falla cerrada, sin pérdida). Se aplican los ajustes de enunciado de la auditoría (§5): Y3, Y5, Y6, Y7, Y8, R3, R4, R5, S4, S5, B3, B4 y L1. | 0 | Adoptada |
| **D80** | **DEF-152 se mantiene en Alto (14a).** | — | Adoptada |
| **D81** | **Piso de secuencia en `log_meta.json`.** El archivo pasa a guardar `pruned_through_seq` y `sealed_through_seq` (el fin del último segmento sellado). Se reescribe con `write_atomic` en cada rotación, antes de crear el `active.wal` nuevo. Al abrir, `head_seq` es el máximo entre los nombres de segmento, `active.wal`, `pruned_through_seq` y `sealed_through_seq`. Costo: una escritura atómica por rotación (cada 1.000 operaciones o 32 MiB por defecto). | 2 | Propuesta |
| **D82** | **JSON persistido con checksum.** `log_meta.json`, `meta_room.json`, `schemas/{id}.json` y el roster se escriben como el JSON, un salto de línea y una última línea `crc32:<8 hex>` con el CRC32 de los bytes del JSON. Siguen siendo legibles a mano. Leer un archivo sin esa línea o con un CRC que no coincide equivale a un archivo ilegible: `log_meta.json`, `meta_room.json` y los esquemas hacen fallar la apertura; el roster se descarta con un warning, como hoy. Un roster con UTF-8 inválido también se descarta (hoy falla la apertura, al revés de lo que dice §5.2). Sin compatibilidad con archivos sin checksum (V1). La apertura nunca borra segmentos a partir de un valor que no verificó. | 2 | Propuesta |
| **D83** | **Lock exclusivo de `data_dir`.** El servidor toma `data_dir/server.lock` (`fs2`, como `log.lock`) al arrancar, antes de recuperar el relay o el registry de esquemas. Si el lock está tomado, se niega a arrancar con un error que lo explica. `delete_room` además toma el `log.lock` de la sala antes de borrarla. | 2 | Propuesta |
| **D84** | **Borrado de sala con lápida.** `delete_room` renombra `rooms/<id>` a `rooms/.deleted-<uuid>`, sincroniza `rooms/` y recién después borra el directorio renombrado. Al arrancar se borran las lápidas que quedaron. `RoomId` no admite `.`, así que una lápida nunca choca con una sala. `create_room` además purga el relay de ese id (P10). | 5 | Propuesta |
| **D85** | **Absorción atómica en storage.** En lugar de anexar el WAL activo a `.wal.compacting` en el lugar, se arma `room_{id}.wal.compacting.tmp` con `.wal.compacting` más el WAL activo, se sincroniza, se renombra sobre `.wal.compacting`, se sincroniza el directorio y recién después se trunca el WAL activo. `.wal.compacting` nunca queda con una cola rota, así que desaparecen la excepción de corrupción de `.wal.compacting` y `highest_seq_in_valid_frames`. Al adoptarla se fijó: el temporal tiene nombre fijo y la apertura lo borra sin leerlo (se suma a los temporales de INV-S4); una falla al escribirlo, sincronizarlo o renombrarlo lo borra y deja la sala usable; una falla del sync del directorio después del rename la marca `RoomFailed`; un daño que llega al final de `.wal.compacting` sigue siendo una escritura rota. | 3 | Adoptada |
| **D86** | **`apply_snapshot` instala en su propia tarea.** Desde el staging hasta la instalación (rename, sync, vaciado del WAL, borrado de `.wal.compacting`), `apply_snapshot` corre en una tarea propia, como `compact_room`. Cancelar el future deja de abandonar operaciones de archivo en vuelo, y `close_room` espera a la tarea (comparten el lock de compactación). Al adoptarla se fijó: decodificar y esperar el lock siguen en quien llama; quien suelta el future ya no deja la sala `RoomFailed`, porque el snapshot se aplica entero. | 3 | Adoptada |
| **D87** | **El apagado espera las tareas de varios pasos.** Un `TaskTracker` registra las tareas que corren por su cuenta (borrado de sala, escrituras del relay, recarga de esquema). `serve_until_shutdown` las espera dentro de la gracia de 10 s, antes de `shutdown_all`. | 5 | Propuesta |
| **D89** | **`open_room` del storage recupera en su propia tarea.** La recuperación corre en una tarea dueña del lock de la sala y del handle del WAL hasta que terminan sus operaciones de archivo, como `apply_snapshot` (D86). Si quien llama se va, la apertura termina igual y la sala queda abierta. | 3 | Propuesta |
| **D88** | **Redes de prueba en el repo.** Las simulaciones y proptests de la auditoría entran como tests con semillas fijas. Cada red entra en la suite normal en una versión corta: todas juntas suman como mucho ~30 s en debug. Las versiones largas quedan como `#[ignore]`, así que `cargo test` no las corre, ni en local ni en la CI de cada push o PR. Corren en un workflow aparte (`.github/workflows/randomized.yml`, `cargo test --workspace --release -- --ignored`) una vez por semana y a mano al cerrar cada lote, como parte de la revisión de delta (regla 4). El fuzzer de decoders y los roundtrips usan generadores propios con un PRNG determinista (SplitMix64), sin `proptest`: su versión actual exige exactamente el MSRV de los tests (1.88) y sus dependencias transitivas podrían romper ese job. No hay `cargo fuzz`. Las mediciones de memoria quedan fuera, porque requieren `unsafe`. | 0 | Adoptada |

---

## 3. Resumen

| Orden | Lote | Ítems | Prioridad | Progreso |
|---|---|---|---|---|
| 0 | Contrato y redes de prueba | D70–D72, D78, D79, D88 | Inmediata (las redes viven en copias temporales) | ✅ |
| 1 | Arreglos chicos de alto valor | DEF-146, 147, 155 | Inmediata | ✅ 3/3 |
| 2 | Abrir solo lo verificado y sincronizado | DEF-144, 145, 148, 158, 149 | Alta (requisito de Fase 4) | ⬜ 0/5 |
| 3 | Storage: cancelación y absorción | DEF-150, 151, 165 | Alta (requisito de Fase 4) | 🟡 2/3 |
| 4 | Recursos | DEF-152, 159, 153, 163, 162, 164, 154, 157 | Media | ⬜ 0/8 |
| 5 | Ciclo de vida | DEF-156, 160, 161 | Baja | ⬜ 0/3 |
| 6 | Documentación | ARCHITECTURE, README, ROADMAP | Baja | ⬜ |

**Avance total:** 5 de 22 ítems (Lotes 0 y 1 cerrados; Lote 3 con DEF-165 pendiente).

**Ítems nuevos de este plan.** Surgen de las invariantes adoptadas (D79) y de hallazgos de la auditoría que no tenían ID:
- **DEF-159** · Medio · P6: reparto del presupuesto de respuestas.
- **DEF-160** · Bajo · P7: borrado atómico.
- **DEF-161** · Bajo · P10: el relay de una sala recreada.
- **DEF-162** · Bajo · P11: subida de un chunk confirmada fuera de rango.
- **DEF-163** · Bajo · Designación congelada por una subida ajena.
- **DEF-164** · Bajo · R5: la marca de hueco no cubre los snapshots por debajo del cursor que lo encontró (revisión de delta del Lote 1).
- **DEF-165** · Medio · S1, S6 y L2: un `open_room` soltado borra tarde un `.wal.compacting` ajeno (revisión de delta del Lote 3).

**Por qué este orden, en una línea por lote:**
0. Sin las redes de prueba en el repo, cada lote se revisa a mano. Las redes viven en copias que se pueden perder.
1. DEF-146 es el defecto más probable en un uso real, y los tres juntos son menos de 50 líneas.
2. Es todo el camino de apertura del log del servidor más el `fsync` de storage. Cambia el formato de los metadatos, así que va una sola vez y antes de la Fase 4.
3. Storage es el motor del cliente de la Fase 4.
4. Defensas contra miembros hostiles y escritores sostenidos; ninguna pierde commits.
5. Disparadores raros sin pérdida de commits.
6. La documentación tiene que reflejar los lotes 1 a 5.

---

## Lote 0 — Contrato y redes de prueba

> Objetivo: que `INVARIANTES.md` refleje las decisiones de esta fase y que las redes de prueba de la auditoría queden en el repo antes de tocar código.

#### Contrato · ✅ Hecho
**Solución.** Aplicar en `INVARIANTES.md`:
- D79, las invariantes nuevas con su ID definitivo, y los ajustes de enunciado;
- D70 (INV-Y7) y D71 (INV-Y8);
- D72 y P8 como riesgos aceptados en §4;
- D77 (el hueco de Linux) como riesgo aceptado;
- D78 (excepción de INV-D2).

Mismo commit: la regla del SDK de D70 en ARCHITECTURE §6, y la corrección de §7.2 *Ordering* (un commit rechazado puede emitir `snapshot_wanted`).
**Test exigido.** Ninguno (documentación). Revisión cruzada de cada enunciado nuevo contra el código actual: si una invariante nueva no se cumple todavía, se marca con el lote que la va a cumplir.

**Hecho.** `INVARIANTES.md`, versión 2:
- **50 invariantes, 22 núcleo.** Las nuevas, con su ID definitivo:

  | ID | Origen |
  |---|---|
  | INV-D6 | P1 |
  | INV-D7 | P2 |
  | INV-D8 | P3 |
  | INV-D9 | P4 |
  | INV-Y9 | P5 |
  | INV-B5 | P6 |
  | INV-L6 | P7 |
  | INV-S6 | P9 |
  | INV-R6 | P10 |
  | INV-R7 | P11 |

  P12 y P13 se sumaron a los enunciados de INV-B2 e INV-B1.
- **Columna de estado** en el resumen, con el estado de cada invariante en `50fe195` y el lote que la cierra (§1.5). Las que todavía no se cumplen dicen *Pendiente* en "Sostenida por".
- **Ajustes de enunciado** de la auditoría.
- **Cuatro riesgos nuevos en §4:** D72, D77, P8 y D78.

En `ARCHITECTURE.md`: la regla del SDK de D70 (§6) y la corrección de §7.2 *Ordering*.

#### Redes de prueba · ✅ Hecho
**Solución.** D88. Traer de las copias de la auditoría las redes que pasan en `50fe195`:

| Red | Copia | Invariantes |
|---|---|---|
| Simulaciones de entrega y paginación con oráculo (`audit_y2_*`) | `inv/y` | Y1, Y2 |
| Convergencia entre motores (`audit_y8_*`) | `inv/y` | Y8 |
| Simulación de onboarding (`audit_sim`) | `inv/r` | R1, R2, R4, R5 |
| Imágenes de crash e inyección de errores, con el gancho `CrashPlan` en `fail_point.rs` de storage | `inv/s` | S1, S2, S3, D3 |
| Daño aleatorio de WAL | `inv/s` | D3 |
| Fuzzer de decoders (como proptest) | `inv/ew` (`crates/fuzzw`) | W1 |
| Proptests de roundtrip y de evolución de esquema | `inv/ew` | W2, E1 |
| Matriz endpoint × credencial y body sin token | `inv/a` | A1, A2 |
| Poda que falla a mitad del prefijo (`audit_r2_*`) | `inv/r` | R2 |

Los tests de ruptura (los que fallan en `50fe195`) no entran acá: entran con su fix.
**Test exigido.** `cargo test --workspace` en verde, sin sumar más de ~30 s. Los largos, como `#[ignore]`, también en verde (`cargo test --workspace --release -- --ignored`). Para cada red, una mutación conocida que la haga fallar (por ejemplo, la de mover el borrado de `.wal.compacting` antes del rename, que la simulación de §3.2 detectó).

**Hecho.** Todas las redes de la tabla entraron con nombres que describen la propiedad, en la estructura de `GEMINI.md`, sin dependencias nuevas, sin `unsafe` y sin cambios de producción. Los únicos agregados fuera de los tests son el gancho de crash plans en el `fail_point.rs` de storage y un `mod test_support` en el `lib.rs` del servidor, los dos bajo `#[cfg(test)]`.

**Dónde quedó cada cosa:**
- **Storage:**
  - `src/disk/tests/simulation.rs`: imágenes de crash, inyección de errores, lectores concurrentes y daño del WAL.
  - `tests/replica_convergence_tests.rs`.
  - `tests/randomized_engine_tests.rs`.
  - `tests/common/rng.rs`.
- **Core:** `tests/decoder_fuzz_tests.rs`, `tests/format_roundtrip_tests.rs`, `tests/schema_evolution_tests.rs` y `tests/common/mod.rs`.
- **Servidor:**
  - simulaciones de entrega, paginación, poda y byte-flip en `log/tests/tiered_log.rs`;
  - catch-up de commits, Y4 (respawn), Y5, Y6 y onboarding en `actor/tests/room.rs`;
  - fuzz de sidecars y del lector de frames en `log/tests/segment_index.rs` y `segment_reader.rs`;
  - fuzz del registry en `tests/schema_registry.rs`;
  - la matriz de credenciales en `tests/credential_matrix_tests.rs`;
  - el control del `DELETE` por TCP en `api/tests/control_plane.rs`.

  Un solo PRNG compartido por los tests del crate: `src/tests/support.rs`.

**Números:**
- **Tests:** 758 pasan y 36 ignorados; antes del lote eran 715.
- **Suite normal:** 96,6 s, igual que antes (96,4 s). Las versiones cortas suman unos 15 s de CPU, pero corren en paralelo con los tests de integración que ya marcaban el tiempo.
- **Versiones largas:** 36 de 36 en verde, en ~7 min de tests más ~4,5 min de compilación en release.
- **Mutaciones:** unas 40, entre los tres agentes de integración y la matriz. Cada red detecta al menos una.
  - Cuatro mutaciones del log solo las detectan versiones largas o tests que ya existían: salto en `DeltaBatch::extend`, presupuesto que no se agota, poda sin ancla y `expire` con `continue`.
  - Ninguna mutación de producción quedó sin detectar.

**Qué no entró:**
- **Medición de memoria de `b.patch`:** requiere `unsafe`.
- **Daño de `.wal.compacting`:** falla en 2 de 300 semillas por el patrón de DEF-151. Entra con el Lote 3.
- **`audit_y7_*`:** pasa, pero documenta un comportamiento que D70 deja al SDK.

**Nota para el workflow semanal.** La simulación de onboarding no es del todo determinista: la demanda y la designación vencen en tiempo real, así que su trayectoria depende de la velocidad de la máquina. Exige cobertura mínima en conjunto. Si falla en CI sin un cambio de código, revisar primero eso.

---

## Lote 1 — Arreglos chicos de alto valor

> Objetivo: cerrar los defectos más probables en uso real, que además son los más baratos.

#### DEF-146 · Medio · ✅ Hecho
**Problema.** `is_duplicate` usa `LruCache::get`, que refresca la recencia: el reintento de una mutación que está entre las últimas N puede secuenciarse otra vez, con o sin reapertura.
**Solución.** P5: `peek` en lugar de `get`. Ajustar `full_cache_evicts_the_least_recently_used_entry`, que fija el comportamiento actual, y el doc de `is_duplicate`.
**Test exigido.** Los de la auditoría:
- `audit_y4_retry_of_a_mutation_in_the_window_is_not_sequenced_again` (`inv/y`);
- `audit_l4_a_key_in_the_dedup_window_is_dropped_by_the_reopen` (`inv/l`).

Además, que el mismo reintento reciba la misma respuesta antes y después de un respawn.

**Hecho.** `is_duplicate` usa `peek` y toma `&self`. Tests:
- `full_cache_evicts_the_oldest_recorded_entry_even_if_it_was_just_found` (`src/tests/dedup.rs`) reemplaza a `full_cache_evicts_the_least_recently_used_entry`.
- `after_a_respawn_a_retry_inside_the_dedup_window_is_recognised` (Lote 0, `actor/tests/room.rs`) pasa a llamarse `a_retry_inside_the_dedup_window_is_recognised_before_and_after_a_respawn`. Con la misma historia (1, 2, reintento de 1, 3), el reintento de 2 recibe la seq 2 antes y después del respawn, sin mover el head. Es el test de `inv/y`, y cubre "la misma respuesta antes y después de un respawn".

**Desvío.** El de `inv/l` no se trajo: afirmaba que la mutación 1 seguía en la ventana antes de reabrir, y con P5 no sigue. Lo que quería probar (la misma ventana antes y después de reabrir) queda cubierto por el test anterior.

Los dos tests fallan en `main`. Mutación verificada: con `get` fallan los dos.

#### DEF-147 · Medio · ✅ Hecho
**Problema.** El chunk 0 sirve un snapshot que la sala ya marcó como por debajo de un hueco.
**Solución.** `GetLogBounds` devuelve también si el snapshot activo es usable (la marca de D62). El chunk 0 sin anclar lo trata como no usable: lo descarta y responde `RoomNotFound`, igual que a un snapshot fuera de rango.
**Test exigido.** `audit_r5_chunk_zero_serves_a_snapshot_below_a_known_gap` (`inv/r`, o su gemelo en `inv/delta`).

**Hecho.** `GetLogBounds` y `RoomManager::log_bounds` responden con `relay::LogBounds`, que suma `unrestorable_snapshot` (la marca de D62). `LogBounds::admits` la excluye. Así, el chunk 0 sin anclar descarta el snapshot marcado, y una subida nueva en esa secuencia se rechaza:
- con `BadRequest` al empezar o en un solo request;
- con `SnapshotSuperseded` al completarse.

**Test.** `snapshot_below_a_gap_stops_being_usable` (`actor/tests/room.rs`), extendido con el escenario de `inv/r`:
- después del hueco, el chunk 0 responde `RoomNotFound` y el relay se queda sin snapshot;
- volver a subir en 2 recibe `BadRequest`;
- el snapshot nuevo en 6 se sirve, porque una marca en otra secuencia no lo descarta.

Falla en `main`. La simulación de onboarding ahora afirma que el chunk 0 nunca sirve el snapshot marcado; antes solo lo contaba.

**Mutaciones verificadas:**
- `admits` sin la marca;
- solo el chunk 0 mirando la marca;
- el actor informando `None`.

**Desvíos.**
- **La marca es la secuencia, no un booleano.** El chunk 0 la compara con la del snapshot que leyó del relay. Si ese snapshot se reemplaza entre las dos lecturas, no se descarta el nuevo por un booleano que hablaba del anterior.
- **El snapshot se borra y su secuencia se rechaza.** D62 dejaba el snapshot marcado en el relay hasta que lo reemplazara otra subida o el TTL, y eso impedía volver a subir en la misma secuencia. Al descartarlo, una subida en esa secuencia se confirmaría sin ser usable (INV-R7), así que `admits` la rechaza.

#### DEF-155 · Bajo · ✅ Hecho
**Problema.** Una subida en un solo request de otro miembro, con la misma seq, descarta una subida por chunks en curso.
**Solución.** `stage_snapshot` recibe el uploader. Si hay una subida con la misma seq que no le cede el lugar (`yields_to`), responde `SnapshotSuperseded`, la misma regla que ya rige para los chunks. Hay un prototipo validado en `inv/a/fix`.
**Test exigido.** `inv_a3_foreign_upload_at_the_same_seq_survives_a_single_request_upload` (`inv/a`), contra la subida de otro miembro y contra la del worker.

**Hecho.** `stage_snapshot` recibe el `Uploader` (el handler lo toma de `RelayAuth`). La regla se chequea bajo el lock del slot, después de vencer las subidas inactivas y del reintento idempotente.

**Test.** `single_request_upload_replaces_an_upload_at_the_same_seq_only_where_a_chunk_could` (`tests/snapshot_relay_tests.rs`), el de `inv/a`, en una tabla:
- **Contra otro miembro y contra el worker:** 409, y la subida por chunks termina y es la que se sirve.
- **Control, lo que la regla sigue dejando pasar:** la subida del mismo miembro y la del worker reemplazan a la que estaba en curso, y la de otro miembro también si su secuencia es mayor.

Falla en `main` en los dos primeros casos. Mutaciones verificadas: sin la guarda, con una guarda que ignora `yields_to` y con una que ignora la secuencia.

**Cierre del lote.**
- **Chequeos:** fmt, clippy, tests y doc en verde. 759 tests pasan y 36 están ignorados; antes del lote eran 758. Los tests nuevos se sumaron casi todos a tests existentes que ya tenían el mismo escenario.
- **Versiones largas:** `cargo test -p zemdb-server --release -- --ignored`, 11 de 11 en verde.
- **Revisión de delta:** la hizo un agente aparte, de contexto fresco y solo lectura.
  - Los tres fixes cierran su escenario, y deshacer cada uno hace fallar un test.
  - El diff no rompe ninguna invariante núcleo.
  - Lo que encontró y entró en el lote: la subida en la secuencia marcada (ver DEF-147); el caso de otra secuencia en el test de DEF-155; la aserción en la simulación; y retoques de documentación.
  - Lo que no entró: DEF-164 (preexistente, Lote 4) y una nota para los tests de DEF-153.
- **Documentos:**
  - `ARCHITECTURE.md`: §5.1 punto 4 y §5.2 (*Opening a Room* y *Damaged Segments*) para la ventana de dedup y la marca de hueco, y §6 para el chunk 0 y las subidas en un solo request. Esto adelanta el punto de §5.1 del Lote 6.
  - `INVARIANTES.md`: Y4, Y9 y A3 en ✅; R5 queda ❌ por DEF-164 (Lote 4); L4 queda solo para el Lote 2.

---

## Lote 2 — Abrir solo lo verificado y sincronizado

> Objetivo: que abrir una sala nunca confirme lo que ningún `fsync` cubrió, nunca borre a partir de datos sin verificar y nunca reutilice una secuencia. Cambia el formato de los metadatos del servidor.

#### DEF-144 · Alto · ⬜ Pendiente
**Problema.** Reabrir después de un `fsync` fallido confirma registros y entradas de directorio que ningún `fsync` cubrió (P2).
**Solución.** D77, en el servidor y en storage.
**Test exigido.**
- `audit_d1_retry_after_failed_fsync_is_acknowledged_without_being_durable` (`inv/d`), adaptado: el reintento solo se confirma después de un `sync_all` exitoso en esta apertura. Comprobarlo con un fail point que cuente los syncs.
- Los tres tests de entradas de directorio de `inv/l` (`active.wal`, `rooms/<id>` y el `data_dir` de storage).
- `failed_commit_sync_stops_room_and_recovers_from_disk`, ajustado: hoy da por correcto el comportamiento viejo.

#### DEF-145 · Alto · ⬜ Pendiente
**Problema.** Ningún JSON persistido tiene checksum: un bit invertido en `log_meta.json` borra segmentos al abrir, y en un esquema renumera una tabla (P4).
**Solución.** D82.
**Test exigido.**
- `audit_single_bit_flip_in_log_meta_deletes_retained_segments` y `audit_single_bit_flip_in_a_schema_file_renumbers_a_table` (`inv/ew`), o `audit_damaged_log_meta_deletes_retained_segments` (`inv/delta`): el archivo dañado hace fallar la apertura y no se borra nada.
- Un bit invertido en `meta_room.json` hace fallar la carga; en el roster, se descarta.
- Roster con UTF-8 inválido: se descarta.
- Ajustar `log_claiming_the_last_sequence_number_fails_to_open`.

#### DEF-148 · Alto · ⬜ Pendiente
**Problema.** Perder el segmento sellado más nuevo hace retroceder `head_seq` y reutiliza secuencias (P3).
**Solución.** D81.
**Test exigido.** `audit_d2_lost_newest_segment_reuses_sequence_numbers` (`inv/d`): después de perder el segmento, `head_seq` no retrocede y la operación siguiente recibe una secuencia nueva. Lo mismo si se pierde el segmento y después se poda todo.

#### DEF-158 · Bajo · ⬜ Pendiente
**Problema.** Un segmento dañado sin sidecar corta la hidratación del dedup, aunque los segmentos más viejos tengan sidecars válidos.
**Solución.** Saltear el segmento dañado (con el warning de hoy) y seguir con los anteriores.
**Test exigido.** Ajustar `open_tolerates_a_corrupt_newest_segment`: se recuperan los ids de los segmentos anteriores con sidecar.

#### DEF-149 · Medio · ⬜ Pendiente
**Problema.** Un segundo proceso sobre el mismo `data_dir` puede borrar y recrear una sala que posee el primero (P1).
**Solución.** D83.
**Test exigido.**
- `audit_d5_second_process_deletes_a_room_owned_by_the_first` (`inv/d`), adaptado: el segundo servidor no arranca.
- Un `delete_room` sobre una sala con `log.lock` tomado falla sin borrar nada.

---

## Lote 3 — Storage: cancelación y absorción

> Objetivo: que el motor de disco del cliente no pierda lotes confirmados por una cancelación y que un crash durante la absorción no deje una sala que no abre.

#### DEF-150 · Medio · ✅ Hecho
**Problema.** El `rename` de un `apply_snapshot` cancelado aterriza después de reabrir y hace perder lotes que la sala reabierta confirmó.
**Solución.** D86. Incluye el borrado de `.wal.compacting` de `install_applied_snapshot`, que tiene la misma carrera.
**Test exigido.** `audit_l2_rename_of_a_dropped_apply_snapshot_lands_in_the_reopened_room` (`inv/l`, con sus dos puntos de pausa). Además, el barrido de cancelación en cada punto de suspensión de `apply_snapshot` (`audit_l2_apply_snapshot_dropped_at_every_suspension_point`), que pasa hoy y tiene que seguir pasando.

**Hecho.** Desde el primer chequeo del head hasta reemplazar la memoria, `apply_snapshot` corre en un `tokio::spawn` que retiene el lock de compactación.
- **Tests:**
  - `snapshot_dropped_during_its_rename_is_installed_before_the_room_reopens` (`src/tests/disk.rs`) es el de la auditoría, con sus dos pausas: `apply_snapshot.rename`, bloqueante dentro del rename, y `recovery.snapshot_loaded`. En `main`, la reapertura carga el snapshot viejo (head 3) con el rename en vuelo.
  - El barrido sigue en verde y ahora además exige que la sala quede usable. Para eso, `check_after_drop` espera el lock de compactación antes de mirar la sala.
- **Mutación:** correr la instalación otra vez en el future de quien llama hace fallar los dos tests. El primero lo detecta si la reapertura llega a cargar el snapshot en 200 ms; el barrido no depende del tiempo.
- **Desvíos:**
  - `rename_staged_snapshot` usa `std::fs::rename` en `run_blocking`, que es lo que hace `tokio::fs::rename`, para poder pausar dentro del pool bloqueante.
  - Quien suelta el future ya no deja la sala `RoomFailed`: el snapshot se aplica entero.

#### DEF-151 · Medio · ✅ Hecho
**Problema.** Un crash durante la absorción en `.wal.compacting` deja una sala que no reabre (falla cerrada).
**Solución.** D85.
**Test exigido.**
- Los tres casos `inv_s3_torn_absorb_*` (`inv/s`): la sala reabre con todos los lotes.
- `crash_while_absorbing_recovers_the_damaged_copy_from_the_active_wal`, adaptado a la nueva forma de absorber.
- La simulación de imágenes de crash del Lote 0 en verde.

**Hecho.** La absorción escribe `.wal.compacting` más el WAL activo en `room_{id}.wal.compacting.tmp`, lo sincroniza, lo renombra sobre `.wal.compacting` y sincroniza el directorio. Recién después vacía el WAL activo. La recuperación borra el `.tmp` sin leerlo.

Se fueron `append_synced`, `highest_seq_in_valid_frames`, la excepción de corrupción y `ReplayEnd`: la clasificación devuelve el error de corrupción directamente. El código de producción del lote suma −65 líneas netas.
- **Tests:**
  - `crash_at_any_point_of_an_absorption_leaves_the_rotated_wal_whole` toma imágenes de crash en cada punto de una absorción real (solo en Unix). En todas, `.wal.compacting` está entero, y un handle abierto antes de compactar sigue leyendo el archivo viejo. En `main` falla en `compaction.absorb_write`, a medio escribir.
  - `crash_while_absorbing_leaves_a_copy_that_recovery_discards` es el adaptado de `crash_while_absorbing_recovers_...`. Junto con `damaged_rotated_wal_followed_by_valid_frames_is_corruption`, falla en `main`.
  - Fallas antes y después del rename: `failed_write_of_the_absorbed_copy_keeps_the_active_wal`, `failed_rename_of_the_absorbed_copy_keeps_the_active_wal` y `failed_directory_sync_after_absorbing_marks_room_failed_until_reopened`.
  - El daño de `.wal.compacting` que había quedado del Lote 0 entra como variante `rotated` de la simulación de WAL dañado: `damaged_rotated_wal_fails_untouched`, más su versión larga.
  - La simulación de imágenes de crash del Lote 0 sigue en verde.
- **Mutaciones:** cada una de estas hace fallar un test:
  - el código de `main`;
  - escribir en el lugar con una sola escritura;
  - vaciar el WAL activo antes del rename;
  - ignorar el error del rename o del sync del directorio;
  - no borrar el `.tmp`, al fallar o al abrir.

  Sacar un `fsync` no se detecta, como en el resto del crate: las imágenes copian la caché de páginas.
- **Desvíos:**
  - **Los tres `inv_s3_torn_absorb_*` no entran tal cual,** porque su estado de crash ya no existe. La copia rota queda en el `.tmp`, que la recuperación borra sin leer, así que su contenido (un frame embebido, payloads dañados) no influye. Los cubren el test de imágenes y el de la copia descartada.
  - **Dos tests se borraron:**
    - `crash_while_folding_a_damaged_absorb_recovers_on_the_next_open`, porque su estado de partida ahora es corrupción;
    - `failed_background_append_to_compacting_wal_keeps_the_active_wal`, porque la copia se escribe con `std::fs` en el pool bloqueante y no hay escrituras de fondo.
  - **Un `.wal.compacting` dañado con frames válidos después es `WalCorruption`,** aunque el WAL activo tenga esos registros.

#### DEF-165 · Medio · ⬜ Pendiente
**Problema.** La recuperación de `open_room` corre en el future de quien llama. Si ese future se suelta mientras la recuperación borra un `.wal.compacting` ya plegado, el lock de la sala se libera con el borrado todavía en el pool bloqueante. Otra apertura toma la sala y confirma lotes, y una compactación que falla los deja en un `.wal.compacting` nuevo. El borrado tardío se lleva ese archivo, y los lotes se pierden al reabrir (S1, S6, L2). Es preexistente: lo encontró la revisión de delta de este lote.
**Solución.** D89.
**Test exigido.**
- El de la revisión de delta, con una pausa bloqueante en ese borrado: la apertura soltada no borra el `.wal.compacting` de la sala que la sucede. Hoy la reapertura da head 8 en lugar de 10.
- Además, el barrido de cancelación de `open_room` en cada punto de suspensión.

**Revisión de delta del lote.** La hizo un agente aparte, en modo solo lectura. Conclusión: los dos fixes cierran sus escenarios y el código no rompe otras invariantes.
- **Hallazgos que entraron al lote:**
  - un test leía el WAL activo con su lock tomado, lo que falla en Windows;
  - dos mutaciones no tenían test: la escritura en el lugar de una sola vez y el error del rename ignorado;
  - el barrido de snapshots aceptaba `RoomFailed`;
  - faltaba un timeout en un test;
  - dos enunciados de `INVARIANTES.md` decían de más.
- **Hallazgo fuera del lote:** DEF-165.

## Lote 4 — Recursos

> Objetivo: que ningún cliente pueda retener memoria, disco ni la designación más allá de cotas configuradas.

#### DEF-152 · Alto · ⬜ Pendiente
**Problema.** Una respuesta materializa hasta ~32 veces su tamaño de wire, la HotBuffer ocupa ~24 veces `ram_max_bytes`, y `Schema` y `Registered` no toman presupuesto.
**Solución.** D73.
**Test exigido.**
- Con el esquema de 4.096 columnas de la auditoría:
  - un `/sync` no materializa más de `MAX_MESSAGE_VALUES` valores;
  - la HotBuffer no supera `ram_max_bytes` en valores × 24.

  Se miden contando valores, no con un asignador.
- 64 respuestas `/schema` sin leer no superan el presupuesto: las de más reciben `Unavailable`.
- El presupuesto se pide antes de codificar.

#### DEF-159 · Medio · ⬜ Pendiente
**Problema.** El presupuesto de bytes de respuesta en vuelo es un único semáforo global: un miembro de una sala que no lee sus respuestas deja con 503 los `/sync` de todas las demás (P6).
**Solución.** D75.
**Test exigido.**
- Un cliente que retiene respuestas hasta su tope recibe `Unavailable`, mientras otro cliente de otra sala sigue recibiendo sus `/sync`.
- Al soltar los cuerpos, el tope se libera.

#### DEF-153 · Medio · ⬜ Pendiente
**Problema.** Reenviar un chunk ya recibido renueva el plazo de una subida: la subida, la designación y el ancla quedan retenidas sin plazo (P12).
**Solución.** D74.
**Test exigido.**
- `inv_b2_upload_resending_the_same_chunk_is_retained_without_deadline` (`inv/b`), adaptado: el reenvío no renueva el plazo y a los 2 minutos la subida se descarta.
- Una subida que progresa pero supera `snapshot_upload_max_secs` se descarta.
- Los dos casos con tiempo pausado.
- Un miembro que reenvía un chunk retiene la secuencia también contra las subidas en un solo request de los demás (desde el Lote 1 siguen la misma regla): con D74, a los 2 minutos la secuencia queda libre.

#### DEF-163 · Bajo · ⬜ Pendiente
**Problema.** Cualquier subida en curso, de cualquier miembro, congela la designación del uploader.
**Solución.** D76.
**Test exigido.**
- Con una subida en curso de un miembro que no es el designado, el designado que no sube a tiempo se reemplaza a los 60 s.
- Con una subida en curso del designado, la designación no cambia (`designee_that_does_not_upload_in_time_is_replaced_but_never_during_an_upload`, ajustado).

#### DEF-162 · Bajo · ⬜ Pendiente
**Problema.** Una subida de un solo chunk no queda anclada entre la consulta del rango y el lock del slot, y se completa validando contra ese rango viejo. Puede responder `staged: true` con un snapshot que ya no es usable (P11).
**Solución.** Validar el rango bajo el lock del slot con un rango leído después de tomarlo (o anclar la subida antes de pedir el rango, como la de un solo request). Si el snapshot ya no es usable, `SnapshotSuperseded`.
**Test exigido.** `audit_one_chunk_upload_staged_after_pruning_passed_it` (`inv/r`), adaptado: responde `SnapshotSuperseded`.

#### DEF-164 · Bajo · ⬜ Pendiente
**Problema.** La marca de D62 guarda la secuencia del snapshot activo cuando un sync encuentra el hueco, no el cursor del sync. Un snapshot que se sube después con una secuencia entre la marca y ese cursor se anuncia y se sirve, aunque restaurarlo lleve al mismo hueco (INV-R5). Se corrige solo: el próximo cliente que choca con el hueco marca el nuevo. Bajo, y no Medio como la invariante, porque además hace falta un daño externo del log.
**Solución.** Guardar como marca el mayor `from_seq` de los syncs que encontraron un hueco, y tratar como no usable todo snapshot con S ≤ marca: en `usable_snapshot_seq` y en `LogBounds::admits`.
**Test exigido.** Con `small_segments`: commits 1 a 8, snapshot en 2, ack 8, se borra el segmento 5–6. Un cliente en 4 hace sync y recibe `BehindCompaction` (marca en 2). Un snapshot en 3 se acepta, `Registered` lo anuncia y el chunk 0 lo sirve, pero un sync desde 3 recibe `BehindCompaction`. Reproducido sobre el Lote 1 con un test temporal. Con el fix, la subida en 3 se rechaza.

#### DEF-154 · Medio · ⬜ Pendiente
**Problema.** La cuota no acota el disco de una sala: solo poda cold, la compresión está topada y los sidecars no cuentan.
**Solución.** D69, o su opción de respaldo si supera el tamaño estimado.
**Test exigido.** Los tres casos de `inv_b3` (`inv/b`): con cuota de 1 MiB y un escritor sostenido, `disk_bytes` (que ahora incluye los sidecars) se estabiliza por debajo de la cuota más `active.wal` y lo escrito en una pasada. Además, que la poda por cuota sea siempre un prefijo, incluso con un warm que no se pudo comprimir delante (el test de DEF-132, ajustado).

#### DEF-157 · Bajo · ⬜ Pendiente
**Problema.** `fetch_from_disk` recorre el índice de segmentos desde el inicio.
**Solución.** Buscar el primer segmento con `BTreeMap::range`. La apertura sigue siendo O(segmentos): se acepta en el enunciado de INV-B4 (D79). Con la cuota de D69, la cantidad de segmentos queda acotada.
**Test exigido.** El de `inv/b` (`inv_b4_open_and_sync_cost_vs_segment_count`) como medición `#[ignore]`: el costo del sync no crece con la cantidad de segmentos.

---

## Lote 5 — Ciclo de vida

> Objetivo: que borrar una sala y apagar el servidor nunca dejen estados a medias.

#### DEF-156 · Bajo · ⬜ Pendiente
**Problema.** El apagado ordenado corta a la mitad un `DELETE` que corre en su propia tarea.
**Solución.** D87.
**Test exigido.** `audit_l2_dropped_room_deletion_is_cut_halfway_by_an_orderly_shutdown` (`inv/l`): el borrado termina antes del apagado. El test de control tiene que seguir pasando.

#### DEF-160 · Bajo · ⬜ Pendiente
**Problema.** Un `remove_dir_all` interrumpido deja salas a medias. Si queda el log sin `meta_room.json`, la sala recreada hereda el log viejo bajo una encarnación nueva (P7).
**Solución.** D84.
**Test exigido.**
- El test de `inv/l` (`actor/tests/manager.rs`, borrado parcial): con un crash en cada paso del borrado, la sala existe entera o no existe.
- Una sala recreada después de un borrado interrumpido empieza vacía.

#### DEF-161 · Bajo · ⬜ Pendiente
**Problema.** Si una sala se recrea sin pasar por `delete_room` (por ejemplo, porque se perdió su directorio), el snapshot viejo del relay se vuelve usable cuando el head nuevo alcanza su secuencia (P10).
**Solución.** D84 (`create_room` purga el relay de ese id).
**Test exigido.** Crear una sala con un snapshot del relay de una sala anterior del mismo id: el snapshot no se ofrece nunca.

---

## Lote 6 — Documentación

> Objetivo: que `ARCHITECTURE.md`, el README y el ROADMAP describan lo que cambiaron los lotes 1 a 5.

**Solución.**
- **`ARCHITECTURE.md`:**
  - §5.1: la ventana de dedup como función del log.
  - §5.2: el `fsync` al abrir; el checksum de los metadatos; el piso de secuencia; la cuota sobre warm y sidecars; el lock de `data_dir`.
  - §6: el plazo y el tope de las subidas; la designación; las subidas de un chunk; el borrado con lápida.
  - §7.2: `Unavailable` en `Schema` y `Registered`.
  - §7.3: los settings nuevos y `max_send_buf_size`; corregir el "at most `max_sse_streams`" (el reemplazo puede superarlo en uno por cliente).
  - §8: la absorción atómica y `apply_snapshot` en su propia tarea.
- **README:** las variables nuevas. **ROADMAP:** la Fase 3.9.
- **Riesgo para el futuro:** dejar anotado en el ROADMAP el riesgo de D77 (la reescritura de `active.wal` después de un EIO).

**Test exigido.** Ninguno. Revisión cruzada de cada afirmación cambiada contra el código.

---

## Fuera de este plan

Siguen abiertos y diferidos, sin cambios respecto de sus planes:
- **DEF-85:** la ventana de dedup tiene un límite. P5 (DEF-146) la vuelve coherente, pero no la amplía.
- **DEF-34** (`reload_schema` en storage), **DEF-81** (snapshots falsos) y **DEF-54** (Zstd en wasm32).
- **Residuo de DEF-95:** la conexión SSE semiabierta.
- **D77:** la reescritura de `active.wal` después de un EIO en Linux.

## Backlog Bajo

Hallazgos de la auditoría que no rompen ninguna invariante (`INVARIANTES.md` §1.2). No entran en los lotes. Se toman cuando se toque el código de al lado, o en un lote de limpieza. Los marcados con **(barato)** son de pocas líneas y pueden sumarse al lote del área sin costo.

**Log y metadatos del servidor**
- **(barato)** `seq + 1` y `end + 1` sin `checked_add` en `segment_reader.rs` y `cold_disk.rs`; `version += 1` sin `checked_add` en `Schema::add_column`.
- **(barato)** `create_dir_all_synced` del servidor acepta `AlreadyExists` sin comprobar que sea un directorio (`durable.rs`).
- `TieredLog::append` acepta cualquier secuencia si `head_seq == 0`, y devuelve `Err` si la HotBuffer falla después de que el registro ya es durable. Las dos cosas son inalcanzables hoy.
- `rotate_active_segment` indexa un segmento inexistente si `active.wal` desaparece con la sala abierta.
- Un roster de 0 bytes se carga vacío sin warning.
- `ClientLeaseTracker::open_or_create` no acota los cursores del roster a `head_seq`.
- `SchemaRegistry::persist`: si falla el sync del directorio después del rename, el disco queda adelante de la caché.
- Un symlink roto en `schemas/` hace fallar el arranque (D67): documentarlo.

**Protocolo, relay y auth**
- **(barato)** `verify_client_token` hace `split('.').collect()` antes de contar las partes; usar `splitn`.
- El prefijo `Bearer` es opcional para los tokens de cliente y obligatorio en `AdminAuth`.
- `ct_eq` sobre slices de distinto largo revela por timing si el largo del admin secret coincide; se pueden comparar hashes.
- `stage_snapshot` y `stage_chunk` crean el slot del relay antes de confirmar que la sala existe.
- Los parsers de nombres de archivo del relay y de segmentos aceptan números no canónicos (`05`, `+5`).
- `query_token` no decodifica percent-encoding: documentarlo.
- Un reinicio del servidor durante una subida por chunks hace que el último chunk responda `staged: false`, sin forma de saber qué chunks faltan.
- Un `Connected` que choca con un hueco en `sync` sigue `Connected` (no se llama a `observe`).
- `check_newer_than_active` compara contra el snapshot activo aunque ya no sea usable.
- La marca de hueco solo se pone si S ≤ `from_seq`.
- Los 404/405 binarios hacen eco del path completo (hasta 65 KB).
- No hay presupuesto global para los bodies de request, y `Bytes::from_request` copia el body (pico de 2×).

**Disponibilidad**
- La apertura de la sala corre en la tarea de la request, fuera del timeout de `ask`.
- El lock por sala y la E/S del relay no están acotados por `ACTOR_TIMEOUT`.
- Después de que `serve_until_shutdown` devuelve, soltar el runtime no está acotado a 10 s.
- `Shutdown` no cierra el mailbox: los comandos posteriores reciben 503 en lugar de reenviarse.
- `dropped_reply_error` usa una heurística de 1 s: con el runtime saturado, un pánico puede responder 503 en lugar de 500.
- Los comandos encolados antes de un cierre por inactividad responden con la versión de esquema vieja.

**Esquema y formatos**
- `ColumnDef`, `AddColumnRequest` y `CreateSchemaRequest` aceptan campos desconocidos, y `primary_key: ["id","id"]` se acepta.
- En `.snap`, un CRC de payload igual a 0 funciona como "sin CRC".
- `Value::Bytes` pre-reserva hasta 1 MiB según la longitud declarada.
- Los snapshots no son canónicos: el orden del `HashMap` hace que el mismo estado dé bytes y hashes distintos entre motores.
- `create_snapshot` puede producir payloads que `apply_snapshot` rechaza con el límite por defecto, y `apply_snapshot` ocupa ~6× el payload descomprimido.
- El límite de ventana de zstd no está fijado explícitamente para snapshots de otros peers.
- `run_blocking` convierte un pánico en `StorageError::Other`, que no es un error tipado.

**Storage**
- Un WAL que falta con el snapshot presente se recrea sin sincronizar el directorio (cubierto en parte por D77).
- Un batch de más de 16 MiB codificado se rechaza como `WalCorruption`, que es un error engañoso.
- Un crash entre el rename de `apply_snapshot` y el vaciado del WAL deja registros viejos en el WAL activo hasta la próxima compactación (inocuo; infla `wal_len`).

**Tests**
- `concurrent_commits_are_all_sequenced_and_signalled` solo cuenta las operaciones; la simulación del Lote 0 cubre el contenido.
- Faltan tests de un ack con un hueco después de un cursor válido, y de un sync servido que el presupuesto convierte en `Unavailable` (el cursor avanza igual).
