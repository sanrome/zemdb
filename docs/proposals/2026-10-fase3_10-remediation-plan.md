# Plan de Remediación — Fase 3.10

**Fecha:** 18 de Octubre de 2026
**Origen:** [`docs/audits/2026-10-invariantes-audit-2.md`](../audits/2026-10-invariantes-audit-2.md) (DEF-167 a DEF-181, más los ajustes y propuestas de sus §5 y §6)
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md)
**Plan anterior:** [Fase 3.9](2026-10-fase3_9-remediation-plan.md) (D69 a D89, DEF-144 a DEF-166)
**Tipo:** Documento vivo (plan + seguimiento). El detalle de cada defecto (dónde, escenario, evidencia) está en la auditoría; acá van el orden, las decisiones, la solución y el test exigido.

---

## 0. Cómo usar este documento

- Los defectos están agrupados en **lotes** ordenados por prioridad de ejecución. Cada lote es una unidad de trabajo revisable (uno o pocos commits) de **~200 líneas de producción como máximo**; si un lote las supera, se parte.
- Cada ítem tiene: severidad, estado, solución, test exigido y las interacciones a revisar.
- Al cerrar un ítem se actualiza su estado aquí y se anota el commit.

**Estados:** ⬜ Pendiente · 🟡 Parcial · ✅ Hecho · ⏸ Diferido · ❌ Descartado

**Reglas de trabajo** (las del plan de la Fase 3.9, más dos nuevas):
1. Tests según la estructura de dos niveles de `GEMINI.md`, con al menos un test negativo que reproduzca el fallo y que falle sin el fix. La auditoría dejó ese test para todos los ítems. Está en el parche de su grupo en [`docs/audits/2026-10-invariantes-audit-2/`](../audits/2026-10-invariantes-audit-2/README.md); cuando este plan dice `inv2/<grupo>`, se refiere a `<grupo>.patch`. Se trae y se adapta (sin los nombres `audit_*`/`audit2_*` ni códigos `INV-`).
2. Sin códigos de auditoría (`DEF-xx`) en código, tests ni docstrings.
3. Cada lote cierra con `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` y `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` en verde.
4. Cada lote cierra con una **revisión de delta** contra las invariantes que toca. **Nuevo:** también contra los mecanismos existentes con los que interactúa cada fix (§0.1, paso 4).
5. Un cambio que agrega, quita o debilita una garantía actualiza `INVARIANTES.md` en el mismo commit.
6. **Nueva:** esta fase no termina en otra auditoría. Termina en la revisión de cierre del Lote 5 (D90).

### 0.1. Procedimiento por lote

Cada lote se ejecuta con el mismo ciclo, idealmente en una sesión propia, siguiendo el orden de §3. Este procedimiento es lo único que hace falta para retomar el trabajo sin el contexto de sesiones anteriores.

**Orden:**
- Los Lotes 1 y 2 van en ese orden: los dos cambian la apertura del log (`tiered_log.rs`, `warm_disk.rs`).
- Los Lotes 3 y 4 son independientes entre sí y de los anteriores, y se pueden intercambiar. Los dos tocan `relay/lifecycle.rs`, en funciones distintas.

**0. Arranque.**
- **El árbol:** `git pull`, con el árbol de trabajo limpio y la CI de `main` en verde en las tres plataformas.
- **Qué leer:** la sección del lote en este plan, las decisiones que cita (§2), las invariantes que toca en `INVARIANTES.md`, el detalle del defecto en la auditoría y `GEMINI.md`.
- **Decisiones en estado *Propuesta*:** se confirman con el dueño del proyecto antes de escribir código. Al confirmarse pasan a *Adoptada*, con los cambios que hayan surgido.

**1. Primero el test que falla.** Traer el test de ruptura del ítem desde su parche (`inv2/<grupo>`) y adaptarlo a `GEMINI.md`. Después, **comprobar que falla en el `main` actual**: si no falla, no reproduce el defecto y hay que reescribirlo.

**2. El fix mínimo.** Implementar lo que pide la decisión, sin cambios de alcance. Seguir los principios de `GEMINI.md`: validar antes de mutar, guards RAII, persistencia atómica, nada de I/O bloqueante en el runtime.

**3. Verificación.**
- **Chequeos de la regla 3.** Si el lote toca `core` o `storage`, también `cargo build -p zemdb-core -p zemdb-storage -p zemdb-client --target wasm32-unknown-unknown`.
- **Mutación inversa:** deshacer el fix (o sacar la guarda que agrega) y confirmar que el test nuevo falla. Después, restaurarlo.
- **Versiones largas, una sola vez al cerrar el lote:** `cargo test -p <crate> --release -- --ignored` de los crates tocados. Nunca se agregan a la CI de cada push (`.github/workflows/randomized.yml` las corre una vez por semana).
- **Windows:** un test no puede leer un archivo que el log o la sala tienen abierto (los locks de `fs2` son obligatorios ahí). Un test que lee `active.wal` o el WAL lo hace después de soltar el log o la sala.

**4. Revisión de delta (regla 4).**
- **Antes de pedirla, quien implementa lista las interacciones de cada fix** en su nota **Hecho**: los mecanismos que ya existían y que leen o escriben el mismo estado. Cada ítem de este plan trae una lista inicial, que no es exhaustiva. Las cuatro regresiones de la ronda anterior salieron de cuatro preguntas que nadie se hizo:
  1. **¿Agrega un valor persistido o un piso?** ¿Quién más lo lee o lo escribe (apertura, truncado de una escritura rota, rotación, poda, compresión, cuota, relay)? DEF-167: el piso de D81 frente al truncado de D78.
  2. **¿Toma o suelta un lock?** ¿Vive hasta la última operación de archivo que protege, incluidas las de `spawn_blocking` y las de las tareas que lanza? DEF-172: `server.lock` frente al `drop` del runtime.
  3. **¿Agrega o mueve un `.await`?** ¿Qué queda a medias si el future se suelta ahí? DEF-175: la purga de D84 en el future de la request.
  4. **¿Cambia el orden entre leer un estado y usarlo?** ¿Qué puede cambiar en el medio? DEF-176: el rango leído antes de elegir el snapshot.

  Para cada interacción va un test que la ejercite o el argumento de por qué no hace falta. Los tests de evidencia de `inv2/*` (los que pasan) que cubren una interacción de la lista se traen en este paso.
- **El revisor** es independiente: un agente aparte, de contexto fresco y en modo solo lectura. Recibe el diff, las invariantes que toca, los "test exigido" y la lista de interacciones. Responde cuatro cosas:
  - si cada fix cierra el escenario de la auditoría;
  - si deshacerlo hace fallar un test;
  - si la lista de interacciones está completa, y cuál falta;
  - si el diff rompe alguna otra invariante.

Como en la auditoría, sin un test que lo reproduzca no hay hallazgo Alto ni Medio, y la severidad sigue D91.

**5. Documentos, en el mismo cambio.**
- **`ARCHITECTURE.md`:** si cambió un comportamiento descrito ahí.
- **`INVARIANTES.md`:** la columna Estado pasa de ❌ o 🆕 a ✅, y se quita el *Pendiente* de "Sostenida por".
- **Este plan:** el ítem pasa a ✅, con una nota **Hecho** que lista los tests agregados, la mutación verificada, las interacciones revisadas y cualquier desvío respecto de la solución planeada. Se actualizan el resumen (§3) y el avance total.

**6. Alcance.** Lo que aparezca fuera del lote **no se arregla en el momento**:
- si rompe una invariante, se agrega a este plan como ítem nuevo (DEF-182 en adelante), con su test, en el lote que corresponda;
- si no rompe ninguna, va al backlog Bajo.

La única excepción son los ítems del backlog marcados **(barato)** que estén en los mismos archivos que toca el lote.

**7. Entrega.**
- Los cambios se dejan sin commitear para que el dueño revise el diff.
- Con su visto bueno, se arma un commit por lote, o pocos (estilo `fix(server): … (Lote N)`), sin trailers de co-autoría, y se pushea a `main`.
- El lote queda cerrado recién con la CI de `main` en verde en las tres plataformas, Windows incluido.

---

## 1. Criterios del orden

1. **Primero, el contrato y las redes de prueba (Lote 0).** `INVARIANTES.md` tiene que reflejar las decisiones de abajo, porque es el contrato contra el que se revisa cada lote. Las redes de la auditoría viven en parches y se pierden si no entran al repo.
2. **Después, las Altas de durabilidad del log (Lote 1).** DEF-167 es una regresión de la ronda anterior y pierde un commit confirmado. DEF-168 y DEF-169 son baratos y están en el mismo camino de apertura.
3. **DEF-170, solo (Lote 2).** Cambia la misma apertura que el Lote 1 y es el fix más grande del plan.
4. **Las Medias introducidas por la ronda anterior (Lote 3):** dos locks que se sueltan antes que la última operación de archivo que protegen.
5. **Las subidas (Lote 4).** Tienen disparadores hostiles, y ninguna pierde commits.
6. **El cierre (Lote 5):** documentación y revisión de cierre (D90).

**Requisito para empezar la Fase 4:** Lotes 0 a 5 cerrados, con la revisión de cierre del Lote 5 en verde (D90). No hace falta otra auditoría.

---

## 2. Decisiones de diseño

Se continúa la numeración del plan de la Fase 3.9 (que termina en D89). Las decisiones D90 a D97 son las que se tomaron sobre la auditoría (el número entre paréntesis es el de la conversación). Las D98 a D102 son de implementación: se confirman o cambian antes de arrancar el lote correspondiente. D98 se adoptó al cerrar el Lote 0, D99 al empezar el Lote 2, D100 y D101 al empezar el Lote 3, y D102 al empezar el Lote 4.

| ID | Decisión | Lote | Estado |
|---|---|---|---|
| **D90** | **La fase cierra sin otra auditoría amplia (1a).** El cierre tiene tres partes: (a) en cada lote, la revisión de delta con la lista de interacciones (§0.1, paso 4); (b) en el Lote 5, una revisión de cierre del delta completo de la fase, hecha por un agente aparte contra todas las invariantes, pero mirando solo lo que la fase tocó y con qué interactúa; (c) los tests de ruptura de todos los ítems de este plan, en el repo y en verde. Si la revisión de cierre no encuentra una ★ rota, una ruptura Alta ni una Media de origen (a), el código queda estable y arranca la Fase 4. Lo que encuentre de origen (b) o (c), Medio o Bajo, va al backlog, no a un plan nuevo. En `INVARIANTES.md` §1.4, "dos auditorías seguidas en verde" se reemplaza por esta regla: desde entonces, cada cambio se revisa por su delta contra la lista. Una auditoría amplia vuelve a hacer falta solo si cambia el modelo de amenaza (§2 de `INVARIANTES.md`), o si entra código fuera del alcance actual, como el SDK de la Fase 4; en ese caso se audita una vez, con el mismo método, contra las invariantes nuevas de ese código. | 0, 5 | Adoptada |
| **D91** | **Severidad con doble disparador raro (2a).** Una ruptura que necesita dos disparadores (c) independientes a la vez y falla cerrada es Media como máximo, y no cuenta como ★ rota en el corte (§1.4). "Falla cerrada" quiere decir que la sala no abre o se responde un error o `BehindCompaction`, y que nada se pierde ni se inventa sin aviso. Si algo se pierde o se inventa sin aviso, conserva la severidad de la invariante. No cambia lo que arregla este plan: cambia cómo se clasifica lo que encuentre la revisión de cierre. | 0 | Adoptada |
| **D92** | **Los nombres de las entradas de directorio son confiables (3a, DEF-171).** El §2 de `INVARIANTES.md` suma, en "Se confía en", que la corrupción accidental afecta el contenido de los archivos, no los nombres de sus entradas de directorio: ext4 con `metadata_csum`, XFS v5, btrfs y ZFS verifican los bloques de directorio y devuelven EIO antes que un nombre alterado. Se ajustan los enunciados de INV-D9 e INV-W2 ("todo byte del contenido") y no se adopta INV-W3. La frase de `ARCHITECTURE.md` §5.2 *Atomic Server Metadata* ("never deletes a segment based on a value it did not verify") se precisa. Como barato, en el Lote 1: `parse_segment_filename` rechaza `start > end`. | 0, 1 | Adoptada |
| **D93** | **DEF-173 es un riesgo aceptado (4b).** Apagar el runtime de Tokio con un `open_room`, un `apply_snapshot` o una compactación en vuelo, y reabrir la misma sala en el mismo proceso antes de que el hilo bloqueante termine, puede perder lotes confirmados sin aviso. No pasa en wasm32 (no hay pool de hilos), ni al salir del proceso (es un crash, INV-S3), ni al soltar un future (D86, D89). Va a `INVARIANTES.md` §4, con una nota en S1, S3 y L2, y al contrato del storage (rustdoc de `StorageEngine` y `ARCHITECTURE.md` §8): quien maneja el runtime cierra las salas con `close_room`, que espera a la apertura, a la compactación y a `apply_snapshot`, antes de apagarlo, si va a reabrirlas en el mismo proceso. **Es un requisito de diseño del SDK de la Fase 4**, y se anota en el ROADMAP. INV-L7 entra con esta excepción. | 0 | Adoptada |
| **D94** | **Y9 se ajusta y DEF-179 no lleva código (5b).** Opción (A) de la auditoría: antes de un respawn, la ventana en memoria también puede reconocer claves de operaciones podadas o de segmentos ilegibles. Después del respawn, un reintento de esas operaciones se comporta como uno fuera de la ventana (riesgo aceptado DEF-85). Se corrigen las dos frases opuestas de `ARCHITECTURE.md` §5.1, punto 4, y la de la línea 73 ("se aplica de nuevo"). | 0 | Adoptada |
| **D95** | **La lista de archivos de INV-D8 es cerrada (6a).** El piso de secuencia considera solo los archivos del log. La fila D78 de §4 pasa a decir que el riesgo vale aunque el roster (`last_ack_seq`) o el relay (la secuencia del snapshot activo) tengan la secuencia que se reutiliza. Se suma la fila gemela del storage: un daño en el último frame de `.wal.compacting` se trata como escritura rota, y si el WAL activo ya se vació, ese lote se pierde. Se ajustan los enunciados de D2, D8 y D3 (storage). | 0 | Adoptada |
| **D96** | **Invariantes nuevas y ajustes de enunciado (7a, 9a).** Se agregan, ninguna núcleo: **INV-L7** (ningún efecto en disco sobrevive al lock que lo serializa, con la excepción de D93) e **INV-D10** (cada archivo del log es contiguo por construcción). No se adoptan: INV-W3 (por D92); INV-S7, que pasa a §4 como riesgo hasta la Fase 4 (si se pierde el `.snap` después de una compactación que vació el WAL, la sala abre vacía en la secuencia 0: el cliente vuelve a bajar todo del servidor, y cómo lo detecta lo decide el SDK); INV-R8, que queda como nota en "Sostenida por" de R4; INV-R9, que se decide cuando se construya el worker (hoy la rompería el escenario de DEF-181, que ocupa el tope de subidas de la sala). Se aplican en bloque los ajustes de enunciado de la auditoría (§5); los de D2, D3, D8, D9, W2 e Y9 siguen D92, D94 y D95. | 0 | Adoptada |
| **D97** | **Alcance de las Medias y Bajas (8a).** Entran DEF-174 (la única Media sin disparador raro), DEF-176 (regresión (a); el límite aceptado de R7 se apoya en R5) y DEF-180 (una línea). DEF-177, DEF-178 y DEF-181 van al backlog: R4, R7 y A3 quedan con rupturas Bajas abiertas, marcadas así en la columna Estado (§1.5). DEF-179 se resuelve con D94. | — | Adoptada |
| **D98** | **Sellar `active.wal` al abrir si quedó por debajo del piso (DEF-167).** Al abrir, si `active_end_seq < head_seq`, `active.wal` se sella enseguida con su rango real (o, si quedó vacío, el `active.wal` nuevo arranca en `head_seq + 1`), y `seal_active_segment` graba `sealed_through_seq = max(end, meta.sealed_through_seq)`. La secuencia perdida queda como un hueco (`BehindCompaction`), como prevé D8. Como defensa, `append` rechaza un registro que no continúa `active_end_seq` con un error que detiene la sala (falla cerrada); con el sellado al abrir, es inalcanzable. Al adoptarla se descartó la alternativa de sellar en el `append` y seguir: oculta un estado que no debería existir. | 1 | Adoptada |
| **D99** | **La apertura del log retiene solo la ventana de RAM (DEF-170).** `inspect_active_segment` valida todos los frames y junta los ids de mutación, pero mientras lee retiene solo una ventana deslizante acotada por `ram_max_ops` y `ram_max_bytes`, con el cobro de D73. `segment_mutations` (un segmento sin sidecar) junta solo los ids. La memoria de la apertura queda acotada por la ventana de RAM más un frame, además de los ids de la ventana de dedup. Al adoptarla se precisó: la ventana es un modo de `ReadBudget` que siempre conserva la última operación (dice dónde terminó la lectura), y vale también para la verificación del cold duplicado del Lote 1 (DEF-168). | 2 | Adoptada |
| **D100** | **`main` síncrono (DEF-172).** `main` deja de ser `#[tokio::main]`: toma `server.lock`, arma el runtime, hace `block_on` del servidor, suelta el runtime (que espera a los `spawn_blocking` en curso) y recién después suelta el lock. No se usa `shutdown_timeout`, que soltaría el lock con tareas todavía corriendo. Se descarta filtrar el handle: depende de que el sistema operativo lo suelte al salir y deja el orden implícito. | 3 | Adoptada |
| **D101** | **La purga del relay retiene el lock del slot (DEF-175).** La purga (`relay/lifecycle.rs`) toma el lock del slot de la sala como guard propio (*owned*) y lo mueve a la closure de `run_blocking`, que lo suelta después de su última operación de archivo (INV-L7). Una purga cortada termina antes de que la sala recreada pueda instalar un snapshot. Vale para `create_room` y `delete_room`, que comparten la purga, y para `drop_stale` (`relay/download.rs`), que borra el snapshot descartado con el mismo patrón y es la otra ruptura de INV-L7 en el relay. La alternativa, correr `create_room` en su propia tarea, cierra este caso pero no el patrón. Al implementarla se precisó: como la purga sacaba el slot del mapa, retener su guard no frenaba a nadie; ahora deja en el mapa un slot nuevo, ya tomado, y es ese guard el que mueve a la closure. | 3 | Adoptada |
| **D102** | **El tope total de una subida se cuenta por uploader (DEF-174).** Completa el fix de DEF-153: hoy el tope se hereda cuando el uploader reinicia con la sesión viva, pero no a través de un vencimiento. El slot recuerda el uploader y el tope de la última sesión vencida, y una sesión nueva del mismo uploader que arranca dentro de `UPLOAD_IDLE_TIMEOUT` desde ese vencimiento hereda el tope: si ya pasó, se rechaza como una subida vencida. Así una subida, su ancla de poda (D61) y la designación que congela (D76) quedan acotadas por `snapshot_upload_max_secs`, y entre dos ciclos hay al menos un plazo de inactividad en el que la designación se reemplaza. Cumple el ajuste de INV-B2 ("por uploader") y hace pasar los cuatro tests de `inv2/b` e `inv2/delta`. La alternativa, que `designate` deje de congelar después de `designation_timeout + snapshot_upload_max_secs`, cierra R4 pero no el tope de la subida ni el ancla. Al adoptarla se precisó: (a) el vencimiento de una sesión es el instante en que quedó inactiva, `min(deadline, last_chunk_at + UPLOAD_IDLE_TIMEOUT)`; `expire_locked` olvida el tope recordado pasado el vencimiento más `UPLOAD_IDLE_TIMEOUT`, y `cleanup_expired` no suelta un slot mientras lo recuerde; (b) vale también para el worker con sus propias subidas, pero el tope recordado de un cliente nunca lo afecta; (c) solo se recuerda la última sesión vencida, así que dos miembros que se alternan pueden seguir anclando la poda, pero ninguno retiene su designación; (d) el chunk rechazado recibe `SnapshotSuperseded`; (e) sumada por la revisión de delta: una sesión cuyo último chunk no termina instalado (el rango se movió o el archivo no verifica) se recuerda igual que una que quedó inactiva, porque si no, mandar el último chunk con basura renueva el tope en cada ciclo. | 4 | Adoptada |

---

## 3. Resumen

| Orden | Lote | Ítems | Prioridad | Progreso |
|---|---|---|---|---|
| 0 | Contrato y redes de prueba | D90–D97 | Inmediata (las redes viven en parches) | ✅ |
| 1 | Log del servidor: abrir sin perder ni borrar de más | DEF-167, 168, 169 | Alta | ✅ 3/3 |
| 2 | Apertura acotada | DEF-170 | Alta | ✅ 1/1 |
| 3 | Locks y ciclo de vida | DEF-172, 175 | Media | ✅ 2/2 |
| 4 | Subidas | DEF-174, 176, 180 | Media | ✅ 3/3 |
| 5 | Cierre | Documentación y revisión de cierre (D90) | Requisito de la Fase 4 | ⬜ |

**Avance total:** Lotes 0 a 4 cerrados; 9 de 9 ítems.

**Por qué este orden, en una línea por lote:**
0. Sin el contrato actualizado, cada lote se revisa contra enunciados viejos; sin las redes en el repo, se pierden.
1. Tres Altas de durabilidad en el mismo camino de apertura, una de ellas regresión de la ronda anterior; juntas son menos de 100 líneas.
2. La única ★ que se rompe en el camino normal (con un esquema ancho legal).
3. Las dos Medias que introdujo la ronda anterior, con la misma causa (INV-L7).
4. Defensas contra miembros hostiles en las subidas; ninguna pierde commits.
5. La revisión de cierre reemplaza la tercera auditoría (D90).

---

## Lote 0 — Contrato y redes de prueba

> Objetivo: que `INVARIANTES.md` refleje D90 a D97 y que las redes de prueba de la auditoría queden en el repo antes de tocar código.

#### Contrato · ✅ Hecho
**Solución.** En `INVARIANTES.md`:
- **§1.4:** D90 (la regla de cierre reemplaza "dos auditorías seguidas en verde") y D91 (severidad con doble disparador raro).
- **§1.5:** un estado nuevo, "❌ Bajo (backlog)", para una invariante cuyas rupturas abiertas son todas Bajas y no entran en ningún lote. No bloquea el corte.
- **§1.6:** las decisiones pasan a ser D1 a D102.
- **§2:** D92.
- **§3:** INV-L7 e INV-D10 (D96), los ajustes de enunciado de la auditoría (§5) y los de D92, D94 y D95.
- **§4:** D93 (DEF-173), D95 (la fila D78 ampliada y la gemela de `.wal.compacting`) y la propuesta S7 (D96).
- **Columna Estado del resumen:**

  | Estado | Invariantes |
  |---|---|
  | ❌ (Lote 1) | D1, D3, D7 |
  | ❌ (Lote 2) | B1 |
  | ❌ (Lote 3) | D6, L2 |
  | ❌ (Lote 4) | R4 (DEF-181 queda en backlog), R5, B2, A3 (DEF-181 queda en backlog) |
  | ❌ Bajo (backlog) | R7 (DEF-177, DEF-178) |
  | 🆕 (Lote 1) | D10 |
  | 🆕 (Lote 3) | L7 |
  | ✅ | D9 y W2 (D92), S1 y S3 (D93), Y9 (D94) |

  El encabezado del resumen pasa a decir que el estado es el de `b4cb390` según la 2ª auditoría, con los lotes de este plan.

En el mismo cambio:
- **`ARCHITECTURE.md`:** §5.1, punto 4, y la línea 73 (D94); la frase de §5.2 sobre valores verificados (D92); el requisito de D93 en §8.
- **Rustdoc de `StorageEngine`** (`crates/storage/src/engine.rs`): el requisito de D93 y que soltar `apply_snapshot`, `open_room` o `compact_room` no los cancela (backlog de la auditoría, **(barato)**).
- **ROADMAP:** el requisito de D93 en la Fase 4.

**Test exigido.** Ninguno (documentación). Revisión cruzada de cada enunciado nuevo contra el código actual: si una invariante nueva no se cumple todavía, se marca con el lote que la va a cumplir.

**Hecho.** `INVARIANTES.md`, versión 3:
- **§1.4:** D91 y D90. Además de la revisión de cierre y la revisión por delta, dice cuándo vuelve a hacer falta una auditoría amplia: solo si cambia el modelo de amenaza o entra código fuera del alcance actual (como el SDK de la Fase 4), y en ese caso, una vez y contra las invariantes nuevas de ese código.
- **§1.1, §1.2, §1.5, §1.6 y §2:** lo indeterminado y las propuestas pasan a la revisión de cierre o de delta; los estados "❌ Bajo (backlog)" y "✅ (Dnn)"; las decisiones hasta D102; D92.
- **§3:** INV-D10 e INV-L7, y los ajustes de enunciado. Cada invariante que todavía no se cumple dice *Pendiente (Lote N)* en "Sostenida por", y R4, R7 y A3 nombran sus rupturas Bajas abiertas.
- **§4:** la fila D78 ampliada y tres filas nuevas: `.wal.compacting` (D95), el runtime apagado (D93) y el `.snap` perdido (S7).
- **Columna Estado:** la de la tabla de arriba.

En el mismo cambio:
- **`ARCHITECTURE.md`:** §5.1 punto 4, sin la frase que decía que la ventana es la misma después de reabrir; §5.2 *Atomic Server Metadata*, que se confía en los nombres; §8 *Room Lock*, el riesgo de D93 y qué hacer.
- **`StorageEngine`:** el rustdoc del trait dice qué tareas propias tiene `DiskStorageEngine`, que soltar el future no las cancela y que hay que cerrar las salas antes de apagar el runtime.
- **ROADMAP:** el requisito para la Fase 4 apunta a este plan, y §4.4 lleva el requisito de diseño de D93.

**Revisión cruzada.** La hizo un agente aparte, de contexto fresco y solo lectura, contra el código. Los 25 ajustes de §5 están aplicados y la columna Estado coincide con la tabla de arriba. Encontró, y se corrigió en el mismo cambio:
- **Enunciados que el código o el plan desmienten:**
  - L2 decía que la creación de sala corre en su propia tarea, y D101 no hace eso;
  - la nota de R8 en R4 no mencionaba el gemelo de DEF-176, que descarta un snapshot usable;
  - L4 contradecía el Y9 nuevo;
  - B1 era más estricto que D99;
  - L6 describía mal una purga fallida;
  - el alcance de D93 no incluía `open_room`, que tiene el mismo patrón;
  - `ARCHITECTURE.md` exageraba que un reintento podado "se aplica de nuevo" (también puede recibir `BehindCompaction`).
- **Una ruptura literal de L7 que no estaba en ningún lote:** `drop_stale` borra en el future de la request. Entra en el Lote 3 con D101.
- **Menores:** el estado "✅ (Dnn)" en §1.5, "Lote 0" ambiguo entre fases, y las menciones a "la próxima ronda" en §1.1 y §1.2.

#### Redes de prueba · ✅ Hecho
**Solución.** D88, igual que en la Fase 3.9. Traer las redes que la auditoría recomienda (§8) y que pasan en `main`:

| Red | Parche | Invariantes |
|---|---|---|
| Simulación de fallas aleatorias y reaperturas del log (`audit_d1_d2_d8_random_failures_*`; la versión larga, `#[ignore]`) | `inv2/d` | D1, D2, D8 |
| Operaciones soltadas y carreras del storage, con pánicos dentro de las tareas (`audit_s6_*`, `audit_s1_*`, `audit_s3_*`; agrega cuatro puntos de pánico bajo `cfg(test)`) | `inv2/s` | S1, S3, S6 |
| Cada bit de los cuatro JSON con checksum, y los tests de `write_checked` y `verified_json` (los `audit_d9_*` que pasan) | `inv2/m` | D9, W2 |
| Bordes de `iter_from` y sync sin presupuesto (`audit_y2_*`, `audit_y6_*`) | `inv2/delta` | Y2, Y6 |

Además, los tres barridos de cancelación de storage (`apply_snapshot`, `open_room` y `compact_room`, en `storage/src/tests/disk.rs`) dejan de ser `#[ignore]`: tardan menos de 1 s y son la red de D86 y D89 (backlog de la auditoría).

No entran acá:
- los tests de ruptura, que entran con su fix;
- los de los riesgos aceptados (D92, D93, D94) y los de los Bajos que van al backlog;
- los tests de evidencia, que entran con el lote cuya lista de interacciones cubren (§0.1, paso 4).

**Test exigido.** `cargo test --workspace` en verde, sin sumar más de ~15 s. Las versiones largas, también en verde (`cargo test --workspace --release -- --ignored`). Para cada red, una mutación conocida que la haga fallar.

**Hecho.** Las redes entraron con nombres que describen la propiedad, sin dependencias nuevas ni `unsafe`. Lo hicieron dos agentes en paralelo, uno por crate.

**Storage:**
- `src/disk/tests/simulation.rs`: `dropped_and_racing_operations_keep_memory_equal_to_disk`, más su versión larga en `#[ignore]`. Es solo para Unix, como la simulación de crashes: copia archivos de una sala abierta.
- `src/tests/disk.rs`, seis tests de pánicos y cancelaciones dentro de las tareas:
  - `panic_in_the_snapshot_task_after_installing_marks_the_room_failed`;
  - `panic_in_the_open_task_releases_the_room_and_its_files`;
  - `panic_in_the_compaction_task_loses_no_row`;
  - `close_waits_for_the_task_of_a_dropped_snapshot`;
  - `engine_dropped_during_a_snapshot_task_keeps_the_room_until_it_completes`;
  - `concurrent_and_dropped_opens_of_a_room_open_it_once`.
- **Barridos:** los de `apply_snapshot`, `open_room` y `compact_room` dejaron de ser `#[ignore]`.
- **Producción:** cuatro puntos de pánico de una línea (`compaction.renamed`, `compaction.rotated`, `apply_snapshot.installed`, `recovery.replayed`), con el `fail_point::panic_point` que ya existía y que no hace nada fuera de los tests. Es el único cambio de producción del lote.
- **Helpers compartidos en `src/disk/tests/support.rs`:** `poll_then_drop`, que ahora es genérico, y `open_when_unlocked`.

**Servidor:**
- `log/tests/tiered_log.rs`:
  - `random_failures_and_reopens_keep_every_acknowledged_commit`, más su versión larga, de 100 semillas × 120 pasos;
  - `every_damage_to_log_meta_fails_the_open_without_deleting_anything`;
  - `reads_start_at_the_right_segment_at_every_edge`.
- Daño de cada uno de los otros tres JSON: `every_damage_to_the_roster_discards_it` (`actor/tests/lease.rs`), `every_damage_to_meta_room_fails_to_load` (`actor/tests/manager.rs`) y `every_damage_to_a_schema_file_fails_the_startup` (`tests/schema_registry.rs`).
- `tests/durable.rs`:
  - `checked_file_round_trips_any_json`;
  - `checked_file_rejects_every_variant_of_its_format`;
  - `every_bit_flip_and_truncation_of_a_checked_file_is_rejected`;
  - `verified_json_reads_hostile_files_as_unreadable`.
- `api/tests/data_plane.rs`: `sync_without_budget_does_not_move_the_cursor`.
- **Helper compartido:** `test_support::every_truncation_and_bit_flip` reemplaza el bucle de daño que el parche copiaba en cinco tests.

**Desvíos:**
- La simulación corta del servidor bajó de 3 semillas × 50 pasos a 2 × 25: con el tamaño del parche tardaba 10,6 s en debug.
- El test del archivo de esquema usa el fixture de dos tablas que ya existía. El del parche dañaba una sola tabla, aunque su documentación prometía dos.
- Los helpers de la simulación del storage que solo usa Unix quedaron bajo `#[cfg(unix)]`, para que clippy no falle en Windows por código muerto. No se pudo compilar para Windows desde macOS (`zstd-sys` necesita un toolchain de C). Se comprobó desactivando `cfg(unix)` en una copia: clippy, limpio.

**Números (macOS, sin otra carga):**
- **Tests:** 795 pasan y 38 ignorados; antes del lote eran 774 y 39.
- **Lo nuevo en la suite normal:** ~13 s de tests, ~9 s del servidor y ~4 s del storage. Los más caros: la simulación del log (2,3 s), el daño de `log_meta.json` (2,5 s) y el del esquema (2,2 s) en el servidor; la simulación (2,4 s) y los cuatro barridos (1,6 s entre todos) en el storage. El tiempo total de la suite varía mucho entre corridas (115 s y 189 s), casi todo en el binario de la lib del servidor, por sus tests en tiempo real.
- **Versiones largas:** en verde. Storage, 12 tests; la simulación nueva tarda ~132 s en release. Servidor, 13 tests en ~19 min, medidos con la máquina cargada; casi todo es la simulación nueva. El workflow semanal corre en Linux, con un tope de 120 min.

**Mutaciones** (cada una restaurada después):

| Red | Mutación | La detecta |
|---|---|---|
| Simulación del log | Una poda fallida vuelve sin recalcular la cola | Versión corta |
| Simulación del log | Un error de limpieza de la compresión, tratado como fallo | Versión corta |
| Simulación del log | El head se calcula sin el piso | Solo la versión larga |
| JSON con checksum | `verified_json` acepta cualquier línea de checksum | 7 de los 8 tests |
| JSON con checksum | `split_once` en lugar de `rsplit_once` | `verified_json_reads_hostile_files_as_unreadable` |
| Bordes de lectura | `iter_from` empieza un segmento más tarde | El test de bordes |
| Sync sin presupuesto | El sync mueve el cursor antes de tomar el presupuesto | El test de sync |
| Storage, tareas | `close_room` deja de esperar el lock de compactación | `close_waits_for_the_task_of_a_dropped_snapshot` |
| Storage, tareas | `apply_snapshot` u `open_room` corren en línea | Su barrido y los tests de pánico y cancelación |
| Storage, tareas | `compact_room` corre en línea | La simulación (dos semillas) y el test de pánico |
| Storage, tareas | Un `apply_batch` soltado no marca la sala fallida | La simulación (3 de 4 semillas) |

Mover el borrado de `.wal.compacting` antes del rename no lo detecta la simulación de operaciones soltadas por sí sola: sin un crash, la compactación siguiente reescribe el snapshot. Esa mutación la cubre la simulación de crashes de la Fase 3.9.

**Chequeos del lote:** fmt, clippy del workspace, `cargo test --workspace`, `cargo doc` con `-D warnings`, y el build y clippy hacia wasm32: todo en verde.

**A vigilar en la CI:** los barridos y los seis tests nuevos del storage nunca corrieron en Windows ni en la CI de macOS (el job semanal es solo Linux). El primer push es su primera prueba en esas plataformas.

---

## Lote 1 — Log del servidor: abrir sin perder ni borrar de más

> Objetivo: que la apertura del log nunca deje un commit detrás de un hueco, nunca borre la copia buena de un segmento y nunca sirva un archivo que ningún `fsync` de este proceso cubrió.

#### DEF-167 · Alto · ✅ Hecho
**Problema.** Con `sealed_through_seq` por encima del fin real de `active.wal`, la apertura trunca el último frame dañado y el commit siguiente se escribe detrás de un hueco: la sala no vuelve a abrir, o las lecturas responden `BehindCompaction`. Hacen falta un crash entre `record_meta` y el rename de la rotación y, además, un daño en ese frame. Es una regresión de D81.
**Solución.** D98.
**Test exigido.**
- Los tres `audit_d1_commit_*` de `inv2/d`, incluido el de punta a punta con `CommitAck`.
- Que `append` rechace un registro que no continúa `active_end_seq` (mutación: sacar la guarda).

**Interacciones a revisar:**
- el truncado de una escritura rota (D78);
- la rotación y `record_meta` (D81);
- la compresión, que contrasta el nombre del segmento con su contenido;
- la cuota y la poda, que cuentan a partir de los nombres;
- la hidratación de la ventana de dedup;
- la simulación de fallas y reaperturas del Lote 0.

**Hecho.** Como dice D98: la apertura sella `active.wal` si `active_end_seq < head_seq`, `seal_active_segment` graba el máximo con el `sealed_through_seq` anterior, y `append_record` rechaza con `WalCorruption`, antes de escribir, un registro que no continúa `active_end_seq` (el actor detiene la sala ante cualquier error de `append`). Si `active.wal` quedó vacío no hace falta nada: el próximo registro ya arranca en `head_seq + 1`.

**Tests:**
- `commit_after_a_damaged_last_frame_below_the_sealed_end_survives_a_reopen` (`log/tests/tiered_log.rs`): junta los dos `audit_d1_commit_*` del log en un bucle con `ram_max_ops` 6 (el commit siguiente sella por cantidad) y 7 (no sella). Suma una apertura cuyo sellado falla (en `log_meta.json`) y otra después, que muestra que el sellado no baja el piso. También comprueba que la operación perdida queda como un hueco (`BehindCompaction` desde 3).
- `commit_acknowledged_after_a_truncated_frame_below_the_sealed_end_is_served` (`actor/tests/room.rs`): el de punta a punta con `CommitAck`. Daña el último frame invirtiendo el último byte del archivo.
- La guarda de `append`: se extendió `append_into_empty_existing_active_wal_tracks_its_range_and_continues_it` (`log/tests/warm_disk.rs`, antes `..._tracks_start_seq`).

**Mutaciones** (cada una restaurada después):
- sacar el sellado al abrir: fallan los dos primeros tests;
- grabar `end` sin el máximo: falla el primero;
- sacar la guarda: falla el de `warm_disk`.

**Interacciones:**
- **Truncado de una escritura rota (D78):** sin un piso por encima, una cola rota deja `active_end_seq == head_seq` y no se sella nada. Los tests de escrituras rotas siguen pasando.
- **Rotación y `record_meta` (D81):** el sellado al abrir es la misma `seal_active_segment`. Si falla, la apertura falla sin sellar y la siguiente lo reintenta (está en el test). Los pasos posteriores al rename son los de una rotación común, y ya los cubren la simulación y `rotation_that_fails_to_record_its_end_seals_nothing`.
- **Compresión, cuota y poda:** el segmento sellado lleva su rango real, así que su nombre coincide con su contenido y el sidecar se dimensiona bien.
- **Ventana de dedup:** los ids de `active.wal` se leen antes de sellar, y el sidecar del segmento nuevo se escribe con ellos. El test comprueba el id del commit 6 después de reabrir.
- **HotBuffer y `tail_seq`:** `contiguous_suffix` deja la RAM vacía, porque el último de `active.wal` no es `head_seq`. `tail_seq` se calcula después de sellar.
- **Simulación del Lote 0:** no daña frames, así que nunca llega al sellado. Sigue pasando, también en su versión larga.
- **Tests de evidencia de `inv2/d` y `inv2/c-log`** (`audit_d8_*`, `audit2_d8_*`): no se trajeron. Ejercitan el piso después de rotaciones que sí renombraron, o con fallas en cada paso de una rotación; ahí `active_end_seq` nunca queda por debajo de `head_seq`, así que el sellado nuevo no corre, y la simulación ya cubre esos pasos.

#### DEF-168 · Alto · ✅ Hecho
**Problema.** Ante un cold y un warm del mismo rango, la apertura borra el cold sin leer ninguna de las dos copias, aunque el warm esté dañado y el cold verificado.
**Solución.** En `SegmentIndex::scan`, verificar el cold con `read_cold_file`, exigiendo exactamente `start..=end`. Si verifica, se borra el warm; si no, el cold, como hoy. Ajustar `open_keeps_the_warm_original_of_an_interrupted_compression`, que fija la regla vieja, la frase de `ARCHITECTURE.md` §5.2 y el doc de `CompressedSegment::cleanup_error`.
**Test exigido.** `audit2_d3_open_never_drops_a_verified_cold_copy_for_a_damaged_warm_one` (`inv2/c-log`), con daño en el frame 1 y en el 4.

**Interacciones a revisar:**
- `undeleted_warm` y `record_compression`;
- la cuota: el warm borrado deja de contar;
- el sidecar `.ids` del rango;
- la compresión siguiente, que hoy vuelve a comprimir el warm conservado;
- el costo de la apertura: el cold solo se lee cuando hay un duplicado.

**Hecho.** `scan` lee el cold duplicado con `read_cold_file` (`holds_its_range`). Si su última operación es `end`, se queda: `SegmentIndex::insert` ahora devuelve la entrada reemplazada, y el warm pasa a los restos que se borran. Si es corrupto o le faltan operaciones, se borra el cold, como antes. **Desvío, confirmado con el dueño:** un error de E/S al leer el cold hace fallar la apertura en lugar de borrarlo, porque no prueba que esté dañado. Se ajustaron el doc de `scan`, el de `CompressedSegment::cleanup_error` y `ARCHITECTURE.md` §5.2.

**Tests:**
- `open_never_drops_a_verified_cold_copy_for_a_damaged_warm_one` (`inv2/c-log`), con daño en los frames 1 y 4.
- `open_keeps_the_warm_original_of_an_interrupted_compression` pasó a `open_keeps_the_warm_original_when_its_cold_copy_does_not_verify`: el cold al que le falta la última operación se borra y el warm se vuelve a comprimir.

- La rama de E/S, en el primero: con un fail point nuevo de una línea en `open_segment` (bajo `cfg(test)`, como los demás), la apertura falla y no borra ninguna de las dos copias.

**Mutaciones:**
- sin la verificación (la regla vieja), falla el primero;
- con un error de E/S tratado como cold dañado, también falla el primero.

**Interacciones:**
- **`undeleted_warm` y `record_compression`:** al abrir, la lista está vacía. El warm se borra por los restos de `scan`, con el sync del directorio.
- **Cuota:** `insert` descuenta el warm reemplazado de `total_bytes`.
- **Sidecar `.ids`:** lo comparten las dos copias por el rango, y se conserva.
- **Compresión siguiente:** si queda el cold, no hay nada que recomprimir. Si queda el warm, se recomprime (está en el test ajustado).
- **Costo:** el cold se decodifica entero solo cuando hay un duplicado. Retiene todas sus operaciones, que es el patrón de DEF-170 (con un esquema ancho, un segmento no es una cota). Queda como interacción del Lote 2 (D99), que es donde se resuelve ese patrón.
- **Poda:** los segmentos que terminan en `pruned_through_seq` o por debajo se descartan antes de llegar a la comparación, así que no se leen.

#### DEF-169 · Alto · ✅ Hecho
**Problema.** `SchemaRegistry::new` sirve `schemas/*.json` sin sincronizar `schemas/`. Una entrada que dejó un sync fallido se sirve después de reiniciar sin que ningún `fsync` de este proceso la haya cubierto.
**Solución.**
- `durable::sync_dir(schemas/)` en `SchemaRegistry::new`, antes de leer.
- Lo mismo para `snapshots/` en `recover_snapshots`, que tiene el mismo patrón con un disparador más raro (backlog de la auditoría).
- Sumar `schemas/` y `snapshots/` al "Sostenida por" de D7 y a `ARCHITECTURE.md` §5.2.

**Test exigido.**
- `audit_d7_schema_left_unsynced_by_a_failed_write_is_not_served_unsynced_after_a_restart` (`inv2/d`), para la registración y para `add_column`.
- Para `snapshots/`, un test con un fail point que cuente los syncs, como los de D77.

**Interacciones a revisar:**
- `lock_data_dir`, que ya sincroniza la entrada `schemas/` dentro de `data_dir`;
- la recarga del registry;
- el arranque con un symlink roto en `schemas/` (D67).

**Hecho.** `durable::sync_dir` de `schemas/` en `SchemaRegistry::new` y de `snapshots/` al principio de `recover_snapshots`, antes de leer. **Desvío, confirmado con el dueño:** si falla el sync de `snapshots/`, falla el arranque, como con `schemas/` y con la apertura del log. El sync posterior al borrado sigue con un warning. D7 y `ARCHITECTURE.md` §5.2 (*Data Directory Lock*) suman los dos directorios.

**Tests:**
- `startup_syncs_schemas_left_unsynced_by_a_failed_write_before_serving_them` (`tests/schema_registry.rs`), para la registración y para `add_column`. Comprueba que el arranque falla si falla el sync, y que el siguiente sirve el esquema.
- Para `snapshots/` se extendió `crash_between_promotion_and_install_recovers_the_newest_snapshot` (`relay/tests/recovery.rs`): con el sync armado, el arranque falla. Sin el fix, el sync posterior al borrado consumía el fail point con solo un warning.

**Mutaciones:** sacar cada sync hace fallar su test.

**Interacciones:**
- **`lock_data_dir`:** sincroniza la entrada `schemas/` dentro de `data_dir`, y esto, lo que hay adentro. Los dos corren después de tomar `server.lock` (`main.rs`).
- **Recarga del registry:** no hay recarga desde disco. `reload_schema_for_rooms` recarga las salas desde el registry en memoria.
- **Symlink roto (D67):** el sync abre solo el directorio, y la lectura de cada entrada no cambia.
- **Windows:** `sync_dir` no hace nada ahí, pero el fail point se mira antes, así que los tests corren igual.

**Barato del mismo archivo (D92).** `parse_segment_filename` rechaza `start > end` (`segment_index.rs`, que este lote ya toca por DEF-168).

**Hecho (barato).** Un nombre con `start > end` se ignora como cualquier nombre que no se puede interpretar. Se extendió `scan_indexes_both_tiers_in_sequence_order_and_ignores_other_files`. Mutación: sin la comparación, falla.

**Cierre del lote:**
- **Chequeos:** fmt, clippy del workspace, `cargo test --workspace` y `cargo doc` con `-D warnings`, en verde (Linux). No toca `core` ni `storage`, así que no hace falta el build de wasm32.
- **Versiones largas del servidor** (`--release -- --ignored`): 13 tests en verde, en 374 s (Linux).
- **Líneas de producción:** +67 −19.

**Revisión de delta.** La hizo un agente aparte, de contexto fresco y solo lectura, que repitió cada mutación con la suite de la lib completa. No encontró hallazgos Altos ni Medios, ni otra invariante rota; tampoco violaciones de `GEMINI.md`. Probó además el caso de D98 en el que `active.wal` queda vacío, y pasa. Encontró dos Bajos:
- ningún test cubría la rama de E/S de DEF-168: se agregó el fail point y el caso en el test;
- la interacción con la cota de memoria de la apertura (INV-B1) faltaba en la lista: se sumó arriba y al Lote 2.

---

## Lote 2 — Apertura acotada

> Objetivo: que abrir una sala retenga como mucho la ventana de RAM, sea cual sea el ancho del esquema.

#### DEF-170 · Alto · ✅ Hecho
**Problema.** Abrir una sala decodifica entero `active.wal` antes de recortarlo a la ventana de RAM. Con un esquema ancho legal y filas de `Null` son ~30 MB medidos con 4.096 columnas y ~800 MB estimados con 65.535, por apertura, y varias aperturas concurrentes se suman. Lo mismo pasa con un segmento sin sidecar.
**Solución.** D99.
**Test exigido.**
- `audit_b1_opening_a_room_decodes_all_of_active_wal` (`inv2/b`), adaptado: lo que retiene la apertura cabe en `ram_max_ops` y `ram_max_bytes`.
- Uno igual para un segmento sin sidecar.
- Ajustar `reopening_loads_into_ram_only_what_fits`, que mira la HotBuffer después del recorte y no el pico de la apertura (backlog de la auditoría).

**Interacciones a revisar:**
- la ventana de dedup, que se hidrata en la apertura con los ids de todo `active.wal` y de los segmentos (Y4, Y9);
- la validación de frames y el truncado de una escritura rota (D78): la ventana deslizante no puede saltear la validación de ningún frame;
- el sellado al abrir del Lote 1 (D98), que usa `active_end_seq`;
- el cobro de la HotBuffer (D73);
- la lectura desde disco de lo que quedó fuera de la ventana;
- la verificación del cold duplicado del Lote 1 (DEF-168), que decodifica un segmento entero al abrir.

**Hecho.** Como dice D99. `ReadBudget::window(max_ops, max_bytes)` admite todas las operaciones, y `read_frames`, al terminar cada frame, llama a `slide`: suelta las más viejas que no entran, cobradas con `op_bytes` (D73), y conserva siempre la última. `FrameRead.first_seq` da el inicio de `active.wal`, que la ventana puede ya no tener. Se usa en tres lugares:
- `inspect_active_segment`, con la ventana de RAM de la política (`open_recovering` ahora la recibe);
- `segment_mutations`, con `window(0, 0)`: de un segmento sin sidecar quedan los ids;
- `holds_its_range` (DEF-168), con `window(0, 0)`: queda la última operación para compararla con `end`. **Desvío, confirmado con el dueño:** D99 no lo nombraba, pero el Lote 1 lo dejó como interacción de este lote.

Producción: +45 −12 líneas.

**Tests.** Un pico bajo `cfg(test)` (`budget::PEAK`, thread-local) anota lo que retiene cada lectura después de cada frame, medido en bytes como los cobra la RAM. Se extendieron dos tests, en lugar de traer `audit_b1_*` como test aparte:
- `reopening_loads_into_ram_only_what_fits` es el test de `inv2/b` adaptado. Usa filas de `Null` (un segmento `1..=30` y `31..=50` en `active.wal`, cada uno más grande que 1 MiB de RAM) y corre con y sin el sidecar del segmento. Comprueba que el pico de la reapertura no pasa de `ram_max_bytes`, que `active.wal` arranca en 31, y la HotBuffer y los ids como antes. Sin el fix, el pico es de 1,97 MB.
- `open_never_drops_a_verified_cold_copy_for_a_damaged_warm_one`: el pico de la apertura que verifica el cold no pasa de lo que queda en la HotBuffer.

**Mutaciones** (cada una restaurada después):
- sin ventana en `inspect_active_segment`: falla el primero;
- el inicio tomado de `ops.first()`: falla el primero (por la línea de `active_start_seq`, que se sumó para esto);
- sin ventana en `segment_mutations`: falla el primero, sin sidecar;
- sin ventana en `holds_its_range`: falla el segundo;
- la ventana sin conservar la última operación: falla el segundo.

**Interacciones:**
- **Ventana de dedup (Y4, Y9):** `read.mutations` nunca se recorta. La apertura sigue juntando todos los ids de `active.wal` y de los segmentos, y `active_mutations` (el sidecar de la rotación) los recibe todos. Lo comprueba el primer test (50 ids).
- **Validación de frames y truncado (D78):** la ventana nunca agota el presupuesto (`max_ops` es `usize::MAX`), y con `from_seq` 0 no se saltea ningún frame por su header, así que todos se leen y se verifican. Los tests de escrituras rotas y de corrupción de `warm_disk` siguen pasando.
- **Sellado al abrir (D98):** usa `active_end_seq`, la última operación, que la ventana siempre conserva.
- **Cobro de la HotBuffer (D73):** la ventana cobra con el mismo `op_bytes`; `contiguous_suffix` y `apply_sliding_window` no cambian.
- **Lectura desde disco de lo que quedó afuera:** `read_active_range` lee el archivo, igual que antes.
- **Cold duplicado (DEF-168):** solo necesita la última operación, y la verificación de cada frame no cambia.
- **Las demás lecturas:** con un presupuesto común, `slide` solo anota el pico en los tests.
- **Quienes leen `active_start_seq`**, que ahora sale de `first_seq`: `active_ops_count` (cuándo sella `append`), `compute_tail_seq` (la cola cuando no hay segmentos), y `seal_active_segment` y `rotate_active_segment` (el rango del `SegmentEntry` y los nombres del segmento y su sidecar). Los cubre la línea de `active_start_seq` del primer test.

**Revisión de delta.** La hizo un agente aparte, de contexto fresco y solo lectura, que corrió la suite del log. No encontró hallazgos Altos ni Medios, ni otra invariante rota, ni violaciones de `GEMINI.md`. Confirmó que cada mutación hace fallar un test. Señaló:
- la interacción con quienes leen `active_start_seq`, que faltaba en la lista: se sumó arriba;
- el doc de `read_newest`, que decía que RAM se llena con todo `active.wal`: se corrigió;
- que el pico se mide después de `slide`, así que prueba la ventana pero no el frame de más que admite D99;
- un Bajo previo, fuera del lote: va al backlog.

**Cierre del lote:**
- **Chequeos:** fmt, clippy del workspace, `cargo test --workspace` y `cargo doc` con `-D warnings`, en verde (Linux). No toca `core` ni `storage`, así que no hace falta el build de wasm32.
- **Versiones largas del servidor** (`--release -- --ignored`): 13 tests en verde, en 337 s (Linux).

---

## Lote 3 — Locks y ciclo de vida

> Objetivo: que ningún lock se suelte antes de la última operación de archivo que serializa (INV-L7).

#### DEF-172 · Medio · ✅ Hecho
**Problema.** `server.lock` es una variable local del cuerpo de `#[tokio::main]` y se suelta antes de que `Runtime::drop` espere a los `spawn_blocking` en curso. Un segundo servidor puede arrancar mientras el primero todavía promueve una subida o purga el relay. Es una regresión de D83.
**Solución.** D100.
**Test exigido.** `audit_d6_server_binary_releases_the_data_dir_lock_before_it_stops_writing` (`inv2/l`). Corre el binario real y es solo para Unix; va a `crates/server/tests/` con un nombre que siga `GEMINI.md`.

**Interacciones a revisar:**
- la gracia de 10 s de `serve_until_shutdown`, y el ítem de backlog "soltar el runtime no está acotado a 10 s";
- el arranque (`clear_uploads`, `recover_snapshots`, el borrado de lápidas), que tiene que seguir corriendo con el lock tomado;
- `lock_data_dir` y los tests que arman los componentes sin tomar el lock;
- Windows y el lock de `fs2`.

**Hecho.** Como dice D100. `main` toma el lock, arma el `Runtime`, hace `block_on(serve(config))` y suelta el runtime con un `drop` explícito antes de devolver, así que el lock se suelta después. El resto del cuerpo de `main` pasó tal cual a `serve`.

**Test.** `server_binary_holds_the_data_dir_lock_until_it_stops_writing`, en `crates/server/tests/server_binary_tests.rs` (solo Unix), es el test de `inv2/l` adaptado. Una subida de 256 MiB cuyo cliente corta después del último chunk sigue promoviéndose en el pool bloqueante cuando llega el SIGTERM. El test sondea `server.lock` y comprueba que, si el snapshot terminó en `snapshots/`, ya estaba ahí cuando el lock quedó libre. El otro test del parche (un segundo binario se niega a arrancar) no se trajo: pasa sin el fix y no cubre ninguna interacción de la lista. Que un segundo servidor no pueda tomar el lock ya lo prueba `a_second_server_cannot_lock_the_same_data_dir`, y que `main` lo tome antes de tocar cualquier archivo no cambió.

**Mutación:** con el `#[tokio::main]` de antes, el test falla.

**Interacciones:**
- **La gracia de 10 s:** no cambia. Si se agota, `serve` devuelve el error y `process::exit(1)` sale sin soltar el runtime: es un crash (INV-S3), y el sistema operativo suelta el lock al terminar el proceso. El ítem de backlog "soltar el runtime no está acotado a 10 s" sigue abierto: ahora el lock se retiene durante esa espera, que es lo que pide INV-D6. Un segundo servidor se niega a arrancar en lugar de pisarse con el primero.
- **El arranque** (`clear_uploads`, `recover_snapshots`, el borrado de lápidas): corre dentro de `serve`, con el lock ya tomado.
- **`lock_data_dir` y los tests que arman los componentes sin el lock:** no cambian.
- **Windows y `fs2`:** el handle vive en el hilo de `main` y se suelta después del runtime, igual en las tres plataformas. El test es solo para Unix porque manda SIGTERM.

#### DEF-175 · Medio · ✅ Hecho
**Problema.** Un `POST /admin/rooms` cortado deja la purga del relay corriendo sin el spawn lock ni el lock del slot. Si el admin reintenta y la sala recibe un snapshot, la purga vieja borra su archivo, y el relay lo sigue dando por activo hasta su TTL. Es una regresión de D84.
**Solución.** D101.
**Test exigido.**
- `audit_l2_dropped_room_creation_purges_the_snapshot_of_the_room_created_after_it` (`inv2/l`).
- Para `drop_stale`: con el future cortado durante el borrado, el lock del slot no se libera antes de que termine (un fail point que pause el borrado, como los de `inv2/l`).

**Interacciones a revisar:**
- la purga de `delete_room`, que es la misma función y ya corre en su propia tarea;
- una subida que tiene el lock del slot cuando arranca la purga (backlog: `ARCHITECTURE.md`, línea 245);
- `drop_stale`, que entra con el mismo fix;
- el lock por sala de `RoomManager` y el orden de D84 (la lápida antes de la purga).

**Hecho.** Como dice D101, con la precisión anotada al adoptarla. `purge_room` marca el slot viejo como purgado, pone en el mapa uno nuevo tomado con `try_lock_owned` y mueve ese guard a la closure de `run_blocking`, que lo suelta después de borrar los archivos. Las requests que ya tenían el slot viejo responden `RoomNotFound` como antes, y las que llegan después esperan a que termine el borrado. `remove_files` recibe un `held` que suelta al final de su closure: `drop_stale` le pasa su guard (`lock_owned`), y los demás llamadores, `()`. Producción de los dos ítems: +32 −13 líneas.

**Tests.** Se extendieron dos tests que ya tenían el escenario, en lugar de traer el de `inv2/l` como test aparte:
- `room_deletion_and_creation_finish_when_the_request_is_dropped` (antes `room_deletion_finishes_when_the_request_is_dropped`) suma el test de `inv2/l` adaptado. Una creación se corta con su purga detenida (espera hasta 2 s), el reintento crea la sala, el relay confirma un snapshot, la purga sigue, y el archivo tiene que quedar. Comparte con `disconnected_deletion_over_tcp` el helper nuevo `stage_doomed_snapshot`.
- `first_chunk_drops_a_snapshot_below_the_retained_range` corta el chunk 0 mientras `drop_stale` borra el snapshot (hook nuevo `relay_remove_file`). Comprueba que el lock del slot sigue tomado hasta que el borrado termina.

**Mutaciones** (cada una restaurada después):
- la purga suelta el guard del slot nuevo antes de borrar: falla el primero;
- `drop_stale` pasa `()` en lugar de su guard: falla el segundo.

**Interacciones:**
- **La purga de `delete_room`:** es la misma función. Los dos tests de borrado cortado siguen pasando; en el que apaga el servidor, el `drop` del runtime espera a la closure que retiene el slot.
- **Una subida con el lock del slot cuando arranca la purga:** la purga espera el slot viejo como antes. Después, la subida ve `purged` en sus pasos siguientes.
- **`drop_stale`:** entra con el fix.
- **El lock por sala de `RoomManager` y el orden de D84:** no cambian. Los dos llamadores de `purge_room` lo hacen bajo `lock_room`, así que el slot del mapa es el que tomó la purga, y el `remove_if` condicional pasó a ser un `insert`. Una purga cortada ya instaló su slot nuevo; la siguiente toma ese.
- **`cleanup_expired`:** el slot nuevo se conserva mientras la closure lo retiene (`strong_count > 1`), y el barrido siguiente lo descarta una vez libre. Un barrido que tomó el slot viejo lo encuentra purgado y no hace nada. Antes, la purga sacaba el slot del mapa; ahora queda uno vacío hasta ese barrido.
- **Los demás llamadores de `remove_files`** (`install`, `expire_locked`, las de `upload.rs`): no cambian.
- **`upload_in_progress` y `upload_in_progress_by`** (sumada en la revisión): mientras dura el borrado, ven tomado el slot nuevo, y lo cuentan como una subida en curso. No tiene efecto: durante la purga la sala no tiene actor. `delete_room` lo apaga antes, `create_room` lo crea después, y el reintento de una creación cortada espera en el slot antes de crearlo.
- **Dos purgas de la misma sala** (sumada en la revisión): el `insert` no vuelve a mirar `purged`. Para que dos purgas tomen el mismo slot, las dos tienen que tener `lock_room` a la vez. De una purga cortada sigue viva solo la closure, que no inserta nada.

**Revisión de delta.** La hizo un agente aparte, de contexto fresco y solo lectura. Corrió las mutaciones en un worktree propio y confirmó que cada una hace fallar su test. No encontró hallazgos Altos ni Medios, ni otra invariante rota, ni violaciones de `GEMINI.md`. Verificó que los llamadores que pasan `()` a `remove_files` corren en su propia tarea (`run_to_completion` o el sweeper). Señaló:
- las dos interacciones que faltaban, sumadas arriba;
- en el test del binario, si el último chunk no se procesa entero antes del corte, la subida no se completa y el test pasa sin probar nada (nunca da un falso fallo);
- en el mismo test, si falla un assert antes de `server.wait()`, el proceso hijo queda corriendo.

Los dos últimos son Bajos y van al backlog.

**Cierre del lote:**
- **Chequeos:** fmt, clippy del workspace, `cargo test --workspace` y `cargo doc` con `-D warnings`, en verde (Linux). No toca `core` ni `storage`, así que no hace falta el build de wasm32.
- **Versiones largas del servidor** (`--release -- --ignored`): 13 tests en verde, en 358 s (Linux).

---

## Lote 4 — Subidas

> Objetivo: que un miembro hostil no retenga una subida ni la designación sin plazo, ni rompa la subida de otro, y que el chunk 0 compare con el rango del snapshot que sirve.

#### DEF-174 · Medio · ✅ Hecho
**Problema.** Un uploader que reinicia su subida justo al vencer el tope arranca una sesión con un tope nuevo. Si es el designado, conserva la designación (D76) sin plazo, sin completar nada, y los clientes `Bootstrapping` nunca ven un snapshot usable. Es la variante que dejó abierta el fix de DEF-153.
**Solución.** D102.
**Test exigido.**
- Los dos de `inv2/delta`, con tiempo pausado: `audit_b2_designee_keeps_the_role_by_restarting_its_upload_at_each_deadline` y `audit_b2_an_upload_restarted_at_each_deadline_is_never_bounded`.
- `audit_b2_upload_restarted_at_its_deadline_outlives_the_cap` (`inv2/b`).
- Los controles, en los que el designado no reinicia y pierde el rol, siguen pasando.

**Interacciones a revisar:**
- la herencia del tope al reemplazar una subida por una de secuencia mayor (fix de DEF-153);
- el orden de `expire_locked` frente a la subida en curso;
- `cleanup_expired`, que tiene que olvidar el tope recordado junto con el slot;
- la designación (D76) y el ancla de poda (D61);
- el worker (admin secret), que no puede quedar sin poder subir.

**Hecho.** Como dice D102, con las precisiones anotadas al adoptarla. `UploadSession::idle_at` da el instante en que la sesión queda inactiva, y `is_idle` pasó a compararse con él. Al descartar una sesión inactiva, `expire_locked` guarda en el slot un `ExpiredUpload` (uploader, tope y cuándo olvidarlo); el último chunk también lo guarda, y lo borra solo si la subida se instaló (precisión e). Al empezar una sesión nueva, `stage_chunk` busca el tope del mismo uploader en la sesión viva y en la vencida. Si ya pasó, rechaza el chunk con `SnapshotSuperseded` antes de tocar nada; si no, lo hereda. Esa búsqueda reemplaza la herencia del fix de DEF-153.

**Tests.** Se extendieron los dos tests de `relay/tests/lifecycle.rs` que ya tenían el escenario, en lugar de traer los tres de `inv2/b` e `inv2/delta`:
- `upload_that_keeps_progressing_or_restarts_expires_at_its_deadline` suma el escenario de los tres. Antes, el uploader completa su subida con un último chunk que no verifica y vuelve a empezar: el tope se mantiene. El uploader reinicia justo en el tope, después de un barrido de `cleanup_expired`, y un segundo antes de que pase el plazo de inactividad: los dos se rechazan y no queda subida en curso. Un segundo después puede empezar otra.
- `idle_upload_expires_and_its_file_is_deleted`: el slot se olvida un plazo de inactividad después de que la subida venció, no enseguida.

El test de `inv2/delta` sobre el actor (`designee_keeps_the_role…`) no se trajo. La designación solo mira `upload_in_progress_by`, que filtra con el mismo `is_idle` que `upload_in_progress`, y el test extendido comprueba que este da `None` después del reinicio rechazado. El control del parche, en el que el designado deja de subir y pierde el rol, ya lo cubre `designee_that_does_not_upload_in_time_is_replaced_but_never_during_its_upload`.

**Mutaciones** (cada una restaurada después):
- sin buscar el tope en la sesión vencida: falla el primero;
- `cleanup_expired` suelta el slot aunque recuerde un tope: falla el primero;
- `expire_locked` nunca olvida el tope: fallan los dos;
- el último chunk que no instala no guarda el tope: falla el primero.

**Interacciones:**
- **La herencia al reemplazar por una secuencia mayor (DEF-153):** la misma búsqueda cubre la sesión viva y la vencida. El tramo del test que reinicia con la sesión viva sigue pasando.
- **El orden de `expire_locked` frente a la subida en curso:** corre antes de la búsqueda, bajo el lock del slot. Una sesión viva que llegó a su tope ya pasó a la vencida cuando se busca.
- **`cleanup_expired`:** retiene el slot mientras recuerde un tope, y `expire_locked`, que el barrido llama para cada slot, lo olvida a tiempo (mutaciones 2 y 3).
- **La designación (D76) y el ancla de poda (D61):** las dos leen `upload_in_progress(_by)`. Después del rechazo no hay sesión, así que la designación se reemplaza y el ancla se suelta.
- **El worker:** la búsqueda compara el uploader, así que el tope recordado de un cliente no lo afecta. Su propia subida vencida lo frena un plazo de inactividad, como a cualquiera.
- **`purge_room`:** instala un slot nuevo, que no recuerda nada. Una sala recreada empieza sin topes.
- **La subida en un solo request:** no cambia. No tiene tope total; la acotan los plazos del body.
- **Un `.await` nuevo:** no hay. El rechazo vuelve antes de cualquier `.await` y de cualquier cambio.
- **La completación fallida** (sumada en la revisión): precisión (e). Si el future se suelta durante la promoción, el tope queda recordado, que es lo conservador.
- **El reemplazo por otro uploader con una secuencia mayor, y el del worker** (sumadas en la revisión): la sesión reemplazada no se recuerda. Solo sirve con dos miembros que se alternan o depende del worker, así que entra en lo aceptado por la precisión (c).

#### DEF-176 · Bajo · ✅ Hecho
**Problema.** El chunk 0 sin ancla compara el snapshot que elige con un rango del log leído antes de elegirlo. Puede servir uno por debajo de la cola, si en el medio se instaló una subida, o descartar uno más nuevo y usable. Es una regresión del orden que introdujo el fix de DEF-152.
**Solución.** Partir `get_chunk` en "elegir y comprobar", que espera el rango después de elegir, y "leer", y reservar el frame entre las dos partes (el chunk se sigue leyendo después de reservar, D73 e).
**Test exigido.**
- Los dos `audit_r5_first_chunk_*` de `inv2/r`, que agregan dos pausas bajo `cfg(test)` y el módulo `api/tests/relay.rs`.
- La mutación que ningún test detectaba: leer el chunk antes de reservar tiene que hacer fallar uno (backlog de la auditoría).

**Interacciones a revisar:**
- el presupuesto de respuestas (D73, D75): el nuevo orden no puede volver a abrir DEF-152;
- el ancla de descarga: los chunks anclados no miran el rango;
- el piso que publica el actor (DEF-166) y la marca de hueco (DEF-164).

**Hecho.** `get_chunk` se partió en `pick_chunk`, que elige el snapshot y recién después espera el rango (si el chunk no tiene ancla), y `read_picked`, que lee el archivo. `request_chunk` elige, reserva el frame y lee, en ese orden. `get_chunk` queda solo para los tests (`cfg(test)`), como la composición de las dos.

**Tests.** Los dos de `inv2/r` se reemplazaron por uno, en `api/tests/data_plane.rs`, contra el handler, con un solo hook nuevo en `pick_chunk` (`first_chunk_picked`, entre elegir el snapshot y pedir el rango) en lugar de las dos pausas y el módulo nuevo del parche. `first_chunk_does_not_serve_a_snapshot_below_a_gap_found_after_it_was_picked` instala un snapshot en 2. Mientras el chunk 0 está detenido en el hook, se borra el segmento 3..=4 y un sync encuentra el hueco. El parche movía el log con una pasada de TTL, pero no hace falta: lo que se prueba es el orden. Tampoco sirve una poda por cursores, porque el snapshot activo la ancla. El otro test del parche (un snapshot más nuevo descartado contra un rango viejo) no se puede expresar con un punto después de elegir; las dos regresiones de orden que cubría las detecta este (mutaciones 1 y 2). La revisión de delta mostró que con el hook en el handler, antes de elegir, la mutación 2 no se detectaba.

Para la mutación que ningún test detectaba se extendió `responses_take_a_frame_of_the_clients_budget_before_they_are_built`. El chunk sin presupuesto ahora está anclado a un snapshot instalado, y un hook nuevo en `read_chunk` (`relay_read_chunk`) comprueba que el archivo no se leyó.

**Mutaciones** (cada una restaurada después):
- leer el rango en el handler, antes de elegir, como antes: falla el test nuevo;
- en `pick_chunk`, leer el rango antes de elegir: falla el test nuevo;
- leer el chunk antes de reservar: falla el del presupuesto.

**Interacciones:**
- **El presupuesto de respuestas (D73, D75):** el rango se espera dentro de `pick_chunk`, antes de reservar, así que el chunk 0 no retiene el frame mientras espera a la sala (DEF-152). Lo sigue probando `a_request_waiting_for_its_room_holds_none_of_the_budget`. El archivo se lee después de reservar (mutación 2).
- **El ancla de descarga:** un chunk anclado nunca espera el rango, como antes. Lo prueba el mismo test anterior (el chunk anclado sin la sala).
- **El piso (DEF-166) y la marca de hueco (DEF-164):** llegan en el `LogBounds` que se lee después de elegir. El primer test nuevo pasa por la marca de hueco.
- **El snapshot reemplazado entre elegir y leer:** `read_chunk` responde `SnapshotSuperseded` si el archivo ya no está, como antes. Si todavía está, sirve los bytes del snapshot elegido con su propio hash, así que la descarga es de un solo snapshot (INV-R5).
- **Un `.await` nuevo:** no hay. El del rango se movió del handler a `pick_chunk`, y soltar el future ahí no deja nada a medias.
- **Un snapshot más nuevo instalado entre elegir y leer el rango** (sumada en la revisión): el elegido se compara con un rango posterior y puede responder `RoomNotFound`. `drop_stale` compara el hash, así que no borra el nuevo, y el cliente reintenta el chunk 0.

#### DEF-180 · Bajo · ✅ Hecho
**Problema.** Un chunk con la secuencia, el layout y el hash de la subida en curso de otro miembro entra en ella sin que se mire el uploader, y la rompe.
**Solución.** Agregar `&& current.yields_to(&uploader)` a la guarda de `continues` (`relay/upload.rs`).
**Test exigido.** `audit_a3_a_chunk_with_the_parameters_of_another_members_upload_breaks_it` (`inv2/ya`).

**Interacciones a revisar:**
- el reintento de un chunk del propio uploader;
- el worker (admin secret);
- la herencia del tope de D102, que toca la misma función: probar las dos juntas.

**Hecho.** Como dice la solución.

**Test.** Se extendió `snapshot_worker_preempts_a_member_upload_at_the_same_seq` (`relay/tests/upload.rs`), en lugar de traer el de `inv2/ya`: otro miembro, además de no poder reemplazar la subida falsa, tampoco puede mandarle un chunk con sus mismos parámetros.

**Mutación:** sin el `yields_to` en la guarda, el test falla.

**Interacciones:**
- **El reintento de un chunk del propio uploader:** `yields_to` es verdadero para él, así que sigue continuando la subida. Lo prueban los tests de subida fuera de orden y de reenvío.
- **El worker:** también continúa o reemplaza, como antes. Lo prueba el mismo test. Como `yields_to(Admin)` es verdadero, el worker sigue continuando la subida de un miembro con sus mismos parámetros (sumada en la revisión). No cambia con este fix, y el caso inverso, un miembro que adivina el hash de la subida del worker, ya lo rechaza la guarda.
- **La herencia del tope de D102:** el chunk ajeno con los mismos parámetros cae en la rama que lo rechaza, antes de buscar el tope. Los tests de los dos ítems pasan juntos.

**Revisión de delta del lote.** La hizo un agente aparte, de contexto fresco y solo lectura. Confirmó que DEF-176 y DEF-180 cierran su escenario y que sus mutaciones hacen fallar un test, y aceptó la desviación de los tests de DEF-174. Encontró:
- **Una vía Media que dejaba abierto DEF-174:** completar la subida con un último chunk basura la descartaba sin recordar el tope. Se cerró con la precisión (e) de D102, con su test y su mutación.
- **Un hueco de test Bajo en DEF-176:** el hook estaba antes de elegir. Se movió a `pick_chunk`.
- Las interacciones que faltaban, sumadas arriba en cada ítem.
- `register_reports_only_a_usable_snapshot` falló una vez en la suite completa y pasó en las corridas sueltas. Es el test intermitente que ya está en el backlog; el diff no lo toca.

Los cambios posteriores a la revisión (la precisión e y el hook) no volvieron a revisarse.

**Cierre del lote:**
- **Chequeos locales:** fmt y clippy del workspace en verde; antes de los últimos dos cambios, `cargo test --workspace` y `cargo doc` en verde (macOS). Después de esos cambios, los tests de `relay::` y de `data_plane` pasan y sus mutaciones fallan. No toca `core` ni `storage`, así que no hace falta el build de wasm32.
- **La suite completa sobre el diff final** queda a cargo de la CI, a pedido del dueño (la máquina local no daba más).
- **Versiones largas del servidor** (`--release -- --ignored`): no corrieron sobre el diff final. Quedan para la próxima corrida semanal de `randomized.yml` o para correrlas a mano antes de la revisión de cierre del Lote 5.

---

## Lote 5 — Cierre

> Objetivo: que los documentos describan lo que cambió la fase, y la revisión de cierre que reemplaza a la tercera auditoría (D90).

#### Documentación · ⬜ Pendiente
**Solución.** Cada lote actualiza sus documentos al cerrar; este lote verifica que no falte nada y suma:
- **`ARCHITECTURE.md`:** la salvedad de la línea 234 sobre el reintento de una subida en un solo request (DEF-178, **(barato)**) y la corrección de la línea 245 sobre la subida en curso durante un borrado (backlog).
- **ROADMAP:** la Fase 3.10 y el estado del requisito para la Fase 4.

**Test exigido.** Ninguno. Revisión cruzada de cada afirmación cambiada contra el código.

#### Revisión de cierre (D90) · ⬜ Pendiente
**Qué es.** Un agente aparte, de contexto fresco y en modo solo lectura, revisa el delta completo de la fase (`a2f6806..HEAD`) contra `INVARIANTES.md`, con los riesgos de §4 y la regla de severidad de D91. Usa los veredictos de §1.1, pero solo sobre lo que la fase tocó. Responde:
- si cada ítem cierra su escenario y si su mutación inversa hace fallar un test;
- si las interacciones listadas en cada nota **Hecho** están completas y probadas;
- si el delta rompe alguna invariante.

Un hallazgo trae su test, como en la auditoría. Antes de la revisión, todos los tests de ruptura de este plan están en el repo y pasan, y las versiones largas corrieron en verde (`randomized.yml`, a mano).

**Resultado:**
- **Sin una ★ rota, una ruptura Alta ni una Media de origen (a):** `INVARIANTES.md` §1.4 queda en verde y el ROADMAP marca cumplido el requisito para la Fase 4.
- **Con alguna:** se agrega a este plan como ítem nuevo (DEF-182 en adelante) en un Lote 6, y la revisión de cierre se repite **solo sobre el delta de ese lote**.
- **Lo de origen (b) o (c), Medio o Bajo:** al backlog.

---

## Fuera de este plan

**Decididos en esta fase:**
- **DEF-171:** los nombres de archivo son confiables (D92).
- **DEF-173:** riesgo aceptado y requisito del SDK de la Fase 4 (D93).
- **DEF-179:** se ajusta Y9 (D94).
- **La propuesta S7:** riesgo aceptado hasta la Fase 4 (D96).
- **La propuesta R9:** se decide con el worker (D96).

**Siguen abiertos y diferidos, sin cambios:** DEF-85 (el límite de la ventana de dedup), DEF-34 (`reload_schema` en storage), DEF-81 (snapshots falsos), DEF-54 (Zstd en wasm32), el residuo de DEF-95 (la conexión SSE semiabierta) y D77 (la reescritura de `active.wal` después de un EIO en Linux).

## Backlog Bajo

Sigue vigente el [backlog de la Fase 3.9](2026-10-fase3_9-remediation-plan.md#backlog-bajo). Se suma el de la 2ª auditoría (§7), salvo lo que ya entra en los lotes:

| Ítem del §7 de la auditoría | Dónde entra |
|---|---|
| `recover_snapshots` sin sync de `snapshots/` | Lote 1 (DEF-169) |
| Los barridos de cancelación de storage en `#[ignore]` | Lote 0 |
| Faltaban tests de `write_checked` y `verified_json` | Lote 0 |
| El trait no documenta que soltar las operaciones no las cancela | Lote 0 (D93) |
| `reopening_loads_into_ram_only_what_fits` no mira el pico | Lote 2 |
| Leer el chunk del relay antes de reservar no lo detecta ningún test | Lote 4 (DEF-176) |
| `ARCHITECTURE.md`, línea 245 | Lote 5 |
| `drop_stale` borra en el future de la request | Lote 3 (D101) |

**Tests:** `register_reports_only_a_usable_snapshot` (`actor/tests/room.rs`) corre en tiempo real y falla bajo carga: 4 de 15 corridas con otros tests pesados en paralelo, 0 de 15 sin otra carga. Hay que pasarlo a tiempo pausado.

**Nuevo en el Lote 3 (revisión de delta), Bajo:** el test del binario (`server_binary_tests.rs`) puede pasar sin probar nada si el último chunk no se procesa entero en los 300 ms antes del corte, y deja el proceso hijo corriendo si falla un assert antes de `server.wait()`. Solución: esperar a que el upload llegue a la promoción (por ejemplo, con un `.part` del tamaño total), y matar el hijo con un guard.

**Nuevo en el Lote 2 (revisión de delta), Bajo:** ante una anomalía en un cold, `read_cold_file` descomprime en memoria el resto del segmento (hasta `segment_max_bytes`) para clasificarla. La apertura lo hace si un cold duplicado o un cold sin sidecar están dañados, y eso retiene más que la ventana de RAM y un frame. Necesita un daño accidental y son bytes de wire. Solución: clasificar recorriendo el stream sin juntarlo, o acotar lo que se lee al frame siguiente.

Más los tres Bajos que no entran (D97):
- **DEF-177** (R7): el piso se olvida cuando el actor se detiene, y una subida en vuelo se confirma sin chequearlo. Solución: que la tarea del actor no borre el piso mientras haya una subida en curso, y que `cleanup_expired` lo borre junto con el slot.
- **DEF-178** (R7): el reintento de una subida en un solo request no mira el piso. Solución: comparar con el piso en las dos ramas de `is_active` y responder `SnapshotSuperseded`. La salvedad de `ARCHITECTURE.md` entra en el Lote 5.
- **DEF-181** (A3, R4): las subidas chicas fallidas de un miembro ocupan el lock del relay y congelan la designación. Solución: que `upload_in_progress_by` consulte la sesión aunque el lock esté ocupado, o que `designate` mire el plazo y la conexión antes de volver.
