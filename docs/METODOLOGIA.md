# Metodología de trabajo por fases y lotes

**Fecha:** 21 de Octubre de 2026
**Alcance:** desde la Fase 4. Reúne las reglas que la [Fase 3.11](proposals/2026-10-fase3_11-restricciones-proposal.md) escribió en su §5 y la regla de cierre de la Fase 3.10 (D90), para que cada plan las referencie en lugar de copiarlas. Un plan agrega solo lo propio (por ejemplo, un checklist para los lotes de un crate).
**Contrato:** [`INVARIANTES.md`](INVARIANTES.md) y [`ARCHITECTURE.md`](../ARCHITECTURE.md). Reglas de código: [`GEMINI.md`](../GEMINI.md).

---

## 1. Niveles de plan

- **Plan de fase (plan maestro).** Dice qué entra y qué no, las sub-fases con su orden y sus dependencias, qué entrega cada una y cómo cierra, las decisiones de la fase y el seguimiento. No baja a lotes. Vive en `docs/proposals/AAAA-MM-faseN-plan.md`.
- **Plan de sub-fase.** Se escribe justo antes de arrancar la sub-fase, no al principio de la fase, para aprovechar lo aprendido en la anterior. Baja a lotes: objetivo, archivos, qué se hace, tests exigidos, interacciones a revisar y nota **Hecho**. Vive en `docs/proposals/AAAA-MM-faseN_M-<tema>.md`.
- **El seguimiento vive en el plan**, en una tabla de sub-fases o de lotes con su estado, y no en un documento aparte.
- **`ROADMAP.md`** es el índice de fases, con el enlace al plan de cada una, y el backlog para después de la v0.1. Lo que una fase construye vive en su plan (en la Fase 4, en su Apéndice A).

## 2. El contrato primero

- **Lote 0.** Toda fase, y toda sub-fase que agrega garantías, arranca con un lote que solo toca documentos: las invariantes nuevas o ajustadas en `INVARIANTES.md`, las decisiones del plan y, si cambia, el modelo de amenaza (§2 de `INVARIANTES.md`). Las invariantes se escriben antes que el código, para que el código se revise contra lo que tiene que garantizar y no al revés.
- **Decisiones.** Cada una tiene un id con el prefijo de su plan (DR en la Fase 3.11, DC en la Fase 4) y un estado: *Propuesta*, *Adoptada* o *Diferida*. Una *Propuesta* se confirma con el dueño del proyecto antes de escribir código que dependa de ella.
- **Revisión cruzada.** El Lote 0 cierra con una revisión hecha por un agente aparte, de contexto fresco y solo lectura, que contrasta cada enunciado nuevo con el código actual. Una invariante que todavía no se cumple se marca con el lote que la va a cumplir (estado 🆕 de `INVARIANTES.md` §1.5).

## 3. Spikes

- **Cuándo.** Una decisión que depende de una incógnita técnica (una API de plataforma, el comportamiento de una dependencia, un costo) se apoya en un spike antes de adoptarse.
- **Cómo.** Acotado en tiempo y en alcance: responde una pregunta escrita de antemano. El código del spike no se mergea; lo que queda es la respuesta, anotada en la decisión, con lo que se midió y en qué entorno.

## 4. Reglas de trabajo

1. **Tamaño:** un lote es una unidad revisable de **~300 líneas de producción como máximo**; los tests no cuentan. Si se pasa, se parte. Idealmente, cada lote es una sesión propia.
2. **Tests:** siguen la estructura de dos niveles de `GEMINI.md`. Cada regla de comportamiento nueva tiene al menos un test negativo.
3. **Sin códigos** (`DC`, `DR`, `INV-`, `DEF-`) en código, tests ni docstrings (`GEMINI.md`).
4. **Chequeos de cierre:** cada lote cierra con:
   - `cargo fmt --all -- --check`;
   - `cargo clippy --workspace --all-targets -- -D warnings`;
   - `cargo test --workspace`;
   - `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.

   Si el lote toca `core`, `storage` o `client`, también `cargo build -p zemdb-core -p zemdb-storage -p zemdb-client --target wasm32-unknown-unknown` (con las features de wasm del cliente, cuando existan).
5. **Pruebas largas:** las simulaciones, fuzzers y tests de imágenes de crash tienen una versión corta en la suite normal (unos segundos en debug) y una larga con `#[ignore]`. Las largas corren en `.github/workflows/randomized.yml` (semanal y a mano) y una vez al cerrar cada lote: `cargo test -p <crate> --release -- --ignored` de los crates tocados. Nunca se agregan a la CI de cada push.
6. **Documentos:**
   - `ARCHITECTURE.md` se actualiza en el mismo commit que el comportamiento que describe.
   - `INVARIANTES.md` se actualiza cuando cambia una garantía.
   - El plan se actualiza al cerrar cada lote.
7. **Alcance:** lo que no está en el lote no se hace en el lote (§5, paso 6).

## 5. Procedimiento por lote

Este procedimiento, junto con `GEMINI.md` y el plan del lote, alcanza para retomar el trabajo sin el contexto de sesiones anteriores.

**0. Arranque.**
- **El árbol:** `git pull`, con el árbol de trabajo limpio y la CI de `main` en verde en las tres plataformas.
- **Qué leer:**
  - la sección del lote en su plan;
  - las decisiones que cita;
  - las invariantes que toca en `INVARIANTES.md`;
  - `GEMINI.md`;
  - las secciones de `ARCHITECTURE.md` del componente que toca.
- **Decisiones en estado *Propuesta*:** se confirman con el dueño del proyecto antes de escribir código.

**1. Primero los tests exigidos.**
- Se escriben contra la API nueva, con un esqueleto que compila y conserva el comportamiento viejo (sin `todo!()`).
- Se comprueba que fallan por el comportamiento, no solo por no compilar. Un test que pasa sin la implementación no prueba nada y se reescribe.

**2. La implementación mínima.** Lo que piden las decisiones del lote, sin adelantar el siguiente. Siguiendo `GEMINI.md`: validar antes de mutar, nada de I/O bloqueante ni trabajo de CPU largo en el runtime, y tipos deserializados validados antes de usarse.

**3. Verificación.**
- **Chequeos:** los de la regla 4 de §4.
- **Mutación inversa:** deshacer cada regla que agrega el lote, una por vez, y confirmar que algún test falla. Después, restaurarla.
- **Versiones largas:** las de la regla 5 de §4.
- **Windows:** un test no lee un archivo que la sala tiene abierto.

**4. Revisión de delta.**
- **Quien implementa** lista, en la nota **Hecho** del lote, las interacciones del cambio: los mecanismos que ya existían y que leen o escriben el mismo estado. Cada lote trae una lista inicial, que no es exhaustiva. Para cada interacción va un test que la ejercite, o el argumento de por qué no hace falta.
- **El revisor** es independiente: un agente aparte, de contexto fresco y en modo solo lectura. Recibe el diff, las decisiones del lote, las invariantes que toca, los tests exigidos y la lista de interacciones. Responde:
  1. si el lote implementa lo que dicen sus decisiones, y nada más;
  2. si deshacer cada regla hace fallar un test;
  3. si la lista de interacciones está completa, y cuál falta;
  4. si el diff rompe alguna invariante, existente o nueva;
  5. si se cumplen los checklists propios del plan (por ejemplo, el de determinismo de la Fase 3.11, §5.4, en los lotes que tocan cómo decide una réplica).

Como en las auditorías, sin un test que lo reproduzca no hay hallazgo Alto ni Medio.

**5. Documentos, en el mismo cambio.**
- **`ARCHITECTURE.md`:** la parte del comportamiento que el lote implementa.
- **`INVARIANTES.md`:** el estado de las invariantes que el lote implementa.
- **El plan:** el lote pasa a ✅, con una nota **Hecho** que lista los tests agregados, las mutaciones verificadas, las interacciones revisadas y cualquier desvío respecto de lo planeado.

**6. Alcance.** Lo que aparezca fuera del lote no se arregla en el momento:
- si afecta a la feature en curso, se agrega al plan, en el lote que corresponda;
- si es un defecto preexistente, va al backlog con la severidad que le corresponda por `INVARIANTES.md`.

**7. Entrega.**
- Los cambios se dejan sin commitear para que el dueño revise el diff.
- Con su visto bueno, se arma un commit por lote, o pocos, con el estilo `feat(client): … (Fase 4.2, Lote N)`, y se pushea a `main`.
- El lote queda cerrado recién con la CI de `main` en verde en las tres plataformas.

## 6. Revisiones y auditorías

| Nivel | Cuándo | Quién | Contra qué | Criterio |
|---|---|---|---|---|
| **Revisión de delta** | Al cerrar cada lote | Un agente aparte, de contexto fresco y solo lectura | El diff del lote, sus decisiones, las invariantes que toca y su lista de interacciones | Las cinco preguntas de §5, paso 4 |
| **Revisión de cierre** | Al cerrar cada sub-fase y cada fase | Un agente aparte, de contexto fresco y solo lectura | Todas las invariantes, mirando solo lo que la sub-fase o la fase tocó y los mecanismos con los que eso interactúa | Las tres condiciones de `INVARIANTES.md` §1.4 |
| **Auditoría amplia** | Cuando cambia el modelo de amenaza o entra código fuera del alcance auditado (`INVARIANTES.md` §1.4) | Un auditor por grupo de invariantes (`INVARIANTES.md` §1.6) | Las invariantes nuevas, con el método de `INVARIANTES.md` §1 | Las tres condiciones de `INVARIANTES.md` §1.4 |

- **Lo que pide una revisión:** si una revisión de cierre o una auditoría no sale en verde, sus rupturas se arreglan en lotes nuevos de la misma sub-fase (como los Lotes 6b y 6c de la Fase 3.11), cada uno con su revisión de delta, antes de cerrarla.
- **Lo que no bloquea:** las rupturas Medias y Bajas de origen (b) o (c) que no entran en un lote van al backlog (`INVARIANTES.md` §1.4).

## 7. Estrategia de pruebas

Cada plan adapta estas cuatro capas a su componente (la Fase 3.11 las aplicó al storage en su §5.3):

1. **Reglas.** Un test por cada regla de comportamiento, positivo y negativo.
2. **Modelo de referencia.** Una especificación ejecutable que no comparte código con la implementación (como el oráculo de `crates/storage/tests/common/oracle.rs`). Entradas aleatorias aplicadas al modelo y a la implementación tienen que dar lo mismo.
3. **Simulación, convergencia y recuperación.** Ejecuciones aleatorias con fallas inyectadas (crashes en cada paso, cancelaciones en cada `.await`, red que pierde, demora o repite) que verifican las invariantes al final y en el medio.
4. **Robustez.** Fuzzing de toda entrada que no es de confianza: nunca un pánico, nunca una reserva desmedida.
