# Auditoría por Invariantes (2ª ronda, acotada)

**Fecha:** 18 de Octubre de 2026
**Commit auditado:** `b4cb390` (`main`), al cierre del plan de la [Fase 3.9](../proposals/2026-10-fase3_9-remediation-plan.md)
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md), versión 2 (50 invariantes, 22 núcleo ★)
**Auditoría anterior:** [`2026-10-invariantes-audit.md`](2026-10-invariantes-audit.md), sobre `50fe195` (DEF-144 a DEF-158)
**Alcance:** el que pide el §1 del plan:
- las 18 invariantes que se rompieron en la auditoría anterior;
- las 10 que se agregaron después (28 en total, 13 núcleo);
- la revisión del delta `50fe195..b4cb390`, también contra las 22 invariantes restantes.

---

## 1. Metodología

Es la segunda auditoría con el método de `INVARIANTES.md` §1.

**Auditores.** Fueron 8, en paralelo, cada uno sobre su propio worktree de `b4cb390` y con tests propios:

| Auditor | Invariantes |
|---|---|
| d | D1★, D2★, D3★, D7★, D8★ |
| m | D9★, E1★, W2 |
| s | S1★, S3★, S6 |
| ya | Y4, Y9, A3 |
| r | R5, R6, R7 |
| b | B1★, B2, B3, B4, B5 |
| l | D5★, D6★, L2, L3★, L4, L6 |
| delta | El delta de la Fase 3.9: si cada fix cierra su escenario, la mutación inversa, y las 22 invariantes fuera del alcance frente al delta (D4, S2, S4, S5, Y1–Y3, Y5–Y8, R1–R4, A1, A2, A4, L1, L5, E2, W1) |

**Veredicto obligatorio por invariante.** El origen (a) se mide sobre `50fe195..b4cb390` con `git blame`, `git log -L` y `git show 50fe195:…`. En varios casos, además, se corrió el mismo test sobre un worktree de `50fe195`.

**Verificación del coordinador.** Cada ruptura se reprodujo aplicando el parche de su grupo sobre un worktree limpio de `b4cb390`: todos los tests de ruptura fallan con la salida que se cita en §4.

**Fase adversaria.** Quedó parcial, porque se cortó por su costo.
- **Desafiantes de las ★ que se sostuvieron:**
  - el del log (D2, D3, D8) encontró DEF-168;
  - el del storage (S1, S3) encontró DEF-173;
  - el de D5, L3 y E1 se cortó antes de terminar, así que esas tres quedan con el veredicto de su primer auditor.
- **Escépticos por ruptura** (uno por alcanzabilidad y uno por contrato): terminaron 5, y ninguno refutó:
  - por contrato: DEF-176, DEF-177, DEF-178 y DEF-180, los cuatro con severidad y origen confirmados;
  - por alcanzabilidad: DEF-181, con confianza media.

**Coincidencias entre auditores:**
- DEF-174 la encontraron por separado b y el revisor del delta.
- El escenario de DEF-168 lo anotó m como Bajo, y el desafiante del log lo llevó a un test.

### Estado del build en `b4cb390`

| Chequeo | Resultado |
|---|---|
| `cargo fmt --all -- --check` | Sin diferencias |
| `cargo clippy --workspace --all-targets -- -D warnings` | Sin warnings |
| `cargo test --workspace` | 774 pasan, 0 fallan, 39 ignorados |
| `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` | Sin warnings |
| `cargo build` y `cargo clippy` de `zemdb-core`, `zemdb-storage` y `zemdb-client` hacia `wasm32-unknown-unknown` | Sin errores ni warnings |

### Revisión del delta de la Fase 3.9

- **Los 23 ítems (DEF-144 a DEF-166) cierran el escenario de la auditoría anterior,** salvo DEF-153: cierra el escenario literal, pero queda una variante (DEF-174).
  - Todos los tests que citan las notas **Hecho** existen y pasan.
  - Además se portaron tres escenarios de los parches anteriores, que ahora pasan: el `log_meta.json` dañado, el segmento más nuevo perdido y el chunk 0 por debajo de un hueco.
- **Mutación inversa: 52 mutaciones** (41 del servidor y 11 de storage). Todas las de los ítems Alto y Medio hacen fallar algún test. Ningún test detecta estas:
  - las cuatro que el plan ya admitía:
    - comparar el piso antes del promote;
    - retener `log.lock` hasta después del rename, que solo detecta la CI de Windows;
    - quitar `max_send_buf_size`;
    - reservar para `Schema` y `Registered` después de copiar el esquema;
  - sacar solo un `fsync`, en el servidor o en storage;
  - una que el plan no listaba: leer el chunk del relay antes de reservar.

  Ninguna rompe una invariante en el código actual. Son Bajo (§7).
- **De las 22 invariantes fuera del alcance,** el delta toca el código de 21, y todas se sostienen salvo R4 (DEF-174).
- **La ronda introdujo 4 rupturas (a) y agrandó 2** (ver §2).

---

## 2. Resultado por invariante

**De las 28 del alcance se rompen 15 y se sostienen 13; ninguna queda indeterminada.** Además, de las 22 que se revisaron contra el delta se rompe R4. En total, 16 de 50.

| Grupo | Invariante | En `50fe195` | En `b4cb390` | Ruptura |
|---|---|---|---|---|
| §3.1 | INV-D1 ★ Commit confirmado sobrevive | ❌ | ❌ | DEF-167 |
| | INV-D2 ★ Seq nunca reutilizada | ❌ | ✅ | — |
| | INV-D3 ★ Recuperación no descarta datos | ❌ | ❌ | DEF-168 |
| | INV-D5 ★ Un solo dueño | ❌ | ✅ | — |
| | INV-D6 ★ Un proceso por `data_dir` | 🆕 | ❌ | DEF-172 |
| | INV-D7 ★ Nada visible antes de ser durable | 🆕 | ❌ | DEF-169 |
| | INV-D8 ★ Piso de secuencia fuera del log | 🆕 | ✅ | — |
| | INV-D9 ★ Metadatos verificados | 🆕 | ❌ | DEF-171 |
| §3.2 | INV-S1 ★ `apply_batch` durable o sin efecto | ❌ | ❌ | DEF-173 |
| | INV-S3 ★ Crash se recupera al mismo estado | ❌ | ❌ | DEF-173 |
| | INV-S6 Memoria igual a disco | 🆕 | ✅ | — |
| §3.3 | INV-Y4 Idempotencia en la ventana | ❌ | ✅ | — |
| | INV-Y9 Ventana de dedup función del log | 🆕 | ❌ | DEF-179 |
| §3.4 | INV-R4 Todo atrasado se pone al día (por el delta) | ✅ | ❌ | DEF-174 |
| | INV-R5 Solo snapshots usables | ❌ | ❌ | DEF-176 |
| | INV-R6 Snapshot atado a la encarnación | 🆕 | ✅ | — |
| | INV-R7 Subida confirmada usable | 🆕 | ❌ | DEF-177, DEF-178 |
| §3.5 | INV-A3 Efectos confinados | ❌ | ❌ | DEF-180, DEF-181 |
| §3.6 | INV-B1 ★ Memoria por request | ❌ | ❌ | DEF-170 |
| | INV-B2 Nada sin plazo | ❌ | ❌ | DEF-174 |
| | INV-B3 Memoria, disco y FDs acotados | ❌ | ✅ | — |
| | INV-B4 Costo por request | ❌ | ✅ | — |
| | INV-B5 Ningún cliente acapara | 🆕 | ✅ | — |
| §3.7 | INV-L2 Cancelar no deja estado a medias | ❌ | ❌ | DEF-175 |
| | INV-L3 ★ I/O incierto nunca es éxito | ❌ | ✅ | — |
| | INV-L4 Reabrir no cambia lo confirmado | ❌ | ✅ | — |
| | INV-L6 Borrado de sala atómico | 🆕 | ✅ | — |
| §3.8–3.9 | INV-E1 ★ El esquema solo crece | ❌ | ✅ | — |
| | INV-W2 Roundtrip y checksums | ❌ | ❌ | DEF-171 |

Las otras 21 invariantes, revisadas frente al delta, se sostienen: D4, S2, S4, S5, Y1, Y2, Y3, Y5, Y6, Y7, Y8, R1, R2, R3, A1, A2, A4, L1, L5, E2 y W1 (argumentos en el reporte del delta).

### Criterio de corte (§1.4): **no está en verde**

Fallan las tres condiciones:
1. **Se rompen 8 invariantes núcleo:** D1, D3, D6, D7, D9, S1, S3 y B1.
2. **Hay 4 rupturas Altas:** DEF-167 a DEF-170.
3. **Dos rupturas Medias tienen origen (a):** DEF-172 y DEF-175.

### El patrón

1. **La gravedad bajó respecto de la primera ronda.**
   - Entonces se rompían 9 de 18 ★; ahora se rompen 8 de las 22 del contrato, y los 23 ítems del plan cierran su escenario (salvo la variante de DEF-153).
   - Ninguna ruptura núcleo ocurre en el camino normal, salvo B1, que necesita un esquema ancho legal. Las demás ★ necesitan un disparador (c):
     - un daño accidental en un lugar preciso, combinado con un crash en una ventana (DEF-167, DEF-168);
     - un sync de directorio que falla (DEF-169);
     - un bit invertido en el nombre de un archivo (DEF-171);
     - un apagado con E/S en vuelo (DEF-172, DEF-173).
   - Las rupturas que no necesitan nada raro son de miembros hostiles en las subidas: DEF-174 (Media) y las Bajas.
2. **Las 4 rupturas introducidas (a) son efectos laterales de fixes de la ronda.** Cada una nace de un fix que no se miró junto con un mecanismo que ya existía:
   - DEF-167: D81 junto con D78;
   - DEF-172: D83 junto con `#[tokio::main]`;
   - DEF-175: D84 junto con la cancelación de la request;
   - DEF-176: el orden de D73 junto con la elección del snapshot.

   D69 agrandó dos más (DEF-171, DEF-179). Según §1.3, un origen (a) apunta al proceso: la revisión de delta de cada lote miró cada fix por separado, pero no sus interacciones con lo que ya estaba.
3. **Tres causas raíz explican 11 de los 15 defectos:**
   - **Un efecto en disco que sobrevive al lock que lo serializa** (DEF-172, DEF-173, DEF-175, como antes DEF-150 y DEF-165). Por eso se propone INV-L7 (§6).
   - **Decidir con datos del disco sin verificarlos, o sin mirar el contenido:**
     - los nombres de archivo (DEF-171);
     - el par cold/warm (DEF-168);
     - un piso por encima del fin real de `active.wal` (DEF-167);
     - un directorio que nadie sincronizó (DEF-169).
   - **Subidas y designación:** los fixes de D74, D76 y DEF-166 cerraron el caso literal de cada ítem, no sus variantes (DEF-174, DEF-177, DEF-178, DEF-181).
4. **Lo que se sostiene** tiene redes en el repo y redes nuevas de esta ronda:
   - orden, entrega exacta, convergencia y retención;
   - durabilidad del append y piso de secuencia: D2 y D8, con una simulación de fallas aleatorias y reaperturas;
   - checksums de los JSON (D82), probados bit por bit en los cuatro archivos;
   - storage frente a la cancelación del future y a los crashes: simulaciones con pánicos y operaciones soltadas;
   - presupuesto por cliente (B5), cuota (B3) y borrado con lápida (L6).

---

## 3. Resumen de defectos

Se continúa la numeración: **DEF-167 a DEF-181**.

| ID | Sev. | Origen | Invariantes | Resumen |
|---|---|---|---|---|
| DEF-167 | Alto | (a), disparador (c) | D1★ | Un commit confirmado queda detrás de un hueco en `active.wal`: la apertura truncó su último frame dañado por debajo de `sealed_through_seq`. |
| DEF-168 | Alto | (b), disparador (c) | D3★ | La apertura borra la copia cold verificada de un segmento cuando existe su warm, aunque el warm esté dañado. |
| DEF-169 | Alto | (b), disparador (c) | D7★ | El registro de esquemas sirve `schemas/*.json` al arrancar sin sincronizar `schemas/`. |
| DEF-170 | Alto | (b) | B1★ | Abrir una sala decodifica entero `active.wal`, antes de recortar a la ventana de RAM. |
| DEF-171 | Medio | (b) + (a) en `501d29b`, disparador (c) | D9★, W2 | Los rangos de los segmentos y la sala de los snapshots salen de nombres de archivo que nada verifica. |
| DEF-172 | Medio | (a), disparador (c) | D6★ | El servidor suelta `server.lock` antes de que el runtime termine de escribir. |
| DEF-173 | Medio | (c) | S1★, S3★ | Storage: una tarea cancelada por el apagado del runtime suelta el lock de la sala con E/S todavía en vuelo. |
| DEF-174 | Medio | (b) | B2, R4 | El designado que reinicia su subida al vencer el tope conserva la designación sin plazo. |
| DEF-175 | Medio | (a), disparador (c) | L2 | Un `POST /admin/rooms` cortado deja una purga en vuelo que borra el snapshot de la sala creada después. |
| DEF-176 | Bajo | (a), disparador (c) | R5 | El chunk 0 compara el snapshot que sirve con un rango leído antes de elegirlo. |
| DEF-177 | Bajo | (b), disparador (c) | R7 | El piso se olvida cuando el actor se detiene, y una subida en vuelo se confirma sin chequearlo. |
| DEF-178 | Bajo | (b), disparador (c) | R7 | El reintento en un solo request de la subida activa se confirma sin mirar el piso. |
| DEF-179 | Bajo | (b) + (a) por D69, más (c) | Y9 | La ventana de dedup en memoria conserva claves de operaciones podadas hasta el próximo respawn. |
| DEF-180 | Bajo | (b), disparador (c) | A3 | Un chunk con los parámetros de la subida ajena entra en ella y la rompe. |
| DEF-181 | Bajo | (b) | A3, R4 | Las subidas chicas fallidas de un miembro ocupan el lock del relay y congelan la designación. |

---

## 4. Detalle de los defectos

### Durabilidad y recuperación

#### DEF-167 · Alto · (a), disparador (c) · Un commit confirmado queda detrás de un hueco en `active.wal`
- **Lo encontró:** el auditor d.
- **Escenario:**
  1. Una rotación registra `sealed_through_seq = e` (D81) y el proceso muere antes del rename de `active.wal`.
  2. El último frame de `active.wal` (el `e`, ya sincronizado) aparece dañado por accidente.
  3. La apertura lo trunca como escritura rota (INV-D3, D78), pero `head_seq` queda en `e` por el piso.
  4. El commit `e+1` se confirma y se escribe en el mismo `active.wal`, justo después de `e-1`.
  5. Al reabrir:
     - la apertura falla, porque exige contigüidad entre frames;
     - o bien, si en el medio se selló por cantidad, el segmento se llama `1..=e+1`. Las lecturas cuentan secuencias desde el nombre, saltean `e+1` y responden `BehindCompaction`, y la compresión lo rechaza para siempre.
- **Dónde:**
  - `crates/server/src/log/tiered_log.rs:173-182`: `head_seq` toma `sealed_through_seq`, que queda por encima del fin real de `active.wal` (`warm_disk.rs:244-268`).
  - `tiered_log.rs:229-237` y `warm_disk.rs:71-113`: `append` solo compara con `head_seq`, y `append_record` no comprueba que el registro continúe `active_end_seq`.
  - Al reabrir: `segment_reader.rs:180-187` y `cold_disk.rs:200-205`.
- **Por qué es D1 y no la excepción D78.** D78 acepta perder, y reutilizar, la secuencia `e`. Acá `e` no se reutiliza, así que D2 y D8 se cumplen. Lo que se pierde es `e+1`, confirmado después de la reapertura e intacto en disco.
- **Tests** (`d.patch`):
  - `audit_d1_commit_after_a_damaged_last_frame_below_the_sealed_end_survives_a_reopen`:
    ```
    INV-D1: commit 6 was acknowledged, and the room no longer opens: WAL corruption detected: WAL frame at offset 340 starts at sequence 6, expected 5
    ```
  - `audit_d1_commit_after_a_damaged_last_frame_below_the_sealed_end_is_readable`:
    ```
    INV-D1: commit 6 was acknowledged and a read from 5 fails: Client is behind compaction, full snapshot required
    ```
  - `audit_d1_commit_acknowledged_after_a_truncated_frame_below_the_sealed_end_is_served`, de punta a punta con `CommitAck`:
    ```
    INV-D1: CommitAck 4 was sent, and a sync from 3 after a reopen fails: Client is behind compaction, full snapshot required
    ```
- **Origen: (a), introducida en `3aa4cc3`** (D81). `git blame` de `tiered_log.rs:175` y `:268-271`. En `50fe195` la rotación no escribía nada antes del rename, y `head_seq` era el máximo de lo podado, los segmentos y `active.wal`. Con el mismo daño, `e` se reutilizaba (D78) y `active.wal` seguía contiguo.
- **Disparador (c), doble:**
  - un crash en la ventana entre `record_meta` y el rename, que dura microsegundos y se abre una vez cada `ram_max_ops` commits;
  - además, un daño justo en el último frame.
- **Severidad: Alto, la de D1.** Es el mismo criterio que con DEF-148. Atenuantes: falla cerrada, no inventa nada y los bytes siguen en disco.
- **Solución** (probada por el auditor en una copia):
  - Al abrir, si `active_end_seq < head_seq`, sellar `active.wal` enseguida con su rango real, y en `seal_active_segment` grabar `sealed_through_seq = max(end, meta.sealed_through_seq)`. Así la secuencia perdida queda como un hueco (`BehindCompaction`), como prevé D8.
  - Como defensa, que `append` rechace o selle antes un registro que no continúe `active_end_seq`.

#### DEF-168 · Alto · (b), disparador (c) · La apertura borra la copia cold verificada cuando existe su warm
- **Lo encontró:** el desafiante del log. El auditor m lo había anotado como Bajo.
- **Escenario:**
  1. Una compresión deja en disco el cold, ya renombrado, sincronizado y verificado, y también el warm original. Pasa por un crash o un corte antes de que el borrado del warm sea durable, o por un borrado fallido dos veces seguido de la sala detenida antes del reintento.
  2. Desde ese momento el índice lee el cold, y el warm es una copia vieja.
  3. Después, el warm se daña por accidente (§2).
  4. La apertura borra el cold intacto, porque existe un warm del mismo rango, y se queda con la copia dañada. Las operaciones confirmadas del segmento ya no se pueden leer: todo cursor por debajo recibe `BehindCompaction`.
- **Dónde:**
  - `crates/server/src/log/segment_index.rs:142-151`: la regla de duplicados de `scan` manda el cold a borrar sin leer ninguna de las dos copias.
  - La ventana está en `cold_disk.rs:82-104` (rename, verificación y borrado del warm) y en `tiered_log.rs:446-457` (`record_compression` y `undeleted_warm`).
  - `ARCHITECTURE.md` §5.2 lo describe como esperado: "a cold segment whose warm original still exists (the warm segment is kept and compressed again later)".
- **Test:** `audit2_d3_open_never_drops_a_verified_cold_copy_for_a_damaged_warm_one` (`c-log.patch`). Prueba con daño en el frame 1 y en el 4:
  ```
  INV-D3 (damaged frame 1): the open deleted the verified cold copy (still there: false) and kept the damaged warm one; a read from 0 gives Err(BehindCompaction)
  ```
- **Origen: (b).** La regla es idéntica en `50fe195` (`segment_index.rs:132-142`), y el mismo test falla ahí.
- **Disparador (c):** un crash o una falla de E/S en la ventana, más un daño del warm antes de la apertura siguiente.
- **Severidad: Alto, la de D3.** Atenuantes: doble disparador, falla cerrada (la sala pide un snapshot) y los ids de dedup sobreviven en el sidecar.
- **Solución.** Ante un cold y un warm del mismo rango, verificar el cold antes de decidir: `read_cold_file`, exigiendo exactamente `start..=end`.
  - Si verifica, se borra el warm y se queda el cold.
  - Si no verifica, se borra el cold, como hoy.

  Son unas 20 líneas, probadas en una copia. Hay que ajustar `open_keeps_the_warm_original_of_an_interrupted_compression`, que fija la regla vieja, la frase de §5.2 y el doc de `CompressedSegment::cleanup_error`.

#### DEF-169 · Alto · (b), disparador (c) · `schemas/` no se sincroniza al arrancar
- **Lo encontró:** el auditor d.
- **Escenario:**
  1. `register_schema` o `add_column` escribe `schemas/{id}.json` con el patrón tmp, sync y rename, pero el sync de `schemas/` falla, y la escritura devuelve error.
  2. El servidor se reinicia y `SchemaRegistry::new` sirve la entrada que quedó en el page cache. Sobre ella se crean salas y se confirman commits, sin que ningún `fsync` de este proceso haya cubierto esa entrada de directorio.
  3. Un corte de energía puede borrarla o, en el caso de `add_column`, devolver la versión anterior por debajo de operaciones que ya usan la columna nueva (y eso rompe E1).
- **Dónde:**
  - `crates/server/src/schema_registry.rs:31-85`: `create_dir_all_synced` solo sincroniza lo que crea, y el listado no hace `sync_dir`.
  - `ServerConfig::lock_data_dir` (`config.rs:479-492`) sincroniza la entrada `schemas/` dentro de `data_dir`, pero no lo que hay adentro de `schemas/`.
- **Test:** `audit_d7_schema_left_unsynced_by_a_failed_write_is_not_served_unsynced_after_a_restart` (`d.patch`). Cubre la registración y `add_column`:
  ```
  INV-D7: schema todo is served after a restart, but no fsync of this process covered its directory entry (the only sync of it failed)
  ```
- **Origen: (b).** `SchemaRegistry::new` es igual en `50fe195`. La revisión de delta del Lote 2 sumó el sync de `data_dir`, pero no el de las entradas de adentro de `schemas/`.
- **Disparador (c):** un sync de directorio que falla, más un reinicio, más un corte de energía.
- **Severidad: Alto, la de D7.** Es el patrón de DEF-144.
- **Solución.** Llamar a `durable::sync_dir(schemas/)` en `SchemaRegistry::new` antes de leer (probado por el auditor), y sumar `schemas/` al "Sostenida por" de D7 y a §5.2. El mismo patrón, con un disparador más raro, aparece en el relay (`snapshots/`, §7).

#### DEF-171 · Medio · (b) + (a), disparador (c) · Los nombres de archivo deciden sin checksum
- **Lo encontró:** el auditor m.
- **Escenario.** Un bit invertido en la entrada de directorio de un segmento cambia el rango que el servidor lee del nombre. Según el caso:
  - la apertura borra un segmento retenido;
  - una poda por TTL graba el fin dañado como `pruned_through_seq`, con su checksum, y la apertura siguiente borra segmentos más jóvenes: el CRC termina certificando un valor que salió de un nombre;
  - la cuota, que cuenta los sidecars a partir del rango del nombre, borra el log entero;
  - una poda por cursores borra un segmento necesario.

  En el relay, la sala y la secuencia también salen del nombre del `.snap`, y el BLAKE3 cubre solo el contenido. Si se invierte el nombre de la sala, después de un reinicio el snapshot se le ofrece a otra sala.
- **Dónde:**
  - `crates/server/src/log/segment_index.rs:112` (borra si `end <= pruned_through_seq`) y `:287` (`parse_segment_filename`, que ni siquiera exige `start <= end`);
  - `tiered_log.rs:545` (`pruned_end` sale de los nombres);
  - `segment_index.rs:53-58`, junto con `tiered_log.rs:467` y `:527` (cuota);
  - `relay/recovery.rs:51` y `relay/files.rs:50`;
  - la compresión sí contrasta el nombre con el contenido (`cold_disk.rs:220`); la poda no.
- **Tests** (`m.patch`). Cada uno comprueba que los dos nombres difieren en un solo bit:

  | Test | Qué muestra |
  |---|---|
  | `audit_d9_damaged_segment_name_makes_the_open_delete_a_retained_segment` | `…11_…0015.wal` pasa a `…11_…0005.wal`, y la apertura borra 11..=15 |
  | `audit_d9_ttl_prune_of_a_damaged_name_makes_the_next_open_delete_younger_segments` | El cold `1..5` pasa a `1..25`: el TTL lo poda, `pruned_through_seq=25` queda con CRC y la apertura borra 6..=20 |
  | `audit_w2_damaged_segment_name_makes_maintenance_delete_the_whole_log` | `6..10` pasa a `6..8000000000000010`: la cuota cuenta 1,9·10¹⁷ bytes y borra todo; `head` salta de 20 a 8·10¹⁵ |
  | `audit_w2_damaged_segment_name_lets_a_cursor_prune_delete_a_needed_segment` | `11..15` pasa a `11..05`: una poda en 11 lo borra y el cursor 10 recibe `BehindCompaction` |
  | `audit_w2_damaged_snapshot_name_hands_the_snapshot_to_another_room` | `room-a_5_<hash>.snap` pasa a `room-c_…`: el relay le ofrece a room-c el snapshot de room-a |
- **Origen: (b),** del borrado por nombre (`918289eb`) y de `pruned_end` (`10b01d3`). Cuatro de los cinco tests fallan igual en `50fe195`.
- **Amplificación (a) en `501d29b`.** El test de la cuota pasa en `50fe195`, porque ahí la cuota sumaba el tamaño real y solo podaba cold.
- **Disparador (c):** un daño en los metadatos del sistema de archivos.
- **Severidad: Medio, bajada desde Alto** (la de D9). ext4 con `metadata_csum`, XFS v5, btrfs y ZFS verifican los bloques de directorio y devuelven EIO en lugar de un nombre alterado. Pero §2 no excluye este daño ("bytes dañados, archivos faltantes"), y DEF-148 ya trató como amenaza un segmento que desaparece. Hay que decidirlo (§6).
- **Solución:**
  1. Al abrir, antes de borrar nada, validar cada rango indexado contra el piso verificado: `start <= end <= meta.sealed_through_seq`, ascendentes y sin solaparse. Si no cumple, la apertura falla, como con un `log_meta.json` ilegible. Para eso, `SegmentIndex::scan` tiene que devolver los sobrantes en lugar de borrarlos.
  2. Acotar `pruned_end` a `meta.sealed_through_seq`.
  3. En el relay, poner un CRC de `<room>_<seq>_<hash>` en el nombre, o llevar un índice escrito con `write_checked`.
  4. Corregir §5.2 *Atomic Server Metadata*, que dice "never deletes a segment based on a value it did not verify".

### Dueño, cancelación y apagado

#### DEF-172 · Medio · (a), disparador (c) · `server.lock` se suelta antes de que el runtime deje de escribir
- **Lo encontró:** el auditor l.
- **Escenario:**
  1. El servidor A recibe SIGTERM mientras una tarea separada trabaja en el pool bloqueante. Puede ser una subida por chunks cuyo cliente cortó después del último chunk (`run_to_completion` sigue con el sync, la verificación y el promote), o un `DELETE` cortado que todavía purga el relay.
  2. A suelta `server.lock` al terminar el cuerpo de `main`, y recién después suelta el runtime, que espera a esas tareas.
  3. En esa ventana un servidor B toma el lock y arranca (`clear_uploads`, `recover_snapshots`, borrado de lápidas) mientras A sigue escribiendo.

  Los logs de las salas siguen protegidos por `log.lock`.
- **Dónde:**
  - `crates/server/src/main.rs:17` declara el lock como variable local del cuerpo de `#[tokio::main]`.
  - La macro se expande a `Builder::…build().block_on(body)`, y `Runtime::drop` espera a los `spawn_blocking` en curso.
  - Las tareas en vuelo salen de `api/relay.rs:185`, `api/control_plane.rs:146` y `relay/files.rs:29`.
- **Test:** `audit_d6_server_binary_releases_the_data_dir_lock_before_it_stops_writing` (`l.patch`). Corre el binario real, solo en Unix, y falló en 4 de 4 corridas:
  ```
  server.lock was free while the first server was still running (alive: true), and that server promoted the upload into snapshots/ after it released the lock (staged when unlocked: false, after exit: true): a second server could have started and used data_dir meanwhile
  ```
  Con el lock filtrado al final de `main`, pasa.
- **Origen: (a), introducida en `3aa4cc3`** (D83). En `50fe195` no había lock.
- **Severidad: Medio, bajada desde Alto.** `log.lock` sigue protegiendo el log de cada sala, así que quedan expuestos solo el relay y las lápidas. En un experimento, la purga en vuelo de A borró un snapshot que el relay de B había confirmado.
- **Solución.** Un `main` síncrono que tome el lock, arme el runtime, haga `block_on` y suelte el runtime antes de soltar el lock. Otra opción es filtrar el handle para que lo suelte el sistema operativo al salir.

#### DEF-173 · Medio · (c) · Storage: una tarea cancelada por el apagado del runtime suelta el lock de la sala con E/S en vuelo
- **Lo encontró:** el desafiante del storage.
- **Escenario.** D86 y D89 garantizan que ninguna operación de archivo aterriza después de reabrir la sala, pero solo mientras el runtime sigue sondeando la tarea. Si el runtime se apaga (`shutdown_background`, `shutdown_timeout`, o un `drop` en otro hilo mientras otro runtime sigue), la tarea se suelta en su `.await`. Con ella se van el `Arc<DiskRoom>`, `room_{id}.lock` y el handle del WAL, mientras la closure del pool bloqueante, que solo tiene rutas, aterriza más tarde.
  - **S1:** el rename de `apply_snapshot` aterriza debajo de una sala que se reabrió en otro runtime con el snapshot anterior (head 3). Esa sala confirma con `Ok` los lotes 4 a 6, y la apertura siguiente carga el snapshot de head 5 y saltea 4 y 5.
  - **S3:** la limpieza de la fase 3 de una compactación borra el `.wal.compacting` nuevo, al que la sala reabierta ya había rotado los lotes 4 a 6. Un crash en la fase 2 de esa compactación reabre la sala en head 3.
- **Dónde:**
  - `crates/storage/src/disk/compactor.rs:478-493` (`rename_staged_snapshot`) y `:169-171`/`:530-540` (`remove_if_exists`): en las dos, la closure de `run_blocking` no retiene el lock.
  - `disk/mod.rs:217`: el lock vive en `DiskRoom`.
  - `sys.rs`: `run_blocking` es un `spawn_blocking` común.
  - El mismo patrón aparece en `compactor.rs:153` y `:189`.
- **Tests** (`c-storage.patch`, solo Unix; los dos fallaron en 6 de 6 corridas):
  - `audit2_s1_rename_of_a_snapshot_task_cancelled_by_a_runtime_shutdown_lands_under_the_reopened_room`:
    ```
    batch 4, acknowledged with Ok by the room opened at head 3, is gone after the next open (head 6): the rename of the cancelled snapshot task landed under the open room
    ```
  - `audit2_s3_cleanup_of_a_compaction_cancelled_by_a_runtime_shutdown_removes_the_next_rotated_wal`:
    ```
    a crash while compacting batches 4 to 6 reopens the room at head 3: the cleanup of the compaction cancelled by the runtime shutdown removed their `.wal.compacting` (left: 3, right: 6)
    ```
- **Origen: (c).** Es el residuo de DEF-150 y DEF-165: D86 y D89 cerraron el caso de quien suelta el future, pero no el de la tarea cancelada. La ventana de la compactación ya existía, porque la compactación corría en su propia tarea.
- **Severidad: Medio**, como DEF-150 y DEF-165. Se baja desde la de S1 y S3 porque exige apagar el runtime con E/S en vuelo y reabrir la sala en el mismo proceso dentro de esa ventana. Por ejemplo, un binding que crea un runtime por llamada.
- **Solución.** Atar el lock de la sala a cada operación de archivo, no a la tarea: guardarlo en un `Arc<File>` y mover un clon a cada closure del pool bloqueante que toque archivos de la sala, también en la recuperación. Un prototipo con dos closures hace pasar los dos tests. La alternativa es aceptarlo como riesgo (§6).

#### DEF-175 · Medio · (a), disparador (c) · Una creación de sala cortada borra el snapshot de la sala creada después
- **Lo encontró:** el auditor l.
- **Escenario:**
  1. Se corta un `POST /admin/rooms` mientras `create_room` espera la purga del relay (D84). El `run_blocking` de la purga sigue corriendo sin el spawn lock y sin el lock del slot.
  2. El admin reintenta: la sala se crea, recibe commits y el relay le confirma un snapshot.
  3. La purga vieja termina y borra el archivo de ese snapshot.

  El relay lo sigue teniendo como activo y la sala lo considera usable, así que no pide otro. Las descargas fallan con `NotFound` hasta que vence el TTL (7 días) u otra subida lo reemplaza.
- **Dónde:**
  - `crates/server/src/api/control_plane.rs:105-115`: `create_room` corre en el future de la request, mientras que `delete_room` corre en su propia tarea (`:146`).
  - `actor/manager.rs:534`.
  - `relay/lifecycle.rs:22-44`.
- **Test:** `audit_l2_dropped_room_creation_purges_the_snapshot_of_the_room_created_after_it` (`l.patch`):
  ```
  the purge of a room creation whose request was dropped deleted the snapshot that the relay confirmed afterwards for the room created by the retry: files left [], active snapshot Some(SequenceNumber(1)), download Err(Io(Os { code: 2, kind: NotFound, ... }))
  ```
  Con `create_room` en una tarea propia, pasa.
- **Origen: (a), introducida en `8fc1d8c`** (Lote 5). En `50fe195`, `create_room` no tenía ningún `.await` después del spawn lock.
- **Severidad: Medio**, la de L2. En la práctica el efecto es del nivel de R7 o R4.
- **Solución.** Correr `create_room` en su propia tarea y documentar que no es *cancel-safe*. Mejor todavía: que la purga retenga el lock del slot hasta su última operación de archivo, que es la propuesta INV-L7.

### Recursos

#### DEF-170 · Alto · (b) · Abrir una sala decodifica entero `active.wal`
- **Lo encontró:** el auditor b.
- **Escenario.** La sala tiene un esquema ancho, legal por `MAX_COLUMNS`, y un miembro confirma filas de `Null`. Cuando la sala se reabre (por inactividad, reinicio o fallo), la request que dispara la apertura materializa todas las operaciones de `active.wal`. Recién después la ventana de RAM las recorta.
- **Dónde:**
  - `crates/server/src/log/warm_disk.rs:235-241`: `inspect_active_segment` lee con `ReadBudget::ops(usize::MAX)`.
  - `tiered_log.rs:171`, con el recorte en `:192-197`.
  - El camino desde la request: `RoomManager::slot`, `spawn_actor` y `RoomActor::open` (`actor/manager.rs:293-299`, `actor/room.rs:147`).
  - Lo mismo, con disparador (c), en `tiered_log.rs:771-772`: un segmento sin sidecar se decodifica entero.
- **Cuánto,** con la configuración por defecto (`active.wal` se sella a las 1.000 operaciones o a los 32 MiB de wire):

  | Esquema (filas de `Null`) | Operaciones | Valores | Memoria |
  |---|---|---|---|
  | 4.096 columnas, medido | 300 | 1.229.100 | 29,5 MB |
  | 4.096 columnas, estimado | 999 | ~4,1 M | ~98 MB |
  | 65.535 columnas, estimado | ~511 | ~33,5 M | ~800 MB |
- **Test:** `audit_b1_opening_a_room_decodes_all_of_active_wal` (`b.patch`):
  ```
  opening the room decoded 300 operations: 1229100 values, 29498400 bytes in memory (MAX_MESSAGE_VALUES 1048576, ram_max_bytes 8388608)
  ```
- **Origen: (b).** Viene de `10b01d3`, y `50fe195` hace la misma lectura. D73 cambió cómo se cobra la HotBuffer, pero no cómo se llena al abrir.
- **Severidad: Alto.** Tiene la misma precondición que DEF-152, que D80 mantuvo en Alto. La cota depende de la configuración (`segment_max_bytes` × 24 por byte de `Null`), pero es una cota por apertura, no por request, y varias aperturas concurrentes se suman.
- **Solución.** Validar todos los frames y juntar los ids de mutación, reteniendo mientras se lee solo una ventana deslizante acotada por `ram_max_ops` y `ram_max_bytes`. En `segment_mutations`, juntar solo los ids.

#### DEF-174 · Medio · (b) · El designado conserva la designación reiniciando su subida al vencer el tope
- **Lo encontraron:** el auditor b y el revisor del delta, por separado.
- **Escenario.** El designado sube por chunks: manda uno nuevo antes de cada plazo de inactividad y nunca manda el último. Cuando la subida llega a `snapshot_upload_max_secs`, manda otro chunk, que encuentra la sesión vencida y arranca una nueva con un tope nuevo. Así la designación queda congelada (D76) por tantos topes como quiera, y con ella el ancla de poda y el actor abierto. Nunca completa nada, y los clientes `Bootstrapping` no ven un snapshot usable.
- **Dónde:**
  - `crates/server/src/relay/upload.rs:263`: `expire_locked` corre antes de mirar la subida en curso.
  - `upload.rs:295-302`: el tope se hereda solo si queda una sesión viva para reemplazar.
  - `relay/lifecycle.rs:102`, `actor/room.rs:843-853` y `actor/snapshot_demand.rs:142`.
- **Tests:**
  - `audit_b2_upload_restarted_at_its_deadline_outlives_the_cap` y `audit_b2_designee_restarting_its_upload_at_the_deadline_keeps_the_role` (`b.patch`). El segundo corre en tiempo real y falló en 4 de 4 corridas:
    ```
    round 2: the uploader has held an upload in progress for 599s, past snapshot_upload_max_secs (300s)
    the designee kept its role for 9.03s with uploads it never completes (snapshot_upload_max_secs 3s, designation timeout 2s); designations meanwhile: []
    ```
  - `audit_b2_designee_keeps_the_role_by_restarting_its_upload_at_each_deadline` y `audit_b2_an_upload_restarted_at_each_deadline_is_never_bounded` (`delta.patch`), con tiempo pausado y deterministas:
    ```
    the designee still holds the role 13.7s after its first chunk, past the 3.3s total deadline of its upload, without completing any upload (events: [])
    ```
  - Los controles, en los que el designado no reinicia y pierde el rol, pasan.
- **En producción.** El tope es predecible: 2 h desde el primer chunk. Al atacante le alcanza con una ráfaga de chunks alrededor de cada vencimiento, al costo de un chunk de 64 KiB cada 2 minutos. Solo pierde el rol si un tick cae en la ventana de milisegundos entre el vencimiento y el chunk siguiente, del orden de 0,5 % cada 2 h.
- **Origen: (b).** En `50fe195` no había tope, y cualquier chunk renovaba el plazo (DEF-153). `501d29b` agregó el tope y lo hereda cuando el uploader reinicia con la sesión viva, pero no a través de un vencimiento: el fix de DEF-153 quedó incompleto.
- **Severidad: Medio**, la de B2 y R4. Requiere un miembro hostil (§2) y no pierde datos.
- **Solución, una de dos:**
  - que `SnapshotDemand::designate` deje de congelar la designación cuando ya pasaron `designation_timeout + snapshot_upload_max_secs` desde que se designó;
  - que el slot recuerde el tope de la última sesión de cada uploader y la sesión siguiente lo herede también después de un vencimiento.

### Snapshots, subidas y aislamiento

#### DEF-176 · Bajo · (a), disparador (c) · El chunk 0 compara con un rango leído antes de elegir el snapshot
- **Lo encontró:** el auditor r. El escéptico de contrato la confirmó.
- **Escenario:**
  1. El handler de un chunk 0 sin ancla lee el rango.
  2. Una pasada de TTL borra segmentos por encima de S.
  3. Una subida cuyo rango se leyó antes de la pasada se instala en la ventana del límite aceptado de R7, antes de que se publique el piso.
  4. El handler elige el snapshot activo (esa subida), lo compara con el rango viejo y sirve su chunk 0, aunque tail−1 > S.

  El gemelo Bajo es el caso inverso: el chunk 0 descarta un snapshot más nuevo y usable, comparándolo con el rango viejo.
- **Dónde:** `crates/server/src/api/relay.rs:101-104` (`Either::Left(ready(log_bounds(..).await))`), con la elección en `relay/download.rs:38` y la comparación en `:51-53`.
- **Tests** (`r.patch`, que agrega dos pausas bajo `cfg(test)`):
  - `audit_r5_first_chunk_serves_a_snapshot_installed_after_it_read_the_range`:
    ```
    the first chunk offered the snapshot at 2 while the room's log has tail 7 and head 6
    ```
  - `audit_r5_first_chunk_drops_a_newer_usable_snapshot_against_the_range_it_read_before`, el gemelo.
- **Origen: (a), introducida en `501d29b`** (DEF-152: el chunk 0 reserva después de pedir el rango). En `50fe195`, el future del rango se esperaba después de elegir el snapshot.
- **Severidad: Bajo** (la de R5 es Medio). Hacen falta dos ventanas a la vez, y el costo es una descarga inútil seguida de `BehindCompaction` y un nuevo bootstrap. Es el precedente de DEF-164.
- **Consecuencia en el contrato.** Invalida la justificación del límite aceptado de R7 ("el snapshot no se anuncia ni se sirve"), y comparar con el piso dentro de `get_chunk` no alcanza para cerrarla.
- **Solución.** Partir `get_chunk` en "elegir y comprobar", que espera el rango, y "leer", y reservar el frame entre las dos partes.

#### DEF-177 · Bajo · (b), disparador (c) · El piso se olvida cuando el actor se detiene
- **Lo encontró:** el auditor r. El escéptico de contrato la confirmó.
- **Escenario:**
  1. Una subida lee el rango.
  2. Una pasada de TTL o de cuota poda más allá de S y publica el piso.
  3. El actor se detiene, por un append o una rotación fallidos o por un pánico, y su tarea borra el piso.
  4. La subida se instala antes de que la sala se reabra, sin chequeo, y responde `SnapshotStaged`.

  `shutdown_all` no sirve de disparador: corre después de drenar las conexiones.
- **Dónde:** `crates/server/src/actor/manager.rs:435` (`clear_snapshot_floor`), y `relay/upload.rs:404-407`, donde `is_some_and` no chequea nada si no hay piso.
- **Test:** `audit_r7_upload_installed_after_its_actor_stopped_is_confirmed_below_the_floor` (`r.patch`). El caso en que la sala se reabre antes de la instalación pasa:
  ```
  reopened before the install: false; the relay answered Ok("SnapshotStaged") to the upload at 2, but the log's tail is 7 and the room offers None
  ```
- **Origen: (b).** En `50fe195` no había piso, y cualquier pasada confirmaba la subida (DEF-166). `8fc1d8c` dejó abierto este subcaso, con la decisión de que el piso vive lo que el actor.
- **Severidad: Bajo**, la de R7. Fuera del límite aceptado: el piso sí se publicó, y la ventana dura hasta la reapertura.
- **Solución.** Que la tarea del actor no borre el piso mientras haya una subida en curso, y que `cleanup_expired` lo borre junto con el slot. La alternativa es sumarlo al límite de R7.

#### DEF-178 · Bajo · (b), disparador (c) · El reintento en un solo request no mira el piso
- **Lo encontró:** el auditor r. El escéptico de contrato la confirmó.
- **Escenario.** El uploader no recibió `SnapshotStaged` y reenvía el mismo snapshot. Mientras el reintento se valida o espera el lock del slot, una pasada poda más allá de S. El reintento entra por la rama `is_active` y devuelve `Ok` sin mirar el piso.
- **Dónde:** `crates/server/src/relay/upload.rs:185-188`. La rama gemela de los chunks (`:265-268`) es la cláusula 2 del límite de R7.
- **Test:** `audit_r7_retried_single_request_upload_is_confirmed_below_the_floor` (`r.patch`):
  ```
  the relay answered Ok("SnapshotStaged") to the retry at 2 with the room's floor at Some(SequenceNumber(6)); the room offers None
  ```
- **Origen: (b).** El retorno temprano ya estaba en `50fe195`, y `8fc1d8c` agregó el piso solo en `promote_and_install`.
- **Severidad: Bajo.**
- **Solución.** Comparar con el piso en las dos ramas de `is_active` y responder `SnapshotSuperseded`; eso cierra también la cláusula 2 del límite. Además, `ARCHITECTURE.md:234` ("repeating a single-request upload of the active snapshot returns success") necesita una salvedad.

#### DEF-179 · Bajo · (b) + (a), más (c) · La ventana de dedup conserva claves de operaciones podadas
- **Lo encontró:** el auditor ya.
- **Escenario.** Ninguna poda, ni por cursores ni por TTL ni por cuota, saca de la ventana en memoria las claves de las operaciones podadas. En cambio, la ventana que se rearma en un respawn lee solo el log retenido. Así, un mismo reintento recibe la secuencia original antes del respawn, y después se secuencia otra vez o recibe `BehindCompaction`.
- **Dónde:** `crates/server/src/actor/room.rs:928` y `:968` (podas), `dedup.rs:48` (el desalojo es solo por capacidad) y `log/tiered_log.rs:731-758` (hidratación).
- **Tests** (`ya.patch`):
  - `audit_y9_a_pruned_mutation_is_recognised_only_until_the_room_respawns`:
    ```
    with the retained log 7..=8, a retry of mutation 1 got (sequence, head) (Ok(1), 8) before a respawn and (Ok(9), 9) after it
    ```
  - `audit_y9_a_mutation_pruned_by_the_quota_is_recognised_only_until_the_room_respawns`: `(Ok(1), 12)` antes y `(Ok(13), 13)` después.
  - `audit_y9_a_damaged_segment_without_sidecar_changes_the_window_at_the_respawn`, el caso (c): `(Ok(4), 6)` antes y `(Ok(7), 7)` después.
- **Origen: (b).** El caso de cursores falla igual en `50fe195`. D69 lo agrandó (a): antes la cuota solo podaba cold, y ahora alcanza mutaciones de hace minutos. El caso (c) es el `continue` de DEF-158 sobre un segmento dañado sin sidecar.
- **Severidad: Bajo.** La memoria solo reconoce de más, y lo que pasa después del respawn es lo que admite DEF-85.
- **Solución.** Hay que elegir:
  - **(A) ajustar Y9:** antes de un respawn, la ventana también puede conservar claves de operaciones podadas o ilegibles;
  - **(B) desalojar las claves con secuencia menor que `tail_seq` después de cada poda.** Son unas 5 líneas, pero cambia `retried_commit_from_client_now_behind_retention_returns_original_seq`, que fija el comportamiento actual.

  El caso (c) necesita (A) de todos modos. Además, `ARCHITECTURE.md` §5.1, en su punto 4, tiene dos frases que tiran para lados opuestos.

#### DEF-180 · Bajo · (b), disparador (c) · Un chunk con los parámetros de la subida ajena la rompe
- **Lo encontró:** el auditor ya. El escéptico de contrato la confirmó.
- **Escenario.** Mallory manda un chunk con la secuencia, el layout y el hash de la subida en curso de Alice, pero con otros bytes. El chunk entra por la rama `continues` sin que se mire el uploader. Cuando Alice completa la subida, la verificación BLAKE3 falla y su subida se descarta.
- **Dónde:** `crates/server/src/relay/upload.rs:270-277`. La regla `yields_to` (`:106`, `:284`) solo se aplica cuando los parámetros difieren.
- **Test:** `audit_a3_a_chunk_with_the_parameters_of_another_members_upload_breaks_it` (`ya.patch`):
  ```
  Mallory's chunk (Ok(false)) went into Alice's upload at the same sequence: Alice's last chunk got Err(BadRequest("BLAKE3 digest verification failed: the assembled snapshot does not match its hash")), and the active snapshot is None
  ```
- **Origen: (b).** La guarda es idéntica en `50fe195`.
- **Disparador (c):** hace falta el hash de una subida en curso, que ninguna respuesta revela.
- **Severidad: Bajo.** Viola el texto literal de A3 ("subida en curso ajena con la misma secuencia") y de `ARCHITECTURE.md:235`. Pero el mismo poder ya se tiene, aceptado, subiendo en S+1 (DEF-81). Un miembro puede incluso llevar el head a S+1 con un commit propio. Es el precedente de DEF-155.
- **Solución.** Agregar `&& current.yields_to(&uploader)` a la guarda de `continues` (verificado: con eso la suite `relay::` pasa).

#### DEF-181 · Bajo · (b) · Las subidas chicas fallidas de un miembro congelan la designación
- **Lo encontró:** el auditor ya. El escéptico de alcanzabilidad la confirmó, con confianza media.
- **Escenario.** Mallory no es la designada. Mantiene en vuelo dos subidas de un chunk chico con un hash incorrecto, que es el tope por sala y no recibe `RateLimited`. Cada una retiene el lock del slot mientras su archivo se sincroniza y se verifica. `upload_in_progress_by` toma un lock ocupado como una subida del designado. Resultado: un designado que no sube, o que se desconectó, nunca se reemplaza, porque `designate` vuelve antes de mirar el timeout y `is_connected`.
- **Dónde:** `crates/server/src/relay/upload.rs:463-465`, `actor/room.rs:846` y `actor/snapshot_demand.rs:142-144`.
- **Test:** `audit_a3_a_member_failing_small_uploads_keeps_a_designee_that_does_not_upload` (`ya.patch`, tiempo real, 6 s). Con la mutación que trata el lock ocupado como "sin subida", pasa:
  ```
  in 6s the writer, which never uploaded, was never replaced as the designee (snapshot events meanwhile: []; Mallory's failed uploads: 1127, rate limited: 0)
  ```
  El test no tiene latencia de red. En producción, que el lock quede ocupado en cada tick depende del tiempo de `fsync` y del tráfico, y basta un tick con el lock libre para que el designado se reemplace.
- **Origen: (b).** Es el residuo de DEF-163, por la aproximación del lock que dejó D76 (plan, línea 652).
- **Severidad: Bajo.** Un designado honesto y conectado igual puede completar su subida esperando el lock.
- **Solución.** Que `upload_in_progress_by` consulte la sesión aunque el lock esté ocupado, o que `designate` mire el timeout y la conexión antes de volver.

---

## 5. Ajustes al documento de invariantes

Son veredictos "se sostiene" o rupturas en los que el enunciado promete más, o menos, que el contrato real.

| Invariante | Ajuste propuesto |
|---|---|
| INV-D2, INV-D8, D78 | La lista de archivos de D8 es del log, pero el roster (`last_ack_seq`) y el relay (la secuencia del snapshot activo) también registran secuencias asignadas. En el caso D78, una secuencia que ellos tienen se reutiliza igual: el snapshot en S se vuelve usable con el efecto viejo, y un cliente con cursor S nunca recibe la operación nueva. Hay dos caminos: decir que la lista es cerrada (y corregir la fila de D78 en §4), o sumar al piso el máximo cursor del roster y descartar en el relay los snapshots por encima del head. |
| INV-D3 (storage) | Desde D85, un último frame dañado de `.wal.compacting` solo puede venir de un daño accidental. Hoy se trata como escritura rota, y si el WAL activo ya se vació, el lote confirmado se pierde (`compacting_wal_with_a_torn_tail_is_folded`). Es el equivalente de D78 en storage: se puede listar como riesgo en §4, o tratar como escritura rota solo el daño que el WAL activo continúa. |
| INV-D5 | Precisar que cubre los archivos de la sala, es decir, su directorio. El relay lo protegen el lock del slot y `server.lock`. |
| INV-D6 | Con el fix de DEF-172, precisar que el lock se retiene hasta que el proceso termina. |
| INV-D7 | Hoy dice "antes de que lo cubra un `fsync` exitoso hecho por este proceso". Al abrir no se vuelve a sincronizar el contenido de los segmentos, sidecars, JSON ni `.snap`. Proponer: "…o, para un archivo que solo toma su nombre definitivo después de un `fsync` exitoso de quien lo escribió, antes de que este proceso sincronice su directorio". Sumar `schemas/` y `snapshots/` a "Sostenida por". |
| INV-D9, INV-W2 | Según lo que se decida para DEF-171 (§6). D9: "ni a partir de un valor verificado que se derivó de uno sin verificar". W2: o se cubren los nombres, o el enunciado dice "todo byte del contenido". |
| INV-E1 | En "Sostenida por", sumar D82, el lock del registry y el orden de la recarga. |
| INV-S1 | En "Sostenida por": `apply_batch` no corre en una tarea. Lo que impide que su E/S en vuelo aterrice después de reabrir es el lock del handle del WAL más el `flush` de `close_room`. D86 y D89 valen mientras el runtime siga sondeando la tarea (DEF-173). |
| INV-S3 | "…o el del snapshot, si `apply_snapshot` devolvió `Ok`, falló después del rename o se soltó con su tarea en curso". |
| INV-S6 | Vale en los puntos sin una operación en curso. Mientras corren las tareas de operaciones soltadas, la memoria va detrás del disco, y `close_room` espera a esas tareas. |
| INV-Y4 | "Con un segmento sellado dañado" solo vale si su sidecar se puede leer. |
| INV-Y6 | Un `/register` que recibe `Unavailable` por falta de presupuesto ya redefinió el cursor. Es inocuo, porque el reintento es idempotente. |
| INV-Y9 | Definir "log retenido" frente a los segmentos ilegibles, y decidir entre las opciones (A) y (B) de DEF-179. |
| INV-R5 | Decir "el chunk 0 sin ancla": un chunk 0 anclado se sirve sin mirar el rango. Las excepciones a "no se reemplaza por uno menor" incluyen la purga de `delete_room` y de `create_room`. |
| INV-R7 | El límite aceptado se apoya en R5 ("no se sirve"), y eso es falso mientras siga DEF-176. DEF-177 y DEF-178 no están dentro del límite. |
| INV-A3 | Los commits de un miembro pueden sacar claves ajenas de la ventana compartida (DEF-85), y con la cuota (D69) pueden dejar atrás a otros clientes. Las dos cosas pasan por el log de la sala, no por el estado del otro cliente. |
| INV-B1 | `max(wire, 24 × valores)` es una cota inferior: con `Text` o `Bytes` cortos, la memoria real llega a 2–2,5 veces eso. Las copias de hasta 64 KiB que se entregan a h2 quedan fuera del presupuesto. Nombrar la apertura de una sala disparada por una request (DEF-170). |
| INV-B2 | El tope total tiene que valer por uploader o por designación, no por subida (DEF-174). |
| INV-B4 | Un `Ack` que poda hace un unlink por cada segmento podado dentro del actor. Es amortizado, pero es trabajo proporcional a la historia dentro de una sola request. |
| INV-B5 | Con la parte por defecto (2 frames sobre unos 16 en total), 8 clientes que cooperan agotan el total. El enunciado es por cliente, así que se cumple, pero no cubre ese caso. |
| INV-L2 | "Cortar una tarea equivale a un crash" no es exacto: lo que está en `spawn_blocking` sigue corriendo (DEF-172, DEF-173, DEF-175). Sumar `create_room` a las operaciones de varios pasos. |
| INV-L6 | Si la purga falla después del rename durable, el relay conserva el snapshot de la sala borrada hasta su TTL, y un `get_chunk` anclado lo sigue sirviendo sin mirar el rango (`relay/download.rs:42-51`). |

## 6. Propuestas de invariantes y decisiones pendientes

**Invariantes propuestas.** Agregarlas es una decisión del dueño (§1.2):

| Propuesta | Enunciado | Origen |
|---|---|---|
| INV-L7 | **Ningún efecto en disco sobrevive al lock que lo serializa.** Una operación de archivo lanzada bajo un lock (spawn lock, slot del relay, `server.lock`, lock de la sala del storage) termina antes de que el lock se suelte, o corre en una tarea que lo retiene hasta su última operación. | DEF-172, 173, 175; antes DEF-150 y 165 |
| INV-W3 | **Los nombres que el servidor interpreta se validan antes de usarse:** el rango de un segmento, la sala y la secuencia de un snapshot, el id de un esquema. Se contrastan con un dato verificado antes de borrar, podar, contar cuota o asignar. | DEF-171 |
| INV-D10 | **Cada archivo del log es contiguo por construcción.** Ni `active.wal`, ni un segmento sellado, ni su copia cold tienen huecos u operaciones fuera del rango que indican su nombre o `active_start_seq`. | DEF-167 |
| INV-S7 | **El piso de secuencia del storage vive fuera del snapshot.** Hoy, si se pierde el `.snap` después de una compactación que vació el WAL, la sala abre vacía en la secuencia 0 y vuelve a aceptar la 1 (`recovery.rs:225-229`; evidencia en `s.patch`). | Auditor s |
| INV-R8 | **El relay solo descarta un snapshot activo** si no es usable, si venció o si su sala se borró o se recreó. INV-R4 supone que un snapshot usable sobrevive. | Gemelo de DEF-176 |
| INV-R9 | **Ningún miembro deja al worker (admin secret) sin una vía para subir un snapshot aceptable.** Hoy solo lo dice ARCHITECTURE §6. El tope de 2 subidas en un solo request por sala lo comparte el worker, y desde DEF-162 cuenta también las de un chunk. | Auditor ya |

**Decisiones para el dueño.** En cada una, se acepta el riesgo o se rechaza.
1. **DEF-171.** ¿Las entradas de directorio son confiables (§2)? Si lo son, D9 y W2 se sostienen con el ajuste de enunciado de §5. Si no, el fix es el que se describe en §4.
2. **DEF-173.** Apagar un runtime con tareas del storage en vuelo y reabrir la sala en el mismo proceso, ¿entra en el modelo? Si no entra, va como riesgo aceptado en §4, con una nota en S1, S3 y L2.
3. **DEF-179.** Elegir la opción (A) o la (B).
4. **D78 frente al roster y el relay**, y su equivalente en la cola de `.wal.compacting` del storage (§5).

## 7. Backlog Bajo

Hallazgos que no rompen ninguna invariante (`INVARIANTES.md` §1.2).

**Servidor: log y metadatos**
- Las copias warm que no se pudieron borrar (`undeleted_warm`) no cuentan en `disk_bytes` (`tiered_log.rs:453`, `:512-519`).
- Las lápidas `.deleted-*` cuyo borrado falla se acumulan hasta el próximo arranque (`manager.rs:195`).
- Los sidecars `.ids` huérfanos nunca se borran ni cuentan en la cuota (`segment_index.rs:97-121`).
- `LogMeta` y `RoomMetadata` no tienen `deny_unknown_fields`, y `meta_room.json` no contrasta su `room_id` con el directorio (`tiered_log.rs:41`, `manager.rs:52`, `:757`).
- `recover_snapshots` sirve snapshots sin sincronizar `snapshots/` (`relay/recovery.rs:39-131`). Solo pasa si en el promote fallan el sync de directorio y además el borrado del archivo final.
- Una poda que falla después de fijar su piso deja `tail_seq` por debajo del piso registrado. Si después hay un crash, la apertura termina la poda, y un cliente que se registró en ese rango recibe `BehindCompaction` (`tiered_log.rs:571-578`).

**Servidor: relay, subidas y ciclo de vida**
- `get_or_spawn(room, Some(schema))` crea una sala sin purgar el relay (`manager.rs:349-380`). Es API pública, aunque hoy solo la usan los tests.
- `create_room` decide si la sala existe solo por `meta_room.json` (`manager.rs:524`). Una sala que perdió ese archivo se adopta con una encarnación nueva y hereda el log y el roster.
- `drop_stale` borra en el future de la request (`relay/download.rs:106-118`). Es el mismo patrón que DEF-175, pero inocuo.
- Entre dos actores de una sala no hay piso publicado (`manager.rs:435`, `room.rs:193`): una subida que se instala durante un respawn por fallo se compara solo con el rango que leyó.
- `ARCHITECTURE.md:245` dice "an upload that was under way when the room was deleted fails with RoomNotFound". Pero una subida que tiene el lock del slot cuando arranca la purga se confirma y se purga enseguida.
- `announced_snapshot` compara solo la secuencia (`room.rs:869`): un snapshot nuevo con la misma secuencia no se anuncia por `snapshot_available`.
- `ARCHITECTURE.md:73`: un reintento que llega después de que se podó su segmento no "se aplica de nuevo". En memoria se reconoce (DEF-179).

**Storage**
- `open_room` retiene el write lock del mapa de salas durante toda la recuperación (`disk/mod.rs:405`, `:414-470`), y mientras tanto esperan las operaciones de todas las demás salas.
- El `fsync` de un `apply_batch` soltado sigue corriendo con su propio handle, y `close_room` no lo espera. Una apertura inmediata recibe un `RoomLocked` engañoso.
- `create_dir_all_synced` no reintenta un sync fallido, y D77 solo sincroniza `data_dir` y su padre. Un fallo dos niveles más arriba no se vuelve a intentar nunca (`compactor.rs:580`, `disk/mod.rs:423`). Evidencia en `c-storage.patch`.
- `replace_with_absorbed_copy` copia `.wal.compacting` sin revisar su cola. Si su último frame se dañó, el daño queda en el medio y la apertura siguiente falla por corrupción: falla cerrada, con disparador (c).
- `recovery.rs:232`, `:339` y `compactor.rs:236` abren archivos con `std::fs` en un worker de Tokio (GEMINI §8).
- El trait no documenta que soltar `apply_snapshot`, `open_room` o `compact_room` no los cancela (`engine.rs:104-127`). Además, los `JoinError` convierten un pánico en `StorageError::Other` (`disk/mod.rs:362`, `:472`, `:745`).

**Tests (red débil)**
- Mutaciones que ningún test detecta (§1): leer el chunk antes de reservar; sacar solo un `fsync` en el servidor o en storage; y las cuatro que el plan ya admitía.
- Los barridos de cancelación de `apply_snapshot`, `open_room` y `compact_room` son `#[ignore]`, aunque tardan menos de 1 s (`storage/src/tests/disk.rs:884`, `:907`, `:930`). Son la red de D86 y D89.
- `reopening_loads_into_ram_only_what_fits` mira la HotBuffer después del recorte, no el pico de la apertura (DEF-170).
- `onboarding_sim` llama al relay con rangos calculados en una sola tarea, así que no ve órdenes como el de DEF-176.
- No había tests de `write_checked` y `verified_json`; los agrega `m.patch`.

## 8. Tests de la auditoría

Están en [`2026-10-invariantes-audit-2/`](2026-10-invariantes-audit-2/README.md), un parche por auditor contra `b4cb390`. Cada uno trae sus tests de ruptura, que fallan en `b4cb390`, y los tests de evidencia, que pasan. Además de los tests de ruptura, la ronda dejó redes que conviene traer al repo con el fix de su grupo:
- la simulación de fallas aleatorias y reaperturas del log (`audit_d1_d2_d8_*`, `d.patch`);
- la simulación de operaciones soltadas y carreras del storage, con pánicos dentro de las tareas (`audit_s6_*`, `s.patch`);
- los tests de cada bit de los cuatro JSON con checksum (`audit_d9_*`, `m.patch`);
- los bordes de `iter_from` y el sync sin presupuesto (`audit_y2_*` y `audit_y6_*`, `delta.patch`).
