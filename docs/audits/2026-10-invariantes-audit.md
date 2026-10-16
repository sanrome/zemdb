# Auditoría por Invariantes (1ª ronda)

**Fecha:** 15 de Octubre de 2026
**Commit auditado:** `50fe195` (`main`), después del plan de la [Fase 3.8](../proposals/2026-10-fase3_8-remediation-plan.md)
**Contrato:** [`docs/INVARIANTES.md`](../INVARIANTES.md) (40 invariantes, 18 núcleo ★)
**Auditoría anterior:** [`2026-10-post-fase3_7-audit.md`](2026-10-post-fase3_7-audit.md) (DEF-115 a DEF-143)

---

## 1. Metodología

Es la primera auditoría hecha con el método de `INVARIANTES.md` §1, en vez de búsquedas abiertas por perspectiva:

- **9 auditores en paralelo**, en modo solo lectura y cada uno sobre su propia copia (`git archive`):
  - **Revisión del delta** de la Fase 3.8 (`da796da..6485f46`): si cada fix cierra su escenario y si deshacerlo hace fallar algún test.
  - **Un auditor por grupo de invariantes**: §3.1 a §3.7, más §3.8 y §3.9 juntos.
- **Veredicto obligatorio por invariante:** se sostiene (con el argumento y los caminos que la cubren), se rompe (con un test que lo reproduzca) o indeterminada.
- **Origen de cada ruptura:**
  - (a) la introdujo la ronda;
  - (b) preexistente;
  - (c) solo se dispara con un disparador raro (hardware o SO fallando, o una secuencia hostil improbable).
- **Consolidación:** 4 rupturas las encontraron 2 o 3 auditores por separado (DEF-144, 145, 146, 147). El coordinador verificó en el código las 4 rupturas que más se repetían.
- **Más allá de leer código:** además de leerlo, los auditores construyeron redes de prueba.
  - **Simulaciones con oráculo:**
    - entrega exacta desde cualquier cursor;
    - convergencia entre motores;
    - 24 corridas de *liveness* del onboarding;
    - 2.722 imágenes de crash del storage;
    - 2.259 errores inyectados.
  - **Fuzzing:** unos 28,8 M de ejecuciones sobre 12 decoders.
  - **Proptests de roundtrip** de todos los formatos.
  - **Mediciones de memoria** con un asignador que cuenta bytes.

  Ver §6.

### Estado del build en `50fe195`

| Chequeo | Resultado |
|---|---|
| `cargo fmt --all -- --check` | Sin diferencias |
| `cargo clippy --workspace --all-targets -- -D warnings` | Sin warnings |
| `cargo test --workspace` | 715 en verde, 0 fallos |

### Revisión del delta de la Fase 3.8

- **Los 29 ítems (DEF-115 a DEF-143) cierran el escenario de la auditoría.** Todos los tests de "Hecho" existen y pasan.
- **Ningún fix quedó sin protección:** las 31 mutaciones que deshacen fixes, entre ellas todas las de los ítems Alto y Medio, hacen fallar al menos un test.
- **La ronda introdujo 2 rupturas:**
  - DEF-145, en la parte de `log_meta.json`: el fix de DEF-120 borra segmentos según un archivo sin checksum.
  - DEF-147: D62 dejó afuera al chunk 0.

---

## 2. Resultado por invariante

**22 se sostienen, 18 se rompen, 0 indeterminadas** (INV-B1 tiene además una parte indeterminada, ver DEF-152).

| Grupo | Invariante | Veredicto | Ruptura |
|---|---|---|---|
| §3.1 | INV-D1 ★ Commit confirmado sobrevive | ❌ | DEF-144 |
| | INV-D2 ★ Seq nunca reutilizada | ❌ | DEF-144, DEF-148 |
| | INV-D3 ★ Recuperación no descarta datos | ❌ (los frames se sostienen) | DEF-145 |
| | INV-D4 Metadatos atómicos | ✅ | — |
| | INV-D5 ★ Un solo dueño | ❌ (entre procesos) | DEF-149 |
| §3.2 | INV-S1 ★ `apply_batch` durable o sin efecto | ❌ (solo por la carrera de DEF-150) | DEF-150 |
| | INV-S2 Lectores ven un prefijo | ✅ | — |
| | INV-S3 ★ Crash se recupera al mismo estado | ❌ (*liveness*, falla cerrada) | DEF-151 |
| | INV-S4 ★ Abrir no descarta datos | ✅ | — |
| | INV-S5 `apply_snapshot` todo o nada | ✅ | — |
| §3.3 | INV-Y1 ★ Orden total contiguo | ✅ | — |
| | INV-Y2 ★ Entrega exacta | ✅ | — |
| | INV-Y3 ★ Quedarse atrás se señala | ✅ (precisar enunciado) | — |
| | INV-Y4 Idempotencia en la ventana | ❌ | DEF-146 |
| | INV-Y5 Commit rechazado sin rastro | ✅ (precisar enunciado) | — |
| | INV-Y6 Cursores | ✅ (precisar enunciado) | — |
| | INV-Y7 Encarnación | ✅ (alcance a decidir) | — |
| | INV-Y8 ★ Convergencia | ✅ (estado lógico) | — |
| §3.4 | INV-R1 ★ Poda no borra lo necesario | ✅ | — |
| | INV-R2 Rango contiguo | ✅ | — |
| | INV-R3 Roster volátil | ✅ | — |
| | INV-R4 Todo atrasado se pone al día | ✅ | — |
| | INV-R5 Solo snapshots usables | ❌ | DEF-147 |
| §3.5 | INV-A1 ★ Nadie actúa sobre otra sala o cliente | ✅ | — |
| | INV-A2 Nada se decodifica antes de autenticar | ✅ | — |
| | INV-A3 Efectos confinados | ❌ | DEF-155 |
| | INV-A4 ★ Rutas dentro de `data_dir` | ✅ | — |
| §3.6 | INV-B1 ★ Memoria por request | ❌ | DEF-152 |
| | INV-B2 Nada sin plazo | ❌ | DEF-153 |
| | INV-B3 Memoria, disco y FDs acotados | ❌ (disco) | DEF-154 |
| | INV-B4 Costo por request | ❌ | DEF-157 |
| §3.7 | INV-L1 Toda request tiene respuesta | ✅ (reformular la cota) | — |
| | INV-L2 Cancelar no deja estado a medias | ❌ | DEF-150, DEF-156 |
| | INV-L3 ★ I/O incierto nunca es éxito | ❌ | DEF-144 |
| | INV-L4 Reabrir no cambia lo confirmado | ❌ | DEF-146, DEF-158 |
| | INV-L5 Apagado no pierde lo confirmado | ✅ | — |
| §3.8–3.9 | INV-E1 ★ El esquema solo crece | ❌ (solo por corrupción de disco) | DEF-145 |
| | INV-E2 Esquema viejo se entera | ✅ | — |
| | INV-W1 ★ Ninguna entrada hace pánico | ✅ | — |
| | INV-W2 Roundtrip y checksums | ❌ (checksums; el roundtrip se sostiene) | DEF-145 |

### Criterio de corte (§1.4): **no está en verde**

**Se rompen 9 de las 18 invariantes ★:** D1, D2, D3, D5, S1, S3, B1, L3 y E1. Sin embargo, el patrón es muy distinto al de las auditorías anteriores:

1. **Siete de esas 9 tienen un disparador (c):** un `fsync` que falla seguido de un corte de energía, un bit invertido en reposo, un segmento que desaparece o una carrera de cancelación estrecha. Las dos que no lo tienen:
   - D5 necesita un error de despliegue (dos procesos sobre el mismo `data_dir`).
   - B1 necesita un esquema ancho, que es legal.
2. **Tres causas raíz explican 10 de los 15 defectos:**
   - **Confiar en lo que hay en disco sin verificarlo:** DEF-144, 145, 148 y 158.
   - **Contar recursos en bytes de wire, o no contarlos:** DEF-152, 153, 154 y 157.
   - **Carreras de cancelación con E/S en vuelo:** DEF-150 y 156.
3. **Las garantías centrales de sincronización se sostienen con simulación y oráculo:** orden, entrega exacta, señalización del atraso, convergencia, retención y *liveness* del onboarding. También aislamiento, decodificación hostil y rutas.

---

## 3. Resumen de defectos

Se continúa la numeración: **DEF-144 a DEF-158**.

| ID | Sev. | Origen | Invariantes | Resumen |
|---|---|---|---|---|
| DEF-144 | Alto | (c) | D1★, D2★, L3★ | Reabrir después de un `fsync` fallido confirma datos y entradas de directorio que ningún `fsync` cubrió. |
| DEF-145 | Alto | (a) + (b), disparador (c) | D3★, E1★, W2 | Ningún JSON persistido tiene checksum: un bit invertido en `log_meta.json` borra segmentos retenidos al abrir, y en un esquema renumera una tabla. |
| DEF-146 | Medio | (b) | Y4, L4 | El dedup refresca la recencia al consultar: el reintento de una mutación dentro de la ventana se secuencia otra vez. |
| DEF-147 | Medio | (a) | R5 | El chunk 0 sirve un snapshot que la sala ya marcó como por debajo de un hueco. |
| DEF-148 | Alto | (b), disparador (c) | D2★ | Si se pierde el segmento sellado más nuevo, `head_seq` retrocede y se reutilizan secuencias. |
| DEF-149 | Medio | (b) | D5★ | Un segundo proceso sobre el mismo `data_dir` puede borrar y recrear una sala que posee el primero. |
| DEF-150 | Medio | (c) | S1★, L2 | Storage: el `rename` de un `apply_snapshot` cancelado aterriza después de reabrir y hace perder lotes confirmados. |
| DEF-151 | Medio | (c) | S3★ | Storage: un crash durante la absorción en `.wal.compacting` deja una sala que no reabre (falla cerrada). |
| DEF-152 | Alto | (b) | B1★ | Una respuesta materializa hasta ~32× su tamaño de wire y la HotBuffer ~24× `ram_max_bytes`; `Schema` y `Registered` no toman presupuesto. |
| DEF-153 | Medio | (b) | B2 | Reenviar un chunk ya recibido renueva el plazo de una subida: subida, designación y ancla quedan retenidas sin plazo. |
| DEF-154 | Medio | (b) + (a) | B3 | El disco de una sala no lo acota la cuota: solo poda cold, la compresión está topada y los sidecars no cuentan. |
| DEF-155 | Bajo | (b) | A3 | Una subida en un solo request de otro miembro, con la misma seq, descarta una subida por chunks ajena. |
| DEF-156 | Bajo | (c) | L2 | El apagado ordenado corta a la mitad un `DELETE` que corre en su propia tarea. |
| DEF-157 | Bajo | (b) | B4 | Abrir una sala cuesta O(segmentos retenidos); `fetch_from_disk` recorre el índice desde el inicio. |
| DEF-158 | Bajo | (c) | L4 | Un segmento dañado sin sidecar corta la hidratación del dedup aunque los más viejos tengan sidecars válidos. |

---

## 4. Detalle de los defectos

### Confiar en el disco sin verificarlo

#### DEF-144 · Alto · (c) · Reabrir confirma lo que ningún `fsync` cubrió
- **Lo encontraron:** §3.1 y §3.7 por separado; el coordinador lo verificó en el código.
- **Dónde:**
  - `crates/server/src/log/warm_disk.rs:257-260`: `inspect_active_segment` solo hace `sync_all` cuando trunca.
  - `crates/server/src/actor/room.rs:510-522`: la rama de dedup confirma sin E/S.
  - `crates/server/src/log/warm_disk.rs:78-90` y `crates/server/src/durable.rs:34-37`: el directorio solo se sincroniza cuando el propio llamado crea la entrada.
  - En storage, `create_dir_all_synced` documenta que un sync fallido no se reintenta (`compactor.rs:555`).
- **Escenario:**
  1. El `sync_data` de un commit falla, o el proceso cae dentro de él.
  2. La sala se reabre y lee el registro desde el page cache.
  3. El reintento con el mismo `MutationId` recibe `CommitAck`.
  4. Un corte de energía posterior pierde el registro, y la secuencia se asigna a otra operación.

  Lo mismo pasa con las entradas de directorio de `active.wal`, de `rooms/<id>` y del `data_dir` de storage cuyo sync falló: los intentos siguientes adoptan el archivo y confirman sin volver a sincronizar el directorio.
- **Tests (copias de auditoría):**
  - `audit_d1_retry_after_failed_fsync_is_acknowledged_without_being_durable`: el commit con seq 2 queda confirmado y después del corte la seq 2 se reasigna.
  - Los tres tests de entradas de directorio de §3.7.
  - `failed_commit_sync_stops_room_and_recovers_from_disk` (`room.rs:322`), del repo, da por correcto este comportamiento.
- **Solución:**
  - **Servidor:** al abrir un log, antes de servir o deduplicar, hacer `sync_all` de `active.wal` y `sync_dir` de cada directorio del camino (`segments/`, el de la sala, `rooms/`).
  - **Storage:** lo mismo en `open_room`, para el WAL y `data_dir`.
  - **Salvedad de Linux:** después de un EIO, un `fsync` desde un fd nuevo puede reportar éxito. Lo robusto es reescribir `active.wal` (copia, `fsync`, rename) cuando la apertura sigue a un fallo conocido. Con el modelo de §2, alcanza con el `fsync` al abrir.
- **Relacionado:** el patrón *fsyncgate* de PostgreSQL. ARCHITECTURE §5.2 dice "If the record did reach disk, deduplication recognizes it", pero desde el código no se puede saber si llegó.

#### DEF-145 · Alto · (a) + (b), disparador (c) · Ningún JSON persistido tiene checksum
- **Lo encontraron:** el delta y §3.8–3.9 por separado; el coordinador lo verificó en el código.
- **Dónde:**
  - `crates/server/src/log/tiered_log.rs:843-862`: `read_pruned_through_seq` solo rechaza `u64::MAX`.
  - `crates/server/src/log/segment_index.rs:101-110`: `scan` borra todo segmento con `end <= pruned_through_seq`. El borrado lo agregó `918289e` (Lote 5, fix de DEF-120), y antes de eso un valor dañado no borraba nada.
  - El resto de los JSON (`schemas/{id}.json`, `meta_room.json`, el roster) tampoco tienen checksum.
- **Escenarios (reproducidos):**
  - `{"pruned_through_seq":10}` pasa a `30` invirtiendo un bit, y la apertura borra los segmentos 11..=30 sin error (`tail_seq=31`). En `e6d8254`, el mismo test no borraba nada.
  - Un bit invertido en el `table_id` de `schemas/todo.json` hace que el registry reabra con la tabla `notes` como id 3 en vez de 1, sin error. Las operaciones del log sobre la tabla 1 pasan a ser `TableNotFound`.
- **Solución:**
  - Un checksum (CRC32 o BLAKE3) en todo JSON persistido que decide algo, verificado antes de usarlo; si no verifica, el archivo cuenta como ilegible.
  - La apertura no borra nada a partir de datos no verificados.
  - Ajustar `log_claiming_the_last_sequence_number_fails_to_open` (`log/tests/tiered_log.rs:270-276`), que da por buena la conducta actual.

#### DEF-148 · Alto · (b), disparador (c) · Perder el segmento sellado más nuevo reutiliza secuencias
- **Dónde:** `crates/server/src/log/tiered_log.rs:162-170`. Justo después de una rotación no existe `active.wal`, y `head_seq` sale de los nombres de segmento que quedan.
- **Escenario (reproducido):** se pierde el segmento sellado más nuevo y `head_seq` cae de 10 a 5. Las secuencias 6 a 10 se asignan de nuevo. Un cliente con cursor en 10 recibe `InvalidSequence` hasta que el head lo pase, y después se saltea las nuevas 6 a 10 sin enterarse. Pasa lo mismo si se pierde `log_meta.json` después de una poda total.
- **Test:** `audit_d2_lost_newest_segment_reuses_sequence_numbers`.
- **Solución:** que el piso de `head_seq` sea el máximo de los nombres de sidecars, la seq del snapshot activo y los cursores del roster. O, más simple, escribir el fin de cada segmento sellado en `log_meta.json` al rotar (con el checksum de DEF-145).
- **Para el documento:** INV-D3 obliga a truncar un último frame dañado de `active.wal`, porque es indistinguible de una escritura rota, y eso reutiliza su secuencia. Hay que decidir si INV-D2 excluye ese caso (ver §5).

#### DEF-149 · Medio · (b) · Un segundo proceso puede borrar una sala ajena
- **Dónde:** `crates/server/src/actor/manager.rs:530-545`. `delete_room` hace `remove_dir_all` sin tomar `log.lock`, y no existe un lock de `data_dir`.
- **Escenario (reproducido):** dos procesos servidor sobre el mismo `data_dir`, por ejemplo durante un deploy que se solapa. El segundo no puede abrir la sala, pero puede borrarla y recrearla. Termina con dos actores vivos sobre la misma sala.
- **Test:** `audit_d5_second_process_deletes_a_room_owned_by_the_first`.
- **Solución:** un lock exclusivo de `data_dir` al arrancar, antes de la recuperación del relay y del registry. Además, que `delete_room` tome `log.lock` antes de borrar.

#### DEF-158 · Bajo · (c) · Un segmento dañado sin sidecar corta la hidratación del dedup
- **Dónde:** `crates/server/src/log/tiered_log.rs:725-731`. El test `open_tolerates_a_corrupt_newest_segment` da por bueno el comportamiento actual.
- **Qué pasa:** la lectura hacia atrás se detiene en el segmento dañado, aunque los más viejos tengan sidecars intactos. Esa razón (la contigüidad de la RAM) desapareció con D57.
- **Solución:** saltar el segmento dañado y seguir con los sidecars de los anteriores.

### Idempotencia y onboarding

#### DEF-146 · Medio · (b) · El dedup refresca la recencia al consultar
- **Lo encontraron:** §3.3 y §3.7 por separado; el coordinador lo verificó en el código.
- **Dónde:** `crates/server/src/dedup.rs:41-43`. `is_duplicate` usa `cache.get`, que promueve la entrada. La hidratación, en cambio, carga las últimas N claves por orden del log (`tiered_log.rs:757-770`), y §5.1 promete "the last `dedup_lru_capacity` mutations".
- **Escenario (reproducido, `dedup_lru_capacity = 2`):** se confirman m1 y m2, se reintenta m1 y se confirma m3. El reintento de m2, que está entre las últimas 2 mutaciones, recibe la seq 4. Eso es una aplicación doble, que puede pisar escrituras posteriores. Después de un respawn, el mismo reintento devuelve la seq original: la ventana depende de si hubo respawn.
- **Tests:** `audit_y4_retry_of_a_mutation_in_the_window_is_not_sequenced_again` y `audit_l4_a_key_in_the_dedup_window_is_dropped_by_the_reopen`.
- **Solución:** `peek` en lugar de `get`. Ajustar `full_cache_evicts_the_least_recently_used_entry`, que fija el comportamiento actual. Comprobado con un mutante.

#### DEF-147 · Medio · (a) · El chunk 0 sirve un snapshot por debajo de un hueco
- **Lo encontraron:** §3.4 y el delta por separado.
- **Dónde:** `crates/server/src/relay/download.rs:51-57` solo comprueba `LogBounds::admits`. La marca `unrestorable_snapshot` vive en el actor (`room.rs:666-669`, `:783-789`), y `GetLogBounds` no la devuelve.
- **Escenario (reproducido):** después de un `BehindCompaction` por hueco, `HeartbeatAck` y `Registered` dejan de ofrecer el snapshot, pero `get_chunk(0, sin hash)` lo sirve. El impacto es acotado: cuesta un ciclo de descarga por cliente, y la demanda ya está encendida.
- **Solución:** que `GetLogBounds` devuelva también la marca (o `usable_snapshot_seq`), y que el chunk 0 lo use.

### Storage: cancelación y absorción

#### DEF-150 · Medio · (c) · Un `rename` en vuelo aterriza después de reabrir
- **Dónde:** `crates/storage/src/disk/compactor.rs:466-473` (`tokio::fs::rename` no retiene ningún handle); `disk/mod.rs:374-392` (el lock de sala se suelta al cerrar).
- **Escenario (reproducido con dos puntos de pausa instrumentados):**
  1. Se suelta un `apply_snapshot` con su `rename` en vuelo en el pool bloqueante.
  2. La sala queda `RoomFailed` y se cierra y reabre, como pide la documentación.
  3. El `rename` aterriza después de que la reapertura cargó el snapshot anterior.
  4. La sala reabierta confirma los lotes 4 y 5.
  5. La siguiente apertura carga el snapshot nuevo (head 5) y saltea esos registros, que se pierden.

  Sin instrumentar, con la máquina cargada, el `rename` todavía no había corrido 50 ms después de soltar el future.
- **Solución:** correr la instalación del snapshot en su propia tarea (como `compact_room`), o hacer que `close_room` y el lock de sala esperen a que terminen las operaciones de archivo en vuelo. Lo mismo para el borrado de `.wal.compacting` en `install_applied_snapshot`.

#### DEF-151 · Medio · (c) · Un crash durante la absorción deja una sala que no reabre
- **Dónde:**
  - `crates/storage/src/disk/compactor.rs:265`, `:305`: el append a `.wal.compacting` se sincroniza una sola vez, al final.
  - `crates/storage/src/disk/recovery.rs:98-122`, `:486`: `highest_seq_in_valid_frames`.
  - `crates/core/src/protocol/wal_frame.rs:269`, `:344`, `:349`: la búsqueda byte a byte y el tope de 16 chequeos.
- **Escenario (reproducido):** un crash con escrituras rotas en la absorción. La cobertura se sobreestima en tres casos, y la apertura da `WalCorruption` aunque el WAL activo tiene todos los registros:
  - un valor `Bytes` que embebe un frame válido, después de un header roto;
  - un frame embebido encontrado dentro de un payload roto;
  - 17 o más payloads rotos seguidos, sin ningún valor hostil: el frame que sigue se toma sin verificar y cuenta como `u64::MAX`.

  No se pierde ni se inventa nada (falla cerrada), pero la réplica local no abre sin intervención. El caso equivalente en el WAL activo está documentado en ARCHITECTURE §8, pero no en los riesgos aceptados.
- **Solución:** hacer atómica la absorción: un `.tmp` con `.wal.compacting` más el WAL activo, `fsync`, rename y sync del directorio, y recién entonces truncar el activo. Así `.wal.compacting` nunca queda roto y desaparecen la excepción y `highest_seq_in_valid_frames`. Como mínimo, saltar `frame_len` en los frames con header válido.

### Recursos

#### DEF-152 · Alto · (b) · La memoria de las respuestas no está acotada por su tamaño de wire
- **Dónde:**
  - `crates/server/src/log/budget.rs:5-7`, `hot_buffer.rs:94-95`, `segment_reader.rs:156-164`, `:195`: el presupuesto de lectura cobra `encoded_len`, en bytes de wire.
  - `crates/server/src/api/data_plane.rs:50-60`: el presupuesto en vuelo se toma después de materializar y codificar.
  - `data_plane.rs:122-132`, `:177`, `:380`: `Schema` y `Registered` no toman presupuesto.
- **Escenario (medido):** un `Null` ocupa 1 byte de wire y 24 en memoria.

  | Esquema | `/sync` de wire | Pico de memoria | HotBuffer residente |
  |---|---|---|---|
  | 65.535 columnas | 15,9 MiB | 526 MiB (16,7 M valores) | 192 MiB con `ram_max_bytes` de 8 MiB |
  | 4.096 columnas | — | 129 MiB | — |
  | 3 columnas | — | Irrelevante | — |

  Además, 64 respuestas `/schema` sin leer dejan 64 MiB vivos con un presupuesto de 16 MiB.
- **Severidad:** Alto, que es la de la invariante. La precondición es un esquema ancho, que es legal (`MAX_COLUMNS`) pero lo define el admin. Si los esquemas de miles de columnas se consideran fuera de uso, puede bajarse a Medio.
- **Indeterminado:** h2 0.4.20 puede retener por stream hasta ~400 KiB de copias fuera del presupuesto (lo que contradice "a couple of frames" en ARCHITECTURE §7.3). Hace falta un cliente h2 a medida para medirlo.
- **Solución:** que el presupuesto de lectura y la HotBuffer cobren `max(wire, 24 × valores)`, o que una respuesta no materialice más de `MAX_MESSAGE_VALUES`. Que `Schema` y `Registered` tomen presupuesto. Pedir el presupuesto antes de codificar.

#### DEF-153 · Medio · (b) · Reenviar un chunk ya recibido renueva el plazo
- **Dónde:** `crates/server/src/relay/upload.rs:252-259`, `:304-308` (refresca `last_chunk_at` aunque el chunk ya estaba), `:107-109`.
- **Escenario (medido con tiempo pausado):** un miembro reenvía el chunk 0 cada 119 s. Después de 24 h y 727 reenvíos, la subida sigue en curso y se retienen sin plazo:
  - la designación del uploader;
  - el ancla de poda;
  - el actor de la sala, que no se apaga.

  Otro miembro con la misma seq recibe `SnapshotSuperseded`.
- **Solución:** que solo un chunk nuevo renueve el plazo, y un tiempo total máximo por subida.

#### DEF-154 · Medio · (b) + (a) · La cuota no acota el disco de una sala
- **Dónde:** `crates/server/src/log/tiered_log.rs:503-509` (la cuota solo poda cold y corta en el primer warm), `:29` (4 compresiones por pasada) y `:604-606` (los sidecars no cuentan).
- **Escenario (medido):**
  - Con cuota de 1 MiB y TTL warm por defecto, una ráfaga de 8 MiB deja 7,8 MiB.
  - Con TTL warm 0 y un escritor sostenido, crece +1,6 MiB por pasada.
  - Los sidecars suman 1,8× la cuota con ids aleatorios.
  - Durante una subida conviven el snapshot activo y la parcial.
- **Origen:** la cuota que solo poda cold y el tope de compresión son (b); los sidecars fuera de la cuota son (a) (D57).
- **A decidir:** el diseño (§5.2 *TTL & Quota Compaction*) nunca prometió acotar el warm, así que en parte es el enunciado de INV-B3 el que promete de más. Opciones: que la cuota cuente warm y sidecars y aplique backpressure (comprimir o podar el warm), o reformular INV-B3 como "cuota + ventana warm + sidecars + 2 snapshots".

#### DEF-157 · Bajo · (b) · Abrir una sala cuesta O(segmentos)
- **Dónde:** `crates/server/src/log/segment_index.rs:80-126` (lista y hace `stat` de cada segmento) y `tiered_log.rs:330-336` (`fetch_from_disk` recorre el índice desde el inicio).
- **Medición:**

  | Segmentos | `SegmentIndex::scan` | Sync a disco |
  |---|---|---|
  | 100 | 0,87 ms | 24 µs |
  | 5×10⁴ | 446 ms | 491 µs |

  Son unos 10 µs por segmento, y con la cuota por defecto se llega a 10⁴–3×10⁴ segmentos.
- **Solución:** `BTreeMap::range` en `fetch_from_disk`. Para la apertura, aceptar O(segmentos) en el enunciado de INV-B4 o persistir el índice.

### Aislamiento y ciclo de vida

#### DEF-155 · Bajo · (b) · Una subida en un solo request descarta una subida por chunks ajena
- **Dónde:** `crates/server/src/relay/upload.rs:148-198` (`stage_snapshot` no mira la subida en curso) y `relay/mod.rs:134-138` (`install` borra toda subida con seq menor o igual).
- **Escenario (reproducido):** Alice sube por chunks en la seq S, y Mallory hace `POST /snapshot/upload` en la misma seq. La subida de Mallory se instala y la de Alice se borra. Pasa lo mismo contra el worker.
- **Severidad:** Bajo, porque subir en S+1 ya da ese poder y eso está aceptado en DEF-81.
- **Solución (prototipo validado en la copia de §3.5):** pasar el uploader a `stage_snapshot` y responder `SnapshotSuperseded` si hay una subida con la misma seq que no le cede el lugar.

#### DEF-156 · Bajo · (c) · El apagado corta un `DELETE` a la mitad
- **Dónde:** `crates/server/src/api/serve.rs:95-99` y `crates/server/src/main.rs:50-60`. `serve_until_shutdown` solo espera las conexiones, y al soltarse el runtime se cancela la tarea del borrado.
- **Escenario (reproducido, determinista):** el cliente de un `DELETE` se desconecta durante el purge y el servidor se apaga. La sala queda viva sin su snapshot. Es un residuo de DEF-125.
- **Solución:** un `TaskTracker` para las tareas de varios pasos (borrado, relay, recarga de esquema), esperado dentro de la gracia del apagado. Mejor todavía, el borrado atómico con lápida (ver la propuesta P7).

---

## 5. Ajustes al documento de invariantes

Varios veredictos "se sostiene" vienen con la observación de que el enunciado promete más que el contrato real (ARCHITECTURE). No son defectos del código; hay que corregir `INVARIANTES.md`.

| Invariante | Ajuste propuesto |
|---|---|
| INV-D2 / INV-D3 | Se contradicen: INV-D3 obliga a truncar un último frame dañado de `active.wal` (indistinguible de una escritura rota), y eso reutiliza su secuencia. Opciones: excluir ese caso de INV-D2, o sumar un piso de secuencia fuera del log (P3). |
| INV-Y3 | Precisar que el registro, un reintento deduplicado o un ack con un hueco posterior no reciben `BehindCompaction`, pero sí una respuesta que obliga a un sync que lo recibe (`tail_seq`, `has_more`, `head_seq > cursor`). |
| INV-Y5 | Excluir `snapshot_wanted`: un commit rechazado por `BehindCompaction` pide un snapshot. Corregir también ARCHITECTURE §7.2 *Ordering*, que dice "no trace in the SSE stream". |
| INV-Y6 | "Entre dos registros, el cursor nunca retrocede": un registro lo redefine al `current_seq` reportado. |
| INV-Y7 | Decidir: extender la garantía a sync, ack y commit (enviar `known_incarnation`, cambio de protocolo), o limitarla al registro y dejar la obligación al SDK. |
| INV-Y8 | Definir "mismo estado" como filas completadas con `Null` hasta el ancho del esquema, o hacer que un `Update` ensanche solo hasta `idx + 1` (hoy el ancho físico depende del esquema con que se aplicó). |
| INV-R3 | "~1 s" vale para cursores, registros y estados, no para `last_seen`. |
| INV-R4 | "…un `Connected` cuyo estado esté más allá de todo hueco". |
| INV-R5 | "Nunca retrocede mientras está activo": tras un vencimiento o un archivo ilegible al arrancar se puede activar uno menor. |
| INV-S4, INV-S5 | Los archivos sin datos (lock, `.wal` y `.snap` vacíos, `.snap.tmp.*`) se tocan antes del chequeo; en S5, un I/O que falla después del rename deja `RoomFailed`, y al reabrir aparece el snapshot. Las cotas de memoria son "una constante + O(bytes)". |
| INV-B3 | Ver DEF-154. FDs: "conexiones + 2 por sala + transitorios acotados". |
| INV-B4 | Aclarar que O(clientes) en memoria está permitido, y la cota de la apertura. |
| INV-L1 | La cota real es `ACTOR_TIMEOUT` + `ACTOR_EXIT_WAIT` (×3 en la suscripción SSE), más la apertura de la sala y la E/S del relay. |

## 6. Propuestas de invariantes nuevas y de riesgos aceptados

**Invariantes propuestas** (agregarlas es una decisión del dueño, §1.2):

| ID | Propuesta | Origen |
|---|---|---|
| P1 | **Exclusividad del `data_dir`:** un segundo proceso falla al arrancar antes de tocar nada. | DEF-149 |
| P2 | **Nada se vuelve visible antes de ser durable en este proceso:** lo leído al reabrir se sirve o confirma solo después de un `fsync` exitoso propio. | DEF-144 |
| P3 | **Piso de secuencia fuera del log:** `head_seq` nunca baja de una secuencia que conste como asignada en cualquier archivo durable. | DEF-148 |
| P4 | **Metadatos verificados:** todo JSON que decide algo tiene checksum; ningún borrado al abrir se basa en datos no verificados. | DEF-145 |
| P5 | **La ventana de dedup es función del log:** son las últimas N claves distintas, iguales en memoria y al reabrir. | DEF-146 |
| P6 | **Ningún cliente acapara un recurso global:** el presupuesto de respuestas en vuelo tiene cuota por cliente o por sala. Hoy un miembro de una sala puede dejar sin `/sync` a todas las demás. | §3.5 |
| P7 | **El borrado de una sala es atómico frente a crashes y apagados:** la sala existe entera o no existe. Hoy un `remove_dir_all` parcial hace que la sala recreada herede el log viejo. | §3.7 |
| P8 | **El contenido de un valor nunca cambia cómo se clasifica un daño del log.** | DEF-151 |
| P9 | **Memoria igual a disco:** sin `RoomFailed`, el estado visible es el que daría reabrir. Las simulaciones de §3.2 ya lo chequean. | §3.2 |
| P10 | **Un snapshot solo se ofrece a la encarnación que lo aceptó.** | §3.4 |
| P11 | **Una subida confirmada es usable en ese momento,** o se responde `SnapshotSuperseded` (hoy solo vale para subidas de varios chunks). | §3.4 |
| P12 | **Progreso de subidas:** solo un chunk nuevo renueva el plazo, con un tope total. | DEF-153 |
| P13 | **Materialización acotada:** ninguna respuesta materializa más de `MAX_MESSAGE_VALUES` valores. | DEF-152 |

**Riesgos a aceptar o rechazar explícitamente en §4:**
- **Un stream SSE sigue recibiendo eventos después de que vence su token,** y mantiene la sala abierta.
- **Un miembro puede congelar la designación del uploader** con una subida en seq = head que nunca completa. DEF-153 lo acota en tiempo, pero no lo elimina.
- **Un valor escrito por un miembro que embebe un frame válido** puede convertir una escritura rota en corrupción. Ya está documentado en ARCHITECTURE §8, pero no figura en §4.

## 7. Redes de prueba construidas por los auditores

Están en las copias de auditoría, fuera del repo, en `scratchpad/inv/<grupo>/repo`. Son candidatas a sumarse al repo como tests de propiedades (`INVARIANTES.md` §5):

| Red | Copia | Qué verifica |
|---|---|---|
| Simulación de entrega con oráculo | `inv/y` | INV-Y1 e INV-Y2: cualquier cursor reconstruye su sufijo exacto, con el log cambiando entre páginas. |
| Convergencia entre motores | `inv/y` | INV-Y8: 40 semillas, snapshot en un punto aleatorio, comparación bit a bit. |
| Simulación de onboarding | `inv/r` | INV-R1, R2, R4 y R5: 24 corridas con reinicios, caídas, huecos y subidas fallidas. |
| Imágenes de crash e inyección de errores | `inv/s` | INV-S1, S2, S3 y D3: 2.722 imágenes y 2.259 errores contra `MemoryStorageEngine`. |
| Fuzz de decoders | `inv/ew` (`crates/fuzzw`) | INV-W1: 12 decoders, asignador que cuenta bytes. |
| Proptests de roundtrip | `inv/ew` | INV-W2 e INV-E1. |
| Matriz endpoint × credencial | `inv/a` | INV-A1 e INV-A2: muchas celdas sin test previo. |
| Mediciones de memoria y costo | `inv/b` | INV-B1 a INV-B4. |

Varias tardan minutos en debug (por los `fsync` en macOS): conviene sumarlas con semillas fijas y dejar las largas como `#[ignore]` o en un job aparte.
