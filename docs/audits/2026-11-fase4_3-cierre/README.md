# Tests de la revisión de cierre de 4.3

Son los tests que cada revisor de la revisión de cierre de la sub-fase 4.3 (Lote 11 del [plan de 4.3](../../proposals/2026-11-fase4_3-transporte-worker.md)) escribió en su copia del repo, guardados como parches contra `ea1cafe`. Las copias vivían en un directorio temporal.

**El resultado:** la revisión no salió en verde.
- **Cinco tests fallan en `ea1cafe`:**
  - cuatro rompen invariantes; los arreglan los Lotes 11b y 11c de la sub-fase, que traen esos tests al repo;
  - el quinto fija un Bajo de documentos, corregido en el Lote 11.
- **Los demás pasan,** y son la evidencia de lo que se sostiene, o de un Bajo.

El veredicto por invariante y los hallazgos están en la nota **Hecho** del Lote 11.

Para usarlos, en una copia en `ea1cafe`:

```bash
git apply docs/audits/2026-11-fase4_3-cierre/<parche>.patch
```

Antes de traer un test al repo:
- tiene que salir del formato de la revisión: usa sus nombres (`review_a_*`, `review_b_*`), sus docstrings en castellano con códigos de invariantes, y no sigue el de `cargo fmt`;
- hay que renombrarlo, formatearlo y sacarle cualquier código de decisión o de invariante (`GEMINI.md`).

| Parche | Revisor | Test | En `ea1cafe` |
|---|---|---|---|
| `worker.patch` | Worker y réplica (K1, K4, K5, K6, K7, K11, K17) | `review_a_an_events_retry_beyond_the_clock_leaves_a_live_worker_running_as_in_periodic` (`crates/client/tests/review_a_worker.rs`): con `events_retry: Duration::MAX` y el SSE del servidor apagado, el worker en `live` sigue en `periodic` | **Falla** (K17, K12 ★, K7): pánico en la tarea del worker. Lo arregla el Lote 11c |
| | | `review_a_a_refusal_events_disabled_with_events_retry_beyond_the_clock_does_not_stop_the_worker`: lo mismo con un `EventsDisabled` que escribe la red | **Falla**, por lo mismo |
| `transporte.patch` | Transporte y robustez (K2, K3, K12, K15) | `review_b_an_ops_event_that_does_not_validate_leaves_no_head_seen_for_the_writes_after_it` (`crates/client/tests/review_b_transporte.rs`): un `ops` con una operación que no valida deja su head como head visto, y una escritura que entra después termina `OutcomeUnknown` con su secuencia aplicada | **Falla** (K2 ★). Lo arregla el Lote 11b |
| | | `review_b_an_events_retry_of_duration_max_does_not_stop_the_worker_of_a_room` | **Falla** (K12 ★, K17), el mismo hallazgo que los de `worker.patch`. Lo arregla el Lote 11c |
| | | `review_b_a_response_without_a_length_reserves_at_most_a_frame`: sin `Content-Length`, el búfer de una respuesta de un frame crece al doble | **Falla**: reserva unos 32 MiB. Fija un Bajo de documentos (el texto de K12), corregido en el Lote 11 |
| | | `review_b_a_forged_ops_of_another_incarnation_keeps_a_sound_replica`: un `ops` falsificado de otra encarnación registra la réplica con su cursor, sin descartarla | Pasa |
| | | `review_b_a_tls_handshake_the_server_never_answers_is_cut_by_the_connect_timeout`: el plazo de conexión corta un handshake de TLS que el servidor no contesta | Pasa |
| | | `review_b_random_and_damaged_streams_with_other_seeds` (`crates/client/src/tests/events.rs`, `#[ignore]`): el fuzzing del parser con 40 semillas de 2000 corridas | Pasa |
| | | `review_b_streams_over_http_with_other_seeds` (`#[ignore]`): el fuzzing por HTTP con 20 semillas de 500 | Pasa |
