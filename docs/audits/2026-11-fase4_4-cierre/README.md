# Tests de la revisión de cierre de 4.4

Son los tests que cada revisor de la revisión de cierre de la sub-fase 4.4 (el Lote 11 del [plan de 4.4](../../proposals/2026-11-fase4_4-api-rust.md)) escribió en su copia del repo. Se guardaron como parches contra `ea1cafe`, porque las copias vivían en un directorio temporal.

**El resultado:** la revisión salió en verde. Ningún test rompe una invariante, y todos pasan en `ea1cafe`. El veredicto por invariante, los puntos abiertos que decidió y los Bajos están en la nota **Hecho** del Lote 11.

**Ya están en la suite:** el Lote 11 trajo los nueve tests, a pedido del dueño del proyecto, con otros nombres, formateados y sin códigos de decisión ni de invariante (`GEMINI.md`). El de la ventana de `close_room` cambió con el arreglo de DEF-201: ahora verifica que el cierre detiene el worker. Los dos de las columnas cifradas `Bytes` y del corte de una escritura entraron a `crypto_tests.rs`, con sus helpers. La revisión de delta del lote sumó tres tests más, que no están en estos parches, en `worker_tests.rs`: los de la sala abierta otra vez antes de que `close_room` la mire (también mientras la reapertura de su sala del storage la saca del cliente) y del arranque durante `shutdown`. Los parches quedan como evidencia de lo que corrió la revisión.

Para usarlos, en una copia en `ea1cafe`:

```bash
git apply docs/audits/2026-11-fase4_4-cierre/<parche>.patch
```

| Parche | Revisor | Test de la revisión | En la suite |
|---|---|---|---|
| `escrituras.patch` | Escrituras y cifrado (K2, K8, K9, K12, C3) | `review_a_an_encrypted_bytes_column_goes_out_and_is_stored_only_encrypted` (`tests/review_a_tests.rs`). Una mutación que mandaba en claro un valor `Bytes` sobrevivía a toda la suite | `an_encrypted_bytes_column_goes_out_and_is_stored_only_encrypted` (`tests/crypto_tests.rs`) |
| | | `review_a_a_write_cut_while_it_encrypts_or_prechecks_sends_nothing_and_leaves_nothing` (`tests/review_a_tests.rs`) | `a_write_cut_while_it_encrypts_or_prechecks_sends_nothing_and_leaves_nothing` (`tests/crypto_tests.rs`, sobre el esquema de la liga) |
| | | `review_a_an_insert_planned_before_an_encrypted_column_is_added_goes_without_it` (`src/api/tests/table.rs`): el límite de INV-K9 | `an_insert_planned_before_an_encrypted_column_is_added_goes_without_it` |
| | | `review_a_an_update_or_a_delete_that_does_not_validate_is_schema_validation` (`crates/storage/tests/check_operation_tests.rs`). Una mutación que validaba solo los `insert` sobrevivía | `an_update_or_a_delete_that_does_not_validate_is_schema_validation` |
| `replica.patch` | Réplica y reactividad (K3, K4, K7, K10, K15, K17) | `review_b_in_live_a_write_stays_offline_while_events_bring_operations_until_a_heartbeat` (`tests/facade_worker_tests.rs`): el límite del primer punto abierto | `in_live_a_write_stays_offline_while_events_bring_operations_until_a_heartbeat` |
| | | `review_b_a_worker_started_between_the_stop_and_the_close_of_a_close_sends_nothing` (`tests/worker_tests.rs`): la ventana de DEF-201 | `a_worker_started_between_the_stop_and_the_close_of_a_close_is_stopped_by_it`, con el arreglo |
| | | `review_b_after_a_failed_read_of_a_scan_rows_equal_to_the_last_emitted_are_not_emitted` (`tests/watch_tests.rs`) | `after_a_failed_read_of_a_scan_rows_equal_to_the_last_emitted_are_not_emitted` |
| | | `review_b_discarding_a_room_leaves_the_replica_the_metadata_and_the_watch_of_another` (`tests/discard_tests.rs`) | `discarding_a_room_leaves_the_replica_the_metadata_and_the_watch_of_another` |
| | | `review_b_replicas_that_write_at_once_hold_the_same_rows_of_every_table` (`tests/ranking_app_tests.rs`) | `replicas_that_write_at_once_hold_the_same_rows_of_every_table` |
