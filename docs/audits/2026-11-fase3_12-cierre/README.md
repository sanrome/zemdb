# Tests de la revisión de cierre de la Fase 3.12

Son los tests que cada revisor de la revisión de cierre de la [Fase 3.12](../../proposals/2026-11-fase3_12-rendimiento-servidor.md) (su Lote 8) escribió en su copia del repo, guardados como parches contra `6b93115`. Las copias vivían en un directorio temporal.

**El resultado:** la revisión no salió en verde.
- **Tres tests rompen invariantes** y fallan en `6b93115`. Los arreglan los Lotes 8b (el de la memoria de la búsqueda, ya en el repo) y 8c de la fase, que traen esos tests al repo.
- **Los demás pasan,** y son la evidencia de lo que se sostiene, o de un Bajo.

El veredicto por invariante y los hallazgos están en la nota **Hecho** del Lote 8.

Para usarlos, en una copia en `6b93115`:

```bash
git apply docs/audits/2026-11-fase3_12-cierre/<parche>.patch
```

Antes de traer un test al repo:
- tiene que salir del formato de la revisión: usa sus nombres (`review_a_*`, `review_b_*`, `review_c_*`) y no sigue el de `cargo fmt`;
- hay que renombrarlo, formatearlo y sacarle cualquier código de decisión o de invariante (`GEMINI.md`);
- el de la memoria de la búsqueda mide la memoria residente del proceso (`VmHWM`) y corre solo en Linux.

| Parche | Revisor | Test | En `6b93115` |
|---|---|---|---|
| `durabilidad.patch` | Durabilidad y ciclo de vida (D1, D2, D3, D7, L3, L5, L7, R1, R3) | `review_a_damage_in_a_synced_group_whose_later_group_was_lost_whole_truncates_it` (`log/tests/tiered_log.rs`): un daño del medio en un grupo sincronizado que no es el último, y el grupo siguiente perdido entero; la apertura trunca desde el daño. Fija el Bajo que precisó el texto de "el último grupo" | Pasa |
| | | `review_a_a_room_stopped_by_a_failed_fsync_reopens_only_after_its_stop_wrote_the_roster` (`actor/tests/manager.rs`) | Pasa |
| | | `review_a_shutdown_all_with_one_io_thread_writes_every_roster_changed_within_its_period` (`actor/tests/manager.rs`) | Pasa |
| | | `review_a_a_prune_that_deletes_on_the_slow_tick_writes_the_roster_first` (`actor/tests/room.rs`) | Pasa |
| | | `review_a_a_failed_group_whose_records_are_lost_leaves_nothing_behind` (`actor/tests/room.rs`) | Pasa |
| `entrega.patch` | Entrega, esquema, aislamiento y formatos (Y1, Y2, Y4, Y5, E3, A1, W1, W2) | `review_b_commits_rejected_in_a_group_move_no_cursor_and_free_their_mutation_ids` (`actor/tests/room.rs`) | Pasa |
| | | `review_b_a_commit_behind_the_log_in_a_group_leaves_no_trace` | Pasa |
| | | `review_b_catch_ups_of_a_group_larger_than_the_ram_window_are_the_log` | Pasa |
| | | `review_b_events_across_a_reopen_after_an_evolution_continue_the_log` | Pasa |
| | | `review_b_a_commit_answered_timeout_while_a_group_syncs_is_still_sequenced`: fija el enunciado precisado de INV-Y5 | Pasa |
| `recursos.patch` | Recursos y disponibilidad (B1, B3, B4, B5, L1, L2) | `review_c_the_search_after_a_damaged_group_decodes_no_more_values_than_a_client_message_may` (`crates/core/tests/review_c_group_search_memory.rs`, nuevo): 111 MiB de una vez contra una cota de unos 48 MiB | **Falla** (INV-B1 ★ e INV-W1 ★). Lo trajo el Lote 8b, con otro nombre, una forma de frame más y cada caso en un proceso propio: `the_search_after_a_damaged_group_holds_at_most_a_frame_whatever_the_frames_after_the_damage_hold` (`crates/core/tests/group_search_memory.rs`) |
| | | `review_c_an_opening_that_outlasts_its_request_is_kept_for_the_next_one` (`actor/tests/manager.rs`) | **Falla** (INV-L1; Lote 8c) |
| | | `review_c_an_opening_that_waited_for_a_full_pool_is_kept_for_the_next_request` (`actor/tests/manager.rs`) | **Falla** (INV-L1; Lote 8c) |
| | | `review_c_the_budget_returns_to_zero_when_responses_read_from_disk_end_or_are_cancelled` (`api/tests/data_plane.rs`) | Pasa |
