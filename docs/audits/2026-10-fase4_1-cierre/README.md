# Tests de la revisión de cierre de 4.1

Son las modificaciones que cada revisor de la revisión de cierre de la sub-fase 4.1 (Lote 10 del [plan de 4.1](../../proposals/2026-10-fase4_1-storage-servidor.md)) hizo en su copia del repo, guardadas como parches contra `977837f`. Se guardaron porque las copias vivían en un directorio temporal. La revisión salió en verde: ningún test rompe una invariante, todos pasan en `977837f`, y cada uno es la evidencia de un Bajo que fue al backlog (DEF-183, DEF-185 y DEF-186, en la nota del Lote 10). El cambio de DEF-184 (que la capa de prueba rechace un segundo handle) no se guardó: lo describe la nota.

Para usarlos, en una copia o worktree en `977837f`:

```bash
git apply docs/audits/2026-10-fase4_1-cierre/<parche>.patch
```

Los tests usan los nombres de la revisión (`review_*`, o el archivo `review_frame_search_oracle.rs`), no siguen el formato de `cargo fmt` y alguno menciona decisiones (`DC17`) en un docstring. Al traer un test al repo hay que renombrarlo, formatearlo y sacarle los códigos (`GEMINI.md`); el oráculo de la búsqueda de frames, además, necesita una versión corta, con la larga en `#[ignore]` (`docs/METODOLOGIA.md` §4, regla 5).

| Parche | Revisor | Tests | Bajo |
|---|---|---|---|
| `core-server.patch` | Core y servidor (Lotes 1 a 3) | `review_concurrent_evolutions_reach_every_room_in_order` (`crates/server/src/actor/tests/manager.rs`): 4 salas abiertas, 24 evoluciones concurrentes y un observador por sala que exige que la versión nunca baje | DEF-186 |
| `storage.patch` | Storage (Lotes 4 a 9) | `frame_search_matches_the_search_of_9b90d1d` (`crates/core/tests/review_frame_search_oracle.rs`, unos 100 s en debug): la búsqueda de frames contra la de `9b90d1d`, copiada tal cual, en 3000 logs aleatorios | DEF-183 |
| | | `review_rotation_that_leaves_a_dead_handle_loses_nothing` y `review_rollback_that_leaves_a_dead_handle_marks_the_room_failed` (`crates/storage/src/disk/tests/opfs_like.rs`, con un flag `fail_reopen` en la capa de prueba): el handle muerto de DC17 | DEF-185 |
