# Tests de la 2ª auditoría por invariantes

Son las modificaciones que cada auditor de la [2ª auditoría por invariantes](../2026-10-invariantes-audit-2.md) hizo en su copia del repo, guardadas como parches contra `b4cb390`. Se guardaron porque las copias vivían en un directorio temporal. Cada ruptura de la auditoría cita el parche donde está su test.

Para usarlos, en una copia o worktree en `b4cb390`:

```bash
git apply docs/audits/2026-10-invariantes-audit-2/<grupo>.patch
```

Los parches son independientes y varios tocan los mismos archivos de tests, así que se aplican de a uno. Solo traen tests, más algunas pausas o fail points de una línea bajo `#[cfg(test)]`. Los tests usan los nombres de la auditoría (`audit_*`, `audit2_*`) y mencionan invariantes (`INV-`), algo que `GEMINI.md` no admite en el repo: al traer un test hay que renombrarlo y sacar esas referencias.

Los tests de ruptura **fallan a propósito** en `b4cb390`: con un parche aplicado, la suite del crate sale en rojo hasta que se arregle la ruptura.

| Parche | Auditor | Fallan en `b4cb390` (rupturas) | Pasan (evidencia) |
|---|---|---|---|
| `d.patch` | §3.1, log del servidor (D1, D2, D3, D7, D8) | DEF-167 (`audit_d1_commit_*`, ×3), DEF-169 (`audit_d7_*`) | `audit_d8_*` (×3), `audit_d1_quota_prune_*`, la simulación `audit_d1_d2_d8_random_failures_*` (la versión larga es `#[ignore]`), `audit_d3_*` (storage) |
| `m.patch` | Metadatos, esquema y formatos (D9, E1, W2) | DEF-171 (`audit_d9_damaged_segment_name_*`, `audit_d9_ttl_prune_*`, `audit_w2_*`, ×3) | `audit_d9_*` (formato con checksum, cada bit de los cuatro JSON), `audit_e1_*` |
| `s.patch` | §3.2, storage (S1, S3, S6) | — | La simulación de operaciones soltadas (`audit_s6_*`), los pánicos en las tareas (`audit_s1_*`, `audit_s3_*`), evidencia de Bajos (`audit_bajo_*`) y de la propuesta de piso del storage (`audit_proposal_*`). Agrega 4 puntos de pánico bajo `cfg(test)` |
| `ya.patch` | Idempotencia y aislamiento (Y4, Y9, A3) | DEF-179 (`audit_y9_*`, ×3), DEF-180 (`audit_a3_a_chunk_*`), DEF-181 (`audit_a3_a_member_failing_*`) | `audit_y4_*`, el resto de `audit_a3_*` |
| `r.patch` | §3.4, snapshots (R5, R6, R7) | DEF-176 (`audit_r5_first_chunk_*`, ×2), DEF-177 (`audit_r7_upload_installed_*`), DEF-178 (`audit_r7_retried_*`) | `audit_r6_*`, `audit_r5_anchored_*`, `audit_r5_replacement_*`. Agrega dos pausas y el módulo nuevo `api/tests/relay.rs` |
| `b.patch` | §3.6, recursos (B1–B5) | DEF-170 (`audit_b1_*`), DEF-174 (`audit_b2_upload_restarted_*`, `audit_b2_designee_restarting_*`) | El control `audit_b2_designee_whose_upload_*` y `audit_b5_*` (módulo nuevo `src/tests/response_budget.rs`) |
| `l.patch` | Dueño y ciclo de vida (D5, D6, L2, L3, L4, L6) | DEF-172 (`audit_d6_server_binary_releases_*`, que corre el binario real, solo Unix), DEF-175 (`audit_l2_*`) | `audit_d5_*`, `audit_d6_second_server_*`, `audit_d6_deletion_*`, `audit_l3_*`, `audit_l4_*`, `audit_l6_*` (×2). Agrega un hook `room_log_unlocked` y `tests/audit_l_server_binary_tests.rs` |
| `delta.patch` | Revisión del delta `50fe195..b4cb390` | DEF-174 (`audit_b2_designee_keeps_*`, `audit_b2_an_upload_restarted_*`) | `audit_y2_*`, `audit_y6_*`, `audit_w1_*`, y escenarios de la auditoría anterior portados (`audit_d9_damaged_log_meta_*`, `audit_d8_lost_newest_*`, `audit_r5_chunk_zero_*`) |
| `c-log.patch` | Desafiante de D2, D3 y D8 | DEF-168 (`audit2_d3_*`) | `audit2_d2_*`, `audit2_d8_*` (×2) |
| `c-storage.patch` | Desafiante de S1 y S3 | DEF-173 (`audit2_s1_*`, `audit2_s3_*`, solo Unix), evidencia de un Bajo (`audit2_bajo_*`) | — |
