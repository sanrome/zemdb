# Tests de la auditoría por invariantes

Las modificaciones que cada auditor de la [auditoría por invariantes](../2026-10-invariantes-audit.md) hizo en su copia del repo, como parches contra `50fe195`. Se guardaron porque las copias vivían en un directorio temporal. El [plan de la Fase 3.9](../../proposals/2026-10-fase3_9-remediation-plan.md) cita sus tests como "test exigido".

Para usarlos, en una copia o worktree en `50fe195`:

```bash
git apply docs/audits/2026-10-invariantes-audit/<grupo>.patch
```

Los parches son independientes entre sí: varios tocan los mismos archivos de tests, así que se aplican de a uno. Los tests usan los nombres y comentarios de la auditoría (`audit_*`, `inv_*`, códigos `INV-`), que no siguen `GEMINI.md`. Al traer un test al repo hay que renombrarlo y sacar esas referencias.

| Parche | Auditor | Contenido |
|---|---|---|
| `a.patch` | §3.5 Aislamiento | Matriz endpoint × credencial y body sin token (entraron en el Lote 0 como `crates/server/tests/credential_matrix_tests.rs`). Rupturas de INV-A3 (DEF-155): `inv_a3_*`. |
| `b.patch` | §3.6 Recursos | Mediciones de memoria con un asignador que cuenta bytes (usa `unsafe`). Rupturas de DEF-152 (`inv_b_memory.rs`), DEF-153 (`inv_b2.rs`), DEF-154 (`inv_b3.rs`) y mediciones de DEF-157 (`inv_b4.rs`). |
| `d.patch` | §3.1 Durabilidad del servidor | Rupturas de DEF-144 (`audit_d1_*`), DEF-148 (`audit_d2_*`) y DEF-149 (`audit_d5_*`). `audit_d3_*` pasa y entró en el Lote 0 (`every_byte_flip_of_active_wal_is_corruption_or_a_torn_tail`). |
| `delta.patch` | Revisión del delta | Rupturas de DEF-145 (`audit_damaged_log_meta_deletes_retained_segments`) y DEF-147 (`audit_chunk_zero_serves_a_snapshot_below_a_known_gap`). |
| `ew.patch` | §3.8–3.9 Esquema y formatos | Crate `fuzzw` (fuzzer y roundtrips con `proptest`; entró en el Lote 0 sin `proptest`). Rupturas de DEF-145 (`audit_single_bit_flip_*`). |
| `l.patch` | §3.7 Ciclo de vida | Rupturas de DEF-144 (entradas de directorio), DEF-146 (`audit_l4_*`), DEF-150 (`audit_l2_rename_*`, con dos puntos de pausa en storage), DEF-156 (`audit_l2_dropped_room_deletion_*`) y P7 (borrado parcial). Entraron en el Lote 0 los barridos de cancelación de storage, que pasan, y el test de control del `DELETE` cuyo cliente se desconecta. |
| `r.patch` | §3.4 Retención | Simulación de onboarding y `audit_r2_*` (entraron en el Lote 0). Rupturas de DEF-147 (`audit_r5_*`) y DEF-162 (`audit_one_chunk_*`). |
| `s.patch` | §3.2 Storage | Gancho `CrashPlan` y simulaciones de crash, errores y daño del WAL activo (entraron en el Lote 0). Rupturas de DEF-151 (`inv_s3_torn_*`; también `sim_damaged_compacting_wal`, que falla en 2 de 300 semillas por la misma causa). |
| `y.patch` | §3.3 Secuencia | Simulaciones de entrega y convergencia, y los tests de Y4 (respawn), Y5 e Y6 adaptados a los enunciados nuevos (entraron en el Lote 0). Ruptura de DEF-146 (`audit_y4_retry_*`). |
