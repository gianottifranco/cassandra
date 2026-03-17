# Orden de ejecución y modo de uso

## Supuesto de partida

Ya se ejecutaron los prompts 01–10 del paquete anterior y el repositorio tiene:
- workspace Rust multi-crate,
- harness diferencial Java↔Rust,
- storage/coordinator/messaging base,
- documentación/ADRs iniciales,
- una ruta preliminar a GA.

## Orden recomendado

1. Prompt 11 — Auditoría final de cobertura  
2. Prompt 12 — Frontend / native protocol / CQL long tail  
3. Prompt 13 — Write path estándar  
4. Prompt 14 — Read path  
5. Prompt 15 — Storage engine final y formatos on-disk  
6. Prompt 16 — System keyspaces / virtual tables / schema surfaces  
7. Prompt 17 — Control plane clásico y nuevo  
8. Prompt 18 — Streaming y topology changes  
9. Prompt 19 — Repair y anti-entropía  
10. Prompt 20 — LWT / Paxos / counters / Accord  
11. Prompt 21 — Índices / SAI / vector / MVs  
12. Prompt 22 — Seguridad / DDM / audit / FQL  
13. Prompt 23 — Tooling / nodetool / cqlsh / SSTable tools  
14. Prompt 24 — Upgrade / migración / rollback  
15. Prompt 25 — Soak / chaos / performance / sign-off

## Modo práctico recomendado

- 1 prompt = 1 rama.
- Exige a Codex:
  - código,
  - tests,
  - docs,
  - scripts/harness,
  - y reporte final.
- Antes de fusionar:
  - build verde,
  - subset de tests verde,
  - evidencia diferencial o golden,
  - y gaps residuales explícitos.
- Si una fase toca una ruta muy crítica (storage, control plane, repair, security):
  - añade benchmarks,
  - añade chaos/failure tests,
  - y no cierres la fase sin runbook o ADR si cambió una decisión importante.

## Política de baseline

Si apuntas a un release estable (por ejemplo 5.0.x), congélalo con commit exacto.
Si apuntas a `trunk`, mantén además una baseline estable auxiliar para distinguir:
- estable,
- experimental,
- trunk-only.

Esa clasificación debe quedar materializada por Prompt 11.