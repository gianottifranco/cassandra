# Orden de ejecución

## Fase A — Arranque imprescindible (P0)
- Prompt 00
- Prompt 01
- Prompt 02
- Prompt 03
- Prompt 04
- Prompt 05
- Prompt 06
- Prompt 07
- Prompt 08
- Prompt 09
- Prompt 10

## Fase B — Beta real (P1)
- Prompt 11
- Prompt 12
- Prompt 13
- Prompt 14
- Prompt 15
- Prompt 16
- Prompt 17
- Prompt 18
- Prompt 19
- Prompt 20

## Fase C — Camino a GA (P2)
- Prompt 21
- Prompt 22
- Prompt 23
- Prompt 24
- Prompt 25
- Prompt 26

## Recomendaciones
- Ejecuta primero Prompt 00 aunque te parezca “solo gestión”; evita perder trazabilidad.
- Si Prompt 01 o Prompt 02 encuentran blockers de arquitectura, resuélvelos antes de seguir.
- No saltes directamente a SAI/TCM/Accord si el server, QueryProcessor, StorageProxy, gossip o mensajería internodo siguen rotos.
- Usa Prompt 26 solo cuando la mayor parte de los prompts previos estén cerrados y el cluster ya pueda correr workloads reales.