# Checklist de cierre de la serie

## Gates funcionales
- [ ] Hay servidor TCP real de protocolo nativo.
- [ ] QueryProcessor existe y ejecuta queries end-to-end.
- [ ] StorageProxy/StorageService coordinan reads y writes.
- [ ] Gossip loop converge entre nodos.
- [ ] Mensajería internodo usa conexiones persistentes con integridad y límites.
- [ ] Compaction tiene execution engine y lifecycle crash-safe.
- [ ] IO util/compression/mmap existen.
- [ ] SSTables tienen historia seria de compatibilidad o migración.
- [ ] Streaming mueve datos reales por red.
- [ ] Repair consistente existe.
- [ ] Auth persiste en system_auth.
- [ ] Guardrails/config runtime existen.
- [ ] Hay motor de índices al menos para 2i y SAI/vector según baseline.
- [ ] Virtual tables/system tables existen.
- [ ] Cache subsystem existe.
- [ ] Tooling clave no está en stub.
- [ ] Gap guards relevantes ya no están ignorados.

## Gates operativos
- [ ] Cluster de 3 nodos validado.
- [ ] Hay diff tests contra Java para áreas críticas.
- [ ] Hay soak/chaos/perf básicos.
- [ ] Hay historia probada de migración/rollback.
- [ ] Hay un juicio claro RC / not RC con evidencia.

## Gates de calidad
- [ ] Los TODOs/stubs críticos fueron eliminados o formalizados con tracking.
- [ ] No se añadieron nuevos gaps invisibles.
- [ ] Hay documentación suficiente para ingeniería y operaciones.