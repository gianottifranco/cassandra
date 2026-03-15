# Paquete: reescritura completa de Apache Cassandra de Java a Rust

Este paquete contiene un plan maestro de reescritura, un plan específico de optimización posterior en Rust, una batería de prompts largos para Codex y una matriz de subsistemas para aterrizar la migración.

## Qué asumí para resolver tu pedido

- **“Reescritura completa”** significa reimplementar el **servidor y sus subsistemas Java** en Rust con paridad observable: CQL, protocolo nativo, storage engine, coordinación distribuida, repair/streaming, seguridad, observabilidad y tooling operativo.
- No consideré necesario “reescribir” componentes que **ya no son Java** si siguen siendo válidos como superficie externa, por ejemplo `cqlsh`/`pylib` en Python, salvo que el plan los reemplace por una razón funcional u operativa.
- Trato la reescritura como un **programa incremental con oráculo diferencial** usando Cassandra Java congelado en una baseline, no como una traducción clase a clase.
- Dado que la rama `trunk` puede contener trabajo en progreso (por ejemplo piezas ligadas a consenso/Accord), el plan fuerza un **freeze explícito de baseline** antes de ejecutar nada serio.

## Orden sugerido de lectura

1. `01_plan_reescritura_cassandra_java_a_rust.md`
2. `02_plan_optimizacion_rust_post_reescritura.md`
3. `04_mapa_de_subsistemas_y_crates.md`
4. `03_prompts_codex_reescritura_completa.md`
5. `05_fuentes_y_baseline.md`

## Cómo usar los prompts

- Están pensados para ejecutarse **en secuencia**.
- Cada prompt repite restricciones importantes a propósito: mantener Java como oráculo, no borrar prematuramente, añadir tests y dejar el repo en estado compilable.
- Los prompts están escritos para que Codex **materialice código, tests, docs y harnesses**, no solo recomendaciones.
- Si quieres bajar riesgo, ejecuta cada prompt en una rama independiente y exige reporte de gaps antes de pasar al siguiente.

## Idea central del paquete

La forma menos arriesgada de llevar Cassandra a Rust es esta:

1. congelar baseline;
2. extraer contrato de compatibilidad;
3. montar un oráculo diferencial Java↔Rust;
4. portar por **vertical slices**;
5. conseguir primero **paridad conservadora** (`big` SSTable, memtable clásica, core distribuido);
6. introducir después optimizaciones estructurales (`BTI`, memtables trie, UCS, etc.);
7. cerrar finalmente la **ruta de migración** y el hardening operativo.

## Contenido

- **Plan maestro**: fases, gates de salida, riesgos y orden realista de ejecución.
- **Plan de optimización Rust**: disciplina de perf, toolchain, memoria, I/O, concurrencia y budgets.
- **Prompts Codex**: 10 prompts grandes para ejecutar la reescritura paso a paso.
- **Mapa de subsistemas**: propuesta de crates Rust y correspondencia con áreas del código actual.
- **Fuentes**: baseline factual y enlaces consultados.

## Nota sobre la URL de Code Wiki

La URL que compartiste (`https://codewiki.google/github.com/apache/cassandra`) la tomé como **acelerador opcional para explorar el repo**, pero el contenido de este paquete está apoyado sobre todo en:

- documentación oficial de Apache Cassandra,
- superficies públicas del repo `apache/cassandra`,
- y referencias oficiales/autoritativas de Rust para la parte de optimización.

Eso hace el material más resistente a cambios de la herramienta y más fácil de auditar.