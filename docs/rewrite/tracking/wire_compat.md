# Wire Compatibility Tracking

Tracks binary wire-level compatibility between Java and Rust implementations.

## CQL Native Protocol

| Component | Status | Owning Prompt | Closure Criteria |
|-----------|--------|---------------|------------------|
| Frame codec (v4/v5) | Partial (codec done, no TCP) | 01, 02 | TCP listener serves protocol frames |
| Protocol negotiation | Missing | 01 | Version negotiation on connect |
| All opcodes | Partial | 02 | All 16 opcodes handled |
| Event push (TOPOLOGY_CHANGE, etc.) | Missing | 07 | Events sent on cluster changes |
| Backpressure | Missing | 01 | Client throttling under load |
| Payload size limits | Missing | 01 | Max frame size enforced |

## Internode Messaging

| Component | Status | Owning Prompt | Closure Criteria |
|-----------|--------|---------------|------------------|
| Verb enum | Done | — | — |
| Frame codec | Stub | 08 | Binary-compatible with Java frames |
| CRC32 integrity | Missing | 08 | CRC on every internode frame |
| LZ4 compression | Missing | 08 | LZ4 on internode payloads |
| 3-channel architecture | Missing | 08 | Urgent/small/large channels |
| Connection pooling | Missing | 08 | Persistent connections, no TCP per send |
| Wire compatibility with Java | Missing | 08 | Mixed Java+Rust cluster messaging |

## Gossip Protocol

| Component | Status | Owning Prompt | Closure Criteria |
|-----------|--------|---------------|------------------|
| Gossip message format | Partial (structures, not wire) | 07 | Wire-compatible with Java gossip |
| Phi-accrual failure detection | Partial | 07 | Detects node failures correctly |
| Schema exchange | Missing | 07, 18 | Schema versions exchanged via gossip |
