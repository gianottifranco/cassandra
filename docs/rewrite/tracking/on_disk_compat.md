# On-Disk Compatibility Tracking

Tracks binary on-disk format compatibility between Java and Rust.

## SSTable Format

| Component | Status | Owning Prompt | Closure Criteria |
|-----------|--------|---------------|------------------|
| Read Java big-format SSTables | Missing | 11 | Rust reads Java SSTables correctly |
| Write Java-compatible SSTables | Missing | 11 | Java reads Rust-written SSTables |
| Trie-based partition index | Missing | 05 | New SSTable format index support |
| Bloom filter interop | Missing | 11 | Shared bloom filter format |
| Compression (LZ4/Snappy/Zstd) | Partial (LZ4, Snappy) | 10 | All three codecs + Java interop |
| Statistics/metadata files | Missing | 11 | Stats files parseable by both |

## CommitLog

| Component | Status | Owning Prompt | Closure Criteria |
|-----------|--------|---------------|------------------|
| Segment format | Partial (CRC, basic) | 04 | Segment format with encryption/compression |
| Compression | Missing | 10 | Compressed commit log segments |
| Encryption | Missing | 15 | Encrypted commit log segments |
| CDC integration | Missing | 19 | CDC reads from commit log |

## system_auth / system Tables

| Component | Status | Owning Prompt | Closure Criteria |
|-----------|--------|---------------|------------------|
| system_auth.roles | Missing | 14 | Roles persisted and queryable |
| system_auth.role_permissions | Missing | 14 | Permissions persisted |
| system_auth.credentials | Missing | 14 | Password hashes persisted |
| system.paxos | Missing | 22 | Paxos state survives restart |
| system_traces | Missing | 19 | Trace sessions persisted |

## Migration Path

| Component | Status | Owning Prompt | Closure Criteria |
|-----------|--------|---------------|------------------|
| SSTable migration tool | Missing | 11, 26 | Tool converts Java SSTables to Rust format |
| Full binary SSTable conversion | TODO | 11, 26 | Lossless conversion verified |
| Upgrade procedure validated | Missing | 26 | Rolling upgrade documented and tested |
