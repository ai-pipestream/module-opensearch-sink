# Task: High-Throughput OpenSearch Migration (100k Push)

**Status:** Completed & PRs Open
**Date:** 2026-04-26

## 1. Problem Statement
The existing OpenSearch indexing pipeline was identified as the primary bottleneck preventing the 100,000 document end-to-end test. Key issues included:
- **Silent Document Drops:** Context capture bugs in gRPC stubs causing `CANCELLED` statuses.
- **Unary Bottleneck:** The manager was performing a "join-of-unary-tasks" pattern, paying a network round-trip and cluster-state check for every document.
- **Transactional Race Conditions:** High-concurrency streams were triggering DB unique constraint and foreign key violations in the vector-set binding layer.
- **Stale Protos:** The streaming RPC was missing the fields required for flat indexing strategies (`document_map`, `chunk_documents`).

## 2. Actions Performed

### A. High-Fidelity Validation (WireMock)
- **Repo:** `pipestream-wiremock-server` (0.1.58)
- **Actions:**
    - Implemented `indexDocumentsBatch` and enhanced `streamIndexDocuments` to simulate production pressure.
    - Added `x-bulk-fail-rate` header support to inject probabilistic failures (Chaos Testing).
    - Implemented `ProvisionIndex` mock to validate the "Eager Provisioning" model.
    - Added `OpenSearchManagerBulkPerfTest` to lock in the contract.
- **PR:** https://github.com/ai-pipestream/pipestream-wiremock-server/pull/81

### B. Protocol Evolution (Protos)
- **Repo:** `pipestream-protos`
- **Actions:**
    - Updated `StreamIndexDocumentsRequest` to include `document_map` and `chunk_documents`.
    - Aligned the streaming path with the unary path to support `SEPARATE_INDICES` and `CHUNK_COMBINED` strategies.
- **PR:** https://github.com/ai-pipestream/pipestream-protos/pull/44

### C. OpenSearch Sink Optimization
- **Worktree:** `opensearch-sink-pr0`
- **Actions:**
    - **Eager Provisioning:** Implemented a proactive `ProvisionIndex` call at the start of every stream/batch.
    - **Caffeine Caching:** Added local cache to ensure provisioning is an O(1) no-op after the first document.
    - **Optimized Streaming:** Fixed Mutiny logic to use `group().intoLists().of(100)` for efficient batch relaying.
- **PR:** https://github.com/ai-pipestream/module-opensearch-sink/pull/10

### D. OpenSearch Manager Overhaul
- **Worktree:** `opensearch-manager-pr0`
- **Actions:**
    - **True Batch Indexing:** Refactored `NestedIndexingStrategy` and `SeparateIndicesIndexingStrategy` to use the `BulkQueueSet` asynchronously.
    - **Relay Architecture:** Schema validation now happens once per batch; documents are relayed to background draining queues immediately.
    - **Transactional Safety:** Fixed race conditions in `resolveOrCreateVectorSet` and `ensureIndexBinding` using a strict "select-then-insert-and-flush" pattern with conflict handling.
    - **JSON Performance:** Surgical Map manipulation in `transformSemanticSetsToNestedFieldsMap` to avoid full re-serialization overhead.
- **PR:** https://github.com/ai-pipestream/pipestream-opensearch/pull/25

## 3. Engineering Standards Applied
- **No Hacks/Fallbacks:** Removed all individual-retry loops and manual serialization fallbacks. The system now strictly follows the protocol spec.
- **Fail Loudly:** Protocol violations (e.g., missing `document_map`) now return correlated error responses immediately.
- **Golden State Compliance:** All services now use Virtual Threads + Async Relay architecture for the hot path.

## 4. Next Steps for Validation
1.  **Merge Protos PR** to stabilize the build dependencies.
2.  **Run 10k Scale Test:** Use `module-testing-sidecar` against the updated worktrees to verify throughput.
3.  **Run 100k Scale Test:** Monitor `engine.queue.depth` and `opensearch_bulk_queue_depth` to ensure background draining keeps up with intake.
