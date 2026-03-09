# OpenSearch Sink Module

The OpenSearch Sink is a pipeline sink module responsible for persisting processed pipeline documents and their semantic vectors into OpenSearch, ready for hybrid search and dashboarding. It accepts PipeDoc messages from upstream pipeline stages, converts them to an OpenSearchDocument, ensures the target index and fields exist, and indexes the document via the OpenSearch Manager gRPC service.

- Module type: sink (auto‑registered)
- Service style: Quarkus gRPC service (exposes an HTTP port as well for gRPC‑Web via proxy)
- Frontend: Quinoa build that integrates with the platform web‑proxy for live gRPC calls

For overall sink module concepts and how modules render in the platform, see:
- docs/architecture/Module_rendering_architecture.md
- docs/architecture/Module_rendering_guide.md
- docs/architecture/Pipeline_design.md
- docs/architecture/frontend/Web_Proxy.md


## What this module does

- Receives ModuleProcessRequest messages and writes them to OpenSearch.
- Derives the target index name from the document type: pipeline-<docType>.
- Converts PipeDoc into an OpenSearchDocument, carrying both textual fields and embedding vectors.
- Ensures index/field mappings exist for stored columns; creates missing fields as needed. The embedder typically pre‑creates vector fields as data flows through the pipeline.
- Returns a ModuleProcessResponse including success status and processing logs.

Implementation entrypoint: src/main/java/io/pipeline/module/opensearchsink/OpenSearchSinkServiceImpl.java


## Produced metadata (indexed fields)
The sink writes a normalized OpenSearchDocument with the following notable fields. These appear as columns in the collection output and in dashboards:

- originalDocId: The upstream PipeDoc unique ID.
- docType: The document/category type, used to derive the index name.
- title: Optional title extracted from the PipeDoc.
- body: Optional body/content text.
- tags: Keyword list derived from PipeDoc keywords.
- lastModifiedAt: Timestamp propagated from the PipeDoc.
- embeddings: A repeated structure capturing semantic vectors produced by upstream embedder stages:
  - vector: The numeric embedding vector.
  - sourceText: Text span that was embedded (e.g., chunk text).
  - chunkConfigId: The chunking configuration identifier associated with the vector.
  - embeddingId: The embedding configuration identifier/model reference.
  - isPrimary: Flag to mark primary vectors (e.g., for title‑based vectors).

Notes
- The sink can create missing fields/mappings on first write. In practice, the embedder stage(s) create vector fields as the pipeline progresses, so most mappings will be in place.
- Columns are designed to align with pipeline semantics so dashboards can be built directly on top of the index.


## OpenSearch integration

This module integrates with OpenSearch through the OpenSearch Manager gRPC service (MutinyOpenSearchManagerServiceGrpc). The manager is responsible for index creation, mapping updates, and document indexing. The sink focuses on:

- Transforming pipeline documents to an OpenSearch‑ready shape.
- Choosing the index name per document type.
- Requesting the manager to index the document.

Vector/k‑NN configuration
- Default vector search behavior is described by configuration records in src/main/java/io/pipeline/module/opensearchsink/config/:
  - OpenSearchSinkOptions: top‑level module options
  - KnnMethod: engine and space type (e.g., LUCENE + COSINESIMIL)
  - KnnParameters: HNSW parameters (m, ef_construction, ef_search)
  - BatchOptions: bulk indexing controls and client timeouts (connection overrides)
- The service shares a JSON configuration schema during service registration so the platform can render a UI for these options.

Index creation & mappings
- The sink and manager collaborate so that indices and fields are created if they don’t exist.
- k‑NN vector fields are created with the appropriate engine/space type and HNSW parameters.
- Text and keyword fields (title, body, tags, ids, timestamps) are mapped accordingly to support hybrid queries and filtering.


## Features at a glance
- gRPC sink module (auto‑registers with the pipeline coordinator)
- Hybrid‑search‑ready indexing (text + vector fields)
- Automatic index/field creation on demand
- Configurable batch/bulk behavior and client timeouts
- Quinoa frontend build targeting the platform’s web‑proxy (for gRPC‑Web in live mode)
- Designed for dashboard consumption (columns aligned with pipeline semantics)


## Build and run

Dev mode
- From the repository root:
  - ./gradlew :modules:opensearch-sink:quarkusDev
- The service exposes an HTTP port for dev/proxy use. You can adjust QUARKUS_HTTP_PORT if needed (see below).

Packaging (JVM mode)
- ./gradlew :modules:opensearch-sink:build
- Artifact: modules/opensearch-sink/build/quarkus-app
- Run: java -jar modules/opensearch-sink/build/quarkus-app/quarkus-run.jar

Native build (optional)
- ./gradlew :modules:opensearch-sink:build -Dquarkus.native.enabled=true
- Containerized native build:
  - ./gradlew :modules:opensearch-sink:build -Dquarkus.native.enabled=true -Dquarkus.native.container-build=true

Quinoa & the web‑proxy
- The module’s frontend is built with Quinoa. In live/dev mode, the frontend connects through the platform web‑proxy for gRPC calls.
- For details on the proxy and gRPC‑Web wiring, see docs/architecture/frontend/Web_Proxy.md and the platform frontend docs. The sink delegates to those components at runtime; you don’t need to configure gRPC‑Web directly here.


## Configuration
This sink primarily communicates with the OpenSearch Manager over gRPC. Core OpenSearch cluster configuration is handled by that service. The sink exposes:

- BatchOptions (bulk size, time window, connection timeouts)
- Default vector method settings (engine, space type, HNSW parameters)

Environment variables commonly used during development:
- QUARKUS_HTTP_PORT: Overrides the HTTP port (useful when running behind the web‑proxy); defaults may be set by scripts.
- MODULE_HOST: Host binding hint used by shared scripts/utilities.

At registration time, the service publishes a JSON schema for its options so the platform UI can render and persist module configuration. See OpenSearchSinkServiceImpl.getServiceRegistration and config records under src/main/java/io/pipeline/module/opensearchsink/config/.


## Docker
Prebuilt Dockerfiles are provided under src/main/docker/.

Build a JVM image
- From repository root:
  1) Build the module: ./gradlew :modules:opensearch-sink:build
  2) Build the image with the Quarkus JVM Dockerfile:
     docker build -f modules/opensearch-sink/src/main/docker/Dockerfile.jvm -t opensearch-sink:local .

Run the container
- docker run --rm \
    -e QUARKUS_HTTP_PORT=19004 \
    -p 19004:19004 \
    --name opensearch-sink \
    opensearch-sink:local

Native image (optional)
- Build: ./gradlew :modules:opensearch-sink:build -Dquarkus.native.enabled=true -Dquarkus.native.container-build=true
- Image: docker build -f modules/opensearch-sink/src/main/docker/Dockerfile.native -t opensearch-sink-native:local .


## Dashboard frontend
This module’s outputs are intended to back a dashboard experience. The index columns align with pipeline semantics (ids, types, timestamps, tags, title/body, and embeddings), enabling:
- Full‑text queries with filters on tags/types/time
- Vector similarity search (k‑NN) over one or more embedding fields
- Hybrid search combining filters and vectors

The dashboard and gRPC‑Web flows run through the platform web‑proxy. Refer to the frontend documentation for wiring and usage details: docs/architecture/frontend/Web_Proxy.md


## Related documentation
- docs/architecture/Module_rendering_architecture.md
- docs/architecture/Module_rendering_guide.md
- docs/architecture/Pipeline_design.md
- docs/architecture/frontend/Web_Proxy.md
