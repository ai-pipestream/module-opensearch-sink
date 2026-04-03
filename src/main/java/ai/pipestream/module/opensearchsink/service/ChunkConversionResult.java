package ai.pipestream.module.opensearchsink.service;

import ai.pipestream.opensearch.v1.OpenSearchChunkDocument;
import ai.pipestream.opensearch.v1.OpenSearchDocumentMap;

import java.util.List;

/**
 * Holds the converted chunk documents alongside the parent document map
 * and audit log messages generated during the conversion process.
 */
public record ChunkConversionResult(
        OpenSearchDocumentMap documentMap,
        List<OpenSearchChunkDocument> chunkDocuments,
        List<String> auditLogs
) {
}
