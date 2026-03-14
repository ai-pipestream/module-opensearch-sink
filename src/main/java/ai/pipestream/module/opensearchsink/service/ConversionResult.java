package ai.pipestream.module.opensearchsink.service;

import ai.pipestream.opensearch.v1.OpenSearchDocument;

import java.util.List;

/**
 * Holds the converted OpenSearch document alongside audit log messages
 * generated during the conversion process.
 */
public record ConversionResult(
        OpenSearchDocument document,
        List<String> auditLogs
) {
}
