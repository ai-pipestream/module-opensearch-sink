package ai.pipestream.module.opensearchsink.kafka;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.repository.v1.IndexingRequestEvent;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Registered {@link UuidKeyExtractor} for {@link IndexingRequestEvent}.
 * Apicurio auto-discovers this bean and uses it on every
 * {@code ProtobufEmitter.send(event)} to mint the Kafka partition key.
 *
 * <p>The key is a deterministic UUID-v3 (name-based) over the
 * {@code document_id} field of the envelope. Same doc → same key →
 * same partition → ordered delivery. Producer retries / worker-loop
 * redeliveries of the same logical request all land on the same
 * partition.
 *
 * <p>{@code document_id} is read directly off the envelope (not by
 * inspecting {@code document_ref} or unpacking {@code inline_payload})
 * — the proto guarantees it's populated regardless of which transport
 * the producer chose, so the keyer doesn't have to inspect oneof state
 * or unpack {@code Any}. That's the whole reason {@code document_id}
 * was added to the envelope.
 */
@ApplicationScoped
public class IndexingRequestEventKeyExtractor implements UuidKeyExtractor<IndexingRequestEvent> {

    @Override
    public UUID extractKey(IndexingRequestEvent event) {
        String docId = event.getDocumentId();
        if (docId == null || docId.isBlank()) {
            throw new IllegalStateException(
                    "IndexingRequestEvent emitted without document_id; refusing to mint a partition key. "
                            + "The producer must populate document_id on every event regardless of transport.");
        }
        return UUID.nameUUIDFromBytes(docId.getBytes(StandardCharsets.UTF_8));
    }
}
