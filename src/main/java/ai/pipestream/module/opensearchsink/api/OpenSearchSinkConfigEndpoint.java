package ai.pipestream.module.opensearchsink.api;

import ai.pipestream.module.opensearchsink.config.OpenSearchSinkOptions;
import ai.pipestream.module.opensearchsink.schema.SchemaExtractorService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.Optional;

/**
 * Minimal REST endpoint that exposes the OpenSearchSinkOptions JSON Schema.
 * The primary consumer is the gRPC getServiceRegistration() method (via SchemaExtractorService),
 * but this endpoint also makes the schema browseable via Swagger UI.
 */
@Path("/api/opensearch-sink/service")
@Tag(name = "OpenSearch Sink Service", description = "OpenSearch sink configuration schema")
@Produces(MediaType.APPLICATION_JSON)
public class OpenSearchSinkConfigEndpoint {

    private static final Logger LOG = Logger.getLogger(OpenSearchSinkConfigEndpoint.class);

    @Inject
    SchemaExtractorService schemaExtractorService;

    @GET
    @Path("/config/jsonforms")
    @Operation(summary = "Get OpenSearch Sink configuration schema for JSONForms",
               description = "Returns the resolved OpenSearchSinkOptions JSON Schema (no $refs) for JSONForms rendering")
    @APIResponse(
        responseCode = "200",
        description = "Resolved JSON Schema for OpenSearchSinkOptions",
        content = @Content(
            mediaType = MediaType.APPLICATION_JSON,
            schema = @Schema(implementation = OpenSearchSinkOptions.class)
        )
    )
    public Uni<Response> getConfigJsonForms() {
        return Uni.createFrom().item(() -> {
            Optional<String> schema = schemaExtractorService.extractConfigSchemaResolvedForJsonForms();
            if (schema.isPresent()) {
                return Response.ok(schema.get()).type(MediaType.APPLICATION_JSON).build();
            }
            LOG.warn("Could not extract OpenSearchSinkOptions schema from OpenAPI document");
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                .entity(Map.of("error", "Schema not available"))
                .build();
        });
    }
}
