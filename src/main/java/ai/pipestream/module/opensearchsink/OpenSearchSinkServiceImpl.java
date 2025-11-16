package ai.pipestream.module.opensearchsink;

import ai.pipestream.common.service.SchemaExtractorService;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.module.*;
import ai.pipestream.opensearch.v1.*;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

@Singleton
@GrpcService
public class OpenSearchSinkServiceImpl implements PipeStepProcessor {

    private static final Logger LOG = Logger.getLogger(OpenSearchSinkServiceImpl.class);

    @Inject
    SchemaExtractorService schemaExtractorService;

    @GrpcClient("opensearch-manager")
    MutinyOpenSearchManagerServiceGrpc.MutinyOpenSearchManagerServiceStub openSearchManager;

    @Override
    public Uni<ModuleProcessResponse> processData(ModuleProcessRequest request) {
        if (!request.hasDocument()) {
            return Uni.createFrom().item(ModuleProcessResponse.newBuilder()
                .setSuccess(true)
                .addProcessorLogs("No document to process.")
                .build());
        }

        OpenSearchDocument osDoc = convertToOpenSearchDocument(request.getDocument());
        String indexName = "pipeline-" + request.getDocument().getSearchMetadata().getDocumentType().toLowerCase();

        return openSearchManager.indexDocument(IndexDocumentRequest.newBuilder()
                .setIndexName(indexName)
                .setDocument(osDoc)
                .build())
            .map(response -> ModuleProcessResponse.newBuilder()
                .setOutputDoc(request.getDocument())
                .setSuccess(response.getSuccess())
                .addProcessorLogs(response.getMessage())
                .build());
    }

    private OpenSearchDocument convertToOpenSearchDocument(PipeDoc pipeDoc) {
        OpenSearchDocument.Builder builder = OpenSearchDocument.newBuilder()
            .setOriginalDocId(pipeDoc.getDocId())
            .setDocType(pipeDoc.getSearchMetadata().getDocumentType())
            .setLastModifiedAt(pipeDoc.getSearchMetadata().getLastModifiedDate());

        if (pipeDoc.getSearchMetadata().hasTitle()) builder.setTitle(pipeDoc.getSearchMetadata().getTitle());
        if (pipeDoc.getSearchMetadata().hasBody()) builder.setBody(pipeDoc.getSearchMetadata().getBody());
        if (pipeDoc.getSearchMetadata().hasKeywords() && pipeDoc.getSearchMetadata().getKeywords().getKeywordCount() > 0) {
            builder.addAllTags(pipeDoc.getSearchMetadata().getKeywords().getKeywordList());
        }

        // Convert embeddings
        for (var result : pipeDoc.getSearchMetadata().getSemanticResultsList()) {
            for (var chunk : result.getChunksList()) {
                if (chunk.hasEmbeddingInfo() && chunk.getEmbeddingInfo().getVectorCount() > 0) {
                    builder.addEmbeddings(Embedding.newBuilder()
                        .addAllVector(chunk.getEmbeddingInfo().getVectorList())
                        .setSourceText(chunk.getEmbeddingInfo().getTextContent())
                        .setChunkConfigId(result.getChunkConfigId())
                        .setEmbeddingId(result.getEmbeddingConfigId())
                        .setIsPrimary(result.getChunkConfigId().contains("title"))
                        .build());
                }
            }
        }

        return builder.build();
    }


    @Override
    public Uni<ModuleProcessResponse> testProcessData(ModuleProcessRequest request) {
        return processData(request);
    }

    @Override
    public Uni<ServiceRegistrationMetadata> getServiceRegistration(RegistrationRequest request) {
        // Using declarative registration via application.properties instead
        return Uni.createFrom().item(ServiceRegistrationMetadata.newBuilder()
                .setModuleName("opensearch-sink")
                .setVersion("1.0.0-SNAPSHOT")
                .build());
    }
}