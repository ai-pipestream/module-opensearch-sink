package ai.pipestream.module.opensearchsink.service;

import ai.pipestream.repository.pipedoc.v1.PipeDocServiceGrpc;
import ai.pipestream.repository.pipedoc.v1.SavePipeDocRequest;
import ai.pipestream.repository.pipedoc.v1.SavePipeDocResponse;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class RepoClient {

    @Inject
    @GrpcClient("repository")
    PipeDocServiceGrpc.PipeDocServiceBlockingStub repositoryStub;

    public SavePipeDocResponse savePipeDoc(SavePipeDocRequest request) {
        return repositoryStub.savePipeDoc(request);
    }
}
