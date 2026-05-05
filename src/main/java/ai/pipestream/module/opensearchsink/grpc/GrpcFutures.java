package ai.pipestream.module.opensearchsink.grpc;

import io.grpc.stub.StreamObserver;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Tiny {@link StreamObserver}-to-{@link CompletableFuture} adapter for unary
 * gRPC calls. Same shape as the helpers in module-testing-sidecar,
 * pipestream-engine-kafka-sidecar, and repository-service.
 *
 * <p>Why this exists: Mutiny gRPC stubs (the {@code MutinyXxxGrpc::newMutinyStub}
 * factories) wrap a {@link io.grpc.ClientCall} in a {@code Uni}. The Mutiny
 * adapter has a known failure mode where transport-level terminal events from
 * the underlying {@code ClientCall.Listener} fire in an order the adapter
 * doesn't expect — typically presenting as
 * {@code CANCELLED: io.grpc.Context was cancelled without error} when the
 * caller's Vert.x context is invalidated mid-call. The {@code Uni} stays
 * subscribed forever, no callback lands, no thread parks, and the orchestrator
 * stalls with no log line.
 *
 * <p>Plain async stubs do not have this problem. A {@code StreamObserver}
 * receives the gRPC framework's terminal events directly: {@code onNext} for
 * the response, {@code onError} for any transport or status failure,
 * {@code onCompleted} when the call closes. Nothing in between to lose
 * events.
 *
 * <p>Usage:
 * <pre>{@code
 * MyServiceGrpc.MyServiceStub stub =
 *     grpcClientFactory.getAsyncClient("my-service", MyServiceGrpc::newStub);
 * CompletableFuture<MyResponse> future = GrpcFutures.unary(
 *     observer -> stub.myRpc(request, observer));
 * MyResponse response = future.get();   // blocks the calling (virtual) thread
 * }</pre>
 */
public final class GrpcFutures {

    private GrpcFutures() {}

    /**
     * Invokes a unary gRPC call and returns a {@link CompletableFuture} that
     * completes with the single response, fails with the gRPC status, or
     * fails with {@link IllegalStateException} if the call closes without
     * producing a response.
     *
     * @param call function that takes a response {@link StreamObserver} and
     *             initiates the gRPC call (e.g. {@code observer -> stub.rpc(req, observer)})
     */
    public static <T> CompletableFuture<T> unary(Consumer<StreamObserver<T>> call) {
        Objects.requireNonNull(call, "call");
        CompletableFuture<T> response = new CompletableFuture<>();
        AtomicBoolean sawResponse = new AtomicBoolean(false);
        try {
            call.accept(new StreamObserver<>() {
                @Override
                public void onNext(T value) {
                    sawResponse.set(true);
                    response.complete(value);
                }

                @Override
                public void onError(Throwable t) {
                    response.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                    if (!sawResponse.get()) {
                        response.completeExceptionally(new IllegalStateException(
                                "gRPC unary call completed without a response"));
                    }
                }
            });
        } catch (Throwable t) {
            response.completeExceptionally(t);
        }
        return response;
    }
}
