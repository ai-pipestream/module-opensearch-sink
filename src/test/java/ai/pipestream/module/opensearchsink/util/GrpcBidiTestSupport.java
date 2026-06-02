package ai.pipestream.module.opensearchsink.util;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Drives a client-side bidi gRPC call with plain {@link StreamObserver} callbacks
 * (no Mutiny). Collects responses until the server completes the stream.
 */
public final class GrpcBidiTestSupport {

    private GrpcBidiTestSupport() {}

    public static <Req, Resp> List<Resp> sendAndCollect(
            BidiInvoker<Req, Resp> invoker,
            List<Req> requests,
            long timeoutSeconds) throws InterruptedException {
        CapturingObserver<Resp> responseObserver = new CapturingObserver<>();
        StreamObserver<Req> requestObserver = invoker.open(responseObserver);
        for (Req request : requests) {
            requestObserver.onNext(request);
        }
        requestObserver.onCompleted();
        if (!responseObserver.awaitCompletion(timeoutSeconds, TimeUnit.SECONDS)) {
            throw new AssertionError("gRPC bidi stream did not complete within " + timeoutSeconds + "s");
        }
        if (responseObserver.error.get() != null) {
            throw new AssertionError("gRPC bidi stream failed", responseObserver.error.get());
        }
        return List.copyOf(responseObserver.responses);
    }

    public static <Req, Resp> List<Resp> sendOne(BidiInvoker<Req, Resp> invoker, Req request)
            throws InterruptedException {
        return sendAndCollect(invoker, List.of(request), 30);
    }

    @FunctionalInterface
    public interface BidiInvoker<Req, Resp> {
        StreamObserver<Req> open(StreamObserver<Resp> responseObserver);
    }

    private static final class CapturingObserver<Resp> implements StreamObserver<Resp> {
        final List<Resp> responses = new ArrayList<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        private final CountDownLatch completed = new CountDownLatch(1);

        @Override
        public void onNext(Resp value) {
            responses.add(value);
        }

        @Override
        public void onError(Throwable t) {
            error.set(t);
            completed.countDown();
        }

        @Override
        public void onCompleted() {
            completed.countDown();
        }

        boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
            return completed.await(timeout, unit);
        }
    }
}
