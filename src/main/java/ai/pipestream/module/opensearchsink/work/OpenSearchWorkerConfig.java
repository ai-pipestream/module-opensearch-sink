package ai.pipestream.module.opensearchsink.work;

import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.module.runtime.work.ModuleWorkEngineClient;
import ai.pipestream.module.runtime.work.ModuleWorkerLoop;
import ai.pipestream.module.runtime.work.RampController;
import ai.pipestream.module.runtime.work.WorkerLoopConfig;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Wires demand-pull work consumption for the OpenSearch sink
 * ({@code pipestream.module.opensearch-sink} on the engine).
 */
@IfBuildProfile(anyOf = {"prod", "dev"})
@ApplicationScoped
public class OpenSearchWorkerConfig {

    @Inject
    OpenSearchModuleProcessor processor;

    @Inject
    ModuleWorkEngineClient engineClient;

    @Produces
    @Singleton
    ModuleWorkerLoop<PipeStream> openSearchWorkerLoop(WorkerLoopConfig config, RampController rampController) {
        return new ModuleWorkerLoop<>(
                PipeStream.class, processor, engineClient, config, rampController);
    }

    void onStart(@Observes StartupEvent ev, ModuleWorkerLoop<PipeStream> loop) {
        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            loop.onStart(ev);
        }, "opensearch-sink-worker-loop-starter").start();
    }

    void onStop(@Observes ShutdownEvent ev, ModuleWorkerLoop<PipeStream> loop) {
        loop.onStop(ev);
    }
}
