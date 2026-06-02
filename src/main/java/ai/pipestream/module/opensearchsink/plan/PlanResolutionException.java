package ai.pipestream.module.opensearchsink.plan;

/**
 * Thrown when the sink cannot resolve one or more IndexPlan ids referenced
 * by its config. Carries an explicit, human-readable message that names every
 * offending plan id so operators can see exactly which plan is missing,
 * not READY, or failed to load.
 * <p>
 * The sink fails loud rather than degrading silently: if any plan is missing
 * or not READY, the config is rejected for the in-flight document. There is
 * no fallback path.
 */
public class PlanResolutionException extends RuntimeException {

    public PlanResolutionException(String message) {
        super(message);
    }

    public PlanResolutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
