package zincflow.core

/** A processor is a pure function over FlowFiles. Implementations can
 * hold config/state (fields) but the [.process] method
 * must be thread-safe — the executor may invoke it concurrently for
 * different flowfiles under the same instance. */
interface Processor {
    /** Stable identifier used in config.yaml and `/api/flow` responses.
     * Defaults to the simple class name; override for custom naming. */
    fun name(): String {
        return javaClass.getSimpleName()
    }

    fun process(ff: FlowFile): ProcessorResult
}
