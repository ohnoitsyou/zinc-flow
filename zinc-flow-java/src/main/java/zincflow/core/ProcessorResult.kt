package zincflow.core

import java.util.List

/** Result of a single [Processor.process] call. Sealed
 * so the executor can pattern-match on the shape:
 * 
 *  * [Single] — one FlowFile flows to the processor's "success" connections
 *  * [Multiple] — fan-out; every FlowFile in the list follows "success"
 *  * [Routed] — named relationship (e.g. "matched", "not-matched")
 *  * [MultiRouted] — multiple FlowFiles, each with its own named relationship
 *  * [Dropped] — terminate this branch; no downstream dispatch
 *  * [Failure] — follow "failure" connections (or log+drop if none)
 */
interface ProcessorResult {
    @JvmRecord
    data class Single(@JvmField val flowFile: FlowFile?) : ProcessorResult {
        init {
            requireNotNull(flowFile) { "flowFile must not be null" }
        }
    }

    class Multiple(flowFiles: MutableList<FlowFile?>?) : ProcessorResult {
        val flowFiles: MutableList<FlowFile?>?

        init {
            var flowFiles = flowFiles
            requireNotNull(flowFiles) { "flowFiles must not be null" }
            flowFiles = List.copyOf<FlowFile?>(flowFiles)
            this.flowFiles = flowFiles
        }
    }

    @JvmRecord
    data class Routed(@JvmField val route: String?, @JvmField val flowFile: FlowFile?) : ProcessorResult {
        init {
            require(!(route == null || route.isEmpty())) { "route must not be blank" }
            requireNotNull(flowFile) { "flowFile must not be null" }
        }
    }

    /** Emit N FlowFiles on N different relationships. Used by record-level
     * routing primitives (e.g. RouteRecord) that partition one incoming
     * batch into several outputs, each tagged with its route name. */
    class MultiRouted(outputs: MutableList<Entry?>?) : ProcessorResult {
        @JvmRecord
        data class Entry(val route: String?, val flowFile: FlowFile?) {
            init {
                require(!(route == null || route.isEmpty())) { "route must not be blank" }
                requireNotNull(flowFile) { "flowFile must not be null" }
            }
        }

        val outputs: MutableList<Entry?>?

        init {
            var outputs = outputs
            requireNotNull(outputs) { "outputs must not be null" }
            outputs = List.copyOf<Entry?>(outputs)
            this.outputs = outputs
        }
    }

    /** Singleton — no state, reuse the shared instance. */
    enum class Dropped : ProcessorResult {
        INSTANCE
    }

    class Failure(reason: String?, val flowFile: FlowFile?) : ProcessorResult {
        val reason: String?

        init {
            var reason = reason
            if (reason == null) reason = ""
            requireNotNull(flowFile) { "flowFile must not be null" }
            this.reason = reason
        }
    }

    companion object {
        // Static factories for ergonomics — match the C# ctor sites' feel.
        @JvmStatic
        fun single(ff: FlowFile?): ProcessorResult {
            return Single(ff)
        }

        fun multiple(ffs: MutableList<FlowFile?>?): ProcessorResult {
            return Multiple(ffs)
        }

        fun routed(route: String?, ff: FlowFile?): ProcessorResult {
            return Routed(route, ff)
        }

        fun multiRouted(outputs: MutableList<MultiRouted.Entry?>?): ProcessorResult {
            return MultiRouted(outputs)
        }

        @JvmStatic
        fun dropped(): ProcessorResult {
            return Dropped.INSTANCE
        }

        fun failure(reason: String?, ff: FlowFile?): ProcessorResult {
            return Failure(reason, ff)
        }
    }
}
