package zincflow.core

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
sealed class ProcessorResult {
    @ConsistentCopyVisibility
    data class Single private constructor(val flowFile: FlowFile) : ProcessorResult() {
        companion object {
            operator fun invoke(flowFile: FlowFile): ProcessorResult {
                return Single(FlowFile(flowFile.id, flowFile.attributes, flowFile.content,
                    flowFile.timestampMillis, flowFile.hopCount))
            }
        }
    }

    @ConsistentCopyVisibility
    data class Multiple private constructor(val flowFiles: List<FlowFile>) : ProcessorResult() {
        companion object {
            operator fun invoke(flowFiles: List<FlowFile>): Multiple {
                return Multiple(flowFiles.toList())
            }
        }
    }

    @ConsistentCopyVisibility
    data class Routed private constructor(val route: String, val flowFile: FlowFile) : ProcessorResult() {
        companion object {
            operator fun invoke(route: String, flowFile: FlowFile): Routed {
                return Routed(route, FlowFile(flowFile.id, flowFile.attributes, flowFile.content,
                    flowFile.timestampMillis, flowFile.hopCount))
            }
        }
    }

    /** Emit N FlowFiles on N different relationships. Used by record-level
     * routing primitives (e.g. RouteRecord) that partition one incoming
     * batch into several outputs, each tagged with its route name. */
    @ConsistentCopyVisibility
    data class MultiRouted private constructor(val outputs: List<Routed>) : ProcessorResult() {
        companion object {
            operator fun invoke(outputs: List<Routed>): MultiRouted {
                return MultiRouted(outputs)
            }
        }
    }

    class Dropped : ProcessorResult() { }

    @ConsistentCopyVisibility
    data class Failure private constructor(val reason: String, val flowFile: FlowFile) : ProcessorResult() {
        companion object {
            operator fun invoke(reason: String, flowFile: FlowFile): Failure {
                return Failure(reason, FlowFile(flowFile.id, flowFile.attributes, flowFile.content,
                    flowFile.timestampMillis, flowFile.hopCount))
            }
        }
    }
}
