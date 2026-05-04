package zincflow.providers

import jdk.internal.vm.ThreadContainers.container
import org.apache.commons.collections4.queue.CircularFifoQueue
import zincflow.core.ComponentState
import zincflow.core.Provider
import zincflow.core.ProviderPlugin
import java.util.Collections
import java.util.function.IntFunction
import kotlin.concurrent.Volatile
import kotlin.math.min

/** Provenance recorder — a bounded ring buffer of FlowFile lifecycle
 * events (created / processed / routed / dropped / failed). Oldest
 * entries evict once the buffer fills. When the provider is disabled
 * [.record] becomes a no-op, so production operators can flip
 * provenance off without rebuilding the pipeline.
 * 
 * Mirrors zinc-flow-csharp's ProvenanceProvider (Core/Providers.cs). */
class ProvenanceProvider @JvmOverloads constructor(val capacity: Int = DEFAULT_CAPACITY) : Provider {
    enum class EventType {
        CREATED, PROCESSED, ROUTED, DROPPED, FAILED, UNKNOWN
    }

    @JvmRecord
    data class Event(
        val flowFileId: Long,
        val type: EventType?,
        val component: String?,
        val details: String?,
        val timestampMillis: Long
    ) {
        companion object {
            val EMPTY = Event(-1, EventType.UNKNOWN, "", "", 0)
        }
    }

    class CircularBuffer<T> private constructor(override val size: Int = DEFAULT_CAPACITY, private val container: MutableList<T>) : MutableList<T> by container {
        companion object {
            operator fun <T>invoke(size: Int = DEFAULT_CAPACITY) : CircularBuffer<out T> {
                return CircularBuffer(size, mutableListOf())
            }
        }
    }

    private val b = CircularBuffer<Event>(capacity)
    private val buffer = CircularFifoQueue<Event>(capacity)
    private val lock = Any()
    private var head = 0 // next write slot
    private var count = 0

    @Volatile
    private var state = ComponentState.DISABLED

    init {
        require(capacity > 0) { "ProvenanceProvider capacity must be > 0, got $capacity" }
    }

    override fun name(): String {
        return NAME
    }

    override fun providerType(): String {
        return TYPE
    }

    override fun state(): ComponentState {
        return state
    }

    override fun enable() {
        state = ComponentState.ENABLED
    }

    override fun disable(drainTimeoutSeconds: Int) {
        state = ComponentState.DISABLED
    }

    override fun shutdown() {
        state = ComponentState.DISABLED
    }

    fun capacity(): Int {
        return capacity
    }

    fun size(): Int {
        synchronized(lock) { return count }
    }

    /** Drop an event in the ring buffer. Silent no-op when disabled so
     * callers can sprinkle `record(...)` calls in hot paths
     * without a per-call enable check. */
    @JvmOverloads
    fun record(flowFileId: Long, type: EventType, component: String?, details: String? = "") {
        if (!isEnabled) return
        val evt = Event(
            flowFileId,
            type,
            component ?: "",
            details ?: "",
            System.currentTimeMillis()
        )
        synchronized(lock) {
            buffer.add(evt)
        }
    }

    /** Events for a single FlowFile, oldest first. Empty if none recorded
     * (either the id never appeared, or it was evicted). */
    fun getEvents(flowFileId: Long): List<Event> {
        return synchronized(lock) {
            buffer.asIterable().asSequence().filter { it.flowFileId == flowFileId }.take(count).toList()
        }
    }

    /** Most recent N events across every FlowFile, oldest first within
     * the window. If fewer than N are available all are returned. */
    fun getRecent(n: Int): List<Event> {
        if (n <= 0) return mutableListOf()
        val out = mutableListOf<Event>()
        synchronized(lock) {
            val toTake = min(n, count)
            val start = ((head - toTake) % capacity + capacity) % capacity
            for (i in 0..< toTake) {
                val e = buffer[(start + i) % capacity]
                if (e != null) out.add(e)
            }
        }
        return out
    }

    class Plugin : ProviderPlugin {
        override fun providerType(): String {
            return TYPE
        }

        override fun description(): String {
            return "Bounded ring buffer of FlowFile lifecycle events."
        }

        override fun configKeys(): List<String> {
            return listOf("buffer")
        }

        override fun create(config: MutableMap<String, Any>): Provider {
            val buf = config["buffer"]
            return ProvenanceProvider(buf as? Int ?: DEFAULT_CAPACITY)
        }
    }

    companion object {
        const val NAME: String = "provenance"
        const val TYPE: String = "ProvenanceProvider"

        /** Default buffer size — matches the C# default. */
        const val DEFAULT_CAPACITY: Int = 100000

        /** Shared no-op instance — `record(...)` early-returns because
         * the provider stays DISABLED. Useful as a null-object for callers
         * that want to call `record` unconditionally when no
         * provenance provider is wired. */
        @JvmField
        val DISABLED: ProvenanceProvider = ProvenanceProvider(1)
    }
}
