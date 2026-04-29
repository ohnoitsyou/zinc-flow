package zincflow.providers

import zincflow.core.ComponentState
import zincflow.core.Provider
import zincflow.core.ProviderPlugin
import kotlin.concurrent.Volatile
import kotlin.math.min

/** Provenance recorder — a bounded ring buffer of FlowFile lifecycle
 * events (created / processed / routed / dropped / failed). Oldest
 * entries evict once the buffer fills. When the provider is disabled
 * [.record] becomes a no-op, so production operators can flip
 * provenance off without rebuilding the pipeline.
 * 
 * Mirrors zinc-flow-csharp's ProvenanceProvider (Core/Providers.cs). */
class ProvenanceProvider @JvmOverloads constructor(capacity: Int = DEFAULT_CAPACITY) : Provider {
    enum class EventType {
        CREATED, PROCESSED, ROUTED, DROPPED, FAILED
    }

    @JvmRecord
    data class Event(
        @JvmField val flowFileId: Long,
        @JvmField val type: EventType?,
        @JvmField val component: String?,
        @JvmField val details: String?,
        @JvmField val timestampMillis: Long
    )

    private val buffer: Array<Event?>
    private val capacity: Int
    private val lock = Any()
    private var head = 0 // next write slot
    private var count = 0

    @Volatile
    private var state = ComponentState.DISABLED

    init {
        require(capacity > 0) { "ProvenanceProvider capacity must be > 0, got " + capacity }
        this.capacity = capacity
        this.buffer = arrayOfNulls<Event>(capacity)
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
    fun record(flowFileId: Long, type: EventType?, component: String?, details: String? = "") {
        if (!isEnabled()) return
        val evt = Event(
            flowFileId,
            type,
            if (component == null) "" else component,
            if (details == null) "" else details,
            System.currentTimeMillis()
        )
        synchronized(lock) {
            buffer[head] = evt
            head = (head + 1) % capacity
            if (count < capacity) count++
        }
    }

    /** Events for a single FlowFile, oldest first. Empty if none recorded
     * (either the id never appeared, or it was evicted). */
    fun getEvents(flowFileId: Long): MutableList<Event?> {
        val out: MutableList<Event?> = ArrayList<Event?>()
        synchronized(lock) {
            val start = if (count < capacity) 0 else head
            for (i in 0..<count) {
                val e = buffer[(start + i) % capacity]
                if (e != null && e.flowFileId == flowFileId) out.add(e)
            }
        }
        return out
    }

    /** Most recent N events across every FlowFile, oldest first within
     * the window. If fewer than N are available all are returned. */
    fun getRecent(n: Int): MutableList<Event?> {
        if (n <= 0) return mutableListOf<Event?>()
        val out: MutableList<Event?> = ArrayList<Event?>(min(n, capacity))
        synchronized(lock) {
            val take = min(n, count)
            val start = ((head - take) % capacity + capacity) % capacity
            for (i in 0..<take) {
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

        override fun configKeys(): MutableList<String?> {
            return mutableListOf<String?>("buffer")
        }

        override fun create(config: MutableMap<String?, Any?>): Provider {
            val buf = config.get("buffer")
            val capacity = if (buf is Number) buf.toInt() else DEFAULT_CAPACITY
            return ProvenanceProvider(capacity)
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
