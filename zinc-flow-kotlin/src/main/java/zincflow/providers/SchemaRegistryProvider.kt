package zincflow.providers

import zincflow.core.ComponentState
import zincflow.core.Provider
import zincflow.core.ProviderPlugin
import java.util.Collections
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.Volatile

/** Embedded, airgapped schema registry behind the [Provider]
 * interface. Matches Confluent semantics closely enough that the
 * [zincflow.fabric.SchemaRegistryHandler] can expose a
 * drop-in-compatible REST surface.
 * 
 * <h2>ID model</h2>
 * Every unique schema definition is assigned a single global integer
 * id on first registration. Registering the same definition under a
 * different subject reuses the existing id. This matches Confluent's
 * "one id per schema" model — tooling that resolves schemas by id
 * (most Avro-serializer clients) works verbatim.
 * 
 * Per-subject version numbers are sequential starting at 1. Deleting
 * a version doesn't renumber later ones: if you delete v2 after
 * having v1, v2, v3, the remaining versions stay v1 + v3. */
class SchemaRegistryProvider : Provider {
    /** Immutable record of a registered schema. */
    @JvmRecord
    data class Schema(@JvmField val id: Int, @JvmField val subject: String, @JvmField val version: Int, @JvmField val definition: String) {
        init {
            require(subject.isNotEmpty()) { "schema subject must not be blank" }
            require(version >= 1) { "schema version must be >= 1" }
            require(id >= 1) { "schema id must be >= 1" }
            require(definition.isNotBlank()) { "schema definition must not be blank" }
        }
    }

    /** Map from subject → ordered list of [Schema]. Concurrent
     * access uses a per-subject monitor (the [List] itself) so
     * registrations under different subjects don't contend. */
    private val bySubject = ConcurrentHashMap<String, MutableList<Schema>>()

    /** Definition text → global id. Lets us dedupe schemas across
     * subjects and give back stable ids. */
    private val idByDefinition = ConcurrentHashMap<String, Int>()

    /** Global id → one representative Schema record (for id-based
     * lookup). The record's subject/version reflect the first
     * subject that registered the definition. */
    private val byId = ConcurrentHashMap<Int, Schema>()
    private val nextId = AtomicInteger()

    @Volatile
    private var state = ComponentState.DISABLED

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
        bySubject.clear()
        idByDefinition.clear()
        byId.clear()
    }

    /** Register a new schema version under `subject`. When the
     * definition is already known (under this or any other subject)
     * the existing global id is reused; the subject still gets a new
     * per-subject version entry so its history is preserved. */
    fun register(subject: String, definition: String): Schema {
        require(subject.isNotBlank()) { "subject must not be blank" }
        require(definition.isNotBlank()) { "definition must not be blank" }

        // Short-circuit: same definition already present under the same
        // subject → return the existing entry rather than bumping to
        // a new version. Matches Confluent's idempotent register behavior.
        val existing = findBySubjectAndDefinition(subject, definition)
        if (existing != null) return existing

        val id = idByDefinition.computeIfAbsent(definition) { nextId.incrementAndGet() }
        val versions = bySubject.computeIfAbsent(subject) {
            Collections.synchronizedList(ArrayList<Schema>())
        }
        val registered = synchronized(versions) {
            val nextVersion = if (versions.isEmpty()) 1 else versions.get(versions.size - 1).version + 1
            Schema(id, subject, nextVersion, definition).also { versions.add(it) }
        }
        byId.putIfAbsent(id, registered)
        return registered
    }

    private fun findBySubjectAndDefinition(subject: String, definition: String): Schema? {
        val versions = bySubject[subject] ?: return null
        return synchronized(versions) {
            versions.firstOrNull { it.definition == definition }
        }
    }

    fun getById(id: Int): Optional<Schema> {
        return Optional.ofNullable<Schema>(byId[id])
    }

    fun getEntry(subject: String?, version: Int): Optional<Schema> {
        val versions = bySubject[subject] ?: return Optional.empty<Schema>()
        val item = synchronized(versions) {
            versions.firstOrNull { it.version == version }
        }
        return Optional.ofNullable(item)
    }

    fun latest(subject: String): Optional<Schema> {
        val versions = bySubject[subject] ?: return Optional.empty<Schema>()
        if(versions.isEmpty()) return Optional.empty<Schema>()
        return synchronized(versions) {
            Optional.of<Schema>(versions.last())
        }
    }

    fun listSubjects(): List<String> {
        return bySubject.keys.sorted()
    }

    fun listVersions(subject: String): List<Int> {
        val versions = bySubject[subject] ?: return listOf()
        return synchronized(versions) {
            versions.map { it.version }
        }
    }

    /** Delete every version under `subject` and return the
     * version numbers that were removed (so the HTTP layer can echo
     * them back the way Confluent does). Returns empty when the
     * subject is unknown. */
    fun deleteSubject(subject: String): List<Int> {
        val versions = bySubject.remove(subject) ?: return listOf()
        return synchronized(versions) {
            versions.map { it.version }
        }
    }

    fun deleteVersion(subject: String?, version: Int): Boolean {
        val versions = bySubject[subject] ?: return false
        return synchronized(versions) {
            versions.removeIf { it.version == version }
        }
    }

    /** Snapshot for diagnostics — returns a subject → versions map
     * that's safe to iterate without holding the per-subject lock. */
    fun snapshot(): Map<String, List<Int>> {
        return listSubjects().associateWith {
            listVersions(it)
        }
    }

    fun size(): Int {
        return nextId.get()
    }

    class Plugin : ProviderPlugin {
        override fun providerType(): String {
            return TYPE
        }

        override fun description(): String {
            return "Embedded Confluent-shape schema registry."
        }

        override fun create(config: Map<String, Any>): Provider {
            return SchemaRegistryProvider()
        }
    }

    companion object {
        const val NAME: String = "schema_registry"
        const val TYPE: String = "SchemaRegistryProvider"
    }
}
