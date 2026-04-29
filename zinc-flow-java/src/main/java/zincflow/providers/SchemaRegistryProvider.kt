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
    data class Schema(@JvmField val id: Int, @JvmField val subject: String?, @JvmField val version: Int, @JvmField val definition: String?) {
        init {
            require(!(subject == null || subject.isEmpty())) { "schema subject must not be blank" }
            require(version >= 1) { "schema version must be >= 1" }
            require(id >= 1) { "schema id must be >= 1" }
            requireNotNull(definition) { "schema definition must not be null" }
        }
    }

    /** Map from subject → ordered list of [Schema]. Concurrent
     * access uses a per-subject monitor (the [List] itself) so
     * registrations under different subjects don't contend. */
    private val bySubject = ConcurrentHashMap<String?, MutableList<Schema>>()

    /** Definition text → global id. Lets us dedupe schemas across
     * subjects and give back stable ids. */
    private val idByDefinition = ConcurrentHashMap<String?, Int?>()

    /** Global id → one representative Schema record (for id-based
     * lookup). The record's subject/version reflect the first
     * subject that registered the definition. */
    private val byId = ConcurrentHashMap<Int?, Schema?>()
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
        require(!(subject == null || subject.isEmpty())) { "subject must not be blank" }
        requireNotNull(definition) { "definition must not be null" }

        // Short-circuit: same definition already present under the same
        // subject → return the existing entry rather than bumping to
        // a new version. Matches Confluent's idempotent register behavior.
        val existing = findBySubjectAndDefinition(subject, definition)
        if (existing != null) return existing

        val id = idByDefinition.computeIfAbsent(definition) { `_`: kotlin.String? -> nextId.incrementAndGet() }!!
        val versions = bySubject.computeIfAbsent(subject) { `_`: String? ->
            Collections.synchronizedList<Schema?>(
                ArrayList<Schema?>()
            )
        }
        val registered: Schema
        synchronized(versions) {
            val nextVersion = if (versions.isEmpty()) 1 else versions.get(versions.size - 1).version + 1
            registered = Schema(id, subject, nextVersion, definition)
            versions.add(registered)
        }
        byId.putIfAbsent(id, registered)
        return registered
    }

    private fun findBySubjectAndDefinition(subject: String?, definition: String?): Schema? {
        val versions = bySubject.get(subject)
        if (versions == null) return null
        synchronized(versions) {
            for (s in versions) {
                if (s.definition == definition) return s
            }
        }
        return null
    }

    fun getById(id: Int): Optional<Schema?> {
        return Optional.ofNullable<Schema?>(byId.get(id))
    }

    fun getEntry(subject: String?, version: Int): Optional<Schema?> {
        val versions = bySubject.get(subject)
        if (versions == null) return Optional.empty<Schema?>()
        synchronized(versions) {
            for (s in versions) if (s.version == version) return Optional.of<Schema?>(s)
        }
        return Optional.empty<Schema?>()
    }

    fun latest(subject: String?): Optional<Schema?> {
        val versions = bySubject.get(subject)
        if (versions == null) return Optional.empty<Schema?>()
        synchronized(versions) {
            return if (versions.isEmpty()) Optional.empty<Schema?>() else Optional.of<Schema?>(versions.get(versions.size - 1))
        }
    }

    fun listSubjects(): MutableList<String?> {
        val out: MutableList<String?> = ArrayList<String?>(bySubject.keys)
        Collections.sort<String?>(out)
        return out
    }

    fun listVersions(subject: String?): MutableList<Int?> {
        val versions = bySubject.get(subject)
        if (versions == null) return mutableListOf<Int?>()
        synchronized(versions) {
            val out: MutableList<Int?> = ArrayList<Int?>(versions.size)
            for (s in versions) out.add(s.version)
            return out
        }
    }

    /** Delete every version under `subject` and return the
     * version numbers that were removed (so the HTTP layer can echo
     * them back the way Confluent does). Returns empty when the
     * subject is unknown. */
    fun deleteSubject(subject: String): MutableList<Int?> {
        val versions = bySubject.remove(subject)
        if (versions == null) return mutableListOf<Int?>()
        synchronized(versions) {
            val removed: MutableList<Int?> = ArrayList<Int?>(versions.size)
            for (s in versions) removed.add(s.version)
            return removed
        }
    }

    fun deleteVersion(subject: String?, version: Int): Boolean {
        val versions = bySubject.get(subject)
        if (versions == null) return false
        synchronized(versions) {
            return versions.removeIf { s: Schema? -> s!!.version == version }
        }
    }

    /** Snapshot for diagnostics — returns a subject → versions map
     * that's safe to iterate without holding the per-subject lock. */
    fun snapshot(): MutableMap<String?, MutableList<Int?>?> {
        val out: MutableMap<String?, MutableList<Int?>?> = LinkedHashMap<String?, MutableList<Int?>?>()
        for (subject in listSubjects()) out.put(subject, listVersions(subject))
        return out
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

        override fun create(config: MutableMap<String?, Any?>?): Provider {
            return SchemaRegistryProvider()
        }
    }

    companion object {
        const val NAME: String = "schema_registry"
        const val TYPE: String = "SchemaRegistryProvider"
    }
}
