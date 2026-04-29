package zincflow.providers

import zincflow.core.ComponentState
import zincflow.core.ContentStore
import zincflow.core.FileContentStore
import zincflow.core.MemoryContentStore
import zincflow.core.Provider
import zincflow.core.ProviderPlugin
import java.io.IOException
import java.nio.file.Path
import kotlin.concurrent.Volatile

/** Wraps a [ContentStore] as a [Provider] so processors can
 * acquire it through the [zincflow.core.ProcessorContext] and
 * share one store per Fabric. The provider name is parameterized so
 * multiple stores (in-memory for small-payload pipelines, on-disk for
 * large-payload pipelines) can coexist under distinct names. */
class ContentProvider(private val name: String, private val store: ContentStore) : Provider {
    @Volatile
    private var state = ComponentState.DISABLED

    constructor(store: ContentStore) : this(NAME, store)

    init {
        require(name.isNotEmpty()) { "name must not be blank" }
    }

    fun store(): ContentStore {
        return store
    }

    override fun name(): String {
        return name
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

    /** Build the configured content store: file-backed when
     * `store: file, directory: /path` is set, otherwise in-memory. */
    class Plugin : ProviderPlugin {
        override fun providerType(): String {
            return TYPE
        }

        override fun description(): String {
            return "FlowFile content store (memory or disk-backed)."
        }

        override fun configKeys(): MutableList<String?> {
            return mutableListOf<String?>("store", "directory")
        }

        override fun create(config: MutableMap<String?, Any>): Provider {
            val kind: String? = if (config.get("store") == null) "memory" else config.get("store").toString()
            if ("file".equals(kind, ignoreCase = true)) {
                val dir: Any = config.get("directory")!!
                requireNotNull(dir) { "ContentProvider: file store requires 'directory'" }
                try {
                    return ContentProvider(FileContentStore(Path.of(dir.toString())))
                } catch (ex: IOException) {
                    throw IllegalStateException(
                        "ContentProvider: failed to init file store at " + dir, ex
                    )
                }
            }
            return ContentProvider(MemoryContentStore())
        }
    }

    companion object {
        /** Conventional name — used by processors that don't want to hard-code
         * a provider name. Matches the C# default. */
        const val NAME: String = "content"
        const val TYPE: String = "ContentProvider"
    }
}
