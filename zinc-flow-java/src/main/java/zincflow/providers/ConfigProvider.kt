package zincflow.providers

import zincflow.core.ComponentState
import zincflow.core.Provider
import zincflow.core.ProviderPlugin
import kotlin.concurrent.Volatile

/** Read-only dot-path access to a configuration map — usually the full
 * config.yaml document. Processors pull typed values through
 * `getString/getInt/getBool` helpers; null-safe on missing keys. */
class ConfigProvider(private val config: Map<String, Any> = mapOf()) : Provider {

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
    }

    /** Resolve a dotted path against the nested config map. Returns
     * null for any missing segment or non-map intermediate value. */
    fun get(dottedPath: String?): Any? {
        if (dottedPath.isNullOrEmpty()) return null
        var cur: Any? = config
        for (part in dottedPath.split(".").mapNotNull { p -> p.trim().takeIf { it.isNotEmpty() } }) {
            if (cur !is MutableMap<*, *>) return null
            cur = cur[part]
            if (cur == null) return null
        }
        return cur
    }

    fun getString(path: String?, defaultValue: String?): String? {
        val v = get(path)
        return v?.toString() ?: defaultValue
    }

    fun getInt(path: String?, defaultValue: Int): Int {
        val v = get(path)
        if (v is Number) return v.toInt()
        if (v == null) return defaultValue
        return try {
            v.toString().toInt()
        } catch (_: NumberFormatException) {
            defaultValue
        }
    }

    fun getBool(path: String?, defaultValue: Boolean): Boolean {
        val v = get(path)
        if (v is Boolean) return v
        if (v == null) return defaultValue
        return v.toString().toBoolean()
    }

    class Plugin : ProviderPlugin {
        override fun providerType(): String {
            return TYPE
        }

        override fun description(): String {
            return "Dot-path accessor over the layered config map."
        }

        override fun create(config: Map<String, Any>): Provider {
            return ConfigProvider(config)
        }
    }

    companion object {
        const val NAME: String = "config"
        const val TYPE: String = "ConfigProvider"
    }
}
