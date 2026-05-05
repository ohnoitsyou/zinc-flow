package zincflow.providers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import zincflow.core.ComponentState
import zincflow.core.Provider
import zincflow.core.ProviderPlugin
import kotlin.concurrent.Volatile

/** Structured-logging façade over slf4j. Processors pull a named child
 * logger via [.logger] and get the provider's enable-gate
 * for free — logs are suppressed when the provider is disabled, which
 * matches zinc-flow-csharp's "turn off chatty logging in prod" model. */
class LoggingProvider : Provider {
    private val rootLogger: Logger = LoggerFactory.getLogger("zincflow")

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

    fun logger(child: String = "app"): Logger {
        return LoggerFactory.getLogger("zincflow.${child}")
    }

    /** Gated info log — drops when disabled. Use for high-volume,
     * processor-level diagnostics that should be toggle-able. */
    fun info(processor: String?, msg: String?, context: Map<String, Any> = mapOf()) {
        if (!isEnabled) return
        rootLogger.info("proc={} msg={} ctx={}", processor, msg, context)
    }

    fun warn(processor: String?, msg: String?, context: Map<String, Any> = mapOf()) {
        if (!isEnabled) return
        rootLogger.warn("proc={} msg={} ctx={}", processor, msg, context)
    }

    fun error(processor: String?, msg: String?, cause: Throwable?) {
        if (!isEnabled) return
        rootLogger.error("proc={} msg={}", processor, msg, cause)
    }

    class Plugin : ProviderPlugin {
        override fun providerType(): String {
            return TYPE
        }

        override fun description(): String {
            return "Structured logging facade over slf4j."
        }

        override fun create(config: Map<String, Any>): Provider {
            return LoggingProvider()
        }
    }

    companion object {
        const val NAME: String = "logging"
        const val TYPE: String = "LoggingProvider"

        /** Convenience factory — a pre-enabled instance, suitable as the
         * default when no external provider is wired. The returned
         * provider logs immediately; toggle it via [.disable]
         * if you want to mute it. */
        @JvmStatic
        fun enabled(): LoggingProvider {
            val p = LoggingProvider()
            p.enable()
            return p
        }
    }
}
