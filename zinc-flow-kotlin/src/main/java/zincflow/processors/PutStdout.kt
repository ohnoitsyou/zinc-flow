package zincflow.processors

import zincflow.core.ContentResolver
import zincflow.core.ContentStore
import zincflow.core.FlowFile
import zincflow.core.Processor
import zincflow.core.ProcessorResult
import zincflow.fabric.FlowFileV3
import java.nio.charset.StandardCharsets
import java.util.HexFormat
import java.util.Locale
import kotlin.math.min

/** Writes the FlowFile's payload to `System.out` and terminates
 * the branch with Dropped. `format` picks the rendering:
 * 
 *  * `raw` (default) — UTF-8 text
 *  * `hex` — first 128 bytes hex-encoded
 *  * `v3` — pack + hex-dump the V3 framing (useful for
 * debugging V3 interop)
 * 
 * Claim-backed content resolves through the supplied
 * [ContentStore]. */
class PutStdout @JvmOverloads constructor(
    private val prefix: String? = "",
    format: String? = "raw",
    private val store: ContentStore? = null
) : Processor {
    private val format: Format = Format.parse(format)

    override fun process(ff: FlowFile): ProcessorResult {
        val resolved = ContentResolver.resolve(ff.content, store)
        if (!resolved.ok()) {
            return ProcessorResult.Failure("PutStdout: " + resolved.error, ff)
        }
        val bytes = resolved.bytes
        when (format) {
            Format.RAW -> println(prefix + String(bytes, StandardCharsets.UTF_8))
            Format.HEX -> println(
                (prefix + "(" + bytes.size + " bytes) "
                        + HexFormat.of().formatHex(bytes, 0, min(bytes.size, 128)))
            )

            Format.V3 -> {
                val packed = FlowFileV3.pack(ff, bytes)
                println(
                    (prefix + "v3 (" + packed.size + " bytes) "
                            + HexFormat.of().formatHex(packed, 0, min(packed.size, 128)))
                )
            }
        }
        return ProcessorResult.Dropped()
    }

    private enum class Format {
        RAW, HEX, V3;

        companion object {
            fun parse(s: String?): Format {
                if (s.isNullOrBlank()) return RAW
                return when (s.lowercase(Locale.getDefault())) {
                    "raw", "text" -> RAW
                    "hex" -> HEX
                    "v3" -> V3
                    else -> throw IllegalArgumentException("PutStdout: format must be raw/hex/v3, got '$s'")
                }
            }
        }
    }
}
