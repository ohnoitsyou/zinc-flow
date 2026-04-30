package zincflow.fabric

import zincflow.core.FlowFile
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/** NiFi FlowFile V3 binary wire format — pack / unpack pairs for the
 * `NiFiFF3` magic, 2-byte (extended to 6-byte) length prefixes,
 * and 8-byte big-endian content length. Mirrors zinc-flow-csharp's
 * FlowFileV3 so Java and C# backends interchange FlowFiles byte-exact.
 * 
 * Format:
 * <pre>
 * magic: "NiFiFF3" (7 bytes)
 * attrCount: field-length (2 or 6 bytes)
 * for each attribute:
 * keyLen: field-length
 * key:    UTF-8
 * valLen: field-length
 * value:  UTF-8
 * contentLen: int64 big-endian (8 bytes)
 * content:    raw bytes
</pre> * 
 * Field-length: 2 bytes unsigned when value &lt; 0xFFFF, otherwise
 * `0xFFFF` sentinel + 4 bytes unsigned int. */
object FlowFileV3 {
    @JvmField
    val MAGIC: ByteArray = "NiFiFF3".toByteArray(StandardCharsets.US_ASCII)
    @JvmField
    val MAGIC_LEN: Int = MAGIC.size

    private const val MAX_VALUE_2_BYTES = 0xFFFF

    // --- Pack ---
    @JvmStatic
    fun pack(ff: FlowFile, contentBytes: ByteArray?): ByteArray {
        var contentBytes = contentBytes
        if (contentBytes == null) contentBytes = ByteArray(0)
        val out = ByteArrayOutputStream(MAGIC_LEN + 256 + contentBytes.size)
        out.writeBytes(MAGIC)

        val attrs = ff.attributes
        writeFieldLength(out, attrs.size)
        for (entry in attrs.entries) {
            val k = entry.key!!.toByteArray(StandardCharsets.UTF_8)
            val v = if (entry.value == null)
                ByteArray(0)
            else
                entry.value!!.toByteArray(StandardCharsets.UTF_8)
            writeFieldLength(out, k.size)
            out.writeBytes(k)
            writeFieldLength(out, v.size)
            out.writeBytes(v)
        }

        val lenBuf = ByteArray(8)
        writeInt64BE(lenBuf, 0, contentBytes.size.toLong())
        out.writeBytes(lenBuf)
        out.writeBytes(contentBytes)
        return out.toByteArray()
    }

    fun packMultiple(flowFiles: MutableList<FlowFile?>, contents: MutableList<ByteArray?>): ByteArray {
        require(flowFiles.size == contents.size) {
            ("flowFiles.size() (" + flowFiles.size + ") must equal contents.size() ("
                    + contents.size + ")")
        }
        val out = ByteArrayOutputStream()
        for (i in flowFiles.indices) {
            out.writeBytes(FlowFileV3.pack(flowFiles.get(i)!!, contents.get(i)))
        }
        return out.toByteArray()
    }

    // --- Unpack ---
    @JvmStatic
    fun unpack(data: ByteArray, offset: Int): UnpackResult {
        if (offset < 0 || offset + MAGIC_LEN > data.size) {
            return UnpackResult(null, offset, "buffer too small for FlowFile V3 magic at offset $offset")
        }
        for (i in 0..<MAGIC_LEN) {
            if (data[offset + i] != MAGIC[i]) {
                return UnpackResult(null, offset, "invalid FlowFile V3 magic at offset $offset")
            }
        }
        data.slice(0 .. MAGIC_LEN)
        var pos = offset + MAGIC_LEN

        val ref = IntArray(1)
        val count = readFieldLength(data, pos, ref)
        pos = ref[0]

        val attrs = mutableMapOf<String, String>()
        for (i in 0..<count) {
            val keyLen = readFieldLength(data, pos, ref)
            pos = ref[0]
            if (pos + keyLen > data.size) {
                return UnpackResult(null, offset, "truncated key at attribute $i")
            }
            val key = String(data, pos, keyLen, StandardCharsets.UTF_8)
            pos += keyLen

            val valLen = readFieldLength(data, pos, ref)
            pos = ref[0]
            if (pos + valLen > data.size) {
                return UnpackResult(null, offset, "truncated value at attribute '$key'")
            }
            val `val` = String(data, pos, valLen, StandardCharsets.UTF_8)
            pos += valLen

            attrs.put(key, `val`)
        }

        if (pos + 8 > data.size) {
            return UnpackResult(null, offset, "truncated content-length header")
        }
        val contentLen = readInt64BE(data, pos)
        pos += 8
        if (contentLen < 0 || pos + contentLen > data.size) {
            return UnpackResult(null, offset, "content length $contentLen overruns buffer")
        }
        val content = ByteArray(contentLen.toInt())
        System.arraycopy(data, pos, content, 0, content.size)
        pos += content.size

        val ff = FlowFile.create(content, attrs)
        return UnpackResult(ff, pos, "")
    }

    /** Decode every FlowFile in the buffer; stops on the first unpack
     * error without throwing (mirrors the C# behavior — partial streams
     * surface whatever was well-formed). Returns empty if the magic is
     * missing at offset 0. */
    @JvmStatic
    fun unpackAll(data: ByteArray): MutableList<FlowFile> {
        val out = mutableListOf<FlowFile>()
        var pos = 0
        while (pos < data.size) {
            val result = unpack(data, pos)
            if (!result.ok() || result.flowFile == null) break
            out.add(result.flowFile)
            pos = result.nextOffset
        }
        return out
    }

    // --- Length + int helpers ---
    private fun writeFieldLength(out: ByteArrayOutputStream, value: Int) {
        if (value < MAX_VALUE_2_BYTES) {
            out.write((value ushr 8) and 0xFF)
            out.write(value and 0xFF)
        } else {
            out.write(0xFF)
            out.write(0xFF)
            out.write((value ushr 24) and 0xFF)
            out.write((value ushr 16) and 0xFF)
            out.write((value ushr 8) and 0xFF)
            out.write(value and 0xFF)
        }
    }

    private fun readFieldLength(data: ByteArray, offset: Int, nextOut: IntArray): Int {
        val high = data[offset].toInt() and 0xFF
        val low = data[offset + 1].toInt() and 0xFF
        val value = (high shl 8) or low
        if (value < MAX_VALUE_2_BYTES) {
            nextOut[0] = offset + 2
            return value
        }
        val ext = (((data[offset + 2].toInt() and 0xFF) shl 24)
                or ((data[offset + 3].toInt() and 0xFF) shl 16)
                or ((data[offset + 4].toInt() and 0xFF) shl 8)
                or (data[offset + 5].toInt() and 0xFF))
        nextOut[0] = offset + 6
        return ext
    }

    private fun writeInt64BE(buf: ByteArray, offset: Int, value: Long) {
        ByteBuffer.wrap(buf, offset, 8).putLong(value)
    }

    private fun readInt64BE(buf: ByteArray, offset: Int): Long {
        return ByteBuffer.wrap(buf, offset, 8).getLong()
    }

    @JvmRecord
    data class UnpackResult(@JvmField val flowFile: FlowFile?, @JvmField val nextOffset: Int, @JvmField val error: String?) {
        fun ok(): Boolean {
            return error == null || error.isEmpty()
        }
    }
}
