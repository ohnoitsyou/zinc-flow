package zincflow.fabric

/** Typed description of a single processor config parameter. Drives the UI
 * form so visual programming can show proper typed inputs instead of raw
 * key/value text boxes.
 * 
 * <h2>Default semantics</h2>
 * 
 *  * `defaultValue == null` → "no default, leave blank"
 *  * `defaultValue == ""` → "default is the empty string"
 * 
 * The UI preserves this distinction when seeding form state. */
// TODO: Fix this to use a delegated constructor of some kind
class ParamInfo(
    @JvmField val name: String?,
    label: String?,
    description: String?,
    kind: ParamKind?,
    @JvmField val required: Boolean,
    @JvmField val defaultValue: String?,
    @JvmField val placeholder: String?,
    choices: List<String>?,
    @JvmField val valueKind: ParamKind?,
    entryDelim: String?,
    pairDelim: String?
) {
    class Builder private constructor(private val name: String?) {
        private var label: String?
        private var description = ""
        private var kind: ParamKind? = ParamKind.STRING
        private var required = false
        private var defaultValue: String? = null
        private var placeholder: String? = null
        private var choices: List<String>? = null
        private var valueKind: ParamKind? = null
        private var entryDelim: String? = ";"
        private var pairDelim: String? = "="

        init {
            this.label = name
        }

        fun label(v: String?): Builder = apply{
            this.label = v
        }

        fun description(v: String?): Builder = apply {
            this.description = v ?: ""
        }

        fun kind(v: ParamKind?): Builder = apply {
            this.kind = v
        }

        fun required(): Builder = apply {
            this.required = true
        }

        fun required(v: Boolean): Builder = apply {
            this.required = v
        }

        fun defaultValue(v: String?): Builder = apply {
            this.defaultValue = v
        }

        fun placeholder(v: String?): Builder = apply{
            this.placeholder = v
        }

        fun choices(vararg v: String?): Builder = apply{
            this.choices = listOfNotNull(*v)
        }

        fun valueKind(v: ParamKind?): Builder  = apply {
            this.valueKind = v
        }

        fun entryDelim(v: String?): Builder = apply {
            this.entryDelim = v
        }

        fun pairDelim(v: String?): Builder = apply {
            this.pairDelim = v
        }

        fun build(): ParamInfo {
            return ParamInfo(
                name, label, description, kind, required,
                defaultValue, placeholder, choices, valueKind, entryDelim, pairDelim
            )
        }
        companion object {
            operator fun invoke(name: String?): Builder {
                return ParamInfo.Builder(name)
            }
        }
    }

    val label: String?
    val description: String?
    val kind: ParamKind?
    val choices: List<String>?
    val entryDelim: String?
    val pairDelim: String?

    init {
        var label = label
        var description = description
        var kind = kind
        var choices = choices
        var entryDelim = entryDelim
        var pairDelim = pairDelim
        require(!name.isNullOrEmpty()) { "ParamInfo.name must be non-empty" }
        if (label == null) label = name
        if (description == null) description = ""
        if (kind == null) kind = ParamKind.STRING
        if (choices != null) choices = choices.toList()
        if (entryDelim == null) entryDelim = ";"
        if (pairDelim == null) pairDelim = "="
        this.label = label
        this.description = description
        this.kind = kind
        this.choices = choices
        this.entryDelim = entryDelim
        this.pairDelim = pairDelim
    }

    companion object {
        /** Concise builder for the common case. Required-param variant is
         * [.required]. */
        fun of(name: String?): Builder {
            return Builder(name)
        }

        fun simple(name: String?): ParamInfo {
            return Builder(name).build()
        }
    }
}

fun ParamInfo.asMap() : Map<String, Any>{
    return buildMap {
        put("name", name ?: "")
        put("label", label ?: "")
        put("description", description ?: "")
        put("kind", kind?.jsonName() ?: "")
        put("required", required)
        put("default", defaultValue ?: "")
        put("placeholder", placeholder ?: "")
        put("choices", choices ?: listOf<String>())
        put("valueKind", valueKind?.jsonName() ?: "")
        put("entryDelim", entryDelim ?: "")
        put("pairDelim", pairDelim ?: "")
    }
}
