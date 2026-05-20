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
class ParamInfo private constructor(
    @JvmField val name: String,
    val label: String,
    val description: String,
    val kind: ParamKind,
    @JvmField val required: Boolean,
    @JvmField val defaultValue: String,
    @JvmField val placeholder: String,
    val choices: List<String>,
    @JvmField val valueKind: ParamKind,
    val entryDelim: String,
    val pairDelim: String
) {
    class Builder(private val name: String) {
        private var label: String = name
        private var description = ""
        private var kind: ParamKind = ParamKind.STRING
        private var required = false
        private var defaultValue: String? = null
        private var placeholder: String? = null
        private var choices: List<String>? = null
        private var valueKind: ParamKind? = null
        private var entryDelim: String? = DEFAULT_ENTRY_DELIM
        private var pairDelim: String? = DEFAULT_PAIR_DELIM

        fun label(label: String): Builder = apply{
            this.label = label
        }

        fun description(description: String): Builder = apply {
            this.description = description
        }

        fun kind(kind: ParamKind): Builder = apply {
            this.kind = kind
        }

        fun required(): Builder = apply {
            this.required = true
        }

        fun required(required: Boolean): Builder = apply {
            this.required = required
        }

        fun defaultValue(defaultValue: String): Builder = apply {
            this.defaultValue = defaultValue
        }

        fun placeholder(placeholder: String): Builder = apply{
            this.placeholder = placeholder
        }

        fun choices(vararg choices: String): Builder = apply{
            this.choices = listOfNotNull(*choices)
        }

        fun valueKind(valueKind: ParamKind): Builder  = apply {
            this.valueKind = valueKind
        }

        fun entryDelim(entryDelim: String): Builder = apply {
            this.entryDelim = entryDelim
        }

        fun pairDelim(pairDelim: String): Builder = apply {
            this.pairDelim = pairDelim
        }

        fun build(): ParamInfo {
            return ParamInfo(
                name,
                label,
                description,
                kind,
                required,
                defaultValue ?: "",
                placeholder ?: "",
                choices ?: emptyList(),
                valueKind ?: ParamKind.STRING,
                entryDelim ?: DEFAULT_ENTRY_DELIM,
                pairDelim ?: DEFAULT_PAIR_DELIM
            )
        }
        companion object {
            private const val DEFAULT_ENTRY_DELIM = ";"
            private const val DEFAULT_PAIR_DELIM = "="
        }
    }

    init {
        require(name.isNotEmpty()) { "ParamInfo.name must be non-empty" }
    }

    companion object {
        /** Concise builder for the common case. Required-param variant is
         * [.required]. */
        fun of(name: String): Builder {
            return Builder(name)
        }

        fun simple(name: String): ParamInfo {
            return Builder(name).build()
        }
    }
}

fun ParamInfo.asMap() : Map<String, Any>{
    return buildMap {
        put("name", name)
        put("label", label)
        put("description", description)
        put("kind", kind.jsonName)
        put("required", required)
        put("default", defaultValue)
        put("placeholder", placeholder)
        put("choices", choices)
        put("valueKind", valueKind.jsonName)
        put("entryDelim", entryDelim)
        put("pairDelim", pairDelim)
    }
}
