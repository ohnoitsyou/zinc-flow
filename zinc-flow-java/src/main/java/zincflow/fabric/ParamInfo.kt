package zincflow.fabric

import java.util.List

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
class ParamInfo(
    @JvmField val name: String?,
    label: String?,
    description: String?,
    kind: ParamKind?,
    @JvmField val required: Boolean,
    @JvmField val defaultValue: String?,
    @JvmField val placeholder: String?,
    choices: MutableList<String?>?,
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
        private var choices: MutableList<String?>? = null
        private var valueKind: ParamKind? = null
        private var entryDelim: String? = ";"
        private var pairDelim: String? = "="

        init {
            this.label = name
        }

        fun label(v: String?): Builder {
            this.label = v
            return this
        }

        fun description(v: String?): Builder {
            this.description = if (v == null) "" else v
            return this
        }

        fun kind(v: ParamKind?): Builder {
            this.kind = v
            return this
        }

        fun required(): Builder {
            this.required = true
            return this
        }

        fun required(v: Boolean): Builder {
            this.required = v
            return this
        }

        fun defaultValue(v: String?): Builder {
            this.defaultValue = v
            return this
        }

        fun placeholder(v: String?): Builder {
            this.placeholder = v
            return this
        }

        fun choices(vararg v: String?): Builder {
            this.choices = List.of<String?>(*v)
            return this
        }

        fun valueKind(v: ParamKind?): Builder {
            this.valueKind = v
            return this
        }

        fun entryDelim(v: String?): Builder {
            this.entryDelim = v
            return this
        }

        fun pairDelim(v: String?): Builder {
            this.pairDelim = v
            return this
        }

        fun build(): ParamInfo {
            return ParamInfo(
                name, label, description, kind, required,
                defaultValue, placeholder, choices, valueKind, entryDelim, pairDelim
            )
        }
    }

    val label: String?
    val description: String?
    val kind: ParamKind?
    val choices: MutableList<String?>?
    val entryDelim: String?
    val pairDelim: String?

    init {
        var label = label
        var description = description
        var kind = kind
        var choices = choices
        var entryDelim = entryDelim
        var pairDelim = pairDelim
        require(!(name == null || name.isEmpty())) { "ParamInfo.name must be non-empty" }
        if (label == null) label = name
        if (description == null) description = ""
        if (kind == null) kind = ParamKind.STRING
        if (choices != null) choices = List.copyOf<String?>(choices)
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
            return ParamInfo.Builder(name)
        }

        fun simple(name: String?): ParamInfo {
            return ParamInfo.Builder(name).build()
        }
    }
}
