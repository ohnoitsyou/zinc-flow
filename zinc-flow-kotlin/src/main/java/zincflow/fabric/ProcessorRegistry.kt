package zincflow.fabric

import zincflow.core.ContentStore
import zincflow.core.Processor
import zincflow.core.ProcessorContext
import zincflow.processors.ConvertAvroToRecord
import zincflow.processors.ConvertCSVToRecord
import zincflow.processors.ConvertJSONToRecord
import zincflow.processors.ConvertOCFToRecord
import zincflow.processors.ConvertRecordToAvro
import zincflow.processors.ConvertRecordToCSV
import zincflow.processors.ConvertRecordToJSON
import zincflow.processors.ConvertRecordToOCF
import zincflow.processors.EvaluateExpression
import zincflow.processors.ExtractRecordField
import zincflow.processors.ExtractText
import zincflow.processors.FilterAttribute
import zincflow.processors.LogAttribute
import zincflow.processors.PackageFlowFileV3
import zincflow.processors.PutFile
import zincflow.processors.PutHTTP
import zincflow.processors.PutStdout
import zincflow.processors.QueryRecord
import zincflow.processors.ReplaceText
import zincflow.processors.RouteOnAttribute
import zincflow.processors.RouteRecord
import zincflow.processors.SplitRecord
import zincflow.processors.SplitText
import zincflow.processors.TransformRecord
import zincflow.processors.UnpackageFlowFileV3
import zincflow.processors.UpdateAttribute
import zincflow.processors.UpdateRecord
import zincflow.providers.ContentProvider
import zincflow.providers.LoggingProvider
import zincflow.toIntOrDefault
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kotlin.Boolean
import kotlin.Comparator
import kotlin.Deprecated
import kotlin.Int
import kotlin.NumberFormatException
import kotlin.String
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/** Processor registry — maps the `type:` value in config.yaml to
 * a factory that instantiates the processor given its config map plus
 * a [ProcessorContext]. Factories that need shared infrastructure
 * (content store, logger) pull it from the context; factories that
 * don't simply ignore the ctx argument.
 * 
 * <h2>Versioning</h2>
 * Every factory is registered with both a `name` and a
 * `version`. Config can pin a specific version via
 * `type: Foo@1.2.0`; omitting the version picks the latest
 * registered version for that name. Default version is
 * {@value #DEFAULT_VERSION}.
 * 
 * Stateless; one instance shared by the Fabric. */
class ProcessorRegistry {

    /** Keyed by "name@version" for pinned lookup. */
    private val versioned = ConcurrentHashMap<String, Factory>()

    /** Keyed by "name" → latest version string; lets unqualified
     * lookups resolve to the most recent release. */
    private val latestVersion = ConcurrentHashMap<String, String>()

    /** Metadata mirror of [.versioned], keyed the same way. */
    private val metadata = ConcurrentHashMap<String, TypeInfo>()

    init {
        registerBuiltins()
    }

    /** A factory for a processor given its config map and the surrounding
     * processor context. The context is never null — callers that don't
     * care can pass `new ProcessorContext()` and ignore it inside
     * the factory. */
    fun interface Factory {
        fun create(config: Map<String, String>, ctx: ProcessorContext): Processor?
    }

    /** Metadata about a registered processor type. The UI renders the
     * typed [.parameters] list in the "add processor" form; older
     * clients fall back to [.configKeys]. Category groups the
     * dropdown visually. */
    class TypeInfo @JvmOverloads constructor(
        val name: String,
        val version: String,
        val description: String,
        val configKeys: List<String> = listOf(),
        val relationships: List<String> = listOf(),
        val category: String = "Other",
        val parameters: List<ParamInfo> = configKeys.map { k: String -> ParamInfo.of(k).build() }
    ) {
        fun qualifiedName(): String {
            return "$name@$version"
        }
    }

    @JvmOverloads
    fun register(type: String, version: String = TypeRefs.DEFAULT_VERSION, factory: Factory) {
        register(TypeInfo(type, version, "", listOf(), listOf()), factory)
    }

    /** Full register — caller supplies the metadata the UI needs. */
    fun register(info: TypeInfo, factory: Factory) {
        val key = info.qualifiedName()
        versioned[key] = factory
        metadata[key] = info
        latestVersion.merge(info.name, info.version) { oldV: String, newV: String ->
            if (TypeRefs.compareVersions(oldV, newV) >= 0) oldV else newV
        }
    }

    fun has(type: String): Boolean {
        return resolveKey(type) != null
    }

    /** Resolve a user-facing type string (either `"Foo"` or
     * `"Foo@1.2.3"`) to the internal map key, or null when
     * the type is unknown. */
    private fun resolveKey(type: String): String? {
        return when {
            type.isEmpty() -> null
            type.contains("@") -> if (versioned.containsKey(type)) type else null
            latestVersion.containsKey(type) -> "$type@${latestVersion[type]}"
            else -> null
        }
    }

    /** Context-free convenience — callers that don't have a
     * [ProcessorContext] handy (e.g. ad-hoc tests) get an empty
     * one. Factories that actually consume the context will surface
     * their own error. */
    @JvmOverloads
    fun create(
        type: String,
        config: Map<String, String>,
        ctx: ProcessorContext = ProcessorContext()
    ): Processor? {
        val key = resolveKey(type) ?: throw IllegalArgumentException("$type is not defined")
        val f: Factory = versioned[key]!!
        return f.create(config, ctx)
    }

    /** All registered unqualified type names (latest only per name). */
    fun types(): MutableSet<String> {
        return latestVersion.keys.filterNotNull().toMutableSet()
    }

    /** Every registered (type, version) pair, sorted by name then
     * ascending version, with metadata. Used by
     * `GET /api/processor-types`. */
    fun listAll(): List<TypeInfo> {
        return metadata.values.sortedWith(Comparator
            .comparing(TypeInfo::name)
            .thenComparing(TypeInfo::version, Comparator { a: String, b: String -> TypeRefs.compareVersions(a, b) })
        )
    }

    /** Every version registered under a given unqualified name, or
     * empty when the name is unknown. */
    fun listVersions(type: String): List<TypeInfo> {
        return metadata.values
            .filter { it.name == type }
            .sortedWith(Comparator.comparing(TypeInfo::version, Comparator { a: String, b: String -> TypeRefs.compareVersions(a, b) }))
    }

    /** Latest [TypeInfo] for a name, null if unknown. */
    fun latest(type: String?): TypeInfo? {
        val latest = latestVersion[type] ?: return null
        return metadata["$type@$latest"]
    }

    /** Rich register — caller supplies a built TypeInfo (with category +
     * typed ParamInfo). Version defaults to {@value TypeRefs#DEFAULT_VERSION}. */
    private fun registerTyped(
        name: String,
        description: String,
        category: String,
        params: List<ParamInfo> = listOf(),
        relationships: List<String> = listOf(),
        factory: Factory
    ) {
        val configKeys = params.mapNotNull(ParamInfo::name)
        register(
            TypeInfo(name, TypeRefs.DEFAULT_VERSION, description, configKeys, relationships, category, params),
            factory
        )
    }

    private fun registerBuiltins() {
        // --- Attribute ---
        registerTyped("LogAttribute", "Logs FlowFile attributes and passes through", "Attribute",
            listOf(ParamInfo.of("prefix").description("Log line prefix").defaultValue("").build()),
            listOf("success")
        ) { cfg: Map<String, String>, ctx: ProcessorContext ->
            LogAttribute(cfg.getOrDefault("prefix", "" ), loggerFrom(ctx))
        }

        registerTyped("UpdateAttribute", "Sets key=value attribute on FlowFiles", "Attribute",
            listOf(
                ParamInfo.of("key").required().placeholder("env").build(),
                ParamInfo.of("value").required().placeholder("prod").build()
            ),
            listOf("success"),
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            UpdateAttribute(required(cfg, "key"), cfg.getOrDefault("value", "") )
        }

        registerTyped("RouteOnAttribute", "Route FlowFiles based on attribute predicates", "Routing",
            listOf(
                ParamInfo.of("routes").kind(ParamKind.MULTILINE).required()
                    .placeholder("premium: tier EQ premium; bulk: tier EQ bulk")
                    .description("semicolon-delimited 'name: attr OP value' entries; operators: EQ, NEQ, CONTAINS, STARTSWITH, ENDSWITH, EXISTS, GT, LT")
                    .build()
            ),
            listOf("unmatched")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            RouteOnAttribute(cfg.getOrDefault("routes", ""))
        }

        registerTyped("FilterAttribute", "Remove or keep specific attributes", "Attribute",
            listOf(
                ParamInfo.of("mode").kind(ParamKind.ENUM).required().defaultValue("remove")
                    .choices("remove", "keep")
                    .description("remove = drop listed attributes; keep = drop everything else").build(),
                ParamInfo.of("attributes").kind(ParamKind.STRING_LIST).required()
                    .placeholder("http.headers;internal.tmp")
                    .description("attribute names to remove or keep").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            FilterAttribute(
                cfg.getOrDefault("mode", "remove"),
                cfg.getOrDefault("attributes", "")
            )
        }

        // --- Sink ---
        registerTyped("PutStdout", "Write FlowFile content to stdout", "Sink",
            listOf(
                ParamInfo.of("prefix").defaultValue("").build(),
                ParamInfo.of("format").kind(ParamKind.ENUM).defaultValue("raw")
                    .choices("raw", "v3").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, ctx: ProcessorContext ->
            PutStdout(cfg.getOrDefault("prefix", ""),
                cfg.getOrDefault("format", "raw"),
                storeFrom(ctx)
            )
        }

        registerTyped("PutFile", "Write FlowFile content to directory", "Sink",
            listOf(
                ParamInfo.of("directory").required().placeholder("/var/lib/zinc/out").build(),
                ParamInfo.of("append").kind(ParamKind.BOOLEAN).defaultValue("false").build(),
                ParamInfo.of("format").kind(ParamKind.ENUM).defaultValue("raw")
                    .choices("raw", "v3").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, ctx: ProcessorContext ->
            PutFile(
                required(cfg, "directory"),
                cfg.getOrDefault("append", "false").toBoolean(),
                cfg.getOrDefault("format", "raw"),
                storeFrom(ctx)
            )
        }

        registerTyped("PutHTTP", "POST FlowFile to downstream HTTP endpoint", "Sink",
            listOf(
                ParamInfo.of("endpoint").required().placeholder("http://localhost:8080/ingest").build(),
                ParamInfo.of("method").kind(ParamKind.ENUM).defaultValue("POST")
                    .choices("POST", "PUT").build(),
                ParamInfo.of("timeoutSeconds").kind(ParamKind.INTEGER).defaultValue("30").build(),
                ParamInfo.of("contentType").defaultValue("application/octet-stream").build(),
                ParamInfo.of("format").kind(ParamKind.ENUM).defaultValue("raw")
                    .choices("raw", "v3").build()
            ),
            listOf("success", "failure")
        ) { cfg: Map<String, String>, ctx: ProcessorContext ->
            PutHTTP(
                required(cfg, "endpoint"),
                cfg.getOrDefault("method", "POST"),
                cfg.getOrDefault("timeoutSeconds", "30").toLong().toDuration(DurationUnit.SECONDS),
                cfg.getOrDefault("contentType", "application/octet-stream"),
                cfg.getOrDefault("format", "raw"),
                storeFrom(ctx)
            )
        }

        // --- V3 framing ---
        registerTyped("PackageFlowFileV3", "Wrap (attributes + content) into NiFi V3 binary content", "V3",
            listOf(),
            listOf("success")
        ) { _: Map<String, String>, ctx: ProcessorContext ->
            PackageFlowFileV3(storeFrom(ctx))
        }

        registerTyped("UnpackageFlowFileV3", "Decode V3 binary content into one or more FlowFiles", "V3",
            listOf(),
            listOf("success")
        ) { _: Map<String, String>, ctx: ProcessorContext ->
            UnpackageFlowFileV3(storeFrom(ctx))
        }

        // --- Text ---
        registerTyped("ReplaceText", "Regex find/replace on content", "Text",
            listOf(
                ParamInfo.of("pattern").required().placeholder("\\berror\\b").build(),
                ParamInfo.of("replacement").defaultValue("").build(),
                ParamInfo.of("mode").kind(ParamKind.ENUM).defaultValue("all")
                    .choices("all", "first").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            ReplaceText(
                required(cfg, "pattern"),
                cfg.getOrDefault("replacement", ""),
                cfg.getOrDefault("mode", "all")
            )
        }

        registerTyped("SplitText", "Split content by delimiter into multiple FlowFiles", "Text",
            listOf(
                ParamInfo.of("delimiter").required().placeholder("\\n\\n").build(),
                ParamInfo.of("headerLines").kind(ParamKind.INTEGER).defaultValue("0").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            SplitText(
                required(cfg, "delimiter"),
                cfg.getOrDefault("headerLines", "0").toIntOrDefault(0)
            )
        }

        registerTyped("ExtractText", "Regex capture groups → attributes", "Text",
            listOf(
                ParamInfo.of("pattern").required().placeholder("(?<user>\\w+)@(?<host>\\w+)").build(),
                ParamInfo.of("groupNames").placeholder("user,host")
                    .description("comma-separated names for positional groups").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, ctx: ProcessorContext ->
            ExtractText(required(cfg, "pattern"), cfg.getOrDefault("groupNames", ""), storeFrom(ctx))
        }

        // --- Conversion ---
        registerTyped("ConvertJSONToRecord", "Parses JSON content into records", "Conversion",
            listOf(ParamInfo.of("schemaName").defaultValue("").build()),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            ConvertJSONToRecord(cfg.getOrDefault("schemaName", ""))
        }

        registerTyped("ConvertRecordToJSON", "Serializes records back to JSON", "Conversion",
            listOf(
                ParamInfo.of("singleObject").kind(ParamKind.BOOLEAN).defaultValue("false").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            ConvertRecordToJSON(cfg.getOrDefault("singleObject", "false").toBoolean())
        }

        registerTyped("ConvertCSVToRecord", "Parse CSV content into records", "Conversion",
            listOf(
                ParamInfo.of("schemaName").defaultValue("").build(),
                ParamInfo.of("delimiter").defaultValue(",").build(),
                ParamInfo.of("hasHeader").kind(ParamKind.BOOLEAN).defaultValue("true").build(),
                ParamInfo.of("fields").placeholder("id:long,name:string").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            ConvertCSVToRecord(
                cfg.getOrDefault("schemaName", ""),
                cfg.getOrDefault("delimiter", ",").first(),
                !cfg.getOrDefault("hasHeader", "true").toBoolean(),
                cfg.getOrDefault("fields", "")
            )
        }

        registerTyped("ConvertRecordToCSV", "Serialize records to CSV", "Conversion",
            listOf(
                ParamInfo.of("delimiter").defaultValue(",").build(),
                ParamInfo.of("includeHeader").kind(ParamKind.BOOLEAN).defaultValue("true").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            ConvertRecordToCSV(
                cfg.getOrDefault("delimiter", ",").first(),
                !cfg.getOrDefault("includeHeader", "true").toBoolean()
            )
        }

        registerTyped("ConvertAvroToRecord", "Decode Avro binary into records", "Conversion",
            listOf(
                ParamInfo.of("schemaName").defaultValue("").build(),
                ParamInfo.of("fields").placeholder("id:long,name:string,amount:double")
                    .description("comma-separated name:type pairs").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            ConvertAvroToRecord(
                cfg.getOrDefault("schemaName", ""),
                cfg.getOrDefault("fields", "")
            )
        }

        registerTyped("ConvertRecordToAvro", "Encode records to Avro binary", "Conversion",
            listOf(),
            listOf("success")
        ) { _: Map<String, String>, _: ProcessorContext ->
            ConvertRecordToAvro()
        }

        registerTyped("ConvertOCFToRecord", "Decode Avro OCF (.avro file) into records", "Conversion",
            listOf(),
            listOf("success")
        ) { _: Map<String, String>, _: ProcessorContext ->
            ConvertOCFToRecord()
        }

        registerTyped("ConvertRecordToOCF", "Encode records to Avro OCF (.avro file)", "Conversion",
            listOf(
                ParamInfo.of("codec").kind(ParamKind.ENUM).defaultValue("null")
                    .choices("null", "deflate", "snappy", "bzip2", "xz", "zstandard").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            ConvertRecordToOCF(cfg.getOrDefault("codec", "null"))
        }

        // --- Transform ---
        registerTyped("EvaluateExpression", "Compute attributes from expressions", "Transform",
            listOf(
                ParamInfo.of("expressions").kind(ParamKind.KEY_VALUE_LIST).required()
                    .valueKind(ParamKind.EXPRESSION).entryDelim(";").pairDelim("=")
                    .placeholder("tax=amount*0.07; label=upper(region)")
                    .description("attr=expression pairs").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            EvaluateExpression(parseTransforms(required(cfg, "expressions")))
        }

        registerTyped("TransformRecord", "Field-level operations on records", "Transform",
            listOf(
                ParamInfo.of("operations").kind(ParamKind.MULTILINE).required()
                    .placeholder("rename:oldName:newName; remove:badField; compute:total:amount*1.07")
                    .description("semicolon-delimited op:arg1[:arg2] directives").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            TransformRecord(required(cfg, "operations"))
        }

        registerTyped("UpdateRecord", "Set or derive record fields via expressions", "Transform",
            listOf(
                ParamInfo.of("updates").kind(ParamKind.KEY_VALUE_LIST).required()
                    .valueKind(ParamKind.EXPRESSION).entryDelim(";").pairDelim("=")
                    .placeholder("tax=amount*0.07; total=amount+tax")
                    .description("field=expression pairs; later pairs see earlier writes").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            UpdateRecord(cfg.getOrDefault("updates", ""))
        }

        // --- Record ---
        registerTyped("ExtractRecordField", "Extract record fields into FlowFile attributes", "Record",
            listOf(
                ParamInfo.of("fields").kind(ParamKind.KEY_VALUE_LIST).required()
                    .entryDelim(";").pairDelim(":")
                    .placeholder("amount:order.amount;region:tenant.region")
                    .description("fieldPath:attrName pairs").build(),
                ParamInfo.of("recordIndex").kind(ParamKind.INTEGER).defaultValue("0").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            ExtractRecordField(
                required(cfg, "fields"),
                cfg.getOrDefault("recordIndex", "0").toIntOrDefault(0)
            )
        }

        registerTyped("QueryRecord", "Filter records using a JsonPath query", "Record",
            listOf(
                ParamInfo.of("query").kind(ParamKind.EXPRESSION).required()
                    .placeholder("$[?(@.amount > 100)]")
                    .description("JsonPath filter against the record batch").build()
            ),
            listOf("success")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            QueryRecord(required(cfg, "query"))
        }

        registerTyped("SplitRecord", "Fan out a RecordContent FlowFile into one FlowFile per record", "Record",
            listOf(),
            listOf("success")
        ) { _: Map<String, String>, _: ProcessorContext ->
            SplitRecord()
        }

        // --- Routing (record-level) ---
        registerTyped("RouteRecord", "Partition records across routes via per-route expression predicates", "Routing",
            listOf(
                ParamInfo.of("routes").kind(ParamKind.KEY_VALUE_LIST).required()
                    .valueKind(ParamKind.EXPRESSION).entryDelim(";").pairDelim(":")
                    .placeholder("premium: tier == \"gold\"; minors: age < 18")
                    .description("name:expression pairs; first-match wins; non-matching records go to 'unmatched'")
                    .build()
            ),
            listOf("unmatched")
        ) { cfg: Map<String, String>, _: ProcessorContext ->
            RouteRecord(cfg.getOrDefault("routes", ""))
        }
    }

    companion object {
        @Deprecated("use {@link TypeRefs#DEFAULT_VERSION}. ")
        const val DEFAULT_VERSION: String = TypeRefs.DEFAULT_VERSION

        /** Return the content store exposed by the `"content"`
         * provider, or null when no such provider is wired. Processors that
         * only handle [zincflow.core.RawContent] don't need this;
         * processors that also accept [zincflow.core.ClaimContent]
         * do — they use it to resolve claims back to bytes. */
        private fun storeFrom(ctx: ProcessorContext): ContentStore? {
            return ctx.getProviderAs(ContentProvider.NAME, ContentProvider::class.java)?.store()
        }

        private fun loggerFrom(ctx: ProcessorContext): LoggingProvider? {
            return ctx.getProviderAs(LoggingProvider.NAME, LoggingProvider::class.java)
        }

        /** Parse a compact key=value spec like
         * `"field1=expr1;field2=expr2"` into a map. Used by the
         * TransformRecord and EvaluateExpression factories so config.yaml
         * can express multi-target settings in one string (the config map
         * is `Map<String,String>`, can't nest directly). */
        private fun parseTransforms(spec: String): Map<String, String> {
            return spec.split(";").mapNotNull { e ->
                e.trim().takeIf { it.isNotEmpty() && it.contains("=") }
                    ?.split("=", limit = 2)?.let {
                        it.first() to it.last()
                    }
            }.toMap()
        }

        private fun required(cfg: Map<String, String>, key: String?): String {
            val value = cfg[key]
            requireNotNull(value) { "'$key' is required but no value included in config"}
            return value
        }
    }
}
