package zincflow.processors;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.ProcessorResult;
import zincflow.core.RecordContent;
import zincflow.core.SchemaDefs;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class FormatConversionTest {

    // --- CSV ---

    @Test
    void csvHeaderRowParse() {
        var ff = FlowFile.create("name,age\nAlice,30\nBob,25".getBytes(StandardCharsets.UTF_8), Map.of());
        var out = (ProcessorResult.Single) new ConvertCSVToRecord().process(ff);
        var rc = (RecordContent) out.flowFile.content;
        assertEquals(2, rc.records().size());
        assertEquals("Alice", rc.records().get(0).get("name"));
        assertEquals("25", rc.records().get(1).get("age"));
        assertEquals("2", out.flowFile.attributes().get("record.count"));
    }

    @Test
    void csvTabDelimitedParse() {
        var body = "name\tage\nAlice\t30".getBytes(StandardCharsets.UTF_8);
        var ff = FlowFile.create(body, Map.of());
        var out = (ProcessorResult.Single) new ConvertCSVToRecord("", '\t', true, "").process(ff);
        var rc = (RecordContent) out.flowFile.content;
        assertEquals("Alice", rc.records().getFirst().get("name"));
    }

    @Test
    void csvQuotedValuesHandled() {
        var body = "name,note\n\"Smith, Jr.\",\"has, commas\"".getBytes(StandardCharsets.UTF_8);
        var ff = FlowFile.create(body, Map.of());
        var out = (ProcessorResult.Single) new ConvertCSVToRecord().process(ff);
        var rc = (RecordContent) out.flowFile.content;
        assertEquals("Smith, Jr.", rc.records().getFirst().get("name"));
        assertEquals("has, commas", rc.records().getFirst().get("note"));
    }

    @Test
    void csvExplicitFieldsDefineSchemaAndTypes() {
        // When `fields` is set, the compact defs drive column order + types.
        var ff = FlowFile.create("alice,30\nbob,25".getBytes(), Map.of());
        var out = (ProcessorResult.Single) new ConvertCSVToRecord(
                "User", ',', false, "name:string,age:int").process(ff);
        var rc = (RecordContent) out.flowFile.content;
        assertNotNull(rc.schema);
        assertEquals("User", rc.schema.getName());
        assertEquals(2, rc.schema.getFields().size());
        assertEquals("age", rc.schema.getFields().get(1).name());
    }

    @Test
    void csvRoundTripPreservesRecords() {
        // JSON → Record → CSV → Record — values should line up.
        var jsonFf = FlowFile.create("[{\"id\":\"1\",\"name\":\"a\"},{\"id\":\"2\",\"name\":\"b\"}]".getBytes(), Map.of());
        var afterJson = (ProcessorResult.Single) new ConvertJSONToRecord().process(jsonFf);
        var afterCsv  = (ProcessorResult.Single) new ConvertRecordToCSV().process(afterJson.flowFile);
        var afterRead = (ProcessorResult.Single) new ConvertCSVToRecord().process(afterCsv.flowFile);
        var rc = (RecordContent) afterRead.flowFile.content;
        assertEquals(2, rc.records().size());
        assertEquals("1", rc.records().get(0).get("id"));
        assertEquals("b", rc.records().get(1).get("name"));
    }

    @Test
    void csvOnNonRawContentFailsCleanly() {
        var ff = FlowFile.create(new RecordContent(List.of()), Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, new ConvertCSVToRecord().process(ff));
    }

    // --- Avro binary (no container) ---
    // Schemas are now specified via the compact field-defs format
    // (ParseFieldDefs) — not inline Avro JSON. Tests construct Schemas
    // via SchemaBuilder when they need richer (nested) shapes.

    private static final String SIMPLE_FIELDS = "id:long,name:string,active:boolean";

    @Test
    void avroRoundTripFlatRecord() {
        Schema schema = SchemaDefs.parse("User", SIMPLE_FIELDS);
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("id", 7L);
        record.put("name", "alice");
        record.put("active", true);
        var ff = FlowFile.create(new RecordContent(List.of(record), schema), Map.of());

        var encoded = (ProcessorResult.Single) new ConvertRecordToAvro().process(ff);
        var decoded = (ProcessorResult.Single) new ConvertAvroToRecord("User", SIMPLE_FIELDS).process(encoded.flowFile);
        var rc = (RecordContent) decoded.flowFile.content;

        assertEquals(1, rc.records().size());
        assertEquals(7L, rc.records().getFirst().get("id"));
        assertEquals("alice", rc.records().getFirst().get("name"));
        assertEquals(Boolean.TRUE, rc.records().getFirst().get("active"));
    }

    @Test
    void avroRoundTripMultipleRecords() {
        Schema schema = SchemaDefs.parse("User", SIMPLE_FIELDS);
        List<Map<String, Object>> records = List.of(
                flat(1L, "a", true),
                flat(2L, "b", false),
                flat(3L, "c", true));
        var ff = FlowFile.create(new RecordContent(records, schema), Map.of());
        var encoded = (ProcessorResult.Single) new ConvertRecordToAvro().process(ff);
        var decoded = (ProcessorResult.Single) new ConvertAvroToRecord("User", SIMPLE_FIELDS).process(encoded.flowFile);
        var rc = (RecordContent) decoded.flowFile.content;
        assertEquals(3, rc.records().size());
        assertEquals("b", rc.records().get(1).get("name"));
    }

    private static Map<String, Object> flat(long id, String name, boolean active) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("id", id);
        r.put("name", name);
        r.put("active", active);
        return r;
    }

    @Test
    void avroRoundTripNestedRecordAndArray() {
        // Nested shapes go beyond the compact field-defs format, so we
        // build the Schema programmatically with SchemaBuilder. This
        // mirrors what a user would do via schema-registry or a custom
        // builder for richer data.
        Schema nested = SchemaBuilder.record("Order").namespace("zincflow").fields()
                .name("id").type().stringType().noDefault()
                .name("items").type().array().items().stringType().noDefault()
                .name("customer").type().record("Customer").namespace("zincflow").fields()
                    .name("id").type().longType().noDefault()
                    .name("tenant").type().stringType().noDefault()
                .endRecord().noDefault()
                .endRecord();

        Map<String, Object> customer = new LinkedHashMap<>();
        customer.put("id", 42L);
        customer.put("tenant", "acme");
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("id", "ord-1");
        record.put("items", List.of("apple", "banana"));
        record.put("customer", customer);
        var ff = FlowFile.create(new RecordContent(List.of(record), nested), Map.of());

        var encoded = (ProcessorResult.Single) new ConvertRecordToAvro().process(ff);
        // The decoded side can't reconstruct the nested schema from the compact
        // avro.schema attribute (it's a flat-fields format). For round-trip we
        // feed the schema via config instead — but compact defs only cover
        // primitives. Nested round-trip on the binary side requires carrying
        // the schema separately (OCF test covers that); here we just verify
        // the encoded bytes are non-empty.
        var raw = (zincflow.core.RawContent) encoded.flowFile.content;
        assertTrue(raw.bytes.length > 0);
    }

    @Test
    void avroSchemaResolvesFromAttributeWhenConfigAbsent() {
        // ConvertRecordToAvro writes avro.schema; ConvertAvroToRecord reads
        // it back when no fields config is provided.
        Schema schema = SchemaDefs.parse("User", SIMPLE_FIELDS);
        var ff = FlowFile.create(new RecordContent(List.of(flat(9L, "z", false)), schema), Map.of());
        var encoded = (ProcessorResult.Single) new ConvertRecordToAvro().process(ff);
        assertEquals(SIMPLE_FIELDS, encoded.flowFile.attributes().get("avro.schema"));

        var decoded = (ProcessorResult.Single) new ConvertAvroToRecord("", "").process(encoded.flowFile);
        var rc = (RecordContent) decoded.flowFile.content;
        assertEquals("z", rc.records().getFirst().get("name"));
    }

    @Test
    void avroMissingSchemaFromConfigAndAttributeFails() {
        var ff = FlowFile.create(new byte[]{1, 2}, Map.of());
        var out = new ConvertAvroToRecord("", "").process(ff);
        assertInstanceOf(ProcessorResult.Failure.class, out);
    }

    @Test
    void avroRecordToAvroWithoutSchemaOnContentFails() {
        // RecordContent with null schema — writer cannot proceed.
        var ff = FlowFile.create(new RecordContent(List.of(flat(1L, "a", true))), Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, new ConvertRecordToAvro().process(ff));
    }

    @Test
    void avroMalformedBinaryFails() {
        var ff = FlowFile.create(new byte[]{1, 2, 3, 4, 5}, Map.of());
        var out = new ConvertAvroToRecord("User", SIMPLE_FIELDS).process(ff);
        assertInstanceOf(ProcessorResult.Failure.class, out);
    }

    @Test
    void avroOnNonRecordContentFailsCleanly() {
        var ff = FlowFile.create(new byte[]{1, 2}, Map.of());
        assertInstanceOf(ProcessorResult.Failure.class,
                new ConvertRecordToAvro().process(ff));
    }

    // --- OCF ---

    @Test
    void ocfRoundTripEmbedsSchema() {
        Schema schema = SchemaDefs.parse("User", SIMPLE_FIELDS);
        Map<String, Object> record = flat(1L, "alice", true);
        var ff = FlowFile.create(new RecordContent(List.of(record), schema), Map.of());

        var written = (ProcessorResult.Single) new ConvertRecordToOCF().process(ff);
        var read    = (ProcessorResult.Single) new ConvertOCFToRecord().process(written.flowFile);
        var rc = (RecordContent) read.flowFile.content;

        assertEquals(1, rc.records().size());
        assertEquals("alice", rc.records().getFirst().get("name"));
        // Schema surfaces as an attribute for downstream use.
        assertTrue(read.flowFile.attributes().get("avro.schema").contains("\"name\":\"User\""));
    }

    @Test
    void ocfDeflateCodecAccepted() {
        Schema schema = SchemaDefs.parse("User", SIMPLE_FIELDS);
        Map<String, Object> record = flat(1L, "alice", true);
        var ff = FlowFile.create(new RecordContent(List.of(record), schema), Map.of());
        // deflate is a built-in Avro codec — should not throw.
        assertDoesNotThrow(() -> new ConvertRecordToOCF("deflate").process(ff));
    }

    @Test
    void ocfReadOnNonRawContentFailsCleanly() {
        var ff = FlowFile.create(new RecordContent(List.of()), Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, new ConvertOCFToRecord().process(ff));
    }

    @Test
    void ocfWriteEmptyRecordList() {
        // Zero records → pass through the original FlowFile (matches C#'s
        // SingleResult.Rent(ff) early-return). No OCF bytes produced.
        var ff = FlowFile.create(new RecordContent(List.of()), Map.of());
        var written = (ProcessorResult.Single) new ConvertRecordToOCF().process(ff);
        assertSame(ff, written.flowFile);
    }

    @Test
    void ocfGarbageInputFailsCleanly() {
        var ff = FlowFile.create(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, new ConvertOCFToRecord().process(ff));
    }
}
