package zincflow.processors;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.ProcessorResult;
import zincflow.core.RecordContent;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class ExpressionAndQueryTest {

    // --- EvaluateExpression ---

    @Test
    void evaluateExpressionReadsAttributes() {
        var proc = new EvaluateExpression(Map.of(
                "routing.key", "attributes['priority'] + '/' + attributes['tenant']"));
        var ff = FlowFile.Companion.create(new byte[0], Map.of("priority", "high", "tenant", "acme"));
        var out = (ProcessorResult.Single) proc.process(ff);
        assertEquals("high/acme", out.flowFile.getAttributes().get("routing.key"));
    }

    @Test
    void evaluateExpressionDoesArithmetic() {
        var proc = new EvaluateExpression(Map.of("doubled", "contentSize * 2 + 1"));
        var ff = FlowFile.Companion.create(new byte[]{1, 2, 3, 4, 5}, Map.of());
        var out = (ProcessorResult.Single) proc.process(ff);
        assertEquals("11", out.flowFile.getAttributes().get("doubled"));
    }

    @Test
    void evaluateExpressionReadsFirstRecord() {
        var records = List.<Map<String, Object>>of(Map.of("amount", 42));
        var ff = FlowFile.Companion.create(RecordContent.Companion.invoke(records, null), Map.of());
        var proc = new EvaluateExpression(Map.of("classification", "record['amount'] > 10 ? 'big' : 'small'"));
        var out = (ProcessorResult.Single) proc.process(ff);
        assertEquals("big", out.flowFile.getAttributes().get("classification"));
    }

    @Test
    void evaluateExpressionNullSafeOnMissingAttribute() {
        var proc = new EvaluateExpression(Map.of("out", "attributes['nope']"));
        var ff = FlowFile.Companion.create(new byte[0], Map.of());
        var out = (ProcessorResult.Single) proc.process(ff);
        assertEquals("", out.flowFile.getAttributes().get("out"));
    }

    @Test
    void evaluateExpressionMultipleTargetsFromSingleInvocation() {
        // Multi-output shape (matches C#): one call sets N attributes.
        // LinkedHashMap preserves target order so evaluations compose
        // predictably when a later expression reads an earlier target.
        var exprs = new LinkedHashMap<String, String>();
        exprs.put("doubled", "contentSize * 2");
        exprs.put("tripled", "contentSize * 3");
        exprs.put("sum",     "contentSize + 100");
        var proc = new EvaluateExpression(exprs);
        var ff = FlowFile.Companion.create(new byte[]{1, 2, 3, 4, 5}, Map.of());
        var out = (ProcessorResult.Single) proc.process(ff);
        assertEquals("10",  out.flowFile.getAttributes().get("doubled"));
        assertEquals("15",  out.flowFile.getAttributes().get("tripled"));
        assertEquals("105", out.flowFile.getAttributes().get("sum"));
    }

    @Test
    void evaluateExpressionInvalidSyntaxRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new EvaluateExpression(Map.of("x", "not ))) valid (((")));
    }

    @Test
    void evaluateExpressionEmptyMapRejected() {
        assertThrows(IllegalArgumentException.class, () -> new EvaluateExpression(Map.of()));
    }

    @Test
    void evaluateExpressionBlankEntriesRejected() {
        assertThrows(IllegalArgumentException.class, () -> new EvaluateExpression(Map.of("x", "")));
        // Blank keys can't exist in Map.of() (NPE check), so we use a constructed map.
        var badKey = new LinkedHashMap<String, String>();
        badKey.put("", "1");
        assertThrows(IllegalArgumentException.class, () -> new EvaluateExpression(badKey));
    }

    // --- TransformRecord ---

    @Test
    void transformRecordComputeRewritesField() {
        var records = List.<Map<String, Object>>of(
                mutable("name", "alice", "score", 10),
                mutable("name", "bob", "score", 20));
        var ff = FlowFile.Companion.create(RecordContent.Companion.invoke(records, null), Map.of());
        var proc = new TransformRecord(
                "compute:score:score * 2;compute:label:name + '!'");
        var out = (ProcessorResult.Single) proc.process(ff);
        var rc = (RecordContent) out.flowFile.getContent();

        assertEquals(20, ((Number) rc.getRecords().getFirst().get("score")).intValue());
        assertEquals("alice!", rc.getRecords().getFirst().get("label"));
        assertEquals(40, ((Number) rc.getRecords().get(1).get("score")).intValue());
        assertEquals("alice", rc.getRecords().get(0).get("name"));
    }

    @Test
    void transformRecordRenameMovesField() {
        var records = List.<Map<String, Object>>of(mutable("oldName", 42));
        var ff = FlowFile.Companion.create(RecordContent.Companion.invoke(records, null), Map.of());
        var proc = new TransformRecord("rename:oldName:newName");
        var rc = (RecordContent) ((ProcessorResult.Single) proc.process(ff)).flowFile.getContent();
        assertFalse(rc.getRecords().getFirst().containsKey("oldName"));
        assertEquals(42, rc.getRecords().getFirst().get("newName"));
    }

    @Test
    void transformRecordRemoveDropsField() {
        var records = List.<Map<String, Object>>of(mutable("keep", 1, "drop", 2));
        var ff = FlowFile.Companion.create(RecordContent.Companion.invoke(records, null), Map.of());
        var proc = new TransformRecord("remove:drop");
        var rc = (RecordContent) ((ProcessorResult.Single) proc.process(ff)).flowFile.getContent();
        assertFalse(rc.getRecords().getFirst().containsKey("drop"));
        assertEquals(1, rc.getRecords().getFirst().get("keep"));
    }

    @Test
    void transformRecordAddInsertsLiteralField() {
        var records = List.<Map<String, Object>>of(mutable("x", 1));
        var ff = FlowFile.Companion.create(RecordContent.Companion.invoke(records, null), Map.of());
        var proc = new TransformRecord("add:region:us-east-1");
        var rc = (RecordContent) ((ProcessorResult.Single) proc.process(ff)).flowFile.getContent();
        assertEquals("us-east-1", rc.getRecords().getFirst().get("region"));
    }

    @Test
    void transformRecordCopyDuplicatesFieldPreservingType() {
        var records = List.<Map<String, Object>>of(mutable("src", 99L));
        var ff = FlowFile.Companion.create(RecordContent.Companion.invoke(records, null), Map.of());
        var proc = new TransformRecord("copy:src:dst");
        var rc = (RecordContent) ((ProcessorResult.Single) proc.process(ff)).flowFile.getContent();
        assertEquals(99L, rc.getRecords().getFirst().get("dst"));
        assertEquals(99L, rc.getRecords().getFirst().get("src"));
    }

    @Test
    void transformRecordUpperAndLowerOperateOnStrings() {
        var records = List.<Map<String, Object>>of(mutable("a", "Hello", "b", "World"));
        var ff = FlowFile.Companion.create(RecordContent.Companion.invoke(records, null), Map.of());
        var proc = new TransformRecord("toUpper:a;toLower:b");
        var rc = (RecordContent) ((ProcessorResult.Single) proc.process(ff)).flowFile.getContent();
        assertEquals("HELLO", rc.getRecords().getFirst().get("a"));
        assertEquals("world", rc.getRecords().getFirst().get("b"));
    }

    @Test
    void transformRecordDefaultSetsMissingOrNullFields() {
        var records = List.<Map<String, Object>>of(mutable("present", "x"));
        var ff = FlowFile.Companion.create(RecordContent.Companion.invoke(records, null), Map.of());
        var proc = new TransformRecord("default:absent:fallback;default:present:never");
        var rc = (RecordContent) ((ProcessorResult.Single) proc.process(ff)).flowFile.getContent();
        assertEquals("fallback", rc.getRecords().getFirst().get("absent"));
        // `default` only fires when field is missing or null — 'present' keeps its value.
        assertEquals("x", rc.getRecords().getFirst().get("present"));
    }

    @Test
    void transformRecordOperationsRunInOrder() {
        // Each step sees the post-previous-step state, so compute:total
        // can reference a field added earlier in the same directive chain.
        var records = List.<Map<String, Object>>of(mutable("price", 100, "qty", 3));
        var ff = FlowFile.Companion.create(RecordContent.Companion.invoke(records, null), Map.of());
        var proc = new TransformRecord(
                "compute:subtotal:price * qty;compute:total:subtotal + 10");
        var rc = (RecordContent) ((ProcessorResult.Single) proc.process(ff)).flowFile.getContent();
        assertEquals(300, ((Number) rc.getRecords().getFirst().get("subtotal")).intValue());
        assertEquals(310, ((Number) rc.getRecords().getFirst().get("total")).intValue());
    }

    @Test
    void transformRecordOnNonRecordContentFails() {
        var ff = FlowFile.Companion.create(new byte[]{1, 2}, Map.of());
        var proc = new TransformRecord("compute:x:1");
        assertInstanceOf(ProcessorResult.Failure.class, proc.process(ff));
    }

    @Test
    void transformRecordEmptySpecRejected() {
        assertThrows(IllegalArgumentException.class, () -> new TransformRecord(""));
        assertThrows(IllegalArgumentException.class, () -> new TransformRecord(null));
    }

    @Test
    void transformRecordUnknownOpRejected() {
        assertThrows(IllegalArgumentException.class, () -> new TransformRecord("uppper:x"));
    }

    @Test
    void transformRecordInvalidJexlInComputeRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new TransformRecord("compute:bad:this is ))) not valid ((("));
    }

    private static Map<String, Object> mutable(Object... keyValues) {
        var m = new LinkedHashMap<String, Object>();
        for (int i = 0; i < keyValues.length; i += 2) {
            m.put((String) keyValues[i], keyValues[i + 1]);
        }
        return m;
    }

    // --- QueryRecord ---

    @Test
    void queryRecordMatchesHighPriority() {
        var records = List.<Map<String, Object>>of(
                Map.of("id", 1, "priority", "high"),
                Map.of("id", 2, "priority", "low"),
                Map.of("id", 3, "priority", "high"));
        var ff = FlowFile.Companion.create(RecordContent.Companion.invoke(records, null), Map.of());
        var proc = new QueryRecord("$[?(@.priority == 'high')]");
        var out = (ProcessorResult.Routed) proc.process(ff);
        assertEquals("matched", out.getRoute());
        var rc = (RecordContent) out.getFlowFile().getContent();
        assertEquals(2, rc.getRecords().size());
    }

    @Test
    void queryRecordNoMatchRoutesUnmatched() {
        var records = List.<Map<String, Object>>of(Map.of("priority", "low"));
        var ff = FlowFile.Companion.create(RecordContent.Companion.invoke(records, null), Map.of());
        var proc = new QueryRecord("$[?(@.priority == 'urgent')]");
        var out = (ProcessorResult.Routed) proc.process(ff);
        assertEquals("unmatched", out.getRoute());
    }

    @Test
    void queryRecordOnNonRecordContentFails() {
        var ff = FlowFile.Companion.create(new byte[]{1, 2}, Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, new QueryRecord("$").process(ff));
    }

    @Test
    void queryRecordInvalidQueryRejected() {
        assertThrows(IllegalArgumentException.class, () -> new QueryRecord(""));
        // Note: JsonPath's compile is lenient — truly invalid syntax like "][["
        // surfaces at compile time.
        assertThrows(IllegalArgumentException.class, () -> new QueryRecord("][?("));
    }
}
