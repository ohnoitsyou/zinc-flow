package zincflow.fabric;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.ProcessorContext;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class ConfigLoaderTest {

    @Test
    void minimalGraphLoads() {
        var yaml = """
                flow:
                  entryPoints: [ingress]
                  processors:
                    ingress:
                      type: LogAttribute
                      config:
                        prefix: "[in] "
                """;
        var graph = new ConfigLoader(new Registry(), new ProcessorContext(), null, null).load(yaml);
        assertEquals(1, graph.processors().size());
        assertEquals(List.of(), graph.next("ingress", "success"));
        assertEquals(List.of("ingress"), graph.entryPoints());
    }

    @Test
    void connectionsResolveToProcessors() {
        var yaml = """
                flow:
                  entryPoints: [a]
                  processors:
                    a:
                      type: UpdateAttribute
                      config:
                        key: stage
                        value: first
                    b:
                      type: LogAttribute
                  connections:
                    a:
                      success: [b]
                """;
        var graph = new ConfigLoader(new Registry()).load(yaml);
        assertEquals(List.of("b"), graph.next("a", "success"));
    }

    @Test
    void missingProcessorTypeRejected() {
        var yaml = """
                flow:
                  entryPoints: [x]
                  processors:
                    x:
                      config: {}
                """;
        assertThrows(IllegalArgumentException.class, () -> new ConfigLoader(new Registry()).load(yaml));
    }

    @Test
    void unknownProcessorTypeRejected() {
        var yaml = """
                flow:
                  entryPoints: [x]
                  processors:
                    x:
                      type: NotARealProcessor
                """;
        assertThrows(IllegalArgumentException.class, () -> new ConfigLoader(new Registry()).load(yaml));
    }

    @Test
    void entryPointsMustReferenceDefinedProcessor() {
        var yaml = """
                flow:
                  entryPoints: [ghost]
                  processors:
                    real:
                      type: LogAttribute
                """;
        assertThrows(IllegalArgumentException.class, () -> new ConfigLoader(new Registry()).load(yaml));
    }

    @Test
    void connectionTargetMustExist() {
        var yaml = """
                flow:
                  entryPoints: [a]
                  processors:
                    a:
                      type: LogAttribute
                  connections:
                    a:
                      success: [nosuch]
                """;
        assertThrows(IllegalArgumentException.class, () -> new ConfigLoader(new Registry()).load(yaml));
    }

    @Test
    void loadedPipelineExecutes() {
        var yaml = """
                flow:
                  entryPoints: [router]
                  processors:
                    router:
                      type: RouteOnAttribute
                      config:
                        routes: "high: priority == urgent"
                    elevate:
                      type: UpdateAttribute
                      config:
                        key: priority
                        value: elevated
                  connections:
                    router:
                      high: [elevate]
                """;
        var graph = new ConfigLoader(new Registry()).load(yaml);
        var pipeline = new Pipeline(graph);
        pipeline.ingest(FlowFile.create(new byte[0], Map.of("priority", "urgent")));
        // Both router and elevate should have been dispatched.
        assertEquals(2L, pipeline.stats().snapshot().get("totalProcessed"));
    }
}
