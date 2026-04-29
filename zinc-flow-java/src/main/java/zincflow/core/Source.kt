package zincflow.core

import java.util.function.Predicate

/** Pull/push source that feeds FlowFiles into a pipeline. Concrete
 * sources (see [PollingSource]) hand off to the
 * pipeline through the `ingest` callback supplied at start —
 * the callback returns `true` on acceptance so the source can
 * mark the underlying item consumed (e.g. `GetFile` moves the
 * file to `.processed/`).
 * 
 * Mirrors zinc-flow-csharp's IConnectorSource. */
interface Source {
    fun name(): String?

    fun sourceType(): String?

    val isRunning: Boolean

    /** Begin emitting. `ingest` is called once per FlowFile; it
     * returns whether the pipeline accepted the submission. Sources
     * must be idempotent w.r.t. `start` — calling twice with the
     * source already running is a no-op. */
    fun start(ingest: Predicate<FlowFile?>?)

    fun stop()
}
