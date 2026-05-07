package zincflow.core

/** Lifecycle state shared by processors, providers, and sources.
 * Matches the zinc-flow-csharp enum 1:1. */
enum class ComponentState {
    DISABLED,
    ENABLED,
    DRAINING
}
