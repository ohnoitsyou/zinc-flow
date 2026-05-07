package zincflow.core

import zincflow.core.ProcessorResult.Routed

/** Conventional relationship names for [Routed]
 * and processor registration metadata. Centralised so a rename ripples
 * in one place, and so config validation can cross-check connection
 * keys against a known set.
 * 
 * Not every processor uses every name — the set here is the NiFi-ish
 * vocabulary the built-in processors share. Plugin processors are
 * free to declare their own relationship names. */
object Relationships {
    const val SUCCESS: String = "success"
    const val FAILURE: String = "failure"
    const val MATCHED: String = "matched"
    const val UNMATCHED: String = "unmatched"
    const val ORIGINAL: String = "original"
}
