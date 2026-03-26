package org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.intervaljoin;

/**
 * A collector that receives join output records and timestamped side-output records.
 *
 * @param <OUT> type of the main output
 */
public interface JoinCollector<OUT> {

    /**
     * Emit a main output record with an associated event-time timestamp.
     *
     * @param record    the joined record
     * @param timestamp event-time to assign ({@code max(tL, tR)})
     */
    void collect(OUT record, long timestamp);

    /**
     * Emit a record to a named side-output (e.g. late data).
     *
     * @param tag    the side-output tag
     * @param value  the value to emit
     */
    <X> void sideOutput(String tag, X value);
}
