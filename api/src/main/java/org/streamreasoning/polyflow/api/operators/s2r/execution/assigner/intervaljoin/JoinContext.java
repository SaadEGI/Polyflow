package org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.intervaljoin;

/**
 * Context object passed to {@link ProcessJoinFunction#processElement}.
 * Provides access to the timestamps of both sides and a side-output facility.
 */
public interface JoinContext {

    /**
     * @return Event-time timestamp of the left record that produced this join match
     */
    long getLeftTimestamp();

    /**
     * @return Event-time timestamp of the right record that produced this join match
     */
    long getRightTimestamp();

    /**
     * @return The output timestamp assigned to this joined pair: {@code max(leftTs, rightTs)}
     */
    long getTimestamp();

    /**
     * Emit a value to a named side-output.
     *
     * @param tag   a string tag identifying the side output
     * @param value the value to emit
     */
    <X> void output(String tag, X value);
}
