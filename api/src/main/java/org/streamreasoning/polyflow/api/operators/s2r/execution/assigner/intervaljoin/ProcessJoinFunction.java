package org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.intervaljoin;

/**
 * User-defined join function invoked for every (left, right) pair that satisfies
 * both the key predicate and the time-interval predicate.
 * <p>
 * Mirrors Flink's {@code ProcessJoinFunction<IN1, IN2, OUT>}.
 *
 * @param <L>   left input type
 * @param <R>   right input type
 * @param <OUT> output type
 */
@FunctionalInterface
public interface ProcessJoinFunction<L, R, OUT> {

    /**
     * Called for every matching (left, right) pair.
     *
     * @param left      the left element
     * @param right     the right element
     * @param context   provides timestamps and side-output access
     * @param collector collects the produced output records
     */
    void processElement(L left, R right, JoinContext context, JoinCollector<OUT> collector);
}
