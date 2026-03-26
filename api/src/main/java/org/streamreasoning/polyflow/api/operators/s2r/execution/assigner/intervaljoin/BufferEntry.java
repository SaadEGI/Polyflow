package org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.intervaljoin;

import java.util.Objects;

/**
 * A single entry held in the left or right keyed state buffer.
 * Mirrors Flink's {@code BufferEntry<T>}.
 *
 * @param <T> the element type
 */
public final class BufferEntry<T> {

    private final T element;
    private boolean hasBeenJoined;

    public BufferEntry(T element, boolean hasBeenJoined) {
        this.element = element;
        this.hasBeenJoined = hasBeenJoined;
    }

    public T getElement() {
        return element;
    }

    public boolean hasBeenJoined() {
        return hasBeenJoined;
    }

    public void markJoined() {
        this.hasBeenJoined = true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BufferEntry<?> that = (BufferEntry<?>) o;
        return hasBeenJoined == that.hasBeenJoined &&
                Objects.equals(element, that.element);
    }

    @Override
    public int hashCode() {
        return Objects.hash(element, hasBeenJoined);
    }

    @Override
    public String toString() {
        return "BufferEntry{element=" + element + ", joined=" + hasBeenJoined + '}';
    }
}
