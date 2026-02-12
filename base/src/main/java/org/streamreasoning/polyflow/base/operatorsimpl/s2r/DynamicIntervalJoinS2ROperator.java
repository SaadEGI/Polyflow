package org.streamreasoning.polyflow.base.operatorsimpl.s2r;

import org.apache.log4j.Logger;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.polyflow.api.operators.s2r.execution.instance.Window;
import org.streamreasoning.polyflow.api.operators.s2r.execution.instance.WindowImpl;
import org.streamreasoning.polyflow.api.sds.timevarying.TimeVarying;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeInstant;
import org.streamreasoning.polyflow.base.sds.TimeVaryingObject;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DynamicIntervalJoinS2ROperator<I, K, W, R extends Iterable<?>>
        implements StreamToRelationOperator<I, W, R> {

    private static final Logger log = Logger.getLogger(DynamicIntervalJoinS2ROperator.class);

    /**
     * Processing mode for dynamic interval joins
     */
    public enum ProcessingMode {
        /**
         * BATCH: Collect all matches during grace period, report when grace period expires.
         * "Extend by full window" - waits for all potential late matches.
         */
        BATCH,

        /**
         * INCREMENTAL: Report immediately on each match found.
         * "Process elem by elem" - emits results as matches are discovered.
         */
        INCREMENTAL
    }

    private final Tick tick;
    private final Time time;
    private final String name;
    private final ContentFactory<I, W, R> cf;
    private final Report report;

    // Grace period: how long to wait for a matching foreign key
    private final long gracePeriod;

    // Key extractor: extracts the join key from an element
    private final Function<I, K> keyExtractor;

    // Timestamp extractor: extracts timestamp from an element
    private final Function<I, Long> timestampExtractor;

    // Build buffer indexed by KEY
    // This contains events from the other stream that we're trying to match
    private final Map<K, TreeMap<Long, List<TimestampedElement<I>>>> buildBufferByKey;

    // Probe buffer for this stream's events (keyed for bidirectional joins)
    private final Map<K, TreeMap<Long, List<TimestampedElement<I>>>> probeBufferByKey = new HashMap<>();

    // Pending windows: windows waiting for a match (key -> PendingWindow)
    private final Map<K, List<PendingWindow<I, K, W, R>>> pendingWindows = new HashMap<>();

    // Completed windows with their content (already matched or grace period expired)
    private final Map<Window, Content<I, W, R>> completedWindows = new LinkedHashMap<>();

    // Windows that have been reported
    private final List<Window> reportedWindows = new ArrayList<>();

    // State retention time for cleanup
    private final long stateRetentionTime;

    // If true, close window on first match; if false, collect all matches during grace period
    private final boolean closeOnFirstMatch;

    // Optional: Factory to create joined result elements
    private final JoinResultFactory<I, K> joinResultFactory;

    // Processing mode: BATCH (extend by full window) or INCREMENTAL (elem by elem)
    private final ProcessingMode processingMode;

    /**
     * Wrapper class to store elements with their timestamps and keys
     */
    public static class TimestampedElement<T> {
        public final T element;
        public final long timestamp;

        public TimestampedElement(T element, long timestamp) {
            this.element = element;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "TS{" + element + "@" + timestamp + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TimestampedElement<?> that = (TimestampedElement<?>) o;
            return timestamp == that.timestamp && Objects.equals(element, that.element);
        }

        @Override
        public int hashCode() {
            return Objects.hash(element, timestamp);
        }
    }

    /**
     * Represents a pending window waiting for a match
     */
    public static class PendingWindow<I, K, W, R> {
        public final K key;
        public final I probeElement;
        public final long probeTimestamp;
        public final long graceDeadline;     // When the grace period expires
        public final Window window;
        public final Content<I, W, R> content;
        public boolean matched;              // Whether a match has been found

        public PendingWindow(K key, I probeElement, long probeTimestamp, long graceDeadline,
                             Window window, Content<I, W, R> content) {
            this.key = key;
            this.probeElement = probeElement;
            this.probeTimestamp = probeTimestamp;
            this.graceDeadline = graceDeadline;
            this.window = window;
            this.content = content;
            this.matched = false;
        }

        public boolean isExpired(long currentTime) {
            return currentTime >= graceDeadline;
        }

        @Override
        public String toString() {
            return "PendingWindow{key=" + key + ", deadline=" + graceDeadline +
                    ", matched=" + matched + "}";
        }
    }

    /**
     * Factory interface to create joined result elements
     */
    @FunctionalInterface
    public interface JoinResultFactory<I, K> {
        /**
         * Create a joined element from probe and build elements
         */
        I createJoinedElement(I probeElement, K probeKey, long probeTs,
                              I buildElement, long buildTs);
    }

    /**
     * Full constructor with all options including processing mode
     *
     * @param tick               The tick type
     * @param time               The shared Time object
     * @param name               Name of this operator
     * @param cf                 Content factory for creating window content
     * @param report             Report strategy
     * @param gracePeriod        Grace period to wait for matching foreign key
     * @param buildBufferByKey   Shared buffer containing events from the other stream
     * @param keyExtractor       Function to extract the join key from elements
     * @param timestampExtractor Function to extract timestamp from elements
     * @param joinResultFactory  Optional factory to create joined result elements
     * @param stateRetentionTime How long to retain state
     * @param closeOnFirstMatch  If true, close window on first match; otherwise collect all
     * @param processingMode     BATCH (extend by full window) or INCREMENTAL (elem by elem)
     */
    public DynamicIntervalJoinS2ROperator(
            Tick tick,
            Time time,
            String name,
            ContentFactory<I, W, R> cf,
            Report report,
            long gracePeriod,
            Map<K, TreeMap<Long, List<TimestampedElement<I>>>> buildBufferByKey,
            Function<I, K> keyExtractor,
            Function<I, Long> timestampExtractor,
            JoinResultFactory<I, K> joinResultFactory,
            long stateRetentionTime,
            boolean closeOnFirstMatch,
            ProcessingMode processingMode) {

        this.tick = tick;
        this.time = time;
        this.name = name;
        this.cf = cf;
        this.report = report;
        this.gracePeriod = gracePeriod;
        this.buildBufferByKey = buildBufferByKey;
        this.keyExtractor = keyExtractor;
        this.timestampExtractor = timestampExtractor;
        this.joinResultFactory = joinResultFactory;
        this.stateRetentionTime = stateRetentionTime;
        this.closeOnFirstMatch = closeOnFirstMatch;
        this.processingMode = processingMode;
    }

    /**
     * Full constructor with all options (defaults to BATCH mode for backward compatibility)
     */
    public DynamicIntervalJoinS2ROperator(
            Tick tick,
            Time time,
            String name,
            ContentFactory<I, W, R> cf,
            Report report,
            long gracePeriod,
            Map<K, TreeMap<Long, List<TimestampedElement<I>>>> buildBufferByKey,
            Function<I, K> keyExtractor,
            Function<I, Long> timestampExtractor,
            JoinResultFactory<I, K> joinResultFactory,
            long stateRetentionTime,
            boolean closeOnFirstMatch) {

        this(tick, time, name, cf, report, gracePeriod, buildBufferByKey,
                keyExtractor, timestampExtractor, joinResultFactory, stateRetentionTime,
                closeOnFirstMatch, ProcessingMode.BATCH);
    }

    /**
     * Simplified constructor - waits for all matches during grace period
     */
    public DynamicIntervalJoinS2ROperator(
            Tick tick,
            Time time,
            String name,
            ContentFactory<I, W, R> cf,
            Report report,
            long gracePeriod,
            Map<K, TreeMap<Long, List<TimestampedElement<I>>>> buildBufferByKey,
            Function<I, K> keyExtractor,
            Function<I, Long> timestampExtractor,
            long stateRetentionTime) {

        this(tick, time, name, cf, report, gracePeriod, buildBufferByKey,
                keyExtractor, timestampExtractor, null, stateRetentionTime, false);
    }

    /**
     * Constructor with closeOnFirstMatch option
     */
    public DynamicIntervalJoinS2ROperator(
            Tick tick,
            Time time,
            String name,
            ContentFactory<I, W, R> cf,
            Report report,
            long gracePeriod,
            Map<K, TreeMap<Long, List<TimestampedElement<I>>>> buildBufferByKey,
            Function<I, K> keyExtractor,
            Function<I, Long> timestampExtractor,
            long stateRetentionTime,
            boolean closeOnFirstMatch) {

        this(tick, time, name, cf, report, gracePeriod, buildBufferByKey,
                keyExtractor, timestampExtractor, null, stateRetentionTime, closeOnFirstMatch);
    }

    @Override
    public void compute(I element, long ts) {
        K key = keyExtractor.apply(element);

        log.debug("Received element with key=" + key + " at ts=" + ts + " (mode=" + processingMode + ")");

        // Step 1: Add element to probe buffer
        probeBufferByKey
                .computeIfAbsent(key, k -> new TreeMap<>())
                .computeIfAbsent(ts, t -> new ArrayList<>())
                .add(new TimestampedElement<>(element, ts));

        // Step 2: Check and update pending windows for this key from build side
        // (When a new element arrives, it might match pending windows from the other stream)
        checkAndMatchPendingWindows(element, key, ts);

        // Step 3: Create a new pending window for this probe element
        long graceDeadline = ts + gracePeriod;
        Window window = new WindowImpl(ts, graceDeadline);
        Content<I, W, R> content = cf.create();
        content.add(element);

        PendingWindow<I, K, W, R> pendingWindow = new PendingWindow<>(
                key, element, ts, graceDeadline, window, content);

        // Step 4: Look for immediate matches in build buffer
        boolean foundMatch = findMatchesInBuildBuffer(pendingWindow, ts);

        // Step 5: Handle based on processing mode and match result
        if (foundMatch && closeOnFirstMatch) {
            // Immediately close and report this window
            completeWindow(pendingWindow, ts);
        } else if (foundMatch && processingMode == ProcessingMode.INCREMENTAL) {
            // INCREMENTAL: Report immediately but keep window open for more matches
            if (report.report(window, content, ts, System.currentTimeMillis())) {
                reportedWindows.add(window);
                time.addEvaluationTimeInstants(new TimeInstant(ts));
                log.debug("INCREMENTAL: Reported match immediately for key=" + key);
            }
            // Still add to pending windows for potential future matches
            pendingWindows.computeIfAbsent(key, k -> new ArrayList<>()).add(pendingWindow);
        } else {
            // BATCH mode or no match: Add to pending windows (wait for matches or grace period expiry)
            pendingWindows.computeIfAbsent(key, k -> new ArrayList<>()).add(pendingWindow);
            log.debug("BATCH: Added to pending, will complete at grace deadline=" + graceDeadline);
        }

        // Step 6: Check for expired windows and complete them
        checkExpiredWindows(ts);

        // Step 7: Update application time
        time.setAppTime(ts);

        // Step 8: Evict old state
        evictOldState(ts);
    }

    /**
     * Find matches in the build buffer for a pending window
     */
    private boolean findMatchesInBuildBuffer(PendingWindow<I, K, W, R> pendingWindow, long currentTime) {
        K key = pendingWindow.key;
        TreeMap<Long, List<TimestampedElement<I>>> eventsForKey = buildBufferByKey.get(key);

        if (eventsForKey == null || eventsForKey.isEmpty()) {
            log.debug("No events found for key=" + key + " in build buffer");
            return false;
        }

        // Find all events for this key (no time restriction - dynamic join)
        boolean foundMatch = false;
        for (Map.Entry<Long, List<TimestampedElement<I>>> entry : eventsForKey.entrySet()) {
            for (TimestampedElement<I> candidate : entry.getValue()) {
                // Create joined result if factory is provided
                if (joinResultFactory != null) {
                    I joinedElement = joinResultFactory.createJoinedElement(
                            pendingWindow.probeElement, key, pendingWindow.probeTimestamp,
                            candidate.element, candidate.timestamp);
                    pendingWindow.content.add(joinedElement);
                } else {
                    pendingWindow.content.add(candidate.element);
                }
                foundMatch = true;
                pendingWindow.matched = true;
                log.debug("Matched: " + pendingWindow.probeElement + " with " +
                        candidate.element + " (key=" + key + ")");

                if (closeOnFirstMatch) {
                    return true; // Stop after first match
                }
            }
        }
        return foundMatch;
    }

    /**
     * Check pending windows and match them with a newly arrived build element
     */
    private void checkAndMatchPendingWindows(I newBuildElement, K key, long ts) {
        List<PendingWindow<I, K, W, R>> windowsForKey = pendingWindows.get(key);
        if (windowsForKey == null || windowsForKey.isEmpty()) {
            return;
        }

        List<PendingWindow<I, K, W, R>> toComplete = new ArrayList<>();

        for (PendingWindow<I, K, W, R> pendingWindow : windowsForKey) {
            // Only match with windows that haven't expired
            if (!pendingWindow.isExpired(ts)) {
                if (joinResultFactory != null) {
                    I joinedElement = joinResultFactory.createJoinedElement(
                            pendingWindow.probeElement, key, pendingWindow.probeTimestamp,
                            newBuildElement, ts);
                    pendingWindow.content.add(joinedElement);
                } else {
                    pendingWindow.content.add(newBuildElement);
                }
                pendingWindow.matched = true;
                log.debug("Late match: " + pendingWindow.probeElement + " with " +
                        newBuildElement + " (key=" + key + ")");

                // INCREMENTAL MODE: Report immediately on each late match
                if (processingMode == ProcessingMode.INCREMENTAL) {
                    if (report.report(pendingWindow.window, pendingWindow.content, ts, System.currentTimeMillis())) {
                        if (!reportedWindows.contains(pendingWindow.window)) {
                            reportedWindows.add(pendingWindow.window);
                        }
                        time.addEvaluationTimeInstants(new TimeInstant(ts));
                        log.debug("INCREMENTAL: Reported late match for key=" + key);
                    }
                }

                if (closeOnFirstMatch) {
                    toComplete.add(pendingWindow);
                }
            }
        }

        // Complete windows that should close on first match
        for (PendingWindow<I, K, W, R> pw : toComplete) {
            completeWindow(pw, ts);
            windowsForKey.remove(pw);
        }
    }

    /**
     * Check for expired pending windows and complete them
     */
    private void checkExpiredWindows(long currentTime) {
        List<K> emptyKeys = new ArrayList<>();

        for (Map.Entry<K, List<PendingWindow<I, K, W, R>>> entry : pendingWindows.entrySet()) {
            K key = entry.getKey();
            List<PendingWindow<I, K, W, R>> windows = entry.getValue();
            List<PendingWindow<I, K, W, R>> toRemove = new ArrayList<>();

            for (PendingWindow<I, K, W, R> pw : windows) {
                if (pw.isExpired(currentTime)) {
                    completeWindow(pw, currentTime);
                    toRemove.add(pw);
                }
            }

            windows.removeAll(toRemove);
            if (windows.isEmpty()) {
                emptyKeys.add(key);
            }
        }

        // Clean up empty key entries
        emptyKeys.forEach(pendingWindows::remove);
    }

    /**
     * Complete a pending window (either matched or expired)
     */
    private void completeWindow(PendingWindow<I, K, W, R> pendingWindow, long currentTime) {
        completedWindows.put(pendingWindow.window, pendingWindow.content);

        // In INCREMENTAL mode: 
        // - Don't report again if already reported (matched windows)
        // - Don't report unmatched windows at all (only report on actual matches)
        // In BATCH mode: Always report (matched or unmatched)
        boolean shouldReport = (processingMode == ProcessingMode.BATCH) ||
                               (processingMode == ProcessingMode.INCREMENTAL && pendingWindow.matched && 
                                !reportedWindows.contains(pendingWindow.window));

        if (shouldReport && report.report(pendingWindow.window, pendingWindow.content, currentTime, System.currentTimeMillis())) {
            if (!reportedWindows.contains(pendingWindow.window)) {
                reportedWindows.add(pendingWindow.window);
            }
            time.addEvaluationTimeInstants(new TimeInstant(currentTime));
        }

        log.debug("Completed window for key=" + pendingWindow.key +
                ", matched=" + pendingWindow.matched +
                ", mode=" + processingMode +
                ", content size=" + getContentSize(pendingWindow.content));
    }

    private int getContentSize(Content<I, W, R> content) {
        R coalesced = content.coalesce();
        int count = 0;
        for (Object ignored : coalesced) {
            count++;
        }
        return count;
    }

    /**
     * Add an element to the build buffer (static helper - does NOT trigger matching)
     */
    public static <I, K> void addToBuildBuffer(
            Map<K, TreeMap<Long, List<TimestampedElement<I>>>> buffer,
            I element,
            K key,
            long timestamp) {

        buffer.computeIfAbsent(key, k -> new TreeMap<>())
                .computeIfAbsent(timestamp, t -> new ArrayList<>())
                .add(new TimestampedElement<>(element, timestamp));
    }

    /**
     * Add an element to the build buffer AND trigger matching with pending windows.
     * Use this method when you want immediate matching behavior.
     *
     * @param element   The element to add
     * @param key       The join key
     * @param timestamp The timestamp
     */
    public void addToBuildBufferAndMatch(I element, K key, long timestamp) {
        // Add to build buffer
        buildBufferByKey.computeIfAbsent(key, k -> new TreeMap<>())
                .computeIfAbsent(timestamp, t -> new ArrayList<>())
                .add(new TimestampedElement<>(element, timestamp));

        // Check and match pending windows for this key
        List<PendingWindow<I, K, W, R>> windowsForKey = pendingWindows.get(key);
        if (windowsForKey == null || windowsForKey.isEmpty()) {
            log.debug("No pending windows for key=" + key);
            return;
        }

        List<PendingWindow<I, K, W, R>> toComplete = new ArrayList<>();

        for (PendingWindow<I, K, W, R> pendingWindow : windowsForKey) {
            // Only match with windows that haven't expired
            if (!pendingWindow.isExpired(timestamp)) {
                if (joinResultFactory != null) {
                    I joinedElement = joinResultFactory.createJoinedElement(
                            pendingWindow.probeElement, key, pendingWindow.probeTimestamp,
                            element, timestamp);
                    pendingWindow.content.add(joinedElement);
                } else {
                    pendingWindow.content.add(element);
                }
                pendingWindow.matched = true;
                log.debug("Matched pending window: " + pendingWindow.probeElement + 
                        " with build element " + element + " (key=" + key + ")");

                // INCREMENTAL MODE: Report immediately on each match
                if (processingMode == ProcessingMode.INCREMENTAL) {
                    if (report.report(pendingWindow.window, pendingWindow.content, timestamp, System.currentTimeMillis())) {
                        if (!reportedWindows.contains(pendingWindow.window)) {
                            reportedWindows.add(pendingWindow.window);
                        }
                        time.addEvaluationTimeInstants(new TimeInstant(timestamp));
                        log.debug("INCREMENTAL: Reported match immediately for key=" + key);
                    }
                }

                if (closeOnFirstMatch) {
                    toComplete.add(pendingWindow);
                }
            }
        }

        // Complete windows that should close on first match
        for (PendingWindow<I, K, W, R> pw : toComplete) {
            completeWindow(pw, timestamp);
            windowsForKey.remove(pw);
        }
    }

    /**
     * Get the probe buffer for sharing with another operator (bidirectional joins)
     */
    public Map<K, TreeMap<Long, List<TimestampedElement<I>>>> getProbeBufferByKey() {
        return probeBufferByKey;
    }

    private void evictOldState(long currentTime) {
        long evictionThreshold = currentTime - stateRetentionTime;

        // Evict from probe buffer
        for (TreeMap<Long, List<TimestampedElement<I>>> keyBuffer : probeBufferByKey.values()) {
            keyBuffer.headMap(evictionThreshold, true).clear();
        }
        probeBufferByKey.entrySet().removeIf(entry -> entry.getValue().isEmpty());

        // Evict from build buffer
        for (TreeMap<Long, List<TimestampedElement<I>>> keyBuffer : buildBufferByKey.values()) {
            keyBuffer.headMap(evictionThreshold, true).clear();
        }
        buildBufferByKey.entrySet().removeIf(entry -> entry.getValue().isEmpty());

        // Evict old completed windows
        completedWindows.entrySet().removeIf(entry -> entry.getKey().getC() < evictionThreshold);

        // Clear old reported windows
        reportedWindows.removeIf(w -> w.getC() < evictionThreshold);
    }

    @Override
    public Content<I, W, R> content(long t_e) {
        // First, find completed windows that start at exactly t_e (most specific match)
        Optional<Window> exactMatch = completedWindows.keySet().stream()
                .filter(w -> w.getO() == t_e)
                .findFirst();

        if (exactMatch.isPresent()) {
            return completedWindows.get(exactMatch.get());
        }

        // Check completed windows that contain t_e
        Optional<Window> matchingWindow = completedWindows.keySet().stream()
                .filter(w -> w.getO() <= t_e && t_e <= w.getC())
                .max(Comparator.comparingLong(Window::getC));

        if (matchingWindow.isPresent()) {
            return completedWindows.get(matchingWindow.get());
        }

        // Check pending windows
        for (List<PendingWindow<I, K, W, R>> windows : pendingWindows.values()) {
            for (PendingWindow<I, K, W, R> pw : windows) {
                if (pw.window.getO() == t_e || (pw.window.getO() <= t_e && t_e <= pw.window.getC())) {
                    return pw.content;
                }
            }
        }

        return cf.createEmpty();
    }

    @Override
    public List<Content<I, W, R>> getContents(long t_e) {
        List<Content<I, W, R>> results = new ArrayList<>();

        // Add from completed windows
        if (!reportedWindows.isEmpty()) {
            for (Window w : reportedWindows) {
                Content<I, W, R> c = completedWindows.get(w);
                if (c != null) {
                    results.add(c);
                }
            }
        }

        // Also check pending windows
        for (List<PendingWindow<I, K, W, R>> windows : pendingWindows.values()) {
            for (PendingWindow<I, K, W, R> pw : windows) {
                if (pw.window.getO() <= t_e && t_e <= pw.window.getC()) {
                    results.add(pw.content);
                }
            }
        }

        return results;
    }

    @Override
    public TimeVarying<R> get() {
        return new TimeVaryingObject<>(this, name);
    }

    @Override
    public Report report() {
        return report;
    }

    @Override
    public Tick tick() {
        return tick;
    }

    @Override
    public Time time() {
        return time;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void evict() {
        reportedWindows.clear();
    }

    @Override
    public void evict(long ts) {
        evictOldState(ts);
        reportedWindows.clear();
    }

    // ========== Utility methods for testing/debugging ==========

    /**
     * Get the number of pending windows
     */
    public int getPendingWindowCount() {
        return pendingWindows.values().stream()
                .mapToInt(List::size)
                .sum();
    }

    /**
     * Get the number of completed windows
     */
    public int getCompletedWindowCount() {
        return completedWindows.size();
    }

    /**
     * Get pending windows for a specific key
     */
    public List<PendingWindow<I, K, W, R>> getPendingWindowsForKey(K key) {
        return pendingWindows.getOrDefault(key, Collections.emptyList());
    }

    /**
     * Check if a key has any matched pending windows
     */
    public boolean hasMatchedWindowForKey(K key) {
        List<PendingWindow<I, K, W, R>> windows = pendingWindows.get(key);
        if (windows == null) return false;
        return windows.stream().anyMatch(pw -> pw.matched);
    }

    /**
     * Force check for expired windows (useful for testing)
     */
    public void forceCheckExpiredWindows(long currentTime) {
        checkExpiredWindows(currentTime);
    }

    /**
     * Get all pending keys
     */
    public Set<K> getPendingKeys() {
        return Collections.unmodifiableSet(pendingWindows.keySet());
    }

    /**
     * Get the number of distinct keys in the probe buffer
     */
    public int getProbeKeyCount() {
        return probeBufferByKey.size();
    }

    /**
     * Get the number of distinct keys in the build buffer
     */
    public int getBuildKeyCount() {
        return buildBufferByKey.size();
    }

    /**
     * Get the grace period
     */
    public long getGracePeriod() {
        return gracePeriod;
    }

    /**
     * Get the current processing mode
     */
    public ProcessingMode getProcessingMode() {
        return processingMode;
    }

    @Override
    public String toString() {
        return "DynamicIntervalJoinS2ROperator{" +
                "name='" + name + '\'' +
                ", gracePeriod=" + gracePeriod +
                ", closeOnFirstMatch=" + closeOnFirstMatch +
                ", processingMode=" + processingMode +
                ", pendingWindows=" + getPendingWindowCount() +
                ", completedWindows=" + completedWindows.size() +
                ", probeKeys=" + probeBufferByKey.size() +
                ", buildKeys=" + buildBufferByKey.size() +
                '}';
    }
}
