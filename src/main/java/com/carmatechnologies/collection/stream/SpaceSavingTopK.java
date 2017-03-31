package com.carmatechnologies.collection.stream;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

@NotThreadSafe
public class SpaceSavingTopK<T extends Comparable<T>> implements TopK<T> {
    /**
     * Doubly-linked node, for O(1) direct access, remove, and add operations.
     *
     * @param <T> Type of the value the node carries.
     */
    private static class Node<T> {
        // N.B.: by default, singleton nodes point to themselves.
        // This makes pointers maintenance easier and avoids special cases.
        //    +--+
        //    |  v
        //    | +--------+
        //    +-|  this  |-+
        //      +--------+ |
        //              ^  |
        //              +--+
        Node<T> prev = this;
        Node<T> next = this;
        private final T value;

        Node(final T value) {
            this.value = value;
        }

        T value() {
            return value;
        }

        void add(final Node<T> node) {
            node.prev = this;
            node.next = next;
            next.prev = node;
            next = node;
        }

        void remove() {
            prev.next = next;
            next.prev = prev;
            prev = this;
            next = this;
        }

        boolean isSingleton() {
            return (prev == this) && (next == this);
        }
    }

    /**
     * A bucket effectively is a label with a number on it.
     * A bucket also points to the list of counters pointing to it, so that
     * it can list values with a count equal to the number the bucket represents.
     *
     * @param <T> Type of the values having a count equal to the number the bucket represents.
     */
    private static class Bucket<T> extends Node<T> {
        private final int count;
        private final Counter<T> headCounter = new Counter<>(null, 0);

        Bucket(final int count) {
            super(null); // We never store any value of type T directly in a bucket.
            this.count = count;
        }

        Bucket<T> prev() {
            return (Bucket<T>) prev;
        }

        Bucket<T> next() {
            return (Bucket<T>) next;
        }

        int count() {
            return count;
        }

        boolean isEmpty() {
            return headCounter.isSingleton();
        }

        void add(final Counter<T> counter) {
            headCounter.prev.add(counter);
        }

        Iterator<Counter<T>> iterator() {
            return new Iterator<Counter<T>>() {
                private Counter<T> counter = headCounter.next();

                @Override
                public boolean hasNext() {
                    return (counter != headCounter);
                }

                @Override
                public Counter<T> next() {
                    final Counter<T> result = counter;
                    counter = counter.next();
                    return result;
                }
            };
        }
    }

    private static class Counter<T> extends Node<T> {
        private final int overEstimation;
        private Bucket<T> bucket;

        Counter(final T value, final int overEstimation) {
            super(value);
            this.overEstimation = overEstimation;
            this.bucket = null;
        }

        Counter(final T value, final int overEstimation, final Bucket<T> bucket) {
            this(value, overEstimation);
            this.bucket = bucket;
        }

        Counter<T> prev() {
            return (Counter<T>) prev;
        }

        Counter<T> next() {
            return (Counter<T>) next;
        }

        int count() {
            return bucket.count();
        }

        int overEstimation() {
            return overEstimation;
        }

        Bucket<T> increment() {
            final int newCount = bucket.count() + 1;
            if (bucket.next().count() != newCount) {
                bucket.add(new Bucket<>(newCount));
            }
            remove(); // Remove counter from current doubly-linked list of counters, i.e. current bucket.
            bucket.next().add(this); // Add counter to the doubly-linked list of the next bucket, i.e. next bucket.
            bucket = bucket.next();
            return bucket.prev(); // Return the former bucket.
        }
    }

    private Bucket<T> minBucket = new Bucket<>(0);
    private final Map<T, Counter<T>> counters;
    private final int m;

    public SpaceSavingTopK(final int m) {
        this.m = m;
        counters = new HashMap<>(m);
    }

    @Override
    public TopK<T> handle(final Stream<T> elements) {
        elements.forEach(this::handle);
        return this;
    }

    @Override
    public TopK<T> handle(final Iterator<T> elements) {
        while (elements.hasNext()) {
            handle(elements.next());
        }
        return this;
    }

    @Override
    public TopK<T> handle(final T element) {
        final Counter<T> counter = counters.computeIfAbsent(
                element,
                o -> new Counter<>(element, minBucket.count(), minBucket)
        );

        final Bucket<T> formerBucket = counter.increment();

        if (size() > m) {
            Counter<T> smallest = minBucket.headCounter.prev();
            if (smallest == counter) {
                smallest = smallest.prev();
            }
            smallest.remove();
            counters.remove(smallest.value());
        }

        // If the bucket is empty, we want to remove it.
        // However, we need to keep the default 0-label if the data structure is not full yet.
        if (formerBucket.isEmpty() && !isInitialDefaultBucket(formerBucket)) {
            // If we are removing the current minimum bucket, we need to update it to the new minimum:
            if (minBucket == formerBucket) {
                minBucket = formerBucket.next();
            }
            // Now safe to remove the empty bucket:
            formerBucket.remove();
        }

        return this;
    }

    private boolean isInitialDefaultBucket(final Bucket<T> bucket) {
        return (size() < m) && (bucket == minBucket);
    }

    @Override
    public Map<T, Estimate> summary() {
        final Map<T, Estimate> summary = new LinkedHashMap<>(size());
        Bucket<T> bucket = minBucket.prev();
        boolean done = false;
        while (!done) {
            if (bucket == minBucket) {
                done = true;
            }
            for (final Iterator<Counter<T>> iterator = bucket.iterator(); iterator.hasNext(); ) {
                final Counter<T> counter = iterator.next();
                summary.put(counter.value(), new Estimate(counter.count(), counter.overEstimation()));
            }
            bucket = bucket.prev();
        }
        return summary;
    }

    @Override
    public int size() {
        return counters.size();
    }

    // Package private, for testing purposes:
    int min() {
        return minBucket.count();
    }
}
