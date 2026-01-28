/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class for distributing tables across multiple snapshot tasks.
 * Used when {@code tasks.max > 1} to enable parallel snapshot processing.
 */
public final class SnapshotTableDistributor {

    /**
     * Distributes items across a specified number of buckets using round-robin distribution.
     * This ensures an even distribution of work across all tasks.
     *
     * @param <T> the type of items to distribute
     * @param items the list of items to distribute
     * @param numBuckets the number of buckets (tasks) to distribute across
     * @return a list of lists, where each inner list contains the items assigned to that bucket
     * @throws IllegalArgumentException if numBuckets is less than 1
     */
    public static <T> List<List<T>> distributeRoundRobin(List<T> items, int numBuckets) {
        if (numBuckets < 1) {
            throw new IllegalArgumentException("Number of buckets must be at least 1");
        }

        if (items == null || items.isEmpty()) {
            List<List<T>> emptyBuckets = new ArrayList<>();
            for (int i = 0; i < numBuckets; i++) {
                emptyBuckets.add(Collections.emptyList());
            }
            return emptyBuckets;
        }

        List<List<T>> buckets = new ArrayList<>();
        for (int i = 0; i < numBuckets; i++) {
            buckets.add(new ArrayList<>());
        }

        for (int i = 0; i < items.size(); i++) {
            buckets.get(i % numBuckets).add(items.get(i));
        }
        return buckets;
    }

    private SnapshotTableDistributor() {
        // intentionally private
    }
}
