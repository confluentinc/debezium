/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class SnapshotTableDistributorTest {

    @Test
    public void shouldDistributeItemsEvenlyAcrossBuckets() {
        List<String> items = Arrays.asList("A", "B", "C", "D", "E", "F");
        int numBuckets = 3;

        List<List<String>> result = SnapshotTableDistributor.distributeRoundRobin(items, numBuckets);

        assertThat(result).hasSize(3);
        assertThat(result.get(0)).containsExactly("A", "D");
        assertThat(result.get(1)).containsExactly("B", "E");
        assertThat(result.get(2)).containsExactly("C", "F");
    }

    @Test
    public void shouldDistributeItemsWithUnevenDistribution() {
        List<String> items = Arrays.asList("A", "B", "C", "D", "E");
        int numBuckets = 3;

        List<List<String>> result = SnapshotTableDistributor.distributeRoundRobin(items, numBuckets);

        assertThat(result).hasSize(3);
        assertThat(result.get(0)).containsExactly("A", "D");
        assertThat(result.get(1)).containsExactly("B", "E");
        assertThat(result.get(2)).containsExactly("C");
    }

    @Test
    public void shouldHandleSingleBucket() {
        List<String> items = Arrays.asList("A", "B", "C");
        int numBuckets = 1;

        List<List<String>> result = SnapshotTableDistributor.distributeRoundRobin(items, numBuckets);

        assertThat(result).hasSize(1);
        assertThat(result.get(0)).containsExactly("A", "B", "C");
    }

    @Test
    public void shouldHandleMoreBucketsThanItems() {
        List<String> items = Arrays.asList("A", "B");
        int numBuckets = 5;

        List<List<String>> result = SnapshotTableDistributor.distributeRoundRobin(items, numBuckets);

        assertThat(result).hasSize(5);
        assertThat(result.get(0)).containsExactly("A");
        assertThat(result.get(1)).containsExactly("B");
        assertThat(result.get(2)).isEmpty();
        assertThat(result.get(3)).isEmpty();
        assertThat(result.get(4)).isEmpty();
    }

    @Test
    public void shouldHandleEmptyList() {
        List<String> items = Collections.emptyList();
        int numBuckets = 3;

        List<List<String>> result = SnapshotTableDistributor.distributeRoundRobin(items, numBuckets);

        assertThat(result).hasSize(3);
        assertThat(result.get(0)).isEmpty();
        assertThat(result.get(1)).isEmpty();
        assertThat(result.get(2)).isEmpty();
    }

    @Test
    public void shouldHandleNullList() {
        List<String> items = null;
        int numBuckets = 3;

        List<List<String>> result = SnapshotTableDistributor.distributeRoundRobin(items, numBuckets);

        assertThat(result).hasSize(3);
        assertThat(result.get(0)).isEmpty();
        assertThat(result.get(1)).isEmpty();
        assertThat(result.get(2)).isEmpty();
    }

    @Test
    public void shouldThrowExceptionForZeroBuckets() {
        List<String> items = Arrays.asList("A", "B", "C");

        assertThatThrownBy(() -> SnapshotTableDistributor.distributeRoundRobin(items, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Number of buckets must be at least 1");
    }

    @Test
    public void shouldThrowExceptionForNegativeBuckets() {
        List<String> items = Arrays.asList("A", "B", "C");

        assertThatThrownBy(() -> SnapshotTableDistributor.distributeRoundRobin(items, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Number of buckets must be at least 1");
    }

    @Test
    public void shouldDistributeNineTablesAcrossThreeTasks() {
        // Example from the plan: 9 tables across 3 tasks
        List<String> tables = Arrays.asList(
                "inventory.products",
                "inventory.orders",
                "inventory.customers",
                "inventory.items",
                "inventory.suppliers",
                "inventory.shipments",
                "inventory.payments",
                "inventory.reviews",
                "inventory.categories");
        int numTasks = 3;

        List<List<String>> result = SnapshotTableDistributor.distributeRoundRobin(tables, numTasks);

        assertThat(result).hasSize(3);
        // Task 0: products, items, payments
        assertThat(result.get(0)).containsExactly(
                "inventory.products", "inventory.items", "inventory.payments");
        // Task 1: orders, suppliers, reviews
        assertThat(result.get(1)).containsExactly(
                "inventory.orders", "inventory.suppliers", "inventory.reviews");
        // Task 2: customers, shipments, categories
        assertThat(result.get(2)).containsExactly(
                "inventory.customers", "inventory.shipments", "inventory.categories");
    }

    @Test
    public void shouldWorkWithGenericTypes() {
        // Test with Integer type to verify generic implementation
        List<Integer> items = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        int numBuckets = 4;

        List<List<Integer>> result = SnapshotTableDistributor.distributeRoundRobin(items, numBuckets);

        assertThat(result).hasSize(4);
        assertThat(result.get(0)).containsExactly(1, 5, 9);
        assertThat(result.get(1)).containsExactly(2, 6, 10);
        assertThat(result.get(2)).containsExactly(3, 7);
        assertThat(result.get(3)).containsExactly(4, 8);
    }

    @Test
    public void shouldHandleSingleItem() {
        List<String> items = Collections.singletonList("A");
        int numBuckets = 3;

        List<List<String>> result = SnapshotTableDistributor.distributeRoundRobin(items, numBuckets);

        assertThat(result).hasSize(3);
        assertThat(result.get(0)).containsExactly("A");
        assertThat(result.get(1)).isEmpty();
        assertThat(result.get(2)).isEmpty();
    }
}
