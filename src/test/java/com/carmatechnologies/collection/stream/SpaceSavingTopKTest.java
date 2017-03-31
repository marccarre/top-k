package com.carmatechnologies.collection.stream;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SpaceSavingTopKTest {
    @Test
    public void xyxz() {
        SpaceSavingTopK<String> streamSummary = new SpaceSavingTopK<>(2);
        assertThat(streamSummary.size(), is(0));
        assertThat(streamSummary.min(), is(0));

        streamSummary.handle("X");
        assertThat(streamSummary.size(), is(1));
        assertThat(streamSummary.summary(), is(ImmutableMap.of("X", new Estimate(1, 0))));
        assertThat(streamSummary.min(), is(0)); // We still have the default 0-bucket as the data structure is not full.

        streamSummary.handle("Y");
        assertThat(streamSummary.size(), is(2));
        assertThat(streamSummary.summary(), is(ImmutableMap.of("X", new Estimate(1, 0), "Y", new Estimate(1, 0))));
        assertThat(streamSummary.min(), is(1));

        streamSummary.handle("X");
        assertThat(streamSummary.size(), is(2));
        assertThat(streamSummary.summary(), is(ImmutableMap.of("X", new Estimate(2, 0), "Y", new Estimate(1, 0))));
        assertThat(streamSummary.min(), is(1));

        streamSummary.handle("Z");
        assertThat(streamSummary.size(), is(2));
        assertThat(streamSummary.min(), is(2));
        assertThat(streamSummary.summary(), is(ImmutableMap.of("X", new Estimate(2, 0), "Z", new Estimate(2, 1))));
    }

    @Test
    public void xzyy() {
        SpaceSavingTopK<String> streamSummary = new SpaceSavingTopK<>(2);
        assertThat(streamSummary.size(), is(0));
        assertThat(streamSummary.min(), is(0));

        streamSummary.handle("X");
        assertThat(streamSummary.size(), is(1));
        assertThat(streamSummary.summary(), is(ImmutableMap.of("X", new Estimate(1, 0))));
        assertThat(streamSummary.min(), is(0)); // We still have the default 0-bucket as the data structure is not full.

        streamSummary.handle("Z");
        assertThat(streamSummary.size(), is(2));
        assertThat(streamSummary.summary(), is(ImmutableMap.of("X", new Estimate(1, 0), "Z", new Estimate(1, 0))));
        assertThat(streamSummary.min(), is(1));

        streamSummary.handle("Y");
        assertThat(streamSummary.size(), is(2));
        assertThat(streamSummary.summary(), is(ImmutableMap.of("Y", new Estimate(2, 1), "X", new Estimate(1, 0))));
        assertThat(streamSummary.min(), is(1));

        streamSummary.handle("Y");
        assertThat(streamSummary.size(), is(2));
        assertThat(streamSummary.min(), is(1));
        assertThat(streamSummary.summary(), is(ImmutableMap.of("Y", new Estimate(3, 1), "X", new Estimate(1, 0))));
    }

    @Test
    public void xyyz() {
        SpaceSavingTopK<String> streamSummary = new SpaceSavingTopK<>(2);
        assertThat(streamSummary.size(), is(0));
        assertThat(streamSummary.min(), is(0));

        streamSummary.handle("X");
        assertThat(streamSummary.size(), is(1));
        assertThat(streamSummary.summary(), is(ImmutableMap.of("X", new Estimate(1, 0))));
        assertThat(streamSummary.min(), is(0)); // We still have the default 0-bucket as the data structure is not full.

        streamSummary.handle("Y");
        assertThat(streamSummary.size(), is(2));
        assertThat(streamSummary.summary(), is(ImmutableMap.of("X", new Estimate(1, 0), "Y", new Estimate(1, 0))));
        assertThat(streamSummary.min(), is(1));

        streamSummary.handle("Y");
        assertThat(streamSummary.size(), is(2));
        assertThat(streamSummary.summary(), is(ImmutableMap.of("Y", new Estimate(2, 0), "X", new Estimate(1, 0))));
        assertThat(streamSummary.min(), is(1));

        streamSummary.handle("Z");
        assertThat(streamSummary.size(), is(2));
        assertThat(streamSummary.min(), is(2));
        assertThat(streamSummary.summary(), is(ImmutableMap.of("Y", new Estimate(2, 0), "Z", new Estimate(2, 1))));
    }

    @Test
    public void loadTestOnLinearDistribution() {
        List<Integer> stream = stream(1000); // 1000*1001/2 = 500500 elements: (1, 2,2, 3,3,3, ..., 1000)
        SpaceSavingTopK<Integer> streamSummary = new SpaceSavingTopK<>(10);
        streamSummary.handle(stream.iterator());
        System.out.println(streamSummary.summary());
    }

    private static List<Integer> stream(final int n) {
        final List<Integer> elements = new ArrayList<>(n * (n + 1) / 2);
        for (int i = 1; i <= n; ++i) {
            for (int j = 0; j < i; ++j) {
                elements.add(i);
            }
        }
        // At this point, elements contains: 1, 2, 2, 3, 3, 3, etc.
        Collections.shuffle(elements);
        return elements;
    }
}