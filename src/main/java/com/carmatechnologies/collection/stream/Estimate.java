package com.carmatechnologies.collection.stream;

import java.util.Objects;

public class Estimate {
    private final int count;
    private final int overEstimation;
    private volatile int hashCode;
    private volatile String toString;

    public Estimate(final int count, final int overEstimation) {
        this.count = count;
        this.overEstimation = overEstimation;
    }

    public int count() {
        return count;
    }

    public int overEstimation() {
        return overEstimation;
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Objects.hash(count, overEstimation);
        }
        return hashCode;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == null) return false;
        if (object == this) return true;
        if (!(object instanceof Estimate)) return false;
        final Estimate that = (Estimate) object;
        return (this.count == that.count) && (this.overEstimation == that.overEstimation);
    }

    @Override
    public String toString() {
        if (toString == null) {
            toString = "{\"count\":" + count + ",\"overEstimation\":" + overEstimation + "}";
        }
        return toString;
    }
}
