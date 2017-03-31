package com.carmatechnologies.collection.stream;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public interface TopK<T> {
    TopK<T> handle(Stream<T> elements);
    TopK<T> handle(Iterator<T> elements);
    TopK<T> handle(T element);
    Map<T, Estimate> summary();
    int size();
}
