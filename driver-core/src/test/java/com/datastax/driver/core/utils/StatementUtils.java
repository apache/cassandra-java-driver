package com.datastax.driver.core.utils;

import java.lang.reflect.Array;
import java.util.ArrayList;

public class StatementUtils {
    /**
     * Create a list containing {@code n} times a given {@code value}.
     * <p/>
     * This is identical to the Java 8 form : {@code Stream.generate(() -> value).limit(n).collect(toList())}
     *
     * @param n     the number of time to repeat a value
     * @param value the value to contain multiple times
     * @param <E>   The type of the value
     * @return a list of {@code n} references to {@code value}
     */
    public static <E> ArrayList<E> listOf(int n, E value) {
        ArrayList<E> inArgs = new ArrayList<E>(n);
        for (int i = 0; i < n; i++) {
            inArgs.add(value);
        }
        return inArgs;
    }

    /**
     * Create n array containing {@code n} times a given {@code value}.
     *
     * @param n     the number of time to repeat a value
     * @param value the value to contain multiple times
     * @param <E>   The type of the value
     * @return an array of {@code n} references to {@code value}
     */
    @SuppressWarnings("unchecked")
    public static <E> E[] arrayOf(int n, E value) {
        return (E[]) listOf(n, value).toArray();
    }

}
