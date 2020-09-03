package com.adrienben.demo.kstreamconnectionsaggregationexample.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.function.BiConsumer;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Utils {
	public static <T, V> void setIfNotNull(T target, V value, BiConsumer<T, V> setter) {
		if (value != null) {
			setter.accept(target, value);
		}
	}
}
