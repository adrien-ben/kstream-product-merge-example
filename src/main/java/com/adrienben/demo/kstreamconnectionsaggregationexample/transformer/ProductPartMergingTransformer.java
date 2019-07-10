package com.adrienben.demo.kstreamconnectionsaggregationexample.transformer;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Product;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.function.BiConsumer;

/**
 * Utility class used to create {@link MergingTransformer} instances that merge product parts into a {@link Product}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ProductPartMergingTransformer {

	public static final String PRODUCT_STORE_NAME = "product_store";

	/**
	 * Create a {@link MergingTransformer} instance that merges product parts into a {@link Product}.
	 *
	 * @param productPartMergingFunction The function used to merge the product part into the product.
	 * @param <V>                        The type of the part to merge.
	 * @return A new {@link MergingTransformer} instance.
	 */
	public static <V> MergingTransformer<String, Product, V> fromMergingFunction(BiConsumer<Product, V> productPartMergingFunction) {
		return new MergingTransformer<>(PRODUCT_STORE_NAME, Product::new, productPartMergingFunction, Product::isComplete);
	}
}
