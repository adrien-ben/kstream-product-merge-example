package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SkuDetails {
	private String skuId;
	private String productId;
	private String name;
	private String description;

	public static SkuDetails fromAvro(SkuDetailsAvro skuDetailsAvro) {
		return new SkuDetails(
				skuDetailsAvro.getSkuId().toString(),
				skuDetailsAvro.getProductId().toString(),
				skuDetailsAvro.getName().toString(),
				skuDetailsAvro.getDescription().toString());
	}
}
