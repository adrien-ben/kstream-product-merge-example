package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in;

import com.adrienben.demo.domain.in.SkuDetailsProto;
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

	public static SkuDetails fromProto(SkuDetailsProto skuDetailsProto) {
		return new SkuDetails(
				skuDetailsProto.getSkuId(),
				skuDetailsProto.getProductId(),
				skuDetailsProto.getName(),
				skuDetailsProto.getDescription());
	}
}
