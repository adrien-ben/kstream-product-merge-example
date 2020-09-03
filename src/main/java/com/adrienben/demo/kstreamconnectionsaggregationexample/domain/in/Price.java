package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Price {
	private String offerId;
	private String productId;
	private String skuId;
	private Float amount;

	public static Price fromAvro(PriceAvro priceAvro) {
		return new Price(
				priceAvro.getOfferId().toString(),
				priceAvro.getProductId().toString(),
				priceAvro.getSkuId().toString(),
				priceAvro.getAmount());
	}
}
