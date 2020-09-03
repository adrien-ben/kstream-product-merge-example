package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OfferDetails {
	private String offerId;
	private String productId;
	private String skuId;
	private String name;
	private String description;

	public static OfferDetails fromAvro(OfferDetailsAvro offerDetailsAvro) {
		return new OfferDetails(
				offerDetailsAvro.getOfferId().toString(),
				offerDetailsAvro.getProductId().toString(),
				offerDetailsAvro.getSkuId().toString(),
				offerDetailsAvro.getName().toString(),
				offerDetailsAvro.getDescription().toString());
	}
}
