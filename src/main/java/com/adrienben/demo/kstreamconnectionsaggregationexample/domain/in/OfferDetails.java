package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in;

import com.adrienben.demo.domain.in.OfferDetailsProto;
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

	public static OfferDetails fromProto(OfferDetailsProto offerDetailsProto) {
		return new OfferDetails(
				offerDetailsProto.getOfferId(),
				offerDetailsProto.getProductId(),
				offerDetailsProto.getSkuId(),
				offerDetailsProto.getName(),
				offerDetailsProto.getDescription());
	}
}
