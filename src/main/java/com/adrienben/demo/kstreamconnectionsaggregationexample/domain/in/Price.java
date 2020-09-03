package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in;

import com.adrienben.demo.domain.in.PriceProto;
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

	public static Price fromProto(PriceProto priceProto) {
		return new Price(
				priceProto.getOfferId(),
				priceProto.getProductId(),
				priceProto.getSkuId(),
				priceProto.getAmount());
	}
}
