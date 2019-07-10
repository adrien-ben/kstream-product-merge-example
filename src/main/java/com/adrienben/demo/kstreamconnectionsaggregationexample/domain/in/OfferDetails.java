package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in;

import lombok.Data;

@Data
public class OfferDetails {
	private String offerId;
	private String productId;
	private String skuId;
	private String name;
	private String description;
}
