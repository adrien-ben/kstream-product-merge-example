package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in;

import lombok.Data;

@Data
public class Price {
	private String offerId;
	private String productId;
	private String skuId;
	private Float amount;
}
