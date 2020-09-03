package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in;

import com.adrienben.demo.domain.in.ProductDetailsProto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductDetails {
	private String id;
	private String name;
	private String description;
	private String brand;

	public static ProductDetails fromProto(ProductDetailsProto productDetailsProto) {
		return new ProductDetails(
				null,
				productDetailsProto.getName(),
				productDetailsProto.getDescription(),
				productDetailsProto.getBrand());
	}
}
