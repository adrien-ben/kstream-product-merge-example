package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductDetails {
	private String name;
	private String description;
	private String brand;

	public static ProductDetails fromAvro(ProductDetailsAvro productDetailsAvro) {
		return new ProductDetails(
				productDetailsAvro.getName(),
				productDetailsAvro.getDescription(),
				productDetailsAvro.getBrand());
	}
}
