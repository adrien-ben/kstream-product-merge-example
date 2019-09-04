package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.Price;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Offer {

	private String id;
	private String name;
	private String description;
	private Float price;

	public Offer(String id) {
		this.id = id;
	}

	public void mergePrice(Price price) {
		this.price = price.getAmount();
	}

	public void mergeOfferDetails(OfferDetails details) {
		this.name = details.getName();
		this.description = details.getDescription();
	}

	public OfferAvro toAvro() {
		return OfferAvro.newBuilder()
				.setId(id)
				.setName(name)
				.setDescription(description)
				.setPrice(price)
				.build();
	}
}
