package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out;

import com.adrienben.demo.domain.out.OfferProto;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.Price;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import static com.adrienben.demo.kstreamconnectionsaggregationexample.util.Utils.setIfNotNull;

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

	public OfferProto toProto() {
		var offer = OfferProto.newBuilder();
		setIfNotNull(offer, id, OfferProto.Builder::setId);
		setIfNotNull(offer, name, OfferProto.Builder::setName);
		setIfNotNull(offer, description, OfferProto.Builder::setDescription);
		setIfNotNull(offer, price, OfferProto.Builder::setPrice);
		return offer.build();
	}
}
