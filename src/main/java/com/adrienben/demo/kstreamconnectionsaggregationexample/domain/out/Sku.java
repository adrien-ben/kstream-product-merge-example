package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out;

import com.adrienben.demo.domain.out.SkuProto;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.Price;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.SkuDetails;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.adrienben.demo.kstreamconnectionsaggregationexample.util.Utils.setIfNotNull;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Sku {

	private String id;
	private String name;
	private String description;
	private List<Offer> offers = new ArrayList<>();

	public Sku(String id) {
		this.id = id;
	}

	public void mergeDetails(SkuDetails details) {
		this.name = details.getName();
		this.description = details.getDescription();
	}

	public void mergePrice(Price price) {
		mergeOfferData(price.getOfferId(), offer -> offer.mergePrice(price));
	}

	public void mergeOfferDetails(OfferDetails offerDetails) {
		mergeOfferData(offerDetails.getOfferId(), offer -> offer.mergeOfferDetails(offerDetails));
	}

	private void mergeOfferData(String offerId, Consumer<Offer> merger) {
		var offer = getOfferById(offerId);
		merger.accept(offer);
	}

	private Offer getOfferById(String offerId) {
		return offers.stream()
				.filter((o -> o.getId().equals(offerId)))
				.findFirst()
				.orElseGet(() -> {
					var newOffer = new Offer(offerId);
					offers.add(newOffer);
					return newOffer;
				});
	}

	public SkuProto toProto() {
		var sku = SkuProto.newBuilder();
		setIfNotNull(sku, id, SkuProto.Builder::setId);
		setIfNotNull(sku, name, SkuProto.Builder::setName);
		setIfNotNull(sku, description, SkuProto.Builder::setDescription);
		return sku.addAllOffers(offers.stream().map(Offer::toProto).collect(Collectors.toList())).build();
	}
}
