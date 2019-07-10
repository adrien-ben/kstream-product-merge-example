package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.Price;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.ProductDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.SkuDetails;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Product {

	private String id;
	private String name;
	private String description;
	private String brand;
	private List<Sku> skus = new ArrayList<>();

	public Product(String id) {
		this.id = id;
	}

	@JsonIgnore
	public boolean isComplete() {
		return id != null && !"".equals(id) && name != null && brand != null;
	}

	public void mergeProductDetails(ProductDetails productDetails) {
		this.name = productDetails.getName();
		this.description = productDetails.getDescription();
		this.brand = productDetails.getBrand();
	}

	public void mergeSkuDetails(SkuDetails skuDetails) {
		mergeSkuData(skuDetails.getSkuId(), sku -> sku.mergeDetails(skuDetails));
	}

	public void mergePrice(Price price) {
		mergeSkuData(price.getSkuId(), sku -> sku.mergePrice(price));
	}

	public void mergeOfferDetails(OfferDetails offerDetails) {
		mergeSkuData(offerDetails.getSkuId(), sku -> sku.mergeOfferDetails(offerDetails));
	}

	private void mergeSkuData(String skuId, Consumer<Sku> merger) {
		var sku = getSkuById(skuId);
		merger.accept(sku);
	}

	private Sku getSkuById(String id) {
		return skus.stream()
				.filter(s -> s.getId().equals(id))
				.findFirst()
				.orElseGet(() -> {
					var newSku = new Sku(id);
					skus.add(newSku);
					return newSku;
				});
	}
}
