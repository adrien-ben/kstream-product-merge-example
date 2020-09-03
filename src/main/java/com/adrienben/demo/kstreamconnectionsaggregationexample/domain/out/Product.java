package com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out;

import com.adrienben.demo.domain.out.ProductProto;
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
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Product {

	private String id;
	private String name;
	private String description;
	private String brand;
	private List<Sku> skus = new ArrayList<>();

	@JsonIgnore
	public boolean isComplete() {
		return id != null && !"".equals(id) && name != null && brand != null;
	}

	public Product mergeProductDetails(ProductDetails productDetails) {
		this.id = productDetails.getId();
		this.name = productDetails.getName();
		this.description = productDetails.getDescription();
		this.brand = productDetails.getBrand();
		return this;
	}

	public Product mergeSkuDetails(SkuDetails skuDetails) {
		mergeSkuData(skuDetails.getSkuId(), sku -> sku.mergeDetails(skuDetails));
		return this;
	}

	public Product mergePrice(Price price) {
		mergeSkuData(price.getSkuId(), sku -> sku.mergePrice(price));
		return this;
	}

	public Product mergeOfferDetails(OfferDetails offerDetails) {
		mergeSkuData(offerDetails.getSkuId(), sku -> sku.mergeOfferDetails(offerDetails));
		return this;
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

	public ProductProto toProto() {
		return ProductProto.newBuilder()
				.setId(id)
				.setName(name)
				.setDescription(description)
				.setBrand(brand)
				.addAllSkus(skus.stream().map(Sku::toProto).collect(Collectors.toList()))
				.build();
	}
}
