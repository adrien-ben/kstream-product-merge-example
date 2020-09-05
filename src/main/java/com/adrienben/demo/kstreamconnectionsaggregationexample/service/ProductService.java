package com.adrienben.demo.kstreamconnectionsaggregationexample.service;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.Price;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.ProductDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.SkuDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Offer;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Product;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Sku;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class ProductService {

	public Product mergeProductDetails(String productId, ProductDetails productDetails, Product product) {
		product.setId(productId);
		product.setName(productDetails.getName());
		product.setDescription(productDetails.getDescription());
		product.setBrand(productDetails.getBrand());
		return product;
	}

	public Product mergeSkuDetails(String productId, SkuDetails skuDetails, Product product) {
		var sku = getSkuForSkuId(product, skuDetails.getSkuId());
		sku.setId(skuDetails.getSkuId());
		sku.setName(skuDetails.getName());
		sku.setDescription(skuDetails.getDescription());
		return product;
	}

	public Product mergeOfferDetails(String productId, OfferDetails offerDetails, Product product) {
		var sku = getSkuForSkuId(product, offerDetails.getSkuId());
		var offer = getOfferForOfferId(sku, offerDetails.getOfferId());
		offer.setId(offerDetails.getOfferId());
		offer.setName(offerDetails.getName());
		offer.setDescription(offerDetails.getDescription());
		return product;
	}

	public Product mergePrice(String productId, Price price, Product product) {
		var sku = getSkuForSkuId(product, price.getSkuId());
		var offer = getOfferForOfferId(sku, price.getOfferId());
		offer.setId(price.getOfferId());
		offer.setPrice(price.getAmount());
		return product;
	}

	private static Sku getSkuForSkuId(Product product, CharSequence skuId) {
		return product.getSkus()
				.stream()
				.filter(sku -> Objects.equals(sku.getId(), skuId))
				.findFirst()
				.orElseGet(() -> {
					var sku = Sku.newBuilder().setId(skuId).build();
					product.getSkus().add(sku);
					return sku;
				});
	}

	private static Offer getOfferForOfferId(Sku sku, CharSequence offerId) {
		return sku.getOffers()
				.stream()
				.filter(offer -> Objects.equals(offer.getId(), offerId))
				.findFirst()
				.orElseGet(() -> {
					var offer = Offer.newBuilder().setId(offerId).build();
					sku.getOffers().add(offer);
					return offer;
				});
	}

	public boolean isProductComplete(String productId, Product product) {
		return product.getId() != null
				&& !"".equals(product.getId().toString())
				&& product.getName() != null
				&& product.getBrand() != null;
	}
}
