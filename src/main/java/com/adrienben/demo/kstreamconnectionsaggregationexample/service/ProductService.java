package com.adrienben.demo.kstreamconnectionsaggregationexample.service;

import com.adrienben.demo.domain.in.OfferDetails;
import com.adrienben.demo.domain.in.Price;
import com.adrienben.demo.domain.in.ProductDetails;
import com.adrienben.demo.domain.in.SkuDetails;
import com.adrienben.demo.domain.out.Offer;
import com.adrienben.demo.domain.out.Product;
import com.adrienben.demo.domain.out.Sku;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Service
public class ProductService {

	public Product mergeProductDetails(String productId, ProductDetails productDetails, Product product) {
		var newProduct = product.toBuilder();
		setOrClear(newProduct, productId, Product.Builder::setId, Product.Builder::clearId);
		setOrClear(newProduct, productDetails.getName(), Product.Builder::setName, Product.Builder::clearName);
		setOrClear(newProduct, productDetails.getDescription(), Product.Builder::setDescription, Product.Builder::clearDescription);
		setOrClear(newProduct, productDetails.getBrand(), Product.Builder::setBrand, Product.Builder::clearBrand);
		return newProduct.build();
	}

	public Product mergeSkuDetails(String productId, SkuDetails skuDetails, Product product) {
		var newProduct = product.toBuilder();
		var sku = getSkuBuilderForSkuId(newProduct, skuDetails.getSkuId());
		setOrClear(sku, skuDetails.getSkuId(), Sku.Builder::setId, Sku.Builder::clearId);
		setOrClear(sku, skuDetails.getName(), Sku.Builder::setName, Sku.Builder::clearName);
		setOrClear(sku, skuDetails.getDescription(), Sku.Builder::setDescription, Sku.Builder::clearDescription);
		return newProduct.build();
	}

	public Product mergeOfferDetails(String productId, OfferDetails offerDetails, Product product) {
		var newProduct = product.toBuilder();
		var sku = getSkuBuilderForSkuId(newProduct, offerDetails.getSkuId());
		var offer = getOfferBuilderForOfferId(sku, offerDetails.getOfferId());
		setOrClear(offer, offerDetails.getOfferId(), Offer.Builder::setId, Offer.Builder::clearId);
		setOrClear(offer, offerDetails.getName(), Offer.Builder::setName, Offer.Builder::clearName);
		setOrClear(offer, offerDetails.getDescription(), Offer.Builder::setDescription, Offer.Builder::clearDescription);
		return newProduct.build();
	}

	public Product mergePrice(String productId, Price price, Product product) {
		var newProduct = product.toBuilder();
		var sku = getSkuBuilderForSkuId(newProduct, price.getSkuId());
		var offer = getOfferBuilderForOfferId(sku, price.getOfferId());
		setOrClear(offer, price.getAmount(), Offer.Builder::setPrice, Offer.Builder::clearPrice);
		return newProduct.build();
	}

	private static <T, V> void setOrClear(T target, V value, BiConsumer<T, V> setter, Consumer<T> clearer) {
		if (value != null) {
			setter.accept(target, value);
		} else {
			clearer.accept(target);
		}
	}

	private static Sku.Builder getSkuBuilderForSkuId(Product.Builder product, String skuId) {
		return product.getSkusBuilderList()
				.stream()
				.filter(sku -> Objects.equals(sku.getId(), skuId))
				.findFirst()
				.orElseGet(() -> product.addSkusBuilder().setId(skuId));
	}

	private static Offer.Builder getOfferBuilderForOfferId(Sku.Builder sku, String offerId) {
		return sku.getOffersBuilderList()
				.stream()
				.filter(offer -> Objects.equals(offer.getId(), offerId))
				.findFirst()
				.orElseGet(() -> sku.addOffersBuilder().setId(offerId));
	}

	public boolean isProductComplete(String productId, Product product) {
		return product.getId() != null
				&& !"".equals(product.getId())
				&& product.getName() != null
				&& product.getBrand() != null;
	}
}
