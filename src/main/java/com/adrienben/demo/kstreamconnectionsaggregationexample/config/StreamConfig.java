package com.adrienben.demo.kstreamconnectionsaggregationexample.config;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.Price;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.ProductDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.SkuDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Product;
import com.adrienben.demo.kstreamconnectionsaggregationexample.transformer.ProductPartMergingTransformer;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import static com.adrienben.demo.kstreamconnectionsaggregationexample.transformer.ProductPartMergingTransformer.PRODUCT_STORE_NAME;

@Slf4j
@Configuration
@EnableKafkaStreams
public class StreamConfig {

	private static final String PRODUCT_DETAILS_TOPIC = "product_details";
	private static final String PRICES_TOPIC = "prices";
	private static final String SKU_DETAILS_TOPIC = "sku_details";
	private static final String OFFER_DETAILS_TOPIC = "offer_details";

	private static final String PRICES_BY_PRODUCT_ID_REKEY_TOPIC = "prices_by_product_id_rekey";
	private static final String SKU_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC = "sku_details_by_product_id_rekey";
	private static final String OFFER_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC = "offer_details_by_product_id_rekey";

	private static final String PRODUCTS_TOPIC = "products";

	private ObjectMapper mapper;

	public StreamConfig(ObjectMapper mapper) {
		this.mapper = mapper;
	}

	@Bean
	public KStream<String, Product> kStream(StreamsBuilder streamBuilder) {
		// First we define the various serialization/deserialization objects we will need
		var stringSerde = Serdes.String();
		var productDetailsSerde = jsonSerde(ProductDetails.class);
		var productDetailsConsumed = Consumed.with(stringSerde, productDetailsSerde);
		var priceSerde = jsonSerde(Price.class);
		var priceConsumed = Consumed.with(stringSerde, priceSerde);
		var priceProduced = Produced.with(stringSerde, priceSerde);
		var skuDetailsSerde = jsonSerde(SkuDetails.class);
		var skuDetailsConsumed = Consumed.with(stringSerde, skuDetailsSerde);
		var skuDetailsProduced = Produced.with(stringSerde, skuDetailsSerde);
		var offerDetailsSerde = jsonSerde(OfferDetails.class);
		var offerDetailsConsumed = Consumed.with(stringSerde, offerDetailsSerde);
		var offerDetailsProduced = Produced.with(stringSerde, offerDetailsSerde);
		var productSerde = jsonSerde(Product.class);
		var productProduced = Produced.with(stringSerde, productSerde);

		// Now we need to add out custom state store so it can be accessed from the Processor/Transformer API
		streamBuilder.addStateStore(
				Stores.keyValueStoreBuilder(
						Stores.persistentKeyValueStore(PRODUCT_STORE_NAME), stringSerde, productSerde));

		// Merge price
		// - We first stream the prices
		// - Then we change the key of the message using select key so we can 'join' prices with products
		// - Before we can join though, we need to re-repartition the prices to make sure price and product
		// with the same key end up in the same partitions.
		// - We can then merge the price in the product using the generic transformer and send the product in
		// an output topic.
		streamBuilder.stream(PRICES_TOPIC, priceConsumed)
				.selectKey((key, price) -> price.getProductId())
				.through(PRICES_BY_PRODUCT_ID_REKEY_TOPIC, priceProduced)
				.transform(() -> ProductPartMergingTransformer.fromMergingFunction(Product::mergePrice), PRODUCT_STORE_NAME)
				.to(PRODUCTS_TOPIC, productProduced);

		// Merge offer details
		// Same principe as the price merge
		streamBuilder.stream(OFFER_DETAILS_TOPIC, offerDetailsConsumed)
				.selectKey((key, offerDetails) -> offerDetails.getProductId())
				.through(OFFER_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC, offerDetailsProduced)
				.transform(() -> ProductPartMergingTransformer.fromMergingFunction(Product::mergeOfferDetails), PRODUCT_STORE_NAME)
				.to(PRODUCTS_TOPIC, productProduced);

		// Merge sku details
		// Same principe as the price merge
		streamBuilder.stream(SKU_DETAILS_TOPIC, skuDetailsConsumed)
				.selectKey((key, skuDetails) -> skuDetails.getProductId())
				.through(SKU_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC, skuDetailsProduced)
				.transform(() -> ProductPartMergingTransformer.fromMergingFunction(Product::mergeSkuDetails), PRODUCT_STORE_NAME)
				.to(PRODUCTS_TOPIC, productProduced);

		// Merge product details
		// The process is almost the same as with the previous parts except we don't need the re-keying because the id
		// already match those of the products.
		var products = streamBuilder.stream(PRODUCT_DETAILS_TOPIC, productDetailsConsumed)
				.transform(() -> ProductPartMergingTransformer.fromMergingFunction(Product::mergeProductDetails), PRODUCT_STORE_NAME);
		products.to(PRODUCTS_TOPIC, productProduced);

		return products;
	}

	private <T> Serde<T> jsonSerde(Class<T> targetClass) {
		return Serdes.serdeFrom(
				new JsonSerializer<>(mapper),
				new JsonDeserializer<>(targetClass, mapper, false));
	}
}
