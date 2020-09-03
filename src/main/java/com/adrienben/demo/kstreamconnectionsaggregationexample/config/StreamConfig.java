package com.adrienben.demo.kstreamconnectionsaggregationexample.config;

import com.adrienben.demo.domain.in.OfferDetailsProto;
import com.adrienben.demo.domain.in.PriceProto;
import com.adrienben.demo.domain.in.ProductDetailsProto;
import com.adrienben.demo.domain.in.SkuDetailsProto;
import com.adrienben.demo.domain.out.ProductProto;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.Price;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.ProductDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.SkuDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Slf4j
@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class StreamConfig {

	// Input topics
	public static final String PRODUCT_DETAILS_TOPIC = "product_details";
	public static final String PRICES_TOPIC = "prices";
	public static final String SKU_DETAILS_TOPIC = "sku_details";
	public static final String OFFER_DETAILS_TOPIC = "offer_details";

	// Internals
	private static final String PRICES_BY_PRODUCT_ID_REKEY_TOPIC = "prices_by_product_id_rekey";
	private static final String SKU_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC = "sku_details_by_product_id_rekey";
	private static final String OFFER_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC = "offer_details_by_product_id_rekey";
	private static final String PRODUCT_STORE_NAME = "product_store";

	// Output topics
	public static final String PRODUCTS_TOPIC = "products";

	private final KafkaProtobufSerde<ProductDetailsProto> productDetailsProtoSerde;
	private final KafkaProtobufSerde<SkuDetailsProto> skuDetailsProtoSerde;
	private final KafkaProtobufSerde<OfferDetailsProto> offerDetailsProtoSerde;
	private final KafkaProtobufSerde<PriceProto> priceProtoSerde;
	private final KafkaProtobufSerde<ProductProto> productProtoSerde;
	private final ObjectMapper mapper;

	@Bean
	public KStream<String, ProductProto> kStream(StreamsBuilder streamBuilder) {
		// First we define the various serialization/deserialization objects we will need
		var stringSerde = Serdes.String();
		var skuDetailsJsonSerde = jsonSerde(SkuDetails.class);
		var offerDetailsJsonSerde = jsonSerde(OfferDetails.class);
		var priceJsonSerde = jsonSerde(Price.class);
		var productJsonSerde = jsonSerde(Product.class);
		var productDetailsProtoConsumed = Consumed.with(stringSerde, productDetailsProtoSerde);
		var priceProtoConsumed = Consumed.with(stringSerde, priceProtoSerde);
		var skuDetailsProtoConsumed = Consumed.with(stringSerde, skuDetailsProtoSerde);
		var offerDetailsProtoConsumed = Consumed.with(stringSerde, offerDetailsProtoSerde);
		var productProtoProduced = Produced.with(stringSerde, productProtoSerde);

		// Group prices
		// - We first stream the prices
		// - Then we change the key of the message using select key so we can 'join' prices with products
		// - Before we can join though, we need to re-repartition the prices to make sure price and product
		// with the same key end up in the same partitions.
		// - We can then group the prices by key.
		var groupedPrices = streamBuilder.stream(PRICES_TOPIC, priceProtoConsumed)
				.selectKey((key, price) -> price.getProductId())
				.mapValues(Price::fromProto)
				.repartition(Repartitioned.<String, Price>as(PRICES_BY_PRODUCT_ID_REKEY_TOPIC)
						.withKeySerde(stringSerde)
						.withValueSerde(priceJsonSerde))
				.groupByKey();

		// Group offer details
		// Same principe as the price grouping
		var groupedOfferDetails = streamBuilder.stream(OFFER_DETAILS_TOPIC, offerDetailsProtoConsumed)
				.selectKey((key, offerDetails) -> offerDetails.getProductId())
				.mapValues(OfferDetails::fromProto)
				.repartition(Repartitioned.<String, OfferDetails>as(OFFER_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC)
						.withKeySerde(stringSerde)
						.withValueSerde(offerDetailsJsonSerde))
				.groupByKey();

		// Group sku details
		// Same principe as the price grouping
		var groupedSkuDetails = streamBuilder.stream(SKU_DETAILS_TOPIC, skuDetailsProtoConsumed)
				.selectKey((key, skuDetails) -> skuDetails.getProductId())
				.mapValues(SkuDetails::fromProto)
				.repartition(Repartitioned.<String, SkuDetails>as(SKU_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC)
						.withKeySerde(stringSerde)
						.withValueSerde(skuDetailsJsonSerde))
				.groupByKey();

		// Group sku details
		// Same principe as the price grouping
		var groupedProductDetails = streamBuilder.stream(PRODUCT_DETAILS_TOPIC, productDetailsProtoConsumed)
				.mapValues(ProductDetails::fromProto)
				.mapValues((id, productDetails) -> {
					productDetails.setId(id);
					return productDetails;
				})
				.groupByKey();

		// Now our parts are ready to be merged using the co-group operation.
		// For each of our grouped stream we define how it's gonna be merged in the final Product.
		// We also define how to create the initial Product.
		// Finally we check that our product is complete before we send it to the output topic.
		var products = groupedProductDetails
				.<Product>cogroup((key, productDetails, product) -> product.mergeProductDetails(productDetails))
				.cogroup(groupedSkuDetails, (key, skuDetails, product) -> product.mergeSkuDetails(skuDetails))
				.cogroup(groupedOfferDetails, (key, offerDetails, product) -> product.mergeOfferDetails(offerDetails))
				.cogroup(groupedPrices, (key, price, product) -> product.mergePrice(price))
				.aggregate(Product::new,
						Materialized.<String, Product, KeyValueStore<Bytes, byte[]>>as(PRODUCT_STORE_NAME)
								.withKeySerde(stringSerde)
								.withValueSerde(productJsonSerde))
				.toStream()
				.filter((key, product) -> product.isComplete())
				.mapValues(Product::toProto);

		products.to(PRODUCTS_TOPIC, productProtoProduced);

		return products;
	}

	private <T> Serde<T> jsonSerde(Class<T> targetClass) {
		return Serdes.serdeFrom(
				new JsonSerializer<>(mapper),
				new JsonDeserializer<>(targetClass, mapper, false));
	}
}
