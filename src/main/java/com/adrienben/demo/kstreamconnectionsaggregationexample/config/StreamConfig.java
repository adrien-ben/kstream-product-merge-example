package com.adrienben.demo.kstreamconnectionsaggregationexample.config;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetailsAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.Price;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.PriceAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.ProductDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.ProductDetailsAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.SkuDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.SkuDetailsAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Product;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.ProductAvro;
import com.adrienben.demo.kstreamconnectionsaggregationexample.transformer.ProductPartMergingTransformer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
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

	public static final String PRODUCT_DETAILS_TOPIC = "product_details";
	public static final String PRICES_TOPIC = "prices";
	public static final String SKU_DETAILS_TOPIC = "sku_details";
	public static final String OFFER_DETAILS_TOPIC = "offer_details";

	public static final String PRICES_BY_PRODUCT_ID_REKEY_TOPIC = "prices_by_product_id_rekey";
	public static final String SKU_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC = "sku_details_by_product_id_rekey";
	public static final String OFFER_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC = "offer_details_by_product_id_rekey";

	public static final String PRODUCTS_TOPIC = "products";

	private final SpecificAvroSerde<ProductDetailsAvro> productDetailsAvroSerde;
	private final SpecificAvroSerde<SkuDetailsAvro> skuDetailsAvroSerde;
	private final SpecificAvroSerde<OfferDetailsAvro> offerDetailsAvroSerde;
	private final SpecificAvroSerde<PriceAvro> priceAvroSerde;
	private final SpecificAvroSerde<ProductAvro> productAvroSerde;
	private final Serde<SkuDetails> skuDetailsJsonSerde;
	private final Serde<OfferDetails> offerDetailsJsonSerde;
	private final Serde<Price> priceJsonSerde;
	private final Serde<Product> productJsonSerde;

	public StreamConfig(
			SpecificAvroSerde<ProductDetailsAvro> productDetailsAvroSerde,
			SpecificAvroSerde<SkuDetailsAvro> skuDetailsAvroSerde,
			SpecificAvroSerde<OfferDetailsAvro> offerDetailsAvroSerde,
			SpecificAvroSerde<PriceAvro> priceAvroSerde,
			SpecificAvroSerde<ProductAvro> productAvroSerde,
			ObjectMapper mapper
	) {
		this.productDetailsAvroSerde = productDetailsAvroSerde;
		this.skuDetailsAvroSerde = skuDetailsAvroSerde;
		this.offerDetailsAvroSerde = offerDetailsAvroSerde;
		this.priceAvroSerde = priceAvroSerde;
		this.productAvroSerde = productAvroSerde;
		this.skuDetailsJsonSerde = jsonSerde(mapper, SkuDetails.class);
		this.offerDetailsJsonSerde = jsonSerde(mapper, OfferDetails.class);
		this.priceJsonSerde = jsonSerde(mapper, Price.class);
		this.productJsonSerde = jsonSerde(mapper, Product.class);
	}

	@Bean
	public KStream<String, ProductAvro> kStream(StreamsBuilder streamBuilder) {
		// First we define the various serialization/deserialization objects we will need
		var stringSerde = Serdes.String();
		var productDetailsAvroConsumed = Consumed.with(stringSerde, productDetailsAvroSerde);
		var priceAvroConsumed = Consumed.with(stringSerde, priceAvroSerde);
		var priceJsonProduced = Produced.with(stringSerde, priceJsonSerde);
		var skuDetailsAvroConsumed = Consumed.with(stringSerde, skuDetailsAvroSerde);
		var skuDetailsJsonProduced = Produced.with(stringSerde, skuDetailsJsonSerde);
		var offerDetailsAvroConsumed = Consumed.with(stringSerde, offerDetailsAvroSerde);
		var offerDetailsJsonProduced = Produced.with(stringSerde, offerDetailsJsonSerde);
		var productAvroProduced = Produced.with(stringSerde, productAvroSerde);

		// Now we need to add out custom state store so it can be accessed from the Processor/Transformer API
		streamBuilder.addStateStore(
				Stores.keyValueStoreBuilder(
						Stores.persistentKeyValueStore(PRODUCT_STORE_NAME), stringSerde, productJsonSerde));

		// Merge price
		// - We first stream the prices in avro format
		// - Then we change the key of the message and map the value to our business object using map so we can 'join' prices with products
		// - Before we can join though, we need to re-repartition the prices to make sure price and product
		// with the same key end up in the same partitions.
		// - We can then merge the price in the product using the generic transformer map it to an Avro compatible object and send the product in
		// an output topic.
		streamBuilder.stream(PRICES_TOPIC, priceAvroConsumed)
				.map((key, price) -> KeyValue.pair(price.getProductId(), Price.fromAvro(price)))
				.through(PRICES_BY_PRODUCT_ID_REKEY_TOPIC, priceJsonProduced)
				.transform(() -> ProductPartMergingTransformer.fromMergingFunction(Product::mergePrice), PRODUCT_STORE_NAME)
				.mapValues((key, product) -> product.toAvro())
				.to(PRODUCTS_TOPIC, productAvroProduced);

		// Merge offer details
		// Same principe as the price merge
		streamBuilder.stream(OFFER_DETAILS_TOPIC, offerDetailsAvroConsumed)
				.map((key, offerDetails) -> KeyValue.pair(offerDetails.getProductId(), OfferDetails.fromAvro(offerDetails)))
				.through(OFFER_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC, offerDetailsJsonProduced)
				.transform(() -> ProductPartMergingTransformer.fromMergingFunction(Product::mergeOfferDetails), PRODUCT_STORE_NAME)
				.mapValues((key, product) -> product.toAvro())
				.to(PRODUCTS_TOPIC, productAvroProduced);

		// Merge sku details
		// Same principe as the price merge
		streamBuilder.stream(SKU_DETAILS_TOPIC, skuDetailsAvroConsumed)
				.map((key, skuDetails) -> KeyValue.pair(skuDetails.getProductId(), SkuDetails.fromAvro(skuDetails)))
				.through(SKU_DETAILS_BY_PRODUCT_ID_REKEY_TOPIC, skuDetailsJsonProduced)
				.transform(() -> ProductPartMergingTransformer.fromMergingFunction(Product::mergeSkuDetails), PRODUCT_STORE_NAME)
				.mapValues((key, product) -> product.toAvro())
				.to(PRODUCTS_TOPIC, productAvroProduced);

		// Merge product details
		// The process is almost the same as with the previous parts except we don't need the re-keying because the id
		// already match those of the products.
		var products = streamBuilder.stream(PRODUCT_DETAILS_TOPIC, productDetailsAvroConsumed)
				.mapValues((key, productDetails) -> ProductDetails.fromAvro(productDetails))
				.transform(() -> ProductPartMergingTransformer.fromMergingFunction(Product::mergeProductDetails), PRODUCT_STORE_NAME)
				.mapValues((key, product) -> product.toAvro());
		products.to(PRODUCTS_TOPIC, productAvroProduced);

		return products;
	}

	private static <T> Serde<T> jsonSerde(ObjectMapper mapper, Class<T> targetClass) {
		return Serdes.serdeFrom(
				new JsonSerializer<>(mapper),
				new JsonDeserializer<>(targetClass, mapper, false));
	}
}
