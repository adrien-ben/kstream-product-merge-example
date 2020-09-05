package com.adrienben.demo.kstreamconnectionsaggregationexample.config;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.OfferDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.Price;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.ProductDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.in.SkuDetails;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.out.Product;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@TestConfiguration
public class SchemaRegistryConfiguration {

	@Bean
	public SchemaRegistryClient schemaRegistryClient() throws IOException, RestClientException {
		var client = new MockSchemaRegistryClient();
		client.register(ProductDetails.class.getName(), new AvroSchema(ProductDetails.SCHEMA$));
		client.register(SkuDetails.class.getName(), new AvroSchema(SkuDetails.SCHEMA$));
		client.register(OfferDetails.class.getName(), new AvroSchema(OfferDetails.SCHEMA$));
		client.register(Price.class.getName(), new AvroSchema(Price.SCHEMA$));
		client.register(Product.class.getName(), new AvroSchema(Product.SCHEMA$));
		return client;
	}
}
