package com.adrienben.demo.kstreamconnectionsaggregationexample.config;

import com.adrienben.demo.domain.in.OfferDetails;
import com.adrienben.demo.domain.in.Price;
import com.adrienben.demo.domain.in.ProductDetails;
import com.adrienben.demo.domain.in.SkuDetails;
import com.adrienben.demo.domain.out.Product;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@TestConfiguration
public class SchemaRegistryConfiguration {

	@Bean
	public SchemaRegistryClient schemaRegistryClient() throws IOException, RestClientException {
		var client = new MockSchemaRegistryClient();
		client.register(ProductDetails.class.getName(), new ProtobufSchema(ProductDetails.getDescriptor()));
		client.register(SkuDetails.class.getName(), new ProtobufSchema(SkuDetails.getDescriptor()));
		client.register(OfferDetails.class.getName(), new ProtobufSchema(OfferDetails.getDescriptor()));
		client.register(Price.class.getName(), new ProtobufSchema(Price.getDescriptor()));
		client.register(Product.class.getName(), new ProtobufSchema(Product.getDescriptor()));
		return client;
	}
}
