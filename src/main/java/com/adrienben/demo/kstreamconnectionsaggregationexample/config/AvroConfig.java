package com.adrienben.demo.kstreamconnectionsaggregationexample.config;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.HashMap;
import java.util.Optional;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Configuration
public class AvroConfig {

	@Bean
	@Scope("prototype")
	public <T extends SpecificRecord> SpecificAvroSerde<T> specificAvroSerde(
			@Value("${spring.kafka.streams.properties.schema.registry.url}") String schemaRegistryUrl,
			Optional<SchemaRegistryClient> schemaRegistryClient
	) {
		HashMap<String, Object> properties = new HashMap<>();
		properties.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
		properties.put(AUTO_REGISTER_SCHEMAS, false);

		SpecificAvroSerde<T> serde = schemaRegistryClient.map(SpecificAvroSerde<T>::new).orElseGet(SpecificAvroSerde::new);
		serde.configure(properties, true);
		serde.configure(properties, false);
		return serde;
	}
}
