package com.adrienben.demo.kstreamconnectionsaggregationexample.config;

import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.HashMap;
import java.util.Optional;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig.DERIVE_TYPE_CONFIG;

@Configuration
public class ProtobufSerdeConfig {

	@Bean
	@Scope("prototype")
	public <T extends Message> KafkaProtobufSerde<T> protobufSerde(
			@Value("${spring.kafka.streams.properties.schema.registry.url}") String schemaRegistryUrl,
			Optional<SchemaRegistryClient> schemaRegistryClient
	) {
		var properties = new HashMap<String, Object>();
		properties.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
		properties.put(AUTO_REGISTER_SCHEMAS, false);
		properties.put(DERIVE_TYPE_CONFIG, true);
		properties.put(VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());
		properties.put(KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());

		var serde = schemaRegistryClient.map(KafkaProtobufSerde<T>::new).orElseGet(KafkaProtobufSerde::new);
		serde.configure(properties, true);
		serde.configure(properties, false);
		return serde;
	}
}
