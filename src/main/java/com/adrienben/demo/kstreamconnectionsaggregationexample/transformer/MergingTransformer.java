package com.adrienben.demo.kstreamconnectionsaggregationexample.transformer;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Generic transformer use to merge some complex object part with the current state of the full object.
 * <p>
 * Users need to provide:
 *
 * <ul>
 * <li>The name of the store that will hold the full object current state</li>
 * <li>A value initializer that will initialize the full object upon receiving the first part from the key of the part</li>
 * <li>A merger function that merge the received part in the current state</li>
 * <li>A validation against which the updated state will be checked to verify whether the state should be propagated or not</li>
 * </ul>
 *
 * @param <K>  The type of the key.
 * @param <VO> The type of the full object.
 * @param <VI> The type of the part to merge.
 */
@RequiredArgsConstructor
public class MergingTransformer<K, VO, VI> implements Transformer<K, VI, KeyValue<K, VO>> {

	/**
	 * The name of the store that will holt the current state of the full object.
	 */
	private final String storeName;

	/**
	 * The function that will initialize the full object from its key.
	 */
	private final Function<K, VO> valueInitializer;

	/**
	 * The function that will be used to merge the part into the current object's state.
	 */
	private final BiConsumer<VO, VI> merger;

	/**
	 * The function that will check whether the updated state should be propagated.
	 */
	private final Predicate<VO> validator;

	private KeyValueStore<K, VO> store;

	@Override
	public void init(ProcessorContext context) {
		store = getStateStore(context);
	}

	@SuppressWarnings("unchecked")
	private KeyValueStore<K, VO> getStateStore(ProcessorContext context) {
		return (KeyValueStore<K, VO>) context.getStateStore(storeName);
	}

	@Override
	public KeyValue<K, VO> transform(K key, VI value) {
		var current = Optional.ofNullable(store.get(key)).orElseGet(() -> valueInitializer.apply(key));
		merger.accept(current, value);
		store.put(key, current);
		if (validator.test(current)) {
			return KeyValue.pair(key, current);
		}
		return null;
	}

	@Override
	public void close() {
	}
}
