package teststreams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class Demo {
	
	public static void main(String[] args) throws InterruptedException {

		Properties pro = new Properties();
		pro.put(StreamsConfig.APPLICATION_ID_CONFIG, "mywordcount");
		pro.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.184.128:9092");
		pro.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		pro.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> textLines = builder.stream("streamstopic-in");
		KTable<String, Long> wordCounts = textLines.flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split(" ")))
				.groupBy((key, word) -> word)
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
		wordCounts.toStream().to("streamstopic-out", Produced.with(Serdes.String(), Serdes.Long()));
		KafkaStreams streams = new KafkaStreams(builder.build(), pro);
		streams.start();
	}
}
