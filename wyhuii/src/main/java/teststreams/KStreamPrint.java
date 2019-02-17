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

/**
 * 使用KStream实现单词个数统计
 * @author wyhui
 */
public class KStreamPrint {

	public static void main(String[] args) throws InterruptedException {
		//构造实例化KafkaStreams对象的配置
		Properties prop = new Properties();
		/**
		 * 每个Streams应用程序必须要有一个应用ID，这个ID用于协调应用实例，也用于命名内部的本地存储和相关主题。
		 * 对于同一个kafka集群里的每一个Streams应用来说，这个名字必须是唯一的。
		 */
		prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "mywordcount");//名字随意起，但必须唯一
		prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.184.128:9092");//kafka服务器IP
		//在读写数据时，应用程序需要对消息进行序列化和反序列化，因此提供了序列化类和反序列化类。
		prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		/*
		 * 创建KStreamBuilder对象
		 * KStreamBuilder builder = new KStreamBuilder();
		 * 在网上参考的很多博客以及书籍都是用的这种方法来创建KStreamBuiler对象，从而得到KStream，
		 * 但是在我编写代码时会发现这个类无法导入，后来看了官方文档后发现，我用的是最新版的kafka，里面的一些类更新了。
		 * 所以在此参考kafka官方示例中给出的方法：创建StreamsBuilder，从而得到KStream。
		 * 由此可见，还是官方文档给的示例最权威。
		 */
		
		//创建StreamsBuilder对象
		StreamsBuilder builder = new StreamsBuilder();
		//以要处理的Topic作为参数使用builder的stram方法得到一个KStream流
		KStream<String, String> countStream = builder.stream("streamstopic-in");
		KTable<String, Long> wordcounts = countStream
				.flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split(" ")))//将topic中得到的流中的每一个值都变成小写然后以空格分割
				.groupBy((key, word) -> word)//分组
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));//计数
		//将更新日志流转为记录流然后输出到另一个Topic
		wordcounts.toStream().to("streamstopic-out", Produced.with(Serdes.String(), Serdes.Long()));
		//基于拓扑和配置属性定义一个KafkaStreams对象
		KafkaStreams streams = new KafkaStreams(builder.build(), prop);
		//启动kafkastreams引擎
		streams.start();
		/*//一般情况下，Streams应用程序会一直运行下去，此处由于模拟测试，数据量少，我们就让线程休眠一段时间然后停止运行
		Thread.sleep(5000L);
		//停止运行
		streams.close();*/

	}

}
