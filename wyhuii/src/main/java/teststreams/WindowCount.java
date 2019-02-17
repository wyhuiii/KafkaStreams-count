package teststreams;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

/**
 * @description count 10min,30min,1hour groupBy itemId
 * @author wyhui
 *
 */
public class WindowCount {

	public static void main(String[] args) {
		Properties prop = new Properties();
		prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "mywindowcount");
		prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.184.128:9092");//kafka服务器IP
		prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		prop.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\IT\\tool\\kafka-state-store");//设置状态仓库的存储路径
		StreamsBuilder builder = new StreamsBuilder();
		//从topic中读入数据
		KStream<String, String> input = builder.stream("window-count");
		//使用map()将数据的Key设置为与value相同。
		KGroupedStream<String, String> groupBy = input
			.map(new KeyValueMapper<String, String, KeyValue<String, String>>(){
	
				@Override
				public KeyValue<String, String> apply(String key, String value) {
					return new KeyValue<String, String>(value, value);
				}
				
			})
			//gorupBy()可以指定任意想要group的值
			.groupBy(new KeyValueMapper<String, String, String>(){

				@Override
				public String apply(String key, String value) {
					//这里我们根据itemId来进行group，此方法要返回的就是我们要group的值，也就是group之后的key。
					//假设我们的topic读入的数据格式是：itemId="0001",itemName="华硕电脑",userId="1003"
					return value.substring(8, 12);//截取itemId
				}
				
			});
		/*
		 * 对数据进行group之后，我们该对不同的itemId加窗统计，由于这里我们是想求得同一个Id在不同时间段内的购买次数，
		 * 就需要施加多个window，所以在这里我们需要分成多个分流进行不同的加窗，然后把最终的结果都输出到window-count-out这个topic中。
		 * 要对上面的同一个group结果进行不同的操作，我们就需要将其复制多份来进行不同的操作。
		 */
		KGroupedStream<String, String> groupBy10min = groupBy;
		KGroupedStream<String, String> groupBy30min = groupBy;
		KGroupedStream<String, String> groupBy1hour = groupBy;
		groupBy10min.windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1)))//以Min为单位，转换为毫秒
					.count(Materialized.as("itemGroupBy1min-state-store"))//指定存储中间状态的state-store，存储在上面配置的STATE_DIR_CONFIG路径下
					.toStream()//经过上面的count之后得到的是KTable，输出到topic中需要转为流
					//经过上面的操作之后，会自动地在key的后面加上一个时间戳，而我们在输出时并不需要这些数据，所以使用map（）将key中的时间戳处理掉
					/*
					 * .map((key, count) -> KeyValue.pair(key.toString(), count.toString()+"10min"))
					 * 如果使用这样的方法，得到的输出结果就是：offset:0,key:[0001@1550313600000/1550314200000],value:110min
					 * 所以我们需要对Key进行处理
					 */
					//上面统计的count是Long类型的，此处需要转为String。为了在输出count时能区分出是哪个时间段统计的，在这里我们加上"10min"来指示
					.map((key, count) -> KeyValue.pair(key.toString().substring(1, key.toString().indexOf("@")), count.toString()+"******1min"))
					.to("window-count-out");//输出到topic中
		groupBy30min.windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(3)))//以Min为单位，转换为毫秒
					.count(Materialized.as("itemGroupBy3min-state-store"))//指定存储中间状态的state-store
					.toStream()
					.map((key, count) -> KeyValue.pair(key.toString().substring(1, key.toString().indexOf("@")), count.toString()+"******3min"))
					.to("window-count-out");
		groupBy1hour.windowedBy(TimeWindows.of(TimeUnit.HOURS.toMillis(1)))
					.count(Materialized.as("itemGroupBy1hour-state-store"))
					.toStream()
					.map((key, count) -> KeyValue.pair(key.toString().substring(1, key.toString().indexOf("@")), count.toString()+"*******1hour"))
					.to("window-count-out");
		KafkaStreams streams = new KafkaStreams(builder.build(), prop);
		streams.cleanUp();
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

}
