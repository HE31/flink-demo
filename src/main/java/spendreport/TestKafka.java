package spendreport;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by HE31 on 2020/11/30 11:03
 */
public class TestKafka {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String hostname = params.get("host");
        String port = params.get("port");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String kafkaServer = hostname+":"+port;
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaServer);
       // props.setProperty("group.id", "consumer-group");
        props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<>("flink-topic", new SimpleStringSchema(), props));
        SingleOutputStreamOperator<WordCount> sum = streamSource.flatMap((String s, Collector<WordCount> collector) -> {
            String[] split = s.split("\\s");
            Arrays.stream(split).map(s1 -> new WordCount(s1, 1)).forEach(collector::collect);
        })
                .returns(Types.POJO(WordCount.class))
                .keyBy(WordCount::getWord)
                .sum("count");
        sum.print();
        env.execute("kafka stream wordCount");
    }
}
