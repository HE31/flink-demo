package spendreport;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by HE31 on 2020/12/13 19:50
 */
public class FlinkSink {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<WordCount> sinkCounts = new ArrayList<>(Arrays.asList(new WordCount("flink", 1)
                , new WordCount("flink", 2)
                , new WordCount("java", 1)
                , new WordCount("sql", 1)
                , new WordCount("java", 1)));

        SingleOutputStreamOperator<String> streamOperator = executionEnvironment.fromCollection(sinkCounts)
                .map(wordCount -> new WordCount("sink_" + wordCount.getWord(), wordCount.getCount()).toString());
        //FlinkKafkaProducer()三个参数分别为：brokerList,topicId,SerializationSchema,这里为方便起见，直接将WordCount对象转为了String
        streamOperator.addSink(new FlinkKafkaProducer<>("localhost:9092", "test-topic", new SimpleStringSchema()));

        executionEnvironment.execute("sink");
    }
}
