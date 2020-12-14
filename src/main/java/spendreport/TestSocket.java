package spendreport;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Created by HE31 on 2020/11/27 16:33
 */
public class TestSocket {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String hostname = params.get("host");
        String port = params.get("port");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> textStream = env.socketTextStream(hostname, Integer.valueOf(port), "\n");
        SingleOutputStreamOperator<WordCount> sum = textStream.flatMap((String s, Collector<WordCount> collector) -> {
            String[] strings = s.split("\\s");
            Arrays.stream(strings).forEach(w -> {
                collector.collect(new WordCount(w, 1));
            });
        })
                //明确指定返回类型
                .returns(Types.POJO(WordCount.class))
                .filter(s -> !s.getWord().isEmpty())
                .keyBy(s -> s.getWord())
                .timeWindow(Time.seconds(1)).sum("count");


        // 将结果打印到控制台，注意这里使用的是单线程打印，而非多线程
        sum.print().setParallelism(1);

      /*  List<Integer> integers = Arrays.asList(1, 2, 3);
        DataStreamSource<Integer> collection = env.fromCollection(integers);
       collection.print();*/
        env.execute("Socket Window WordCount");
    }


}
