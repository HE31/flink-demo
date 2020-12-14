package spendreport;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by HE31 on 2020/12/6 16:16
 */
public class FlinkStreamAPI {
    public static void main(String[] args) throws Exception {

        //1. 创建环境方式
        //1.1 创建一个执行环境，表示当前执行程序的上下文。 如果程序是独立调用的，则 此方法返回本地执行环境；
        // 如果从命令行客户端调用程序以提交到集群，则此方法 返回此集群的执行环境，
        // 也就是说，getExecutionEnvironment 会根据查询运行的方 式决定返回什么样的运行环境，
        // 是最常用的一种创建执行环境的方式
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2. 返回本地执行环境，需要在调用时指定默认的并行度
       /* LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();*/
        //1.3 返回集群执行环境，将 Jar 提交到远程服务器。需要在调用时指定 JobManager 的 IP 和端口号，并指定要在集群中运行的 Jar 包。
      /*  StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment.
                createRemoteEnvironment("JobManagerHost",6201,"/usr/flink-demo.jar");*/

        //2.source
        //2.1 从集合读取数据
        ArrayList<Integer> list = new ArrayList<>(Arrays.asList(1,2,3));
        DataStreamSource<Integer> fromCollection = executionEnvironment.fromCollection(list);
       // fromCollection.print("collectionStream");

        //2.2 从文件读取数据
        String filePath = "C:\\DATA\\projectHere\\myGitHub\\flink-demo\\src\\main\\resources\\FlinkData.txt";
        DataStreamSource<String> textFile = executionEnvironment.readTextFile(filePath);
        //textFile.print("fileStream");


        //2.3 socket读取数据
        DataStreamSource<String> socketTextStream = executionEnvironment.socketTextStream("localhost", 9000);
       // socketTextStream.print("socketStream");

        //2.4 kafka读取数据
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","upcloud.vicp.io:9092");
        props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> kafkaStream = executionEnvironment.addSource(new FlinkKafkaConsumer<>("flink-topic", new SimpleStringSchema(), props));
        kafkaStream.print("kafkaStream");

        //2.5 自定义数据源，可自定义从redis，mysql等关系型数据库读取数据
        CustomSource source = new CustomSource();
        source.cancel();
        DataStreamSource<Integer> customSource = executionEnvironment.addSource(source);
       // customSource.print("customSourceStream");


        //3. transform 转换算子
        //3.1 map 与java8中的map很类似，都是用来做转换处理的
        //例：将数据流1,2,3 转换为wordCount对象 word为 "id_"+值，count为值*2
        ArrayList<Integer> mapList = new ArrayList<>(Arrays.asList(1,2,3));
        SingleOutputStreamOperator<WordCount> map = executionEnvironment.fromCollection(mapList).map(s -> new WordCount("id_" + s, s * 2));
        //map.print("tranform-map");

        //3.2 flatMap  对流做打平操作
        //例： 将[[1,2],[3,4],[5,6]]转换为[1,2,3,4,5,6]
        List<List<Integer>> lists = new ArrayList<>();
        lists.add(Arrays.asList(1,2));
        lists.add(Arrays.asList(3,4));
        lists.add(Arrays.asList(5,6));
        DataStreamSource<List<Integer>> listDataStreamSource = executionEnvironment.fromCollection(lists);
       // listDataStreamSource.print("transform-flatMap-before");
        SingleOutputStreamOperator<Integer> flatMapStream = listDataStreamSource.flatMap((List<Integer> l, Collector<Integer> collector) -> {
            l.forEach(collector::collect);
        }).returns(Types.INT);
       // flatMapStream.print("transform-flatMap-after");


        // 3.3 filter 过滤
        //例：从1,2,3中过滤出1
        ArrayList<Integer> filterList = new ArrayList<>(Arrays.asList(1,2,3));
        SingleOutputStreamOperator<Integer> filter = executionEnvironment.fromCollection(filterList).filter(s -> s == 1);
        //filter.print("transform-filter");



        //3.4 keyBy 逻辑地将一个流拆分成不相交的分区，每个分 区包含具有相同 key 的元素，在内部以 hash 的形式实现的,类似sql中的group by
        //例：将 wordCount根据word做分区
        ArrayList<WordCount> wordCounts = new ArrayList<>(Arrays.asList(new WordCount("flink", 1)
                , new WordCount("flink", 1)
                , new WordCount("java", 1)
                , new WordCount("sql", 1)
                , new WordCount("java", 1)));
        KeyedStream<WordCount, String> wordCountStringKeyedStream = executionEnvironment.fromCollection(wordCounts).keyBy(WordCount::getWord);
        //wordCountStringKeyedStream.print("transform-keyBy");



        //sum/min/max/maxBy/minBy 滚动聚合算子

        ArrayList<WordCount> aggWordCounts = new ArrayList<>(Arrays.asList(new WordCount("flink", 1)
                , new WordCount("flink", 2)
                , new WordCount("java", 1)
                , new WordCount("sql", 3)
                , new WordCount("java", 1)));
        DataStreamSource<WordCount> streamSource = executionEnvironment.fromCollection(aggWordCounts);
        SingleOutputStreamOperator<WordCount> sum = streamSource.keyBy(WordCount::getWord).sum("count");
       // sum.print("sum");



        //reduce
        ArrayList<WordCount> reduceCounts = new ArrayList<>(Arrays.asList(new WordCount("flink", 1)
                , new WordCount("flink", 2)
                , new WordCount("java", 1)
                , new WordCount("sql", 1)
                , new WordCount("java", 1)));
        DataStreamSource<WordCount> personDataStreamSource = executionEnvironment.fromCollection(reduceCounts);

        SingleOutputStreamOperator<WordCount> reduce = personDataStreamSource.keyBy(WordCount::getWord)
                .reduce((curWord, nextWord) -> new WordCount(curWord.getWord(),curWord.getCount()+nextWord.getCount()));
     //   reduce.print("reduce");


        //split & select
        List<Person> students = new ArrayList<>(Arrays.asList(new Person("Dave", "male", 18)
                , new Person("Tom", "male", 20)
                , new Person("John", "male", 17)
                , new Person("Sarah", "female", 19)
                , new Person("Helen", "female", 25)
        ));

        DataStreamSource<Person> personDataStreamSource1 = executionEnvironment.fromCollection(students);
        //打分流标签
        SplitStream<Person> splitStream = personDataStreamSource1.split(person -> {
            ArrayList<String> tags = new ArrayList<>();
            if (person.getAge() >= 18) {

                tags.add("adult");
            } else {
                tags.add("teen");
            }
            return tags;
        });
        //根据标签筛选
        DataStream<Person> adult = splitStream.select("adult");
        DataStream<Person> teen = splitStream.select("teen");
        DataStream<Person> all = splitStream.select("adult", "teen");
        //adult.print("adult");
        //teen.print("teen");
        //all.print("all");




        List<Person> coStudent = new ArrayList<>(Arrays.asList(new Person("Dave", "male", 18)
                , new Person("Tom", "male", 20)
                , new Person("John", "male", 17)
                , new Person("Sarah", "female", 19)
                , new Person("Helen", "female", 25)
        ));

        ArrayList<WordCount> coWordCounts = new ArrayList<>(Arrays.asList(new WordCount("flink", 1)
                , new WordCount("flink", 2)
                , new WordCount("java", 1)
                , new WordCount("sql", 1)
                , new WordCount("java", 1)));

        DataStreamSource<Person> personDataStreamSource2 = executionEnvironment.fromCollection(coStudent);
        DataStreamSource<WordCount> wordCountDataStreamSource = executionEnvironment.fromCollection(coWordCounts);
        ConnectedStreams<Person, WordCount> connectedStreams = personDataStreamSource2.connect(wordCountDataStreamSource);
        SingleOutputStreamOperator<Object> coMap = connectedStreams.map(new CoMapFunction<Person, WordCount, Object>() {
            @Override
            public Object map1(Person value) throws Exception {
                return value.getName();
            }

            @Override
            public Object map2(WordCount value) throws Exception {
                return value.getWord();
            }
        });
      //  coMap.print("coMap");


        List<Person> class1 = new ArrayList<>(Arrays.asList(new Person("Dave", "male", 18)
                , new Person("Tom", "male", 20)
                , new Person("John", "male", 17)
                , new Person("Sarah", "female", 19)
                , new Person("Helen", "female", 25)
        ));

        List<Person> class2 = new ArrayList<>(Arrays.asList(new Person("Bob", "male", 19)
                , new Person("Ted", "male", 23)
                , new Person("Joe", "male", 27)
                , new Person("Amy", "female", 24)
                , new Person("Anne", "female", 25)
        ));

        List<Person> class3 = new ArrayList<>(Arrays.asList(new Person("Bill", "male", 23)
                , new Person("Vivian", "female", 23)
                , new Person("Ted", "male", 25)
                , new Person("Emma", "female", 24)
                , new Person("Lily", "female", 15)
        ));

        DataStreamSource<Person> class1Stream = executionEnvironment.fromCollection(class1);
        DataStreamSource<Person> class2Stream = executionEnvironment.fromCollection(class2);
        DataStreamSource<Person> class3Stream = executionEnvironment.fromCollection(class3);
        //合并三个班级
        DataStream<Person> personDataStream = class1Stream.union(class2Stream).union(class3Stream);
        SingleOutputStreamOperator<String> streamOperator = personDataStream.map(person -> person.getName());
       // streamOperator.print("union");




        ArrayList<WordCount> sinkCounts = new ArrayList<>(Arrays.asList(new WordCount("flink", 1)
                , new WordCount("flink", 2)
                , new WordCount("java", 1)
                , new WordCount("sql", 1)
                , new WordCount("java", 1)));

        DataStreamSource<WordCount> collection = executionEnvironment.fromCollection(sinkCounts);
        SingleOutputStreamOperator<WordCount> operator = collection.map(wordCount -> new WordCount("sink_" + wordCount.getWord(), wordCount.getCount()));
        operator.addSink(
                StreamingFileSink.forRowFormat(new Path("C:\\DATA\\projectHere\\myGitHub\\flink-demo\\src\\main\\resources\\sink")
                        , new SimpleStringEncoder<WordCount>() {
        }).build()).setParallelism(1);


        executionEnvironment.execute("flink api job");
    }
}
