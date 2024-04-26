package com.mfec;

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.mfec.pojo.Person;
import com.mfec.pojo.PersonInfo;
import com.mfec.pojo.PersonInfoOut;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

public class joinStream {
    public static StreamExecutionEnvironment joinStreamWindow(StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv) {

        JsonDeserializationSchema<Person> personFormat = new JsonDeserializationSchema<>(Person.class);
        JsonDeserializationSchema<PersonInfo> personInfoFormat = new JsonDeserializationSchema<>(PersonInfo.class);

        KafkaSource<Person> personSource = KafkaSource.<Person>builder()
                .setBootstrapServers("streaming-dev.xyz:29092")
                .setTopics("persons")
                .setGroupId("flink-kafka-app32112")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(personFormat)
                .build();

        KafkaSource<PersonInfo> personInfoSource = KafkaSource.<PersonInfo>builder()
                .setBootstrapServers("streaming-dev.xyz:29092")
                .setTopics("personsInfo")
                .setGroupId("flink-kafka-app32123")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(personInfoFormat)
                .build();

        DataStream<Person> dataStream1 = env.fromSource(personSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)),
                // IngestionTimeWatermarkStrategy.create(),
                "Person Source")
                .setParallelism(1);

        DataStream<PersonInfo> dataStream2 = env.fromSource(personInfoSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)),
                // IngestionTimeWatermarkStrategy.create(),
                "PersonInfo Source")
                .setParallelism(1);

        DataStream<PersonInfoOut> joinOut = dataStream1.join(dataStream2)
                .where(Person::getName)
                .equalTo(PersonInfo::getName)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(20)))
                .apply(new JoinFunction<Person, PersonInfo, PersonInfoOut>() {
                    @Override
                    public PersonInfoOut join(Person person, PersonInfo personInfo) throws Exception {
                        PersonInfoOut p = new PersonInfoOut();
                        p.setName(person.getName());
                        p.setJobTitle(person.getJobTitle());
                        p.setAccountName(personInfo.getAccountName());
                        p.setAmount(personInfo.getAmount());
                        p.setTransactionType(personInfo.getTransactionType());
                        p.setProctime(person.getPProctime());
                        return p;
                    }
                });
        // dataStream2.print();
        joinOut.print();

        // DataStream<Tuple2<String, String>> dataStreamMap1 = dataStream1
        // .map(new MapFunction<Person, Tuple2<String, String>>() {
        // @Override
        // public Tuple2<String, String> map(Person value) throws Exception {
        // return new Tuple2<>(value.getName(), value.getJobTitle());
        // }
        // });

        // DataStream<Tuple4<String, String, Double, String>> dataStreamMap2 =
        // dataStream2
        // .map(new MapFunction<PersonInfo, Tuple4<String, String, Double, String>>() {
        // @Override
        // public Tuple4<String, String, Double, String> map(PersonInfo value) throws
        // Exception {
        // return new Tuple4<>(value.getName(), value.getAccountName(),
        // value.getAmount(),
        // value.getTransactionType());
        // }
        // });

        // // dataStreamMap1.print();
        // // dataStreamMap2.print();

        // DataStream<Tuple4<String, String, Double, String>> joinedStream =
        // dataStreamMap1
        // .join(dataStreamMap2)
        // .where(p -> p.f0)
        // .equalTo(pi -> pi.f0)
        // .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
        // .apply((new JoinFunction<Tuple2<String, String>, Tuple4<String, String,
        // Double, String>, Tuple4<String, String, Double, String>>() {
        // @Override
        // public Tuple4<String, String, Double, String> join(
        // Tuple2<String, String> first, Tuple4<String, String, Double, String> second)
        // {
        // return new Tuple4<String, String, Double, String>(first.f0, second.f1,
        // second.f2, second.f3);
        // }
        // }));

        // joinedStream.print();

        // Table inputTable = tableEnv.fromDataStream(joinedStream);

        // tableEnv.createTemporaryView("InputTable", inputTable);
        // // Table resultTable = tableEnv.sqlQuery("SELECT UPPER(f0), f1 ,f2 ,f3 FROM
        // // InputTable");
        // Table resultTable = tableEnv.sqlQuery("SELECT f0, SUM(f2) FROM InputTable
        // GROUP BY f0");

        // DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

        return env;
    }

    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

        private IngestionTimeWatermarkStrategy() {
        }

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }
}
