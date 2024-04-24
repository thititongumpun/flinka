package com.mfec;

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


import java.time.Duration;
import java.util.Random;

public class JoinExample {
    static final String[] NAMES = { "tom", "jerry", "alice", "bob", "john", "grace" };
    static final String[] JOBTITLES = { "engineer", "manager", "sales", "marketing" };
    static final int AMOUNT = 10000;

    public static void joinStreamExample(StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv) throws Exception {

        DataStream<Tuple2<String, String>> persons = env.fromSource(
                getPersonGeneratorSource(3L),
                IngestionTimeWatermarkStrategy.create(),
                "Person Data Generator")
                .setParallelism(1);

        DataStream<Tuple3<String, String, Integer>> personsInfo = env.fromSource(
                getPersonInfoGeneratorSource(3L),
                IngestionTimeWatermarkStrategy.create(),
                "PersonInfo Data Generator")
                .setParallelism(1);

        DataStream<Tuple4<String, String, String, Integer>> joinedStream = persons.join(personsInfo)
                .where(p -> p.f0)
                .equalTo(pi -> pi.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(2000)))
                .apply(
                        new JoinFunction<Tuple2<String, String>, Tuple3<String, String, Integer>, Tuple4<String, String, String, Integer>>() {

                            @Override
                            public Tuple4<String, String, String, Integer> join(
                                    Tuple2<String, String> first, Tuple3<String, String, Integer> second) {
                                return new Tuple4<String, String, String, Integer>(
                                        first.f0, first.f1, second.f1, second.f2);
                            }
                        });

        joinedStream.print();

        env.execute();

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

    public static DataGeneratorSource<Tuple2<String, String>> getPersonGeneratorSource(
            double elementsPerSecond) {
        return getTupleGeneratorSource(elementsPerSecond);
    }

    /** Continuously generates (name, salary). */
    public static DataGeneratorSource<Tuple3<String, String, Integer>> getPersonInfoGeneratorSource(
            double elementsPerSecond) {
        return getTuplePersonInfoGeneratorSource(elementsPerSecond);
    }

    private static DataGeneratorSource<Tuple2<String, String>> getTupleGeneratorSource(
            double elementsPerSecond) {

        String[] NAMES = { "tom", "jerry", "alice", "bob", "john", "grace" };
        String[] JOBTITLES = { "engineer", "manager", "sales", "marketing" };

        final Random rnd = new Random();
        final GeneratorFunction<Long, Tuple2<String, String>> generatorFunction = index -> new Tuple2<>(
                NAMES[rnd.nextInt(NAMES.length)], JOBTITLES[rnd.nextInt(JOBTITLES.length)]);

        return new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(elementsPerSecond),
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                }));
    }

    private static DataGeneratorSource<Tuple3<String, String, Integer>> getTuplePersonInfoGeneratorSource(
            double elementsPerSecond) {

        String[] NAMES = { "tom", "jerry", "alice", "bob", "john", "grace" };
        String[] ACCOUNTNAME = { "Savings Account", "Credit Card Account", "Home Loan", "Person Loan Account" };

        final Random rnd = new Random();
        final GeneratorFunction<Long, Tuple3<String, String, Integer>> generatorFunction = index -> new Tuple3<>(
                NAMES[rnd.nextInt(NAMES.length)], ACCOUNTNAME[rnd.nextInt(ACCOUNTNAME.length)],
                rnd.nextInt(AMOUNT) + 1);

        return new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(elementsPerSecond),
                TypeInformation.of(new TypeHint<Tuple3<String, String, Integer>>() {
                }));
    }
}
