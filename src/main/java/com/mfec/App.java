package com.mfec;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.mfec.pojo.Product;
import com.mfec.pojo.Purchase;

public class App {
    public static void main(String[] args) throws Exception {
        Configuration cfg = new Configuration();
        int defaultLocalParallelism = Runtime.getRuntime().availableProcessors();
        cfg.setString("taskmanager.memory.network.max", "1gb");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironment(defaultLocalParallelism, cfg);
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // StreamExecutionEnvironment dataStreamToTable = dataStreamToTable(env,
        // tableEnv);
        // dataStreamToTable.execute();
        // StreamExecutionEnvironment changelogStream = changelogStream(env, tableEnv);
        // changelogStream.execute();
        // StreamExecutionEnvironment multipleStream = multipleTopics(env, tableEnv);
        // multipleStream.execute();

        // StreamExecutionEnvironment joinResult = joinStream.joinStreamWindow(env,
        // tableEnv);
        // joinResult.execute("Windowed join example");

        // StreamExecutionEnvironment tableJoin = TableJoin.tableJoin(env, tableEnv);
        // tableJoin.execute();
        // TestJoin.testJoin();

        // JoinExample.joinStreamExample(env, tableEnv);
        TestJoin2.testJoin();
    }

    public static StreamExecutionEnvironment dataStreamToTable(StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("streaming-dev.xyz:29092")
                .setTopics("stream1")
                .setGroupId("flink-app321")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        Table inputTable = tableEnv.fromDataStream(dataStream);

        tableEnv.createTemporaryView("InputTable", inputTable);
        Table resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable");
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);
        resultStream.print();

        return env;
    }

    // public static StreamExecutionEnvironment
    // changelogStream(StreamExecutionEnvironment env,
    // StreamTableEnvironment tableEnv) {
    // JsonDeserializationSchema<Purchase> jsonFormat = new
    // JsonDeserializationSchema<>(Purchase.class);
    // KafkaSource<Purchase> source = KafkaSource.<Purchase>builder()
    // .setBootstrapServers("devops1:9092")
    // .setTopics("purchases")
    // .setGroupId("flink-kafka-app321")
    // .setStartingOffsets(OffsetsInitializer.earliest())
    // .setValueOnlyDeserializer(jsonFormat)
    // .build();

    // DataStream<Purchase> dataStream = env.fromSource(source,
    // WatermarkStrategy.noWatermarks(), "Kafka Source");
    // Table inputTable = tableEnv.fromDataStream(dataStream).as("id", "item_type",
    // "price_per_unit", "quantity");
    // tableEnv.createTemporaryView("InputTable", inputTable);
    // Table resultTable = tableEnv.sqlQuery(
    // "SELECT item_type, SUM(quantity) FROM InputTable GROUP BY item_type");

    // Table simpleTable = tableEnv
    // .fromDataStream(dataStream)
    // .as("id", "item_type", "price_per_unit", "quantity")
    // .groupBy($("item_type"))
    // .select("item_type, SUM(quantity)");

    // DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
    // resultStream.print();

    // return env;
    // }

    public static StreamExecutionEnvironment multipleTopics(StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv) {

        JsonDeserializationSchema<Purchase> purchaseFormat = new JsonDeserializationSchema<>(Purchase.class);
        JsonDeserializationSchema<Product> productFormat = new JsonDeserializationSchema<>(Product.class);

        KafkaSource<Purchase> purchaseSource = KafkaSource.<Purchase>builder()
                .setBootstrapServers("devops1:9092")
                .setTopics("purchases")
                .setGroupId("flink-kafka-app321")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(purchaseFormat)
                .build();

        KafkaSource<Product> productSource = KafkaSource.<Product>builder()
                .setBootstrapServers("devops1:9092")
                .setTopics("product")
                .setGroupId("flink-kafka-app321")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(productFormat)
                .build();

        DataStream<Purchase> dataStream1 = env.fromSource(purchaseSource, WatermarkStrategy.noWatermarks(),
                "Perchase Source");
        DataStream<Product> dataStream2 = env.fromSource(productSource, WatermarkStrategy.noWatermarks(),
                "Product Source");

        dataStream1.print();
        dataStream2.print();

        return env;
    }
}
