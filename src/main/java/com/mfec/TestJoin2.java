package com.mfec;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import com.mfec.pojo.Purchase;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

import org.apache.flink.configuration.Configuration;

public class TestJoin2 {
    public static void testJoin() throws Exception {
        // StreamExecutionEnvironment env =
        // StreamExecutionEnvironment.getExecutionEnvironment();

        // // Create a StreamTableEnvironment
        // StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        StreamExecutionEnvironment env;
        StreamTableEnvironment tableEnv;
        Configuration configuration = new Configuration();

        // in case of idle sources
        configuration.setString("table.exec.source.idle-timeout", "500 ms");

        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporaryTable("Currency", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("update_time", DataTypes.STRING())
                        .column("currency", DataTypes.STRING().notNull())
                        .column("rate", DataTypes.INT())
                        .column("proctime", DataTypes.TIMESTAMP_LTZ(3))
                        .watermark("proctime", "proctime - INTERVAL '10' SECOND")
                        .build())
                .option("topic", "currencyRates")
                .option("properties.bootstrap.servers", "streaming-dev.xyz:29092")
                .option("properties.group.id", "currency-groupid")
                .option("scan.startup.mode", "latest-offset")
                .option("value.format", "json")
                .build());

        tableEnv.createTemporaryTable("Order", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("order_time", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .column("currency", DataTypes.STRING().notNull())
                        .column("proctime", DataTypes.TIMESTAMP_LTZ(3))
                        .watermark("proctime", "proctime - INTERVAL '5' SECOND")
                        .build())
                .option("topic", "orders")
                .option("properties.bootstrap.servers", "streaming-dev.xyz:29092")
                .option("properties.group.id", "order-groupid")
                .option("scan.startup.mode", "latest-offset")
                .option("value.format", "json")
                .build());

        Table currencyTable = tableEnv.from("Currency")
                .select($("update_time"), $("currency").as("r_currency"), $("rate").as("r_rate"),
                        $("proctime").as("r_proctime"));
        Table orderTable = tableEnv.from("Order")
                .select($("order_time"), $("amount").as("o_amount"), $("currency").as("o_currency"),
                        $("proctime").as("o_proctime"));

        // TemporalTableFunction personTemporal = personTable
        // .createTemporalTableFunction(
        // $("pProctime"),
        // $("personName"));

        TemporalTableFunction rates = currencyTable
                .createTemporalTableFunction($("r_proctime"), $("r_currency"));

        tableEnv.createTemporarySystemFunction("rates", rates);

        Table result = orderTable
                .joinLateral(call("rates", $("o_proctime")),
                        $("o_currency").isEqual($("r_currency")))
                // .select($("o_amount").times($("r_rate")).sum().as("amount"));
                // .select($("o_amount").sum().as("amount"));
                .select($("*"));
        // Table result = orderTable
        // .joinLateral(call("rates", $("o_proctime")),
        // $("o_currency").isEqual($("r_currency")));
        tableEnv.toChangelogStream(result).print();

        env.execute();
    }
}
