package com.mfec;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class TemporalJoin {
    public static StreamExecutionEnvironment temporalJoin(StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv) {

        tableEnv.createTemporaryTable("Pageview", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("viewtime", DataTypes.BIGINT())
                        .column("userid", DataTypes.STRING())
                        .column("pageid", DataTypes.STRING())
                        .build())
                .option("topic", "pageviews")
                .option("properties.bootstrap.servers", "devops1:9092")
                .option("properties.group.id", "pageview-tb")
                .option("scan.startup.mode", "earliest-offset")
                .option("value.format", "json")
                .build());

        // Create a users source table
        tableEnv.createTemporaryTable("User", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("userid", DataTypes.STRING().notNull())
                        .column("registertime", DataTypes.BIGINT())
                        .column("regionid", DataTypes.STRING())
                        .column("gender", DataTypes.STRING())
                        .build())
                .option("topic", "users")
                .option("properties.bootstrap.servers", "devops1:9092")
                .option("properties.group.id", "user-tb")
                .option("value.format", "json")
                .option("properties.auto.offset.reset", "earliest")
                .build());

        // Table pageview = tableEnv.from("Pageview");
        // Table user = tableEnv.from("User");

        Table joinedTable = tableEnv.sqlQuery(
                "SELECT * FROM Pageview p, User u ");

        DataStream<Row> joinedStream = tableEnv.toDataStream(joinedTable);
        // tableEnv.createTemporaryView("InputTable", inputTable);

        // define temporal table function
        // TemporalTableFunction historicalPageviewUserLookupFunction =
        // pageview.createTemporalTableFunction(
        // $("userid"),
        // $("viewtime"));

        // tableEnv.createTemporarySystemFunction("historicalPageviewUserLookupFunction",
        // historicalPageviewUserLookupFunction);

        // Table result = user
        // .joinLateral(call("historicalPageviewUserLookupFunction",
        // $("userid")),
        // $("userid").isEqual($("userid")))
        // .select($("userid"));
        // // $("time_now"),
        // // $("t_timestamp"),
        // // $("s_timestamp"),
        // // $("shares_purchased").times($("price")).as("total_amount"));

        // DataStream<Row> resultStream = tableEnv.toDataStream(result);

        joinedStream.print();

        return env;
    }
}
