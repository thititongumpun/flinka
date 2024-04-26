package com.mfec;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import com.mfec.pojo.Purchase;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class TestJoin {
    public static void testJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a StreamTableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporaryTable("Person", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("jobTitle", DataTypes.STRING())
                        .column("pProctime", DataTypes.TIMESTAMP_LTZ(3))
                        .watermark("pProctime", "pProctime - INTERVAL '5' SECOND")
                        .build())
                .option("topic", "persons")
                .option("properties.bootstrap.servers", "streaming-dev.xyz:29092")
                .option("properties.group.id", "person-groupid")
                .option("scan.startup.mode", "earliest-offset")
                .option("value.format", "json")
                .build());

        tableEnv.createTemporaryTable("PersonInfo", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("accountName", DataTypes.STRING())
                        .column("amount", DataTypes.DOUBLE())
                        .column("transactionType", DataTypes.STRING())
                        .column("piProctime", DataTypes.TIMESTAMP_LTZ(3))
                        .watermark("piProctime", "piProctime - INTERVAL '5' SECOND")
                        .build())
                .option("topic", "personsInfo")
                .option("properties.bootstrap.servers", "streaming-dev.xyz:29092")
                .option("properties.group.id", "personInfo-groupid")
                .option("scan.startup.mode", "earliest-offset")
                .option("value.format", "json")
                .build());

        Table personTable = tableEnv.from("Person")
                .renameColumns($("name").as("personName"));
        Table personInfoTable = tableEnv.from("PersonInfo")
                .renameColumns($("name").as("personInfoName"));

        TemporalTableFunction personTemporal = personTable
                .createTemporalTableFunction(
                        $("pProctime"),
                        $("personName"));

        tableEnv.createTemporarySystemFunction("personTemporal", personTemporal);

        Table result = personInfoTable
                .joinLateral(call("personTemporal",
                        $("piProctime")),
                        $("personInfoName").isEqual($("personName")));

        // tableEnv.toDataStream(personTable).print();
        // tableEnv.toDataStream(personInfoTable).print();
        // DataStream<Row> outputStream = 
        tableEnv.toChangelogStream(result).print();
        // outputStream.print();

        env.execute();
    }
}
