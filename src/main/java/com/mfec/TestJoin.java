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
                        .column("proctime", DataTypes.TIMESTAMP_LTZ(3))
                        .watermark("proctime", "proctime - INTERVAL '5' SECOND")
                        .build())
                .option("topic", "persons")
                .option("properties.bootstrap.servers", "devops1:9092")
                .option("properties.group.id", "person-groupid")
                .option("scan.startup.mode", "latest-offset")
                .option("value.format", "json")
                .build());

        tableEnv.createTemporaryTable("personInfo", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("accountName", DataTypes.STRING())
                        .column("amount", DataTypes.DOUBLE())
                        .column("transactionType", DataTypes.STRING())
                        .column("proctime", DataTypes.TIMESTAMP_LTZ(3))
                        .watermark("proctime", "proctime - INTERVAL '5' SECOND")
                        .build())
                .option("topic", "personsInfo")
                .option("properties.bootstrap.servers", "devops1:9092")
                .option("properties.group.id", "personInfo-groupid")
                .option("scan.startup.mode", "latest-offset")
                .option("value.format", "json")
                .build());

        Table personTable = tableEnv.from("Person")
                .renameColumns($("name").as("personName"), $("proctime").as("p_proctime"));
        Table personInfoTable = tableEnv.from("personInfo")
                .renameColumns($("name").as("personInfoName"), $("proctime").as("pi_proctime"));

        TemporalTableFunction personTemporal = personTable
                .createTemporalTableFunction(
                        $("p_proctime"),
                        $("personName"));

        tableEnv.createTemporarySystemFunction("personTemporal", personTemporal);

        Table result = personInfoTable
                .joinLateral(call("personTemporal",
                        $("pi_proctime")),
                        $("personInfoName").isEqual($("personName")));

        tableEnv.toChangelogStream(result).print();

        // Table join = personTable.join(personInfoTable)
        // .where($("personName").isEqual($("personInfoName")));
        // Table join = personTable.join(personInfoTable)
        // .where($("personName").isEqual($("personInfoName")))
        // .groupBy($("personName"))
        // .select($("personName"), $("amount").sum());

        // DataStream<Row> outputStream = tableEnv.toChangelogStream(join);
        // outputStream.print();

        env.execute();
    }
}
