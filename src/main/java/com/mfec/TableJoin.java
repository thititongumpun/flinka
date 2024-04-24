package com.mfec;

import static org.apache.flink.table.api.Expressions.$;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableJoin {
    public static StreamExecutionEnvironment tableJoin(StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv) {

        // Create a blogs source table
        tableEnv.createTemporaryTable("Blog", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("blogId", DataTypes.INT())
                        .column("url", DataTypes.STRING())
                        .build())
                .option("topic", "blogs")
                .option("properties.bootstrap.servers", "devops1:9092")
                .option("properties.group.id", "blog-groupid")
                .option("scan.startup.mode", "earliest-offset")
                .option("value.format", "json")
                .build());

        // Create a posts source table
        tableEnv.createTemporaryTable("Post", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("postId", DataTypes.INT())
                        .column("title", DataTypes.STRING())
                        .column("content", DataTypes.STRING())
                        .column("blogId", DataTypes.INT())
                        .build())
                .option("topic", "posts")
                .option("properties.bootstrap.servers", "devops1:9092")
                .option("properties.group.id", "post-groupid")
                .option("scan.startup.mode", "earliest-offset")
                .option("value.format", "json")
                .build());

        // Table blogTable = tableEnv.from("Blog").as("b");
        // Table postTable = tableEnv.from("Post").as("p");

        // Table join = blogTable.join(postTable)
        // .where($("blogId").isEqual($("blogId")));
        Table blogTable = tableEnv.from("Blog");
        Table postTable = tableEnv.from("Post");

        Table join = blogTable.join(postTable)
                .where($("Blog.blogId").isEqual($("Post.blogId")));

        // Table output = tableEnv.sqlQuery(
        // "SELECT * FROM Blog b JOIN Post p ON b.blogId = p.blogId");

        tableEnv.createTemporaryView("join", join);

        DataStream<Row> outputStream = tableEnv.toChangelogStream(join);

        // DataStream<Row> result = tableEnv.toChangelogStream(join);
        outputStream.print();
        // res.printSchema();

        return env;
    }

}
