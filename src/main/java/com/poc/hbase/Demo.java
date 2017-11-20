package com.poc.hbase;

import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hduser on 20/11/17.
 */
public class Demo {

    public void displayData() {

        SparkSession session = SparkSession.builder()
                .master("local[4]")
                .appName("POC")
                .getOrCreate();

        SQLContext sqlContext = session.sqlContext();

        String catalog1 = "{\n" +
                "  \"table\": {\n" +
                "    \"namespace\": \"default\",\n" +
                "    \"name\": \"emp\"\n" +
                "  },\n" +
                "  \"rowkey\": \"key\",\n" +
                "  \"columns\": {\n" +
                "    \"id\": {\n" +
                "      \"cf\": \"rowkey\",\n" +
                "      \"col\": \"key\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    \"email\": {\n" +
                "      \"cf\": \"professional\",\n" +
                "      \"col\": \"email\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    \"empid\": {\n" +
                "      \"cf\": \"professional\",\n" +
                "      \"col\": \"empId\",\n" +
                "      \"type\": \"string\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Map<String, String> map = new HashMap<>();

        map.put(HBaseTableCatalog.tableCatalog(), catalog1);

        Dataset<Row> ds1 = sqlContext.read().options(map).format("org.apache.spark.sql.execution.datasources.hbase").load();

        List<Row> tableData1 = ds1.collectAsList();

        System.out.println("Details of Table1: " + tableData1);

        String catalog2 = "{\n" +
                "  \"table\": {\n" +
                "    \"namespace\": \"default\",\n" +
                "    \"name\": \"person\"\n" +
                "  },\n" +
                "  \"rowkey\": \"key\",\n" +
                "  \"columns\": {\n" +
                "    \"personid\": {\n" +
                "      \"cf\": \"rowkey\",\n" +
                "      \"col\": \"key\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    \"name\": {\n" +
                "      \"cf\": \"personal\",\n" +
                "      \"col\": \"name\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    \"city\": {\n" +
                "      \"cf\": \"personal\",\n" +
                "      \"col\": \"city\",\n" +
                "      \"type\": \"string\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        map.put(HBaseTableCatalog.tableCatalog(), catalog2);

        Dataset<Row> ds2 = sqlContext.read().options(map).format("org.apache.spark.sql.execution.datasources.hbase").load();

        List<Row> tableData2 = ds2.collectAsList();

        System.out.println("Details of Table2: " + tableData2);

        Column column = new Column("id").equalTo(new Column("personid"));

        Dataset<Row> joinDs = ds1.join(ds2, column).select("id", "empid", "name", "city", "email").sort("id");

        System.out.println("Join result: " + joinDs.collectAsList());

        ds1.createOrReplaceTempView("temp_view1");

        ds2.createOrReplaceTempView("temp_view2");

        List<Row> joinResult =  sqlContext.sql("select id, empid, name, city, email from temp_view1 join temp_view2 on id = personid order by id").collectAsList();

        System.out.println("Join result: " + joinResult);

        session.close();
    }
}
