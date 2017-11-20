package com.poc.hbase;

import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
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

        String catalog1 = "{\"table\":{\"namespace\":\"default\",\"name\":\"test\"},\"rowkey\":\"key\",\"columns\":{\"col0\":{\"cf\":\"rowkey\",\"col\":\"key\",\"type\":\"string\"},\"col1\":{\"cf\":\"cf\",\"col\":\"a\",\"type\":\"string\"},\"col2\":{\"cf\":\"cf\",\"col\":\"b\",\"type\":\"string\"},\"col3\":{\"cf\":\"cf\",\"col\":\"c\",\"type\":\"string\"}}}";

        Map<String, String> map = new HashMap<>();
        map.put(HBaseTableCatalog.tableCatalog(), catalog1);

        Dataset<Row> ds1 = sqlContext.read().options(map).format("org.apache.spark.sql.execution.datasources.hbase").load();

        List<Row> tableData1 = ds1.collectAsList();

        System.out.println("Details of Table1: " + tableData1);

        String catalog2 = "{\"table\":{\"namespace\":\"default\",\"name\":\"test2\"},\"rowkey\":\"key\",\"columns\":{\"col4\":{\"cf\":\"rowkey\",\"col\":\"key\",\"type\":\"string\"},\"col5\":{\"cf\":\"cf\",\"col\":\"a\",\"type\":\"string\"},\"col6\":{\"cf\":\"cf\",\"col\":\"c\",\"type\":\"string\"},\"col7\":{\"cf\":\"cf\",\"col\":\"d\",\"type\":\"string\"}}}";

        map.put(HBaseTableCatalog.tableCatalog(), catalog2);

        Dataset<Row> ds2 = sqlContext.read().options(map).format("org.apache.spark.sql.execution.datasources.hbase").load();

        List<Row> tableData2 = ds2.collectAsList();

        System.out.println("Details of Table2: " + tableData2);

        Column column = new Column("col1").equalTo(new Column("col5"));

        Dataset<Row> joinDs = ds1.join(ds2, column).select("col2", "col3", "col6", "col7");

        System.out.println("Join result: " + joinDs.collectAsList());

        ds1.createOrReplaceTempView("temp_view1");

        ds2.createOrReplaceTempView("temp_view2");

        List<Row> joinResult =  sqlContext.sql("select col2, col3, col6, col7 from temp_view1 join temp_view2 on col1 = col5").collectAsList();

        System.out.println("Join result: " + joinResult);

        session.close();
    }
}
