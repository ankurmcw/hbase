package com.poc.hbase;

import com.poc.hbase.model.SHCExampleTable1;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hduser on 17/11/17.
 */
public class SHCTest {

    private static Logger logger = LoggerFactory.getLogger(SHCTest.class);

    public void readData() {
        /*Connection connection = context.getBean(Connection.class);
        if (connection != null) {
            Table table = connection.getTable(TableName.valueOf("test"));
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result: scanner) {
                logger.info(Bytes.toString(result.getValue("cf".getBytes(), "a".getBytes())));
                logger.info(Bytes.toString(result.getValue("cf".getBytes(), "b".getBytes())));
                logger.info(Bytes.toString(result.getValue("cf".getBytes(), "c".getBytes())));
            }
        } else {
            logger.error("Bean initialization failed");
        }*/

       String tableName = "test";

		SparkConf conf = new SparkConf().setAppName("POC").setMaster("local[2]");
		conf.set("spark.hbase.host", "127.0.0.1");
        SparkContext sparkContext = new SparkContext(conf);

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", "localhost:60000");
        configuration.setInt("timeout", 120000);
        configuration.set("hbase.zookeeper.quorum", "localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set(TableInputFormat.INPUT_TABLE, tableName);

        RDD rdd = sparkContext.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        logger.info("Table count: {}", rdd.count());
        //sparkContext.stop();


        SparkSession session = SparkSession.builder()
                .master("local[4]")
                .appName("POC")
                .config(conf)
                .sparkContext(sparkContext)
                .config("hbase.master", "localhost:60000")
                .config("timeout", 120000)
                .config("hbase.zookeeper.quorum", "127.0.0.1")
                .config("hbase.zookeeper.property.clientPort", "2181")
                .getOrCreate();

        SQLContext sqlContext = session.sqlContext();

        logger.info("Configuration: {}", sqlContext.getAllConfs());


        String catalog1 = "{\"table\":{\"namespace\":\"default\",\"name\":\"test\"},\"rowkey\":\"key\",\"columns\":{\"col0\":{\"cf\":\"rowkey\",\"col\":\"key\",\"type\":\"string\"},\"col1\":{\"cf\":\"cf\",\"col\":\"a\",\"type\":\"string\"},\"col2\":{\"cf\":\"cf\",\"col\":\"b\",\"type\":\"string\"},\"col3\":{\"cf\":\"cf\",\"col\":\"c\",\"type\":\"string\"}}}";

        Map<String, String> map = new HashMap<>();
        map.put(HBaseTableCatalog.tableCatalog(), catalog1);

        List<Row> rows = sqlContext.read().options(map).format("org.apache.spark.sql.execution.datasources.hbase").load().collectAsList();
        System.out.println(rows.toString());

        Dataset<Row> dataSet1 = sqlContext.read().option(HBaseTableCatalog.tableCatalog(), catalog1).format("org.apache.spark.sql.execution.datasources.hbase").load();

        logger.info("Table count is {}", dataSet1.count());

        dataSet1.createOrReplaceTempView("table1");

        List<Row> rows1 =  sqlContext.sql("select col1, col2, col3 from table1").collectAsList();

        logger.info("Table result1: {}", rows1.toString());

        String catalog2 = "{\"table\":{\"namespace\":\"default\",\"name\":\"test2\"},\"rowkey\":\"key\",\"columns\":{\"col0\":{\"cf\":\"rowkey\",\"col\":\"key\",\"type\":\"string\"},\"col5\":{\"cf\":\"cf\",\"col\":\"a\",\"type\":\"string\"},\"col6\":{\"cf\":\"cf\",\"col\":\"c\",\"type\":\"string\"},\"col7\":{\"cf\":\"cf\",\"col\":\"d\",\"type\":\"string\"}}}";

        Dataset<Row> dataSet2 = sqlContext.read().option(HBaseTableCatalog.tableCatalog(), catalog2).format("org.apache.spark.sql.execution.datasources.hbase").load();

        dataSet2.createOrReplaceTempView("table2");

        List<Row> rows2 =  sqlContext.sql("select col5, col6, col7 from table2 limit 1").collectAsList();

        logger.info("Table result2:\n{}", rows2);

        Dataset<Row> dataSet3 = dataSet1.crossJoin(dataSet2);

        logger.info("Join result:\n{}", dataSet3.collectAsList());

        Dataset<Row> dataSet4 = dataSet1.join(dataSet2, "col0").select("col1", "col2", "col3", "col5", "col6", "col7");

        logger.info("Join result:\n{}", dataSet4.collectAsList());

        //sparkContext.stop();

        /*String catalog2 = "{\"table\":{\"namespace\":\"default\",\"name\":\"shcExampleTable1\",\"tableCoder\":\"PrimitiveType\"},\"rowkey\":\"key\",\"columns\":{\"col0\":{\"cf\":\"rowkey\",\"col\":\"key\",\"type\":\"string\"},\"col1\":{\"cf\":\"cf1\",\"col\":\"col1\",\"type\":\"boolean\"},\"col2\":{\"cf\":\"cf2\",\"col\":\"col2\",\"type\":\"double\"},\"col3\":{\"cf\":\"cf3\",\"col\":\"col3\",\"type\":\"float\"},\"col4\":{\"cf\":\"cf4\",\"col\":\"col4\",\"type\":\"int\"},\"col5\":{\"cf\":\"cf5\",\"col\":\"col5\",\"type\":\"bigint\"},\"col6\":{\"cf\":\"cf6\",\"col\":\"col6\",\"type\":\"smallint\"},\"col7\":{\"cf\":\"cf7\",\"col\":\"col7\",\"type\":\"string\"},\"col8\":{\"cf\":\"cf8\",\"col\":\"col8\",\"type\":\"tinyint\"}}}";

        SparkContext sparkContext = session.sparkContext();

        List<SHCExampleTable1> table1s = new ArrayList<>();

        for (int i=0; i<120; i++) {
            table1s.add(getRecord(0));
        }

        Seq<SHCExampleTable1> seq = JavaConversions.asScalaBuffer(table1s);

        RDD<SHCExampleTable1> rdd = sparkContext.parallelize(seq, 10, scala.reflect.ClassTag$.MODULE$.apply(SHCExampleTable1.class));

        sqlContext.createDataFrame(rdd, SHCExampleTable1.class);*/

        session.close();
    }

    private static SHCExampleTable1 getRecord(int val) {
        String row = "row_" + val;
        Double d = (double) val;
        Float f = (float) val;
        Long l = (long) val;
        Short s = (short) val;
        Byte b = (byte) val;

        return new SHCExampleTable1(row, val % 2==0, d, f, val, l, s, val + " extra", b);
    }
}
