package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class SparkHiveExample {
        public static void main(String[] args) {

                SparkConf conf = new SparkConf()
                                .set("spark.sql.warehouse.dir", "file:///home/jeery/bigdata-data/hive/warehouse")
                                .set("spark.sql.hive.metastore.jars", "path")
                                .set("spark.sql.hive.metastore.jars.path",
                                                "file:///home/jeery/bigdata-installation/hive/apache-hive-3.1.2-bin/lib/*")
                                .set("spark.driver.extraJavaOptions", "-Dhive.metastore.uris=thrift://localhost:9083")
                                .set("spark.sql.hive.metastore.version", "3.1.2")
                                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

                SparkSession spark = SparkSession.builder().appName("spark hive run").config(conf)
                                .master("spark://DESKTOP-TPBLPIL.localdomain:7077").enableHiveSupport()
                                .getOrCreate();

                //eg 1 - student table
                Dataset<Row> studentDf = spark.read().format("hive").table("student");
                studentDf.show();
                //spark.sql("INSERT INTO TABLE student VALUES ('Dikshant',1,95.5),('Akshat', 2, 96),('Dhruv',3,90.5);").show();

                spark.sql("SELECT count(*) FROM student").show();
                spark.sql("SELECT * FROM student").show();

                //eg 2 - id table
                Dataset<Long> ds = spark.range(4, 12);
                ds.write().saveAsTable("id_table");

                spark.sql("show tables").show();

                
                // spark SQL native syntax
                // spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING
                // hive");

                // HQL syntax
                // spark.sql("CREATE TABLE hive_records(key int, value string) STORED AS
                // PARQUET");


                //scala eg  ---
                // val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i,
                // s"val_$i")))
                // df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
                // df.write.partitionBy("key").format("hive").saveAsTable("hive_part_tbl")

                // table creation spark sql eg --- 
                // spark.sql("CREATE TABLE IF NOT EXISTS employee (name STRING,id INT,salary
                // FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
                // .show();



                spark.stop();
               
        }
}

