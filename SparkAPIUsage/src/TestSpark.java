

import oracle.kv.hadoop.table.TableInputFormat;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/*
start-all.sh for spark
javac -cp /Users/stluo/program/spark-2.2.1-bin-hadoop2.7/jars/*:../dist/lib/kvstore.jar:./hello hello/HelloBigDataWorld.java
jar -cvf my.jar hello
cp my.jar ~/program/spark-2.2.1-bin-hadoop2.7/
./bin/spark-submit --class hello.HelloBigDataWorld --master spark://localhost:7077 --jars /Users/stluo/Documents/workspace/kv.testfix/kvstore/dist/lib/kvstore.jar,/Users/stluo/Documents/workspace/kv.testfix/kvstore/dist/lib/je.jar  my.jar
*/
public class TestSpark {

    @SuppressWarnings("null")
    public static void main(String args[]) throws Exception {
        final SparkConf sparkConf = new SparkConf();
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Configuration conf = new Configuration();
        conf.set("oracle.kv.kvstore", "mystore");
        conf.set("oracle.kv.tableName", "mytable");
        conf.set("oracle.kv.hosts", "localhost:20000");

        JavaPairRDD<PrimaryKey, Row> rddPair =
           sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                              PrimaryKey.class, Row.class);

        JavaRDD<MyTable> rdd =
                rddPair.map(
                    x -> new MyTable(x._1().get("id").asInteger().get(),
                                     x._2().get("tsMillis").asLong().get(),
                                     x._2().get("logLevel").asString().get(),
                                     x._2().get("hostId").asInteger().get(),
                                     x._2().get("message").asString().get()));

        final SparkSession dfSession = SparkSession.builder().getOrCreate();
        Dataset<org.apache.spark.sql.Row> df =
            dfSession.createDataFrame(rdd, MyTable.class);

        df.createOrReplaceTempView("mytable");

        conf.set("oracle.kv.tableName", "hosttable");
        rddPair = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                                     PrimaryKey.class, Row.class);
        JavaRDD<HostTable> rddHost = rddPair.map(
            x -> new HostTable(x._1().get("id").asInteger().get(),
                               x._2().get("name").asString().get()));
        df = dfSession.createDataFrame(rddHost, HostTable.class);

        df.createOrReplaceTempView("hosttable");
        System.out.println("select * from mytable");
        Dataset<org.apache.spark.sql.Row> sqlDF =
            dfSession.sql("select * from mytable");
        sqlDF.show();
        System.out.println("select * from hosttable");
        sqlDF = dfSession.sql("select * from hosttable");
        sqlDF.show();

        System.out.println("select count(*) from mytable group by logLevel");
        sqlDF = dfSession.sql("select count(*) from mytable group by logLevel");
        sqlDF.show();

        System.out.println(
            "select id, tsMillis, message from mytable where id < 10 order by tsMillis");
        sqlDF = dfSession.sql(
            "select id, tsMillis, message from mytable where id < 10 order by tsMillis");
        sqlDF.show();

        System.out.println("select sum(id) from mytable group by logLevel");
        sqlDF = dfSession.sql("select sum(id) from mytable group by logLevel");
        sqlDF.show();

        System.out.println("select avg(id) from mytable group by logLevel");
        sqlDF = dfSession.sql("select avg(id) from mytable group by logLevel");
        sqlDF.show();

        System.out.println("select * from mytable " +
                "inner join hosttable on mytable.hostId = hosttable.id");
        sqlDF =
            dfSession.sql(
                "select * from mytable " +
                "inner join hosttable on mytable.hostId = hosttable.id");
        sqlDF.show(1000);

        dfSession.close();
        sc.close();
    }

    public static class MyTable {
        private int id;
        private long tsMillis;
        private String logLevel;
        private int hostId;
        private String message;

        public MyTable(int id,
                long tsMillis,
                String logLevel,
                int hostId,
                String message) {
            super();
            this.id = id;
            this.tsMillis = tsMillis;
            this.logLevel = logLevel;
            this.hostId = hostId;
            this.message = message;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public long getTsMillis() {
            return tsMillis;
        }

        public void setTsMillis(long tsMillis) {
            this.tsMillis = tsMillis;
        }

        public String getLogLevel() {
            return logLevel;
        }

        public void setLogLevel(String logLevel) {
            this.logLevel = logLevel;
        }

        public int getHostId() {
            return hostId;
        }

        public void setHostId(int hostId) {
            this.hostId = hostId;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }

    public static class HostTable {
        private int id;
        private String name;
        public HostTable(int id, String name) {
            super();
            this.id = id;
            this.name = name;
        }
        public int getId() {
            return id;
        }
        public void setId(int id) {
            this.id = id;
        }
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
    }
}

