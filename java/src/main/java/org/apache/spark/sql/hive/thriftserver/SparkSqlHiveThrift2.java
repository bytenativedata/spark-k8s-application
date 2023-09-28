package org.apache.spark.sql.hive.thriftserver;

public class SparkSqlHiveThrift2 {
    static public void main(String[] args) throws Exception {
        System.out.println("Running from org.apache.spark.sql.hive.thriftserver.SparkSqlHiveThrift2");
        System.out.println("    which wrapped up org.apache.spark.sql.hive.thriftserver.HiveThriftServer2");
        System.out.println("    for running Spark SQL Shell with cluster mode...");
        HiveThriftServer2.main(args);
    }
}
