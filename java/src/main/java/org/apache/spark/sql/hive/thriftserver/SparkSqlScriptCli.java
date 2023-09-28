package org.apache.spark.sql.hive.thriftserver;

import org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver;
public class SparkSqlScriptCli {
    static public void main(String[] args) throws Exception {
        System.out.println("Running from org.apache.spark.sql.hive.thriftserver.SparkSqlScriptCli");
        System.out.println("    which wrapped up org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver");
        System.out.println("    for running Spark SQL Shell from with cluster mode...");
        SparkSQLCLIDriver.main(args);
    }
}