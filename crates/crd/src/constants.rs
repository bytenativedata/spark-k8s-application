pub const APP_NAME: &str = "bn-spark";
pub const OPERATOR_NAME: &str = "bn-spark-operator";
pub const OPERATOR_LOG_ENV: &str = "BN_SPARK_OPERATOR_LOG";

pub const CONTROLLER_KIND_JOB: &str = "SparkJob";
pub const CONTROLLER_KIND_SCHD_JOB: &str = "SparkScheduledJob";
pub const CONTROLLER_KIND_SESSION: &str = "SparkSession";
pub const CONTROLLER_KIND_SKO_APPLICATION: &str = "SparkApplication";
pub const CONTROLLER_KIND_SKO_SCHEDULED_APPLICATION: &str = "ScheduledSparkApplication";

pub const CONTROLLER_NAME_JOB: &str = "sparkjob";
pub const CONTROLLER_NAME_SCHD_JOB: &str = "sparkscheduledjob";
pub const CONTROLLER_NAME_SESSION: &str = "sparksession";

pub const CONTROLLER_NAME_SKO_APPLICATION: &str = "sko-application";

pub const SKO_DEFAULT_SERVICE_ACCOUNT: &str = "sparkoperator-spark";

pub const RESOURCE_ROLE_SKO: &str = "spark-k8s-operator";
pub const SPARK_DEFAULT_VERSION: &str = "3.1.1";

pub const SKO_APPLICATION_TYPE_SCALA: &str = "Scala";
pub const SKO_APPLICATION_TYPE_JAVA: &str = "Java";
pub const SKO_APPLICATION_TYPE_PYTHON: &str = "Python";
pub const SKO_APPLICATION_TYPE_R: &str = "R";

pub const SPARK_MAIN_APPLICATION_FILE: &str = "local:///opt/spark/jars/spark-oper-sql_3.1.1-0.1.0.jar";
pub const SPARK_SQL_MAIN_CLASS: &str = "org.apache.spark.sql.hive.thriftserver.SparkSqlScriptCli";
pub const SPARK_SESSION_MAIN_CLASS: &str = "org.apache.spark.sql.hive.thriftserver.SparkSqlHiveThrift2";

// ------------
// spark dirver thrift constants
pub const HIVE_SERVER2_THRIFT_DEFAULT_PORT: i32 = 10001;
pub const HIVE_SERVER2_THRIFT_DEFAULT_NODE_PORT: i32 = 10001;
pub const HIVE_SERVER2_THRIFT_DEFAULT_SERVICE_TYPE: &str = "NodePort";
// spark dirver ui constants
pub const HIVE_SERVER2_UI_DEFAULT_PORT: i32 = 8009;
pub const HIVE_SERVER2_UI_DEFAULT_NODE_PORT: i32 = 8009;
pub const HIVE_SERVER2_UI_DEFAULT_SERVICE_TYPE: &str = "NodePort";

// config names
pub const SPARK_HIVE_SERVER2_WEBUI_HOST: &str = "spark.hive.server2.webui.host";
pub const SPARK_HIVE_SERVER2_WEBUI_PORT: &str = "spark.hive.server2.webui.port";
pub const SPARK_HIVE_SERVER2_THRIFT_BIND_HOST: &str = "spark.hive.server2.thrift.bind.host";
pub const SPARK_HIVE_SERVER2_THRIFT_PORT: &str = "spark.hive.server2.thrift.port";
pub const SPARK_HIVE_SERVER2_ENABLE_DOAS: &str = "spark.hive.server2.enable.doAs";




// TODO:
pub const HISTORY_ROLE_NAME: &str = "node";
pub const HISTORY_IMAGE_BASE_NAME: &str = "spark-k8s";
pub const HISTORY_CONFIG_FILE_NAME: &str = "spark-defaults.conf";
pub const HISTORY_CONFIG_FILE_NAME_FULL: &str = "/bytenative/spark/conf/spark-defaults.conf";

pub const SPARK_CLUSTER_ROLE: &str = "spark-k8s-clusterrole";
pub const SPARK_UID: i64 = 1000;


pub const SQL_FILE_CONFIG_MAP_PREFIX: &str = "sql-statement-";
pub const SQL_FILE_LOCAL_FILE_NAME: &str = "statement.sql";
pub const SQL_FILE_LOCAL_DIR_NAME: &str = "/bytenative/sqls";

pub const S3_SECRET_DIR_NAME: &str = "/bytenative/secrets";
pub const S3_ACCESS_KEY_ID: &str = "accessKey";
pub const S3_SECRET_ACCESS_KEY: &str = "secretKey";