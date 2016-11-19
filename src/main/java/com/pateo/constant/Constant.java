package com.pateo.constant;

public interface Constant {

	/**
	 * 项目配置相关的常量
	 */
	String hbase_zookeeper_property_clientPort = "hbase.zookeeper.property.clientPort";
	String hbase_zookeeper_quorum = "hbase.zookeeper.quorum";
	
	String prod_hbase_zookeeper_property_clientPort = "prod.hbase.zookeeper.property.clientPort";
	String prod_hbase_zookeeper_quorum = "prod.hbase.zookeeper.quorum";
	
	String columnFamily = "columnFamily";
	String prefix = "obd_data_id_";
	
	// 一些表名 
	public static String TABLE_BREAKDOWN_TABLE = "breakdown_table";
	public static String TABLE_LOCUS_CAR_SCORE = "locus_car_score";
	public static String TABLE_LOCUS_CAR_SCORE_DETAIL = "locus_car_score_detail";
	public static String TABLE_PRE_LOCUS_CAR_SCORE= "pre_locus_car_score";
	public static String TABLE_T_BY_CAR = "t_by_car";
	public static String TABLE_T_USER = "t_user";
	public static String TABLE_T_VIOLATION = "t_violation";
	
	public static String TABLE_LOCUS_BYCAR = "locus_bycar";
	
	public static String CODE_CODE = "code";
	public static String CODE_TYPE = "type";
	public static String CODE_LEVEL= "level";
	public static String CODE_DESCRIPTION = "descrption";
	public static String CODE_REMIND = "remind";
	
	
	String JDBC_DRIVER = "jdbc.driver";
	public static String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
	String JDBC_URL_BOTAIAPP = "jdbc.url.botaiapp";
	String JDBC_URL_OBDVIN_LOG = "jdbc.url.obdvin_log";
	String JDBC_USER = "jdbc.user";
	String JDBC_PASSWORD = "jdbc.password";
	String JDBC_URL_PROD_OBDVIN_LOG = "jdbc.url.prod.obdvin_log";
	String JDBC_URL_PROD_BOTAIAPP = "jdbc.url.prod.botaiapp";
	String JDBC_USER_PROD = "jdbc.user.prod";
	String JDBC_PASSWORD_PROD = "jdbc.password.prod";
	String SPARK_LOCAL = "spark.local";
	String SPARK_LOCAL_MYSQL = "spark.local.mysql";
	String SPARK_LOCAL_TASKID_SESSION = "spark.local.taskid.session";
	String SPARK_LOCAL_TASKID_PAGE = "spark.local.taskid.page";
	String SPARK_LOCAL_TASKID_PRODUCT = "spark.local.taskid.product";
	String KAFKA_METADATA_BROKER_LIST = "kafka.metadata.broker.list";
	String KAFKA_TOPICS = "kafka.topics";
	String redis_ip = "redis.ip";
	String redis_port = "redis.port";
}
