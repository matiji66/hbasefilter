package com.pateo.redistohbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pateo.constant.Constant;
import com.pateo.utils.PropertyUtils;

import redis.clients.jedis.Jedis;

/**
 * Java操作redis (增删改查)
 * 
 * @author sh04595
 *
 */
public class VehicleOnOffFlagFromJedisToHbase {

	// static Jedis redis = new Jedis("obd-redis.marathon.pateo.cn", 6379);//
	// 连接redis

	static Jedis redis = new Jedis(PropertyUtils.getValue(Constant.redis_ip),
			Integer.valueOf(PropertyUtils.getValue(Constant.redis_port)));// 连接redis
	static Logger LOG = LoggerFactory.getLogger(VehicleOnOffFlagFromJedisToHbase.class.getSimpleName());
	static HBaseAdmin admin = null;

	/**
	 * 查看namespace是否存在
	 * @param nameSpace
	 * @return 是否存在该namespace 
	 * @throws IOException
	 */
	public static boolean existsNameSpace(String nameSpace) throws IOException {
		
		boolean flag = false ;
		   NamespaceDescriptor[] listNamespace = admin.listNamespaceDescriptors();
	        for (NamespaceDescriptor namespaceDescriptor : listNamespace) {
				String ns = namespaceDescriptor.getName();
			 if (nameSpace.equals(ns)) {
				flag = true;
			}
		}
		return flag;
	}
	/**
	 * 
	 * @param tableName
	 * @return 是否存在该表
	 * @throws IOException
	 */
	public static boolean existsTable(String tableName) throws IOException {
		    return admin.isTableAvailable(tableName);
	}
	
	
	public static void main(String[] args) {
		HConnection connection = null;
		HTableInterface t1Table = null;
		String columnFamily = "f1";
		Put put = null;
		String tableName = "obd_locus:locus_travel";
		 
		Configuration configuration = HBaseConfiguration.create();
		configuration.set(Constant.hbase_zookeeper_quorum,"10.1.16.36,10.1.16.35,10.1.16.34");
		configuration.set("hbase.zookeeper.property.clientPort","21810");
		LOG.info("------------- begain! ------------- ");

		System.out.println("---------------connecting -----");
		// 建立连接
		try {
			connection = HConnectionManager.createConnection(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// 获取表
		try {
			t1Table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOG.info("------------- begain! ------------- ");

		String rowkey = "ROW111";
		put = new Put(Bytes.toBytes(rowkey));
		byte[] family = Bytes.toBytes(columnFamily);
		byte[] qualify_c1 = Bytes.toBytes("c1");
		byte[] value_on = Bytes.toBytes("on");
		put.add(family, qualify_c1, value_on);
		try {
			t1Table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			t1Table.close();
			LOG.info("------------- Table connection closed! Finished! ------------- ");

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		LOG.info("------------- Finished! ------------- ");
		System.out.println("---------------Finished -----");

	}
	
	public static void redisToHbase() {


//		System.out.println(" cf is :" +PropertyUtils.getValue(Constant.columnFamily));
//		System.exit(0);
		
		// 过滤出符合条件key: obd_data_id_P006000300007695 (obd_data_id_ + obd_id)
		
		// 列出所有的key，查找特定的key如：redis.keys("foo")
		Set<String> keys = redis.keys("obd_data_id_*");
		LOG.info(" ------------------  total size " + keys.size() + " ------------------  ");
		
		System.out.println(" total size " + keys.size());

		HConnection connection = null;
		HTableInterface t1Table = null;
		String columnFamily = "f1";
		Put put = null;
		String nameSpace  = "obd_minix";
		String tableName = "obd_minix:vehicle_on_off_flag_tmp";
		
		
		Configuration configuration = HBaseConfiguration.create();
		configuration.set(Constant.hbase_zookeeper_quorum,PropertyUtils.getValue(Constant.hbase_zookeeper_quorum));
		// configuration.addResource("/usr/local/hbase-1.0.1.1/conf/hbase-site.xml");
		// configuration.set(TableInputFormat.INPUT_TABLE,"obd_minix:vehicle_on_off_flag");

		
		try {
			admin = new HBaseAdmin(configuration);
			// create namespace named "obd_minix" 
			
			if (! existsNameSpace(nameSpace)) {
				admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
				LOG.info(" ----------------- create nameSpace " + nameSpace + " ----------------");
			}
			// create tableDesc, with namespace name "obd_minix" and table name
			if (! existsTable(tableName) ) {

				HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
//				ASYNC_WAL ： 当数据变动时，异步写WAL日志
//				SYNC_WAL ： 当数据变动时，同步写WAL日志
//				FSYNC_WAL ： 当数据变动时，同步写WAL日志，并且，强制将数据写入磁盘
//				SKIP_WAL ： 不写WAL日志
//				USE_DEFAULT ： 使用HBase全局默认的WAL写入级别，即 SYNC_WA
				tableDesc.setDurability(Durability.SYNC_WAL);
				
				// add a column family columnFamily
				HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
				tableDesc.addFamily(hcd);
				
				admin.createTable(tableDesc);
				LOG.info(" -----------------create table " + tableName + " ----------------");
			}

			admin.close();

		} catch (MasterNotRunningException e1) {
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		// 建立連接
		try {
			connection = HConnectionManager.createConnection(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// 获取表
		try {
			t1Table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}

		byte[] family = Bytes.toBytes(columnFamily);
		byte[] qualify_c1 = Bytes.toBytes("c1");
		byte[] value_on = Bytes.toBytes("on");
		byte[] value_180 = Bytes.toBytes("180");
		
		
		byte[] qualify_c2 = Bytes.toBytes("c2");		
		byte[] value_off = Bytes.toBytes("off");
		byte[] value_60 = Bytes.toBytes("60");
		
		String field_obd_id = "obd_id";
		String field_on_off_flag = "on_off_flag";
		
		// redis 中put格式为 key(String): value(HashMap)
		// String hget = redis.hget("obd_data_id_P006000300007695", "obd_id");
		// System.out.println(hget);

		Iterator<String> keyiterator = keys.iterator();
		Integer index = 0;

		while (keyiterator.hasNext()) {

			String key = keyiterator.next();

			List<String> value = redis.hmget(key, field_obd_id, field_on_off_flag);
			String status = value.get(1);
			String rowkey = value.get(0);

			put = new Put(Bytes.toBytes(rowkey));
			if ("on".equals(status)) {
				// 当status为 on的时候，put f1 c1 on, put f1 c2 180
				put.add(family, qualify_c1, value_on);
				put.add(family, qualify_c2,value_180);

			} else {
				// 当status为 on的时候，put f1 c1 off, put f1 c2 60
				put.add(family, qualify_c1, value_off);
				put.add(family, qualify_c2, value_60);
			}
			try {
				t1Table.put(put);
			} catch (IOException e) {
				e.printStackTrace();
			}
			// 也可以put list集合
			// t1Table.put(puts)

			index++;
			if (index % 1000 == 0) {
				LOG.info(" Now put " + index + " record !");
			}
		}

		try {
			t1Table.close();
			LOG.info("------------- Table connection closed! Finished! ------------- ");
			LOG.info("-------------- Total put " + index + " records   ------------- ");

		} catch (IOException e) {
			e.printStackTrace();
		}
		// create_namespace 'obd_minix'
		// create 'obd_minix:vehicle_on_off_flag', 'f1'
		// put 'obd_minix:vehicle_on_off_flag', 'P006000300000274','f1:c1','on'value=on value=180
		// delete 'obd_minix:vehicle_on_off_flag','P006000300000274','f1:c1','on'

		// P006000600005038 column=f1:c1, timestamp=1462955867397, value=off
		// P006000600005038 column=f1:c2, timestamp=1462955867397, value=60
		// P008000400000405 column=f1:c1, timestamp=1462955867397, value=on
		// P008000400000405 column=f1:c2, timestamp=1462955867397, value=180

		// hive中的表的格式
		// key string from deserializer
		// status string from deserializer
		// time int from deserializer

	
	}
	
	
}