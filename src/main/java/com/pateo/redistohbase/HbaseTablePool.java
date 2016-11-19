package com.pateo.redistohbase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import com.pateo.constant.Constant;
import com.sparkproject.conf.ConfigurationManager;

public class HbaseTablePool {

//	private LinkedList<Table> tablePool = new LinkedList<Table>();
	static ConcurrentLinkedDeque<Table> tablePool = new ConcurrentLinkedDeque<Table>();
	
	private static HbaseTablePool instance = null;

	public synchronized Table getTable() {
		
		while (tablePool.size() == 0) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return tablePool.poll();
	}

	public void returnTable(Table table) {
		tablePool.push(table);
	}

	public static HbaseTablePool getInstance() {
		if (instance == null) {
			synchronized (HbaseTablePool.class) {
				if (instance == null) {
					System.out.println("-------------new");
					instance = new HbaseTablePool();
				}
			}
		}
		return instance;
	}

	private HbaseTablePool() {
		// 首先第一步，获取数据库连接池的大小，就是说，数据库连接池中要放多少个数据库连接
		// 这个，可以通过在配置文件中配置的方式，来灵活的设定
		int datasourceSize = ConfigurationManager
				.getInteger(Constant.JDBC_DATASOURCE_SIZE);

		// 然后创建指定数量的数据库连接，并放入数据库连接池中
		for (int i = 0; i < datasourceSize; i++) {
			// boolean local = ConfigurationManager
			// .getBoolean(Constant.SPARK_LOCAL_MYSQL);
			long start = System.currentTimeMillis();

			String tableName = "obd_locus:locus_for_app";

			Configuration configuration = HBaseConfiguration.create();
			// TODO 访问的时候需要配置host文件
			configuration.set(Constant.hbase_zookeeper_quorum,
//					"10.1.16.36,10.1.16.35,10.1.16.34"
					"10.172.10.169,10.172.10.168,10.172.10.170"
					);
			configuration.set("hbase.zookeeper.property.clientPort", "21810");
			configuration.set("hbase.zookeeper.property.clientPort", "2181");

			Connection createConnection = null;
			try {
				createConnection = ConnectionFactory
						.createConnection(configuration);
			} catch (IOException e2) {
				e2.printStackTrace();
			}
			Table table = null;
			try {
				table = createConnection.getTable(TableName.valueOf(tableName));
			} catch (IOException e2) {
				e2.printStackTrace();
			}
			tablePool.push(table);
			
			System.out.println("--------table hash" + table.hashCode());
			System.out.println( i+ " --------Table connected cost time "
					+ (System.currentTimeMillis() - start) + " ms");
//			try {
////				Thread.sleep(3000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}

	}

	public static Table initPool() {

		String tableName = "obd_locus:locus_for_app";
		Configuration configuration = HBaseConfiguration.create();
		// TODO 访问的时候需要配置host文件
		configuration.set(Constant.hbase_zookeeper_quorum,
				"10.1.16.36,10.1.16.35,10.1.16.34");
		configuration.set("hbase.zookeeper.property.clientPort", "21810");
		Connection createConnection = null;
		try {
			createConnection = ConnectionFactory
					.createConnection(configuration);
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		Table table2 = null;
		try {
			table2 = createConnection.getTable(TableName.valueOf(tableName));
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		return table2;
	}
	
	public static void main(String[] args) {
		
		 ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(10);
		 int maxThread = 500;
		 for (int i = 0; i < maxThread; i++) {
				newFixedThreadPool.execute(new Runnable() {

					public void run() {
						// TODO Auto-generated method stub
						
						HbaseTablePool instance2 = HbaseTablePool.getInstance();
						System.out.println(tablePool.size() +"-----before------" + Thread.currentThread().getName());

						instance2.getTable();
						System.out.println(tablePool.size() +"------after------" + Thread.currentThread().getName());
						
					}

				});
				
				
				if (i==(maxThread -1)) {
					
				}
		}
		
		
	}

}
