package com.pateo.redistohbase;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.pateo.constant.Constant;
import com.sparkproject.conf.ConfigurationManager;

public class HbaseConnectionPool {

	// private LinkedList<Table> tablePool = new LinkedList<Table>();
	static ConcurrentLinkedDeque<Connection> connectionPool = new ConcurrentLinkedDeque<Connection>();

	private static HbaseConnectionPool instance = null;

	public synchronized Connection getConnection() {

		while (connectionPool.size() == 0) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return connectionPool.poll();
	}

	public void returnTable(Connection connection) {
		connectionPool.push(connection);
	}

	public static HbaseConnectionPool getInstance() {
		if (instance == null) {
			synchronized (HbaseConnectionPool.class) {
				if (instance == null) {
					System.out.println("-------------new----------");
					instance = new HbaseConnectionPool();
				}
			}
		}
		return instance;
	}

	private HbaseConnectionPool() {
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
					"10.1.16.36,10.1.16.35,10.1.16.34");
			configuration.set("hbase.zookeeper.property.clientPort", "21810");
			Connection createConnection = null;
			try {
				createConnection = ConnectionFactory
						.createConnection(configuration);
				connectionPool.push(createConnection);

			} catch (IOException e2) {
				e2.printStackTrace();
			}

			System.out.println("--------table hash"
					+ createConnection.hashCode());
			System.out.println(i + " --------Table connected cost time "
					+ (System.currentTimeMillis() - start) + " ms");
			// try {
			// // Thread.sleep(3000);
			// } catch (InterruptedException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
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

	public static void main(String[] args) throws IOException {

		ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(10);
		final String tableName = "obd_locus:locus_for_app";
		final String tableName2 = "obd_locus:locus_travel";
		final String tableName3 = "obd_locus:obd_locus_last";

		for (int i = 0; i < 1; i++) {

			HbaseConnectionPool instance2 = HbaseConnectionPool.getInstance();
			System.out.println(connectionPool.size() + "-----before------"
					+ Thread.currentThread().getName());

			Connection connection = instance2.getConnection();
//			new HTable(conf, tableName);
			Table table = null;
//			Scan scan = new Scan();
			// SingleColumnValueFilter filter = new SingleColumnValueFilter(
			// Bytes.toBytes("f1"), Bytes.toBytes("start_time"),
			// CompareOp.EQUAL, new LongComparator(2));
//			SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
//					Bytes.toBytes("f1"), Bytes.toBytes("start_time"),
//					CompareOp.EQUAL, Bytes.toBytes("2016-11-16 15:33:58"));
//
//			scan.setFilter(filter2);

			Get get = new Get(Bytes.toBytes("P006000400004244"));
			// 默认是一个版本
//			System.out.println("----------------get.setMaxVersions() "+ get.getMaxVersions());

			try {
				get.setMaxVersions(10);
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				table = connection.getTable(TableName.valueOf(tableName3));
			} catch (IOException e1) {
				e1.printStackTrace();
			}

			Result rs = null;
			try {
				rs = table.get(get);
			} catch (IOException e) {
				e.printStackTrace();
			}

			Cell[] rawCells = rs.rawCells();
			for (int j = 0; j < rawCells.length; j++) {
				Cell cell = rawCells[j];
				// rowKey
				byte[] cloneRow = CellUtil.cloneRow(cell);
				// cell
				CellUtil.getCellKeySerializedAsKeyValueKey(cell);
				byte[] cloneValue = CellUtil.cloneValue(cell);
				byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
				System.out.println(Bytes.toString(cloneRow) + "----"
						+ Bytes.toString(cloneQualifier) + "----"
						+ Bytes.toString(cloneValue));
 			}
			System.out.println("-----------------------rawCells.length -"+rawCells.length);

			// 加入setMaxVersions()方法就可以把所有的版本都取出来了
		}
		System.out.println("-----------------------finished--------------------------");

	}

}
