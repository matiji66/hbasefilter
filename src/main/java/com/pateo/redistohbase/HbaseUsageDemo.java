package com.pateo.redistohbase;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pateo.constant.Constant;

/** 
 * @author sh04595
 */
public class HbaseUsageDemo {

	static Logger LOG = LoggerFactory.getLogger(HbaseUsageDemo.class);
	static HBaseAdmin admin = null;

	public static void main(String[] args) throws IOException {
 
		long start1 = System.currentTimeMillis();
		LOG.info("--------Table scan start----- " + start1);
		
		
		 ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(10);
		 
		Integer scanTimes = 1;
		for (int i = 0; i < scanTimes ; i++) {
			newFixedThreadPool.execute(new Runnable() {
				
				public void run() {
					long start = System.currentTimeMillis();
					scan();
					LOG.info("Thread. "+Thread.currentThread().getName()+"--------Table scan  end  "+ (System.currentTimeMillis() - start) +" ms");
				}
			});
		}
		LOG.info("--------Table scan  " + scanTimes +" end  "+ (System.currentTimeMillis() - start1) +" ms");
		LOG.info("------------- Finished! ------------- ");
	}

	/**
	 * 查看namespace是否存在
	 * 
	 * @param nameSpace
	 * @return 是否存在该namespace
	 * @throws IOException
	 */
	public static boolean existsNameSpace(String nameSpace) throws IOException {

		boolean flag = false;
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

	// P006000300000008_1478003668
	public static void scan() {

		Table table = null;
//		Configuration configuration = HBaseConfiguration.create();
//		// TODO 访问的时候需要配置host文件
//		configuration.set(Constant.hbase_zookeeper_quorum,
//				"10.1.16.36,10.1.16.35,10.1.16.34");
//		configuration.set("hbase.zookeeper.property.clientPort", "21810");
	 
		Scan scan = new Scan();
//		PrefixFilter prefixFilter = new PrefixFilter(
//				Bytes.toBytes("P006000400003488"));
		Filter prefixFilter = new  FilterList();

		HbaseTablePool hbaseTablePool = HbaseTablePool.getInstance();
//		P006000300000008  
//		scan.setStartRow(Bytes.toBytes("P006000300000008_1476054812"));
//		//scan.setStopRow(Bytes.toBytes ("P006000300000008_1476057404"));
//		scan.setStopRow(Bytes.toBytes ("P006000600003612_2476057404"));
		

		scan.setStartRow(Bytes.toBytes("P008000500051144_1453651200" ));
		scan.setStopRow(Bytes.toBytes ("P008000500051144_1480089599" ));
//		dateConvertToMills("2016-10-13 23:59:00", "yyyy-MM-dd HH:mm:ss")
		scan.setMaxResultsPerColumnFamily(50);
		scan.setMaxResultSize(50);
		
//		 scan.setFilter(prefixFilter);
		
		ResultScanner rs = null;
		Integer count = 0;
		
		 
		try {
			table = hbaseTablePool.getTable();
//			Get get = new Get(Bytes.toBytes("--"));
//			table.get(get);
//			scan.set
			rs = table.getScanner(scan);

			
			for (Result r : rs) {

				Cell[] rawCells = r.rawCells();
				for (int i = 0; i < rawCells.length; i++) {
					Cell cell = rawCells[i];

					// rowKey
					byte[] cloneRow = CellUtil.cloneRow(cell);
					// cell
					byte[] cloneValue = CellUtil.cloneValue(cell);
					byte[] cloneFamily = CellUtil.cloneFamily(cell);
					byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
					System.out.println(Bytes.toString(cloneRow) + "----"
							+ Bytes.toString(cloneFamily) + "----"
							+ Bytes.toString(cloneQualifier) + "----"
							+ Bytes.toString(cloneValue));
 				}

				count++;
				if (count >100) {
					break ;
				}
			}
			System.out.println("-------------------------" + count+ " row -------------------------");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		hbaseTablePool.returnTable(table);
	}

	/**
	 * 测试hbase filter 根据列进行过滤 此处结合了 列值（过滤某一列的值）过滤器与列过滤器（过滤某一列）
	 * 
	 * @param t1Table
	 * @throws IOException
	 */
	private static void testColumnFilter() throws IOException {

		HConnection connection = null;
		HTableInterface t1Table = null;
		String tableName = "obd_minix:vehicle_on_off_flag";

		Configuration configuration = HBaseConfiguration.create();
		configuration.set(Constant.hbase_zookeeper_quorum,
				"10.1.16.36,10.1.16.35,10.1.16.34");
		configuration.set("hbase.zookeeper.property.clientPort", "21810");
		LOG.info("------------- begain! ------------- ");

		// 建立连接
		try {
			System.out.println("---------------connecting -----");
			connection = HConnectionManager.createConnection(configuration);
			System.out.println("---------------connected -----");

		} catch (IOException e) {
			e.printStackTrace();
		}

		// 获取表
		try {
			System.out.println("---------------getTable start -----");
			t1Table = connection.getTable(TableName.valueOf(tableName));
			System.out.println("---------------getTable end  -----");
		} catch (IOException e) {
			e.printStackTrace();
		}

		long currentTimeMillis = System.currentTimeMillis();
		LOG.info("------------- Column Filter ------------- ");

		// 列值过滤器
		FilterList filterList = new FilterList();
		SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
				Bytes.toBytes("f1"), Bytes.toBytes("c1"), CompareOp.EQUAL,
				Bytes.toBytes("on"));
		filterList.addFilter(singleColumnValueFilter);
		// 列过滤器
		DependentColumnFilter columnFilter = new DependentColumnFilter(
				Bytes.toBytes("f1"), Bytes.toBytes("c1"));
		// filterList.addFilter(columnFilter);
		Scan scan = new Scan();
		// scan.addFamily(Bytes.toBytes("f1"));
		// scan.setFilter(singleColumnValueFilter);
		scan.setFilter(filterList);

		Integer count = 0;

		ResultScanner ss = t1Table.getScanner(scan);

		for (Result r : ss) {
			Cell[] rawCells = r.rawCells();

			count++;
			for (int i = 0; i < rawCells.length; i++) {
				Cell cell = rawCells[i];

				// rowKey
				byte[] cloneRow = CellUtil.cloneRow(cell);
				// cell
				byte[] cloneValue = CellUtil.cloneValue(cell);
				byte[] cloneFamily = CellUtil.cloneFamily(cell);
				byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
				System.out.println(Bytes.toString(cloneRow) + "----"
						+ Bytes.toString(cloneFamily) + "----"
						+ Bytes.toString(cloneQualifier) + "----"
						+ Bytes.toString(cloneValue));

				// int rowOffset = cell.getRowOffset();
				// short rowLength = cell.getRowLength();
				// String key =
				// Bytes.toStringBinary(cell.getRowArray(),rowOffset,rowLength);
			}
		}
		LOG.info("--------scan end ----count : " + count);
		long timeLong = System.currentTimeMillis() - currentTimeMillis;
		LOG.info("--------cost time " + (timeLong / 1000) + " s");

		try {
			t1Table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 测试hbase put
	 */
	public void testPut(String columnFamily, HTableInterface t1Table) {
		String rowkey = "ROW111";
		Put put = new Put(Bytes.toBytes(rowkey));
		byte[] family = Bytes.toBytes(columnFamily);
		byte[] qualify_c1 = Bytes.toBytes("c1");
		byte[] value_on = Bytes.toBytes("on");
		put.add(family, qualify_c1, value_on);
		try {
			t1Table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static long dateConvertToMills(String date, String format) {

		Calendar c = Calendar.getInstance();
		try {
			c.setTime(new SimpleDateFormat(format).parse(date));
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}
		return c.getTimeInMillis();
	}

}