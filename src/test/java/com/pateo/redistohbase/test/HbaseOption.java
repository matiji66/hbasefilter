package com.pateo.redistohbase.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseOption {

	public static void main(String[] args) {

		Configuration conf = HBaseConfiguration.create();

		HTable table = null;
		try {
			table = new HTable(conf, "rd_ns:itable");
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		FilterList filterList = new FilterList(
				FilterList.Operator.MUST_PASS_ALL);

		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes("info"), Bytes.toBytes("age"),
				CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("25"));

		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes("info"), Bytes.toBytes("age"),
				CompareOp.LESS_OR_EQUAL, Bytes.toBytes("30"));

		filterList.addFilter(filter1);

		filterList.addFilter(filter2);

		Scan scan = new Scan();

		scan.setFilter(filterList);

		ResultScanner rs = null;
		try {
			rs = table.getScanner(scan);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		for (Result r : rs) {
			for (Cell cell : r.rawCells()) {
				System.out.println("Rowkey : " + Bytes.toString(r.getRow())
						+ "   Familiy:Quilifier : "
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "   Value : "
						+ Bytes.toString(CellUtil.cloneValue(cell))
						+ "   Time : " + cell.getTimestamp());
			}
		}

		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
//	扫描指定行键范围，通过末尾加0，使得结果集包含StopRow
	public void test2() throws IOException {
		  Configuration conf = HBaseConfiguration. create ();
	        HTable table =  new  HTable(conf,  "rd_ns:itable" );
	        Scan s =  new  Scan();
	        s. setStartRow (Bytes. toBytes ( "100001" ));
	        s. setStopRow (Bytes. toBytes ( " 1000020 " ));

	        ResultScanner rs = table.getScanner(s);

	         for  (Result r : rs) {

	             for  (Cell cell : r.rawCells()) {

	                System. out .println(
	                         "Rowkey : " +Bytes. toString (r.getRow())+
	                         "   Familiy:Quilifier : " +Bytes. toString (CellUtil. cloneQualifier(cell))+
	                         "   Value : " +Bytes. toString (CellUtil. cloneValue (cell))+
	                         "   Time : " +cell.getTimestamp()
	                        );
	            }

	        }

	        table.close();
	}
	
//		（1）扫描表中的 所有行 的最新版本数据
	public void getLastVersionData() throws IOException {

        Configuration conf = HBaseConfiguration. create ();

        HTable table =  new  HTable(conf,  "rd_ns:itable" );

        Scan s =  new  Scan();
        ResultScanner rs = table.getScanner(s);
         for  (Result r : rs) {
             for  (Cell cell : r.rawCells()) {
                System. out .println(
                         "Rowkey : " +Bytes. toString (r.getRow())+
                         "   Familiy:Quilifier : " +Bytes. toString (CellUtil. cloneQualifier(cell))+
                         "   Value : " +Bytes. toString (CellUtil. cloneValue (cell))+
                         "   Time : " +cell.getTimestamp()
                        );
            }

        }
        table.close();
	}
	
	public void getRemovedData() throws IOException {
		Configuration conf = HBaseConfiguration. create ();

        HTable table =  new  HTable(conf,  "rd_ns:itable" );

        Scan s =  new  Scan();
        s.setStartRow(Bytes. toBytes ( "100003" ));
        s.setRaw( true );
        s.setMaxVersions();
        

        ResultScanner rs = table.getScanner(s);

         for  (Result r : rs) {

             for  (Cell cell : r.rawCells()) {

                System. out .println(
                         "Rowkey : " +Bytes. toString (r.getRow())+
                         "   Familiy:Quilifier : " +Bytes. toString (CellUtil. cloneQualifier(cell))+
                         "   Value : " +Bytes. toString (CellUtil. cloneValue (cell))+
                         "   Time : " +cell.getTimestamp()
                        );
            }
        }

        table.close();
	}
}
