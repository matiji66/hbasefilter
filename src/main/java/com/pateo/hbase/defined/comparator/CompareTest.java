package com.pateo.hbase.defined.comparator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.pateo.constant.Constant;
import com.pateo.utils.PropertyUtils;



import com.pateo.hbase.defined.comparator.CustomNumberComparator;
/**
 *  scan 'test:user', {COLUMNS =>'basic:age', LIMIT => 10}
 * 
 * filter 数值的使用 (目前支持的数据类型为double)
 * 使用注意事项，hbase中的value不能为空，否则会报错，
 * 
 * 此时就要两个过滤器结合
 * 	    	SingleColumnValueFilter scnl = new SingleColumnValueFilter(  
		        Bytes.toBytes("basic"),   
		        Bytes.toBytes("age"),   
		        CompareFilter.CompareOp.NOT_EQUAL,   
		        new BinaryComparator(Bytes.toBytes(""))
		        );
		    SingleColumnValueFilter scvf = new SingleColumnValueFilter(  
		        Bytes.toBytes("basic"),   
		        Bytes.toBytes("age"),   
		        CompareFilter.CompareOp.GREATER,   
		        new CustomNumberComparator(Bytes.toBytes(18.0),"double" )
//		        new CustomNumberComparator(Bytes.toBytes(18),"int" )
//		        new CustomNumberComparator(Bytes.toBytes(18.0),"float" )
		        );
 * 
 */

public class CompareTest {
	static HBaseAdmin admin = null;

	public static void main(String[] args) throws IOException {

//		byte[] bytes = Bytes.toBytes("");
//		
//		if (null ==Bytes.toString(bytes)) {
//			System.out.println(" ---- null to bytes--" + Bytes.toString(bytes).length() + "--null-" );
//			
//		}else if ("" == Bytes.toString(bytes)) {
//			System.out.println(" ---- null to bytes--" + Bytes.toString(bytes).length() + "--''-" );
//		}

		Configuration configuration = HBaseConfiguration.create();
		configuration.set(Constant.hbase_zookeeper_quorum,
				PropertyUtils.getValue(Constant.hbase_zookeeper_quorum));

		HConnection connection = null;
		HTableInterface t1Table = null;
		String tableName = "test:user";
		// basic:age
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

		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("basic"));
		FilterList filterList = new FilterList();

		ValueFilter valueFilter = new ValueFilter(CompareOp.GREATER, new LongComparator(2));
//		 filterList.addFilter(valueFilter);
		//1. 字符串是否包含
		ValueFilter valueFilter2 = new ValueFilter(CompareOp.EQUAL,new SubstringComparator(".8"));
		//2. 正则匹配
//		ValueFilter valueFilter2 = new ValueFilter(CompareOp.EQUAL,new RegexStringComparator("ian"));
//		
//		filterList.addFilter(valueFilter2);
 		// 3. 下面的 数值比较不可行 报错
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				Bytes.toBytes("basic"), Bytes.toBytes("name"),
				CompareOp.EQUAL,  new LongComparator(2));
//		 filterList.addFilter(filter);

		// 4. regexStringComparator 正则匹配某一类 推荐使用 get
		//If an already known column qualifier is looked for, use Get.addColumn directly 
		QualifierFilter qualifierFilter = new QualifierFilter(CompareOp.EQUAL,
				new RegexStringComparator("age"));
//		filterList.addFilter(qualifierFilter);
		

		// 5. qualifierComparator
		Filter filterName = new SingleColumnValueFilter(
				Bytes.toBytes("basic"), Bytes.toBytes("name"),
				CompareOp.GREATER_OR_EQUAL, new RegexStringComparator("li"));
//		filterList.addFilter(filterName);
		
		// 6. 这是不奏效的比较方式
		SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("basic"), Bytes.toBytes("age"),  
                CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(20)  
                );

//		filterList.addFilter(singleColumnValueFilter);

		SingleColumnValueFilter bounds = new SingleColumnValueFilter(  
		        Bytes.toBytes("basic"),   
		        Bytes.toBytes("age"),
		        CompareFilter.CompareOp.LESS,   
		        new CustomNumberComparator(Bytes.toBytes(80.0),"double" )
//		        new CustomNumberComparator(Bytes.toBytes(18),"int" ) // 支持性不好
//		        new CustomNumberComparator(Bytes.toBytes(18.0),"float" ) // 支持性不好 
		        );
//		filterList.addFilter(bounds);			
		// 7. 字符串比较 ，自定义的比较器传入我们自己定义的两个参数
//		ByteArrayComparable;
		SingleColumnValueFilter nullFilter = new SingleColumnValueFilter(  
		        Bytes.toBytes("basic"),   
		        Bytes.toBytes("age"),   
		        CompareFilter.CompareOp.NOT_EQUAL,   
		        new BinaryComparator(Bytes.toBytes(""))
		        );
		filterList.addFilter(nullFilter);
 
	
		SingleColumnValueFilter scvf = new SingleColumnValueFilter(  
		        Bytes.toBytes("basic"),   
		        Bytes.toBytes("age"),   
		        CompareFilter.CompareOp.GREATER,   
		        new CustomNumberComparator(Bytes.toBytes(18.0),"double" )
//		        new CustomNumberComparator(Bytes.toBytes(18),"int" ) // 支持性不好
//		        new CustomNumberComparator(Bytes.toBytes(18.0),"float" ) // 支持性不好 
		        );
		filterList.addFilter(scvf);
		
		scan.setFilter(filterList);
		System.out.println("-------------------begain scan ----------------");

		ResultScanner ss = t1Table.getScanner(scan);
		for (Result r : ss) {
			Cell[] rawCells = r.rawCells();
			for (int i = 0; i < rawCells.length; i++) {
				Cell cell = rawCells[i];
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.println(qualifier + "------------" + value);
			}
		}
		System.out.println("-------------------scan end ----------------");
	}
}
