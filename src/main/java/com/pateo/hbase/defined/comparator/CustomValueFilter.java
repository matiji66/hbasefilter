package com.pateo.hbase.defined.comparator;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.BinaryComparator;

import com.pateo.hbase.defined.comparator.CustomNumberComparator;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.CompareType;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.hbase.KeyValue;
 
public class CustomValueFilter extends FilterBase{
	 private byte[] value = null;
	    private boolean filterRow = true;
	    /**
	     * map中存放需要读的行RowKey
	     */
	    public Map<Object, Object> map = new HashMap<Object, Object>();
	 
	    public CustomValueFilter()
	    {
	        super();
	    }
	 
	    public CustomValueFilter(byte[] value)
	    {
	        this.value = value;
	    }
	 
	    
	    @Override
	    public ReturnCode filterKeyValue(Cell ignored) throws IOException
	    {
	        if (this.filterRow == false)
	            return ReturnCode.INCLUDE;
	        else
	            return ReturnCode.NEXT_ROW;
	    }

//		@Override
//		public ReturnCode filterKeyValue(Cell v) throws IOException {
//			// TODO Auto-generated method stub
//			return null;
//		}
	 
	    /**
	     * 行过滤，查询该行RowKey是否在Map中
	     */
	    @Override
	    public boolean filterRowKey(byte[] buffer, int offset, int length)
	    {
	        byte[] rowKey = Arrays.copyOfRange(buffer, offset, offset + length);
	        String str = new String(rowKey);
	        if (map.containsKey(str))
	        { // 在0(1)时间内返回，效率较高
	            this.filterRow = false; // false表示包括这一行
	        }
	        return this.filterRow;
	    }
	 
	    @Override
	    public void reset()
	    {
	        this.filterRow = true;
	    }
	 
	    @Override
	    public boolean filterRow()
	    {
	        return filterRow;
	    }
	 
	    /**
	     * 将Map中的数据以Byte[]形式传给服务器
	     */
//	    @Override
//	    public void write(DataOutput dataOutput) throws IOException
//	    {
//	        Bytes.writeByteArray(dataOutput, this.value);
//	    }
	 
	    /**
	     * 服务器读取Byte[]数据，再将数据存储到Map中 不同的RowKey以","分割
	     */
//	    @Override
//	    public void readFields(DataInput dataInput) throws IOException
//	    {
//	        this.value = Bytes.readByteArray(dataInput);
//	 
//	        String string = new String(this.value);
//	        String[] strs = string.split(",");
//	        for (String str : strs)
//	        {
//	            map.put(str, str);
//	        }
//	    }

}
