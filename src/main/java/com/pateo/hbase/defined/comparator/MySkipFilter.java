package com.pateo.hbase.defined.comparator;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
//import org.apache.hadoop.hbase.io.Writable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySkipFilter extends FilterBase {
	private boolean filterRow = true;
	private Filter filter;
	public String name;
	public static Logger logger = LoggerFactory.getLogger(MySkipFilter.class);

	public MySkipFilter() {
	}

	public MySkipFilter(Filter filter, String name) {
		this.filter = filter;
		this.name = name;
		logger.warn("name=" + name + " SkipFilterA()");
	}

	private void changeFR(boolean value) {
		this.filterRow = value;
		logger.warn("name=" + name + "changeFR()," + filterRow);
	}

	public Filter.ReturnCode filterKeyValue(KeyValue v) throws IOException {
		logger.warn("qualifier="+ Bytes.toString(v.getBuffer(), v.getQualifierOffset(),v.getQualifierLength()));
		if (Bytes.toString(v.getBuffer(), v.getQualifierOffset(),
				v.getQualifierLength()).startsWith("dd_")) {
			Filter.ReturnCode c = this.filter.filterKeyValue(v);
			if (c == Filter.ReturnCode.INCLUDE) {
				changeFR(false);
				logger.warn("name=" + name + "filterRow(),chmod=" + filterRow);
			}
		}
		// changeFR(c == Filter.ReturnCode.INCLUDE);
		return Filter.ReturnCode.INCLUDE;
	}

	public KeyValue transform(KeyValue v) throws IOException {
		return this.filter.transform(v);
	}

	public boolean filterRow() {
		logger.warn("name=" + name + "filterRow(),filterRow=" + filterRow);
		return this.filterRow;
	}

//	public void write(DataOutput out) throws IOException {
//		out.writeUTF(this.filter.getClass().getName());
//		out.writeUTF(this.name);
//		this.filter.write(out);
//	}
//
//	public void readFields(DataInput in) throws IOException {
//		this.filter = Classes.createForName(in.readUTF());
//		this.name = in.readUTF();
//		this.filter.readFields(in);
//	}
	public boolean isFamilyEssential(byte[] name) throws IOException {
		return this.filter.isFamilyEssential(name);
	}

	public String toString() {
		return getClass().getSimpleName() + " " + this.filter.toString();
	}

	@Override
	public ReturnCode filterKeyValue(Cell v) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}