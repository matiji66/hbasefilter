package com.pateo.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;

public class HBasePageModelTest {

	public static void main(String[] args) {
		
		int pageSize = 10;
		HBasePageModel pageModel = new HBasePageModel(pageSize );
		pageModel = scanResultByPageFilter("DLQX:SZYB_DATA",null,null,null,2,pageModel);
		if(pageModel.getResultList().size() == 0) {
		    //本页没有数据，说明已经是最后一页了。
		    return;
		}
	}
	
	
	/**
	 * 检索指定表的第一行记录。<br>
	 * （如果在创建表时为此表指定了非默认的命名空间，则需拼写上命名空间名称，格式为【namespace:tablename】）。
	 * @param tableName 表名称(*)。
	 * @param filterList 过滤器集合，可以为null。
	 * @return
	 */
	public static Result selectFirstResultRow(String tableName,FilterList filterList) {
	    if(StringUtils.isBlank(tableName)) return null;
	    HTable table = null;
	    try {
	        table = HBaseTableManageUtil.getHBaseTable(tableName);
	        Scan scan = new Scan();
	        if(filterList != null) {
	            scan.setFilter(filterList);
	        }
	        ResultScanner scanner = table.getScanner(scan);
	        Iterator<Result> iterator = scanner.iterator();
	        int index = 0;
	        while(iterator.hasNext()) {
	            Result rs = iterator.next();
	            if(index == 0) {
	                scanner.close();
	                return rs;
	            }
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    } finally {
	        try {
	            table.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	    return null;
	}
	/**
	* 分页检索表数据。<br>
	* （如果在创建表时为此表指定了非默认的命名空间，则需拼写上命名空间名称，格式为【namespace:tablename】）。
	* @param tableName 表名称(*)。
	* @param startRowKey 起始行键(可以为空，如果为空，则从表中第一行开始检索)。
	* @param endRowKey 结束行键(可以为空)。
	* @param filterList 检索条件过滤器集合(不包含分页过滤器；可以为空)。
	* @param maxVersions 指定最大版本数【如果为最大整数值，则检索所有版本；如果为最小整数值，则检索最新版本；否则只检索指定的版本数】。
	* @param pageModel 分页模型(*)。
	* @return 返回HBasePageModel分页对象。
	*/
	public static HBasePageModel scanResultByPageFilter(
														String tableName, 
														byte[] startRowKey, 
														byte[] endRowKey,
														FilterList filterList, 
														int maxVersions, 
														HBasePageModel pageModel) 
	{
	    if(pageModel == null) {
	        pageModel = new HBasePageModel(10);
	    }
	    if(maxVersions <= 0 ) {
	        //默认只检索数据的最新版本
	        maxVersions = Integer.MIN_VALUE;
	    }
	    pageModel.initStartTime();
	    pageModel.initEndTime();
	    if(StringUtils.isBlank(tableName)) {
	        return pageModel;
	    }
	    HTable table = null;
	    
	    try {
	        //根据HBase表名称，得到HTable表对象，这里用到了笔者本人自己构建的一个表信息管理类。
	        table = HBaseTableManageUtil.getHBaseTable(tableName);
	        int tempPageSize = pageModel.getPageSize();
	        boolean isEmptyStartRowKey = false;
	        if(startRowKey == null) {
	            //则读取表的第一行记录，这里用到了笔者本人自己构建的一个表数据操作类。
	            Result firstResult = HBaseTableDataUtil.selectFirstResultRow(tableName, filterList);
	            if(firstResult.isEmpty()) {
	                return pageModel;
	            }
	            startRowKey = firstResult.getRow();
	        }
	        if(pageModel.getPageStartRowKey() == null) {
	            isEmptyStartRowKey = true;
	            pageModel.setPageStartRowKey(startRowKey);
	        } else {
	            if(pageModel.getPageEndRowKey() != null) {
	                pageModel.setPageStartRowKey(pageModel.getPageEndRowKey());
	            }
	            //从第二页开始，每次都多取一条记录，因为第一条记录是要删除的。
	            tempPageSize += 1;
	        }
	        
	        Scan scan = new Scan();
	        scan.setStartRow(pageModel.getPageStartRowKey());
	        if(endRowKey != null) {
	            scan.setStopRow(endRowKey);
	        }
	        PageFilter pageFilter = new PageFilter(pageModel.getPageSize() + 1);
	        if(filterList != null) {
	            filterList.addFilter(pageFilter);
	            scan.setFilter(filterList);
	        } else {
	            scan.setFilter(pageFilter);
	        }
	        if(maxVersions == Integer.MAX_VALUE) {
	            scan.setMaxVersions();
	        } else if(maxVersions == Integer.MIN_VALUE) {
	            
	        } else {
	            scan.setMaxVersions(maxVersions);
	        }
	        ResultScanner scanner = table.getScanner(scan);
	        List<Result> resultList = new ArrayList<Result>();
	        int index = 0;
	        for(Result rs : scanner.next(tempPageSize)) {
	            if(isEmptyStartRowKey == false && index == 0) {
	                index += 1;
	                continue;
	            }
	            if(!rs.isEmpty()) {
	                resultList.add(rs);
	            }
	            index += 1;
	        }
	        scanner.close();
	        pageModel.setResultList(resultList);
	    } catch (Exception e) {
	        e.printStackTrace();
	    } finally {
	        try {
	            table.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	    
	    int pageIndex = pageModel.getPageIndex() + 1;
	    pageModel.setPageIndex(pageIndex);
	    if(pageModel.getResultList().size() > 0) {
	        //获取本次分页数据首行和末行的行键信息
	        byte[] pageStartRowKey = pageModel.getResultList().get(0).getRow();
	        byte[] pageEndRowKey = pageModel.getResultList().get(pageModel.getResultList().size() - 1).getRow();
	        pageModel.setPageStartRowKey(pageStartRowKey);
	        pageModel.setPageEndRowKey(pageEndRowKey);
	    }
	    int queryTotalCount = pageModel.getQueryTotalCount() + pageModel.getResultList().size();
	    pageModel.setQueryTotalCount(queryTotalCount);
	    pageModel.initEndTime();
	    pageModel.printTimeInfo();
	    return pageModel;
	}
}
