package com.youku.wireless.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.ExportTool;

/**
 * 
 * @author liuchunjie
 *
 */
public class DataUtils {
	
	/**
	 * 导入PV数据
	 * @param conf
	 * @param properties
	 */
	public static void importPVData(Configuration conf,Properties properties){
		List<String> params = contructBaseConnectionString(properties);
		params.add("--table");
		params.add(properties.getProperty("usr-table-pv"));
		params.add("--export-dir");
		params.add(properties.getProperty("usr-export-dir-pv"));
		importData(conf,params);
	} 

	/**
	 * 导入播放页的PV数据
	 * @param conf
	 * @param properties
	 */
	public static void importPLPVData(Configuration conf,Properties properties){
		List<String> params = contructBaseConnectionString(properties);
		params.add("--table");
		params.add(properties.getProperty("usr-table-pv-pl"));
		params.add("--export-dir");
		params.add(properties.getProperty("usr-export-dir-pv-pl"));
		importData(conf,params);
	}
	
	/**
	 * 构造连接数据库字符串
	 * @param properties
	 * @return
	 */
	private static List<String> contructBaseConnectionString(Properties properties){
		List<String> list = new ArrayList<String>();
		for(Entry<Object,Object> entry : properties.entrySet()){
			String key = entry.getKey().toString();
			if(!key.startsWith("usr")){
				list.add("--"+key);
				list.add(entry.getValue().toString());
			}
		}
		return list;
	}
	
	/**
	 * 通过sqoop导入数据
	 * @param conf
	 * @param params
	 */
	private static void importData(Configuration conf,List<String> params){
		ExportTool exporter = new ExportTool();
		Sqoop sqoop = new Sqoop(exporter);
		sqoop.setConf(conf);
		Sqoop.runSqoop(sqoop, params.toArray(new String[0]));
	}
}
