package com.youku.wireless.statis;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import com.youku.data.driver.MrJobHelper;
import com.youku.data.driver.annotation.Program;
import com.youku.data.driver.annotation.ProgramParam;
import com.youku.data.io.log.AdAccessLogWritable;
import com.youku.data.mapreduce.input.SmartInputFormat;

import com.youku.data.commons.IpArea;
import com.youku.data.commons.ProvinceCityName;
import com.youku.data.commons.IpDataFactory;
import com.youku.data.commons.IpArea.Region;
import com.youku.wireless.guid.guid_statis.LogFilePathFilter;

/**
 * 例子1<br/>
 * 统计不同分类下的广告容量，可以根据参数传递设置是否按照天来区分<br/>
 * 使用的shell命令行如下:<br/>
 * 
 * <pre>
 * hadoop jar data-mr-common-example.jar prog.example1 -R 2 -i/logdata/ad/access/tvalf/20120909 -i/logdata/ad/access/tvalf/20120910 -o/tmp/outdir
 * #解释：
 * # -R设置reduce的个数为2，
 * # -i为输入的文件路径，可以有多个，
 * # -o为输出的文件路径，
 * # -byday为开关，加上则按天统计，不加则为总计
 * 
 * hadoop jar data-mr-common-example.jar prog.example1
 * # 这样使用命令，可以获取详细帮助信息
 * </pre>
 * 
 * @author tuuboo
 * 
 */
@Program(name = "statis.vv", description = "vv统计,可以指定统计的pid，多个pid以,号分割。")
public class vv_statis {

	/**
	 * 这是一个field类型的惨呼声，参数名为pid，命令行可以如下：-pid 69b8,8352
	 */
	@ProgramParam(name = "pid", description = "指定统计的pid,多个pid以\",\"号分割", required = false)
	private String pid = null;
	
	@ProgramParam(name = "byday", description = "统计结果重是否区分天数", required = false)
	private boolean byday = false;
	
	@ProgramParam(name = "stime", description = "起始时间", required = false)
	private String stime = null;
	
	@ProgramParam(name = "etime", description = "结束时间", required = false)
	private String etime = null;

	public static class JobMapper extends
			Mapper<LongWritable, Text, Text, vv_statis_request> {

		//private static LongWritable ONE = new LongWritable(1l);
		//private static Text keyout = new Text();
		private static List<String> pidlist = new ArrayList<String>();
		private static IpArea iparea = null;
		private static ProvinceCityName provcityname = null;
		private boolean byday =false;
		
		@Override
		protected void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException {
			String valueString = value.toString();
			vv_statis_request r = parseRequest(valueString);
			if (r != null) {
				String pid = r.getPid();
				if (pidlist.size() > 0 && !pidlist.contains(pid)){
					return;
				}
				if(!r.getMethod().equals("GET") && !r.getMethod().equals("POST")){
					return;
				}
				
				if(!(r.getUri().contains("statis/vv") && r.getRequest_args().contains("type=begin")) && !(r.getUri().matches("/v1/video(s|)/[a-z,A-Z,0-9]*/playing") && r.getRequest_args().matches(".*event_type=(begin|start).*"))){
					return;
				}
				String prov = "0000000000", city = "0000000000";
				String ip = r.getIp();
				try{
					if(ip!=null && !ip.trim().equals("")){
						Region region = iparea.getRegion(ip);
						//prov = provcityname.getProvinceName(region.getProvince_id());
						//city = provcityname.getCityName(region.getCity_id());
						prov = region.getCountry_id()+region.getProvince_id()+"0000";
						city = region.getCountry_id()+region.getCity_id();
					}
				}catch(Exception e){
					prov = "0000000000";
					city = "0000000000";
				}
				String ver = r.getVer();
				String date_day = r.getDate_day();
				
				Text outKey = new Text();
				Text outKey_ver = new Text();
				Text outKey_all = new Text();
				Text outKey_prov = new Text();
				Text outKey_city = new Text();
				Text outKey_prov_city = new Text();
				
				if(byday){
					outKey.set(date_day+"-"+pid+"-"+prov+"-"+city+"-"+ver);
					outKey_all.set(date_day+"-"+pid +"-" + "ALL" + "-" + "ALL" + "-" + "ALL");
					outKey_ver.set(date_day+"-"+pid +"-" + prov + "-" + city + "-" + "ALL");
					outKey_prov.set(date_day+"-"+pid +"-" + "ALL" + "-" + city + "-" + ver);
					outKey_city.set(date_day+"-"+pid +"-" + prov + "-" + "ALL" + "-" + ver);
					outKey_prov_city.set(date_day+"-"+pid +"-" + "ALL" + "-" + "ALL" + "-" + ver);
				}else{
					outKey.set(pid+"-"+prov+"-"+city+"-"+ver);
					outKey_all.set(pid +"-" + "ALL" + "-" + "ALL" + "-" + "ALL");
					outKey_ver.set(pid +"-" + prov + "-" + city + "-" + "ALL");
					outKey_prov.set(pid +"-" + "ALL" + "-" + city + "-" + ver);
					outKey_city.set(pid +"-" + prov + "-" + "ALL" + "-" + ver);
					outKey_prov_city.set(pid +"-" + "ALL" + "-" + "ALL" + "-" + ver);
				}
				context.write(outKey, r);
				context.write(outKey_all, r);
				context.write(outKey_ver, r);
				context.write(outKey_prov, r);
				context.write(outKey_city, r);
				context.write(outKey_prov_city, r);
			}
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			String pidstr = context.getConfiguration().get("pid");
			if (pidstr!=null && !pidstr.trim().equals("")) {
				String[] pids = pidstr.split(",");
				
				pidlist = new ArrayList<String>();
				for (String pid : pids) {
					pidlist.add(pid);
				}
			}
			
			iparea = IpDataFactory.makeIpArea(context.getConfiguration());
			provcityname = IpDataFactory.makeProvinceCityName(context.getConfiguration());
			byday = context.getConfiguration().getBoolean("byday", false);
		}
		
		private final vv_statis_request parseRequest(String line) throws IOException,
				InterruptedException {
			vv_statis_request request = new vv_statis_request(line);
			String pid = request.getPid();
			if (request != null && pid != null) {
				return request;
			} else {
				return null;
			}
		}

	}
			
	public static class LogReducer extends Reducer<Text, vv_statis_request, Text, Text> {

		public void reduce(Text key, Iterable<vv_statis_request> values, Context context)
				throws IOException, InterruptedException {
			java.util.Iterator<vv_statis_request> it = values.iterator();
			
			int pv = 0;
			vv_statis_request r = null;
			while (it.hasNext()) {
				r = it.next();
				
				pv += 1;
				
			}
			
			Text outValue = new Text();
			outValue.set(Integer.toString(pv));
			context.write(key, outValue);
		}
	}
	
	public static class LogFilePathFilter implements PathFilter {
		
		@Override
		public boolean accept(Path path){
			
			System.out.println("path.getParent.Name:"+path.getParent().getName());
			String pathName = path.getName();
			System.out.println("pathName:"+pathName);
			//String date1 = pathName.substring(0, 8);
			//System.out.println("date1:"+date1);
			
			if (pathName.contains(".bz2") || pathName.contains(".tar.gz") ){
				String date1 = path.getParent().getName();
				if(!timeCompare(date1)){
					return false;
				}else{
					return true;
				}
			}else{
				return true;
			}
			
		}
		
		static FileSystem fs;
		static Configuration conf;

		static void setConf(Configuration _conf) {
			conf = _conf;
			
			stime = conf.get("stime");
			etime = conf.get("etime");
		}
		
		private static String stime;
		private static String etime;
		
		public static final boolean timeCompare(String time1){
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
			try {
				Date d = dateFormat.parse(time1);
				Date d1 = dateFormat.parse(stime);
				Date d2 = dateFormat.parse(etime);
				
				
				if (d.getTime() >= d1.getTime() && d.getTime() <= d2.getTime()) {
					return true;
				} else {
					return false;
				}
			} catch (ParseException e) {
				return false;
			}
		}

	}
	

	/**
	 * 这是框架会调用的run，函数，返回类型必须为boolean 以下的参数有的是可选的，请认真阅读
	 * 
	 * @param conf
	 *            集群配置信息
	 * @param jobHelper
	 *            可选，job帮助信息
	 * @param infile
	 *            可选，参数名必须为“infile”，输入文件，用-i来传入。另，当输入多个文件时，可以用数组的形式，为，
	 *            <code>String[] infiles</code>
	 * @param outdir
	 *            可选，参数名必须为“outdir”，输出文件路径，用-o来传递
	 * @return 当运行成功返回true，否则返回false；
	 * @throws IOException
	 *             可以抛出任意形式的异常
	 */
	public boolean run(Configuration conf, MrJobHelper jobHelper,
			String[] infiles, String outdir) throws IOException {
		Job job = jobHelper.newJob(conf, JobMapper.class, LogReducer.class,Text.class, Text.class);
		job.getConfiguration().set("pid", pid);
		job.getConfiguration().setBoolean("byday", byday);
		job.getConfiguration().set("stime", stime);
		job.getConfiguration().set("etime", etime);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(vv_statis_request.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fstm = FileSystem.get(conf);
		Path outDir = new Path(outdir);
		fstm.delete(outDir, true);
		
		job.setInputFormatClass(SmartInputFormat.class);
		if(stime!=null && !stime.equals("") && etime!=null && !etime.equals("")){
			LogFilePathFilter.setConf(job.getConfiguration());
			SmartInputFormat.setInputPathFilter(job, LogFilePathFilter.class);
		}
		SmartInputFormat.setInputFiles(job, infiles);
		FileOutputFormat.setOutputPath(job, new Path(outdir));

		return jobHelper.runJob(job);
	}
}
