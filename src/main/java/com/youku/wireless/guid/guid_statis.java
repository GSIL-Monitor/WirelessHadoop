package com.youku.wireless.guid;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.youku.data.driver.MrJobHelper;
import com.youku.data.driver.annotation.Program;
import com.youku.data.driver.annotation.ProgramParam;
import com.youku.data.mapreduce.input.SmartInputFormat;
import com.youku.wireless.utils.DataUtils;
import com.youku.wireless.utils.LogStrUtils;

@Program(name = "statis.guid", description = "guid无线设备库,统计访问接口“initial”并且guid唯一的用户设备数。可根据stime和etime参数配合选择起始时间和结束时间。")
public class guid_statis {

	@ProgramParam(name = "stime", description = "起始时间，包含日期。", required = false)
	private String stime = null;
	
	@ProgramParam(name = "etime", description = "结束时间，包含日期。", required = false)
	private String etime = null;
	
	@ProgramParam(name = "formatlog", description = "是否需要格式化日志，现在用于切分按小时日志中无用数据。0:不需要（default）  1:需要", required = false)
	private int formatlog = 0;
	
	@ProgramParam(name = "pidfile", description = "加上此参数，则统计文件内的pid用户", required = false)
	private String pidfile = null;

	public static class JobMapper extends
			Mapper<LongWritable, Text, Text, guid_statis_request> {
		
		private static int formatlog;
		private static final List<String> pidList = new ArrayList<String>();
		private Text outKey;
				
		@Override
		protected void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException {
			String valueString = value.toString();
			if(!valueString.contains("initial")){
				return;
			}
			
			if (formatlog == 1){
				valueString = LogStrUtils.getLineFromHourfile(valueString);
			}
			
			guid_statis_request r = parseRequest(valueString);
			if (r != null) {
				String guid = r.getGuid();
				String pid = r.getPid();
				
				if (pidList!=null && pidList.size()>0){
					if (pidList.contains(pid)){
						outKey.set(guid);
					}else{
						return;
					}
				}else{
					outKey.set(guid);
				}
				context.write(outKey, r);
			}
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			formatlog = context.getConfiguration().getInt("formatlog",0);
			outKey = new Text();
			initPidList(context.getConfiguration());
		}
		
		private final guid_statis_request parseRequest(String line) throws IOException,
				InterruptedException {
			guid_statis_request r = new guid_statis_request(line);
			if (r==null || r.getGuid()==null){
				return null;
			}
			return r;
		}
		
		private void initPidList(Configuration conf){
			String filename = conf.get("pidfile");
			InputStream input  = null;  
	        try{  
	            FileSystem fs = FileSystem.get(conf);         
	            FSDataInputStream in = fs.open(new Path(filename));            
	            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
	            
	            String line = null;
	            while ((line = reader.readLine())!=null){
	            	if(line.equals(""))
	            		continue;
	            	else
	            		pidList.add(line.trim());
	            }
	        }catch(IOException e){
	        	e.printStackTrace();
	        }finally{  
	            IOUtils.closeStream(input);  
	        }
		}

	}
			
	public static class LogReducer extends Reducer<Text, guid_statis_request, Text, Text> {

		public void reduce(Text key, Iterable<guid_statis_request> values, Context context)
				throws IOException, InterruptedException {
			java.util.Iterator<guid_statis_request> it = values.iterator();
			
			
			long regtime = 0;
			guid_statis_request r_tmp = null;
			//guid_statis_request r = null;
			String response_code = "";
			String method = "";
			String pid = "";
			String ver = "";
			String guid2 = "";
			String os = "";
			String os_ver = "";
			String btype = "";
			String brand = "";
			String operator = "";
			String deviceid = "";
			String ndeviceid = "";
			String mac = "";
			String imei = "";
			String uuid = "";
			
			while (it.hasNext()) {
				r_tmp = it.next();
				long time = r_tmp.getLongtime();
				if (regtime == 0) {
					regtime = time;
					pid = r_tmp.getPid();
					ver = r_tmp.getVer();
					guid2 = r_tmp.getGuid2();
					os = r_tmp.getOs();
					os_ver = r_tmp.getOs_ver();
					btype = r_tmp.getBtype();
					brand = r_tmp.getBrand();
					operator = r_tmp.getOperator();
					deviceid = r_tmp.getDeviceid();
					ndeviceid = r_tmp.getNdeviceid();
					mac = r_tmp.getMac();
					imei = r_tmp.getImei();
					uuid = r_tmp.getUuid();
					response_code = r_tmp.getResponse_code();
					method = r_tmp.getMethod();
					//r = r_tmp;
				} else {
					if (regtime > time) {
						regtime = time;
						pid = r_tmp.getPid();
						ver = r_tmp.getVer();
						guid2 = r_tmp.getGuid2();
						os = r_tmp.getOs();
						os_ver = r_tmp.getOs_ver();
						btype = r_tmp.getBtype();
						brand = r_tmp.getBrand();
						operator = r_tmp.getOperator();
						deviceid = r_tmp.getDeviceid();
						ndeviceid = r_tmp.getNdeviceid();
						mac = r_tmp.getMac();
						imei = r_tmp.getImei();
						uuid = r_tmp.getUuid();
						response_code = r_tmp.getResponse_code();
						method = r_tmp.getMethod();
						//r = r_tmp;
					}
				}
				
			}
			
			Text outValue = new Text();
			outValue.set(pid + "\t" + ver + "\t" 
					+ guid2 +"\t" + os + "\t" + os_ver + "\t"
					+ btype + "\t" + brand + "\t"
					+ operator + "\t" + deviceid + "\t"
					+ ndeviceid + "\t" + regtime + "\t"
					+ mac + "\t" + imei + "\t" + uuid +"\t" + response_code + "\t" + method);
			context.write(key, outValue);
		}
	}
	
	public static class LogFilePathFilter implements PathFilter {
		
		@Override
		public boolean accept(Path path){
			
			String pathName = path.getName();
			
			if (pathName.contains(".bz2") || pathName.contains("gz")){
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

	public boolean run(Configuration conf, MrJobHelper jobHelper,
			String[] infiles, String outdir) throws IOException {
		Job job = jobHelper.newJob(conf, JobMapper.class, LogReducer.class,
				Text.class, Text.class);
		job.getConfiguration().set("stime", stime);
		job.getConfiguration().set("etime", etime);
		job.getConfiguration().setInt("formatlog", formatlog);
		if (pidfile != null && !pidfile.isEmpty()){
			job.getConfiguration().set("pidfile", pidfile);
		}
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(guid_statis_request.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fstm = FileSystem.get(conf);
		Path outDir = new Path(outdir);
		fstm.delete(outDir, true);
		
		job.setInputFormatClass(SmartInputFormat.class);
		//job.setOutputFormatClass(GuidOutputFormat.class);
		LogFilePathFilter.setConf(job.getConfiguration());
		SmartInputFormat.setInputPathFilter(job, LogFilePathFilter.class);
		SmartInputFormat.setInputFiles(job, infiles);
		FileOutputFormat.setOutputPath(job, new Path(outdir));
		System.exit(jobHelper.runJob(job) ? 0 : 1);
		return jobHelper.runJob(job);
//		if(jobHelper.runJob(job)){
//			FsPermission fp = FsPermission.createImmutable((short)660);
//			fstm.setPermission(outDir, fp);
//			return true;
//		}
//		return false;
//		if (jobHelper.runJob(job)){
//			Properties p = new Properties();
//			p.load(this.getClass().getClassLoader().getResourceAsStream("datasource.properties"));
//			DataUtils.importPLPVData(conf, p);
//		}
//		
//		return true;
	}
}
