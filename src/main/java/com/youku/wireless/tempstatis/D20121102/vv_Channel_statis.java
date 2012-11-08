package com.youku.wireless.tempstatis.D20121102;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.youku.data.commons.IpArea;
import com.youku.data.commons.IpArea.Region;
import com.youku.data.commons.IpDataFactory;
import com.youku.data.commons.ProvinceCityName;
import com.youku.data.commons.VideoDataFactory;
import com.youku.data.driver.MrJobHelper;
import com.youku.data.driver.annotation.Program;
import com.youku.data.driver.annotation.ProgramParam;
import com.youku.data.mapreduce.input.SmartInputFormat;

@Program(name = "Channel.vv", description = "分地域频道的vv统计！")
public class vv_Channel_statis {
	
	@ProgramParam(name = "stime", description = "起始时间", required = false)
	private String stime = null;
	
	@ProgramParam(name = "etime", description = "结束时间", required = false)
	private String etime = null;

	public static class JobMapper extends
			Mapper<LongWritable, Text, Text, vv_Channel_statis_request> {

		private static List<String> pidlist = new ArrayList<String>();
		private static IpArea iparea = null;
		private static ProvinceCityName provcityname = null;
		private boolean byday =false;
		
		@Override
		protected void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException {
			String valueString = value.toString();
			vv_Channel_statis_request r = parseRequest(valueString);
			if (r != null) {
				String pid = r.getPid();
				if (pidlist.size() > 0 && !pidlist.contains(pid)){
					return;
				}
				String vid = r.getVid();
				
				//String prov = "0000000000", city = "0000000000";
				String prov = "null", city = "null";
				String ip = r.getIp();
				try{
					if(ip!=null && !ip.trim().equals("")){
						Region region = iparea.getRegion(ip);
						prov = provcityname.getProvinceName(region.getProvince_id());
						city = provcityname.getCityName(region.getCity_id());
						//prov = region.getCountry_id()+region.getProvince_id()+"0000";
						//city = region.getCountry_id()+region.getCity_id();
					}
				}catch(Exception e){
					//prov = "0000000000";
					//city = "0000000000";
					prov = "null";
					city = "null";
				}
				//String ver = r.getVer();
				String date_day = r.getDate_day();
				
				Text outKey = new Text();
				outKey.set(date_day+"-"+prov+"-"+city+"-"+vid);
				context.write(outKey, r);
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
		
		private final vv_Channel_statis_request parseRequest(String line) throws IOException,
				InterruptedException {
			vv_Channel_statis_request request = new vv_Channel_statis_request();
			if(line.contains("statis/vv") && line.contains("type=begin")){
				request.vv_Channel_statis_request_api3(line);
			}else{
				if(line.contains("/iphone/videos") && line.contains("/play")){
					request.vv_Channel_statis_request_ios(line);
				}else if(line.contains("getVideoDetail")){
					request.vv_Channel_statis_request_api2(line);
					if(request.getVid()==null || request.getVid().equals("")){
						request.vv_Channel_statis_request_go(line);
					}
				}else if(line.matches("/v1/video(s|)/[a-z,A-Z,0-9]*/playing") && !line.contains("Paike;1.5;iPhone")){
					request.vv_Channel_statis_request_paike(line);
				}else{
					return null;
				}
			}
			String vid = request.getVid();
			String date = request.getDate_day();
			if (request != null && vid != null && date!=null) {
				return request;
			} else {
				return null;
			}
		}

	}
			
	public static class LogReducer extends Reducer<Text, vv_Channel_statis_request, Text, Text> {

		public void reduce(Text key, Iterable<vv_Channel_statis_request> values, Context context)
				throws IOException, InterruptedException {
			java.util.Iterator<vv_Channel_statis_request> it = values.iterator();
			
			int pv = 0;
			vv_Channel_statis_request r = null;
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
			
			//System.out.println("path.getParent.Name:"+path.getParent().getName());
			String pathName = path.getName();
			//System.out.println("pathName:"+pathName);
			//String date1 = pathName.substring(0, 8);
			//System.out.println("date1:"+date1);
			
			if (pathName.contains(".bz2") || pathName.contains(".gz") ){
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
		Job job = jobHelper.newJob(conf, JobMapper.class, LogReducer.class,Text.class, Text.class);
		if(stime!=null && !stime.equals("") && etime!=null && !etime.equals("")){
			job.getConfiguration().set("stime", stime);
			job.getConfiguration().set("etime", etime);
		}
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(vv_Channel_statis_request.class);
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

		//VideoDataFactory vdf = VideoDataFactory.
		
		return jobHelper.runJob(job);
	}
}
