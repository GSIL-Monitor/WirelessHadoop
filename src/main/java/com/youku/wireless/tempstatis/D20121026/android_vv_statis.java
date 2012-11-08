package com.youku.wireless.tempstatis.D20121026;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
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
@Program(name = "temp.statis.20121026", description = "临时统计:android平台vv中没传type=end的url， 区分统计type=begin的URL中play_types=(local|net)分别量。")
public class android_vv_statis {

	/**
	 * 这是一个field类型的惨呼声，参数名为pid，命令行可以如下：-pid 69b8,8352
	 */
	@ProgramParam(name = "hdfspid", description = "指定统计的pid文件。", required = false)
	private String hdfspid = null;

	public static class JobMapper extends
			Mapper<LongWritable, Text, Text, android_vv_statis_request> {

		private static List<String> pidlist = new ArrayList<String>();
		private static String hdfspid = null;
		
		@Override
		protected void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException {
			String valueString = value.toString();
			android_vv_statis_request r = parseRequest(valueString);
			if (r != null) {
				String pid = r.getPid();
				if (pidlist.size() > 0 && !pidlist.contains(pid)){
					return;
				}
				if(!r.getMethod().equals("GET") && !r.getMethod().equals("POST")){
					return;
				}
				
				if(!(r.getUri().contains("statis/vv") && r.getRequest_args().contains("type=begin"))){
					return;
				}
				
				if(r.getSessionid()==null || r.getSessionid().equals("")){
					return;
				}

				Text outKey = new Text();
				outKey.set(r.getSessionid());
				context.write(outKey, r);
			}
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			System.out.println("map setup:"+context.getConfiguration().get("hdfspid"));
			initPid_hdfs(context);
		}
		
		private final android_vv_statis_request parseRequest(String line) throws IOException,
				InterruptedException {
			android_vv_statis_request request = new android_vv_statis_request(line);
			String pid = request.getPid();
			if (request != null && pid != null) {
				return request;
			} else {
				return null;
			}
		}
		
		private void initPid_hdfs(Context context){
			String filename = context.getConfiguration().get("hdfspid");
			System.out.println("filename:"+filename);
			InputStream input  = null;  
	        try{  
	        	FileSystem fs = FileSystem.get(URI.create(filename), context.getConfiguration());  
	            input =  fs.open(new Path(filename)); 
	            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
	            String line = null;
	            while((line = reader.readLine()) != null){
	            	pidlist.add(line.trim());
	            }
	        }catch(IOException e){
	        	e.printStackTrace();
	        }finally{  
	            IOUtils.closeStream(input);  
	        }
	        
	        System.out.println("pidlist.size():"+pidlist.size());
		}

	}
			
	public static class LogReducer extends Reducer<Text, android_vv_statis_request, Text, Text> {

		public void reduce(Text key, Iterable<android_vv_statis_request> values, Context context)
				throws IOException, InterruptedException {
			java.util.Iterator<android_vv_statis_request> it = values.iterator();
			
			int vv = 0;
			android_vv_statis_request r = null;
			String play_types = "";
			String statisvv_type = "";
			boolean begin = false;
			boolean end = false;
			while (it.hasNext()) {
				r = it.next();
				vv += 1;
//				
//				statisvv_type = r.getStatisvv_type();
//				if(statisvv_type.equalsIgnoreCase("begin")){
//					begin = true;
//					play_types = r.getPlay_type();
//				}else if(statisvv_type.equalsIgnoreCase("end")){
//					end = true;
//				}else{
//					
//				}
				
			}
			Text outValue = new Text();
			outValue.set(String.valueOf(vv));
			context.write(key, outValue);
//			
//			if (begin && !end){
//				Text outValue = new Text();
//				outValue.set(play_types);
//				context.write(key, outValue);
//			}
			
			
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
		System.out.println("hdfspid:"+hdfspid);
		job.getConfiguration().set("hdfspid", hdfspid);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(android_vv_statis_request.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fstm = FileSystem.get(conf);
		Path outDir = new Path(outdir);
		fstm.delete(outDir, true);
		
		job.setInputFormatClass(SmartInputFormat.class);
		SmartInputFormat.setInputFiles(job, infiles);
		FileOutputFormat.setOutputPath(job, new Path(outdir));

		return jobHelper.runJob(job);
	}
}
