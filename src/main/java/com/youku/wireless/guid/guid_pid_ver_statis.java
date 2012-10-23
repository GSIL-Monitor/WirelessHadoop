package com.youku.wireless.guid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
@Program(name = "statis.guid.pid.ver", description = "无线设备库guid。")
public class guid_pid_ver_statis {

	/**
	 * 这是一个field类型的惨呼声，参数名为pid，命令行可以如下：-pid 69b8,8352
	 */
	@ProgramParam(name = "pid", description = "指定统计的pid,多个pid以\",\"号分割", required = false)
	private String pid = null;

	public static class JobMapper extends
			Mapper<LongWritable, Text, Text, guid_statis_request> {

		//private static LongWritable ONE = new LongWritable(1l);
		//private static Text keyout = new Text();
		private static List<String> pidlist = new ArrayList<String>();
		private static IpArea iparea = null;
		private static ProvinceCityName provcityname = null;
		
		
		
		
		@Override
		protected void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException {
			String valueString = value.toString();
			guid_statis_request r = parseRequest(valueString);
			if (r != null) {
				String pid = r.getPid();
				if (pidlist.size() > 0 && !pidlist.contains(pid)){
					return;
				}
				if(!r.getMethod().equals("GET") && !r.getMethod().equals("POST")){
					return;
				}
				if(!r.getUri().contains("statis/vv") || !r.getRequest_args().contains("type=begin")){
					return;
				}
				String prov = null, city = null;
				String ip = r.getIp();
				try{
					if(ip!=null && !ip.trim().equals("")){
						Region region = iparea.getRegion(ip);
						prov = provcityname.getProvinceName(region.getProvince_id());
						city = provcityname.getCityName(region.getCity_id());
					}
				}catch(Exception e){
					
				}
				Text outKey = new Text();
				outKey.set(pid+"-"+prov+"-"+city);
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
			//provcity.getCityName(ips.parseInt(""));
		}
		
		private final guid_statis_request parseRequest(String line) throws IOException,
				InterruptedException {
			guid_statis_request request = new guid_statis_request(line);
			String pid = request.getPid();
			if (request != null && pid != null) {
				return request;
			} else {
				return null;
			}
		}

	}
			
	public static class LogReducer extends Reducer<Text, guid_statis_request, Text, Text> {

		public void reduce(Text key, Iterable<guid_statis_request> values, Context context)
				throws IOException, InterruptedException {
			java.util.Iterator<guid_statis_request> it = values.iterator();
			
			int pv = 0;
			guid_statis_request r = null;
			while (it.hasNext()) {
				r = it.next();
				
				pv += 1;
				
			}
			
			Text outValue = new Text();
			outValue.set(Integer.toString(pv));
			context.write(key, outValue);
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
		//IpArea ips = IpDataFactory.makeIpArea(conf);
		//ProvinceCityName provcity = IpDataFactory.makeProvinceCityName(conf);
		//provcity.getCityName(ips.parseInt(""));
		Job job = jobHelper.newJob(conf, JobMapper.class, LogReducer.class,
				Text.class, Text.class);
		job.getConfiguration().set("pid", pid);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(guid_statis_request.class);
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
