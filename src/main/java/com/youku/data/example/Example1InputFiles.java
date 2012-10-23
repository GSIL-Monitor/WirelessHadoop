package com.youku.data.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
@Program(name = "prog.example1", description = "例子1，输入多个广告access日志文件,统计每个分类下的广告容量")
public class Example1InputFiles {

	/**
	 * 这是一个field类型的惨呼声，参数名为startdate，命令行可以如下：-startdate 20120918
	 */
	@ProgramParam(name = "byday", description = "是否按照天来统计", required = false)
	private boolean byday = false;

	public static class JobMapper extends
			Mapper<LongWritable, AdAccessLogWritable, Text, LongWritable> {

		private static LongWritable ONE = new LongWritable(1l);
		private static Text keyout = new Text();
		private boolean byday =false;

		@Override
		protected void map(LongWritable key, AdAccessLogWritable value,
				Context context) throws IOException, InterruptedException {
			if(byday){
				keyout.set(value.getCat() + "_" + value.getDateStr());
			}else{
				keyout.set(value.getCat());
			}
			context.write(keyout, ONE);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			byday = context.getConfiguration().getBoolean("byday", false);
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
		
		Job job = jobHelper.newJob(conf, JobMapper.class, LongSumReducer.class,
				Text.class, LongWritable.class);
		job.getConfiguration().setBoolean("byday", byday);
		job.setInputFormatClass(SmartInputFormat.class);

		SmartInputFormat.setInputFiles(job, infiles);

		FileOutputFormat.setOutputPath(job, new Path(outdir));

		return jobHelper.runJob(job);
	}
}
