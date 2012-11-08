package com.youku.wireless.tempstatis.D20121102;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.youku.data.driver.MrJobHelper;
import com.youku.data.driver.annotation.Program;
import com.youku.data.mapreduce.input.SmartInputFormat;

@Program(name = "Channel.vv.Video", description = "video info cat!")
public class Video_Cat_Statis {

	public static class JobMapper extends
			Mapper<LongWritable, Text, TextPair, TextPair> {

		@Override
		protected void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException {
			String valueString = value.toString();
			Video_Cat_Statis_request r = parseRequest(valueString);
			if (r != null) {
				String vid = r.getVid();
				if(vid == null){
					return;
				}
				int flag = r.getFlag();
				
				if (flag == 1){
					if(!vid.matches("[0-9]+")){
						return;
					}
					TextPair tp = new TextPair(vid, "1");
					tp.setCid(r.getCid());
					tp.setFlag(flag);
					context.write(tp, tp);
				}else if(flag==2){
					TextPair tp = new TextPair(vid, "2");
					tp.setDate(r.getDate());
					tp.setProv(r.getProv());
					tp.setCity(r.getCity());
					tp.setCount(r.getCount());
					tp.setFlag(flag);
					context.write(tp, tp);
				}else{}
			}
		}

		private final Video_Cat_Statis_request parseRequest(String line) throws IOException,
				InterruptedException {
			Video_Cat_Statis_request request = new Video_Cat_Statis_request(line);
			if(request!=null && request.getVid()!=null && !request.getVid().equals("")){
				return request;
			}
			return null;
		}

	}
			
	public static class LogReducer extends Reducer<TextPair, TextPair, Text, Text> {
		private final Text key1 = new Text(); 

		public void reduce(TextPair key, Iterable<TextPair> values, Context context)
				throws IOException, InterruptedException {
			
			key1.set(key.getVid()); 
			java.util.Iterator<TextPair> it = values.iterator();

			String vid, date, prov, city, count;
			String cid = "null";
			int flag = 0;
			TextPair r = null;
			
			while (it.hasNext()) {
				r = it.next();
				
				vid = r.getVid();
				flag = r.getFlag();
				
				if (flag == 1){
					cid = r.getCid();
				}
				if (flag == 2){
					date = r.getDate();
					prov = r.getProv();
					city = r.getCity();
					count = r.getCount();
					
					Text outValue = new Text();
					outValue.set(cid + "\t" + prov + "\t" + city + "\t" + date + "\t" + count);
					context.write(key1, outValue);
						
				}

			}
			
		}
	}
	
	
	public static class KeyPartitioner extends Partitioner<TextPair, TextPair>{

		public void configure(JobConf arg0) {
			
		}

		public int getPartition(TextPair key, TextPair value, int numPartitions) {
			return (key.getVid().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
		
	}
	
	public static class GroupingComparator extends WritableComparator {

		protected GroupingComparator() {  
			super(TextPair.class, true);  
		}  
		
		@Override  
		public int compare(WritableComparable w1, WritableComparable w2) {  
			TextPair ip1 = (TextPair) w1;  
			TextPair ip2 = (TextPair) w2;  
			String l = ip1.getVid();  
			String r = ip2.getVid();
			return l.equals(r) ? 0 : (l.hashCode() < r.hashCode() ? -1 : 1);  
		}
		
	}
	
	
	public boolean run(Configuration conf, MrJobHelper jobHelper,
			String[] infiles, String outdir) throws IOException {
		Job job = jobHelper.newJob(conf, JobMapper.class, LogReducer.class,Text.class, Text.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(TextPair.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setPartitionerClass(KeyPartitioner.class);

		FileSystem fstm = FileSystem.get(conf);
		Path outDir = new Path(outdir);
		fstm.delete(outDir, true);
		
		job.setInputFormatClass(SmartInputFormat.class);
		SmartInputFormat.setInputFiles(job, infiles);
		FileOutputFormat.setOutputPath(job, new Path(outdir));
		
		return jobHelper.runJob(job);
	}
}
