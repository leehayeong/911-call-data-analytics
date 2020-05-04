package mju.hadoop.call911;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Call911 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text type = new Text();
		private Text time = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// .csv 파일 배열에 저장 
			String[] colums = value.toString().split(",");
			StringTokenizer typeTokenizer;
			StringTokenizer timeTokenizer;
			String t_type, t_time;

			// time 저장
			timeTokenizer = new StringTokenizer(colums[3], "-");
			t_time = timeTokenizer.nextToken();
			t_time += "-";
			t_time += timeTokenizer.nextToken();
			time.set(t_time);

			// type 저장
			typeTokenizer = new StringTokenizer(colums[2], ":");
			t_type = typeTokenizer.nextToken();
			type.set(t_type);

			context.write(time, type);          

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text value = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String type = null;
			type = conf.get("type");
			String result = "";

			// 입력값 없을 경우 
			if(type == null) {
				int ems_count = 0;
				int traffic_count = 0;
				int fire_count = 0;

				for(Text value : values) {    	   		
					if(value.compareTo(new Text("EMS")) == 0) ems_count++;
					else if(value.compareTo(new Text("Traffic")) == 0) traffic_count++;
					else if(value.compareTo(new Text ("Fire")) == 0) fire_count++;   	   					
				}
				int total = ems_count + traffic_count + fire_count;

				// EMS 비율 구하기 
				result += "   EMS: ";
				result += String.format("%.1f", ((double)ems_count/total)*100) + "%   ";

				// Traffic 비율 구하기 
				result += "Traffic: ";
				result += String.format("%.1f", ((double)traffic_count/total)*100) + "%   ";

				// Fire 비율 구하기
				result += "Fire: ";
				result += String.format("%.1f", ((double)fire_count/total)*100) + "%";

			} 
			
			// 입력값 있을 경우 
			else {
				int count = 0;
				int total = 0;
				for(Text value : values) {    	   		
					if(value.compareTo(new Text(type)) == 0){ count++; }  
					total++;
				}

				result = "   " + type + ": " + String.format("%.1f", ((double)count/total)*100) + "%";
			}
			
			// 리듀스 종료 
			value.set(result);
			context.write(key, value);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		if(args.length == 3) {
			conf.set("type",args[2]);
		}

		Job job = new Job(conf, "call911");
		job.setJarByClass(Call911.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);

		FileInputFormat .setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean success = job.waitForCompletion(true);
		System.out.println(success);
	}
}