package com.bigdata.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

public class TheftCasesDistrictWise {
	 private static Logger logger = Logger.getLogger(TheftCasesDistrictWise.class);
	 private static String CASE_TYPE_THEFT = "THEFT";
	public static class CasesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text district = new Text();
		int rowCount = 1;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String record = value.toString();
			String[] columnValues = record.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1);
			if(columnValues.length>15){
				String caseType = columnValues[5];
				if(CASE_TYPE_THEFT.equals(caseType)){
					logger.info("caseType:"+columnValues[5]+" columnValues[11]:"+columnValues[11]);
					district.set(columnValues[11]);
					context.write(district,one);
				}
			}
		}
	}
	
	public static class CaseReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text fbiCode,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int totalCases = 0;
			for (IntWritable val : values) {
				totalCases = totalCases + val.get();
			}
			context.write(fbiCode, new IntWritable(totalCases));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		Job job = new Job(config,"TheftCasesDistrictWise");
		job.setJarByClass(TheftCasesDistrictWise.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(CasesMapper.class);
		job.setCombinerClass(CaseReducer.class);
		job.setReducerClass(CaseReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new
				Path(args[1]));
		job.waitForCompletion(true);
	}

}
