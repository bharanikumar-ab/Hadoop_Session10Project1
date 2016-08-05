package com.bigdata.main;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

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

public class CasesByDateRange {
	private static Logger logger = Logger.getLogger(CasesByDateRange.class);
	private static Long START_DATE = new GregorianCalendar(2014,Calendar.OCTOBER,1).getTimeInMillis();
	private static Long END_DATE = new GregorianCalendar(2015,Calendar.OCTOBER,31).getTimeInMillis();
	public static class CasesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text dateText = new Text();
		SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String record = value.toString();
			
			String[] columnValues = record.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1);
			if(columnValues.length>15){
				try {
					String dateStr = columnValues[2];
					Date date = sdf.parse(dateStr);
					Long dateInMisslis = date.getTime();
					if(dateInMisslis>START_DATE && dateInMisslis<END_DATE){
						dateText.set("inRange");
						context.write(dateText,one);
					}
					
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
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
		Job job = new Job(config,"CasesByDateRange");
		job.setJarByClass(CasesByDateRange.class);
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
