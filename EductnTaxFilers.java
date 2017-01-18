package CensusPack;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EductnTaxFilers {
	
	public static class MapClass extends Mapper <LongWritable,Text,Text,Text>{
		
		public void map(LongWritable Key,Text value,Context context)throws IOException, InterruptedException
		{
			String record = value.toString();
			String[] parts = record.split(",");
			
			if(parts[4].contains("Nonfiler"))
			{
				context.write(new Text(parts[4]),new Text(parts[1]));
			}
			
		}
	}
	
	
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>	
	{
		public void reduce (Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			int hedu=0;
			int ledu=0;
			float total;
			 for (Text val : values) 
			 {
				
				 String str=val.toString();
				 String[] p=str.split(",");
				 if((p[0].contains("Associates degree-academic program")||(p[0].contains("Bachelors degree(BA AB BS)")||(p[0].contains("Doctorate degree(PhD EdD)")||(p[0].contains("Masters degree(MA MS MEng MEd MSW MBA)")||(p[0].contains("Prof school degree (MD DDS DVM LLB JD)")))))))
				 {
					 hedu++;
				 
				 }else if((p[0].contains(" Some college but no degree")||(p[0].contains(" Less than 1st grade")||(p[0].contains(" High school graduate")||(p[0].contains("Associates degree-occup /vocational")||(p[0].contains("7th and 8th grade"))||(p[0].contains("10th grade"))||(p[0].contains("5th or 6th grade"))||(p[0].contains("9th grade")))))))
						
				 {
					 ledu++;
				 }
				 
			 }
			 total=hedu+ledu;
			 String heduNontax="Percentage of Highly educated Non Tax Filers  "+(hedu/total)*100;
			 String leduNontax="Percentage of Less educated Non Tax Filers  "+(ledu/total)*100;
			 context.write(key, new Text (heduNontax));
			 context.write(key, new Text (leduNontax));
		}
	}

	
	public static void  main (String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf);
	    job.setJarByClass(EductnTaxFilers.class);
	    job.setJobName("EDUCATION VS NON TAXPAYER ");
	    job.setMapperClass(MapClass.class);
	    job.setReducerClass(ReduceClass.class);
	    job.setNumReduceTasks(1);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);


	}
}
