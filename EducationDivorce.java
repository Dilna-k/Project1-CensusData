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

import CensusPack.EductnTaxFilers.MapClass;
import CensusPack.EductnTaxFilers.ReduceClass;

public class EducationDivorce {
public static class MapClass extends Mapper <LongWritable,Text,Text,Text>{
		
		public void map(LongWritable Key,Text value,Context context)throws IOException, InterruptedException
		{
			String record = value.toString();
			String[] parts = record.split(",");
			
			if(parts[2].contains(" Divorced"))
			{
				context.write(new Text(parts[2]),new Text(parts[1]));
			}
			
		}
	}



public static class ReduceClass extends Reducer<Text, Text, Text, Text>	
{
	public void reduce (Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException 
	{
		int edu1=0,edu2=0,edu3=0,edu4=0,edu5=0,edu6=0,edu7=0,edu8=0,edu9=0,edu10=0,edu11=0,edu12=0;
		int ledu=0;
		float total=0;
		 for (Text val : values) 
		 {
			total++;
			 String str=val.toString();
			 String[] p=str.split(",");
			 if(p[0].contains("10th grade"))
			 {
				 edu1++;
			 
			 }else if(p[0].contains(" 11th grade"))
			{
				 edu2++;
			 }
			 else if(p[0].contains(" 1st 2nd 3rd or 4th grade"))
						{
				 edu3++;
						 }
			 else if(p[0].contains(" 5th or 6th grade"))
				{
		 edu4++;
				 }
			 else if(p[0].contains(" 7th and 8th grade"))
				{
		 edu5++;
				 }
			 else if(p[0].contains(" 9th grade"))
				{
		 edu6++;
				 } 
			 else if(p[0].contains(" Associates degree-academic program"))
				{
		 edu7++;
				 }
			 else if(p[0].contains(" Bachelors degree(BA AB BS)"))
				{
		 edu8++;
				 }
			 else if(p[0].contains(" Doctorate degree(PhD EdD)"))
				{
		 edu9++;
				 } 
			 else if(p[0].contains(" High school graduate"))
				{
		 edu10++;
				 } 
			 else if(p[0].contains(" Masters degree(MA MS MEng MEd MSW MBA)"))
				{
		 edu11++;
				 } 
			 else if(p[0].contains(" Some college but no degree"))
				{
		 edu12++;
				 } 
			 
		 }

		 String s1="Divorce have 10th grade  "+(edu1/total)*100;

		 String s2="Divorce have 11th grade  "+(edu2/total)*100;

		 String s3="Divorce have 1st 2nd 3rd or 4th grade  "+(edu3/total)*100;

		 String s4="Divorce have 5th or 6th grade  "+(edu4/total)*100;

		 String s5="Divorce have 7th and 8th grade  "+(edu5/total)*100;

		 String s6="Divorce have 9th grade  "+(edu6/total)*100;
		 
		 String s7="Divorce have Associates degree-academic program  "+(edu7/total)*100;
		 
		 String s8="Divorce have Bachelors degree(BA AB BS)  "+(edu8/total)*100;
		 
		 String s9="Divorce have Doctorate degree(PhD EdD)  "+(edu9/total)*100;
		 
		 String s10="Divorce have High school graduate  "+(edu10/total)*100;
		 
		 String s11="Divorce have Masters degree(MA MS MEng MEd MSW MBA)  "+(edu11/total)*100;

		 String s12="Divorce have Some college but no degree  "+(edu12/total)*100;
		 
		 context.write(key, new Text (s1));
		 context.write(key, new Text (s2));
		 context.write(key, new Text (s3));
		 context.write(key, new Text (s4));
		 context.write(key, new Text (s5));
		 context.write(key, new Text (s6));
		 context.write(key, new Text (s7));
		 context.write(key, new Text (s8));
		 context.write(key, new Text (s9));
		 context.write(key, new Text (s10));
		 context.write(key, new Text (s11));
		 context.write(key, new Text (s12));
	}
}
public static void  main (String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(EducationDivorce.class);
    job.setJobName("EDUCATION VS DIVORCE ");
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
