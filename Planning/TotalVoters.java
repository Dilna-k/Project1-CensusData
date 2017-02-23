package planningpack;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TotalVoters {
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		 
		private Configuration conf;

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			
			
				conf = context.getConfiguration();
				String x = conf.get("value");
				int i=Integer.parseInt(x);
				String[] str = value.toString().split(",");
				String part1=str[0];
				int age =Integer.parseInt(part1);
				
				int age1=age+i;
				
				if(age1>=18)
				{
					context.write(new Text("null"), new Text(age1+","+i));
				}
				
			
		}
	}
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int count = 0;int x=0;
			for (Text val : values) {
							
				String[] rec = val.toString().split(",");
				x=Integer.parseInt(rec[1]);
				count++;
				
			}
			context.write(new Text(""), new Text("Total no. of Voters after "+x+" years are "+count)); 
		}
	}
	

public static void main(String[] args) throws Exception {
	
	Configuration conf = new Configuration();
	
	
	@SuppressWarnings("resource")
	Scanner sc=new Scanner(System.in);
	System.out.println("Enter the value of x years");
	 int x;
	x=sc.nextInt();
	
	conf.setInt("value", x);
	Job job = Job.getInstance(conf);
	
	
	job.setJarByClass(TotalVoters.class);
	job.setJobName("Total Voters in x Years");
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
