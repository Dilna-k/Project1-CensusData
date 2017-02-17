package socialPack;

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

public class PensionAnalysis {
	
	  

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		 
		private Configuration conf;

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			
			
				conf = context.getConfiguration();
				String v = conf.get("value");
				int i=Integer.parseInt(v);
				String[] str = value.toString().split(",");
				String part1=str[0];
				String[] p1=part1.split(" ");
				
				int age =Integer.parseInt(p1[1]);
				int age1=age+i;
				if(age1>=60)
				{
				context.write(new Text("null"), new Text(str[0]+","+age1+","+i));
				}
				
			
		}
	}
		
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int pcount = 0;int nopen=0;int xvalue=0;int age=0;int sum=0;
			String age1=null;
			String s1=null;
			int p1=1000,p2=2000,p3=3000,p4=4000,p5=5000;
			int count1=0,count2=0,count3=0,count4=0,count5=0;
			for (Text val : values) {
				String[] rec = val.toString().split(",");
				
				s1=rec[0];
				
				age1=rec[1];
				//new age
				age=Integer.parseInt(age1);
				
				String xvalue1=rec[2];
				xvalue=Integer.parseInt(xvalue1);
					
				pcount++;
				if((age>=60)&&(age<=70)){
					count1++;
									
				}
				else if((age>=71)&&(age<=80))
				{
					count2++;
				}
				else if((age>=81)&&(age<=90))
				{
					count3++;
				}
				else if((age>=91)&&(age<=100))
				{
					count4++;
				}
				else if(age>100)
				{
					count5++;
				}

			}
		
			String PensionAva1 = " Pension Amount after "+xvalue+" years for 60 to 70 age :- "+p1+"  No.of people "+count1+" Total pension amount "+(count1*p1);
			String PensionAva2 = " Pension Amount after "+xvalue+" years for 71 to 80 age :- "+p2+"  No.of people "+count2+" Total pension amount "+(count2*p2);
			String PensionAva3 = " Pension Amount after "+xvalue+" years for 81 to 90 age :- "+p3+"  No.of people "+count3+" Total pension amount "+(count3*p3);
			String PensionAva4 = " Pension Amount after "+xvalue+" years for 91 to 100 age :- "+p4+"  No.of people "+count4+" Total pension amount "+(count4*p4);
			String PensionAva5 = " Pension Amount after "+xvalue+" years for above 100 age :- "+p5+"  No.of people "+count5+" Total pension amount "+(count5*p5);
			
			context.write(null, new Text(PensionAva1));
			context.write(null, new Text(PensionAva2));
			context.write(null, new Text(PensionAva3));
			context.write(null, new Text(PensionAva4));
			context.write(null, new Text(PensionAva5));

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
	
	
	job.setJarByClass(PensionAnalysis.class);
	job.setJobName("Pension amount after x years.");
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
