package p2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q2 {
	
	//Mapper Class
	public static class MapForHotOrColdDay extends Mapper<LongWritable, Text, Text, FloatWritable>{
		
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			
				String line = value.toString();
				String day = line.substring(6, 14).replaceAll(" ", "");
				String maxTemp = line.substring(38, 45).replaceAll(" ", "");
				String minTemp = line.substring(46, 53).replaceAll(" ", "");
				Float maxTempDouble = Float.parseFloat(maxTemp);
				Float minTempDouble = Float.parseFloat(minTemp);
				FloatWritable minTempValue = new FloatWritable(minTempDouble);
				FloatWritable maxTempValue = new FloatWritable(maxTempDouble);
				
				if(minTempDouble < 10){
					con.write(new Text("Cold Day " + day), minTempValue);
				}
				if(maxTempDouble > 35){
					con.write(new Text("Hot Day " + day), maxTempValue);
				}
				
		}
	}
	
	//Reducer Class
	public static class ReduceForHotOrColdDay extends Reducer<Text, FloatWritable, Text, FloatWritable>{
				
		public void reduce(Text word, Iterable<FloatWritable> values, Context con) throws IOException, InterruptedException{
			
			float temp = 0;
			for (FloatWritable value: values){
					temp = value.get();
				
			}
			con.write(new Text(word), new FloatWritable(temp));
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration c= new Configuration();
		Job j = Job.getInstance(c, "hotorcoldDay:");
		j.setJarByClass(Q2.class);
		j.setMapperClass(MapForHotOrColdDay.class);
		j.setReducerClass(ReduceForHotOrColdDay.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true)?0:1);
	}

}
