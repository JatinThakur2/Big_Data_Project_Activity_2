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

public class Q1 {
	
	//Mapper Class
	public static class MapForHighestTemperatureForEachyear extends Mapper<LongWritable, Text, Text, FloatWritable>{
		
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			
				String line = value.toString();
				//String[] words = line.split(" ");	
				String year = line.substring(6, 10).replaceAll(" ", "");
				Text outputkey = new Text(year);
				
				String maxTemp = line.substring(38, 45).replaceAll(" ", "");
				Float maxTempDouble = Float.parseFloat(maxTemp);
				FloatWritable outputvalue = new FloatWritable(maxTempDouble);
				con.write(outputkey, outputvalue);
		}
	}
	
	//Reducer Class
	public static class ReduceForHighestTemperatureForEachyear extends Reducer<Text, FloatWritable, Text, FloatWritable>{
				
		public void reduce(Text word, Iterable<FloatWritable> values, Context con) throws IOException, InterruptedException{
			
			float max_temp = 0;
			for (FloatWritable value: values){
				if(value.get() > max_temp){
					max_temp = value.get();
				}
			}
			
			con.write(new Text("Highest Temperature for the Year " + word + " is "), new FloatWritable(max_temp));
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration c= new Configuration();
		Job j = Job.getInstance(c, "highestTemperature:");
		j.setJarByClass(Q1.class);
		j.setMapperClass(MapForHighestTemperatureForEachyear.class);
		j.setReducerClass(ReduceForHighestTemperatureForEachyear.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true)?0:1);
	}

}
