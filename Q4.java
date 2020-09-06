package p2;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4 {
	// Mapper Class
	public static class MapForMaximumAndMinimum extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context con)
				throws IOException, InterruptedException {

			String line = value.toString();
			String date = line.substring(6, 14).replaceAll(" ", "");
			String highTemp = line.substring(38, 45).replaceAll(" ", "");
			String coldTemp = line.substring(46, 53).replaceAll(" ", "");

			con.write(new Text(date), new Text(highTemp + ":" + coldTemp));

		}
	}

	// Reducer Class
	public static class ReduceForMaximumAndMinimum extends Reducer<Text, Text, Text, Text> {

		TreeMap<Text, Text> map = new TreeMap<Text, Text>();

		public void reduce(Text text, Iterable<Text> values, Context con)
				throws IOException, InterruptedException {

			for (Text value : values) {
				map.put(new Text(text), new Text(value));
			}
		}

		public void cleanup(Context con) throws IOException,
				InterruptedException {
			con.write(new Text("Date"), new Text(
					"Max Temperature  and	Min Temperature"));

			for (Map.Entry<Text, Text> entry : map.entrySet()) {
				String[] temp = entry.getValue().toString().split(":");

				con.write(new Text(entry.getKey()), new Text(temp[0] + "	"
						+ temp[1]));
			}
		}
	}
	
	// Main Method for execution
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			Configuration c= new Configuration();
			Job j = Job.getInstance(c, "Question 4:");
			j.setJarByClass(Q4.class);
			j.setMapperClass(MapForMaximumAndMinimum.class);
			j.setReducerClass(ReduceForMaximumAndMinimum.class);
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(j, new Path(args[0]));
			FileOutputFormat.setOutputPath(j, new Path(args[1]));
			System.exit(j.waitForCompletion(true)?0:1);

		}

}