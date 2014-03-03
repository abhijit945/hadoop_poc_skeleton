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

public class RatedN {
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text userId = new Text();
		private static final IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			String[] str = line.split("::");
			userId.set(str[0]);
			context.write(userId, one);
		}
	}
	static int minRatingsCount = 0;
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, Text> {

		Text blank = new Text();
		public void setup(Context context) {
			minRatingsCount = Integer.parseInt(context.getConfiguration().get("parameter"));
		}

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			// Get the list value and the key and increment the counter for the
			// number of movies in the list

			for (IntWritable val : values) {
				sum += val.get();
			}
			blank.set("");
			if (sum >= minRatingsCount) {
//				context.write(key, new IntWritable(sum));
				context.write(key, blank);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		
		try {
			if (args.length > 2) {
				conf.set("parameter", args[2]);
				if(!(Integer.parseInt(args[2]) > 0)){
					System.out.println("Please enter a non negative count");
					System.exit(-1);
				}
				System.out.println("Input is "+conf.get("parameter"));
			}
			else
			{
				System.out.println("Please enter count for number of ratings");
				System.exit(-1);
			}
		} catch (NumberFormatException e) {
			System.out.println("---Illegal input for minimum ratings count---"
					+ e.getMessage());
			System.exit(-1);
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("Please enter count for number of ratings"
					+ e.getMessage());
			System.exit(-1);
		}
		
		Job job = new Job(conf, "ratedn");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setJarByClass(RatedN.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}