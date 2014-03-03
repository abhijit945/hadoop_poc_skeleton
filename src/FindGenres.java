import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FindGenres {

	static HashSet<String> movieList = new HashSet<String>();

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text genre = new Text();
		private Text one = new Text("1");

		// Setup method for Mapper

		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			int count = 0;
			count = Integer.parseInt(config.get("count"));
			for (int i = 1; i <= count; i++) {
				movieList.add(config.get("parameter" + i));
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			String[] str = line.split("::");

			for (String s : movieList) {
				if (str[1].equals(s) && str[1].length() == s.length()) {
					String[] arr = str[2].split("\\|");
					for (String s1 : arr) {
						genre.set(s1);
						context.write(one, genre);
					}
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Text blank = new Text("");
		private Text genre = new Text();
		HashSet<String> cutomGenreSet = new HashSet<String>();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				cutomGenreSet.add(val.toString());
			}
			genre.set(cutomGenreSet.toString());
			context.write(genre, blank);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		if (args.length > 2) {
			int i, count = 0;
			for (i = 2; i < args.length; i++) {
				count++;
				conf.set("parameter" + count, args[i]);
				// movieList.add(args[i]);
				System.out.println("Movies added " + args[i]);
			}
			conf.set("count", count + "");
		} else {
			System.out.println("Please enter a valid movie name");
			System.exit(-1);
		}

		Job job = new Job(conf, "findgenres");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(FindGenres.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}