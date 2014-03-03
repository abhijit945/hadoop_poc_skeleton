import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AvgAge {
	// TODO 1 Calculate average age of all the users by Zip code
	// TODO 2 Get the Zip codes of 10 descending average ages
	// Output is in the format - Zipcode ----- Average Age

	public static class Map extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		DoubleWritable ageVal = new DoubleWritable();
		Text zipVal = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] str = line.split("::");

			zipVal.set(str[4].trim());
			ageVal.set(Double.parseDouble(str[2]));

			// Send Zip code and Age to Reducer to reduce
			context.write(zipVal, ageVal);
		}
	}

	public static class Reduce extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		DoubleWritable avgVal = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double avg = 0.000;
			int sum = 0, count = 0;
			for (DoubleWritable individualAge : values) {
				sum += individualAge.get();
				count++;
			}
			avg = (double) sum / count;
			// Now we have ZipCode to Avg Age output
			avgVal.set(avg);
			context.write(key, avgVal);
		}
	}

	// Now that we have obtained the Avg Age for each zip code

	// TODO 1. Get the Average ages and the Zip codes and pass each of them to
	// the Reducer

	// TODO 2. Use Reducer to get the Decreasing order of the items and select
	// 10 of them

	public static class TopMap extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		TreeMap<Text, DoubleWritable> map = new TreeMap<Text, DoubleWritable>();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] str = line.split("\t");
			
			Text zipVal = new Text();
			DoubleWritable ageVal = new DoubleWritable();
			
			zipVal.set(str[0].trim());
			ageVal.set(Double.parseDouble(str[1]));

			// Send Zip code and Age to Reducer to reduce
			map.put(zipVal, ageVal);
		}

		@SuppressWarnings("unchecked")
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			int i = 0;
			for (Entry<Text, DoubleWritable> entry : ((TreeMap<Text, DoubleWritable>)sortByValues(map)).entrySet()) {
				// Sending only top 10 Zip and AvgAges
				if(i>9){
					break;
				}
				context.write(entry.getKey(), entry.getValue());
				i++;
			}
		}

		@SuppressWarnings("hiding")
		public static <Text, DoubleWritable extends Comparable<DoubleWritable>> TreeMap<Text, DoubleWritable> sortByValues(final TreeMap<Text, DoubleWritable> map) {
			
			Comparator<Text> valueComparator =  new Comparator<Text>() {
			    public int compare(Text k1, Text k2) {
			        int compare = map.get(k1).compareTo(map.get(k2));
			        if (compare == 0) return 1;
			        else return compare;
			    }
			};
			TreeMap<Text, DoubleWritable> sortedByValues = new TreeMap<Text, DoubleWritable>(valueComparator);
			sortedByValues.putAll(map);
			return sortedByValues;
		}
	}

	// Sort -> Reverse -> Output Top 10

	public static class TopReduce extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		TreeMap<Text, DoubleWritable> map = new TreeMap<Text, DoubleWritable>();

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			for (DoubleWritable zipValue : values) {
				context.write(key, zipValue);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "avgage");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setJarByClass(AvgAge.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if (job.waitForCompletion(true)) {
			// If on "avgage" job complete proceed to getting the descending
			// order and
			// finding Top 10 Zipcodes

			Configuration chainConf = new Configuration();
			Job postjob = new Job(chainConf, "gettop10zip");

			postjob.setOutputKeyClass(Text.class);
			postjob.setOutputValueClass(DoubleWritable.class);
			postjob.setJarByClass(AvgAge.class);
			postjob.setMapperClass(TopMap.class);
			postjob.setReducerClass(TopReduce.class);
			postjob.setInputFormatClass(TextInputFormat.class);
			postjob.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(postjob, new Path(args[1]));
			FileOutputFormat.setOutputPath(postjob, new Path(args[2]));

			postjob.waitForCompletion(true);
		}
	}

}