package com.github.gaoyangthu.ytz.mapreduce;

import com.github.gaoyangthu.yts.util.HashUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2015/1/20 0020
 */
public class CostLogParser {

	public static class MapClass extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] items = value.toString().split("\\|", -1);
			String phone = items[1];
			double charge;
			String content = items[6];

			try {
				long p = Long.parseLong(phone);
				if (p > 13000000000L && p < 19000000000L) {
					try {
						JSONTokener jt = new JSONTokener(content);
						if (jt.more()) {
							JSONObject contentObject = (JSONObject) jt.nextValue();
							if (contentObject.has("dataObject")) {
								if (contentObject.get("dataObject") instanceof JSONObject) {
									JSONObject dataObject = contentObject.getJSONObject("dataObject");
									if (dataObject.has("chargeAll")) {
										charge = dataObject.getDouble("chargeAll");
										int cost = 0;
										if (charge > 200.0) {
											cost = 2;
										} else if (charge > 50.0) {
											cost = 1;
										}

										Text k = new Text();
										Text v = new Text();
										k.set(HashUtils.encryptPhone(phone));
										v.set(String.valueOf(cost));
										context.write(k, v);
									}
								}
							}
						}
					} catch (JSONException e) {
						// skip this log
					}
				}
			} catch (NumberFormatException e) {
				// if main phone number is not a mobile phone number, skip this record
			}
		}
	}

	public static void main(String[] args) throws Exception {
		/**
		 * Set a job and its configurations
		 */
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CostLogParser <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "cost log parser");
		job.setJarByClass(CostLogParser.class);

		/**
		 * set the class of mapper and reducer
		 */
		job.setMapperClass(MapClass.class);
		job.setNumReduceTasks(0);

		/**
		 * Set output class of mapper and reducer
		 */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		/**
		 * Set input and output files path of the job
		 */
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
