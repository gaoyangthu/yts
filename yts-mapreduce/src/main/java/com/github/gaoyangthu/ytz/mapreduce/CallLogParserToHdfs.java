package com.github.gaoyangthu.ytz.mapreduce;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2015/1/14 0014
 */
public class CallLogParserToHdfs {

	public static class MapClass extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] items = value.toString().split("\\|", -1);
			String phone = items[1];
			String content = items[6];

			try {
				long p = Long.parseLong(phone);
				if (p > 13000000000L && p < 19000000000L) {
					try {
						JSONTokener jt = new JSONTokener(content);
						if (jt.more()) {
							JSONObject contentObject = (JSONObject) jt.nextValue();
							if (contentObject.has("dataObject")) {
								if (contentObject.get("dataObject") instanceof JSONArray) {
									JSONArray dataObject = contentObject.getJSONArray("dataObject");
									for (int i = 0; i < dataObject.length(); i++) {
										if (dataObject.getJSONObject(i) != null) {
											JSONObject callInfo = dataObject.getJSONObject(i);
											if (callInfo.has("callMobile") && callInfo.has("callTime")) {
												String callMobile = callInfo.getString("callMobile");
												String callTime = callInfo.getString("callTime");
												try {
													long m = Long.parseLong(callMobile);
													if (m > 13000000000L && m < 19000000000L) {
														Text k = new Text();
														if (p < m) {
															k.set(phone + callMobile);
														} else {
															k.set(callMobile + phone);
														}
														Text v = new Text();
														v.set(callTime);
														context.write(k, v);
													}
												} catch (NumberFormatException e) {
													// if call mobile number is not a mobile phone number, skip this record
												}
											}
										}
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

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<String> set = new HashSet<String>();
			for (Text value : values) {
				set.add(value.toString());
			}
			int frequency = set.size();

			Text v = new Text();
			v.set(String.valueOf(frequency));
			context.write(key, v);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CallLogParserToHdfs <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "cost log parser");
		job.setJarByClass(CostLogParser.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
