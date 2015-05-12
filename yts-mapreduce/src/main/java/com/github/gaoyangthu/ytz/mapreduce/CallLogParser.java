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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
public class CallLogParser {
	public static final String QUORUM = "master004,master005,master006";
	public static final String PORT = "2181";
	public static final String PARENT = "/hbase_wins";
	public static final String TABLE = "yts_calling";
	public static final String FAMILY = "f:";

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

	public static class ReduceClass extends TableReducer<Text, Text, ImmutableBytesWritable> {
		private byte[] family = null;
		private byte[] qualifier = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			String column = context.getConfiguration().get("conf.column");
			byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
			family = colkey[0];
			if (colkey.length > 1) {
				qualifier = colkey[1];
			}
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<String> set = new HashSet<String>();
			for (Text value : values) {
				set.add(value.toString());
			}
			int frequency = set.size();

			byte[] rowkey = DigestUtils.md5(key.toString());
			Put put = new Put(rowkey);
			if (frequency < 256) {
				put.add(family, qualifier, Bytes.toBytes((byte)frequency));
			} else {
				put.add(family, qualifier, Bytes.toBytes((short)frequency));
			}
			context.write(new ImmutableBytesWritable(rowkey), put);
		}
	}

	public static void main(String[] args) throws Exception {
		/**
		 * Set a job and its configurations
		 */
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: CallLogParser <in>");
			System.exit(1);
		}

		/**
		 * Set HBase parameters
		 */
		conf.set("hbase.zookeeper.quorum", QUORUM);
		conf.set("hbase.zookeeper.property.clientPort", PORT);
		conf.set("zookeeper.znode.parent", PARENT);
		conf.set("conf.column", FAMILY);
		Job job = new Job(conf, "call log parser");
		job.setJarByClass(CallLogParser.class);

		/**
		 * set the class of mapper
		 */
		job.setMapperClass(MapClass.class);
		//job.setReducerClass(ReduceClass.class);

		/**
		 * Set output class of mapper and reducer
		 */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE);

		/**
		 * Set input and output files path of the job
		 */
		TableMapReduceUtil.initTableReducerJob(TABLE, ReduceClass.class, job);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		//FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
