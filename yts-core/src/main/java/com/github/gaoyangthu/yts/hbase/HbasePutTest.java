package com.github.gaoyangthu.yts.hbase;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2015/1/16 0016
 */
public class HbasePutTest {
	public static final String QUORUM = "master004,master005,master006";
	public static final String PORT = "2181";
	public static final String PARENT = "/hbase_wins";
	public static final String TABLE = "yts_calling_summary";
	public static final String QUALIFIER = "";
	public static Configuration configuration;

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", QUORUM);
		configuration.set("hbase.zookeeper.property.clientPort", PORT);
		configuration.set("zookeeper.znode.parent", PARENT);
		configuration.set("hbase.client.retries.number", "3");
		configuration.set("hbase.rpc.timeout", "5000");
	}

	/**
	 * 插入数据
	 *
	 * @param mainPhone
	 *            第一个电话号码 11位Long类型
	 * @param coupledPhone
	 *            第二个电话号码 11位Long类型
	 * @param frequency
	 *            保存两个号码当前账期的通话频率的整型值
	 * @param total
	 *            保存两个号码所有账期的通话频率的整型值
	 * @throws IOException
	 */
	public static void hbaseTestDataInsert(HTable table, Long mainPhone, Long coupledPhone, int frequency, int total)
		throws IOException {
		String combine = null;
		if (mainPhone < coupledPhone) {
			combine = String.valueOf(mainPhone) + String.valueOf(coupledPhone);
		} else {
			combine = String.valueOf(coupledPhone) + String.valueOf(mainPhone);
		}

		byte[] rowKey = DigestUtils.md5(combine);
		Put put = new Put(rowKey);
		byte[] f = new byte[1];
		f[0] = (byte)frequency;
		byte[] t = new byte[1];
		t[0] = (byte)total;
		put.add(Bytes.toBytes("frequency"), null, f);
		put.add(Bytes.toBytes("total"), null, t);
		table.put(put);
	}

	public static void main(String[] args) throws IOException {
		HTable table = new HTable(configuration, TABLE);
		if (args.length != 4) {
			System.out.println("参数错误");
		}
		try {
			long mainPhone = Long.parseLong(args[0]);
			long coupledPhone = Long.parseLong(args[1]);
			int frequency = Integer.parseInt(args[2]);
			int total = Integer.parseInt(args[3]);
			hbaseTestDataInsert(table, mainPhone, coupledPhone, frequency, total);
		} catch (Exception e) {
			System.out.println("插入异常");
			e.printStackTrace();
		} finally {
			table.close();
		}
		System.out.println("插入完成！OK");
	}
}
