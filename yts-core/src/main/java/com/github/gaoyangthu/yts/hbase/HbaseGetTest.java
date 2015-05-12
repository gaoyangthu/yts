package com.github.gaoyangthu.yts.hbase;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2015/1/22 0022
 */
public class HbaseGetTest {
	public static final String QUORUM = "master004,master005,master006";
	public static final String PORT = "2181";
	public static final String PARENT = "/hbase_wins";
	public static final String TABLE = "yts_calling_summary";
	public static final String QUALIFIER = "";
	private static final String FAMILY1 = "frequency";
	private static final String FAMILY2 = "total";
	public static Configuration configuration;
	private static HConnection connection;

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", QUORUM);
		configuration.set("hbase.zookeeper.property.clientPort", PORT);
		configuration.set("zookeeper.znode.parent", PARENT);
		configuration.set("hbase.client.retries.number", "3");
		configuration.set("hbase.rpc.timeout", "5000");
		try {
			connection = HConnectionManager.createConnection(configuration);
		} catch (IOException e) {
			connection = null;
		}
	}

	private int getByRowkey(byte[] rowkey, byte[] family) {
		HTableInterface table = null;
		int num = -1;
		try {
			table = connection.getTable(TABLE);
			Get get = new Get(rowkey);
			Result rs = table.get(get);
			if (rs.isEmpty()) {
				return -1;
			} else {
				List<Cell> list = rs.getColumnCells(family, null);
				if (list.isEmpty()) {
					return -1;
				} else {
					Cell cell = list.get(0);
					byte[] b = CellUtil.cloneValue(cell);
					return CellUtil.cloneValue(cell)[0];
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (Exception e2) {
					// TODO: handle exception
				}
			}
		}
		return num;
	}

	/**
	 * @param tel1 电话号码
	 * @param tel2 电话号码
	 * @param type 2 为查询两个号码所有账期的通话频率的整型值
	 *             1 为两个号码当前账期的通话频率的整型值
	 * @return
	 */
	public int getFrequency(String tel1, String tel2, int type) {
		String rowkey = "";
		long t1 = Long.parseLong(tel1);
		long t2 = Long.parseLong(tel2);
		if (t1 > t2) {
			rowkey = tel2 + tel1;
		} else {
			rowkey = tel1 + tel2;
		}
		byte[] rowbyte = DigestUtils.md5(rowkey);
		byte[] family = null;
		if (type == 1) {
			family = FAMILY1.getBytes();
		} else if (type == 2) {
			family = FAMILY2.getBytes();
		} else {
			return -1;
		}
		try {
			int x = getByRowkey(rowbyte, family);
			return x;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	public void close() {
		try {
			connection.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public int getF(String tel1, String tel2) {
		String key = "";
		long t1 = Long.parseLong(tel1);
		long t2 = Long.parseLong(tel2);
		if (t1 < t2) {
			key = tel1 + tel2;
		} else {
			key = tel2 + tel1;
		}
		byte[] rowkey = DigestUtils.md5(key);
		HTableInterface table = null;
		int num = -1;
		try {
			table = connection.getTable(TABLE);
			Get get = new Get(rowkey);
			Result rs = table.get(get);
			if (rs.isEmpty()) {
				return -1;
			} else {
				List<Cell> list = rs.getColumnCells(Bytes.toBytes("f"), Bytes.toBytes(""));
				if (list.isEmpty()) {
					return -1;
				} else {
					Cell cell = list.get(0);
					byte[] b = CellUtil.cloneValue(cell);
					return CellUtil.cloneValue(cell)[0];
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (Exception e2) {
					// TODO: handle exception
				}
			}
		}
		return num;
	}

	public static void main(String[] args) {
		System.out.println("-------start---------");
		HbaseGetTest hgt = new HbaseGetTest();
		//int x = hgt.getFrequency(args[0], args[1], Integer.parseInt(args[2]));
		int x = hgt.getF(args[0], args[1]);
		System.out.println("x========" + x);
	}
}
