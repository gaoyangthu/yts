package com.github.gaoyangthu.yts.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2015/1/22 0022
 */
public class HbaseScanTest {
	public static final String QUORUM = "master004,master005,master006";
	public static final String PORT = "2181";
	public static final String PARENT = "/hbase_wins";
	public static final String TABLE = "yts_calling_summary";
	public static final String QUALIFIER = "";
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

	public void scan() {
		HTableInterface table = null;
		try {
			table = connection.getTable(TABLE);
			Scan scan = new Scan();
			scan.setMaxVersions(100);
			ResultScanner scanner = table.getScanner(scan);
			for (Result rs : scanner) {
				System.out.println(rs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (Exception e2) {
					e2.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {
		System.out.println("-------start---------");
		HbaseScanTest hst = new HbaseScanTest();
		hst.scan();
		System.out.println("--------end----------");
	}
}
