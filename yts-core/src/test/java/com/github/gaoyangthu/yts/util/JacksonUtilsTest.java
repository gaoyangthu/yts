package com.github.gaoyangthu.yts.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2015/1/14 0014
 */
public class JacksonUtilsTest {
	public JacksonUtilsTest() {
		System.out.println("A new JacksonUtilsTest instance.");
	}

	@Before
	public void setUp() throws Exception {
		System.out.println("Call @Before before a test mechod");
	}

	@After
	public void tearDown() throws Exception {
		System.out.println("Call @After after a test mechod");
	}

	/*@Test
	public void yunyinParse() {
//		String log = "18900628976\t{\"code\":0,\"errorDescription\":\"操作成功\",\"dataObject\":[" +
//			"{\"callType\":\"0\",\"callMobile\":\"18260625540\",\"callTime\":\"2014-12-02 19:13:24\",\"callTimeCost\":\"00:00:14\"}," +
//			"{\"callType\":\"1\",\"callMobile\":\"13775266448\",\"callTime\":\"2014-12-05 12:17:10\",\"callTimeCost\":\"00:02:16\"}," +
//			"{\"callType\":\"1\",\"callMobile\":\"13775266448\",\"callTime\":\"2014-12-07 11:48:21\",\"callTimeCost\":\"00:00:35\"}," +
//			"{\"callType\":\"1\",\"callMobile\":\"13017576907\",\"callTime\":\"2014-12-10 13:48:59\",\"callTimeCost\":\"00:01:09\"}]}";
		String log = "billrecord|3C4D2F7117E273909EE09C69E544E5E8||1|20141201|20141231|" +
			"{\"code\":0,\"errorDescription\":\"操作成功\",\"dataObject\":[" +
			"{\"callType\":\"0\",\"callMobile\":\"4008011888\",\"callTime\":\"2014-12-01 20:53:13\",\"callTimeCost\":\"00:04:09\"}," +
			"{\"callType\":\"0\",\"callMobile\":\"4008011888\",\"callTime\":\"2014-12-01 20:57:35\",\"callTimeCost\":\"00:01:57\"}," +
			"{\"callType\":\"0\",\"callMobile\":\"4008011888\",\"callTime\":\"2014-12-02 22:20:38\",\"callTimeCost\":\"00:02:24\"}," +
			"{\"callType\":\"1\",\"callMobile\":\"051510010\",\"callTime\":\"2014-12-03 20:30:24\",\"callTimeCost\":\"00:10:06\"}," +
			"{\"callType\":\"1\",\"callMobile\":\"051510010\",\"callTime\":\"2014-12-04 10:20:30\",\"callTimeCost\":\"00:00:41\"}," +
			"{\"callType\":\"0\",\"callMobile\":\"6F7F75A1208B6F806B5665FA01F62FE7\",\"callTime\":\"2014-12-04 11:20:09\",\"callTimeCost\":\"00:00:11\"}," +
			"{\"callType\":\"1\",\"callMobile\":\"4C0A88D59D925A1EC5846D969B33E81C\",\"callTime\":\"2014-12-19 19:06:28\",\"callTimeCost\":\"00:01:41\"}," +
			"{\"callType\":\"1\",\"callMobile\":\"4C0A88D59D925A1EC5846D969B33E81C\",\"callTime\":\"2014-12-19 19:21:24\",\"callTimeCost\":\"00:00:59\"}," +
			"{\"callType\":\"1\",\"callMobile\":\"10000\",\"callTime\":\"2014-12-23 15:51:41\",\"callTimeCost\":\"00:02:21\"}," +
			"{\"callType\":\"1\",\"callMobile\":\"10000\",\"callTime\":\"2014-12-23 15:55:16\",\"callTimeCost\":\"00:00:22\"}," +
			"{\"callType\":\"0\",\"callMobile\":\"10000\",\"callTime\":\"2014-12-23 20:44:25\",\"callTimeCost\":\"00:00:32\"}," +
			"{\"callType\":\"0\",\"callMobile\":\"10000\",\"callTime\":\"2014-12-25 22:09:10\",\"callTimeCost\":\"00:01:07\"}," +
			"{\"callType\":\"0\",\"callMobile\":\"10000\",\"callTime\":\"2014-12-26 12:57:02\",\"callTimeCost\":\"00:01:49\"}," +
			"{\"callType\":\"0\",\"callMobile\":\"10000\",\"callTime\":\"2014-12-26 12:58:57\",\"callTimeCost\":\"00:01:23\"}," +
			"{\"callType\":\"0\",\"callMobile\":\"4006187125\",\"callTime\":\"2014-12-29 12:57:07\",\"callTimeCost\":\"00:02:00\"}]}";
		String[] tmp = log.split("\\|");
		String mainPhone = tmp[1];
		String content = tmp[6];
		System.out.println(mainPhone);
		try {
			ArrayList dataObject = (ArrayList) JacksonUtils.parseMap(content).get("dataObject");
			for (Object dataContent : dataObject) {
				Map dataMap = JacksonUtils.parseMap(JacksonUtils.toJson(dataContent));
				String coupledPhone = dataMap.get("callMobile").toString();
				if (coupledPhone.length() == 32) {
					System.out.println(mainPhone+coupledPhone);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		String phone1 = "13311101234";
		String phone2 = "18911901191";
		byte[] key = DigestUtils.md5(phone1 + phone2);
		System.out.println(Arrays.toString(key));
		for (byte b : key) {
			String hex = Integer.toHexString(b & 0xFF);
			if (hex.length() == 1) {
				hex = '0' + hex;
			}
			System.out.print(hex.toUpperCase());
		}
		System.out.println();
	}*/

	/*@Test
	public void feiyongParse() throws IOException {
		String log = "historybill|EEE1DB65109BF36CC6E1CE1A96C925E5|201412|1|||" +
			"{\"code\":0,\"errorDescription\":\"操作成功\",\"dataObject\":" +
			"{\"chargeAll\":\"105.0\",\"chargePaid\":\"105.0\",\"chargeShouldpay\":\"0.0\",\"acctName\":\"9E60C523F405EF016539169703DC6D4D\",\"chargeDiscount\":\"0.0\",\"chargeAccountInfo\":\"手机：EEE1DB65109BF36CC6E1CE1A96C925E5\",\"billItemInfo\":[" +
			"{\"showlevel\":\"1\",\"chargeDisname\":\"天翼乐享3G-201108上网版89元\",\"accountMobile\":\"手机：EEE1DB65109BF36CC6E1CE1A96C925E5\",\"classId\":\"1\",\"parentClassId\":\"0\",\"chargeName\":\"套餐基本费\",\"charge\":\"89.0\"}," +
			"{\"showlevel\":\"2\",\"chargeDisname\":null,\"accountMobile\":null,\"classId\":\"2\",\"parentClassId\":\"1\",\"chargeName\":\"套餐月基本费\",\"charge\":\"89.0\"}," +
			"{\"showlevel\":\"1\",\"chargeDisname\":\"天翼乐享3G-201108上网版89元\",\"accountMobile\":\"手机：EEE1DB65109BF36CC6E1CE1A96C925E5\",\"classId\":\"3\",\"parentClassId\":\"0\",\"chargeName\":\"上网及数据通信费\",\"charge\":\"10.0\"}," +
			"{\"showlevel\":\"2\",\"chargeDisname\":null,\"accountMobile\":null,\"classId\":\"4\",\"parentClassId\":\"3\",\"chargeName\":\"手机上网闲时包\",\"charge\":\"10.0\"}," +
			"{\"showlevel\":\"1\",\"chargeDisname\":\"天翼乐享3G-201108上网版89元\",\"accountMobile\":\"手机：EEE1DB65109BF36CC6E1CE1A96C925E5\",\"classId\":\"5\",\"parentClassId\":\"0\",\"chargeName\":\"综合信息服务费\",\"charge\":\"6.0\"}," +
			"{\"showlevel\":\"2\",\"chargeDisname\":null,\"accountMobile\":null,\"classId\":\"6\",\"parentClassId\":\"5\",\"chargeName\":\"七彩铃音费\",\"charge\":\"6.0\"}]}}";

		String[] tmp = log.split("\\|");
		String mainPhone = tmp[1];
		String month = tmp[2];
		String content = tmp[6];
		String charge = "";
		//System.out.println(mainPhone+"\t"+month);
		try {
			Object dataObject = JacksonUtils.parseMap(content).get("dataObject");
			if (dataObject instanceof Map) {
				charge = (String) ((Map)dataObject).get("chargeAll");
//					Object billItemInfo = (ArrayList) JacksonUtils.parseList(JacksonUtils.toJson(((Map)dataObject).get("billItemInfo")));
//					if (billItemInfo instanceof List) {
//						double charge = 0.0;
//						for (Object billContent : (ArrayList) billItemInfo) {
//							Object billMap = JacksonUtils.parseMap(JacksonUtils.toJson(billContent));
//							if (billMap instanceof Map) {
//								if (((String) ((Map)billMap).get("showlevel")).equals("2")) {
//									sum += 1;
//									charge += Double.valueOf((String) ((Map)billMap).get("charge"));
//								}
//							}
//						}
//						System.out.println(charge);
//					}
			}
			System.out.println(mainPhone + "\t" + month + "\t" + charge);
		} catch (Exception e) {
			//System.out.println(mainPhone+"\t"+month);
			//e.printStackTrace();
		}
	}*/

	@Test
	public void md5() {
		byte[] key = DigestUtils.md5("110000198012120010");
		for (byte b : key) {
			String hex = Integer.toHexString(b & 0xFF);
			if (hex.length() == 1) {
				hex = '0' + hex;
			}
			System.out.print(hex.toUpperCase());
		}
		System.out.println();
	}
}
