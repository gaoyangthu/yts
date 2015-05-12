package com.github.gaoyangthu.yts.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2015/1/13 0013
 */
public class TimeUtilsTest {
	public TimeUtilsTest() {
		System.out.println("A new TimeUtilsTest instance.");
	}

	@Before
	public void setUp() throws Exception {
		System.out.println("Call @Before before a test mechod");
	}

	@After
	public void tearDown() throws Exception {
		System.out.println("Call @After after a test mechod");
	}

	@Test
	public void getTime() {
		String format = "yyyy-MM-dd HH:mm:ss";
		Calendar calendar = Calendar.getInstance();
		calendar.set(2015, 0, 1, 0, 0, 0);
		Date date = calendar.getTime();
		String str = "2015-01-01 00:00:00";
		System.out.println(TimeUtils.datetoString(date, format));
		System.out.println(TimeUtils.stringToDate(str, format));
	}
}
