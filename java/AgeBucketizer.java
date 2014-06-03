/*******************************************************************************
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *******************************************************************************/
package com.yahoo.ads.pb.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.ads.pb.platform.service.ConfigurationManager;
import com.yahoo.ads.pb.util.property.ConfigUtils;

/**
 * Map users into below buckets, return the bucket id
 *    18-24   25-34   35-44   45-54  55-64   65+
 * 
 * @author kailiu
 * 
 */
public class AgeBucketizer {

	public static final String ADMOVATE_TIMEZONE_STRING = "US/Pacific";
	public static final String ADMOVATE_TIMEZONE_KEY = "Admovate.TimeZone";
	public static final String DH_DATE_FORMAT = "yyyyMMddHHmm";
	public static final String DH_DATE_FORMAT1 = "yyyyMMdd";
	public static final String ADMOVATE_DATE_FORMAT = "yyyy_MM_dd";
	private static Logger logger = LoggerFactory.getLogger(DateUtil.class);

	// thread-safe calendar
	private static ThreadLocal<AdmovateCalendar> m_calendar = new ThreadLocal<AdmovateCalendar>() {
		protected AdmovateCalendar initialValue() {
			AdmovateCalendar caCalendar = new AdmovateCalendar();
			return caCalendar;
		}
	};

	// thread-safe date format
	private static ThreadLocal<SimpleDateFormat> m_dateFormat = new ThreadLocal<SimpleDateFormat>() {
		protected SimpleDateFormat initialValue() {
			SimpleDateFormat sdf = new SimpleDateFormat(ADMOVATE_DATE_FORMAT);
			sdf.setCalendar(new AdmovateCalendar());
			return sdf;
		}
	};

	public static TimeZone getAdmovateTimeZone() {
		String timeZoneStr = ADMOVATE_TIMEZONE_STRING;
		try {
			timeZoneStr = ConfigUtils.getStringKey(
					ConfigurationManager.getConfiguration(),
					ADMOVATE_TIMEZONE_KEY, ADMOVATE_TIMEZONE_STRING);
		} catch (Exception e) {
			logger.warn("Fail to get timezone str, using default {}",
					ADMOVATE_TIMEZONE_STRING);
		}
		TimeZone timeZone = TimeZone.getTimeZone(timeZoneStr);
		return timeZone;
	}

	public static final AdmovateCalendar getAdmovateCalendar() {
		return m_calendar.get();
	}

	public static final SimpleDateFormat getAdmovateDateFormat() {
		return m_dateFormat.get();
	}

	/**
	 * Get current year.
	 * 
	 * @return
	 */
	public static int getCurrentYear() {
		AdmovateCalendar caCal = getAdmovateCalendar();
		caCal.setTime(new Date());
		return caCal.get(Calendar.YEAR);
	}

	/**
	 * Get the current Unix time.
	 * 
	 * @return
	 */
	public static int getCurrentTimeInSeconds() {
		return (int) (System.currentTimeMillis() / 1000L);
	}

	/**
	 * # of days passed since the Unix epoch start using 1970/01/01 (US/Pacific)
	 * as reference.
	 * 
	 * -1 for NULL input.
	 * 
	 * @param date
	 * @return
	 */
	public static final int getDateKey(Date date) {
		if (date == null) {
			return -1;
		}

		AdmovateCalendar caCal = getAdmovateCalendar();
		caCal.setTime(date);
		return (int) caCal.getUnixDay();
	}

	/**
	 * # of days passed since the Unix epoch start using 1970/01/01 (US/Pacific)
	 * as reference.
	 * 
	 * @param timeInMillis
	 * @return
	 */
	public static final int getDateKey(long timeInMillis) {
		AdmovateCalendar caCal = getAdmovateCalendar();
		caCal.setTime(new Date(timeInMillis));
		return (int) caCal.getUnixDay();
	}

	/**
	 * # of days passed since the Unix epoch start using 1970/01/01 (US/Pacific)
	 * as reference.
	 * 
	 * @param dateStr
	 * @return 0 if input is invalid date string
	 */
	public static final int getDateKey(String dateStr) {
		SimpleDateFormat sdf = m_dateFormat.get();

		int dateKey = 0;
		try {
			Date date = sdf.parse(dateStr);
			dateKey = getDateKey(date);
		} catch (ParseException e) {
		}

		return dateKey;
	}

	/**
	 * Get the midnight of a given day, which is the begin of a day.
	 * 
	 * @param dateKey
	 * @return
	 */
	public static final Date getBeginOfDay(int dateKey) {
		return getDate(dateKey, 0);
	}

	/**
	 * Get the last millisecond of a day, which is the end of a day.
	 * 
	 * @param dateKey
	 * @return
	 */
	public static final Date getEndOfDay(int dateKey) {
		return getDate(dateKey, 23, 59, 59, 999);
	}

	/**
	 * Get the date for a given hour in the given day.
	 * 
	 * @param dateKey
	 * @param hour
	 * @return
	 */
	public static final Date getDate(int dateKey, int hour) {
		return getDate(dateKey, hour, 0, 0, 0);
	}

	public static final String getDateStrForDateKey(int dateKey) {
		Date date = getDate(dateKey, 12, 0, 0, 0);
		return getAdmovateDateString(date);
	}

	private static final Date getDate(int dateKey, int hour, int minute,
			int second, int milliSecond) {
		if (dateKey <= 0) {
			return null;
		}

		AdmovateCalendar tc = getAdmovateCalendar();
		tc.setTime(tc.getUnixBaseDate());
		tc.add(Calendar.DAY_OF_YEAR, dateKey);
		tc.add(Calendar.HOUR_OF_DAY, hour);
		tc.add(Calendar.MINUTE, minute);
		tc.add(Calendar.SECOND, second);
		tc.add(Calendar.MILLISECOND, milliSecond);

		return tc.getTime();
	}

	public static final int getHour(Date date) {
		long timeInMilli = date.getTime();
		return getHour(timeInMilli);
	}

	public static final int getHour(long timeInMilli) {
		AdmovateCalendar tc = getAdmovateCalendar();
		tc.setTimeInMillis(timeInMilli);
		return tc.get(Calendar.HOUR_OF_DAY);
	}

	public static final int getMinute(Date date) {
		AdmovateCalendar tc = getAdmovateCalendar();
		tc.setTimeInMillis(date.getTime());
		return tc.get(Calendar.MINUTE);
	}

	public static final String getDHDateString(long time) {
		SimpleDateFormat sdf = new SimpleDateFormat(DH_DATE_FORMAT);
		sdf.setCalendar(new AdmovateCalendar());

		return sdf.format(new Date(time));
	}

	public static final long getTimeGap(String timeStr) {
		SimpleDateFormat sdf = new SimpleDateFormat(DH_DATE_FORMAT);
		sdf.setCalendar(new AdmovateCalendar());
		try {
			Date date = sdf.parse(timeStr);
			return date.getTime()/1000;
		} catch (Exception e) {
			logger.error("getTimeGap failed");
		}
		return System.currentTimeMillis()/1000;
	}

	public static final String getDHCurrent() {
		SimpleDateFormat sdf = new SimpleDateFormat(DH_DATE_FORMAT1);
//		sdf.setCalendar(new AdmovateCalendar());
		return sdf.format(new Date(System.currentTimeMillis()));
	}
	
	public static final String getDHYesterday() {
		SimpleDateFormat sdf = new SimpleDateFormat(DH_DATE_FORMAT1);
		//sdf.setCalendar(new AdmovateCalendar());
		return sdf.format(new Date(System.currentTimeMillis() - 24*60*60*1000));
	}

	public static final String getAdmovateDateString(long time) {
		return getAdmovateDateString(new Date(time));
	}

	public static final String getAdmovateDateString(Date date) {
		SimpleDateFormat sdf = m_dateFormat.get();
		return sdf.format(date);
	}

	public static final int getLastSundayDateKey(Date date) {
		AdmovateCalendar tc = getAdmovateCalendar();
		tc.setTimeInMillis(date.getTime());
		int dayOfWeek = tc.get(Calendar.DAY_OF_WEEK);

		return getDateKey(date) - dayOfWeek + 1; // DAY_OF_WEEK starts from 1
	}

	public static final int getCurrentMonthFirstDayDateKey(Date date) {
		AdmovateCalendar tc = getAdmovateCalendar();
		tc.setTimeInMillis(date.getTime());
		int dayOfMonth = tc.get(Calendar.DAY_OF_MONTH);

		return getDateKey(date) - dayOfMonth + 1; // DAY_OF_MONTH starts from 1
	}

	public static final int getCurrentMonthLastDayDateKey(Date date) {
		AdmovateCalendar tc = getAdmovateCalendar();
		tc.setTimeInMillis(date.getTime());
		int dayOfMonth = tc.get(Calendar.DAY_OF_MONTH);
		int lastDayOfMonth = tc.getActualMaximum(Calendar.DAY_OF_MONTH);

		return getDateKey(date) + (lastDayOfMonth - dayOfMonth); // DAY_OF_MONTH
																	// starts
																	// from 1
	}

	public static final int getLastMonthFirstDayDateKey(Date date) {
		Date currentMonthFirstDay = getBeginOfDay(getCurrentMonthFirstDayDateKey(date));
		Date lastMonthFirstDay = DateUtils.addMonths(currentMonthFirstDay, -1);
		return getCurrentMonthFirstDayDateKey(lastMonthFirstDay);
	}

	public static final int getLastMonthLastDayDateKey(Date date) {
		Date currentMonthFirstDay = getBeginOfDay(getCurrentMonthFirstDayDateKey(date));
		Date lastMonthFirstDay = DateUtils.addMonths(currentMonthFirstDay, -1);
		return getCurrentMonthLastDayDateKey(lastMonthFirstDay);
	}

	public static Date parseMsftDate(String date, String timezone)
			throws ParseException {

		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-mm-dd hh:mm:ss");
		dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));
		return dateFormat.parse(date);

	}

	public static void main(String[] args) {

		System.out.println(getDHDateString(getTimeGap("201309081300")));
		System.out.println(getDHDateString(System.currentTimeMillis()));
		System.out.println(getDHCurrent());
		System.out.println(getDHYesterday());

		System.out.println(getAdmovateDateString(new Date()));

		int dateKey = getDateKey(new Date());

		System.out.println(dateKey + "\t" + System.currentTimeMillis());

		System.out.println(getBeginOfDay(dateKey));
		System.out.println(getDate(dateKey, 8));

		int lastSundayDateKey = getLastSundayDateKey(new Date());
		System.out.println(getBeginOfDay(lastSundayDateKey));

		System.out
				.println(getBeginOfDay(getCurrentMonthFirstDayDateKey(new Date())));
		System.out
				.println(getEndOfDay(getCurrentMonthLastDayDateKey(new Date())));

		System.out
				.println(getBeginOfDay(getLastMonthFirstDayDateKey(new Date())));
		System.out.println(getEndOfDay(getLastMonthLastDayDateKey(new Date())));
	}
}

