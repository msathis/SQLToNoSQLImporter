package net.sathis.export.sql.model;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class FieldTypeParser {

	public static String dateToString(Object o ) {

		Date date = (Date) o;

		SimpleDateFormat df = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ssz" );

		df.setTimeZone( TimeZone.getTimeZone( "IST" ) );

		String output = df.format( date );

		output = output.replaceAll( "IST", "" );

		return output;

	}
	/**
	 * Convert to double
	 * @param o
	 * @return
	 */
	public static double getDouble(Object o) {
		return new Double(o.toString());
	}

	/**
	 * Convert to Integer
	 * @param o
	 * @return
	 */
	public static int getInt(Object o) {
		return new Integer(o.toString());
	}

	/**
	 * Convert to Long
	 * @param o
	 * @return
	 */
	public static long getLong(Object o) {
		return new Long(o.toString());
	}

	/**
	 * Convert to String
	 * @param o
	 * @return
	 */
	public static String getString(Object o) {
		return String.valueOf(o);
	}

	/**
	 * Convert to boolean
	 * @param o
	 * @return
	 */
	public static boolean getBoolean(Object o) {
		return new Boolean(o.toString());
	}

	/**
	 * Convert to BigInteger
	 * @param o
	 * @return
	 */
	public static BigInteger getBigint(Object o) {
		return new BigInteger(o.toString());
	}

	/**
	 * Convert to BigDecimal
	 * @param o
	 * @return
	 */
	public static BigDecimal getBigDecimal(Object o) {
		return new BigDecimal(o.toString());
	}
}
