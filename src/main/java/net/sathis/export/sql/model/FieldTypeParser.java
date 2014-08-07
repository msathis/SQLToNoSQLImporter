package net.sathis.export.sql.model;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class FieldTypeParser {

	public static String dateToString(Object o ) {
        
		Date date = (Date) o;
		
        SimpleDateFormat df = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ssz" );
        
        TimeZone tz = TimeZone.getTimeZone( "UTC" );
        
        df.setTimeZone( tz );

        String output = df.format( date );

        int inset0 = 9;
        
        String s0 = output.substring( 0, output.length() - inset0 );
        String s1 = output.substring( output.length() - inset0, output.length()- 6) + "UTC";

        String result = s0 + s1;

        result = result.replaceAll( "UTC", ":00Z" );
        
        return result;
        
    }
	public static double getDouble(Object o) {
		return new Double(o.toString());
	}
	public static int getInt(Object o) {
		return new Integer(o.toString());
	}
	public static long getLong(Object o) {
		return new Long(o.toString());
	}
	public static String getString(Object o) {
		return String.valueOf(o);
	}
	public static boolean getBoolean(Object o) {
		return new Boolean(o.toString());
	}
}
