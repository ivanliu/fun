import java.util.ArrayList;
import java.util.Set;
import java.util.Date;
import java.util.HashSet;
import java.util.Arrays;
import java.text.SimpleDateFormat;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class Timestamp {

    public static void main(String[] args) {

        String nominalTime = "201308011200";
        
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMddHHmm");
        String unixTime = String.valueOf(formatter.parseMillis(nominalTime)/1000);

        System.out.println("nominal time: " + nominalTime);
        System.out.println("unix time: " + unixTime);

        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        System.out.println("now date: " + sdf.format(date));

    }
}
