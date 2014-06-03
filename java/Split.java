import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

public class Split {


    public String extractDate(String path, String prefix, String suffix) {

        String date;

        int prefixIndex = path.indexOf(prefix);
        int suffixIndex = path.indexOf(suffix);
        System.out.println("prefix index = " + prefixIndex + " suffix index = " + suffixIndex);
        if ( (prefixIndex == -1) || (suffixIndex == -1) ) {
            return null;
        }

        int start = prefixIndex + prefix.length(); 
        System.out.println("start = " + start);
        if ( suffix != "" ) {
            date = path.substring(start, suffixIndex);
        } else {
            date = path.substring(start);
        }

        return date.replaceAll("/", "");
        //return date;

    }


    public static void main(String[] args) {

        String input = "DAC:hdfs://dilithiumblue-nn1.blue.ygrid.yahoo.com:8020/projects/btmp/prod/raw/DataGen";
        String[] kv = input.split(":", 2);
        System.out.println("original string: " + input);
        System.out.println("length of split string: " + kv.length);
        System.out.println("part 1: " + kv[0]);
        System.out.println("part 2: " + kv[1]);

        /*
           ArrayList<String> data = null;
           for (String sth : data) {
           System.out.println("the string: " + sth);
           }
           */

        Long aa = 123L;
        Long bb = null;
        String aa_s = String.valueOf(aa);
        String bb_s = String.valueOf(bb);
        System.out.println("Long aa in string = " + aa_s);
        System.out.println("Long bb in string = " + bb_s);

        Long aa_l = Long.valueOf(aa_s);
        Long bb_l;
        if (bb_s.equals("null")) {
            bb_l = null;
        } else {
            bb_l = Long.valueOf(bb_s);
        }
        System.out.println("Long aa in Long = " + aa_l);
        System.out.println("Long bb in Long = " + bb_l);

        //Long bb_l = Long.parseLong(bb);

        input = "kailiu is good, right?";
        String pattern1 = "good";
        String pattern2 = "bad";
        String pattern3 = "";
        Integer pos1 = input.indexOf(pattern1);
        Integer pos2 = input.indexOf(pattern2);
        Integer pos3 = input.indexOf(pattern3);
        System.out.println("Orignial string = " + input);
        System.out.println("Pos of pattern 1 = " + pos1);
        System.out.println("Pos of pattern 2 = " + pos2);
        System.out.println("Pos of pattern 3 = " + pos3);

        String substr1 = input.substring(6);
        String substr2 = input.substring(6, 12);
        System.out.println("Sub string 1 = " + substr1);
        System.out.println("Sub string 2 = " + substr2);

        String aaaa = "{12\\,34\\,";
        String bbbb = aaaa.replaceAll("\\\\,$", "");
        System.out.println("str a: " + aaaa);
        System.out.println("str b: " + bbbb);

        Split ss = new Split();
        String path = "/projects/targeting/mrt/prod/yuid2sid/enrich_data/20130612/";
        String prefix = "/projects/targeting/mrt/prod/yuid2sid/enrich_data";
        String suffix = "";
        String date = ss.extractDate(path, prefix, suffix);
        System.out.println("path: " + path);
        System.out.println("prefix: " + prefix);
        System.out.println("suffix: " + suffix);
        System.out.println("date: " + date);

        String aabb = "aaa,bbb,ccc,ddd";
        String array[] = aabb.split(",");
        for (String tt : array) {
           System.out.println("str: " + tt);
        }

        String abcd = null;
        abcd = (abcd == null) ? "haha" : abcd;
        System.out.println("abcd = " + abcd);

        String names = ",DAC,FACEBOOK,MAIL,,";
        String[] nameArray = names.split(",");
        Set<String> nameSet = new HashSet<String>(Arrays.asList(nameArray));
        System.out.println("name string: " + names);
        System.out.println("len of array: " + nameArray.length);
        System.out.println("name set: " + nameSet);

        Long ll = null;
        Long tt = ll * 100;

        System.out.println("ll: " + ll);
        System.out.println("tt: " + tt);


    }
}
