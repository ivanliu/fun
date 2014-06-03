import java.sql.Timestamp;

public class TS {
	public static void main(String[] args) {

                Timestamp ts = new Timestamp(System.currentTimeMillis());
		System.out.println("Current milliseconds: " + ts.getTime());

                Long ts_sec = (Long) System.currentTimeMillis()/1000;
		System.out.println("Current seconds: " + ts_sec);

	}
}
