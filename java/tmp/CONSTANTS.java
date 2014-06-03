import java.sql.Timestamp;

public class CONSTANTS {

    public static final String ORIGIN = "haha"; 

	public static void main(String[] args) {

                Timestamp ts = new Timestamp(System.currentTimeMillis());
		System.out.println("Current milliseconds: " + ts.getTime());

                Long ts_sec = (Long) System.currentTimeMillis()/1000;
		System.out.println("Current seconds: " + ts_sec);

		System.out.println("the original: " + ORIGIN);

        ORIGIN = "hoho";
		System.out.println("the original: " + ORIGIN);

	}
}
