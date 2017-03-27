import java.util.concurrent.ThreadLocalRandom;

public class RequestorRunnable implements Config, Runnable {

	private long deadline = 0L;
	private long staleness = 0L;
	private boolean isExpired = false;
	private double gearNo = 0.0;
	public static boolean isLastRequestComplete = true;

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (true) {
			while (isLastRequestComplete) {
				int randomAngle = ThreadLocalRandom.current().nextInt(0, 360 + 1);
				gearNo = 2 * Math.sin(randomAngle) + 3;

			}
		}

	}

}
