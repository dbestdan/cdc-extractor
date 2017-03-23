import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Timestamp;
import java.util.Date;

public class QueryRequestRunnable implements Runnable,Config{
	private Writer out;
	private long sessionNextTimeStamp = 0L;

	public QueryRequestRunnable() {
		
		String stalenessFileName = "staleness_" + System.getProperty("numberOfThread")
		+ "_coordinator_sleep_time_" + System.getProperty("sleepDuration") + "_"+dateFormat.format(new Date());
	
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(stalenessFileName, true), "UTF-8"));
			
		} catch (UnsupportedEncodingException | FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	@Override
	public void run() {
		sessionNextTimeStamp = System.currentTimeMillis()+60000;
		while(true) {
			
			try {
				long staleness = System.currentTimeMillis() - WorkerRunnable.uptodate.getTime();
				out.append((System.currentTimeMillis() - CoordinatorRunnable.sessionStartTime) +","+ staleness+"\n");
				if(System.currentTimeMillis()>sessionNextTimeStamp) {
					sessionNextTimeStamp +=60000;
					out.flush();
				}
			} catch (IOException e1) {
				e1.printStackTrace();
			}
					//have a file to record staleness
					//buffer the staleness into a list, flush the list periodically to the on-disk file
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
