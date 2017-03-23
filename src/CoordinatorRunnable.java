import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class CoordinatorRunnable implements Runnable, Config {
	private Connection conn = null;
	private PreparedStatement stmt = null;
	private ResultSet rs = null;
	public long maxSeqID = 0L;
	private BlockingQueue<Task> queue = null;
	private long sessionEndTime = 0L;
	public static long sessionStartTime = 0L;
	private Writer out;
	private Writer taskProcessingTimeOutput;
	private String tableNames = null;

	public CoordinatorRunnable(BlockingQueue<Task> queue, long sessionEndTime) {
		this.queue = queue;
		this.sessionStartTime = System.currentTimeMillis();
		this.sessionEndTime = sessionEndTime;
		try {
			conn = Client.getConnection();
			String query = "select max(event_id) from audit.logged_actions where " + "table_name in("
					+ tables.get(System.getProperty("tables")) + ")";
			stmt = conn.prepareStatement(query);
			// stmt.setArray(1, conn.createArrayOf("text",
			// tables.get(System.getProperty("tables"))));
			rs = stmt.executeQuery();
			rs.next();
			maxSeqID = rs.getLong(1);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		String fileName = "log_file_thread_" + System.getProperty("numberOfThread") + "_coordinator_sleep_time_"
				+ System.getProperty("sleepDuration") + "_"+ dateFormat.format(new Date());
		String taskProcessingTimeFileName = "task_processing_time_" + System.getProperty("numberOfThread")
				+ "_coordinator_sleep_time_" + System.getProperty("sleepDuration") + "_"+ dateFormat.format(new Date());
		;
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, true), "UTF-8"));
			taskProcessingTimeOutput = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(taskProcessingTimeFileName, true), "UTF-8"));
		} catch (UnsupportedEncodingException | FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {

		// writing to the log
		writeLog("Program running with " + System.getProperty("numberOfThread")
				+ " threads, Coordinator Sleep Duration: " + System.getProperty("sleepDuration") + " and run duration: "
				+ System.getProperty("runDuration"));

		while (sessionEndTime > System.currentTimeMillis()) {
			try {
				long sleepDuration = Long.parseLong(System.getProperty("sleepDuration"));
				Thread.sleep(sleepDuration);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			try {
				rs = stmt.executeQuery();
				rs.next();
				long tmpMaxSeqID = rs.getLong(1);
				if (tmpMaxSeqID > maxSeqID) {
					queue.put(new Task(maxSeqID, tmpMaxSeqID));
					synchronized(queue) {
						writeLog("Queue Size: " + queue.size());
					}
					maxSeqID = tmpMaxSeqID;
					// System.out.println(maxSeqID);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				try {
					rs.close();
					// stmt.close();
					// conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

		}
	}

	public void writeLog(String log) {

		try {
			out.append(dateFormat.format(new Date()) + " : " + log + "\n");
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void writeTaskProcessingTime(long taskCount, long taskProcessingTime, int taskId) {
		try {
			taskProcessingTimeOutput.append(taskCount + "," + taskProcessingTime + "," + taskId + "\n");
			taskProcessingTimeOutput.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
