import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class WorkerRunnable implements Runnable, Config {
	public static Timestamp uptodate = null;
	private BlockingQueue<Task> queue = null;
	// private FileOutputStream fop = null;
	// private File file;
	private Writer out = null;
	private Map<Timestamp, HashSet<Long>> map = null;
	// private File changedDataRecordCSvFile = null;
	// private PrintWriter changedDataRecordFileWriter = null;
	private Long totalWaitTime = 0L;
	private int threadID = 0;
	private long sessionNextTimeStamp = 0;
	private long sessionEndTime = 0L;
	private long taskCount = 0L;
	private long taskCountPerMinute = 0L;
	private long rowCountPerMinute = 0L;
	private long averageRowCountPerMinute = 0L;
	private long rowCount = 0L;
	private long averageRowCount = 0L;
	private CoordinatorRunnable parent = null;
	private long taskProcessingTime = 0L;

	public WorkerRunnable(int threadID, BlockingQueue<Task> queue, long sessionEndTime, CoordinatorRunnable parent) {
		this.parent = parent;
		this.queue = queue;
		this.sessionEndTime = sessionEndTime;
		this.map = new HashMap<Timestamp, HashSet<Long>>();
		Date date = null;
		try {
			date = dateFormat.parse("2010-10-10-10-10-10");
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		long time = date.getTime();
			
		uptodate = new Timestamp(time);

		// file = new File(fileName);
		// try {
		// fop = new FileOutputStream(file);
		// // if file doesn't exists, then create it
		// if (!file.exists()) {
		// file.createNewFile();
		// }
		// } catch (FileNotFoundException e) {
		// e.printStackTrace();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		String fileName = "chunk" + threadID;
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, true), "UTF-8"));
		} catch (UnsupportedEncodingException | FileNotFoundException e) {
			e.printStackTrace();
		}
		this.totalWaitTime = 0L;
		this.threadID = threadID;
	}

	@Override
	public void run() {
		Set<Long> ids = new HashSet<Long>();
		Connection conn = Client.getConnection();
		PreparedStatement stmt = null;
		PreparedStatement countStmt = null;
		ResultSet rs = null;
		try {
			String query = "select * from audit.logged_actions " + "where event_id > ? and event_id <= ? "
					+ "and table_name in (" + tables.get(System.getProperty("tables")) + ")";
			stmt = conn.prepareStatement(query);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		try {

			long threadStartTime = System.nanoTime();
			long sessionStartTime = System.currentTimeMillis();
			sessionNextTimeStamp = System.currentTimeMillis() + 60000;
			while (sessionEndTime > System.currentTimeMillis()) {
				long startWaitTime = System.nanoTime();
				long startWaitTimeMili = System.currentTimeMillis();
				Task task = queue.take();

				// Total weight time
				totalWaitTime = totalWaitTime + (System.nanoTime() - startWaitTime);

				// keep track of task
				taskCount++;
				taskCountPerMinute++;

				stmt.setLong(1, task.getMinSeqID());
				stmt.setLong(2, task.getMaxSeqID());
				rs = stmt.executeQuery();
				ids.clear();

				while (rs.next()) {
					// keep track of number of rows
					rowCountPerMinute++;
					rowCount++;

					// write to a file
					writeLocalFile(rs);

					Long taID = rs.getLong(9);
					ids.add(taID);
				}
				for (Long id : ids) {
					rs = conn.createStatement().executeQuery("select pg_xact_commit_timestamp('" + id + "'::xid)");
					rs.next();
					Timestamp t = rs.getTimestamp(1);
					if (map.keySet().contains(t)) {
						map.get(t).add(id);
					} else {
						HashSet<Long> set = new HashSet<Long>();
						set.add(id);
						map.put(t, set);
					}
				}
				Timestamp tmp = Collections.max(map.keySet());
				synchronized(uptodate) {
					if(uptodate == null || uptodate.before(tmp)) {
						uptodate = tmp;
					}
				}
				
				// keep track of task processing time
				taskProcessingTime = (System.currentTimeMillis() - startWaitTimeMili);
				parent.writeTaskProcessingTime(taskCount, taskProcessingTime, threadID);
				
				
				if (sessionNextTimeStamp < System.currentTimeMillis()) {
					// Total weight time
					long duration = TimeUnit.SECONDS.convert(totalWaitTime, TimeUnit.NANOSECONDS);
					System.out.println("Thread-" + threadID + " Weight time: " + duration);
					parent.writeLog("Thread-" + threadID + " Weight time: " + duration + " seconds");

					// average row count
					averageRowCount = rowCount / taskCount;
					averageRowCountPerMinute = rowCountPerMinute / taskCountPerMinute;

					parent.writeLog("Thread-" + threadID + " Number Of Task within a minute: " + taskCountPerMinute);
					parent.writeLog("Thread-" + threadID + " Total Number Of Task : " + taskCount);
					parent.writeLog("Thread-" + threadID + " Total Row count : " + rowCount);
					parent.writeLog("Thread-" + threadID + " Average row count perminute: " + averageRowCountPerMinute);
					parent.writeLog("Thread-" + threadID + " Total Average row count : " + averageRowCount);

					//

					sessionNextTimeStamp += 60000;
					taskCountPerMinute = 0L;
					rowCountPerMinute = 0L;
				}
			}
			System.out.println("thread exit: " + threadID);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
				out.close();
			} catch (SQLException | IOException e) {
				e.printStackTrace();
			}
		}

	}

	private void writeLocalFile(ResultSet rs) {

		try {
			StringBuffer sb = new StringBuffer();
			for (int i = 1; i < 18; i++) {
				sb.append(rs.getString(i) + "|");
			}
			sb.append("\n");
			out.append(sb.toString());
			out.flush();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}

	}

}
