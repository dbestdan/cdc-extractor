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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class WorkerThread implements Runnable {
	private BlockingQueue<Task> queue = null;
	// private FileOutputStream fop = null;
	// private File file;
	private Writer out = null;
	private Map<Timestamp, HashSet<Long>> map = null;
	// private File changedDataRecordCSvFile = null;
	// private PrintWriter changedDataRecordFileWriter = null;
	private long totalWaitTime = 0L;
	private int threadID = 0;
	private long sessionNextTimeStamp =0;
	private long sessionEndTime=0L;
	
	public WorkerThread(int threadID, BlockingQueue<Task> queue, long sessionEndTime) {
		this.queue = queue;
		this.sessionEndTime = sessionEndTime;
		this.map = new HashMap<Timestamp, HashSet<Long>>();

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
		String fileName = "chunk"+threadID;
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
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement("select * from audit.logged_actions where event_id > ? and event_id <= ?");
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		try {
			long threadStartTime = System.nanoTime();
			long sessionStartTime = System.currentTimeMillis();
			sessionNextTimeStamp = System.currentTimeMillis()+60000;
			while (sessionEndTime>System.currentTimeMillis()) {
				long startWaitTime = System.nanoTime();
				Task task = queue.take();
				totalWaitTime += System.nanoTime()-startWaitTime;
				stmt.setLong(1, task.getMinSeqID());
				stmt.setLong(2, task.getMaxSeqID());
				rs = stmt.executeQuery();
				ids.clear();

				while (rs.next()) {
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
				if(sessionNextTimeStamp< System.currentTimeMillis()){
					System.out.println("Thread-"+threadID+" sleep time: "+totalWaitTime);
					sessionNextTimeStamp +=60000;
				}
			}
			System.out.println("thread exit: "+ threadID);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs!=null)
					rs.close();
				if(stmt!=null)
					stmt.close();
				if(conn!=null)
					conn.close();
				out.close();
			} catch (SQLException | IOException e) {
				e.printStackTrace();
			}
		}

	}

	private void writeLocalFile(ResultSet rs) {
		// try {
		// ResultSetMetaData resultSetMetaData = rs.getMetaData();
		// int numberOfColumns = resultSetMetaData.getColumnCount();
		// StringBuilder rowStringBuilder = new StringBuilder();
		// for (int i = 1; i <= numberOfColumns; i++) {
		// rowStringBuilder.append(rs.getString(i));
		// rowStringBuilder.append("§");
		// }
		//
		// if (rowStringBuilder.length() > 0)
		// rowStringBuilder.setLength(rowStringBuilder.length() - 1);
		//
		// changedDataRecordFileWriter.println(rowStringBuilder);
		// } catch (SQLException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// try {
		//
		//
		// Path file = Paths.get(fileName);
		// Files.write(file, lines, Charset.forName("UTF-8"));
		// } catch (IOException e) {
		// e.printStackTrace();
		// } catch (SQLException e1) {
		// e1.printStackTrace();
		// }
		// List<String> lines = Arrays.asList(
		// rs.getString(1),
		// rs.getString(2),
		// rs.getString(3),
		// rs.getString(4),
		// rs.getString(5),
		// rs.getString(6),
		// rs.getString(7),
		// rs.getString(8),
		// rs.getString(9),
		// rs.getString(10),
		// rs.getString(11),
		// rs.getString(12),
		// rs.getString(13),
		// rs.getString(14),
		// rs.getString(15),
		// rs.getString(16),
		// rs.getString(17)
		// );
		try {
			StringBuffer sb = new StringBuffer();
			for (int i = 1; i < 18; i++) {
				sb.append(rs.getString(i)+"|");
			}
			sb.append("\n");
			out.append(sb.toString());
			out.flush();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}

	}

}
