import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;

public class CoordinatorThread implements Runnable{
	private Connection conn = null;
	private PreparedStatement stmt = null;
	private ResultSet rs = null;
	public long maxSeqID = 0L;
	private BlockingQueue<Task> queue = null;
	private long  sessionEndTime = 0L;
	
	public CoordinatorThread(BlockingQueue<Task> queue, long sessionEndTime){
		this.queue = queue;
		this.sessionEndTime = sessionEndTime;
		try {
			conn = Client.getConnection();
			stmt = conn.prepareStatement("select max(event_id) from audit.logged_actions");
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
	}
	
	@Override
	public void run() {

		while(sessionEndTime>System.currentTimeMillis()){
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
				if(tmpMaxSeqID > maxSeqID){
					queue.put(new Task(maxSeqID, tmpMaxSeqID));
					maxSeqID = tmpMaxSeqID;
//					System.out.println(maxSeqID);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				try {
					rs.close();
					//stmt.close();
					//conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
		}
	}

}
