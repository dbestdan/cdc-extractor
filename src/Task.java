
public class Task {
	private long minSeqID;
	private long maxSeqID;
	public Task(long min, long max) {
		this.minSeqID = min;
		this.maxSeqID = max;
	}
	public long getMinSeqID() {
		return this.minSeqID;
	}
	public long getMaxSeqID(){
		return this.maxSeqID;
	}
	
}
