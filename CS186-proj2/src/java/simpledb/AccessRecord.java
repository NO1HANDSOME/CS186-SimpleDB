package simpledb;

/**
 * 当<code>BufferPool</code>访问某个page时，会在<code>BufferPool</code>中更新一个<code>AccessRecord</code>
 * <p>
 * <code>AccessRecord</code>存储了上一次访问时间等信息，可以用于<code>BufferPool</code>实现LRU算法
 */
public class AccessRecord implements Comparable<AccessRecord> {

	private PageId pid;

	private long lastAccessTime;

	private TransactionId tid;

	public AccessRecord(TransactionId tid, PageId pid, long lastAccessTime) {
		this.pid = pid;
		this.lastAccessTime = lastAccessTime;
		this.tid = tid;
	}

	public TransactionId getTid() {
		return tid;
	}

	public PageId getPid() {
		return pid;
	}

	public long getLastAccessTime() {
		return lastAccessTime;
	}

	public void setLastAccessTime(long lastAccessTime) {
		this.lastAccessTime = lastAccessTime;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof AccessRecord) {
			AccessRecord other = (AccessRecord) obj;
			return this.pid.equals(other.getPid());
		}
		return false;
	}

	@Override
	public int compareTo(AccessRecord o) {
		// TODO Auto-generated method stub
		if (this.pid.equals(o.getPid())) {
			return 0;
		}
		return this.lastAccessTime - o.getLastAccessTime() > 0 ? 1 : -1;
	}

	@Override
	public int hashCode() {
		return 31 * pid.hashCode();
	}
}
