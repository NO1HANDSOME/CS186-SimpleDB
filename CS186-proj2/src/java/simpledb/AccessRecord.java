package simpledb;

/**
 * ��<code>BufferPool</code>����ĳ��pageʱ������<code>BufferPool</code>�и���һ��<code>AccessRecord</code>
 * <p>
 * <code>AccessRecord</code>�洢����һ�η���ʱ�����Ϣ����������<code>BufferPool</code>ʵ��LRU�㷨
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
