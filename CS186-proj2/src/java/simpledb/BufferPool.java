package simpledb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 */
public class BufferPool {
	/** Bytes per page, including header. */
	public static final int PAGE_SIZE = 4096;

	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */
	public static final int DEFAULT_PAGES = 50;

	/**
	 * BufferPool容量
	 */
	private final int PAGES_NUM;

	private HashMap<PageId, Page> id2page;

	/**
	 * BufferPool访问page的记录，按发生时间进行升序排序
	 */
	private TreeSet<AccessRecord> id2record;

	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		// some code goes here
		PAGES_NUM = numPages;
		id2page = new HashMap<PageId, Page>(PAGES_NUM);
		id2record = new TreeSet<AccessRecord>();
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire a
	 * lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is present,
	 * it should be returned. If it is not present, it should be added to the buffer
	 * pool and returned(在proj2中，缓冲池中需要实现替换策略). If there is insufficient space in
	 * the buffer pool, an page should be evicted and the new page should be added
	 * in its place.
	 *
	 * @param tid  the ID of the transaction requesting the page.在pro1中，事务id似乎不需要用到.
	 * @param pid  the ID of the requested page
	 * @param perm the requested permissions on the page
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		// some code goes here
		// 记录访问
		recordAccess(tid, pid);
		// 先从缓冲区中检索
		if (id2page.containsKey(pid))
			return id2page.get(pid);
		int curSize = id2page.size();
		if (curSize == PAGES_NUM)// 执行缓冲区替换策略
			evictPage();
		// 从磁盘检索表
		int tableid = pid.getTableId();
		DbFile table = Database.getCatalog().getDbFile(tableid);
		// 从磁盘文件检索页，然后读取该页
		Page page = table.readPage(pid);
		// 将页加入缓冲区后返回该页
		insertPage(pid, page);
		return page;
	}

	/**
	 * 用来记录指定page最近一次的访问时间
	 */
	private void recordAccess(TransactionId tid, PageId pid) {
		long curTime = System.currentTimeMillis();
		AccessRecord record = new AccessRecord(tid, pid, curTime);
		id2record.add(record);// O(logn)
		// System.out.println("add record:" + record + " " + record.getPid());
	}

	/**
	 * 快速将页添加进缓冲区
	 * 
	 * @param pid
	 * @param page
	 */
	private void insertPage(PageId pid, Page page) {
		id2page.put(pid, page);
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result in
	 * wrong behavior. Think hard about who needs to call this and why, and why they
	 * can run the risk of calling it.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 * @param pid the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for proj1
	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for proj1
	}

	/** Return true if the specified transaction has a lock on the specified page */
	public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for proj1
		return false;
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid    the ID of the transaction requesting the unlock
	 * @param commit a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// some code goes here
		// not necessary for proj1
	}

	/**
	 * Add a tuple to the specified table behalf of transaction tid. Will acquire a
	 * write lock on the page the tuple is added to(Lock acquisition is not needed
	 * for lab2). May block if the lock cannot be acquired.(触发锁)
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling their
	 * markDirty bit, and updates cached versions of any pages that have been
	 * dirtied so that future requests see up-to-date pages.(更新缓存中的page)
	 *
	 *
	 * @param tid     the transaction adding the tuple
	 * @param tableId the table to add the tuple to
	 * @param t       the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for proj1
		DbFile file = Database.getCatalog().getDbFile(tableId);
		file.insertTuple(tid, t);// 注意markDirty等操作会在下层执行
	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write lock on
	 * the page the tuple is removed from. May block if the lock cannot be
	 * acquired.(触发锁)
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling their
	 * markDirty bit. Does not need to update cached versions of any pages that have
	 * been dirtied, as it is not possible that a new page was created during the
	 * deletion (note difference from addTuple)(不明白什么意思).
	 *
	 * @param tid the transaction adding the tuple.
	 * @param t   the tuple to add
	 */
	public void deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// some code goes here
		// not necessary for proj1
		int tableId = t.getRecordId().getPageId().getTableId();
		DbFile file = Database.getCatalog().getDbFile(tableId);
		file.deleteTuple(tid, t);// 注意markDirty等操作会在下层执行
	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes
	 * dirty data to disk so will break simpledb if running in NO STEAL mode.
	 * <p>
	 * 测试用的方法，不需要太过关注，实现功能即可。
	 */
	public synchronized void flushAllPages() throws IOException {
		// some code goes here
		// not necessary for proj1
		Iterator<AccessRecord> iterator = id2record.iterator();
		while (iterator.hasNext()) {
			flushPage(iterator.next().getPid());
		}
	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in its
	 * cache.
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// not necessary for proj1
		id2record.pollFirst();
		id2page.remove(pid);
	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		// some code goes here
		// not necessary for proj1
		DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
		Page page = id2page.get(pid);
		if (page.isDirty() != null)// dirty才flush
			file.writePage(page);
		page.markDirty(false, null);
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for proj1
		Iterator<AccessRecord> iterator = id2record.iterator();
		while (iterator.hasNext()) {
			AccessRecord ar = iterator.next();
			if (ar.getTid().equals(tid))
				flushPage(ar.getPid());
		}
	}

	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure
	 * dirty pages are updated on disk.
	 * <p>
	 * Algorithm:LRU
	 */
	private synchronized void evictPage() throws DbException {
		// some code goes here
		// not necessary for proj1
		// 得到距离上次访问最长的pid
		PageId pid = id2record.first().getPid();// O(1)
		if (id2page.get(pid) == null) {
			System.out.println("ERROR");
		}
		// flush
		try {
			flushPage(pid);// 会判断是否dirty，dirty才会flushPage
		} catch (IOException e) {
			e.printStackTrace();
		}
		// 从BufferPool中移除它
		discardPage(pid);
	}

}
