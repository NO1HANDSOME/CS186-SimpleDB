package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {

	/**
	 * 迭代器当前所在的表的唯一标识
	 */
	private int tableId;
	/**
	 * 迭代器当前所在的表的页数
	 */
	private int numPages;
	/**
	 * 迭代器当前所在的页的编号
	 */
	private int curPageNo;
	/**
	 * 当前所在页的所有元组的迭代器
	 */
	private Iterator<Tuple> tuplesInPage;// 开始是定义了Page对象，参考后修改为Tuple迭代器
	/**
	 * 当前事务的唯一标识
	 */
	private TransactionId tid;

	private boolean isOpen;

	/**
	 * 根据一个指定的表id，建立一个该表的迭代器.能够迭代访问该表的所有元组.
	 * 
	 * @param tid     事务的唯一标识
	 * @param tableId 需要迭代的表的唯一标识
	 */
	public HeapFileIterator(TransactionId tid, int tableId) {
		this.tid = tid;// // 假定上层传递的tid是一定合法的
		this.tableId = tableId;// 假定上层传递的tableId是一定合法的
		this.tuplesInPage = null;
		this.isOpen = false;
		HeapFile file = (HeapFile) Database.getCatalog().getDbFile(tableId);
		this.numPages = file.numPages();
		this.curPageNo = 0;
	}

	/**
	 * 加载资源
	 */
	@Override
	public void open() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		tuplesInPage = getTupleIterator(curPageNo);
		isOpen = true;
	}

	private Iterator<Tuple> getTupleIterator(int pageNo) throws TransactionAbortedException, DbException {
		if (pageNo >= numPages)
			return null;
		PageId pid = new HeapPageId(tableId, pageNo);
		// 先通过BufferPool获得对应的Page对象
		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
		// 得到Page对象后，获得Tuple的迭代器
		Iterator<Tuple> iterator = page.iterator();
		return iterator;
	}

	/**
	 * 判断是否还有下一个元组可访问
	 */
	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if (!isOpen)// 迭代器没open
			return false;// 这里我认为应该抛异常，但是抛异常就过不了单元测试，得返回false
		if (tuplesInPage.hasNext())
			return true;
		while (curPageNo < numPages - 1) {// 翻页
			tuplesInPage = getTupleIterator(++curPageNo);
			if (tuplesInPage != null && tuplesInPage.hasNext()) {// 找到有元组的下一页
				return true;
			}
		}
		return false;
	}

	/**
	 * 访问下一个元组，如果当前的page已经读完，需要跳转到下一个page进行读取。
	 */
	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		if (!isOpen)// 迭代器没open
			throw new NoSuchElementException("Iterator is closed.");
		if (!tuplesInPage.hasNext()) {// 尝试翻页
			while (curPageNo < numPages - 1) {// 翻页
				tuplesInPage = getTupleIterator(++curPageNo);
				if (tuplesInPage != null && tuplesInPage.hasNext()) {// 找到有元组的下一页
					break;
				}
			}
		}
		return tuplesInPage.next();// 如果这个时候tuplesInPage里没有元组，则说明表已经访问完了
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		open();
	}

	@Override
	public void close() {
		tuplesInPage = null;
		isOpen = false;
	}

}
