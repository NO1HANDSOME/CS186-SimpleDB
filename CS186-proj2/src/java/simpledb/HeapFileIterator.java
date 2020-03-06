package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {

	/**
	 * ��������ǰ���ڵı��Ψһ��ʶ
	 */
	private int tableId;
	/**
	 * ��������ǰ���ڵı��ҳ��
	 */
	private int numPages;
	/**
	 * ��������ǰ���ڵ�ҳ�ı��
	 */
	private int curPageNo;
	/**
	 * ��ǰ����ҳ������Ԫ��ĵ�����
	 */
	private Iterator<Tuple> tuplesInPage;// ��ʼ�Ƕ�����Page���󣬲ο����޸�ΪTuple������
	/**
	 * ��ǰ�����Ψһ��ʶ
	 */
	private TransactionId tid;

	/**
	 * ����һ��ָ���ı�id������һ���ñ�ĵ�����.�ܹ��������ʸñ������Ԫ��.
	 * 
	 * @param tid     �����Ψһ��ʶ
	 * @param tableId ��Ҫ�����ı��Ψһ��ʶ
	 */
	public HeapFileIterator(TransactionId tid, int tableId) {
		this.tid = tid;// // �ٶ��ϲ㴫�ݵ�tid��һ���Ϸ���
		this.tableId = tableId;// �ٶ��ϲ㴫�ݵ�tableId��һ���Ϸ���
		curPageNo = -1;
		numPages = -1;
		tuplesInPage = null;
	}

	/**
	 * ������Դ
	 */
	@Override
	public void open() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		HeapFile file = (HeapFile) Database.getCatalog().getDbFile(tableId);
		numPages = file.numPages();
		curPageNo = 0;
		tuplesInPage = getTupleIterator(curPageNo);
	}

	private Iterator<Tuple> getTupleIterator(int pageNo) throws TransactionAbortedException, DbException {
		if (pageNo >= numPages)
			throw new NoSuchElementException("���ʳ���ʵ��ҳ����Χ.");
		PageId pid = new HeapPageId(tableId, pageNo);
		// ��ͨ��BufferPool��ö�Ӧ��Page����
		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
		// �õ�Page����󣬻��Tuple�ĵ�����
		Iterator<Tuple> iterator = page.iterator();
		return iterator;
	}

	/**
	 * �ж��Ƿ�����һ��Ԫ��ɷ���
	 */
	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if (tuplesInPage == null)// ������ûopen
			return false;
		if (!tuplesInPage.hasNext() && curPageNo >= numPages - 1)
			return false;
		return true;
	}

	/**
	 * ������һ��Ԫ��
	 */
	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		if (tuplesInPage == null)// ������ûopen
			throw new NoSuchElementException("Iterator is null.");
		if (!tuplesInPage.hasNext())
			tuplesInPage = getTupleIterator(++curPageNo);
		return tuplesInPage.next();
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		open();
	}

	@Override
	public void close() {
		tuplesInPage = null;
	}

}
