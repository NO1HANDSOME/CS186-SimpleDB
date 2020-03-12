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

	private boolean isOpen;

	/**
	 * ����һ��ָ���ı�id������һ���ñ�ĵ�����.�ܹ��������ʸñ������Ԫ��.
	 * 
	 * @param tid     �����Ψһ��ʶ
	 * @param tableId ��Ҫ�����ı��Ψһ��ʶ
	 */
	public HeapFileIterator(TransactionId tid, int tableId) {
		this.tid = tid;// // �ٶ��ϲ㴫�ݵ�tid��һ���Ϸ���
		this.tableId = tableId;// �ٶ��ϲ㴫�ݵ�tableId��һ���Ϸ���
		this.tuplesInPage = null;
		this.isOpen = false;
		HeapFile file = (HeapFile) Database.getCatalog().getDbFile(tableId);
		this.numPages = file.numPages();
		this.curPageNo = 0;
	}

	/**
	 * ������Դ
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
		if (!isOpen)// ������ûopen
			return false;// ��������ΪӦ�����쳣���������쳣�͹����˵�Ԫ���ԣ��÷���false
		if (tuplesInPage.hasNext())
			return true;
		while (curPageNo < numPages - 1) {// ��ҳ
			tuplesInPage = getTupleIterator(++curPageNo);
			if (tuplesInPage != null && tuplesInPage.hasNext()) {// �ҵ���Ԫ�����һҳ
				return true;
			}
		}
		return false;
	}

	/**
	 * ������һ��Ԫ�飬�����ǰ��page�Ѿ����꣬��Ҫ��ת����һ��page���ж�ȡ��
	 */
	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		if (!isOpen)// ������ûopen
			throw new NoSuchElementException("Iterator is closed.");
		if (!tuplesInPage.hasNext()) {// ���Է�ҳ
			while (curPageNo < numPages - 1) {// ��ҳ
				tuplesInPage = getTupleIterator(++curPageNo);
				if (tuplesInPage != null && tuplesInPage.hasNext()) {// �ҵ���Ԫ�����һҳ
					break;
				}
			}
		}
		return tuplesInPage.next();// ������ʱ��tuplesInPage��û��Ԫ�飬��˵�����Ѿ���������
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
