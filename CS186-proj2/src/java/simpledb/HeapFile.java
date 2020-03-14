package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	private File file;
	private TupleDesc td;
	private int numPages;

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f the file that stores the on-disk backing store for this heap file.
	 */
	public HeapFile(File f, TupleDesc td) {
		// some code goes here
		this.file = f;
		this.td = td;
		numPages = (int) Math.ceil((f.length() * 1.0 / BufferPool.PAGE_SIZE));
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		// some code goes here
		return file;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note: you
	 * will need to generate this tableid somewhere ensure that each HeapFile has a
	 * "unique id," and that you always return the same value for a particular
	 * HeapFile. We suggest hashing the absolute file name of the file underlying
	 * the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		// some code goes here
		return file.getAbsolutePath().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return td;
	}

	/**
	 * �Ӵ��̶�ȡָ��page,�γ�һ�ݿ���.
	 * 
	 * @param pid ָ����PageId
	 * @return ָ����Page
	 */
	public Page readPage(PageId pid) {
		// some code goes here
		byte[] data = new byte[BufferPool.PAGE_SIZE];
		Page page = null;
		// ����ƫ��
		int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(getFile(), "r");
			raf.seek(offset);
			raf.read(data, 0, data.length);
			page = new HeapPage((HeapPageId) pid, data);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (raf != null)
					raf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return page;
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for proj1
		// �õ�page������
		byte[] data = page.getPageData();
		// ����ƫ��
		int offset = page.getId().pageNumber() * BufferPool.PAGE_SIZE;
		// ͨ��RandomAccessFile��pageд�뵽�����ļ�
		RandomAccessFile raf = new RandomAccessFile(getFile(), "rw");// ע���л�ģʽ
		raf.seek(offset);
		raf.write(data, 0, data.length);
		raf.close();
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		// some code goes here
		return numPages;
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for proj1
		boolean isOk = false;// ��������Ƿ��ҵ����Բ����page
		int tableId = getId();
		int pno = 0;
		HeapPage page = null;
		while (pno < numPages) {// Ѱ����λ�ò����page
			PageId pid = new HeapPageId(tableId, pno);
			// ͨ���������õ���λ�ò����page
			page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
			if (page.getNumEmptySlots() > 0) {
				isOk = true;
				break;
			}
			pno++;// ����һҳ
		}
		if (!isOk) {// ����page�����ˣ��ڴ��̴����µ�page��Ȼ��ͨ��BufferPool�õ��յ�page
			HeapPageId pid = new HeapPageId(tableId, pno);
			HeapPage ePage = new HeapPage(pid, HeapPage.createEmptyPageData());
			writePage(ePage);
			page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
			numPages++;
		}
		page.insertTuple(t);
		page.markDirty(true, tid);
		ArrayList<Page> list = new ArrayList<Page>();// �����б��޸ĵ�page
		list.add(page);
		return list;
	}

	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// some code goes here
		// not necessary for proj1
		// �ҵ���ɾ��page
		PageId pid = t.getRecordId().getPageId();
		if (pid.getTableId() != getId())
			throw new DbException("��ɾ��Tuple�������Table�ϡ�");
		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
		page.deleteTuple(t);
		page.markDirty(true, tid);
		return page;
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		// some code goes here
		return new HeapFileIterator(tid, getId());
	}

}
