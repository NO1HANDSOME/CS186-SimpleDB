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
	 * 从磁盘读取指定page,形成一份拷贝.
	 * 
	 * @param pid 指定的PageId
	 * @return 指定的Page
	 */
	public Page readPage(PageId pid) {
		// some code goes here
		byte[] data = new byte[BufferPool.PAGE_SIZE];
		Page page = null;
		// 计算偏移
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
		// 得到page的数据
		byte[] data = page.getPageData();
		// 计算偏移
		int offset = page.getId().pageNumber() * BufferPool.PAGE_SIZE;
		// 通过RandomAccessFile将page写入到磁盘文件
		RandomAccessFile raf = new RandomAccessFile(getFile(), "rw");// 注意切换模式
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
		boolean isOk = false;// 用来标记是否找到可以插入的page
		int tableId = getId();
		int pno = 0;
		HeapPage page = null;
		while (pno < numPages) {// 寻找有位置插入的page
			PageId pid = new HeapPageId(tableId, pno);
			// 通过缓冲区得到有位置插入的page
			page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
			if (page.getNumEmptySlots() > 0) {
				isOk = true;
				break;
			}
			pno++;// 到下一页
		}
		if (!isOk) {// 所有page都满了，在磁盘创建新的page，然后通过BufferPool得到空的page
			HeapPageId pid = new HeapPageId(tableId, pno);
			HeapPage ePage = new HeapPage(pid, HeapPage.createEmptyPageData());
			writePage(ePage);
			page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
			numPages++;
		}
		page.insertTuple(t);
		page.markDirty(true, tid);
		ArrayList<Page> list = new ArrayList<Page>();// 包含有被修改的page
		list.add(page);
		return list;
	}

	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// some code goes here
		// not necessary for proj1
		// 找到待删除page
		PageId pid = t.getRecordId().getPageId();
		if (pid.getTableId() != getId())
			throw new DbException("待删除Tuple不在这个Table上。");
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
