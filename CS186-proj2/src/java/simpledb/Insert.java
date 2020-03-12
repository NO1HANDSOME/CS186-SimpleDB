package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

	private static final long serialVersionUID = 1L;

	private TransactionId t;

	/* read from */
	private DbIterator child;

	/* insert to */
	private int tableid;

	/* 用于fetchNext */
	private boolean isDone;

	private TupleDesc td;

	/**
	 * Constructor.
	 * 
	 * @param t       The transaction running the insert.
	 * @param child   The child operator from which to read tuples to be inserted.
	 * @param tableid The table in which to insert tuples.
	 * @throws DbException if TupleDesc of child differs from table into which we
	 *                     are to insert.
	 */
	public Insert(TransactionId t, DbIterator child, int tableid) throws DbException {
		// some code goes here
		TupleDesc temp = child.getTupleDesc();
		if (!Database.getCatalog().getTupleDesc(tableid).equals(temp))
			throw new DbException("TupleDesc differs.");
		this.td = new TupleDesc(new Type[] { Type.INT_TYPE });
		this.t = t;
		this.child = child;
		this.tableid = tableid;
		this.isDone = false;
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		return td;
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		child.open();
		super.open();
	}

	public void close() {
		// some code goes here
		child.close();
		super.close();
	}

	/* 对于insert操作，rewind似乎没什么意义 */
	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		child.rewind();
		isDone = false;
	}

	/**
	 * Inserts tuples read from child into the tableid specified by the constructor.
	 * It returns a one field tuple containing the number of inserted records.
	 * Inserts should be passed through BufferPool. An instances of BufferPool is
	 * available via Database.getBufferPool(). Note that insert DOES NOT need check
	 * to see if a particular tuple is a duplicate before inserting it.(不需要查重)
	 * 
	 * @return A 1-field tuple containing the number of inserted records, or null if
	 *         called more than once.
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (isDone)
			return null;
		int count = 0;
		while (child.hasNext()) {
			Tuple tuple = child.next();
			try {
				Database.getBufferPool().insertTuple(t, tableid, tuple);
				count++;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		Tuple res = new Tuple(td);
		res.setField(0, new IntField(count));
		isDone = true;
		return res;
	}

	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		return new DbIterator[] { child };
	}

	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
		child = children[0];
	}
}
