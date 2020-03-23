package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

	private static final long serialVersionUID = 1L;

	private TransactionId t;

	private DbIterator child;

	/* 用于fetchNext */
	private boolean isDone;

	private TupleDesc td;

	/**
	 * Constructor specifying the transaction that this delete belongs to as well as
	 * the child to read from.
	 * 
	 * @param t     The transaction this delete runs in
	 * @param child The child operator from which to read tuples for deletion
	 */
	public Delete(TransactionId t, DbIterator child) {
		// some code goes here
		this.td = new TupleDesc(new Type[] { Type.INT_TYPE });
		this.t = t;
		this.child = child;
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

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		child.rewind();
		isDone = false;
	}

	/**
	 * Deletes tuples as they are read from the child operator. Deletes are
	 * processed via the buffer pool (which can be accessed via the
	 * Database.getBufferPool() method.
	 * 
	 * @return A 1-field tuple containing the number of deleted records.
	 * @see Database#getBufferPool
	 * @see BufferPool#deleteTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (isDone)
			return null;
		// 下面删除的逻辑也可以放在open中执行
		int count = 0;
		while (child.hasNext()) {
			Tuple tuple = child.next();
			Database.getBufferPool().deleteTuple(t, tuple);
			count++;
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
