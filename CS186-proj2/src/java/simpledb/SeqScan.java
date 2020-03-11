package simpledb;

import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

	private static final long serialVersionUID = 1L;

	private DbFileIterator dfileItrator;

	private TransactionId tid;

	private int tableId;

	private String tableName;

	private String alias;

	private TupleDesc td;

	private boolean isOpen = false;

	/**
	 * Creates a sequential scan over the specified table as a part of the specified
	 * transaction.
	 * 
	 * @param tid        The transaction this scan is running as a part of.
	 * @param tableid    the table to scan.
	 * @param tableAlias the alias of this table (needed by the parser); the
	 *                   returned tupleDesc should have fields with name
	 *                   tableAlias.fieldName (note: this class is not responsible
	 *                   for handling a case where tableAlias or fieldName are null.
	 *                   It shouldn't crash if they are, but the resulting name can
	 *                   be null.fieldName, tableAlias.null, or null.null).
	 */
	public SeqScan(TransactionId tid, int tableid, String tableAlias) {
		// some code goes here
		this.tid = tid;
		this.tableId = tableid;
		this.alias = tableAlias;
		this.td = Database.getCatalog().getTupleDesc(tableId);
		this.tableName = Database.getCatalog().getTableName(tableId);
		this.dfileItrator = Database.getCatalog().getDbFile(tableId).iterator(tid);
	}

	/**
	 * @return return the table name of the table the operator scans. This should be
	 *         the actual name of the table in the catalog of the database
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @return Return the alias of the table this operator scans.
	 */
	public String getAlias() {
		// some code goes here
		return alias;
	}

	/**
	 * Reset the tableid, and tableAlias of this operator.
	 * 
	 * @param tableid    the table to scan.
	 * @param tableAlias the alias of this table (needed by the parser); the
	 *                   returned tupleDesc should have fields with name
	 *                   tableAlias.fieldName (note: this class is not responsible
	 *                   for handling a case where tableAlias or fieldName are null.
	 *                   It shouldn't crash if they are, but the resulting name can
	 *                   be null.fieldName, tableAlias.null, or null.null).
	 * @throws TransactionAbortedException
	 * @throws DbException
	 */
	public void reset(int tableid, String tableAlias) throws DbException, TransactionAbortedException {
		// some code goes here
		isOpen = false;
		this.tableId = tableid;
		this.alias = tableAlias;
	}

	public SeqScan(TransactionId tid, int tableid) {
		this(tid, tableid, Database.getCatalog().getTableName(tableid));
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		dfileItrator.open();
		isOpen = true;
	}

	/**
	 * Returns the TupleDesc with field names from the underlying HeapFile, prefixed
	 * with the tableAlias string from the constructor. This prefix becomes useful
	 * when joining tables containing a field(s) with the same name.
	 * 
	 * @return the TupleDesc with field names from the underlying HeapFile, prefixed
	 *         with the tableAlias string from the constructor.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		int len = td.numFields();
		Type[] typeAr = new Type[len];
		String[] nameAr = new String[len];
		for (int i = 0; i < len; i++) {
			typeAr[i] = td.getFieldType(i);
			/************************************************************/
			String prefix = getAlias() == null ? "null." : getAlias() + ".";
			String fieldName = td.getFieldName(i);
			fieldName = fieldName == null ? "null" : fieldName;
			/***********************************************************/
			nameAr[i] = prefix + fieldName;
		}
		return new TupleDesc(typeAr, nameAr);
	}

	public boolean hasNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (!isOpen)
			throw new IllegalStateException("SeqScan is colsed.");
		return dfileItrator.hasNext();
	}

	public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
		// some code goes here
		if (!isOpen)
			throw new IllegalStateException("SeqScan is colsed.");
		return dfileItrator.next();
	}

	public void close() {
		// some code goes here
		dfileItrator.close();
		isOpen = false;
	}

	public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
		// some code goes here
		dfileItrator.rewind();
	}
}
