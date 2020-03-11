package simpledb;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

	private static final long serialVersionUID = 1L;

	/* 聚合前的DbIterator */
	private DbIterator child;

	/* 聚合前的元组描述 */
	private TupleDesc otd;

	/* 聚合后的元组描述 */
	private TupleDesc atd;

	/* 聚合结果 */
	private DbIterator agRes;

	private int afield;

	private int gfield;

	private Aggregator.Op aop;

	/**
	 * Constructor.
	 * 
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an {@link IntAggregator} or {@link StringAggregator} to help you
	 * with your implementation of readNext().
	 * 
	 * 
	 * @param child  The DbIterator that is feeding us tuples.
	 * @param afield The column over which we are computing an aggregate.
	 * @param gfield The column over which we are grouping the result, or -1 if
	 *               there is no grouping
	 * @param aop    The aggregation operator to use
	 */
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		// some code goes here
		this.agRes = null;
		this.child = child;
		this.afield = afield;
		this.gfield = gfield;
		this.aop = aop;
		this.otd = child.getTupleDesc();
	}

	/**
	 * @return If this aggregate is accompanied by a groupby, return the groupby
	 *         field index in the <b>INPUT</b> tuples. If not, return
	 *         {@link simpledb.Aggregator#NO_GROUPING}
	 */
	public int groupField() {
		// some code goes here
		return gfield;
	}

	/**
	 * @return If this aggregate is accompanied by a group by, return the name of
	 *         the groupby field in the <b>OUTPUT</b> tuples If not, return null;
	 */
	public String groupFieldName() {
		// some code goes here
		return gfield == Aggregator.NO_GROUPING ? null : atd.getFieldName(1);
	}

	/**
	 * @return the aggregate field
	 */
	public int aggregateField() {
		// some code goes here
		return afield;
	}

	/**
	 * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
	 */
	public String aggregateFieldName() {
		// some code goes here
		return gfield == Aggregator.NO_GROUPING ? atd.getFieldName(0) : atd.getFieldName(1);
	}

	/**
	 * @return return the aggregate operator
	 */
	public Aggregator.Op aggregateOp() {
		// some code goes here
		return aop;
	}

	public static String nameOfAggregatorOp(Aggregator.Op aop) {
		return aop.toString();
	}

	public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
		// some code goes here
		// 准备聚合器
		Aggregator aggregator = getAggregator();
		// 开始聚合
		child.open();
		while (child.hasNext()) {
			Tuple t = child.next();
			aggregator.mergeTupleIntoGroup(t);
		}
		agRes = aggregator.iterator();
		// 聚合完成
		if (atd == null)
			atd = agRes.getTupleDesc();// 这样子做，先open再调getTupleDesc方法就不会重复计算
		agRes.open();
		super.open();
	}

	/**
	 * 获得聚合器
	 */
	private Aggregator getAggregator() {
		Aggregator aggregator = null;
		Type gType = null;//无分组的话，gType就设置为null
		if (gfield != Aggregator.NO_GROUPING)
			gType = otd.getFieldType(gfield);
		Type aType = otd.getFieldType(afield);
		if (aType.equals(Type.INT_TYPE)) {
			aggregator = new IntegerAggregator(gfield, gType, afield, aop);
		} else if (aType.equals(Type.STRING_TYPE)) {
			aggregator = new StringAggregator(gfield, gType, afield, aop);
		}
		return aggregator;
	}

	/**
	 * Returns the next tuple. If there is a group by field, then the first field is
	 * the field by which we are grouping, and the second field is the result of
	 * computing the aggregate, If there is no group by field, then the result tuple
	 * should contain one field representing the result of the aggregate. Should
	 * return null if there are no more tuples.
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (agRes.hasNext())
			return agRes.next();
		return null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		agRes.rewind();// 我觉得child不需要rewind
	}

	/**
	 * Returns the TupleDesc of this Aggregate. If there is no group by field, this
	 * will have one field - the aggregate column. If there is a group by field, the
	 * first field will be the group by field, and the second will be the aggregate
	 * value column.
	 * 
	 * The name of an aggregate column should be informative. For example:
	 * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are given
	 * in the constructor, and child_td is the TupleDesc of the child iterator.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		if (atd != null) {
			return atd;
		}
		Type[] types;
		String[] names;
		String aName = otd.getFieldName(afield);
		if (gfield == Aggregator.NO_GROUPING) {
			types = new Type[] { Type.INT_TYPE };
			names = new String[] { aName };
		} else {
			types = new Type[] { otd.getFieldType(gfield), Type.INT_TYPE };
			names = new String[] { otd.getFieldName(gfield), aName };
		}
		atd = new TupleDesc(types, names);
		return atd;
	}

	public void close() {
		// some code goes here
		child.close();
		agRes.close();
		super.close();
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
