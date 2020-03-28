package simpledb;

import java.util.*;

import simpledb.Predicate.Op;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

	/**
	 * BNL算法中cache的大小
	 */
	public static final int BLOCK_MEMROY = 131072;

	private static final long serialVersionUID = 1L;

	private int len1;

	private int len2;

	private JoinPredicate p;

	private DbIterator child1;

	private DbIterator child2;

	private TupleDesc td;

	private DbIterator joinRes;

	/**
	 * Constructor. Accepts to children to join and the predicate to join them on
	 * 
	 * @param p      The predicate to use to join the children
	 * @param child1 Iterator for the left(outer) relation to join
	 * @param child2 Iterator for the right(inner) relation to join
	 */
	public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
		// some code goes here
		this.len1 = child1.getTupleDesc().numFields();
		this.len2 = child2.getTupleDesc().numFields();
		this.p = p;
		this.child1 = child1;
		this.child2 = child2;
		this.td = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());// 元组描述不去重
		joinRes = null;
	}

	public JoinPredicate getJoinPredicate() {
		// some code goes here
		return p;
	}

	/**
	 * @return the field name of join field1. Should be quantified by alias or table
	 *         name.
	 */
	public String getJoinField1Name() {
		// some code goes here
		return child1.getTupleDesc().getFieldName(p.getField1());
	}

	/**
	 * @return the field name of join field2. Should be quantified by alias or table
	 *         name.
	 */
	public String getJoinField2Name() {
		// some code goes here
		return child2.getTupleDesc().getFieldName(p.getField2());
	}

	/**
	 * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
	 *      implementation logic.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return td;
	}

	public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
		// some code goes here
		super.open();
		child1.open();
		child2.open();
		joinRes = blockNestedLoopJoin();
		joinRes.open();
	}

	/**
	 * 对左表做缓存<br/>
	 * 相对于普通的NL算法，能够减少内表的磁盘IO次数
	 * 
	 * @throws TransactionAbortedException
	 * @throws DbException
	 */
	private DbIterator blockNestedLoopJoin() throws DbException, TransactionAbortedException {
		List<Tuple> list = new LinkedList<Tuple>();
		int cacheSize = BLOCK_MEMROY / child1.getTupleDesc().getSize();
		ArrayList<Tuple> cache = new ArrayList<Tuple>();
		while (child1.hasNext()) {
			Tuple tuple = child1.next();
			cache.add(tuple);
			if (cache.size() == cacheSize) {
				child2.rewind();
				while (child2.hasNext()) {
					int index = 0;
					Tuple right = child2.next();
					while (index < cache.size()) {
						Tuple left = cache.get(index++);
						if (p.filter(left, right)) {
							Tuple newTuple = mergeTuples(left, right);
							list.add(newTuple);
						}
					}
				}
				// 清空cache
				cache.clear();
			}
		}
		// 处理缓冲区剩余的左表元组
		if (cache.size() != 0) {
			child2.rewind();
			while (child2.hasNext()) {
				int index = 0;
				Tuple right = child2.next();
				while (index < cache.size()) {
					Tuple left = cache.get(index++);
					if (p.filter(left, right)) {
						Tuple newTuple = mergeTuples(left, right);
						list.add(newTuple);
					}
				}
			}
		}
		return new TupleIterator(td, list);
	}

	private DbIterator hashJoin() {
		return null;
	}

	/**
	 * NL
	 */
	private TupleIterator nestedLoopJoin() throws DbException, TransactionAbortedException {
		List<Tuple> list = new LinkedList<Tuple>();
		while (child1.hasNext()) {
			Tuple out = child1.next();
			while (child2.hasNext()) {
				Tuple in = child2.next();
				if (p.filter(out, in)) {
					Tuple newTuple = mergeTuples(out, in);
					list.add(newTuple);
				}
			}
			child2.rewind();// 低效
		}
		return new TupleIterator(td, list);
	}

//	private TupleIterator HashJoin() throws DbException, TransactionAbortedException {
//		List<Tuple> tuples = new ArrayList<>();
//		// 根据要join的Id进行分组
//		HashMap<Integer, List<Tuple>> leftMap = new HashMap<>(); // 一个桶对应分组JoinId相同的Tuple 现在的桶力度特别小，最多情况为重复的值为0则桶数量=元组数量
//		HashMap<Integer, List<Tuple>> rightMap = new HashMap<>();
//		while (child1.hasNext()) {
//			Tuple temp = child1.next();
//			int hashcode = temp.getField(p.getField1()).hashCode();
//			List<Tuple> tempTuples = leftMap.get(hashcode);
//			if (tempTuples == null) {
//				tempTuples = new ArrayList<>();
//			}
//			tempTuples.add(temp);
//			leftMap.put(hashcode, tempTuples);
//		}
//		while (child2.hasNext()) {
//			Tuple temp = child2.next();
//			int hashcode = temp.getField(p.getField2()).hashCode();
//			List<Tuple> tempTuples = rightMap.get(hashcode);
//			if (tempTuples == null) {
//				tempTuples = new ArrayList<>();
//			}
//			tempTuples.add(temp);
//			rightMap.put(hashcode, tempTuples);
//		}
//		// THEN Nest LOOP JOIN
//		leftMap.forEach((k, v) -> {
//			List<Tuple> rightTuples = rightMap.get(k);
//			if (rightTuples != null && rightTuples.size() > 0) {
//				Iterator<Tuple> leftTupleIT = v.iterator();
//				Tuple t1, t2;
//				while (leftTupleIT.hasNext()) {
//					t1 = leftTupleIT.next();
//					Iterator<Tuple> rightTupleIT = rightTuples.iterator();
//					while (rightTupleIT.hasNext()) {
//						t2 = rightTupleIT.next();
//						if (t1 != null && t2 != null && p.filter(t1, t2)) {
//							tuples.add(mergeTuples(t1, t2));
//						}
//					}
//
//				}
//			}
//		});
//
//		return new TupleIterator(getTupleDesc(), tuples);
//	}

	/* 联接两个元组,没有去重 */
	private Tuple mergeTuples(Tuple out, Tuple in) {
		Tuple newTuple = new Tuple(td);
		for (int i = 0; i < len1; i++) {
			Field f = out.getField(i);
			newTuple.setField(i, f);
		}
		for (int i = 0; i < len2; i++) {
			Field f = in.getField(i);
			newTuple.setField(len1 + i, f);
		}
		return newTuple;
	}

	public void close() {
		// some code goes here
		joinRes.close();
		child2.close();
		child1.close();
		super.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		child1.rewind();
		child2.rewind();
		joinRes.rewind();
	}

	/**
	 * Returns the next tuple generated by the join, or null if there are no more
	 * tuples. Logically, this is the next tuple in r1 cross r2 that satisfies the
	 * join predicate. There are many possible implementations; the simplest is a
	 * nested loops join.
	 * <p>
	 * Note that the tuples returned from this particular implementation of Join are
	 * simply the concatenation of joining tuples from the left and right relation.
	 * Therefore, if an equality predicate is used there will be two copies of the
	 * join attribute in the results. (Removing such duplicate columns can be done
	 * with an additional projection operator if needed.)
	 * <p>
	 * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6}, joined
	 * on equality of the first column, then this returns {1,2,3,1,5,6}.
	 * 
	 * @return The next matching tuple.
	 * @see JoinPredicate#filter
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		while (joinRes.hasNext()) {
			Tuple temp = joinRes.next();
			return temp;
		}
		return null;
	}

	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		return new DbIterator[] { child1, child2 };
	}

	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
		child1 = children[0];
		child2 = children[1];
	}

}
