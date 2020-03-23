package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of
 * IntFields.(注意只支持一个字段分组、一个字段聚合)
 */
public class IntegerAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;

	/* 分组字段 */
	private int gbIndex;

	/* 聚合字段 */
	private int agIndex;

	/* 聚合运算符 */
	private Op op;

	/* 聚合结果 */
	private HashMap<Field, Integer> agRes;// 根据分组字段的值来映射不同结果

	/* 聚合后的元组描述 */
	private TupleDesc agTd;

	/* avg运算时用到 */
	private HashMap<Field, Integer[]> avgHelper;

	/* 是否已取得agTd */
	private boolean hasagTd;

	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield     the 0-based index of the group-by field in the tuple, or
	 *                    NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
	 *                    null if there is no grouping
	 * @param afield      the 0-based index of the aggregate field in the tuple
	 * @param what        the aggregation operator
	 */

	public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		this.gbIndex = gbfield;
		this.agIndex = afield;
		this.op = what;
		this.agRes = new HashMap<Field, Integer>();
		this.avgHelper = new HashMap<Field, Integer[]>();
		hasagTd = false;
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor(我们不必等到所有元组都分组完毕再进行聚合计算)
	 * 
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		// some code goes here
		if (!hasagTd)
			getagTd(tup);

		Field gbField = null;
		if (gbIndex != NO_GROUPING)
			gbField = tup.getField(gbIndex);
		IntField agField = (IntField) tup.getField(agIndex);

		if (op.equals(Op.MIN)) {
			min(gbField, agField);
		} else if (op.equals(Op.MAX)) {
			max(gbField, agField);
		} else if (op.equals(Op.SUM)) {
			sum(gbField, agField);
		} else if (op.equals(Op.AVG)) {
			avg(gbField, agField);
		} else if (op.equals(Op.COUNT)) {
			count(gbField, agField);
		}
	}

	/**
	 * 这个方法用于获得聚合后的元组描述，即agTd
	 */
	private void getagTd(Tuple tup) {
		TupleDesc otd;
		Type[] typeArr;
		String[] nameArr;
		otd = tup.getTupleDesc();
		if (otd.getFieldType(agIndex) != Type.INT_TYPE)
			throw new IllegalArgumentException("聚合字段不是INT_TYPE");
		if (gbIndex == NO_GROUPING) {
			typeArr = new Type[] { otd.getFieldType(agIndex) };
			nameArr = new String[] { otd.getFieldName(agIndex) };
		} else {
			typeArr = new Type[] { otd.getFieldType(gbIndex), otd.getFieldType(agIndex) };
			nameArr = new String[] { otd.getFieldName(gbIndex), otd.getFieldName(agIndex) };
		}
		agTd = new TupleDesc(typeArr, nameArr);
		hasagTd = true;
	}

	private void count(Field gbField, IntField agField) {
		if (!agRes.containsKey(gbField))// 该组未创建
			agRes.put(gbField, 1);
		else// 该组已经存在了
			agRes.put(gbField, agRes.get(gbField) + 1);
	}

	/**
	 * 我们计算每一组的count和sum，最后求均值，这里的均值向下取整
	 */
	private void avg(Field gbField, IntField agField) {
		int avg = -1;
		if (avgHelper.containsKey(gbField)) {
			Integer[] tempArr = avgHelper.get(gbField);
			int count = tempArr[0] + 1;
			int sum = tempArr[1] + agField.getValue();
			avg = sum / count;
			agRes.put(gbField, avg);
			avgHelper.put(gbField, new Integer[] { count, sum });
		} else {
			avg = agField.getValue();
			agRes.put(gbField, avg);
			avgHelper.put(gbField, new Integer[] { 1, avg });
		}
	}

	private void sum(Field gbField, IntField agField) {
		if (!agRes.containsKey(gbField))// 该组未创建
			agRes.put(gbField, agField.getValue());
		else// 该组已经存在了
			agRes.put(gbField, agRes.get(gbField) + agField.getValue());
	}

	private void max(Field gbField, IntField agField) {
		if (!agRes.containsKey(gbField))// 该组未创建
			agRes.put(gbField, agField.getValue());
		else// 该组已经存在了
			agRes.put(gbField, Math.max(agRes.get(gbField), agField.getValue()));
	}

	private void min(Field gbField, IntField agField) {
		if (!agRes.containsKey(gbField))// 该组未创建
			agRes.put(gbField, agField.getValue());
		else// 该组已经存在了
			agRes.put(gbField, Math.min(agRes.get(gbField), agField.getValue()));
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 * 
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal) if
	 *         using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in the
	 *         constructor. (可以想一下在sql操作中，聚合函数返回的样子)
	 */
	public DbIterator iterator() {
		// some code goes here
		List<Tuple> tuples = new ArrayList<Tuple>();// 我觉得ArrayList或者LinkedList差别在这里应该不大
		for (Entry<Field, Integer> temp : agRes.entrySet()) {// 遍历agRes
			Tuple e = new Tuple(agTd);
			if (gbIndex == NO_GROUPING) {
				e.setField(0, new IntField(temp.getValue()));// 聚合值
			} else {
				e.setField(0, temp.getKey());// 分组字段
				e.setField(1, new IntField(temp.getValue()));// 聚合值
			}
			tuples.add(e);
		}
		// 返回
		return new TupleIterator(agTd, tuples);
	}

}
