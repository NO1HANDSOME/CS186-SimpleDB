package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

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
	 * @param what        aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException if what != COUNT
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		if (what != Op.COUNT) {
			throw new UnsupportedOperationException("String类型值只支持count操作");
		}
		this.gbIndex = gbfield;
		this.agIndex = afield;
		this.op = what;
		this.agRes = new HashMap<Field, Integer>();
		hasagTd = false;
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
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
		StringField agField = (StringField) tup.getField(agIndex);

		count(gbField, agField);
	}

	/**
	 * 这个方法用于获得聚合后的元组描述，即agTd
	 */
	private void getagTd(Tuple tup) {
		TupleDesc otd;
		Type[] typeArr;
		String[] nameArr;
		otd = tup.getTupleDesc();
		if (otd.getFieldType(agIndex) != Type.STRING_TYPE)
			throw new IllegalArgumentException("聚合字段不是STRING_TYPE");
		if (gbIndex == NO_GROUPING) {
			typeArr = new Type[] { Type.INT_TYPE };
			nameArr = new String[] { otd.getFieldName(agIndex) };
		} else {
			typeArr = new Type[] { otd.getFieldType(gbIndex), Type.INT_TYPE };
			nameArr = new String[] { otd.getFieldName(gbIndex), otd.getFieldName(agIndex) };
		}
		agTd = new TupleDesc(typeArr, nameArr);
		hasagTd = true;
	}

	private void count(Field gbField, StringField agField) {
		if (!agRes.containsKey(gbField))// 该组未创建
			agRes.put(gbField, 1);
		else// 该组已经存在了
			agRes.put(gbField, agRes.get(gbField) + 1);
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 *
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal) if
	 *         using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in the
	 *         constructor.
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
