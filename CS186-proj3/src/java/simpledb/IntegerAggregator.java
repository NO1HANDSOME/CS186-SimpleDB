package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of
 * IntFields.(ע��ֻ֧��һ���ֶη��顢һ���ֶξۺ�)
 */
public class IntegerAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;

	/* �����ֶ� */
	private int gbIndex;

	/* �ۺ��ֶ� */
	private int agIndex;

	/* �ۺ������ */
	private Op op;

	/* �ۺϽ�� */
	private HashMap<Field, Integer> agRes;// ���ݷ����ֶε�ֵ��ӳ�䲻ͬ���

	/* �ۺϺ��Ԫ������ */
	private TupleDesc agTd;

	/* avg����ʱ�õ� */
	private HashMap<Field, Integer[]> avgHelper;

	/* �Ƿ���ȡ��agTd */
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
	 * constructor(���ǲ��صȵ�����Ԫ�鶼��������ٽ��оۺϼ���)
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
	 * ����������ڻ�þۺϺ��Ԫ����������agTd
	 */
	private void getagTd(Tuple tup) {
		TupleDesc otd;
		Type[] typeArr;
		String[] nameArr;
		otd = tup.getTupleDesc();
		if (otd.getFieldType(agIndex) != Type.INT_TYPE)
			throw new IllegalArgumentException("�ۺ��ֶβ���INT_TYPE");
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
		if (!agRes.containsKey(gbField))// ����δ����
			agRes.put(gbField, 1);
		else// �����Ѿ�������
			agRes.put(gbField, agRes.get(gbField) + 1);
	}

	/**
	 * ���Ǽ���ÿһ���count��sum��������ֵ������ľ�ֵ����ȡ��
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
		if (!agRes.containsKey(gbField))// ����δ����
			agRes.put(gbField, agField.getValue());
		else// �����Ѿ�������
			agRes.put(gbField, agRes.get(gbField) + agField.getValue());
	}

	private void max(Field gbField, IntField agField) {
		if (!agRes.containsKey(gbField))// ����δ����
			agRes.put(gbField, agField.getValue());
		else// �����Ѿ�������
			agRes.put(gbField, Math.max(agRes.get(gbField), agField.getValue()));
	}

	private void min(Field gbField, IntField agField) {
		if (!agRes.containsKey(gbField))// ����δ����
			agRes.put(gbField, agField.getValue());
		else// �����Ѿ�������
			agRes.put(gbField, Math.min(agRes.get(gbField), agField.getValue()));
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 * 
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal) if
	 *         using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in the
	 *         constructor. (������һ����sql�����У��ۺϺ������ص�����)
	 */
	public DbIterator iterator() {
		// some code goes here
		List<Tuple> tuples = new ArrayList<Tuple>();// �Ҿ���ArrayList����LinkedList���������Ӧ�ò���
		for (Entry<Field, Integer> temp : agRes.entrySet()) {// ����agRes
			Tuple e = new Tuple(agTd);
			if (gbIndex == NO_GROUPING) {
				e.setField(0, new IntField(temp.getValue()));// �ۺ�ֵ
			} else {
				e.setField(0, temp.getKey());// �����ֶ�
				e.setField(1, new IntField(temp.getValue()));// �ۺ�ֵ
			}
			tuples.add(e);
		}
		// ����
		return new TupleIterator(agTd, tuples);
	}

}
