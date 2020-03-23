package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

	private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

	/**
	 * 从意思上理解，每个page的IO成本
	 */
	static final int IOCOSTPERPAGE = 1000;

	public static TableStats getTableStats(String tablename) {
		return statsMap.get(tablename);
	}

	public static void setTableStats(String tablename, TableStats stats) {
		statsMap.put(tablename, stats);
	}

	public static void setStatsMap(HashMap<String, TableStats> s) {
		try {
			java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
			statsMapF.setAccessible(true);
			statsMapF.set(null, s);
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}

	}

	public static Map<String, TableStats> getStatsMap() {
		return statsMap;
	}

	/**
	 * 记录SimpleDb中所有表的Stat
	 */
	public static void computeStatistics() {
		Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

		System.out.println("Computing table stats.");
		while (tableIt.hasNext()) {
			int tableid = tableIt.next();
			TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
			setTableStats(Database.getCatalog().getTableName(tableid), s);// 记录表的Stat
		}
		System.out.println("Done.");
	}

	/**
	 * Number of bins for the histogram. Feel free to increase this value over 100,
	 * though our tests assume that you have at least 100 bins in your histograms.
	 */
	static final int NUM_HIST_BINS = 100;

	private HeapFile file;

	private int ioCostPerPage;

	/**
	 * 所有的元组数目
	 */
	private int nTuples;

	/**
	 * 记录每一列值域的上下界
	 */
	private HashMap<Integer, Pair> bound;

	/**
	 * 保存每一列的直方图
	 */
	private ArrayList<Object> hisArr;

	/**
	 * Create a new TableStats object, that keeps track of statistics on each column
	 * of a table
	 * 
	 * @param tableid       The table over which to compute statistics
	 * @param ioCostPerPage The cost per page of IO. This doesn't differentiate
	 *                      between sequential-scan IO and disk seeks.
	 */
	public TableStats(int tableid, int ioCostPerPage) {
		// For this function, you'll have to get the
		// DbFile for the table in question,
		// then scan through its tuples and calculate
		// the values that you need.
		// You should try to do this reasonably efficiently, but you don't
		// necessarily have to (for example) do everything
		// in a single scan of the table.
		// some code goes here
		this.file = (HeapFile) Database.getCatalog().getDbFile(tableid);
		this.ioCostPerPage = ioCostPerPage;
		this.nTuples = 0;
		// 扫描表，创建直方图
		init();
	}

	private void init() {
		TransactionId tid = new TransactionId();
		DbFileIterator iterator = file.iterator(tid);
		TupleDesc td = file.getTupleDesc();
		int nFileds = td.numFields();
		hisArr = new ArrayList<Object>();
		bound = new HashMap<Integer, TableStats.Pair>(nFileds);
		try {
			iterator.open();
			// 这里记录每一列的最小与最大值
			// 为构造每列的直方图准备参数
			while (iterator.hasNext()) {
				Tuple tuple = iterator.next();
				nTuples++;
				for (int i = 0; i < nFileds; i++) {
					Field field = tuple.getField(i);
					if (field.getType().equals(Type.INT_TYPE)) {
						int val = ((IntField) field).getValue();
						if (bound.get(i) == null) {
							bound.put(i, new Pair(val, val));
						} else {
							Pair pair = bound.get(i);// 获得指向对象的引用
							if (val < pair.min)
								pair.min = val;
							if (val > pair.max)
								pair.max = val;
						}
					}
				}
			}

			// 为每一列创建一个空直方图
			for (int i = 0; i < nFileds; i++) {
				if (td.getFieldType(i).equals(Type.INT_TYPE)) {
					int min = bound.get(i).min;
					int max = bound.get(i).max;
					hisArr.add(new IntHistogram(NUM_HIST_BINS, min, max));
				} else {
					hisArr.add(new StringHistogram(NUM_HIST_BINS));
				}
			}

			// 为每列的直方图addValue
			iterator.rewind();
			while (iterator.hasNext()) {
				Tuple tuple = iterator.next();
				for (int i = 0; i < nFileds; i++) {
					Field field = tuple.getField(i);
					if (field.getType().equals(Type.INT_TYPE)) {
						IntHistogram his = (IntHistogram) hisArr.get(i);
						his.addValue(((IntField) field).getValue());
					} else {
						StringHistogram his = (StringHistogram) hisArr.get(i);
						his.addValue(((StringField) field).getValue());
					}
				}
			}
		} catch (NoSuchElementException e) {
			e.printStackTrace();
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Estimates the cost of sequentially scanning the file, given that the cost to
	 * read a page is costPerPageIO. You can assume that there are no seeks and that
	 * no pages are in the buffer pool.
	 * 
	 * Also, assume that your hard drive can only read entire pages at once, so if
	 * the last page of the table only has one tuple on it, it's just as expensive
	 * to read as a full page. (Most real hard drives can't efficiently address
	 * regions smaller than a page at a time.)
	 * 
	 * @return The estimated cost of scanning the table.
	 */
	public double estimateScanCost() {
		// some code goes here
		return file.numPages() * ioCostPerPage;
	}

	/**
	 * This method returns the number of tuples in the relation, given that a
	 * predicate with selectivity selectivityFactor is applied.
	 * 
	 * @param selectivityFactor The selectivity of any predicates over the table
	 * @return The estimated cardinality of the scan with the specified
	 *         selectivityFactor
	 */
	public int estimateTableCardinality(double selectivityFactor) {
		// some code goes here
		return (int) Math.ceil(nTuples * selectivityFactor);
	}

	/**
	 * The average selectivity of the field under op.
	 * <p>
	 * It may be needed if you want to implement a more efficient
	 * optimization(这句话是我自己加的)
	 * 
	 * @param field the index of the field
	 * @param op    the operator in the predicate The semantic of the method is
	 *              that, given the table, and then given a tuple, of which we do
	 *              not know the value of the field, return the expected
	 *              selectivity. You may estimate this value from the histograms.
	 */
	public double avgSelectivity(int field, Predicate.Op op) {
		// some code goes here
		return 1.0;
	}

	/**
	 * Estimate the selectivity of predicate <tt>field op constant</tt> on the
	 * table.
	 * 
	 * @param field    The field over which the predicate ranges
	 * @param op       The logical operation in the predicate
	 * @param constant The value against which the field is compared
	 * @return The estimated selectivity (fraction of tuples that satisfy) the
	 *         predicate
	 */
	public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
		// some code goes here
		if (file.getTupleDesc().getFieldType(field).equals(Type.INT_TYPE)) {
			IntHistogram his = (IntHistogram) hisArr.get(field);
			return his.estimateSelectivity(op, ((IntField) constant).getValue());
		} else {
			StringHistogram his = (StringHistogram) hisArr.get(field);
			return his.estimateSelectivity(op, ((StringField) constant).getValue());
		}
	}

	/**
	 * return the total number of tuples in this table
	 */
	public int totalTuples() {
		// some code goes here
		return nTuples;
	}

	/**
	 * 用来辅助记录列值最大与最小值的类
	 * 
	 */
	private class Pair {
		public int min;
		public int max;

		public Pair(int min, int max) {
			this.min = min;
			this.max = max;
		}
	}

}
