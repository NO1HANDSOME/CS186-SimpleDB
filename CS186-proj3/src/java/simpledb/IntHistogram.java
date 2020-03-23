package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 * <p>
 * selectivity是频率
 */
public class IntHistogram {

	private int[] histogram;

	/**
	 * 每个Bucket的宽度
	 */
	private int width;

	private int ntuples;

	private int min;

	private int max;

	private int numB;

	/**
	 * Create a new IntHistogram.
	 * 
	 * This IntHistogram should maintain a histogram of integer values that it
	 * receives. It should split the histogram into "buckets" buckets.
	 * 
	 * The values that are being histogrammed will be provided one-at-a-time through
	 * the "addValue()" function.
	 * 
	 * Your implementation should use space and have execution time that are both
	 * constant with respect to the number of values being histogrammed. For
	 * example, you shouldn't simply store every value that you see in a sorted
	 * list.
	 * 
	 * @param buckets The number of buckets to split the input value into.
	 * @param min     The minimum integer value that will ever be passed to this
	 *                class for histogramming
	 * @param max     The maximum integer value that will ever be passed to this
	 *                class for histogramming
	 */
	public IntHistogram(int buckets, int min, int max) {
		// some code goes here
		this.numB = buckets;
		this.min = min;
		this.max = max;
		double domain = max - min + 1;
		this.width = (int) Math.ceil(domain / numB);
		histogram = new int[numB];
		this.ntuples = 0;
	}

	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 * 
	 * @param v Value to add to the histogram
	 */
	public void addValue(int v) {
		// some code goes here
		int index = getIndex(v);
		histogram[index]++;
		ntuples++;
	}

	/**
	 * Estimate the selectivity of a particular predicate and operand on this table.
	 * 
	 * For example, if "op" is "GREATER_THAN" and "v" is 5, return your estimate of
	 * the fraction of elements that are greater than 5.
	 * 
	 * @param op Operator
	 * @param v  Value
	 * @return Predicted selectivity of this particular operator and value
	 */
	public double estimateSelectivity(Predicate.Op op, int v) {
		// some code goes here
		// https://github.com/iamxpy/SimpleDB/blob/master/CS186-proj3/src/java/simpledb/IntHistogram.java
		int bucketIndex = getIndex(v);
		int height;
		int left = bucketIndex * width + min;
		int right = bucketIndex * width + min + width - 1;

		switch (op) {
		case EQUALS:
			if (v < min || v > max) {
				return 0.0;
			} else {
				height = histogram[bucketIndex];
				return (height * 1.0 / width) / ntuples;
			}
		case GREATER_THAN:
			if (v < min) {
				return 1.0;
			}
			if (v > max) {
				return 0.0;
			}
			height = histogram[bucketIndex];
			double p1 = ((right - v) * 1.0 / width) * height;
			double p2 = 0;
			for (int i = bucketIndex + 1; i < numB; i++) {
				p2 += histogram[i];
			}
			return (p1 + p2) / ntuples;
		case LESS_THAN:
			if (v < min) {
				return 0.0;
			}
			if (v > max) {
				return 1.0;
			}
			height = histogram[bucketIndex];
			double pp1 = ((v - left) * 1.0 / width) * height;
			double pp2 = 0;
			for (int i = 0; i < bucketIndex; i++) {
				pp2 += histogram[i];
			}
			return (pp1 + pp2) / ntuples;
		case LESS_THAN_OR_EQ:
			return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
		case GREATER_THAN_OR_EQ:
			return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
		case LIKE:
			return avgSelectivity();
		case NOT_EQUALS:
			return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
		default:
			throw new RuntimeException("Should not reach hear");
		}
	}

	private int getIndex(int v) {
		return (v - min) / width;
	}

	/**
	 * @return the average selectivity of this histogram.
	 * 
	 *         This is not an indispensable method to implement the basic join
	 *         optimization. It may be needed if you want to implement a more
	 *         efficient optimization
	 */
	public double avgSelectivity() {
		// some code goes here
		return 1.0;
	}

	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	public String toString() {
		// some code goes here
		StringBuilder sb1 = new StringBuilder();
		for (int i = 0; i < histogram.length; i++) {
			int left = i * width + min;
			int right = i * width + min + width - 1;
			sb1.append("[" + left + " , " + right + "]" + ": ");
			sb1.append(histogram[i] + "\n");
		}
		sb1.append("begin: " + min + " end: " + max + " numB: " + numB + " width: " + width + " ntuples: " + ntuples
				+ "\n");
		return sb1.toString();
	}
}
