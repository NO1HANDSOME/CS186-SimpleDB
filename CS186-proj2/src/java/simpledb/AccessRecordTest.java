package simpledb;

import java.util.TreeSet;

public class AccessRecordTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		AccessRecord ar1 = new AccessRecord(new TransactionId(), new HeapPageId(-1, 10), 1);
		AccessRecord ar2 = new AccessRecord(new TransactionId(), new HeapPageId(-1, 10), 2);
		TreeSet<AccessRecord> set = new TreeSet<AccessRecord>();
		set.add(ar1);
		set.add(ar2);
		System.out.println(set);
	}

}
