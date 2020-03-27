package simpledb;

import java.util.Vector;

public class ETest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		JoinOptimizer op = new JoinOptimizer(null, null);
		Vector<Integer> v = new Vector<Integer>();
		v.add(1);
		v.add(2);
		v.add(3);
		op.enumerateSubsets(v, 1);
	}

}
