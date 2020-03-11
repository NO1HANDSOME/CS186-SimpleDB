package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	/**
	 * A help class to facilitate organizing the information of each field
	 */
	public static class TDItem implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * The type of the field
		 */
		Type fieldType;

		/**
		 * The name of the field
		 */
		String fieldName;

		public TDItem(Type t, String n) {
			this.fieldName = n;
			this.fieldType = t;
		}

		public String toString() {
			return fieldName + "(" + fieldType + ")";
		}
	}

	private ArrayList<TDItem> tdItems;

	private int capacity;

	/**
	 * @return An iterator which iterates over all the field TDItems that are
	 *         included in this TupleDesc
	 */
	public Iterator<TDItem> iterator() {
		// some code goes here
		return tdItems.iterator();
	}

	private static final long serialVersionUID = 1L;

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the specified
	 * types, with associated named fields.
	 * 
	 * @param typeAr  array specifying the number of and types of fields in this
	 *                TupleDesc. It must contain at least one entry.
	 * @param fieldAr array specifying the names of the fields. Note that names may
	 *                be null.
	 */
	public TupleDesc(Type[] typeAr, String[] fieldAr) {
		tdItems = new ArrayList<TupleDesc.TDItem>();
		// some code goes here
		if (typeAr.length == 0) {
			throw new IllegalArgumentException("typeAr[].length == 0");
		}
		if (typeAr.length != fieldAr.length) {
			throw new IllegalArgumentException("fieldAr[].length != typeAr[].length");
		}

		for (int i = 0; i < typeAr.length; i++) {
			Type type = typeAr[i];
			String name = fieldAr[i];
			tdItems.add(new TDItem(type, name));
		}
		capacity = tdItems.size();
	}

	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with fields of
	 * the specified types, with anonymous (unnamed) fields.
	 * 
	 * @param typeAr array specifying the number of and types of fields in this
	 *               TupleDesc. It must contain at least one entry.
	 */
	public TupleDesc(Type[] typeAr) {
		// some code goes here
		this(typeAr, new String[typeAr.length]);
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		// some code goes here
		return capacity;
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 * 
	 * @param i index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {
		// some code goes here
		if (i < 0 || i >= capacity)
			throw new NoSuchElementException();

		return tdItems.get(i).fieldName;
	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 * 
	 * @param i The index of the field to get the type of. It must be a valid index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public Type getFieldType(int i) throws NoSuchElementException {
		// some code goes here
		if (i < 0 || i >= capacity)
			throw new NoSuchElementException();

		return tdItems.get(i).fieldType;
	}

	/**
	 * Find the index of the field with a given name.
	 * 
	 * @param name name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException if no field with a matching name is found.
	 */
	public int fieldNameToIndex(String name) throws NoSuchElementException {
		// some code goes here
		if (name == null)
			throw new NoSuchElementException("name is null!");

		Iterator<TDItem> tdItemsIterator = tdItems.iterator();
		int index = 0;
		while (tdItemsIterator.hasNext()) {
			TDItem temp = tdItemsIterator.next();
			if (name.equals(temp.fieldName)) // 细节
				break;
			index++;
		}
		if (index >= capacity)
			throw new NoSuchElementException();

		return index;
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note
	 *         that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		// some code goes here
		Iterator<TDItem> iterator = tdItems.iterator();
		int size = 0;
		while (iterator.hasNext()) {
			size += iterator.next().fieldType.getLen();
		}
		return size;
	}

	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
	 * with the first td1.numFields coming from td1 and the remaining from td2.
	 * 
	 * @param td1 The TupleDesc with the first fields of the new TupleDesc
	 * @param td2 The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
		// some code goes here
		int newCapacity = td1.capacity + td2.capacity;
		Type[] newTypeAr = new Type[newCapacity];
		String[] newNameAr = new String[newCapacity];
		Iterator<TDItem> iterator1 = td1.iterator();
		int index = 0;
		while (iterator1.hasNext()) {
			TDItem temp = iterator1.next();
			newTypeAr[index] = temp.fieldType;
			newNameAr[index++] = temp.fieldName;
		}
		Iterator<TDItem> iterator2 = td2.iterator();
		while (iterator2.hasNext()) {
			TDItem temp = iterator2.next();
			newTypeAr[index] = temp.fieldType;
			newNameAr[index++] = temp.fieldName;
		}
		return new TupleDesc(newTypeAr, newNameAr);
	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they are the same size and if the n-th
	 * type in this TupleDesc is equal to the n-th type in td.
	 * 
	 * @param o the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */
	public boolean equals(Object o) {
		// some code goes here
		if (!(o instanceof TupleDesc))
			return false;
		TupleDesc td = (TupleDesc) o;
		if (this.capacity != td.numFields() || this.getSize() != td.getSize())
			return false;
		Iterator<TDItem> iterator1 = this.iterator();
		Iterator<TDItem> iterator2 = td.iterator();
		while (iterator1.hasNext()) {
			Type thisType = iterator1.next().fieldType;
			Type anotherType = iterator2.next().fieldType;
			if (!thisType.equals(anotherType))
				return false;
		}
		return true;
	}

	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		throw new UnsupportedOperationException("unimplemented");
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the
	 * exact format does not matter.
	 * 
	 * @return String describing this descriptor.
	 */
	public String toString() {
		// some code goes here
		StringBuilder sb = new StringBuilder();
		Iterator<TDItem> iterator = tdItems.iterator();
		while(iterator.hasNext()) {
			TDItem temp = iterator.next();
			sb.append(temp.toString()+",");
		}
		return sb.toString();
	}
}
