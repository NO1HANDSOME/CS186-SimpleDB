package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas. For now, this is a stub catalog that must be populated
 * with tables by a user program before it can be used -- eventually, this
 * should be converted to a catalog that reads a catalog table from disk.
 */

public class Catalog {

	// 存储磁盘文件对象
	private HashMap<Integer, DbFile> id2file;
	// 存储关系约束
	private HashMap<Integer, String> id2pkey;
	// 存储关系名称
	private HashMap<Integer, String> id2name;

	private HashMap<String, Integer> name2id;

	// 关系的总数
	private int size;

	/**
	 * Constructor. Creates a new, empty catalog.
	 * 个人认为，catalog相当于数据库的总表或者说数据库元数据字典.在数据库系统概念的第十章第七节有简单的介绍.
	 * DBMS想要找到某个关系，需要通过Catalog.
	 */
	public Catalog() {
		// some code goes here
		id2file = new HashMap<>();
		id2pkey = new HashMap<>();
		id2name = new HashMap<>();
		name2id = new HashMap<>();
		size = 0;
	}

	/**
	 * Add a new table to the catalog. This table's contents are stored in the
	 * specified DbFile.
	 * 
	 * @param file      the contents of the table to add; file.getId() is the
	 *                  identfier of this file/tupledesc param for the calls
	 *                  getTupleDesc and getFile
	 * @param name      the name of the table -- may be an empty string. May not be
	 *                  null. If a name
	 * @param pkeyField the name of the primary key field conflict exists, use the
	 *                  last table to be added as the table for a given name.
	 */
	public void addTable(DbFile file, String name, String pkeyField) {
		// some code goes here
		if (name == null || pkeyField == null) {
			throw new IllegalArgumentException();
		}
		int tableid = file.getId();
//		if (name2id.containsKey(name)) {
//			// use the last table to be added as the table for a given name.
//			throw new UnsupportedOperationException("目前不支持添加相同名字的table");
//		}
		id2file.put(tableid, file);
		id2name.put(tableid, name);
		id2pkey.put(tableid, pkeyField);
		name2id.put(name, tableid);
		size++;
	}

	public void addTable(DbFile file, String name) {
		addTable(file, name, "");
	}

	/**
	 * Add a new table to the catalog. This table has tuples formatted using the
	 * specified TupleDesc and its contents are stored in the specified DbFile.
	 * 
	 * @param file the contents of the table to add; file.getId() is the identfier
	 *             of this file/tupledesc param for the calls getTupleDesc and
	 *             getFile
	 */
	public void addTable(DbFile file) {
		addTable(file, (UUID.randomUUID()).toString());
	}

	/**
	 * Return the id of the table with a specified name,
	 * 
	 * @throws NoSuchElementException if the table doesn't exist
	 */
	public int getTableId(String name) throws NoSuchElementException {
		// some code goes here
		if (name == null || !name2id.containsKey(name)) {
			throw new NoSuchElementException();
		}
		return name2id.get(name);
	}

	/**
	 * Returns the tuple descriptor (schema) of the specified table
	 * 
	 * @param tableid The id of the table, as specified by the DbFile.getId()
	 *                function passed to addTable
	 * @throws NoSuchElementException if the table doesn't exist
	 */
	public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
		// some code goes here
		return getDbFile(tableid).getTupleDesc();
	}

	/**
	 * Returns the DbFile that can be used to read the contents of the specified
	 * table.
	 * 
	 * @param tableid The id of the table, as specified by the DbFile.getId()
	 *                function passed to addTable
	 */
	public DbFile getDbFile(int tableid) throws NoSuchElementException {
		// some code goes here
		if (!id2file.containsKey(tableid)) {
			throw new NoSuchElementException();
		}
		return id2file.get(tableid);
	}

	public String getPrimaryKey(int tableid) throws NoSuchElementException {
		// some code goes here
		if (!id2pkey.containsKey(tableid)) {
			throw new NoSuchElementException();
		}
		return id2pkey.get(tableid);
	}

	public Iterator<Integer> tableIdIterator() {
		// some code goes here
		return id2file.keySet().iterator();
	}

	public String getTableName(int id) throws NoSuchElementException {
		// some code goes here
		if (!id2name.containsKey(id)) {
			throw new NoSuchElementException();
		}
		return id2name.get(id);
	}

	/** Delete all tables from the catalog */
	public void clear() {
		// some code goes here
		id2file.clear();
		id2pkey.clear();
		id2name.clear();
		name2id.clear();
		size = 0;
	}

	/**
	 * Reads the schema from a file and creates the appropriate tables in the
	 * database.
	 * 
	 * @param catalogFile
	 */
	public void loadSchema(String catalogFile) {
		String line = "";
		String baseFolder = new File(catalogFile).getParent();
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

			while ((line = br.readLine()) != null) {
				// assume line is of the format name (field type, field type, ...)
				String name = line.substring(0, line.indexOf("(")).trim();
				// System.out.println("TABLE NAME: " + name);
				String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
				String[] els = fields.split(",");
				ArrayList<String> names = new ArrayList<String>();
				ArrayList<Type> types = new ArrayList<Type>();
				String primaryKey = "";
				for (String e : els) {
					String[] els2 = e.trim().split(" ");
					names.add(els2[0].trim());
					if (els2[1].trim().toLowerCase().equals("int"))
						types.add(Type.INT_TYPE);
					else if (els2[1].trim().toLowerCase().equals("string"))
						types.add(Type.STRING_TYPE);
					else {
						System.out.println("Unknown type " + els2[1]);
						System.exit(0);
					}
					if (els2.length == 3) {
						if (els2[2].trim().equals("pk"))
							primaryKey = els2[0].trim();
						else {
							System.out.println("Unknown annotation " + els2[2]);
							System.exit(0);
						}
					}
				}
				Type[] typeAr = types.toArray(new Type[0]);
				String[] namesAr = names.toArray(new String[0]);
				TupleDesc t = new TupleDesc(typeAr, namesAr);
				HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
				addTable(tabHf, name, primaryKey);
				System.out.println("Added table : " + name + " with schema " + t);
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IndexOutOfBoundsException e) {
			System.out.println("Invalid catalog entry : " + line);
			System.exit(0);
		}
	}
}
