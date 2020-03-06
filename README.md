# 数据存储

**SimpleDb是一个支持transaction、SQL parser、SQL optimization的关系型数据库管理系统。**

本文主要介绍SimpleDb的pro1实现部分并简要介绍原理。将按类文件来逐个介绍其实现思路，最后再介绍所有的类是如何协作以实现SimpleDb的数据存储功能的。

在pro1中，SimpleDb主要实现了关系型数据库的数据存储功能：

1. 将表的数据以二进制形式存储在磁盘上
2. tuple支持定长记录
3. 支持heap file organization的文件组织方式
2. 实现Catalog，便于DBMS管理所有的表
3. 实现BufferPool以减少DBMS的磁盘I/O，提高效率

下面是SimpleDb_Homeworks主页面的链接：

[https://sites.google.com/site/cs186fall2013/homeworks](https://sites.google.com/site/cs186fall2013/homeworks "SimpleDb_Homeworks主页面")

*在本部分的实现中，用到的参考会在文末给出*

## Tuple

Tuple可以理解为Page中的一行记录。Tuple主要的属性有TupleDesc(即Tuple的描述，它描述了元组的结构，比如各个字段的类型等)和Field数组（用来表示各字段的值）。

>
应当注意的是，就目前来说在SimpleDb中的数据类型只有两种，分别是INT _TYPE和STRING _TYPE。并且SimpleDb都是定长记录，其中前者占4字节，后者占128+4字节（其中前4个字节用来记录数据有效的字节数），更多的细节可以查看Type.java、FieldInt.java和StringField.java的源码。

此外，Tuple还有一个RecordId属性，是用来唯一标识和定位某一个指定的Tuple对象的。

## TupleDesc

如前文所说，TupleDesc用来描述Tuple的schema。（schema被翻译为模式/结构）

在TupleDesc类中，有一个私有内部类TDItem，TDItem描述了一个Field的名称和类型。实际上，整个TupleDesc的实现也是围绕着TDItem进行的。

在TupleDesc中使用链表或者其他的数据结构，将若干个TDItem放入其中，就能够通过TupleDesc的相关方法方便地访问一个Tuple中任意一个Field的信息。

比如说要获取Tuple对象tuple的第1个字段的名称，可以调用：

    tuple.getTupleDesc().getFieldName(1);

## Catalog

关于Catalog的概念，在数据库系统概念第六版的第10.7小节有介绍。（**强烈推荐先看看书上的介绍**）

本文理解为，Catalog是DBMS的一张“总表”，DBMS可以通过Catalog来知道当前系统中有哪些表（注意这里的系统不是指BufferPool，而是整个DBMS）

简单来说，Catalog是为了DBMS追踪/访问所有表而存在的。

在本文的实现中，Catalog类中存在关于表名称、表约束、表的磁盘文件的映射，这样做的意义在于DBMS能够通过Catalog快速访问到表。

举个例子，假设DBMS的BufferPool为空，DBMS现在需要读取一张表table1的数据，DBMS会进行如下操作：

1. 在BufferPool中检索是否存在table1
2. BufferPool中不存在table1，准备去磁盘读取table1
3. 通过Catalog得到table1的磁盘文件对象
4. 最后DBMS会利用table1的磁盘文件对象来读取table1的数据

上面的例子是DBMS通过Catalog快速获取某一张表的磁盘文件对象的实例，实际上DBMS通过Catalog还能获得某一张表的表名等相关的信息。

## BufferPool

关于BufferPool的概念，在数据库系统概念第六版的第10.8小节有介绍。
（**强烈推荐先看看书上的介绍**）

关于BufferPool，书上介绍的比较全面。在这里就不赘述了，但要注意的是再pro1中，BufferPool不需要实现缓冲区的替换策略（比如说LRU）

## HeapPage

在SimpleDb中，一张表中包含多个Page，每个Page中有若干Tuple。Page的默认大小是4096字节（在BufferPool中可以设置Page的默认大小）
### HeapPage的结构
HeapPage主要由header(表头)和tuples（记录）构成。

>注意SimpleDb采用等长记录，也就是Page中每个Tuple字节大小都是一样的

#### header

header用来标识Page中某个Tuple的可用状态。

例如，在Page中删除了某个Tuple，SimpleDb会在header中标识它已被删除，而不是直接删除掉它，因为直接删除会移动后面的Tuple，这样代价比较大
>each page has a header that consists of a bitmap with one bit per tuple slot. If the bit corresponding to a particular tuple is 1, it indicates that the tuple is valid; if it is 0, the tuple is invalid (e.g., has been deleted or was never initialized.) 

#### tuples

因为同个Page中Tuple的字节大小都是一样的，所以我们可以计算单个Page中的Tuple数量：

    tupsPerPage = floor((BufferPool.PAGE_SIZE * 8) / (tuple size * 8 + 1))

然后计算header的字节大小：

	headerBytes = ceiling(tupsPerPage/8)



## HeapFile

HeapFile是磁盘文件的对象，SimpleDb需要借助它来读取磁盘数据。至于它为什么叫HeapFile，是因为SimpleDb的文件组织方式是**heap file organization**（堆文件组织）

HeapFile的读取单位是Page，它有一个getPage方法如下：

	public Page readPage(PageId pid) {
		// some code goes here
		byte[] data = new byte[BufferPool.PAGE_SIZE];
		Page page = null;
		// 计算偏移
		int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(getFile(), "r");
			raf.seek(offset);
			raf.read(data, 0, data.length);
			page = new HeapPage((HeapPageId) pid, data);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (raf != null)
					raf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return page;
	}

是通过计算出指定Page在磁盘文件中的偏移来读取数据的。

## Operators

在pro1中，只实现了SeqScan，蛮简单的，不再赘述。

## How do classes work together

以进行一次SeqScan的代码介绍各个类是如何协作的:

    package simpledb;

	import java.io.*;

	public class test {

	public static void main(String[] argv) {

		// construct a 3-column table schema
		Type types[] = new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
		String names[] = new String[] { "field0", "field1", "field2" };
		TupleDesc descriptor = new TupleDesc(types, names);

		// create the table, associate it with some_data_file.dat
		// and tell the catalog about the schema of this table.
		HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
		Database.getCatalog().addTable(table1, "test");

		// construct the query: we use a simple SeqScan, which spoonfeeds
		// tuples via its iterator.
		TransactionId tid = new TransactionId();
		SeqScan f = new SeqScan(tid, table1.getId());

		try {
			// and run it
			f.open();
			while (f.hasNext()) {
				Tuple tup = f.next();
				System.out.println(tup);
			}
			f.close();
			Database.getBufferPool().transactionComplete(tid);
		} catch (Exception e) {
			System.out.println("Exception : " + e);
			e.printStackTrace();
		}
	}

}

首先生成一个table的磁盘文件对象，然后添加进Catalog中

		HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
		Database.getCatalog().addTable(table1, "test");
然后，生成一个SeqScan对象，用来进行全表扫描的操作

		SeqScan f = new SeqScan(tid, table1.getId());

启动SeqScan

	f.open();

下面是open函数执行的操作，主要是加载资源并启动DbfileItrator
	
	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		tableName = Database.getCatalog().getTableName(tableId);
		td = Database.getCatalog().getTupleDesc(tableId);
		dfileItrator = Database.getCatalog().getDbFile(tableId).iterator(tid);
		dfileItrator.open();
		isOpen = true;
	}

启动SeqScan内的DbfileItrator后，就可以迭代访问table的数据了

	while (f.hasNext()) {
		Tuple tup = f.next();
		System.out.println(tup);
	}

最后要释放资源

	f.close();
## Ref

1. 数据库系统概念第六版

2. How databases work：
[http://coding-geek.com/how-databases-work/](http://coding-geek.com/how-databases-work/ "web1")

3. 其他作者的项目：https://github.com/iamxpy/SimpleDB