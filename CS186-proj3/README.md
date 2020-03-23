# Query manager

在SimpleDb的第二部分，实现了以下功能：

1. 关系代数
2. 聚合操作
3. Insert和Delete功能
4. 根据LRU策略实现BufferPool的Page eviction

实现了这部分的功能后，SimpleDb能够借助**现有的Query Parser**进行简单的SQL查询以及相关操作了。

注意到这里并没有实现Update功能，但是将Insert和Delete组合起来使用是能够实现Update的效果的。

![](http://coding-geek.com/wp-content/uploads/2015/08/query_manager.png)

**但是，有如下需要注意的**：

1. You must preface every field name with its table name, even if the field name is unique (you can use table name aliases, as in the example above, but you cannot use the AS keyword.)
2. Nested queries are supported in the WHERE clause, but not the FROM clause.
3. No arithmetic expressions are supported (for example, you can't take the sum of two fields.)
4. At most one GROUP BY and one aggregate column are allowed.
5. Set-oriented operators like IN, UNION, and EXCEPT are not allowed.
6. Only AND expressions in the WHERE clause are allowed.
7. UPDATE expressions are not supported.
8. The string operator LIKE is allowed, but must be written out fully (that is, the Postgres tilde [~] shorthand is not allowed.)

如果想要去除这些限制呢，我觉得需要修改SQL Parser的源代码。但考虑到我做这个Project的目的是了解数据库而不是学习编译原理，所以并没有重写SQL Parser的打算。

## Filter

Filter继承自Operator，相当于关系代数中的选择操作，可以根据给定的Predicate来过滤掉一些元组。

构造函数如下：

		/**
	 * Constructor accepts a predicate to apply and a child operator to read tuples
	 * to filter from.
	 * 
	 * @param p     The predicate to filter tuples with
	 * @param child The child operator
	 */
	public Filter(Predicate p, DbIterator child) {
		// some code goes here
		this.p = p;
		this.child = child;
	}

通过调用fetchNext就可以达到过滤的功能：
	
	protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException, DbException {
		// some code goes here
		while (child.hasNext()) {
			Tuple temp = child.next();
			if (p.filter(temp))// p is a Predicate Object
				return temp;
		}
		return null;
	}

## Join

Join就是做关系代数中的θ连接，这个θ由JoinPredicate表示。

构造函数如下：

	/**
	 * Constructor. Accepts to children to join and the predicate to join them on
	 * 
	 * @param p      The predicate to use to join the children
	 * @param child1 Iterator for the left(outer) relation to join
	 * @param child2 Iterator for the right(inner) relation to join
	 */
	public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
		// some code goes here
	}

在实现Join的时候，对于如何连接两张表有许多算法。在这里，我选择了最简单的nestedLoopJoin。

	private TupleIterator nestedLoopJoin() throws DbException, TransactionAbortedException {
		List<Tuple> list = new LinkedList<Tuple>();
		while (child1.hasNext()) {
			Tuple out = child1.next();
			while (child2.hasNext()) {
				Tuple in = child2.next();
				if (p.filter(out, in)) {
					Tuple newTuple = mergeTuples(out, in);
					list.add(newTuple);
				}
			}
			child2.rewind();// 低效
		}
		return new TupleIterator(td, list);
	}

## Project

这个部分已经由伯克利的老师完成了，在源代码里面可以看到。

至此，基本的关系代数操作已经完成了。

## Aggregate

在Aggregate中，可以对分组字段进行如下聚合操作：

1. avg
2. count
2. sum
3. max
4. min

### 聚合执行流程

当执行聚合操作时，Aggregate会根据分组字段的类型来调用不同的Aggregator(聚合器接口)，如下：

	if (aType.equals(Type.INT_TYPE)) {
			aggregator = new IntegerAggregator(gfield, gType, afield, aop);
		} else if (aType.equals(Type.STRING_TYPE)) {
			aggregator = new StringAggregator(gfield, gType, afield, aop);
		}

就像上面一样，Aggregate可以产生IntegerAggregator和StringAggregator两种聚合器，它们分别针对不同的数据类型。

然后，Aggregate会调用Aggregator的mergeTupleIntoGroup函数。

	while (child.hasNext()) {
		Tuple t = child.next();
		aggregator.mergeTupleIntoGroup(t);
	}
	agRes = aggregator.iterator();

Aggregate会把待聚合表的所有元组依次传递给Aggregator进行相关操作，最后由Aggregator返回一个迭代器形式的聚合结果。

## HeapFile Mutability

这一部分主要是为了Insert和Delet操作做铺垫。

在这里有一个概念叫Dirty，当一个Page被修改后（即增加或删除了元组），这个Page被认为时Dirty。

这个概念很重要，因为当Page从BufferPool中出去时（evictPage），BufferPool会检查Page是否为Dirty，如果是的话，BufferPool会将该Page整个重写到磁盘文件，以达到更新磁盘文件的目的。

> 在SimpleDb中HeapFile代表了一张表在磁盘上的文件对象，而若干的Page组成了这个HeapFile。

下面以Insert为例，在**HeapFile**层面介绍它的执行原理。

### Insert

在HeapFile中，首先需要找到可插入(not full)的Page，假如这个时候HeapFile中所有的Page都是满的(full)，那么HeapFile需要创建一个空的Page，然后放在整个HeapFile的最后面。

	writePage(ePage);// empty page

然后通过BufferPool得到这个Page，再调用Page的方法即可

	// get Page ......

	page.insertTuple(t);
	page.markDirty(true, tid);

至于HeapPage中，insertTuple是如何执行的，可以查看源代码，也比较简单。

## Insertion and deletion

完成了HeapFile和HeapPage层面的Insert和Delete实现，我们可以在BufferPool上进行Insert和Delete了，只需要调用HeapFile的相关方法即可。

例如，插入元组的操作：

	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for proj1
		DbFile file = Database.getCatalog().getDbFile(tableId);
		file.insertTuple(tid, t);// 注意markDirty等操作会在下层执行
	}

也就是在获得了一张表的磁盘文件对象HeapFile后，调用HeapFile的插入方法。

当我们通过BufferPool对HeapFile中的某个Page进行修改后，只是我们**将这个Page拿到了BufferPool中，然后在内存中对这个Page对象做了修改**，那么对应的磁盘文件如何得到修改？

这就涉及到之前提到过的Dirty概念了，当每个Page离开BufferPool时，BufferPool会检查Dirty状态然后决定是否flush它。

至于这个Page何时离开BufferPool，这就讲到Page eviction了。

## Page eviction

> When more than numPages pages are in the buffer pool, one page should be evicted from the pool before the next is loaded.

BufferPool的Capacity时有限的，当BufferPool中Page数量已经达到上限，而此时恰好需要再读一个Page进来时，必然有一个BufferPool中的Page需要被释放掉。

如何决定哪个Page被释放呢？有如下策略：

1. 随机释放
2. 使用LRU策略
3. 其他策略

在实现中，我选择了LRU策略。

通过PriorityQue来保存BufferPool中每个Page的AccessRecord。然后在需要evictPage时，拿到PriorityQue的队首元素，通过它定位需要被释放的Page即可。

## Query Parser

Parser部分伯克利的老师做好了，700多行大部分是编译原理的东西了。

把Proj2部分的接口都实现了给Parser调用就可以了。

下面是部分逻辑：

	if (s instanceof ZInsert)
		query = handleInsertStatement((ZInsert) s, curtrans.getId());
	else if (s instanceof ZDelete)
		query = handleDeleteStatement((ZDelete) s, curtrans.getId());
	else if (s instanceof ZQuery)
		query = handleQueryStatement((ZQuery) s, curtrans.getId());
	else {
		System.out.println("Can't parse " + s
			+ "\n -- parser only handles SQL transactions, insert, delete, and select statements");
		}
		if (query != null)
			query.execute();