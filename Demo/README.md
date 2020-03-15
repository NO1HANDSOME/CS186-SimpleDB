# SimpleDB Demo

这是一个**演示SQL查询功能**的Demo，已经准备好了CS186提供的测试数据，点击run.bat即可运行SimpleDB。

Demo内置了一些表，你可以在Demo文件夹下看到.dat后缀的文件，那些文件就是以二进制形式存储的表数据。

这是进度50%的SimpleDB版本，只能做一些简单的SQL查询（但它是支持聚合、分组、排序的），后续将增加查询优化和事务处理以及一些其他的功能。

## what you should know

The parser is relatively full featured (including support for SELECTs, INSERTs, DELETEs, and transactions), but does have some problems and does not necessarily report completely informative error messages. Here are some limitations to bear in mind:

1. You must preface every field name with its table name, even if the field name is unique (you can use table name aliases, as in the example above, but you cannot use the AS keyword.)
2. Nested queries are supported in the WHERE clause, but not the FROM clause.
3. No arithmetic expressions are supported (for example, you can't take the sum of two fields.)
4. At most one GROUP BY and one aggregate column are allowed.
5. Set-oriented operators like IN, UNION, and EXCEPT are not allowed.
6. Only AND expressions in the WHERE clause are allowed.
7. UPDATE expressions are not supported.
8. The string operator LIKE is allowed, but must be written out fully (that is, the Postgres tilde [~] shorthand is not allowed.)

# Test SQL code

	SELECT p.title
	FROM papers p
	WHERE p.title LIKE 'selectivity';

	SELECT p.title, v.name
	FROM papers p, authors a, paperauths pa, venues v
	WHERE a.name = 'E. F. Codd'
	AND pa.authorid = a.id
	AND pa.paperid = p.id
	AND p.venueid = v.id;

	SELECT a2.name, count(p.id)
	FROM papers p, authors a1, authors a2, paperauths pa1, paperauths pa2
	WHERE a1.name = 'Michael Stonebraker'
	AND pa1.authorid = a1.id 
	AND pa1.paperid = p.id 
	AND pa2.authorid = a2.id 
	AND pa1.paperid = pa2.paperid
	GROUP BY a2.name
	ORDER BY a2.name;

上面是CS186提供的测试代码，也可以使用自己的SQL代码，如：

	select * from authors;

## Code

所有的源码在**CS186-proj2**里