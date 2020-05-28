# ikv


## 题目

- 某个机器的配置为：CPU 8 cores, MEM 4G, HDD 4T
- 这个机器上有一个 1T 的无序数据文件，格式为 (key_size, key, value_size, value)
- 设计一个索引结构，使得并发随机地读取每一个 key-value 的代价最小
- 允许对数据文件做任意预处理，但是预处理的时间计入到整个读取过程的代价里

## 问题分析

- 数据快速查询需要加上索引，为提高查询速度，索引应尽量全部保存在内存中，通过索引获取value的位置后应该尽量减少查询磁盘的次数
- 原始数据的key和value存储在一起，应拆开分别存储，并按页存储，保证block对齐，以提高读性能

### 索引设计

索引通常通常有几种设计方案：

#### B-Tree
- 查询时间复杂度O(log(mn))
- 由于Mem只有4G，远小于数据文件中的数据量，要把key全部放入内存比较困难，替换页面需要额外读硬盘

### Hash
- 查询时间复杂度O(1)，性能最好
- 需要解决hash冲突的问题，最差情况下需要读多次硬盘获取数据

### Adaptive Radix Tree
- 查询时间复杂度O(k)，取决于key的长度，但是会根据数据内容进行自适应调整，减少树的高度
- 内存占用的空间与数据量无关，增加key的数量不会明显增加内存使用量

针对本题目内存相对总数据量较少的情况，采用Adaptive Radix Tree构建索引。
树的key为数据文件中的key，value为pageId和offset。

## 代码结构

- datafile.go  读取原始数据文件，默认位置为 "/tmp/org.data"，获取到keySize、key、valueSize、value
- indepage.go  读写索引文件，默认位置为 "/tmp/00000000.idx"
- valuepage.go 读写数据文件，默认位置为 "/tmp/00000000.val"
- db.go        启动时加载索引文件到内存，放入索引树，有请求时查询
- server.go    启动server，提供访问接口
- client.go    用户使用的客户端，通过接口获取数据

### 原始数据结构

原始文件的key和value紧凑排列，结构如下：

```
+----------+--------+------------+--------+
| key_size |   key  | value_size |  value |
|  uint32  | []byte |   uint64   | []byte |
+----------+--------+------------+--------+
```

### 索引文件结构

索引文件按页组织数据，每页默认大小为32mb，包含如下字段：
- count      本页数据条目数
- keySizes   每个key的大小
- keyOffsets 每个key在buf中的偏移量
- valPageIds 每个key对应value的pageId
- valOffsets 每个key对应value在page中的偏移量
- buf        key列表，通过keyOffset和keySize访问

```
+--------+----------+------------+------------+------------+--------+
| count  | keySizes | keyOffsets | valPageIds | valOffsets |  buf   |
| uint32 | []uint32 |  []uint32  |  []uint32  | []uint32   | []byte |
+--------+----------+------------+------------+------------+--------+
```

所有在内存中会将key放入Adaptive Radix Tree中，
value使用valPageId和valOffset组成的position，这两个字段都是4byte，每个position只占用8byte。

### 数据文件结构

数据文件按页组织数据，每页默认大小为64mb，包含如下字段：
- count      本页数据条目数
- valSizes   每个value的大小
- valOffsets 每个value在buf中的偏移量
- buf        value列表，通过valOffset和valSize访问

```
+--------+----------+------------+--------+
| count  | valSizes | valOffsets |  buf   |
| uint64 | []uint64 |  []uint64  | []byte |
+--------+----------+------------+--------+
```

## 执行流程

1. 调用 build/indexer 构建索引，处理原始数据。遍历源文件，将key和value 组织成页，分别存储。
2. 调用 build/server 启动服务，从索引文件读取全部key数据，写入Adaptive Radix Tree构建出索引树。
3. 调用 build/client，与server通信，从索引树中获取到key对应value的pageId和offset，再从数据文件中获取value返回。

## 改进方案

- 内存中的position为8B，假如用2GB内存存放，一共可以存储256M个kv对，如果每对kv的平均大小是4KB的话是可以放下的，如果kv的大小较小，则需要有二级索引，即将索引树按hash分组，在内存中维护一部分，根据查询key的值进行替换
- 增加缓冲池，存储最近查询过的value页
- 页中的设计没有考虑大key和value的情况，需要增加溢出页存储
- 完善异常情况处理，增加日志
- 增加配置文件

## 参考资料

[1] [The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases (Specification)](http://www-db.in.tum.de/~leis/papers/ART.pdf)
[2] [A Comparative Study of Secondary Indexing Techniques in LSM-based NoSQL Databases](https://www.cs.ucr.edu/~vagelis/publications/LSM-secondary-indexing-sigmod2018.pdf)
[3] [Bitcask A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf)
[4] [Flavors of I/O](https://medium.com/databasss/on-disk-io-part-1-flavours-of-io-8e1ace1de017)

