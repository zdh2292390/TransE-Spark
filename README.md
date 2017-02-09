# TransE-Spark

TransE 算法在Spark上的实现,代码思想如下：

读取数据和预处理

对训练样本并行化产生  samplesRDD
初始化参数向量，entityVec   relationVec，这两个都在master，因此要把master内存设足够大

for i from 1 to num_epochs:
广播最新的 entityVec   relationVec

for j from 1 to num_batches：

1. 对traindataRDD sample（batchsize）产生sampledRDD

2. 对sampledRDD mapPartitions，并行的计算被抽中样本的梯度（要用到广播的参数向量，广播开销可能很大），更新对应的参数，每个分区产生对应的参数向量，实体和关系分别用两个map保存，键值为实体和关系的id，指向对应的向量

3. 合并所有分区的两个map，产生两个最终的map，然后collect到master

end for

把最终的两个map 更新到master的entityVec和relationVec中

end for

Evaluation Results
==========
FB15k,nepoch=1000, veclen=100

| Model      |     MeanRank(Raw) |   MeanRank(Filter)   |	Hit@10(Raw)	| Hit@10(Filter)| Time(s)|
| :-------- | --------:| :------: | :------: |:------: |:------: |
| Parallel-TransE(12 thread) |    220 | 71.6 |  48.4 | 73.3| 829 |
| TransE-Spark(16 thread) |    218.7 | 78.1 |  46.5 | 69.6| 3300 |
| TransE-THU  |    205 |  63 |  47.9 | 70.2 | 10361 |
