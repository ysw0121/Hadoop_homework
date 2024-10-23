# Hadoop_homework

## 仓库说明
1. 结构与要求基本一致，文件名带`stock`的是与`任务1`有关的，带`word`的是与`任务2`有关的
2. `src`和`target`分别为虚拟机本地的代码及编译后的文件，push时去掉了`.class`的依赖文件；`src_target_in_docker`文件夹是代码在docker容器中编译结果，与虚拟机本地基本一样，除了少了一些中文注释。
3. 由于任务进行了多次输出，均进行保留，在`output`中的`hw5_output_docker_all_test`中，`fin`开头的两个文件夹为最终成功的输出结果，位于`part-r-00000`中。
4. 由于单文件运行，没有进行maven管理项目，直接编译的，因此没有`pom.xml`文件。

## 任务重述
1. 统计数据集上市公司股票代码（“stock”列）的出现次数，按出现次数从⼤到⼩输出，输出格式为“<排名>：<股票代码>，<次数>”；
2. 统计数据集热点新闻标题（“headline”列）中出现的前100个⾼频单词，按出现次数从⼤到⼩输出。要求忽略⼤⼩写，忽略标点符号，忽略停词（stop-word-list.txt）。输出格式为“<排名>：<单词>，<次数>”。

## 关键命令
```
· 启动docker容器
$ sudo docker start h01 h02 h03 h04 h05
$ sudo docker exec -it h01 /bin/bash
· docker与本地互传文件
本地 -> docker
root@your-vm# docker cp your/local/file <image-name>:your/docker/dest-folder
例如：
root@siwenyu-virtual-machine: home/siwenyu/desktop/Hadoop_homework# docker cp data/analyst ratings.csv h01:/$HADOOP_HONE/hw5_local_input

docker -> 本地
root@your-vm: # docker cp <container-name>:your/docker/dest-folder your/local/file

· 编译java文件
javac -cp $(hadoop classpath) wordcount.java

· 打包成jar文件，依赖文件（.class）都在一个目录下，可以直接运行
jar -cvf wordcount.jar .

· hadoop下执行任务，以下操作均在$HADOOP_HOME路径下运行
// 创建HDFS输入文件夹并上传数据集
hdfs dfs -mkdir -p /user/hadoop/hw5_input
hdfs dfs -put hw5_local_input/analyst_ratings.csv /user/hadoop/hw5_input/

// 上传stop-word-list文件到HDFS
hdfs dfs -put hw5_local_input/stop-word-list.txt /user/hadoop/

// 运行任务1
hadoop jar hw5_build/StockCount.jar StockCount /user/hadoop/hw5_input /user/hadoop/hw5_stock_output
// 运行任务2
hadoop jar hw5_build/wordcount.jar wordcount /user/hadoop/hw5_input /user/hadoop/hw5_word_output /user/hadoop/stop-word-list.txt

// 取出output到docker本地
hdfs dfs -get /user/hadoop/hw5_word_output hw5_local_output

```
## 任务1
### 运行截图（由于实验时忘记截图，重新运行后所截图如下）
![stock count](img/stock.png)
### 设计思路
1. Map阶段：`StockCountMapper`类，对输入的csv数据按逗号分割，用最后一列，提取出股票代码，将其作为键，把1作为值，代表出现一次。
2. Reduce阶段：`StockCountReducer`类接收`Mapper`输出的键值对，统计每个股票代码的出现次数，并在`cleanup`方法中将结果按出现次数从大到小排序后输出。
### 性能与拓展性
- 由于Mapper只是简单地分割字符串并输出，性能开销较小。但是只是简单地对数据进行逗号分隔，对最后一列要求一定是`stock`列，程序被写死，一旦遇到数据不匹配或者数据缺失、格式不规范等情况，很可能运行错误。
- Reducer使用HashMap进行计数，时间复杂度为`O(1)`，性能尚可，但是所有数据存储在`mapper`中再给到`reducer`，如果股票种类特别多，`reducer`还只有一个，排序过程中单`reducer`会出现性能短板。`cleanup`中的排序操作复杂度应该为`O(nlogn)`。
### 可改进的地方
1. 错误处理：当前代码在处理输入参数和文件路径时没有进行详细的错误处理，可以增加更多的异常处理逻辑以提高程序的健壮性。
2. 排序优化：`cleanup`方法中的排序操作可以通过使用更高效的排序算法（如`reducer`的二级排序）或并行排序来优化。也可以增加`reducer`个数，由不同reducer处理数据
3. 性能提升：在`map`和`reduce`过程中间加入`partition`或`combine`，对数据进行预处理或者分类，都可以减少`reducer`的压力。

## 任务2
### 运行截图（由于实验时忘记截图，重新运行后所截图如下）
![stock count](img/word.png)
### 设计思路
1. TokenizerMapper：负责将输入文本分割成单词，并对每个单词进行处理。在这个类中，定义了一个`setup`方法，用于在映射操作开始前读取停用词列表，并将其存储在一个`List<String>`中。在`map`方法中，对输入的每一行文本进行处理，去掉标点符号，转换为小写，并忽略停用词和空字符串，然后将处理后的单词作为键，1作为值输出，代表第一次出现。
2. IntSumReducer：负责对映射阶段输出的键值对进行归约操作。在这个类中，定义了一个`reduce`方法，用于累加相同键的值。此外，还重写了`cleanup`方法，在归约操作结束后，将结果写入到输出上下文中。这里使用了一个`Map<String, Integer>`来存储单词及其出现次数，并在`cleanup`方法中对结果进行排序，输出出现频率最高的前100个单词。
3. main：配置了Hadoop作业的相关参数，包括`设置停用词列表的路径`，这里设置为在第三个语句命令上`（args[2]）`、指定Mapper和Reducer类、设置输出键值对的类型等。最后，通过调用`job.waitForCompletion(true)`来启动。

### 性能与拓展性
- `Mapper`使用正则表达式进行单词分割，基本和`任务1`一样的读取方式。但是用`List`存储停词表，`contains`函数运行是线性时间运行，因此数据量过大会速度很慢。可以通过增加更多的`Mapper`和`Reducer`来处理更大规模的数据。
- `Reducer`使用HashMap进行计数，插入和查找操作的时间复杂度为O(1)。`cleanup`中的排序，可以用于中小规模数据，但对于大规模数据可能会成为性能瓶颈。可以通过优化排序算法或使用外部排序来提高大规模数据的处理效率。
### 可改进的地方
1. 在`Mapper`中，每次处理一行数据时都会读取停词表，如果无意义的词表很大会导致循环处理较慢。可以考虑将停词表加载到内存中或者用更高效的数据结构如HashSet，避免重复读取。
2. 在`Reducer`中，使用了Java的HashMap来存储中间结果，这可能会导致内存占用过高。可以考虑使用更高效的数据结构或者分批写入磁盘。还有排序问题，在`任务1`中已说明。
3. 基于书上`WordCount`代码写的，有些类名没有改，与本次作业实际有所区别，对理解代码会有一点障碍。
4. 错误处理：当前代码中对文件读取的错误处理较为简单，可以增加更多的错误处理逻辑，提高程序的健壮性。
