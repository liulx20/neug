LDBC SNB (Social Network Benchmark) Interactive 是国际关联数据基准委员会（LDBC）制定的、用于评估图数据库在事务型在线查询（OLTP）场景性能的行业标准基准。它模拟社交网络业务，覆盖多跳、短路径等复杂查询以及数据增删改查。NeuG 在 LDBC SNB Interactive 基准测试中有世界领先的性能表现。本文给出从零开始跑通 NeuG + LDBC SNB Interactive benchmark的全流程。
### 环境准备
建议使用两台机器：

- **Server机器**：编译并运行 NeuG（`bulk_loader`、`rt_server`），负责数据序列化与服务提供。
- **Client机器**：编译并运行压测 Driver，向 Server 发起基准测试请求。

#### 依赖安装
```bash
sudo apt update
sudo apt install maven git python3 python3-pip cmake gcc g++ wget openjdk-11-jdk -y
```

建议先在两台机器分别设置工作目录变量：

```bash
export NEUG_WORKSPACE=/path/to/neug
export DRIVER_WORKSPACE=/path/to/driver
```

#### NeuG编译
```bash
git clone https://github.com/alibaba/neug.git ${NEUG_WORKSPACE}
cd ${NEUG_WORKSPACE}
cmake -S . -B build -DBUILD_EXECUTABLES=ON -DBUILD_HTTP_SERVER=ON
cmake --build build --target rt_server bulk_loader -j
git clone https://github.com/GraphScope/flex_ldbc_snb.git --single-branch json ${DRIVER_WORKSPACE}
```
该步骤在 **Server机器** 执行。

#### Driver编译
```bash
git clone https://github.com/alibaba/neug.git ${NEUG_WORKSPACE}
cd ${NEUG_WORKSPACE}/tools/java_driver && mvn clean install -DskipTests
git clone https://github.com/GraphScope/flex_ldbc_snb.git --single-branch json ${DRIVER_WORKSPACE}
cd ${DRIVER_WORKSPACE}/driver && mvn clean package -DskipTests
```
该步骤在 **Client机器** 执行。

### 数据处理
#### 数据集生成
LDBC官方提供了[DataGen](https://github.com/ldbc/ldbc_snb_datagen_hadoop)工具，用于生成大规模社交网络数据。这些数据包括人、帖子、评论、地理位置、组织和其他一些社交网络的典型实体和关系。
* 下载ldbc_snb_datagen_hadoop
```bash
git clone https://github.com/ldbc/ldbc_snb_datagen_hadoop.git
cd ldbc_snb_datagen_hadoop && git checkout v1.0.0
```
* 安装hadoop
``` bash
wget https://archive.apache.org/dist/hadoop/core/hadoop-3.2.1/hadoop-3.2.1.tar.gz
tar xf hadoop-3.2.1.tar.gz
export HADOOP_CLIENT_OPTS="-Xmx256G"
export HADOOP_HOME=`pwd`/hadoop-3.2.1
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
```
* 创建params.ini文件，并填充如下内容
```bash
ldbc.snb.datagen.generator.scaleFactor:snb.interactive.10
ldbc.snb.datagen.serializer.numUpdatePartitions:48

ldbc.snb.datagen.serializer.dynamicActivitySerializer:ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.activity.CsvCompositeDynamicActivitySerializer
ldbc.snb.datagen.serializer.dynamicPersonSerializer:ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.person.CsvCompositeDynamicPersonSerializer
ldbc.snb.datagen.serializer.staticSerializer:ldbc.snb.datagen.serializer.snb.csv.staticserializer.CsvCompositeStaticSerializer
ldbc.snb.datagen.serializer.outputDir:/path/to/datagen-out
```
常见字段说明：

- `ldbc.snb.datagen.generator.scaleFactor`：数据规模参数。示例 `snb.interactive.10` 表示生成 SF10 数据；可按需要改为 `snb.interactive.30`、`snb.interactive.100` 等。
- `ldbc.snb.datagen.serializer.outputDir`：DataGen 输出目录。生成后的 Initial datasets、Update Streams、Parameters 都会写到该目录。

建议：`outputDir` 使用绝对路径，并确保目标磁盘空间充足。生成的数据目录结构如下：

```
datagen-out
     ├─────── hadoop
     ├─────── social_network
     │          ├── dynamic
     │          │       ├── comment_0_0.csv
     │          │       ├── comment_hasCreator_person_0_0.csv
     │          │       ├── ...
     │          │       └── post_isLocatedIn_plac_0_0.csv
     │          ├── static
     │          │       ├── organisation_0_0.csv
     │          │       ├── organisation_isLocatedIn_place_0_0.csv
     │          │       ├── ...
     │          │       └── tagclass_isSubclassOf_tagclass_0_0.csv
     │          ├── updateStream_0_0_forum.csv
     │          ├── updateStream_0_0_person.csv
     │          ├── updateStream_0_1_forum.csv
     │          ├── ...
     │          └── updateStream_0_48_person.csv
     └──────── substitution_parameters
                ├── interactive_1_param.txt
                ├── interactive_2_param.txt
                ├── ...
                └── interactive_14_param.txt
```

其中：

- `hadoop`：DataGen 运行过程的中间产物目录，通常不直接参与后续加载与压测。
- `social_network`：Initial datasets目录，后续会被 `bulk_loader` 使用。
  - `dynamic/`：动态图数据（如 `person`、`post`、`comment` 及其关系边），包含点文件与边文件。
  - `static/`：静态维度数据（如 `place`、`tag`、`tagclass`、`organisation` 及其关系边）。
  - `updateStream_*`：增量更新流数据，用于 Interactive 场景下的更新操作回放与压测。
- `substitution_parameters`：交互查询参数文件（`interactive_*.txt`），Driver 在执行 benchmark 时会使用这些参数驱动查询。

在 NeuG 的加载流程中，`import.yaml` 的 `loading_config.data_source.location` 通常应指向 `social_network` 所在路径；例如目录为 `.../datagen-out/social_network` 时，将该路径配置到 `location` 字段即可。

* 数据生成
```bash
./run.sh
```
运行结束会生成相应规模的Initial datasets, UpdateStreams和Parameters。
另外也可以直接在[SNB Interactive v1](https://ldbcouncil.org/benchmarks/snb/datasets/#snb-interactive-v1)界面下载生成好的不同规模的数据和参数，包括Initial datasets（CsvComposite & StringDateFormatter），Update Streams (48 parts)和Parameters。
#### 预处理

需要先对数据模型进行一些调整。
```bash
pip3 install neug
python3 ${DRIVER_DIR}/tools/comment_add_creation_date.py datagen-out/social_network/dynamic datagen-out/social_network/dynamic/comment_hasCreator_person_0_0_creation_date.csv
python3 ${DRIVER_DIR}/tools/post_add_creation_date.py datagen-out/social_network/dynamic datagen-out/social_network/dynamic/post_hasCreator_person_0_0_creation_date.csv
python3 ${DRIVER_DIR}/tools/comment_reorder.py datagen-out/social_network/dynamic datagen-out/social_network/dynamic/comment_0_0_rod.csv
python3 ${DRIVER_DIR}/tools/post_reorder.py datagen-out/social_network/dynamic datagen-out/social_network/dynamic/post_0_0_rod.csv
```
其中：
- `comment_add_creation_date.py`：从 `dynamic/comment_0_0.csv` 中提取 `creationDate`，并写入 `comment_hasCreator_person` 边文件，生成带时间戳的边文件。
- `post_add_creation_date.py`：从 `dynamic/post_0_0.csv` 中提取 `creationDate`，并写入 `post_hasCreator_person` 边文件，生成带时间戳的边文件。

注意事项：
- 请确保 `${DRIVER_DIR}` 指向 `flex_ldbc_snb` 仓库根目录（包含 `tools/` 子目录）。
- 预处理后输出文件名需与 `import.yaml` 中 `inputs` 配置保持一致；若你修改了文件名，请同步更新 `import.yaml`。

#### 序列化
bulk_loader提供数据序列化的能力，能将csv等格式的源数据处理成NeuG接受的二进制序列。
```bash
${NEUG_WORKSPACE}/build/tools/utils/bulk_loader -g ${DRIVER_WORKSPACE}/configs/graph.yaml \
        -l ${DRIVER_WORKSPACE}/configs/import.yaml -d $DATA_DIR [-p n]
```
参数说明如下：

- `-g`：图Schema配置文件，定义点类型、边类型、属性类型和主键。
- `-l`：导入配置文件，定义原始数据位置、文件格式，以及每个CSV文件与点/边类型、属性列之间的映射关系。
- `-d`：序列化输出目录，bulk_loader会将生成的二进制图数据写入该目录，后续启动server时 `rt_server -d` 也需要指向这个目录。
- `-p`：可选参数，指定加载并行度；不指定时默认值为1。

需要注意，`-d`指定的是**输出目录**，不是原始CSV目录。原始LDBC数据集目录需要在`import.yaml`中的`loading_config.data_source.location`里配置，例如：

```yaml
loading_config:
  data_source:
    scheme: file
    location: /path/to/social_network
```




### 性能测试

#### hugepages配置 [可选]
为了复现 NeuG 在 LDBC SNB Interactive 场景的极致性能，需要在 **Server机器** 上配置 hugepages。针对不同规模的数据集可配置不同数量，当前 SF300 示例配置如下：
```bash
echo 0 | sudo tee /proc/sys/vm/nr_hugepages
echo 219376 | sudo tee /proc/sys/vm/nr_hugepages
```
#### 启动NeuG服务
```bash
${NEUG_WORKSPACE}/bin/rt_server -d $DATA_DIR -m 3 -s 192 --host 0.0.0.0
```
参数说明如下：

- `-d`：数据目录（必选），指向 `bulk_loader` 序列化完成后的输出目录。
- `-m`：内存级别（`memory-level`），默认 `1`, 支持hugepages的情况下可以指定3。
- `-s`：并发线程数（`shard-num`），默认 `9`。
- `--host`：服务监听地址，默认 `127.0.0.1`；压测场景通常配置为 `0.0.0.0`。


示例：

```bash
${NEUG_WORKSPACE}/bin/rt_server \
  -d $DATA_DIR \
  -m 3 \
  -s 192 \
  --host 0.0.0.0 
```

启动成功后日志中会出现类似 `Brpc server started on : 0.0.0.0:10000` 的信息。

#### validation测试

validation测试主要验证查询结果正确性。LDBC官方提供了sf0.1-sf10规模的validation数据，包含查询和预期results数据，可以在[validation_params-interactive-v1.0.0-sf0.1-to-sf10](https://datasets.ldbcouncil.org/interactive-v1/validation_params-interactive-v1.0.0-sf0.1-to-sf10.tar.zst)进行下载，下载解压后会得到`validation_params-sf10.csv`等文件。

在${DRIVER_WORKSPACE}/driver/neug/driver/benchmark-{Scale-Factor}.properties中补充下面配置：

```yaml
url=http://IP:PORT
queryDir=${DRIVER_WORKSPACE}/queries/
validate_database=/path/to/validation_params-sf*.csv
ldbc.snb.interactive.parameters_dir=datagen-out/substitution_parameters
```
字段说明：
- `url`：Server 端 `rt_server` 的访问地址，格式为 `http://<server_ip>:<port>`。
- `queryDir`：查询模板目录（Client 机器本地路径），用于加载 Interactive 查询语句。
- `validate_database`: 刚才下载的validation_params-sf*.csv目录, 用于加载查询参数。
- `ldbc.snb.interactive.parameters_dir`：参数目录（Client 机器可访问路径），应包含 `interactive_*_param.txt` 文件。

启动validate
```bash
cd ${DRIVER_WORKSPACE}/driver 
./neug/driver/validate.sh 
```
validate执行完成后，会报告查询结果是否通过验证，若否，会给出没有通过验证的查询数量，具体查询参数、预期结果等信息。



#### 基准测试

基准测试同样需要编辑 ${DRIVER_WORKSPACE}/driver/neug/driver/benchmark-{Scale-Factor}.properties，完成下面的一些配置：
```yaml
url=http://IP:PORT
# path to cypher queries
queryDir=${DRIVER_WORKSPACE}/queries/
# path to updateStream*.csv
ldbc.snb.interactive.updates_dir=datagen-out/social_network
# path to substitution parameters
ldbc.snb.interactive.parameters_dir=datagen-out/substitution_parameters

time_compression_ratio=0.0015
operation_count=610000000
warmup=162000000
```

字段说明：
- `url`, `queryDir`, `ldbc.snb.interactive.parameters_dir` 参数含义与valdiation一致。
- `ldbc.snb.interactive.updates_dir`：更新流目录（Client 机器可访问路径），应包含 `updateStream_*.csv` 文件。
- `time_compression_ratio`：时间压缩比（TCR），值越小压力越大、吞吐潜力越高。
- `operation_count`：本轮总操作数（预热 + 正式测试）。
- `warmup`：预热操作数，需满足 `warmup < operation_count`。



* 时间压缩比 (TCR)
TCR用于压缩查询开始时间之间的间隔，来提高或降低查询频率，从而使系统能够在给定工作负载中达到其最大吞吐。该数值越小，代表的压缩比越高。不同规模的数据集使用的TCR存在差异，当前实验使用的TCR配置如下:

| | SF30 | SF100 | SF300|
|--|--|--|--|
|time_compression_ratio| 0.001|0.0015|0.0046|
|operation_count|272000000|610000000	|690800000|
|warmup	|64000000	|162000000	|188400000|

其中operation count和warm随TCR进行调节，benchmark测试分为预热（warmup）和性能测试（benchmark）两个阶段，相应的查询数量要满足预热阶段至少持续30分钟，性能测试阶段跑满2小时，但不超过2小时15分钟。

在 Server 端完成数据载入后，可在 **Client机器** 上启动压测。
```bash
cd ${DRIVER_WORKSPACE}/driver/
./neug/driver/benchmark.sh driver/benchmark-${SCALE_FACTOR}.properties
```
跑完一轮正常的测试大概耗时3小时，结束后会生成对应的性能结果报告，并对结果进行校验，报告这轮测试结果是否有效；性能结果是否有效取决于查询准时率。
* 查询准时率
LDBC interactive要求每一个查询要尽量在1s内返回，否则视为超时。基准测试要求在一轮测试中，超时查询比例不能超过5%，否则认为性能结果无效；因此在测试过程中不能通过无限调高TCR来达到更高吞吐，在超时查询比例超出要求时，只能上调TCR，降低查询频率，同时线性下调operation count和warmup数值。
### ACID测试

暂不支持

### 持久化测试

持久化测试要求数据库出现瞬时故障宕机时能提供数据持久性保障。
持久性测试包括服务故障、服务重启、服务恢复等流程，通过`kill -9`或者物理关机等方式模拟服务故障，在服务重启后验证是否存在数据丢失。
按基准测试一致的方式启动数据库，运行接近两小时的时刻，数据库将被物理中断，之后重启，并验证driver日志中最后一次写操作是否存在于数据库中。
**STEP 1** 启动NeuG服务，载入SF30数据
**STEP 2** 启动Driver，开始基准测试
**STEP 3** 120 mins后杀掉服务
```bash
(sleep 120; pkill -9 rt_server) &
```
**STEP 4** 使用和STEP 1相同的命令，重启NeuG服务
**STEP 5** 执行测试
```bash
export NEUG_URI=http://IP:PORT
python3 test_recovery.py ${DRIVER_WORKSPACE}/driver/neug/results/LDBC-SNB-results_log.csv datagen-out/social_network sf30
```

### 测试结果

#### 测试环境

|机器型号|Alibaba Cloud ecs.g8a.48xlarge|
|--|--|
|操作系统|Ubuntu 20.04.6 LTS 5.4.0-204-generic|
|CPU| AMD EPYC 9T24 96-Core Processor, 192核|
|内存|768GB|
|磁盘｜  NVMe ESSD, 4TB|

#### 不同规模数据集上吞吐表现
|datasets|throughput|
|--|--|
|SF30|37,654.25|
|SF100|81,715.58|
|SF300|94,201.80|