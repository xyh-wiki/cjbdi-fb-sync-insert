# 一、项目名称：法标库全增量同步查询入库

# 二、项目描述
- 本项目主要用于法标库全增量数据汇聚的增量数据查询及入库操作
- 技术框架采用 flink 实现
## 法标库增量同步
1. 读取 `kafka` 中每天的增删改记录
2. 根据 `c_stm` 去法标库进行增量拉取数据
3. 将拉取到的增量数据写入 `hdfs` 上
4. 拉取成功，更新索引表

# 三、安装说明
本项目采用 `flink` 框架部署模式，部署环境为 `flink on yarn`, 基本启动脚本如下， 具体其他运行配置参数需要根据部署服务器状况和数据量安装部署
```shell script
#!/bin/bash

# 设置 Flink 安装路径
FLINK_HOME=/data/app/flink-1.17.2

# 定义任务相关参数
JAR_FILE=/data/xyh/14-服务部署/04-法标库同步/jars/cjbdi-fb-sync-insert-1.1.0.jar
MAIN_CLASS=com.cjbdi.job.LoadFb
export CONFIG_PATH=/data/xyh/14-服务部署/04-法标库同步/config/application-00.yaml

# 执行 Flink 任务
$FLINK_HOME/bin/flink run -t yarn-application -d \
-Dyarn.application.name=sync-inc-insert-00 \
-Dyarn.application.queue=xyh \
--allowNonRestoredState \
$JAR_FILE
````

# 四、使用说明
无
# 五、配置
```yaml
checkpoint:
  dir: hdfs://192.168.50.37:8020/chk

kafka:
  server: 192.168.50.37:9092
  topic: t-stm
  input:
    groupId: s-20

warehouse:
  path: "hdfs://192.168.50.37:8020/user/hive/warehouse"
  username: "root"
  database: "db_ods15.db"

postgres:
  source:
    url: "jdbc:postgresql://192.168.50.37:5432/fb15_00"
    username: "postgres"
    password: "121019"
    dbid: "00"
  index:
    url: "jdbc:postgresql://192.168.50.37:5432/db_index"
    username: "postgres"
    password: "121019"

job:
  parallelism:
    kafkaSource: 1
    hdfsSink: 1
    indexSink: 1
```

# 六、贡献

# 七、作者

# 八、历史版本

## v-1.2.0(2024-06-26)
- **功能新增**
  1. 新增文书视图表读取和查询

## v-1.1.0 (2024-05-15)
- **新功能增加**
  - 异常日志记录新增 `index_log.t_log` 日志表
- **程序优化**
  - 程序提交方式脚本优化
- **bug 修复**

## v-1.0.0（2024-03-15）
初始版本发布

# 九、其他信息
1. `db_fy` 数据库下主表不存在`c_stm`， 无法进行常规增量拉取, 每天进行全量拉取
2. 日志表和索引表在同一个数据库下， 表名为 `index_log.t_log`
# 十、FAQ(常见问题)

# 十一、示例

