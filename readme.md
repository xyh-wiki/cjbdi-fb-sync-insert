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
# 指定 spark 安装路径
export SPARK_HOME=/data/app/spark-3.3.2

# 指定配置文件路径
export CONFIG_FILE=/data/test/fb-sync/application.yaml

# 指定运行 jar 包路径
APP_JAR=/data/test/fb-sync/cjbdi-fb-sync-compare-1.0.1.jar

# 指定运行 main 方法
APP_MAIN_CLASS=com.cjbdi.job.DataSyncSparkJob

# spark 项目启动脚本
submit_cmd="$SPARK_HOME/bin/spark-submit --master yarn \
--queue xyh \
--driver-memory 512m \
--driver-cores 1 \
--executor-memory 1g \
--num-executors 1 \
--executor-cores 2 \
--class $APP_MAIN_CLASS \
$APP_JAR"

# 打印提交命令
echo "正在提交 spark 任务>> "
echo "$submit_cmd"

# 执行提交命令
eval "$submit_cmd"
````

# 四、使用说明
无
# 五、配置
```yaml
checkpoint.dir=hdfs://192.168.50.37:8020/chk
kafka.server=192.168.50.37:9092
kafka.topic=t-stm
kafka.input.groupId=s-20
warehouse.path=hdfs://192.168.50.37:8020/user/hive/warehouse
postgres.url=jdbc:postgresql://192.168.50.37:5432/fb15_00
postgres.username=postgres
postgres.password=121019
postgres.index.url=jdbc:postgresql://192.168.50.37:5432/db_index
postgres.index.username=postgres
postgres.index.password=121019
dbid=00
```

# 六、贡献

# 七、作者

# 八、历史版本
## v-1.0.0（2024-03-15）
初始版本发布

## v-1.1.0 (2024-05-12)
- **新功能增加**
  - 异常日志记录新增 `index_log.t_log` 日志表
  - 新增 `db_fy` 全库每天全量拉取
- **程序优化**
  - 优化 `yaml` 配置文件加载读取方式， 修改为环境变量加载
  - 重分区优化：`db_msys` 和 `db_sczx` 两个案件类型写 hdfs 的分区数修改为 **100**， 其他案件类型写 hdfs 的分区数修改为 **10**
  - 程序提交方式脚本优化
  - 删除 `spark-core_2.12` 和 `spark-sql_2.12` 依赖 （`spark-hive` 中已包含两者）
- **bug 修复**

# 九、其他信息
1. `db_fy` 数据库下主表不存在`c_stm`， 无法进行常规增量拉取, 每天进行全量拉取
2. 日志表和索引表在同一个数据库下， 表名为 `index_log.t_log`
# 十、FAQ(常见问题)

# 十一、示例

