# 数据库同步脚本使用说明


# 一. 环境要求
- python3.5
- requirements.txt中的python包


# 二. 配置文件
目录：smyt_sync_manager/config

## 1，数据库配置
文件：db_setting.yml
```angular2html
db_source:
  host: yourSourceIP
  port: yourSourcePort
  user: yourUser
  passwd: yourPwd

db_target:
  host: yourTargetIP
  port: yourTargetPort
  user: yourUser
  passwd: yourPwd
```
其中：
1，db_source为源数据库的配置信息;
2，db_target为目标数据库的配置信息;


## 2，需要同步的源库与目标库的库名映射

schema_map为源库与目标库的库名映射，键是源库名, 值为目标库名

```python
{
  # 配置库名映射, 键是源库名, 值为目标库名
  "schema_map": {
    "product": "product_sm",
    "product_mutual": "product_gm"
  }
}
```

## 3，需要同步的表及表的主键
> 可自动生成，生成后可配置需要同步的表

例如：product库会生成product.json文件
```python
{
  "org_monthly_index_static": [
    "org_id",
    "statistic_date",
    "index_id"
  ],
  "fund_weekly_index": [
    "index_id",
    "statistic_date",
    "index_method",
    "data_source"
  ],
}
```

## 4，创建同步状态表
在目标数据库中运行sync_status.sql文件, 新建4张同步状态表

## 5，创建目标库的表结构
将源库的表结构在目标库中全部创建


# 三. 手动启动脚本

命令窗口进入脚本目录, 运行
```
# python3.5 main.py -a sync
```

**参数说明**

|参数名|说明|是否必须|缺省值|
|:---|:---|:---|:---|
|-a, --action|正常同步: sync, 全表检查: check, 更新: update, 删除: remove, 结构检查: structure|是|无|
|-s, --schema|指定数据库|更新及删除时必须传入|None|
|-t, --table|指定表, 需先指定数据库|单表更新须指定|None|
|-d, --drop_deleted|正常同步时是否校对主键, 移除源库中已删除的记录, on 或 off|否|off|
|-kl, --key_limit|每次校对主键数量|否|50|
|-rl, --record_limit|每次增改操作的读取数据条数上限|否|2000|
|-c, --concurrency|设置并行线程上限|否|4|
|-p, --processes|设置进程上限|否|4|

**更新选项说明**

- **正常同步**：先增改新记录, 再根据是否启用drop_deleted, 选择是否移除源库中删除的记录（**启用drop_deleted时, 耗时较长**）
- **全表检查**：校对主键, 增改或删除记录（**耗时较长**）
- **更新**：只增改指定库中的记录
- **删除**: 校对指定库中的主键, 删除记录（**耗时较长**）
- **表结构校对**: 检查表结构是否有变化


**由于校对主键耗时很长, 建议：目前每日只运行更新, 每隔一段时间运行全表检查或启用drop_deleted正常同步**

# 四. 配置定时任务

## 1. 参数
采用crontab语法

```
0    2    *    *    6
*    *    *    *    * 
-    -    -    -    - 
|    |    |    |    |    
|    |    |    |    |    
|    |    |    |    +----- day of week (0 - 7) (Sunday=0 or 7)
|    |    |    +---------- month (1 - 12)
|    |    +--------------- day of month (1 - 31)
|    +-------------------- hour (0 - 23)
+------------------------- min (0 - 59)
```

## 2，运行

命令窗口在当前目录下执行
```
# nohup python3 cron.py "0 17 * * *"
```

> nohup命令将python脚本在后端执行，输出在nohup.out文件中


## 五. 提示信息

1. 更新日志：

    **level说明**

    level定位脚本运行到何处

    |level|位置|
    |:---|:---|
    |UPDATE-0|选中主键从源表取数据, 插入到本地表|
    |DELETE-0|选中主键, 删除本地表中的记录|
    |INSERT-0|将增改数据插入本地表|
    |UPDATE-1|更新本地表|
    |DELETE-1|从本地表中删除记录|
    |CHECK-1|全表检查, 增改删记录|
    |TABLE_STRUCTURE|表结构检查|


2. 错误日志 

- level为TABLE_STRUCTURE的错误日志, 请与我们联系以对表结构进行调整。
- 若启动时直接抛出UpdateError('StatusError', ), 请检查之前运行的任务是否已正常完成。**如需要强制启动, 请进入配置文件目录删除?_status.json文件**
- 若遇到其他错误, 请联系我们, 并将错误日志发送给我们。


