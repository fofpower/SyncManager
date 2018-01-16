# 脚本说明

## 一. 环境

1. Python环境

    python 3.5

    ```
    pip install -r requirements.txt
    ```
2. 创建日志表

    运行私募库内运行product.sql，公募库内运行product_mutual.sql

## 二. 配置

1. 本地数据库

    填写 local_db_setting.yaml 中的本地数据库连接参数

2. 全局参数

    config.setting 中：
    
    SYNC_SCHEMA：选择同步哪些库(全局更改)
    ```
        {
            product: 私募库
            product_mutual: 公募库
        }
    ```
    SCHEMA_DICT：设置库名映射(需设置)
    
    MAIN_KEY_LIMIT： 校对主键时取唯一最左主键个数(根据服务器配置自行调整)
    
    CONCURRENCY： 并行线程数(根据服务器配置自行调整)


## 三. 运行

进入文件目录，查看帮助
```
python chfdb_sync_manager.py -h
```

**参数说明**

|参数名|说明|是否必须|缺省值|
|:---|:---|:---|:---|
|-a，--action|正常同步: sync，全表检查: check，更新: update，删除: remove|是|sync|
|-s，--schema|指定数据库product，product_mutual|更新及删除时必须传入|None|
|-t，--table|指定表, 需先指定数据库|单表更新须指定|None|
|-d，--drop_deleted|正常同步时是否校对主键，移除源库中已删除的记录，on 或 off|否|off|
|-kl，--key_limit|设置MAIN_KEY_LIMIT|否|50|
|-rl，--record_limit|每次增改操作的读取数据条数上限LIMIT_RECORDS|否|2000|
|-c，--concurrency|设置并行线程上限CONCURRENCY|否|4|
|-p，--processes|设置进程上限PROCESS|否|4|



**更新选项说明**

 - **正常同步**：先增改新记录，再根据是否启用drop_deleted，选择是否移除源库中删除的记录（**启用drop_deleted时，耗时较长**）
 - **全表检查**：校对主键，增改或删除记录（**耗时较长**）
 - **更新**：只增改指定库中的记录
 - **删除**: 校对指定库中的主键，删除记录（**耗时较长**）

**由于校对主键耗时很长，建议：目前每日只运行更新，每隔一段时间运行全表检查或启用drop_deleted正常同步**

## 四. 提示信息

1. 更新日志：

    **level说明**
    
    level定位脚本运行到何处
    
    |level|位置|
    |:---|:---|
    |UPDATE-0|选中主键从源表取数据，插入到本地表|
    |DELETE-0|选中主键，删除本地表中的记录|
    |INSERT-0|将增改数据插入本地表|
    |UPDATE-1|更新本地表|
    |DELETE-1|从本地表中删除记录|
    |CHECK-1|全表检查，增改删记录|
    |TABLE_STRUCTURE|表结构检查|


2. 错误日志 

 - level为TABLE_STRUCTURE的错误日志，请与我们联系以对表结构进行调整。
 - 若启动时直接抛出UpdateError('StatusError', )，请检查之前运行的任务是否已正常完成。
 - 若遇到其他错误，请联系我们，并将错误日志发送给我们。
