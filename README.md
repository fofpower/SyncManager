# 数据库同步脚本使用说明

[toc]

## 一. 环境要求
linux系统, 安装python3.5, pip, git


## 二. 安装脚本

root权限运行

```
git clone https://github.com/fofpoer/SyncManager.git

# 进入SyncManager目录
python3 setup.py install

```


## 三. 配置文件

1. 运行脚本前需先创建配置文件, **脚本需在配置文件目录下运行**, 配置文件包括。

    ```
    # 本地数据库配置
    local_db_setting.yaml
    
    # 源数据库配置
    source_db_setting.yaml
    
    # 需要同步的库, 及源库与本地库的库名映射
    schema_config.json
    
    # 需要同步的表及表的主键(可自动生成)
    database.json
    ```

2. 在local_db_setting.yaml和source_db_setting.yaml文件中配置数据库连接参数

3. 在schema_config.json文件中:

    ```
    {
        # 需要同步的源库名
        "schema":
            [
                SOURCE_SCHEMA_NAME
            ]
    
        # 配置库名映射, 键是源库名, 值为本地库名。
        "schema_map":
            {
                SOURCE_SCHEMA_NAME:LOCAL_SCHEMA_NAME
            }
    }
    ```

4. 在本地数据库中运行sync_status.sql文件, 新建4张同步状态表


## 四. 启动脚本

进入配置文件目录, 运行
```
smyt-sync-task -a sync
```

**参数说明**

|参数名|说明|是否必须|缺省值|
|:---|:---|:---|:---|
|-a, --action|正常同步: sync, 全表检查: check, 更新: update, 删除: remove|是|无|
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

**由于校对主键耗时很长, 建议：目前每日只运行更新, 每隔一段时间运行全表检查或启用drop_deleted正常同步**

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

## 六. 配置定时任务

1. 查询smyt-sync-task的绝对路径

    ```
    whereis smyt-sync-task
    # path_to_smyt_sync_task
    PATH_TO_TASK
    ```

2. 新建shell脚本

    ```
    #!/usr/bin/env bash
    echo "Sync Job start!"
    
    # 视资源占用情况, 酌情配置可选参数
    DROP_DELETE='off'
    CONCURRENCY=4
    PROCESS=4
    RECORD_LIMIT=2000
    KEY_LIMIT=50
    
    cd path_to_config_dir #填写配置目录路径
    PATH_TO_TASK -a sync  -c ${CONCURRENCY} -rl ${RECORD_LIMIT} -kl ${KEY_LIMIT} -p ${PROCESS} -d ${DROP_DELETE}
    echo "Sync Job done!"
    
    ```

3. 创建crontab任务, 定时运行shell脚本
