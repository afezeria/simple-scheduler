# simple-scheduler

基于postgresql数据库的多节点定时任务调度器

## 功能

- 根据时间间隔执行任务
- 根据cron表达式执行任务

## 快速开始

初始化数据库,执行sql脚本 [sql/table-function-trigger.sql](https://github.com/afezeria/simple-scheduler/blob/main/sql/table-function-trigger.sql)

引入依赖，下载jar包或从源码构建，打包命令：

```
gradlew jar
```

使用示例：

```kotlin
//提供任务实现
val p = ActionProvider { actionName -> { initData -> println(initData) } }
// 创建调度器
val scheduler = Scheduler(
    dataSource = dataSource,
    actionProvider = p,
    maximumPoolSize = 10,
    pollInterval = 20,
)
//创建按时间间隔执行的计划
scheduler.createBasicPlan(
    name = "test1",
    intervalTime = 2,
    actionName = "abc",
    timeout = 10,
    startTime = LocalDateTime.now(),
    execAfterStart = true,
)
//创建按cron表达式执行的计划
scheduler.createCronPlan(
    name = "dd",
    cron = "* * * * *",
    actionName = "print",
    timeout = 10,
    startTime = LocalDateTime.now(),
    execAfterStart = true,
)
//开始获取/执行任务
scheduler.start()
```

关闭调度器：

```kotlin
scheduler.stop()
```

### cron 表达式语法说明

```shell
# ┌───────────── minute (0 - 59)
# │ ┌───────────── hour (0 - 23)
# │ │ ┌───────────── day of the month (1 - 31)
# │ │ │ ┌───────────── month (1 - 12)
# │ │ │ │ ┌───────────── day of the week (0 - 6) 
# * * * * *
```

| 域   | 允许的值 | 允许的符号  |
| ---- | -------- | ----------- |
| 分钟 | 0-59     | , - * /     |
| 小时 | 0-23     | , - * /     |
| 日   | 1-31     | , - * / ? L |
| 月   | 1-12     | , - * /     |
| 星期 | 0-6      | , - * / ? L |

备注： 日和星期要么同时为 * 要么有一个为 ? ，日的L只能单独出现，星期的L前面必须带上数字。

