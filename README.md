## 本程序用于 mysql 和 elasticsearch 数据同步

### mysql 同步

前提是目标数据库的结构和源数据库结构完全一致，在源数据库中依次遍历表，

在目标数据库中先备份对应表，然后清空目标数据库对应表的数据，最后遍历

源数据库的对应表取出数据写入到目标数据库中。相关配置如下：

```
   ## Mysql 配置
   mysql{
   
    ## 是否启动
    enable=true
    
    ## 允许使用的线程数，默认为 2
    thread-pool-size=2
    
    ## 同步间隔，单位 ：小时
    period = 3
    
    ## 源 Mysql 配置
    res{
        driver="com.mysql.jdbc.Driver"
        url="jdbc:mysql://10.0.0.222:3306/imm_hd?useUnicode=true&characterEncoding=UTF-8"
        user="root"
        password="root"
        connectionName="default-res"
      }
    ## 目标 Mysql 配置
    des{
        driver="com.mysql.jdbc.Driver"
        url="jdbc:mysql://10.0.0.222:3306/imm_hd_sys?useUnicode=true&characterEncoding=UTF-8"
        user="root"
        password="root"
        connectionName="default-des"
      }
   }
```
### elasticsearch 同步

从源 `elasticsearch` 遍历各个索引的所有数据，使用批量插入的形式向目标 `elasticsearch` 插入（或更新）数据。 相关配置如下:

```
  ## ES 配置
   es {
   
     ## 是否启动
     enable=true
   
     ## 允许使用的线程数，默认为 1
     thread-pool-size=1
     
     ## 同步间隔，单位 ：小时
     period = 3
    
      ## 源 ES 配置
      res{
      
        host:"192.168.3.134"
        port="9300"
        name="elasticsearch_zxp1"
      }
      
      # 目标 ES 配置
      des{
      
        host:"192.168.3.134"
        port="8300"
        name="elasticsearch_zxp1_bak"
      }
   }
```
