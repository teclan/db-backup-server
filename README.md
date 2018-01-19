## 本程序用于 mysql 数据同步

### 使用介绍

在项目目录下执行
```
mvn package
```
执行成功后，解压 target 目录下的压缩包，修改相应配置后，执行 startup 脚步即可。

### mysql 同步介绍

前提是目标数据库的结构和源数据库结构完全一致，在源数据库中依次遍历表（指定不需要同步的表外），

根据主键，去目标数据库查询记录是否存在，存在则更新，不存在则插入。相关配置如下：

```
 config {
 
   ## Mysql 配置
   mysql{
   
    ## 是否启动服务
    enable=true
     
    ## 允许使用的线程数，默认为 2
    thread-pool-size=2
    
    ## 同步间隔，单位 ：小时
    period = 3
   
    ## 源 Mysql 配置
      res{
      
        driver="com.mysql.jdbc.Driver"
        url="jdbc:mysql://localhost:3306/teclan?useUnicode=true&characterEncoding=UTF-8"
        user="root"
        password="123456"
        connectionName="default-res"
        
        ## 不需要同步的标
        un-sys-tables=["identifier_info","imm_area","imm_assemble_cfg","imm_forwar_history","imm_systemconfig"]
      }
      
      # 目标 Mysql 配置
      des{
      
        driver="com.mysql.jdbc.Driver"
        url="jdbc:mysql://localhost:3306/teclan_bak?useUnicode=true&characterEncoding=UTF-8"
        user="root"
        password="123456"
        connectionName="default-des"
      }
   }
}
```