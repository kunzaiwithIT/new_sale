# day02数仓生态圈辅助工具

## 1.常见问题

### 问题1

>为什么都是问号?  
>
>原因是下面的CM对应服务没有开启

![image-20230529010201615](day02数仓生态圈辅助工具.assets/image-20230529010201615.png)

### 问题2

>为什么这么多爆红的?
>
> 大多数是因为好多还没有刷新过来,但是如果很久还不能使用,那就可以进行单独重启

![image-20230529005931039](day02数仓生态圈辅助工具.assets/image-20230529005931039.png)

![image-20230529012043632](day02数仓生态圈辅助工具.assets/image-20230529012043632.png)

>如果服务监控和主机监控的服务开启后,正常是以下结果,但是CM服务非常吃内存,所以建议大家打开确认服务已经可以访问
>
>马上关闭对应的CM监控服务,如果 本地内存非常小,那就直接关闭

![image-20230529012701722](day02数仓生态圈辅助工具.assets/image-20230529012701722.png)

### 问题3

>内存不够?
>
>建议配置交换内存

![image-20230529012438873](day02数仓生态圈辅助工具.assets/image-20230529012438873.png)







## 2.今日内容

整体目标：学会hue,sqoop,oozie辅助工具,为后面构建整个项目的实现做技术储备

**可视化交互工具Hue**

- 功能：在一个界面中操作HDFS、Hive等多个工具,提升用户体验

**数据库同步工具Sqoop**

- 功能：实现mysql等RDBMS系统与hadoop(Hive)间进行数据的传递

**工作流调度工具Oozie**

+ 功能：统一调度hadoop系统中常见的hdfs操作、hive操作等



## 3.数据仓库

#### 数据仓库概念、由来、特点

- 数据仓库概念

  > 数据仓库，中文简称==数仓==。英文叫做**Data WareHouse**,简称==DW, DWH==。
  >
  > 数据仓库是==**面向分析**==的==集成化==数据平台，分析的结果给企业提供==**决策支持**==。
  >
  > 应用场景：**满足企业中所有数据的统一化存储，通过规范化的数据处理来实现企业的数据分析应用。**

  ![image-20211006212802855](../../Day03_数据仓库设计/02_课堂笔记/Day03_数据仓库设计.assets/image-20211006212802855.png)

- 数仓的由来

  > 为了更好的==分析数据而来==。正确的废话。

  ```shell
  1、公司建立、开展业务
  2、业务数据存储（事务支持）---->数据库DB
  
  3、经营发展中想赚更多的钱
  4、分析业务数据
  5、DB直接分析影响读性能，干扰业务开展，得不偿失
  6、其他系统、类型的数据也需要一起分析  彼此异构
  7、搭建统一、集成化数据分析平台
  8、建立模型和规范 愉快的进行各种分析
  ```

- 数仓的4大核心特点

  - ==**面向主题性**==

    > 主题(Subject) 是在较高层次上将企业信息系统中某一分析对象（**重点是分析的对象**）的数据进行整合、归类并分析的一种范围，属于一个抽象概念。

    ```properties
    1、数据库: 面向业务划分数据  以业务流程为导向组织数据
    	财务部门：ERP财务管理系统  https://www.kingdee.com/role/finance  金蝶
    		- 现金流量数据表
    		- 汇率调整表
    		- 凭证事务表
    		
        客户部门：CRM客户关系管理系统 https://www.salesforce.com/cn/crm/?bc=OTH
        	- 客户基本信息表
        	- 市场营销计划表
        	- 投诉信息表
    
    2、数据仓库: 面向主题划分数据  以分析需要为导向组织数据
    	 商品主题
    	 供应商主题
    	 顾客主题
         订单主题
    ```

  - ==**集成性**==

    > 数据仓库**不产生数据也不消耗数据**
    >
    > 只会实现存储和加工

    ```properties
    	因为同一个主题的数据可能来自不同的数据源，它们之间会存在着差异（异构数据）：字段同名不同意、单位不统一、编码不统一；
    	因此在集成的过程中需要进行ETL(Extract抽取  Transform转换 load加载)
    ```

  - ==**非易失性**==（稳定性）

    > 数仓上面的数据几乎没有修改操作，都是查询分析的操作。
    >
    > 数仓是分析数据规律的平台 不是创造数据规律的平台。
    >
    > 注意：改指的数据之间的规律不能修改。

  - ==**时变性**==

    > 数仓是一个持续维护建设的东西。
    > 站在==时间的角度，数仓的数据成批次变化更新==。一天一分析（T+1） 一周一分析（T+7）

----

#### OLTP和OLAP区别

- OL==T==P（On-Line ==**Transaction**== Processing）
  - 概念：联机事务处理系统
  - 核心：事务支持
  - 特点
    - 数据安全
    - 数据完整
    - 操作响应效率、时间
    - 并发支持
    - CRUD操作
  - 应用场景
    - 用户注册，注册信息保存在哪里？
    - 用户下单，订单数据保存在哪里？很多人同时下单，能不能快速、安全、稳定的保存？
    - .......各种业务背后的数据存储。
  - 用户：业务操作人员
  - 典型代表
    - RDBMS关系型数据库管理系统，比如MySQL、ORACLE。
- OL==A==P（On-Line ==**Analytical**== Processing）
  - 概念：联机分析处理系统
  - 核心：分析支持
  - 特点
    - 数据量大
    - 事务性要求不高
    - 支撑满足不同程度分析需求
    - 查询操作
  - 用户：数据分析人员
  - 典型代表
    - 数据仓库、数据集市、面向分析的数据库系统

![image-20211006223453716](../../Day03_数据仓库设计/02_课堂笔记/Day03_数据仓库设计.assets/image-20211006223453716.png)

-----

#### 数仓系统架构与核心流程

> 数据仓库提供企业决策分析的数据环境，数据从哪里获取？数据如何存储到数据仓库？决策分析系统如何从数据仓库获取数据进行分析？
>
> 把==数据从获取、存储到数据仓库、数据分析的所有部分==称为一个==数据仓库系统==

![image-20211006223704552](../../Day03_数据仓库设计/02_课堂笔记/Day03_数据仓库设计.assets/image-20211006223704552.png)

- 核心1：==ETL(Extra, Transfer, Load)==

  ```shell
  #1、抽取
  	数据抽取是从各个业务系统、外部系统等源数据处采集源数据。
  	
  #2、转换
  	采集过来的源数据如果要存储到数据仓库需要按照一定的数据格式对源数据进行转换。
  	常见的转换方式有数据类型转换、格式转换、缺失值补充、数据综合等。
  	
  #3、装载
  	转换后的数据就可以存储到数据仓库中，这个过程叫装载。
  	数据装载通常是按一定的频率进行的，比如每天装载前一天的订单数据、每星期装载客户信息等。
  
  ```

  ![image-20211006225344759](../../Day03_数据仓库设计/02_课堂笔记/Day03_数据仓库设计.assets/image-20211006225344759.png)

- 核心2：==数仓分层==

  - 将各种数据的处理流程进行规范化。

    ![image-20211006225632654](../../Day03_数据仓库设计/02_课堂笔记/Day03_数据仓库设计.assets/image-20211006225632654.png)

    ![image-20211006225643092](../../Day03_数据仓库设计/02_课堂笔记/Day03_数据仓库设计.assets/image-20211006225643092.png)

  - 分层的实现

    ```
    当使用Hive作为数据仓库工具的时候，分层是在Hive中逻辑划分实现的。
    常见的做法是：
    	不同的分层创建不同的database
    ```

- 核心3：==数仓建模==

  - 决定了数据存储的方式，表的设计。
  - 比如：有哪些表，表中有哪些字段？表之间有什么关系等等。

#### 数据库建模--ER模型

- a、ER模型概念

  > ==ER模型==也称==实体-关系图==(Entity Relationship Diagram)，由Peter Chen（陈品山）于1976年提出；
  >
  > ER模型围绕真实世界的实体和它们之间的关系展开，被认为是设计数据库的好选择。

  ```properties
  ER模型三要素
  	实体(entity):数据模型中的数据对像。例如，学生，教师，成绩都可以被视为实体。	
  
  	属性(attribute):实体所具有的属性，例如学生具有姓名、学号、年级等属性。
  
  	关系(relationship):用来表现数据对象与数据对象之间的联系，例如学生的实体和成绩表的实体之间有一定的联系，每个学生都有自己的成绩表，这就是一种关系。
  ```

  ![Introduction of ER Model - GeeksforGeeks](../../Day03_数据仓库设计/02_课堂笔记/Day03_数据仓库设计.assets/Database-Management-System-ER-Model-7.png)

- b、ER模型的应用

  > 一般用于==RDBMS系统中来实现业务数据库的建模==。

  ```properties
  数据仓库之父Bill Inmon提出的数据仓库建模方法是自上而下的，从全企业的高度设计一个符合3NF的数据模型，用实体关系（Entity Relationship,ER）模型描述企业的业务。
  
  但是这里存在争议，在现实当中，大多数企业的数据仓库系统更接近Ralph Kimball所倡导的方式：数据仓库是企业内所有数据集市的集合，信息总是被存储在多维模型当中。
  ```

- c、ER模型的构建流程

  > step1：找到所有**实体**，以及每个实体的**属性**
  >
  > step2：找到所有实体之间的**关系**
  >
  > step3：建表，每个实体与每个关系都是一张表

  ```properties
  业务:allen在一品生鲜买了波士顿龙虾4只。
  
  需求:建立表模型把上述业务流程记录下来。
  
  
  - 实体
  		 allen：用户实体	
  			=用户id	 用户name		用户age		  手机		          密码
     		 	 00001     allen         18          18866886688         password
  		 
  		 商店：店铺实体
  			=店铺id	   	店铺名称			营业执照	  经营范围	  地址
  			12300014     一品生鲜浦东旗舰店    shpd_121     生鲜       航都路18号
  
  		 龙虾：商品实体
      		=商品id	  	商品名称		尺寸		颜色		价格
      		 8866225      波士顿龙虾      5斤      红发黑     3元
  
  - 关系
  		订单：实体之间的购买关系
      		=订单id	用户id	店铺id		商品id		订单价格		支付方式	
                o001    00001   12300014      8866225        ....          ...
  
  - 建表
    - 实体：用户表、商品表、店铺表
    - 关系：订单表
  ```

- d、ER模型优缺点

  - **优点：**符合数据库的设计规范，没有冗余数据，保证性能，业务的需求把握的比较全面。
  - **缺点：**设计时候非常复杂，必须找到所有实体和关系，才能构建。





## 4.HUE工具

### HUE页面

>Hadoop  user experience
>
>hadoop用户体验工具

#### 步骤

```properties
1.打开虚拟机进入CM的web页面(默认启动了hue服务)

2.点击hue进入->选择负载均衡的web页面->输入用户名(hue)和密码(hue)

3.可以快速访问hdfs,hive,yarn,mr等页面...
```

#### 示例

![image-20230529020413485](day02数仓生态圈辅助工具.assets/image-20230529020413485.png)

![image-20230529020525779](day02数仓生态圈辅助工具.assets/image-20230529020525779.png)

![image-20230529024923523](day02数仓生态圈辅助工具.assets/image-20230529024923523.png)

![image-20230529020719760](day02数仓生态圈辅助工具.assets/image-20230529020719760.png)

### HUE操作HDFS

#### 步骤

```properties
1.HUE页面左上角直接进入hdfs(原来的方式依然可以进入http://hadoop01:9870)
2.就可以直接进行hdfs相关操作
	创建目录/文件
	编辑文件内容
	上传/下载文件
	删除目录/文件
	以及其他操作(重命名,移动,复制,修改权限...)
```



#### 进入hdfs页面

![image-20230529021659962](day02数仓生态圈辅助工具.assets/image-20230529021659962.png)

![image-20230529021855866](day02数仓生态圈辅助工具.assets/image-20230529021855866.png)

#### 创建目录/文件

![image-20230529022014763](day02数仓生态圈辅助工具.assets/image-20230529022014763.png)

![image-20230529022624737](day02数仓生态圈辅助工具.assets/image-20230529022624737.png)

![image-20230529023016890](day02数仓生态圈辅助工具.assets/image-20230529023016890.png)

#### 编辑文件内容

![image-20230529023309201](day02数仓生态圈辅助工具.assets/image-20230529023309201.png)

![image-20230529023917099](day02数仓生态圈辅助工具.assets/image-20230529023917099.png)

#### 上传/下载文件

![image-20230529024205695](day02数仓生态圈辅助工具.assets/image-20230529024205695.png)

![image-20230529024326589](day02数仓生态圈辅助工具.assets/image-20230529024326589.png)

#### 删除目录/文件

![image-20230529025243695](day02数仓生态圈辅助工具.assets/image-20230529025243695.png)

#### 其他操作...

![image-20230529025401859](day02数仓生态圈辅助工具.assets/image-20230529025401859.png)

### HUE操作HIVE

##### 步骤

```properties
1.HUE页面点击查询进入hive编辑器

2.就可以进行hive基本操作(创建库,创建表,插入数据,查询数据...)  

注意: use关键字在hue中不起作用,操作表的时候采用 库名.表名 方式
注意: hive映射表(业务数据存在hdfs中 ,元数据是存储在mysql数据库中)
```

#### 执行hql语句

![image-20230529030051956](day02数仓生态圈辅助工具.assets/image-20230529030051956.png)

![image-20230529030044436](day02数仓生态圈辅助工具.assets/image-20230529030044436.png)

![image-20230529030310716](day02数仓生态圈辅助工具.assets/image-20230529030310716.png)



#### 查看MR计算

![image-20230529030751689](day02数仓生态圈辅助工具.assets/image-20230529030751689.png)

## 5.Sqoop工具

### Sqoop的功能及应用

- **目标**：**掌握Sqoop的功能及应用场景**

- 官网  sqoop.apache.org

  ![image-20211217172412780](day02数仓生态圈辅助工具.assets/image-20211217172412780-1646464129393.png)

  - **问题**：公司里面的数据都在数据库中【MySQL、Oracle】，需要将数据同步到HDFS或者Hive利用MR做处理和分析？

  - 需求：需要一个工具，能实现将RDBMS数据同步到HDFS或者Hive中

  - **功能**：用于**实现MySQL等RDBMS数据库与HDFS【Hive/Hbase】之间的数据导入与导出**

    ![image-20211217174953222](day02数仓生态圈辅助工具.assets/image-20211217174953222-1646464129393.png)

    - 导入：读MySQL写入到HDFS
    - 导出：读HDFS写入到MySQL
    - 抽取：将A的数据同步到B里面
      - A：数据源：Input读谁
      - B：目标地：Output写谁

  - **本质**

    - 底层就是MapReduce程序：每个Sqoop导入导出的程序就是一个MapReduce程序

      - 问题1：如果MySQL等RDBMS中的数据数据量比较大，导入就比较慢，怎么解决？
        - 解决：构建分布式数据同步
      - 问题2：如果要开发一个分布式工具，非常复杂，成本就比较高，不想开发分布式工具，想实现分布式同步？
        - 解决：实现翻译，自动将Sqoop程序，翻译成一个MapReduce程序

    - **将Sqoop的程序转换成了MapReduce程序，提交给YARN运行，实现分布式采集**

      ![image-20211217175013049](day02数仓生态圈辅助工具.assets/image-20211217175013049-1646464129393.png)

      - 问题：Sqoop的程序转换成的MR程序中一般有没有Shuffle 和 Reduce?
        - 一般都是没有的
        - MapReduce五大阶段对应功能
          - Input：负责读取数据
          - Map：负责分布式并行处理数据
          - Shuffle：负责分区、排序、规约、分组
          - Reduce：负责数据的聚合
          - Output：负责写入结果
        - Sqoop程序的目的在于读A的数据写入B即可，不需要分区、排序、分组、聚合
          - Input：读取A的数据
          - Map：分布式并行读取
          - Output：写入B
        - 大多数情况下：Sqoop转换成的MR程序没有Shuffle和Reduce过程

  - **特点**

    - 必须依赖于Hadoop：MapReduce + YARN

  - **场景**

    - MapReduce是离线计算框架，Sqoop离线数据采集的工具，只能适合于**大数据量的离线数据库同步业务平台**

  - **应用**

    - **数据同步**：定期将离线的数据进行采集同步到数据仓库中
    - **数据迁移**：将历史数据【MySQL、Oracle】存储到HDFS中

  - **测试**

    - 登陆hadoop01的MySQL中

      >可以使用黑窗口也可以datagrip连接
    
      ```
    mysql -uroot -p
      ```

      - 密码：123456

    - mysql中准备业务数据
    
      ```sql
      # 建库
    create database sqoopTest;
      use sqoopTest;
    #建表
      CREATE TABLE tb_tohdfs (
        id int(11) NOT NULL AUTO_INCREMENT,
        name varchar(100) NOT NULL,
        age int(11) NOT NULL,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
      #插入数据
      insert into tb_tohdfs values(null,"laoda",18);
    insert into tb_tohdfs values(null,"laoer",19);
      insert into tb_tohdfs values(null,"laosan",20);
    insert into tb_tohdfs values(null,"laosi",21);
      insert into tb_tohdfs values(null,"laowu",22);
      insert into tb_tohdfs values(null,"laoliu",23);
      insert into tb_tohdfs values(null,"laoqi",24);
      insert into tb_tohdfs values(null,"laoba",25);
      ```
    
    - 需求：将MySQL中tb_tohdfs表的数据导入HDFS的/sqoop/import/test01目录中
    
      ```shell
      sqoop import \
    --connect jdbc:mysql://hadoop01:3306/sqoopTest \
      --username root \
    --password 123456 \
      --table tb_tohdfs \
      --target-dir /sqoop/import/test01
      
      参数说明：
      --connect 连接MySQL（源位置）
      --username 数据库名称
      --password 数据库密码
      --table 指定要同步那个数据表
      --target-dir 指定HDFS的路径即可
      ```

- **小结**

  - Sqoop的功能及应用场景是什么？
    - 功能：实现RDBMS数据库与HDFS之间数据的导入和导出
      - 导入：读RDBMS写HDFS
      - 导出：读HDFS写RDBMS
    - 场景：底层就是MR（默认4个Map），Sqoop只适合于大数据量、离线、数据库的同步场景



### 数据全量、增量、条件概念

- **目标**：**掌握数据全量与数据增量的概念**

- **实施**

  ![image-20230530105704037](day02数仓生态圈辅助工具.assets/image-20230530105704037.png)

  - **全量数据**：从开始到目前为止所有数据的集合为全量数据集
    - 数据同步而言：全量同步：每次都将所有数据同步一遍
    - 第一次：同步100条数据
    - 第二次：同步200条数据
    - 第三次：同步200条数据
    - 第四次：同步400条数据
    - 问题：如果每次直接做全量同步，数据是重复冗余的
    - 解决：工作中做全量同步，一般都是**==覆盖==**【相当于每次同步之前，把之前同步过的先删掉】
  - **增量数据**：从上一次之后到这一次开始之间数据集合为增量数据集
    - 数据同步而言：增量同步：每次只同步新增或者更新的数据，之前同步过的数据就不再进行同步
    - 第一次：同步100条数据
    - 第二次：同步100条数据
    - 第三次：同步0条数据
    - 第四次：同步200条数据
    - 增量同步数据时，每次都是**==追加==**到之前同步的数据中
  - **条件数据**：符合某种条件的数据集合为条件数据集
    - 全量：每次将符合条件的所有数据全量同步覆盖到数据中【少见】
    - 增量：每次将符合条件的所有数据增量同步追加到数据中【常见】
      - 需求：每次增量同步age=20的数据
      - 第一次：同步50条数据
      - 第二次：同步50条数据
      - 第三次：同步0条数据
      - 第四次：同步100条数据

- **小结**

  - 掌握数据全量与数据增量的概念





### Sqoop的开发规则

- **目标**：**掌握Sqoop的开发规则**

- **实施**

  - **版本**

    - sqoop1版本：单纯的工具，直接实现程序的转换的

      - 没有服务端进程，不需要启动进程

    - sqoop2版本：基于Sqoop1的功能之上，引入CS模式【**如果用的MySQL版本比较高，建议使用Sqoop2**】

      ```
      create table tbname (
         name string comment '用户姓名',
         年龄  string
      ) comment ‘表的中文注释’;
      ```

  - **命令**：`${SQOOP_HOMW}/bin/sqoop`

  - **语法**：`sqoop help`

    ```shell
    usage: sqoop COMMAND [ARGS]
    Available commands:
      codegen            Generate code to interact with database records
      create-hive-table  Import a table definition into Hive
      eval               Evaluate a SQL statement and display the results
      export             Export an HDFS directory to a database table
      help               List available commands
      import             Import a table from a database to HDFS
      import-all-tables  Import tables from a database to HDFS
      import-mainframe   Import datasets from a mainframe server to HDFS
      job                Work with saved jobs
      list-databases     List available databases on a server
      list-tables        List available tables in a database
      merge              Merge results of incremental imports
      metastore          Run a standalone Sqoop metastore
      version            Display version information
    
    See 'sqoop help COMMAND' for information on a specific command.
    ```

  - **导入**

    ```
    sqoop import --help
    usage: sqoop import [GENERIC-ARGS] [TOOL-ARGS]
    ```

  - **导出**

    ```
    sqoop export --help
    usage: sqoop export [GENERIC-ARGS] [TOOL-ARGS]
    ```

  - **测试**

    ```shell
    -- 演示sqoop help
    -- ...
    -- 需求: 查看mysql中所有库
    /*
    sqoop list-databases \
    --connect jdbc:mysql://hadoop01:3306 \
    --username root \
    --password 123456
    */
    
    -- 需求: 查看mysql的sqoopTest库下的所有表
    /*
    sqoop list-tables \
    --connect jdbc:mysql://hadoop01:3306/sqoopTest \
    --username root \
    --password 123456
    */
    ```

- **小结**

  - 掌握Sqoop的开发规则



### Sqoop导入HDFS

#### 知识点:

- **MySQL选项**

  ```properties
  --connect: 指定连接的RDBMS数据库的地址
  --username: 指定数据库的用户名
  --password: 指定数据库的密码
  --table: 指定数据库中的表
  --columns: 指定导入的列名
  --where: 指定导入行的过滤条件
  -e/--query: 指定导入数据的SQL语句，不能与--table一起使用，必须指定where条件，where中必须指定$CONDITIONS
  ```

- **HDFS选项**

  ```properties
  --delete-target-dir: 指定如果导入的HDFS路径已存在就提前删除
  --target-dir: 指定导入的HDFS路径
  --fields-terminated-by: 指定导入HDFS的文件中列的分隔符
  ```

- **其他选项**

  ```properties
  -m: 指定底层MapReduce程序的MapTask的个数
  --split-by: MySQL没有主键时，必须添加该参数指定多个MapTask划分数据的方式，MySQL有主键也可以使用该参数
  ```

#### 导入有主键表

需求1：将MySQL中tb_tohdfs表的数据导入HDFS的/sqoop/import/test01目录中,默认分隔符,默认map数为4个

```shell
sqoop import \
--connect jdbc:mysql://hadoop01:3306/sqoopTest \
--username root \
--password 123456 \
--table tb_tohdfs \
--target-dir /sqoop/import/test01
```

需求2：将MySQL中tb_tohdfs表的数据导入HDFS的/sqoop/import/test01目录中,指定分隔符,默认map数为4个

>注意: 如果输出目录已经存在需要加--delete-target-dir

```shell
sqoop import \
--connect jdbc:mysql://hadoop01:3306/sqoopTest \
--username root \
--password 123456 \
--table tb_tohdfs \
--target-dir /sqoop/import/test01 \
--delete-target-dir \
--fields-terminated-by '\t'
```

需求3：将MySQL中tb_tohdfs表的数据导入HDFS的/sqoop/import/test01目录中,指定分隔符,指定map数1个

```shell
sqoop import \
--connect jdbc:mysql://hadoop01:3306/sqoopTest \
--username root \
--password 123456 \
--table tb_tohdfs \
--target-dir /sqoop/import/test01 \
--delete-target-dir \
--fields-terminated-by '\t' \
-m 1
```

需求4：将MySQL中tb_tohdfs表的的id,name两列导入HDFS的/sqoop/import/test01目录中,指定分隔符,指定map数2个

  ```shell
sqoop import \
--connect jdbc:mysql://hadoop01:3306/sqoopTest \
--username root \
--password 123456 \
--table tb_tohdfs \
--columns id,name \
--target-dir /sqoop/import/test01 \
--delete-target-dir \
--fields-terminated-by '\t' \
-m 2
  ```



#### 导入非主键表

需求5:  导入没有主键的表到hdfs中,注意: 加上--split-by  指定拆分字段

  >    - 注意：指定多个MapTask时，分为两种情况
  >    
  >      - 情况一：如果MySQL表有主键，可以直接指定，自动根据主键的值来划分每个MapTask处理的数据
  >    
  >      - 情况二：如果MySQL表没有主键，会报错，加上--split-by指定按照哪一列进行划分数据即可
  >    
  >        #ERROR tool.ImportTool: Import failed: No primary key could be found for table emp. Please specify one with --split-by or perform a sequential import with '-m 1'.

```sql
# MySQL中创建一张没有主键的表
#演示没有主键的表情况
use sqoopTest;
drop table if exists tb_tohdfs2;
CREATE TABLE tb_tohdfs2 (
id int(11) ,
name varchar(100) ,
age int(11)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
-- 插入数据
insert into tb_tohdfs2 values(1,"laoda",18);
insert into tb_tohdfs2 values(2,"laoer",19);
insert into tb_tohdfs2 values(9,"laosan",20);
insert into tb_tohdfs2 values(100,"laosi",21);
insert into tb_tohdfs2 values(300,"laowu",22);
insert into tb_tohdfs2 values(600,"laoliu",23);
insert into tb_tohdfs2 values(700,"laoqi",24);
insert into tb_tohdfs2 values(800,"laoba",25);
```

  ```shell
-- 演示没有主键情况
-- 注意: 如果mysql中表没有主键,要么指定map数是1个,要么加上split-by指定一列作为临时主键,否则报错
-- 方式1：将MySQL中tb_tohdfs表的数据导入HDFS的/sqoop/import/test02目录中,指定map数1个
/*以下命令在linux中执行:
sqoop import \
--connect jdbc:mysql://hadoop01:3306/sqoopTest \
--username root \
--password 123456 \
--table tb_tohdfs2 \
--target-dir /sqoop/import/test02 \
-m 1
*/

-- 方式2：将MySQL中tb_tohdfs表的数据导入HDFS的/sqoop/import/test02目录中,split-by指定临时参考字段
/*以下命令在linux中执行:
sqoop import \
--connect jdbc:mysql://hadoop01:3306/sqoopTest \
--username root \
--password 123456 \
--table tb_tohdfs2 \
--target-dir /sqoop/import/test02 \
--delete-target-dir \
--split-by id
*/
  ```

####  加条件导入数据

需求6：需求6：将tb_tohdfs表中的id >4的数据导入HDFS的/sqoop/import/test03目录中，并且用制表符分隔

```shell
sqoop import \
--connect jdbc:mysql://hadoop01:3306/sqoopTest \
--username root \
--password 123456 \
--table tb_tohdfs \
--where 'id>4' \
--target-dir /sqoop/import/test03 \
--delete-target-dir \
--fields-terminated-by '\t' \
-m 1
```

综合需求：使用两种方式将tb_tohdfs表中的id>4的id和name两列导入/sqoop/import/test04目录中

```shell
#方式1
sqoop import \
--connect jdbc:mysql://hadoop01:3306/sqoopTest \
--username root \
--password 123456 \
--table tb_tohdfs \
--columns id,name \
--where 'id>4' \
--target-dir /sqoop/import/test04 \
--delete-target-dir \
--fields-terminated-by '\t' \
-m 1


#方式2
sqoop import \
--connect jdbc:mysql://hadoop01:3306/sqoopTest \
--username root \
--password 123456 \
-e 'select id,name from tb_tohdfs where id>4 and $CONDITIONS' \
--target-dir /sqoop/import/test04 \
--delete-target-dir \
--fields-terminated-by '\t' \
-m 1
```

  - 注意1：如果使用-e/--query必须在where中加上$CONDITIONS

  - 注意2：-e/--query 与 --table只能二选一

    


### Sqoop导入Hive

- **目标**：**掌握Sqoop导入Hive**

- **实施**

  - **MySQL选项**

    ```properties
    --connect: 指定连接的RDBMS数据库的地址
    --username: 指定数据库的用户名
    --password: 指定数据库的密码
    --table: 指定数据库中的表
    --columns: 指定导入的列名
    --where: 指定导入的条件
    -e/--query: 指定导入数据的SQL语句，不能与--table一起使用，必须指定where条件，where中必须指定$CONDITIONS
    ```

  - **Hive选项**

    ```properties
    # Old API
    --hive-import: 指定导入数据到Hive表
    --hive-database: 指定原生方式导入Hive的数据库名称
    --hive-table: 指定原生方式导入的Hive的表名
    
    # New API
    --hcatalog-database: 指定使用hcatalog方式导入Hive的数据库名称
    --hcatalog-table: 指定使用hcatalog方式导入Hive的数据库名称
    ```

    需求1: 快速生成hive表并且导入对应数据

    ```shell
    -- 需求1: create-hive-table快速复制表结构
    复制表结构
    #将关系型数据的表结构复制到hive中
    sqoop create-hive-table \
    --connect jdbc:mysql://192.168.88.80:3306/sqoopTest \
    --table tb_tohdfs \
    --username root \
    --password 123456 \
    --hive-table test.fromsqoop
    
    #其中
    --table tb_tohdfs为mysql中的数据库sqoopTest中的表
    --hive-table test.fromsqoop 为hive中新建的表名称。如不指定，将会在hive的default库下创建和MySQL同名表
    
    插入数据
    sqoop import \
    --connect jdbc:mysql://hadoop01:3306/sqoopTest \
    --username root \
    --password 123456 \
    --table tb_tohdfs \
    --hive-import \
    --hive-database test \
    --hive-table fromsqoop \
    -m 1
    ```

    

    

    需求2：将MySQL中tb_tohdfs同步到Hive的test数据库下的fromsqoop1表中

       建表

    ```sql
    # Hive中建表
    drop table if exists fromsqoop1;
    create table test.fromsqoop1(
    id int,
    name string,
    age int
    ) row format delimited fields terminated by '\t';
    ```
    
    
    
    - 方式一：OldAPI，自己建表，将数据采集到Hive表中【现在很少使用】
    
      ```shell
      # 同步
      sqoop import \
      --connect jdbc:mysql://hadoop01:3306/sqoopTest \
      --username root \
      --password 123456 \
      --table tb_tohdfs \
    --hive-import \
      --hive-database test \
    --hive-table fromsqoop1 \
      --fields-terminated-by '\t' \
    -m 1
      ```
      
      - 注意：采集到Hive时，默认分隔符是\001，如果Hive表不是默认分隔符，采集时一定要指定
      
    - **方式二：NewAPI，使用hcatalog方式，将数据同步到Hive表**==【主要使用的方式】==
    
      >直接以建表指定的分隔符
      
      ```shell
      # 同步
      sqoop import \
      --connect jdbc:mysql://hadoop01:3306/sqoopTest \
      --username root \
      --password 123456 \
      --table tb_tohdfs \
      --hcatalog-database test \
      --hcatalog-table fromsqoop2 \
      -m 1
      ```

- **Hcatalog**

  ```
    Apache HCatalog是基于Apache Hadoop之上的数据表和存储管理服务。
    - 提供一个共享的模式和数据类型的机制。
    - 抽象出表，使用户不必关心他们的数据怎么存储，底层什么格式。
    - 提供可操作的跨数据处理工具，如Pig，MapReduce，Streaming，和Hive。
  ```

    - Sqoop中Hcatalog：https://sqoop.apache.org/docs/1.4.7/SqoopUserGuide.html#_sqoop_hcatalog_integration

      ![image-20211217205923650](day02数仓生态圈辅助工具.assets/image-20211217205923650.png)

    - 与原生方式的区别

      | 区别     | 原生方式【--hive】         | Hcatalog方式[--hcatalog]                        |
      | -------- | -------------------------- | ----------------------------------------------- |
      | 数据格式 | 较少                       | 支持多种特殊格式：orc/rcfile/squencefile/json等 |
      | 导入方式 | 允许覆盖                   | 不允许覆盖，只能追加                            |
      | 字段匹配 | 顺序匹配，字段名可以不相等 | 字段名匹配，名称必须相等                        |

- **小结**

  - 掌握Sqoop导入Hive



### Sqoop增量导入

- **目标**：**掌握Sqoop的增量导入**

- **实施**

  - **增量需求**:T+1，今天处理昨天的数据，第二天采集第一天数据

    - 第一天

      - Hive：空的

      - MySQL：MySQL产生数据1,2,3,4

        ```
        |  1 | laoda  |  18 |    5.28
        |  2 | laoer  |  19 |    5.28
        |  3 | laosan |  20 |    5.28
        |  4 | laosi  |  21 |    5.28
        ```

    - 第二天

      - Hive：采集第一天的数据(id>0)

        ```
        |  1 | laoda  |  18 |
        |  2 | laoer  |  19 |
        |  3 | laosan |  20 |
        |  4 | laosi  |  21 |
        ```

      - MySQL：MySQL产生数据5,6,7,8

        ```
        |  1 | laoda  |  28 |    5.29
        |  2 | laoer  |  19 |    5.28
        |  3 | laosan |  20 |    5.28
        |  4 | laosi  |  21 |    5.28
        |  5 | laowu  |  22 |    5.29
        |  6 | laoliu |  23 |    5.29
        |  7 | laoqi  |  24 |    5.29
        |  8 | laoba  |  25 |    5.29
        ```

    - 第三天

      - Hive：采集第二天的数据 (id>4)
      - 问题1：Hive是只采集5,6,7,8，还是需要把1,2,3,4,5,6,7,8都采集一遍？
        - 增量：只要采集5,6,7,8
      - 问题2：你怎么知道这些数据是昨天的？
        - 用某一列的值基于上一次的采集来做比较

    - **设计：每次记录这一次采集这一列的值，用于对某一列值进行判断，只要大于上一次的值就会被导入**

      - 第一次：id > 0
      - 第二次：id > 4

  - **需求：将昨天的新增的和更新的数据都进行采集**

  - **Sqoop自带方式**

    ```
    Incremental import arguments:
    --incremental <import-type>    Define an incremental import of type 
    				'append' or 'lastmodified'
    --check-column <column>        Source column to check for incrementalchange
    --last-value <value>           Last imported value in the incremental
    								check column
    ```

    - --incremental：指定Sqoop增量采集方式
      
      - append：适合于只有新增的场景
      
        ```txt
        |  1 | laoda  |  18 |
        |  2 | laoer  |  19 |
        |  3 | laosan |  20 |
        |  4 | laosi  |  21 |
        |  5 | laowu  |  22 |
        |  6 | laoliu |  23 |
        |  7 | laoqi  |  24 |
        |  8 | laoba  |  25 |
        ```
      
      - lastmodified：适合于既有新增，也有更新的场景
      
        ```txt
        |  1 | laoda  |  28 |
        |  2 | laoer  |  19 |
        |  3 | laosan |  20 |
        |  4 | laosi  |  21 |
        |  5 | laowu  |  22 |
        |  6 | laoliu |  23 |
        |  7 | laoqi  |  24 |
        |  8 | laoba  |  25 |
        ```
      
    - --check-column：指定用哪一列来作为增量的依据
    - --last-value：指定的那一列上一次的最后一个值是什么
  
- **Append方式测试**
  
  - 要求：必须有一列自增的值，按照自增的int值进行判断
  
  - 特点：只能导入新增的数据，**无法导入更新的数据**
  
  - 场景：数据只会发生新增，不会发生更新的场景
  
  - 测试
  
    - 第一次导入
  
      ```shell
        sqoop import \
        --connect jdbc:mysql://hadoop01:3306/sqoopTest \
        --username root \
        --password 123456 \
        --table tb_tohdfs \
        --target-dir /sqoop/import/test02 \
        --fields-terminated-by '\t' \
        --incremental append \
        --check-column id \
        --last-value 0 \
        -m 1
      ```
  
    - 第二次产MySQL生新的数据
  
      ```sql
        insert into tb_tohdfs values(null,"laowu",22);
        insert into tb_tohdfs values(null,"laoliu",23);
        insert into tb_tohdfs values(null,"laoqi",24);
        insert into tb_tohdfs values(null,"laoba",25);
      ```
  
    - 第二次导入
  
      ```shell
      sqoop import \
      --connect jdbc:mysql://hadoop01:3306/sqoopTest \
      --username root \
      --password 123456 \
      --table tb_tohdfs \
      --target-dir /sqoop/import/test02 \
      --fields-terminated-by '\t' \
      --incremental append \
      --check-column id \
      --last-value 4 \
      -m 1
      ```
  
- **Lastmodified方式测试**
  
  - 要求：**必须包含动态时间变化这一列**，按照数据变化的时间进行判断
  
  - 特点：既导入新增的数据也导入更新的数据
  
  - 场景：工作中一般用不到，无法满足要求
  
  - 测试
  
    - MySQL中创建测试数据
  
      ```sql
        -- MYSQL中创建测试表
        CREATE TABLE `tb_lastmode` (
        `id` int(11) NOT NULL AUTO_INCREMENT,
          `word` varchar(200) NOT NULL,
        `lastmode` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP  ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        -- MYSQL中插入测试数据
        insert into tb_lastmode values(null,'hadoop',null);
        insert into tb_lastmode values(null,'spark',null);
        insert into tb_lastmode values(null,'hbase',null);
      ```
  
    - 第一次采集
  
      ```shell
        sqoop import \
        --connect jdbc:mysql://hadoop01:3306/sqoopTest \
        --username root \
        --password 123456 \
        --table tb_lastmode \
        --target-dir /sqoop/import/test03 \
        --fields-terminated-by '\t' \
        --incremental lastmodified \
        --check-column lastmode \
        --last-value '2023-05-28 00:00:00' \
        -m 1
      ```
  
    - 数据发生变化
  
      ```sql
        insert into tb_lastmode values(null,'hive',null);  -- 2023-05-29 00:00:00
        update tb_lastmode set word = 'sqoop' where id = 1; -- 2023-05-29  00:00:00
      ```
  
    - 第二次采集 
  
      ```shell
        sqoop import \
        --connect jdbc:mysql://hadoop01:3306/sqoopTest \
        --username root \
        --password 123456 \
        --table tb_lastmode \
        --target-dir /sqoop/import/test03 \
        --fields-terminated-by '\t' \
        --merge-key id \
        --incremental lastmodified \
        --check-column lastmode \
        --last-value '2023-05-29 00:00:00' \
        -m 1
      ```
  
      - --merge-key：将相同ID的数据进行合并
  
- ==**条件过滤方式测试**==
  
  - 由于Append和Lastmodified的场景有限，工作中会见到使用条件过滤的方式来实现增量采集
  
  - MySQL表的数据内容
  
    ```
      id		name		age			……		createTime				updateTime
      			xxx			18				    2022-05-28 00:00:00		2022-05-29 00:00:00
      	  		xxx			28				    2022-05-29 00:00:00		2022-05-30 00:00:00
      	  		xxx			38				    2022-05-30 00:00:00		9999-12-31 00:00:00
    ```
  
  ```
    - createTime：数据产生的时间
    - updateTime：数据更新的时间
  ```
  
- 实现采集：今天采集昨天的数据（T+1）
  
  ```shell
  sqoop import \
  --connect jdbc:mysql://hadoop01:3306/sqoopTest \
  --username root \
  --password 123456 \
  -e 'select * from tb_tohdfs where substr(create_time,1,10) = '2022-05-29' or substr(update_time,1,10) = '2022-05-29' and $CONDITIONS' \
  # 输出目录不能提前存在，每次输出目录都需要不同
  --target-dir /sqoop/import/2022-05-29 \
  --fields-terminated-by '\t' \
  -m 1
  ```
  
- **小结**

  - 掌握Sqoop的增量导入



### Sqoop全量导出

- **目标**：**掌握Sqoop全量导出**

- **实施**

  - 导出：从HDFS读取数据写入MySQL

    - 输入：HDFS
    - 输出：MySQL

  - **MySQL选项**

    ```properties
    --connect: 指定连接的RDBMS数据库的地址
    --username: 指定数据库的用户名
    --password: 指定数据库的密码
    --table: 指定数据库中的表
    ```

  - **HDFS选项**

    ```properties
    --export-dir: 指定导出的HDFS路径
    --input-fields-terminated-by: 指定导出的HDFS文件的列的分隔符
    ```

  - **Hive选项**

    ```properties
    --hcatalog-database: 指定使用hcatalog方式导入Hive的数据库名称
    --hcatalog-table: 指定使用hcatalog方式导入Hive的数据库名称
    ```

  - **准备数据**

  - 已知HDFS的/source/tb_url目录下存在url.txt文件,存储了以下内容

    >1	http://facebook.com/path/p1.php?query=1
    >
    >2	http://www.baidu.com/news/index.jsp?uuid=frank
    >3	http://www.jd.com/index?source=baidu
  - **测试案例**

  - 需求1：将HDFS的/source/tb_url目录下的url.txt数据直接导出到MySQL对应表中

    1.1MySQL中建表

    ```sql
    use sqoopTest;
    CREATE TABLE tb_url (
      id int(11) NOT NULL,
      url varchar(200) NOT NULL,
      PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    ```

    1.2sqoop命令如下

    ```shell
    sqoop export \
    --connect jdbc:mysql://hadoop01:3306/sqoopTest \
    --username root \
    --password 123456 \
    --table tb_url \
    --export-dir '/source/tb_url' \
    --input-fields-terminated-by '\t'
    ```

    注意: 如果==不指定HDFS文件的分隔符==，导致导出到MySQL会失败

  - 需求2：将Hive的某张表的数据导出到MySQL，注意:先truncate清空mysql中的数据

    2.1hive建表

    ```sql
    -- Hive中建表
    use test;
    create table tb_url(
        id int,
        url string
    ) row format delimited fields terminated by '\t';
    
    -- 加载数据到Hive表
    load data  inpath '/source/tb_url/url.txt' into table tb_url;
    ```

    2.2sqoop命令如下

    ```shell
    sqoop export \
    --connect jdbc:mysql://hadoop01:3306/sqoopTest \
    --username root \
    --password 123456 \
    --table tb_url \
    --hcatalog-database test \
    --hcatalog-table tb_url
    ```

- **小结**

  - 掌握Sqoop全量导出



### Sqoop增量导出

- **目标**：**掌握Sqoop增量导出**

- **实施**

  - **增量导出场景**

    - Hive中有一张结果表：存储每天分析的结果

      ```shell
      # Hive第一天：29号
      id		daystr			UV 			PV			IP
      1		2020-11-09		1000		10000		500
      
      # Hive第二天：30号
      id		daystr			UV 			PV			IP
      1		2020-11-09		1000		10000		600
      2		2020-11-10		2000		20000		1000
      ```

    - MySQL：存储每一天的结果

      ```shell
      # MySQL第一天：30存储Hive导出的29号的结果
      1		2020-11-09		1000		10000		500
      
      # MySQL第二天：31存储Hive导出的30号的结果
      1		2020-11-09		1000		10000		600
  2		2020-11-10		2000		20000		1000
      ```

    - 问题：第二天是全量导出到MySQL还是增量导出到MySQL？
    
      - 选择增量导出到MySQL即可


  - **增量导出选项**

    ```properties
    --update-key 字段名: 指定参考的字段
    --update-mode updateonly : 只增量导出更新的数据，不能导出新增的数据
    --update-mode allowinsert : 既导出更新的数据，也导出新增的数据
    ```

  - **准备数据**

  - **updateonly**

    - 修改HDFS的/source/tb_url目录下url.txt数据为:

      ```
      1	http://www.itcast.com/path/p1.php?query=1
      2	http://www.baidu.com/news/index.jsp?uuid=frank
      3	http://www.itcast.cn
      4	http://www.heima.com
      ```

    - 增量导出（把HDFS中的数据导出到MySQL，和Hive没关系，使用Hive就是为了生成一个HDFS的文件夹而已）

      ```shell
      sqoop export \
      --connect jdbc:mysql://hadoop01:3306/sqoopTest \
    --username root \
      --password 123456 \
    --table tb_url \
      --export-dir '/source/tb_url' \
      --input-fields-terminated-by '\t' \
      --update-key id \
      --update-mode updateonly 
      
      ```
    
  - **allowinsert**（底层使用的是replace into）

    - 修改HDFS的/source/tb_url目录下url.txt数据为:

      ```
      1	http://www.itcast.com/path/p1.php?query=1
      2	http://www.baidu.com/news/index.jsp?uuid=frank
      3	http://www.itcast.cn
      4	http://www.itheima.com
      5	http://www.binzi.com
      ```

    - 增量导出

      ```shell
      sqoop export \
      --connect jdbc:mysql://hadoop01:3306/sqoopTest \
    --username root \
      --password 123456 \
    --table tb_url \
      --export-dir '/source/tb_url' \
      --input-fields-terminated-by '\t' \
      --update-key id \
      --update-mode allowinsert \
      -m 1
      ```
    
- **小结**

  - 掌握Sqoop增量导出

## 6.Oozie工具

### 工作流介绍

- 工作流概念

  ```properties
  	工作流（Workflow），指“业务过程的部分或整体在计算机应用环境下的自动化”。是对工作流程及其各操作步骤之间业务规则的抽象、概括描述。
  	工作流解决的主要问题是：为了实现某个业务目标，利用计算机软件在多个参与者之间按某种预定规则自动传递文档、信息或者任务。
  	一个完整的数据分析系统通常都是由多个前后依赖的模块组合构成的：数据采集、数据预处理、数据分析、数据展示等。各个模块单元之间存在时间先后依赖关系，且存在着周期性重复。
  
  	核心概念:依赖执行 周期重复执行
  ```

- 工作流实现方式

  - 自己开发实现调度工具
  - 使用第三方调度软件

----

### Apache Oozie介绍、架构

- oozie介绍

  ```properties
  	Oozie是一个用来管理 Hadoop生态圈job的工作流调度系统。由Cloudera公司贡献给Apache。
  	Oozie是运行于Java servlet容器上的一个java web应用。
  	Oozie的目的是按照DAG（有向无环图）调度一系列的Map/Reduce或者Hive等任务。Oozie 工作流由hPDL（Hadoop Process Definition Language）定义（这是一种XML流程定义语言）。
  	适用场景包括：
  		需要按顺序进行一系列任务；
  		需要并行处理的任务；
  		需要定时、周期触发的任务；
  		可视化作业流运行过程；
  		运行结果或异常的通报。
  ```

  ![image-20211005224357114](day02数仓生态圈辅助工具.assets/image-20211005224357114.png)

- oozie架构

  ![image-20211005224406173](day02数仓生态圈辅助工具.assets/image-20211005224406173.png)

  ```shell
  #Oozie Client
  	提供命令行、java api、rest等方式，对Oozie的工作流流程的提交、启动、运行等操作；
  
  #Oozie WebApp
  	即 Oozie Server,本质是一个java应用。可以使用内置的web容器，也可以使用外置的web容器；
  
  #Hadoop Cluster
  	底层执行Oozie编排流程的各个hadoop生态圈组件；
  ```

----

### Oozie工作流类型

> ==workflow== 普通工作流 没有定时和条件触发功能。
>
> ==coordinator== 定时工作流 可以设置执行周期和频率
>
> bundle 批处理工作流  一次可以提交执行多个coordinator

![image-20211005224532079](day02数仓生态圈辅助工具.assets/image-20211005224532079.png)

![image-20211005224613359](day02数仓生态圈辅助工具.assets/image-20211005224613359.png)

![image-20211005224643377](day02数仓生态圈辅助工具.assets/image-20211005224643377.png)

----

### Oozie使用案例

![image-20230817111453741](day02数仓生态圈辅助工具.assets/image-20230817111453741.png)

+ 普通工作流入门

  

![image-20230208173156198](day02数仓生态圈辅助工具.assets/image-20230208173156198.png)

+ 运行异常原因

![image-20230208174107280](day02数仓生态圈辅助工具.assets/image-20230208174107280.png)

+ 修改yarn容器内存

  

![image-20230208174702766](day02数仓生态圈辅助工具.assets/image-20230208174702766.png)

![image-20230208181102345](day02数仓生态圈辅助工具.assets/image-20230208181102345.png)

![image-20230208181251447](day02数仓生态圈辅助工具.assets/image-20230208181251447.png)

![image-20230208175832247](day02数仓生态圈辅助工具.assets/image-20230208175832247.png)

![image-20230208180114890](day02数仓生态圈辅助工具.assets/image-20230208180114890.png)

+ 重新操作成功

  

![image-20230208173156198](day02数仓生态圈辅助工具.assets/image-20230208173156198.png)

![image-20230208182237978](day02数仓生态圈辅助工具.assets/image-20230208182237978.png)