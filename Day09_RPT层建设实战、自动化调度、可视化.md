

### Day09_RPT层建设实战、自动化调度、可视化

![image-20230825094701535](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230825094701535.png)

#### 知识点01：课程内容大纲与学习目标

```shell
#课程内容大纲
	1、RPT层构建
    2、Presto数据导出
    	presto集成MySQL
    3、自动化调度方案
    	关键在于shell脚本
    4、数据报表可视化 Data Visualization
#学习目标
	掌握RPT层的功能及构建实现
	学会presto整合MySQL
	理解自动化调度方案的实现
	了解数据可视化、大屏构建相关知识
```

----

#### 知识点02：RPT层搭建--目标与需求

- 新零售数仓分层图

  ![image-20211017141939771](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211017141939771.png)

- RPT

  - 名称：==数据报表层（Report）===，其实就是我们所讲的数据应用层DA、APP。

  - 功能：==根据报表、专题分析的需求而计算生成的个性化数据==。表结构与报表系统保持一致。

  - 解释

    > ​		这一层存在的意义在于，**如果报表系统需要一些及时高效的展示分析**。我们可以在RPT层根据其需求**提前**把相关的字段、计算、统计做好，**支撑报表系统的高效、便捷使用**。

- 栗子

  - 比如报表需要展示：门店销售量Top10，展示如下效果

    ![image-20211207134550065](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207134550065.png)

  - 对于其依赖的数据有两种实现方式

    - 方式1：使用专业的BI报表软件直接读取数据仓库或数据集市的数据，然后自己根据需要展示的效果进行数据抽、转换、拼接动作
    - 方式2：==大数据开发工程师针对前端/BI 的TopN展示的需求，提前把数据提前拼接好保存起来，前端获取数据渲染展示。==

    ![image-20211207134945383](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207134945383.png)

- 使用DataGrip在Hive中创建RPT层

  > 注意:==**对于建库建表操作，需直接使用Hive**==，因为Presto只是一个数据分析的引擎，其语法不一定支持直接在Hive中建库建表。
  
  ```sql
  create database if not exists yp_rpt;
  ```

>需求如下:
>
>需求1: 按月统计，==各个门店==的==月销售单量==。       
>
>需求2: 按天统计，==总销售金额==和==销售单量==。
>
>需求3: 按日期统计,不同渠道的==订单量占比== 比如每天,也可以延伸为每周、每月、每个城市、每个品牌等等等。
>
>需求4: 按天统计, 统计出某天销量最多的==top10==商品
>
>需求5:  按天统计, 统计出某天收藏量最多的==top10==商品
>
>需求6: 按天统计,统计出某天购物车最多的==top10==商品
>
>需求7: 综合需求: 统计==会员==相关各类指标(活跃会员数、新增会员数、新增消费会员数、总付费会员数、总会员数、会员活跃率)

#### 知识点03：RPT层搭建--销售主题报表

##### 需求一：==门店月销售单量==

> 按月统计，==各个门店==的==月销售单量==。

- 建表

  ```sql
  CREATE TABLE yp_rpt.rpt_sale_store_cnt_month(
     date_time string COMMENT '统计日期,不能用来分组统计',
     year_code string COMMENT '年code',
     year_month string COMMENT '年月',
     
     city_id string COMMENT '城市id',
     city_name string COMMENT '城市name',
     trade_area_id string COMMENT '商圈id',
     trade_area_name string COMMENT '商圈名称',
     store_id string COMMENT '店铺的id',
     store_name string COMMENT '店铺名称',
     
     order_store_cnt BIGINT COMMENT '店铺成交单量',
     miniapp_order_store_cnt BIGINT COMMENT '小程序端店铺成交单量',
     android_order_store_cnt BIGINT COMMENT '安卓端店铺成交单量',
     ios_order_store_cnt BIGINT COMMENT 'ios端店铺成交单量',
     pcweb_order_store_cnt BIGINT COMMENT 'pc页面端店铺成交单量'
  )
  COMMENT '门店月销售单量排行' 
  ROW format delimited fields terminated BY '\t' 
  stored AS orc tblproperties ('orc.compress' = 'SNAPPY');
  ```

- 实现

  > 从销售主题统计宽表中，找出分组为store，并且时间粒度为month的进行排序即可。
  
  ```sql
  --门店月销售单量排行
  insert into yp_rpt.rpt_sale_store_cnt_month
  select 
     date_time,
     year_code,
     year_month,
     
     city_id,
     city_name,
     trade_area_id,
     trade_area_name,
     store_id,
     store_name,
     
     order_cnt,
     miniapp_order_cnt,
     android_order_cnt,
     ios_order_cnt,
     pcweb_order_cnt
  from yp_dm.dm_sale 
  where time_type ='month' and group_type='store' and store_id is not null 
  order by order_cnt desc;
  ```


  ![image-20211204164443670](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211204164443670.png)

##### 需求二：==日销售曲线==

> 按==天==统计，==总销售金额==和==销售单量==。

- 建表

  ```sql
  --日销售曲线
  DROP TABLE IF EXISTS yp_rpt.rpt_sale_day;
  CREATE TABLE yp_rpt.rpt_sale_day(
     date_time string COMMENT '统计日期,不能用来分组统计',
     year_code string COMMENT '年code',
     month_code string COMMENT '月份编码', 
     day_month_num string COMMENT '一月第几天', 
     dim_date_id string COMMENT '日期',
  
     sale_amt DECIMAL(38,2) COMMENT '销售收入',
     order_cnt BIGINT COMMENT '成交单量'
  )
  COMMENT '日销售曲线' 
  ROW format delimited fields terminated BY '\t' 
  stored AS orc tblproperties ('orc.compress' = 'SNAPPY');
  ```

- 实现

  ```sql
  --日销售曲线
  insert into yp_rpt.rpt_sale_day
  select 
     date_time,
     year_code,
     month_code,
     day_month_num,
     dim_date_id,
     sale_amt,
     order_cnt
  from yp_dm.dm_sale 
  where time_type ='date' and group_type='all'
  --按照日期排序显示曲线
  order by dim_date_id;
  ```

![image-20211207141004223](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207141004223.png)



##### 需求三：==各渠道销售占比==

> 比如每天不同渠道的==订单量占比==。

>
  > 也可以延伸为每周、每月、每个城市、每个品牌等等等。

  - 处理思路
  
    ```sql
    --在dm层的dm_sale表中
    	order_cnt 表示总订单量
    		miniapp_order_cnt 表示小程序订单量
    		android_order_cnt 安卓
    		ios_order_cnt ios订单量
    		pcweb_order_cnt  网站订单量
    --所谓的占比就是
    	每个占order_cnt总订单量的比例 也就是进行除法运算
    	
    --最后需要注意的是
    	上述这几个订单量的字段  存储类型是bigint类型。
    	如果想要得出90.25这样的占比率  需要使用cast函数将bigInt转换成为decimal类型。
    ```

  - 建表

    ```sql
    --渠道销量占比
    DROP TABLE IF EXISTS yp_rpt.rpt_sale_fromtype_ratio;
    CREATE TABLE yp_rpt.rpt_sale_fromtype_ratio(
       date_time string COMMENT '统计日期,不能用来分组统计',
       time_type string COMMENT '统计时间维度：year、month、day',
       year_code string COMMENT '年code',
       year_month string COMMENT '年月',
       dim_date_id string COMMENT '日期',
       
       order_cnt BIGINT COMMENT '成交单量',
       miniapp_order_cnt BIGINT COMMENT '小程序成交单量',
       miniapp_order_ratio DECIMAL(5,2) COMMENT '小程序成交量占比',
       android_order_cnt BIGINT COMMENT '安卓APP订单量',
       android_order_ratio DECIMAL(5,2) COMMENT '安卓APP订单量占比',
       ios_order_cnt BIGINT COMMENT '苹果APP订单量',
       ios_order_ratio DECIMAL(5,2) COMMENT '苹果APP订单量占比',
       pcweb_order_cnt BIGINT COMMENT 'PC商城成交单量',
       pcweb_order_ratio DECIMAL(5,2) COMMENT 'PC商城成交单量占比'
    )
    COMMENT '渠道销量占比' 
    ROW format delimited fields terminated BY '\t' 
    stored AS orc tblproperties ('orc.compress' = 'SNAPPY');
    ```

  - 实现
  
    ```sql
    --渠道销量占比
    insert into yp_rpt.rpt_sale_fromtype_ratio
    select 
       date_time,
       time_type,
       year_code,
       year_month,
       dim_date_id,
       -- 成交单量
       order_cnt,
       --小程序成交单量
       miniapp_order_cnt,
       cast(
          cast(miniapp_order_cnt as DECIMAL(38,4)) / cast(order_cnt as DECIMAL(38,4))
          * 100
          as DECIMAL(5,2)
       ) miniapp_order_ratio,
       --安卓APP订单量占比
       android_order_cnt,
       cast(
          cast(android_order_cnt as DECIMAL(38,4)) / cast(order_cnt as DECIMAL(38,4))
          * 100
          as DECIMAL(5,2)
       ) android_order_ratio,
       --苹果APP订单量
       ios_order_cnt,
       cast(
          cast(ios_order_cnt as DECIMAL(38,4)) / cast(order_cnt as DECIMAL(38,4))
          * 100
          as DECIMAL(5,2)
       ) ios_order_ratio,
       --PC商城成交单量
       pcweb_order_cnt,
       cast(
          cast(pcweb_order_cnt as DECIMAL(38,4)) / cast(order_cnt as DECIMAL(38,4))
          * 100
          as DECIMAL(5,2)
       ) pcweb_order_ratio
    from yp_dm.dm_sale
    where group_type = 'all';
    ```


  ![image-20211204164818513](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211204164818513.png)

---

#### 知识点04：RPT层搭建--商品主题报表

##### 需求一：商品销量==topN==

> 统计出某天销量最多的top10商品
>
> ```sql
> --商品销量TOPN
> drop table if exists yp_rpt.rpt_goods_sale_topN;
> create table yp_rpt.rpt_goods_sale_topN(
>     `dt` string COMMENT '统计日期',
>     `sku_id` string COMMENT '商品ID',
>     `payment_num` bigint COMMENT '销量'
> ) COMMENT '商品销量TopN'
> ROW format delimited fields terminated BY '\t'
> stored AS orc tblproperties ('orc.compress' = 'SNAPPY');
> 
> --商品销量TOPN
> --注意，这里为了最终展示效果，保证有数据，特意在时间dt上做了特殊处理。
> --本来是需要通过dt指定某一天数据的，这里忽略dt过滤 ，直接使用全部数据。
> 
> insert into yp_rpt.rpt_goods_sale_topN
> select
>     '2021-08-17' dt,
>     sku_id,
>     payment_count
> from
>     yp_dws.dws_sku_daycount
> order by payment_count desc
> limit 10;
> ```
>
> 

##### 需求二：商品收藏==topN==

> 统计出某天收藏量最多的top10商品
>
> ```sql
> --商品收藏TOPN
> drop table if exists yp_rpt.rpt_goods_favor_topN;
> create table yp_rpt.rpt_goods_favor_topN(
>     `dt` string COMMENT '统计日期',
>     `sku_id` string COMMENT '商品ID',
>     `favor_count` bigint COMMENT '收藏量'
> ) COMMENT '商品收藏TopN'
> ROW format delimited fields terminated BY '\t' 
> stored AS orc tblproperties ('orc.compress' = 'SNAPPY');
> 
> --商品收藏TOPN
> --注意，这里为了最终展示效果，保证有数据，特意在时间dt上做了特殊处理。
> --本来是需要通过dt指定某一天数据的，这里忽略dt过滤 ，直接使用全部数据。
> insert into yp_rpt.rpt_goods_favor_topN
> select
>     '2021-08-17' dt,
>     sku_id,
>     favor_count
> from
>     yp_dws.dws_sku_daycount 
> order by favor_count desc
> limit 10;
> ```
>
> 

##### 需求三：商品加入购物车==topN==

> 统计出某天购物车最多的top10商品
>
> ```sql
> --商品加入购物车TOPN
> drop table if exists yp_rpt.rpt_goods_cart_topN;
> create table yp_rpt.rpt_goods_cart_topN(
>     `dt` string COMMENT '统计日期',
>     `sku_id` string COMMENT '商品ID',
>     `cart_num` bigint COMMENT '加入购物车数量'
> ) COMMENT '商品加入购物车TopN'
> ROW format delimited fields terminated BY '\t' 
> stored AS orc tblproperties ('orc.compress' = 'SNAPPY');
> 
> 
> --商品加入购物车TOPN
> --注意，这里为了最终展示效果，保证有数据，特意在时间dt上做了特殊处理。
> --本来是需要通过dt指定某一天数据的，这里忽略dt过滤 ，直接使用全部数据。
> insert into yp_rpt.rpt_goods_cart_topN
> select
>     '2021-08-17' dt,
>     sku_id,
>     cart_num
> from
>     yp_dws.dws_sku_daycount
> order by cart_num desc
> limit 10;
> ```
>
> 

**拓展: 商品退款率TOPN**

统计近30天商品的退款率的top10商品

>```sql
>--拓展: 商品退款率TOPN
>drop table if exists yp_rpt.rpt_goods_refund_topN;
>create table yp_rpt.rpt_goods_refund_topN(
>    `dt` string COMMENT '统计日期',
>    `sku_id` string COMMENT '商品ID',
>    `refund_ratio` decimal(10,2) COMMENT '退款率'
>) COMMENT '商品退款率TopN'
>ROW format delimited fields terminated BY '\t' 
>stored AS orc tblproperties ('orc.compress' = 'SNAPPY');
>
>--拓展: 商品退款率TOPN
>--注意，这里为了最终展示效果，保证有数据，特意在时间dt上做了特殊处理。
>--本来是需要通过dt指定某一天数据的，这里忽略dt过滤 ，直接使用全部数据。
>insert into yp_rpt.rpt_goods_refund_topN
>select
>    '2021-08-17' dt,
>    sku_id,
>    cast(
>      cast(refund_last_30d_count as DECIMAL(38,4)) / cast(payment_last_30d_count as DECIMAL(38,4))
>      * 100
>      as DECIMAL(5,2)
>   ) refund_ratio
>from yp_dm.dm_sku 
>where payment_last_30d_count!=0
>order by refund_ratio desc
>limit 10;
>```
>
>



---

#### 知识点05：RPT层搭建--用户主题报表

##### 综合需求: 统计==会员==相关各类指标

> 活跃会员数、新增会员数、新增消费会员数、总付费会员数、总会员数、会员活跃率等。

- 建表

  ```sql
  --用户数量统计
  drop table if exists yp_rpt.rpt_user_count;
  create table yp_rpt.rpt_user_count(
      dt string COMMENT '统计日期',
      day_users BIGINT COMMENT '活跃会员数',
      day_new_users BIGINT COMMENT '新增会员数',
      day_new_payment_users BIGINT COMMENT '新增消费会员数',
      payment_users BIGINT COMMENT '总付费会员数',
      users BIGINT COMMENT '总会员数',
      day_users2users decimal(38,4) COMMENT '会员活跃率',
      payment_users2users decimal(38,4) COMMENT '总会员付费率',
      day_new_users2users decimal(38,4) COMMENT '会员新鲜度'
  )
  COMMENT '用户数量统计报表'
  ROW format delimited fields terminated BY '\t'
  stored AS orc tblproperties ('orc.compress' = 'SNAPPY');
  ```

- 实现思路

  ```properties
  #首先，确定干活统计的日期，比如2021-08-24
  
  #1、活跃会员数
  	如果用户最后一次登录为统计日期之前一天的,表示活跃会员: sum(if(login_date_last = '2021-08-23', 1, 0)
  	
  #2、新增会员数  
  	第一次登录为统计日期前一天的,表示新增会员 : sum(if(login_date_first = '2021-08-23', 1, 0)
  	
  #3、新增消费会员数	
  	首次支付时间为统计日期前一天的,表示新增消费会员:  sum(if(payment_date_first='2021-08-23', 1, 0)
  
  #4、总付费会员数
  	支付次数大于0的,表示付费会员: sum(if(payment_count>0,1,0)
  	
  #5、总会员数 
  	统计所有会员: count(*)
  
  #6、会员活跃率
  	最后一次登录时间为统计日期前一天的,表示这个人很活跃  
  	除以总会员数即是会员活跃率: sum(if(login_date_last = '2021-08-23', 1, 0)/count(*)
  	
  #7、总会员付费率
  	支付次数payment_count大于0次的表示该用户支付过  不管支付几次
  	除以从会员数即是总会员付费率: sum(if(payment_count>0,1,0)/count(*)
  	
  #8、会员新鲜度
  	在统计日期前一天中所有登录用户中，判断哪些是第一次登录的就是新会员: login_date_first='2021-08-23'  
  	会员新鲜度:sum(if(login_date_first='2021-08-23',1,0)/sum(if(login_date_last='2021-08-23',1,0)
  	login_date_last 最后一次登录的时间
  	login_date_first 第一次登录的时间
  ```

- sql实现

  ```sql
  --用户数量统计
  insert into yp_rpt.rpt_user_count
  select
      '2020-08-29',
      sum(if(login_date_last='2020-08-28',1,0)), --活跃会员数
      sum(if(login_date_first='2020-08-28',1,0)),--新增会员数
      sum(if(payment_date_first='2020-08-28',1,0)), --新增消费会员数
      sum(if(payment_count>0,1,0)), --总付费会员数
      count(*), --总会员数
      if(
          sum(if(login_date_last = '2020-08-28', 1, 0)) = 0,
          null,
          cast(sum(if(login_date_last = '2020-08-28', 1, 0)) as DECIMAL(38,4))
      )/count(*), --会员活跃率
      if(
          sum(if(payment_count>0,1,0)) = 0,
          null,
          cast(sum(if(payment_count>0,1,0)) as DECIMAL(38,4))
      )/count(*), --总会员付费率
      if(
          sum(if(login_date_first='2020-08-28',1,0)) = 0,
          null,
          cast(sum(if(login_date_first='2020-08-28',1,0)) as DECIMAL(38,4))
      )/sum(if(login_date_last='2020-08-28',1,0)) --会员新鲜度
  from yp_dm.dm_user;
  
  ```

-----

#### 知识点06：数据导出--RPT层数据至MySQL

- 新零售数仓架构图

  > 从数仓架构图上，感受**为什么最终需要把拼接的数据导出存储在mysql**。
  >
  > **报表系统直接从hive数仓RPT层中读取数据使用可不可以？**  可以但是没必要。

  ![image-20230607234703645](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230607234703645.png)

- step1：presto配置连接MySQL

  - 配置mysql Connector

    > 在每台Presto服务的==etc/catalog==目录下，新建文件==mysql.properties==，内容如下
    >
    > ==**注意: 两台都要配置!!!**==
    >
    > ```shell
    > [root@hadoop01/2 ~]# cd /export/server/presto/etc/catalog
    > [root@hadoop01/2 catalog]# ls
    > hive.properties
    > [root@hadoop01/2 catalog]# vim /export/server/presto/etc/catalog/mysql.properties
    > ```
    >
    > ```txt
    > # 复制粘贴以下内容到mysql.properties中
    > connector.name=mysql
    > connection-url=jdbc:mysql://192.168.88.80:3306?enabledTLSProtocols=TLSv1.2&useUnicode=true&characterEncoding=utf8
    > connection-user=root
    > connection-password=123456
    > ```

  - 重启presto集群

    >==**注意: 两台都要重启!!!**==
    
    ```shell
    [root@hadoop01/2 catalog]# /export/server/presto/bin/launcher restart
    Stopped 6663
    Started as 27027
    ```
    
  - 去页面查看服务是否正常

    >http://192.168.88.80:8090/ui/

  - Datagrip中验证是否可以连接MySQL

    > 在presto中根据自己的需要选择需要刷新的catalog、schema等。

    ![image-20211017150803387](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211017150803387.png)

- step2：MySQL中建库

  > 在DataGrip中==选中mysql数据源==,按下F4，在控制台中输入以下sql

  ```sql
  -- 建库
  CREATE DATABASE yp_olap DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
  ```

  ![image-20211017151233258](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211017151233258.png)

- step3：使用==Presto在MySQL中建表==

  > 建表时使用Presto来操作，create语法和hive大体相同，只需要将hive中的string类型改为varchar。
  >
  > 另外文件格式、压缩格式这些mysql没有的配置去掉就可以了。
  >
  > 注意==presto不能直接删除mysql的表或数据==。

  - 详细sql语句参考课程脚本资料。

  ![image-20211017151441279](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211017151441279.png)

- step4：==使用Presto将数据导出到MySQL中==

  - 语法格式

    ```sql
    insert into mysql.yp_olap.rpt_sale_store_cnt_month 
    select  * from hive.yp_rpt.rpt_sale_store_cnt_month;
    ```

  - 完整版sql可以参考课程资料

    ![image-20211017151915476](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211017151915476.png)

----

#### 知识点07：数据报表/可视化

##### 问题1：什么是报表、数据可视化？

- 概念

  ```properties
  	人类是视觉动物。
  	一般情况下，分析的数据通过表格和图形的方式来呈现更利于理解，我们常说用图表说话就是这个意思。
  	常用的数据图表包括饼图、柱形图、条形图、折线图、散点图、雷达图等，当然可以对这些图表进一步整理加工，使之变为我们所需要的图形，例如金字塔图、矩阵图、漏斗图等。
  	大多数情况下，人们更愿意接受图形这种数据展现方式，因为它能更加有效、直观地传递出分析所要表达的观点。记位，一般情况不，能用图说明问题的就不用表格，能用表格说明问题的就不要用文字。
  ```

  ![image-20211204173817420](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211204173817420.png)

  ![img](Day09_RPT层建设实战、自动化调度、可视化.assets/f3b1fa0c34dd43be73fbce1ee1c72914_720w.jpg)

- 功能与作用

  ```properties
  	报表是数据呈现的载体，功能也就是来展现数据的。
  	报表的作用在于呈现数字，数字呈现的作用在于展示现状，明确与目标的差距，进而用于后续改善方案及行动计划的指南。
  ```

##### 问题2：企业中谁来负责报表？

> ==产品经理、产品运营、BI工程师==
>
> https://www.zhihu.com/question/20565938

![image-20211204174653213](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211204174653213.png)

##### **问题3：如何实现报表可视化？**

![image-20230608003153043](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608003153043.png)

- 开源技术实现前端报表、后端数据查询

  - ==前端==

    > 主要指各种JS技术，可以在html页面上进行各种图形表格、页面布局的绘画。
  >
    > 这当中有的js是已经制作好了各种图形表格，上手即用，有的需要自己重零绘制。
  >
    > 比如：==百度echarts  、highcharts、Vue.js等==

    ![image-20211204175411879](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211204175411879.png)

    ![image-20211204175432592](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211204175432592.png)

  - ==后端==

    > 只要指的是能够从数据库、数据仓库等处查询数据、封装返回给前端页面展示。
  >
    > 常用的技术有：==Java、Python、PHP==等。

- 专业的BI软件

  - ==微软Power BI==

    https://powerbi.microsoft.com/zh-cn/

    ![image-20211204174910380](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211204174910380.png)

  - ==tableau==

    https://www.tableau.com/

    ![image-20211204175040649](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211204175040649.png)

  - ==帆软BI==

    https://www.finebi.com/

    ![image-20211204175116962](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211204175116962.png)

  

----

#### 知识点08：新零售数据可视化--Java后端、Vue前端（了解）

> 详细操作见资料。

- maven配置

- IDEA使用
- Nodejs安装
- WebStorem使用
- 前后端接口调试

----

#### 知识点09：新零售数据可视化--FineBI（理解）

- 介绍

  ```shell
  	FineBI 是帆软软件有限公司推出的一款商业智能（Business Intelligence）产品。FineBI 是定位于自助大数据分析的 BI 工具，能够帮助企业的业务人员和数据分析师，开展以问题导向的探索式分析。
  ```

  ![image-20211207152417037](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207152417037.png)

- 使用教程

  > 可以去之前学习资料中回顾

##### 1.连接yp_olap数据库

![image-20230608152320069](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608152320069.png)

![image-20230608152415116](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608152415116.png)

![image-20230608152718323](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608152718323.png)

![image-20230608152804203](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608152804203.png)

##### 2.关联yp_olap中的数据表

![image-20230608152949368](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608152949368.png)

![image-20230608153047978](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608153047978.png)

![image-20230608153115337](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608153115337.png)

![image-20230608153257312](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608153257312.png)

![image-20230608153331177](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608153331177.png)

![image-20230608153403428](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608153403428.png)

##### 3.新建新零售项目仪表板

![image-20230608155117642](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608155117642.png)

![image-20230608155248282](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608155248282.png)

![image-20230608155354157](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608155354157.png)

![image-20230608162328436](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608162328436.png)



> **进入组件,对维度和指标进行拖拉拽,依次完成对应的每个报表**
>
> **最终多个报表在同一个仪表板展示,示例如下:** 

![image-20230608164346826](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608164346826.png)

#### 知识点10：自动化调度方案--工作流调度与oozie

- **workflow工作流的概念**

  - 工作流（Workflow），指“==业务过程的部分或整体在计算机应用环境下的自动化==”。
  - 工作流解决的主要问题是：为了实现某个业务目标，利用计算机软件在多个参与者之间按某种预定规则自动传递文档、信息或者任务。
  - 核心概念:==依赖执行== ==周期重复执行==
  - DAG（有向无环图）

  ![image-20211207153527233](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207153527233.png)

- **Apache Oozie介绍**

  - Oozie是一个用来管理 Hadoop生态圈job的工作流调度系统。由Cloudera公司贡献给Apache。

  ![image-20211207153604869](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207153604869.png)

  - ==oozie本身单独使用极其不方便，配置文件极其繁琐==，不管是使用shell命令提交工作流还是使用java API提交工作流，都需要编写大量繁琐的xml配置文件；
  - 但是==oozie和hue整合==之后使用还是非常不错的，在hue页面上提供了拖拽功能，直接选择想要调度的脚本或者其他任务，还可以根据自己的需求编写定时规则。

  

**工作流类型**

> ==workflow== **普通工作流** 没有定时和条件触发功能。
>
> ==coordinator== **定时工作流** 可以设置执行周期和频率
>
> **bundle** **批处理工作流**  一次可以提交执行多个coordinator

![image-20211005224532079](../../../../../就业班资料/02_新零售项目/学生端资料/Day02_数仓生态圈辅助工具/02_课堂笔记/day02数仓生态圈辅助工具.assets/image-20211005224532079.png)

![image-20211005224613359](../../../../../就业班资料/02_新零售项目/学生端资料/Day02_数仓生态圈辅助工具/02_课堂笔记/day02数仓生态圈辅助工具.assets/image-20211005224613359.png)

![image-20211005224643377](../../../../../就业班资料/02_新零售项目/学生端资料/Day02_数仓生态圈辅助工具/02_课堂笔记/day02数仓生态圈辅助工具.assets/image-20211005224643377.png)



- ##### ==**栗子 1**==：在Hue上使用oozie提交一个shell脚本执行

  - step1：打开hue页面

    http://hadoop02:8889/hue    用户名、密码：hue

  - step2：上传一个shell脚本或者使用hue在线编写一个shell脚本

    ![image-20211207154319705](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207154319705.png)

  - step3：配置oozie调度任务

    ![image-20211207154407666](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207154407666.png)

    ![image-20211207154520342](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207154520342.png)

    ![image-20211207154608753](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207154608753.png)

    ![image-20211207154631536](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207154631536.png)

    ![image-20211207154729854](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207154729854.png)

    ![image-20211207154756388](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207154756388.png)
    
    >注意: 如果执行失败,一般都是yarn容器内存没有更改,需要去更改yarn容器内存为3g
    >
    >![image-20230608171950193](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230608171950193.png)

![image-20211207154936254](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207154936254.png)

![image-20211207155141788](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207155141788.png)

![image-20211207155209422](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207155209422.png)

- ==**栗子2**==：针对栗子1的调度任务，配置周期定时执行coordinator。

  > 刚才配置的workflow属于一次性的工作流，执行完就结束了。
  >
  > 可以==配置coordinator来控制workflow的执行周期和触发频率==。

  ![image-20211207155342016](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207155342016.png)

  ![image-20211207155435961](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207155435961.png)

  ![image-20211207155503337](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207155503337.png)

  ![image-20211207155712125](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207155712125.png)

  ![image-20211207155851433](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207155851433.png)

  ![image-20211207155910176](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20211207155910176.png)

----

注意: 内存保证够用的情况下



#### 知识点11：自动化调度方案--shell基本知识回顾

- date命令

  ```shell
  #获取今天的日期
  date
  date +%Y%m%d
  
  #获取指定日期的年月日格式输出
  date -d "2014-11-12" +%Y%m%d
  
  #获取指定日期的星期（周几）格式输出
  date --date="2014-11-23" +%w
  
  #获取上周日期（day,month,year,hour）
  date -d "-1 week" +%Y%m%d
  
  #获取昨天日期
  date -d '-1 day' "+%Y-%m-%d"
  date --date="-24 hour" +%Y%m%d
  ```

- 变量提取、反引号的功能

  ```shell
  name="binzi"
  echo ${name}
  
  date
  nowTime=date
  echo ${nowTime}
  
  date
  nowTime=`date` 
  echo ${nowTime}
  ```

- 数字运算

  ```shell
  # 双小括号命令是用来执行数学表达式的，可以在其中进行各种逻辑运算、数学运算，也支持更多的运算符（如++、--等）
  
  echo $((5 * 2))
  
  i=5
  echo $((i=$i*2)) #10
  echo $((i=i*2))  #20
  
  
  echo $((i*2)) # 40
  
  echo $[10 + 20] # 30
  echo `expr 10 + 20` # 30
  ```
  
- 串行与并行

  ```shell
  sleep 5 
  echo "done"
  # 上述脚本需要等待5s后才能在屏幕上看到"done"。
  
  # 串行变成并行加&
  #shell脚本默认是按顺序串行执行的，使用&可以将一个命令放在后台运行，从而使shell脚本能够继续往后执行
  sleep 5 &
  echo "done"
  #上面的脚本执行后会立即打印出"done"，sleep命令被扔给后台执行，不会阻塞脚本执行。
  
  
  # 并行变串行加wait
  #如果想要在进入下个循环前，必须等待上个后台命令执行完毕，可以使用wait命令
  sleep 5 &
  wait
  echo "done"
  #这样，需要等待5s后才能在屏幕上看到"done"。
  ```

- 回顾: shell动态传参

  > 名字, 时间, 类路径等属性不在脚本程序中写死, 而是通过运行时动态传入参数获取. 

  ```shell
  [root@hadoop02 tmp]# vim 2.sh
  #!/bin/bash
  echo "脚本名称是: $0"
  echo "第一个参数是: $1"
  echo "第一个参数是: $2"
  echo "动态传入的参数的个数是: $#"
  echo "动态传入的参数列表显示: $*"
  
  ```

----

#### 知识点12：自动化调度方案--脚本实现、调度实现

- 脚本实现

  > 脚本实现的关键是如何在shell建表中执行sqoop命令、hive sql文件
  >
  > 并且关于==时间==的地方==不能写死==，而是使用shell  ==date命令来动态获取==

  - **例子一：ODS数据导入**

    ![image-20230609102742277](Day09_RPT层建设实战、自动化调度、可视化.assets/image-20230609102742277.png)

    ```shell
    #! /bin/bash
    SQOOP_HOME=/usr/bin/sqoop
    if [[ $1 == "" ]];then
       TD_DATE=`date -d '1 days ago' "+%Y-%m-%d"`
    else
       TD_DATE=$1
    fi
    
    #上述这段shell是什么意思？能否看懂？
    ```

    - 首次执行脚本

      ```shell
      #仅新增
      /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
      --connect 'jdbc:mysql://172.17.0.202:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
      --username root \
      --password 123456 \
      --query "select *, '${TD_DATE}' as dt from t_goods_evaluation where 1=1 \$CONDITIONS" \
      --hcatalog-database yp_ods \
      --hcatalog-table t_goods_evaluation \
      -m 1
      
      # 新增和更新同步
      /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
      --connect 'jdbc:mysql://172.17.0.202:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
      --username root \
      --password 123456 \
      --query "select *, '${TD_DATE}' as dt from t_store where 1=1 and \$CONDITIONS" \
      --hcatalog-database yp_ods \
      --hcatalog-table t_store \
      -m 1
      ```

    - 循环执行脚本

      > 1、通过sqoop的query查询把增量数据查询出来。
      >
      > 增量的范围是TD_DATE值的 00：00：00  至  23：59：59
      >
      > 2、判断的字段是
      >
      > ​	如果是仅新增同步，使用create_time创建时间即可
      >
      > ​	如果是新增和更新同步，需要使用create_time 和 update_time两个时间
      >
      > 3、这也要求在业务系统数据库设计的时候，需要有意识的增加如下字段
      >
      > ​	create_user
      >
      > ​	create_time
      >
      > ​	update_user
      >
      > ​	update_time

      ```shell
      #仅新增
      /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
      --connect 'jdbc:mysql://172.17.0.202:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
      --username root \
      --password 123456 \
      --query "select *, '${TD_DATE}' as dt from t_goods_evaluation where 1=1 and (create_time between '${TD_DATE} 00:00:00' and '${TD_DATE} 23:59:59') and  \$CONDITIONS" \
      --hcatalog-database yp_ods \
      --hcatalog-table t_goods_evaluation \
      -m 1
      
      # 新增和更新同步
      /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
      --connect 'jdbc:mysql://172.17.0.202:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
      --username root \
      --password 123456 \
      --query "select *, '${TD_DATE}' as dt from t_store where 1=1 and ((create_time between '${TD_DATE} 00:00:00' and '${TD_DATE} 23:59:59') or (update_time between '${TD_DATE} 00:00:00' and '${TD_DATE} 23:59:59')) and  \$CONDITIONS" \
      --hcatalog-database yp_ods \
      --hcatalog-table t_store \
      -m 1
      ```

  - **例子二：DWB层sql脚本执行**

    > 在shell中执行hive sql的方式有两种
    >
    > bin/hive -e ‘sql语句’
    >
    > bin/hive -f  xxx.sql文件

    - 首次执行

      ```shell
      #! /bin/bash
      HIVE_HOME=/usr/bin/hive
      
      
      ${HIVE_HOME} -S -e "
      --分区
      SET hive.exec.dynamic.partition=true;
      SET hive.exec.dynamic.partition.mode=nonstrict;
      set hive.exec.max.dynamic.partitions.pernode=10000;
      set hive.exec.max.dynamic.partitions=100000;
      set hive.exec.max.created.files=150000;
      --=======订单宽表=======
      insert into yp_dwb.dwb_order_detail partition (dt)
      select
      	xxxxxxx
      ;
      ```

    - 循环执行

      ```shell
      #! /bin/bash
      HIVE_HOME=/usr/bin/hive
      
      #上个月1日
      Last_Month_DATE=$(date -d "-1 month" +%Y-%m-01)
      
      
      ${HIVE_HOME} -S -e "
      --分区配置
      SET hive.exec.dynamic.partition=true;
      SET hive.exec.dynamic.partition.mode=nonstrict;
      set hive.exec.max.dynamic.partitions.pernode=10000;
      set hive.exec.max.dynamic.partitions=100000;
      set hive.exec.max.created.files=150000;
      
      --=======订单宽表=======
      --增量插入
      insert overwrite table yp_dwb.dwb_order_detail partition (dt)
      select
      	xxxxxx
      -- 读取上个月1日至今的数据
      SUBSTRING(o.create_date,1,10) >= '${Last_Month_DATE}' and o.start_date >= '${Last_Month_DATE}';
      ```

  - **例子三：Presto的sql如何在shell中执行**

    ```shell
    #! /bin/bash
    #昨天
    if [[ $1 == "" ]];then
       TD_DATE=`date -d '1 days ago' "+%Y-%m-%d"`
    else
       TD_DATE=$1
    fi
    
    PRESTO_HOME=/opt/cloudera/parcels/presto/bin/presto
    
    
    ${PRESTO_HOME} --catalog hive --server 172.17.0.202:8090 --execute "
    delete from yp_rpt.rpt_sale_store_cnt_month where date_time = '${TD_DATE}';
    .......
    "
    ```

- 调度实现

  > 脚本可以参考课程资料。





















