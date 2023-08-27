### Day07_DWS层建设实战-2

#### 知识点01：课程内容大纲与学习目标

```shell
#课程内容大纲
	1、DWS层构建
		商品主题统计宽表的实现
			核心：表关系梳理、字段抽取、结果合并
		用户主题统计宽表的实现思路
	2、Hive优化--索引
		index索引问题
		ORC文件格式的索引
			行组索引
			布隆过滤器索引
#学习目标
	掌握主题需求的分析
	掌握表关系梳理与字段抽取
	理解index的概念与作用
	掌握Hive ORC格式文件的两种索引
```

#### 知识点02：DWS层搭建--商品主题宽表

- 主题需求

  - 指标

    ```properties
    下单次数、下单件数、下单金额、被支付次数、被支付件数、被支付金额、被退款次数、被退款件数、被退款金额、被加入购物车次数、被加入购物车件数、被收藏次数、好评数、中评数、差评数
    
    --总共15个指标
    ```

  - 维度

    ```properties
    日期（day）+商品
    ```

- 本主题建表操作

  > 注意：建表操作需要在hive中执行，presto不支持hive的建表语法。
  >
  > ==spu==: **具体的一类商品商品信息(规范),**  iphone 14 pro max
  >
  > ==sku==: **商品最小单位**, 即: 库存量, iphone 14 pro max 暗夜紫 1TB

  ```sql
  create table yp_dws.dws_sku_daycount 
  (
      dt STRING,
      sku_id string comment 'sku_id',
      sku_name string comment '商品名称',
      order_count bigint comment '被下单次数',
      order_num bigint comment '被下单件数',
      order_amount decimal(38,2) comment '被下单金额',
      payment_count bigint  comment '被支付次数',
      payment_num bigint comment '被支付件数',
      payment_amount decimal(38,2) comment '被支付金额',
      refund_count bigint  comment '被退款次数',
      refund_num bigint comment '被退款件数',
      refund_amount  decimal(38,2) comment '被退款金额',
      cart_count bigint comment '被加入购物车次数',
      cart_num bigint comment '被加入购物车件数',
      favor_count bigint comment '被收藏次数',
      evaluation_good_count bigint comment '好评数',
      evaluation_mid_count bigint comment '中评数',
      evaluation_bad_count bigint comment '差评数'
  ) COMMENT '每日商品行为'
  --PARTITIONED BY(dt STRING)
  ROW format delimited fields terminated BY '\t'
  stored AS orc tblproperties ('orc.compress' = 'SNAPPY');
  
  ```

- 扩展知识：如何优雅的给变量起名字？

  > https://www.zhihu.com/question/21440067/answer/1277465532  变量命名的一些规范. 
  >
  > https://unbug.github.io/codelf/  提示你各种命名写法的网站.
  >
  > https://translate.google.cn/   谷歌翻译网址.

-----

#### 知识点03：DWS层搭建--商品主题宽表--需求分析

- 确定指标字段与表的关系

  > 当需求提出指标和维度之后，我们需要做的就是确定通过哪些表能够提供支撑。
  >
  > 思考：是不是意味着==数仓分层之后，上一层的表只能查询下一层的，能不能跨层？==
  >
  > 答案是不一定的。
  >
  > 
  >
  > ==维度:==  都来自于订单明细宽表
  >
  > **日期** , **商品**(编号和名称)
  >
  > 
  >
  > ==指标:== 分别来自不同的表
  >
  > 下单类指标相关字段:  **订单编号 , 购买商品数量 ,下单金额** 来自于订单明细宽表
  >
  > 支付类指标相关字段:  **订单状态/订单是否支付 , 订单编号 ,  购买商品数量 ,  下单金额 **来自于订单明细宽表
  >
  > 退款类指标相关字段:  **退款编号 , 订单编号,  购买商品数量 ,  下单金额**  来自于订单明细宽表
  >
  > 购物车类指标相关字段: **购物车id , 购物件数** 来自于购物车事实表
  >
  > 收藏次数指标相关字段: **商品收藏id** 来自于商品收藏事实表
  >
  > 好中差评指标相关字段: **商品评分** 来自于商品评价明细事实表

```shell
#维度
	dt : 时间
	goods_id : 商品编号
	goods_name: 商品名称

#下单次数、下单件数、下单金额
dwb_order_detail
 	order_id : 下单次数相当于下了多少订单（有多少个包含这个商品的订单）
 	buy_num : 下单件数相当于下了多少商品 
 	total_price : 每个商品下单金额指的是订单金额还是商品金额？应该是商品金额(订单中可能会包含其他商品)

#被支付次数、被支付件数、被支付金额
dwb_order_detail
	#支付状态的判断
		is_pay: 这个字段也可以 0表示未支付，1表示已支付。#推荐使用这个字段来判断
	#次数 件数 金额
    	order_id
    	buy_num
    	total_price

#被退款次数、被退款件数、被退款金额
dwb_order_detail
	#退款的判断
		refund_id: 退款单号 is not null的就表明有退款
	#次数 件数 金额	
		order_id
    	buy_num
    	total_price

#被加入购物车次数、被加入购物车件数
yp_dwd.fact_shop_cart(能够提供购物车相关信息的只有这张表)
	id: 次数
	buy_num: 件数

#被收藏次数
yp_dwd.fact_goods_collect
	id: 次数

#好评数、中评数、差评数
yp_dwd.fact_goods_evaluation_detail
	geval_scores_goods:商品评分0-10分
	
	#如何判断 好  中  差 （完全业务指定）
		得分: >= 9 	 好
		得分: >6  <9   中
		得分：<= 6  	差

```

> 概况起来，计算商品主题宽表，需要参与计算的表有：
>
> ==订单明细宽表 : yp_dwb.dwb_order_detail==
>
> ==购物车事实表 : yp_dwd.fact_shop_cart==
>
> ==商品收藏事实表 : yp_dwd.fact_goods_collect==
>
> ==商品评价事实表 : yp_dwd.fact_goods_evaluation_detail==

----

+ 整体语法结构如下:

```sql
insert into hive.yp_dws.dws_sku_daycount
with order_base as (select...去重...from yp_dwb.dwb_order_detail), -- 获取订单所有数据
	 order_count as (select...from order_base...),-- 查询并计算订单对应3个指标 
	 
	 pay_base as (select...去重...from yp_dwb.dwb_order_detail),-- 获取被支付订单所有数据
	 payment_count as (select...from pay_base...),-- 查询并计算被支付对应的3个指标
	 
	 refund_base as (select...去重...from yp_dwb.dwb_order_detail),-- 获取被退款订单所有数据
	 refund_count as (select...from refund...),-- 查询并计算被退款对应的3个指标
	 
	 cart_count as (select...from yp_dwd.fact_shop_cart...),--查询并计算购物车对应2个指标
	 favor_count as (select...from yp_dwd.fact_goods_collect...),--查询并计算收藏对应1个指标
	 evaluation_count as (select...from yp_dwd.fact_goods_evaluation_detail...), --查询并计算评价对应3个指标
	 
	 unionall as (...) -- 把上述6个临时结果合并到一起
select 
	... -- 注意: 字段顺序必须和建表的时候对应上
from unionall
group by dt, sku_id;
```



#### 知识点04：DWS层搭建--商品主题宽表--step1--下单、支付、退款统计

- 大前提：==**使用row_number对数据进行去重**==

  > 基于dwb_order_detail表根据商品（goods_id）进行统计各个指标的时候；
  >
  > 为了避免同一笔订单下有多个重复的商品出现（正常来说重复的应该合并在一起了）；
  >
  > 应该使用row_number对order_id和goods_id进行去重。

  ```sql
  --订单明细表抽取字段，并且进行去重，作为后续的base基础数据
  with order_base as (select
      dt,
      order_id, --订单id
      goods_id, --商品id
      goods_name,--商品名称
      buy_num,--购买商品数量
      total_price,--商品总金额（数量*单价）
      row_number() over(partition by order_id,goods_id) as rn
  from yp_dwb.dwb_order_detail),
  
  ```
  
- 下单次数、件数、金额统计

  > 基于上述的order_base进行查询

  ```sql
  -- 计算下单次数、件数、金额统计
  order_count as (select
      dt,goods_id as sku_id,goods_name as sku_name,
      count(order_id) order_count,
      sum(buy_num) order_num,
      sum(total_price) order_amount
  from order_base where rn =1
  group by dt,goods_id,goods_name),
  ```

- 支付次数、件数、金额统计

  > 计算支付相关指标之前，可以先使用==is_pay进行订单状态过滤==
  >
  > 然后基于过滤后的数据进行统计
  >
  > 可以继续使用CTE引导

  ```sql
  --订单状态,已支付
  pay_base as(
      select *,
      row_number() over(partition by order_id, goods_id) rn
      from yp_dwb.dwb_order_detail
      where is_pay=1
  ),
  
  --支付次数、件数、金额统计
  payment_count as(
      select dt, goods_id sku_id, goods_name sku_name,
         count(order_id) payment_count,
         sum(buy_num) payment_num,
         sum(total_price) payment_amount
      from pay_base
      where rn=1
      group by dt, goods_id, goods_name
  ),
  ```

- 退款次数、件数、金额统计

  > 可以先使用==refund_id is not null查询出退款订单==，然后进行统计

  ```sql
  --先查询出退款订单
  refund_base as(
      select *,
         row_number() over(partition by order_id, goods_id) rn
      from yp_dwb.dwb_order_detail
      where refund_id is not null
  ),
  -- 退款次数、件数、金额
  refund_count as (
      select dt, goods_id sku_id, goods_name sku_name,
         count(order_id) refund_count,
         sum(buy_num) refund_num,
         sum(total_price) refund_amount
      from refund_base
      where rn=1
      group by dt, goods_id, goods_name
  ),
  ```

-----

#### 知识点05：DWS层搭建--商品主题宽表--step2--购物车、收藏统计

> 思考：==为什么下面这两个查询需要考虑拉链的状态？上面的下单、支付等统计为什么不需要？==

- 购物车次数、件数统计

  ```sql
  -- 购物车次数、件数
  cart_count as (
      select substring(create_time, 1, 10) dt, goods_id sku_id,
             count(id) cart_count,
             sum(buy_num) cart_num
      from yp_dwd.fact_shop_cart
      where end_date = '9999-99-99'
      group by substring(create_time, 1, 10), goods_id
  ),
  ```

- 收藏次数统计

  ```sql
  -- 收藏次数
  favor_count as (
      select substring(c.create_time, 1, 10) dt, goods_id sku_id,
             count(c.id) favor_count
      from yp_dwd.fact_goods_collect c
      where end_date='9999-99-99'
      group by substring(c.create_time, 1, 10), goods_id
  ),
  ```

----

#### 知识点06：DWS层搭建--商品主题宽表--step3--好评、中评、差评

- 好评、中评、差评次数

  > 根据评分来判断好评、中评、差评        

  ```sql
  -- 好评、中评、差评数量                
  evaluation_count as (
      select substring(geval_addtime, 1, 10) dt, e.goods_id sku_id,
             sum(if(geval_scores_goods >= 9, 1, 0)) evaluation_good_count,
             sum(if(geval_scores_goods >6 and geval_scores_goods < 9, 1, 0)) evaluation_mid_count,
             sum(if(geval_scores_goods <= 6, 1, 0)) evaluation_bad_count
      from yp_dwd.fact_goods_evaluation_detail e
      group by substring(geval_addtime, 1, 10), e.goods_id
  ),
  ```

---

#### 知识点07：Union ALL与Full join合并的区别

> 经过前面的计算，并且通过CTE的引导，得到了6个结果表，分别是：
>
> 1、==order_count==  下单次数、件数、金额统计
>
> 2、==payment_count==  支付次数、件数、金额统计
>
> 3、==refund_count==  退款次数、件数、金额统计
>
> 4、==cart_count==  购物车次数、件数
>
> 5、==favor_count== 收藏次数
>
> 6、==evaluation_count== 好中差评数

- 需求：把上述6张表的结果合并在一起

  - 方式1：union all合并
  - 方式2：full outer join合并

  > 注意：上述两种合并有什么区别？？？**谁是行合并**？**谁是列合并**？

----

**斌哥小课堂:**

已知表t1内容如下:

```txt
001	 allen
002  james
```

已知表t2内容如下:

```txt
003  男
004  女
```

要求最终查询结果如下:

![image-20230220114426428](Day07_DWS层建设实战-2.assets/image-20230220114426428.png)

- 建表

  ```sql
  -- 使用测试库
  use test;
  -- 建表t1
  create table test.t1(
      id int,
      name string
  )row format delimited
  fields terminated by '\t';
  -- 插入数据
  insert into test.t1 values(001,'allen'),(002,'james');
  -- 建表t2
  create table test.t2(
      id int,
      sex string
  )row format delimited
  fields terminated by '\t';
  -- 插入数据
  insert into test.t2 values(003,'男'),(004,'女');
  
  -- 把t1和t2数据合并成一个结果集
  -- 方式1: union all
  -- 注意: 字段个数,顺序,类型需要对应,手动补充缺失的字段名和字段值
  select id,name,null sex from test.t1
  union all
  select id,null name,sex from test.t2;
  
  -- 方式2: full outer join
  -- 注意: 如果关联同一个字段没有相同值,会以两列形式展示,缺失的字段值会自动补充null
  -- 如果想要让两个表同一类字段值在同一列展示,需要加if判断转换
  select if(t1.id is not null,t1.id,t2.id),name,sex
  from test.t1
      full join test.t2
          on t1.id = t2.id;
  ```
  
- **==方式1：union all 行合并==**

  >1):union 通常用于多个结果集**行合并**！
  >2):Union 可以细分为**Union all不去重和Union distinct去重**;
  >3):Union 合并时对于**字段个数、顺序、类型有要求**;
  >4):如果想使用union既行合并又进行列合并，缺失的字段内容应该**手动使用0或者null补足**。
  
  ```sql
  -- sql如下：
  select id, name, null as sex from t1
  union all
  select id, null as name, sex from t2;
  ```
  
- **==方式2：full join 列合并==**

  >1):full join  通常用于多个结果集**列合并**！
  >
  >2):full join  使用时需要配合on关键字,在其后添加**两个表关联的条件**
  >
  >3):full join  合并时对于**字段个数、顺序、类型没有有要求**
  >
  >3):如果想使用full join既行合并又进行列合并,缺失的字段内容应该**自动使用0或者null补足**。
  
  ```sql
  -- sql如下
  select if(t1.id is not null,t1.id,t2.id),name,sex from t1
  full outer join
  t2 on t1.id = t2.id;
  ```

----

+ 这里采用union all的方式合并结果集

> 课程资料中提供了full join的方式合并结果集，可以对比理解。

```sql
-- 合并结果集
unionall as (
    select
        dt, sku_id, sku_name,
        order_count,
        order_num,
        order_amount,
        0 as payment_count,
        0 as payment_num,
        0 as payment_amount,
        0 as refund_count,
        0 as refund_num,
        0 as refund_amount,
        0 as cart_count,
        0 as cart_num,
        0 as favor_count,
        0 as evaluation_good_count,
        0 as evaluation_mid_count,
        0 as evaluation_bad_count
    from order_count
    
    union all
    
    select
        dt, sku_id, sku_name,
        0 order_count,
        0 order_num,
        0 order_amount,
        payment_count,
        payment_num,
        payment_amount,
        0 as refund_count,
        0 as refund_num,
        0 as refund_amount,
        0 as cart_count,
        0 as cart_num,
        0 as favor_count,
        0 as evaluation_good_count,
        0 as evaluation_mid_count,
        0 as evaluation_bad_count
    from payment_count
    
    union all
    
    select
        dt, sku_id, sku_name,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 as payment_count,
        0 as payment_num,
        0 as payment_amount,
        refund_count,
        refund_num,
        refund_amount,
        0 as cart_count,
        0 as cart_num,
        0 as favor_count,
        0 as evaluation_good_count,
        0 as evaluation_mid_count,
        0 as evaluation_bad_count
    from refund_count
    
    union all
    
    select
        dt, sku_id, null as sku_name,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 as payment_count,
        0 as payment_num,
        0 as payment_amount,
        0 as refund_count,
        0 as refund_num,
        0 as refund_amount,
        cart_count,
        cart_num,
        0 as favor_count,
        0 as evaluation_good_count,
        0 as evaluation_mid_count,
        0 as evaluation_bad_count
    from cart_count
    
    union all
    
    select
        dt, sku_id, null as sku_name,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 as payment_count,
        0 as payment_num,
        0 as payment_amount,
        0 as refund_count,
        0 as refund_num,
        0 as refund_amount,
        0 as cart_count,
        0 as cart_num,
        favor_count,
        0 as evaluation_good_count,
        0 as evaluation_mid_count,
        0 as evaluation_bad_count
    from favor_count
    
    union all
    
    select
        dt, sku_id, null as sku_name,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 as payment_count,
        0 as payment_num,
        0 as payment_amount,
        0 as refund_count,
        0 as refund_num,
        0 as refund_amount,
        0 as cart_count,
        0 as cart_num,
        0 as favor_count,
        evaluation_good_count,
        evaluation_mid_count,
        evaluation_bad_count
    from evaluation_count
)
```

------



#### 知识点08：DWS层搭建--商品主题宽表--最终完整版sql实现

- 最终对union all的结果集**unionall根据日期和商品进行分组 sum求和**，结果插入目标表中。

  ```sql
  select
      dt, sku_id, max(sku_name),
      sum(order_count),
      sum(order_num),
      sum(order_amount),
      sum(payment_count),
      sum(payment_num),
      sum(payment_amount),
      sum(refund_count),
      sum(refund_num),
      sum(refund_amount),
      sum(cart_count),
      sum(cart_num),
      sum(favor_count),
      sum(evaluation_good_count),
      sum(evaluation_mid_count),
      sum(evaluation_bad_count)
  from unionall
  group by dt, sku_id
  ;
  ```
  
- 最终完整版sql

  ```sql
  insert into hive.yp_dws.dws_sku_daycount
  --订单明细表抽取字段，并且进行去重，作为后续的base基础数据
  with order_base as (select
      dt,
      order_id, --订单id
      goods_id, --商品id
      goods_name,--商品名称
      buy_num,--购买商品数量
      total_price,--商品总金额（数量*单价）
      is_pay,--支付状态（1表示已经支付）
      row_number() over(partition by order_id,goods_id) as rn
  from yp_dwb.dwb_order_detail),
  --下单次数、件数、金额统计
  order_count as (select
      dt,goods_id as sku_id,goods_name as sku_name,
      count(order_id) order_count,
      sum(buy_num) order_num,
      sum(total_price) order_amount
  from order_base where rn =1
  group by dt,goods_id,goods_name),
  
  
  
  --订单状态,已支付
  pay_base as(
      select *,
      row_number() over(partition by order_id, goods_id) rn
      from yp_dwb.dwb_order_detail
      where is_pay=1
  ),
  --支付次数、件数、金额统计
  payment_count as(
      select dt, goods_id sku_id, goods_name sku_name,
         count(order_id) payment_count,
         sum(buy_num) payment_num,
         sum(total_price) payment_amount
      from pay_base
      where rn=1
      group by dt, goods_id, goods_name
  ),
  
  
  --退款次数、件数、金额统计
  refund_base as(
      select *,
         row_number() over(partition by order_id, goods_id) rn
      from yp_dwb.dwb_order_detail
      where refund_id is not null
  ),
  -- 退款次数、件数、金额
  refund_count as (
      select dt, goods_id sku_id, goods_name sku_name,
         count(order_id) refund_count,
         sum(buy_num) refund_num,
         sum(total_price) refund_amount
      from refund_base
      where rn=1
      group by dt, goods_id, goods_name
  ),
  
  
  -- 购物车次数、件数
  cart_count as (
      select substring(create_time, 1, 10) dt, goods_id sku_id,
             count(id) cart_count,
              sum(buy_num) cart_num
      from yp_dwd.fact_shop_cart
      where end_date = '9999-99-99'
      group by substring(create_time, 1, 10), goods_id
  ),
  
  -- 收藏次数
  favor_count as (
      select substring(c.create_time, 1, 10) dt, goods_id sku_id,
             count(c.id) favor_count
      from yp_dwd.fact_goods_collect c
      where end_date='9999-99-99'
      group by substring(c.create_time, 1, 10), goods_id
  ),
  
  -- 好评、中评、差评数量
  evaluation_count as (
      select substring(geval_addtime, 1, 10) dt, e.goods_id sku_id,
             count(if(geval_scores_goods >= 9, 1, null)) evaluation_good_count,
             count(if(geval_scores_goods >6 and geval_scores_goods < 9, 1, null)) evaluation_mid_count,
             count(if(geval_scores_goods <= 6, 1, null)) evaluation_bad_count
      from yp_dwd.fact_goods_evaluation_detail e
      group by substring(geval_addtime, 1, 10), e.goods_id
  ),
  
  --合并结果集
  unionall as (
      select
          dt, sku_id, sku_name,
          order_count,
          order_num,
          order_amount,
          0 as payment_count,
          0 as payment_num,
          0 as payment_amount,
          0 as refund_count,
          0 as refund_num,
          0 as refund_amount,
          0 as cart_count,
          0 as cart_num,
          0 as favor_count,
          0 as evaluation_good_count,
          0 as evaluation_mid_count,
          0 as evaluation_bad_count
      from order_count
      union all
      select
          dt, sku_id, sku_name,
          0 order_count,
          0 order_num,
          0 order_amount,
          payment_count,
          payment_num,
          payment_amount,
          0 as refund_count,
          0 as refund_num,
          0 as refund_amount,
          0 as cart_count,
          0 as cart_num,
          0 as favor_count,
          0 as evaluation_good_count,
          0 as evaluation_mid_count,
          0 as evaluation_bad_count
      from payment_count
      union all
      select
          dt, sku_id, sku_name,
          0 order_count,
          0 order_num,
          0 order_amount,
          0 as payment_count,
          0 as payment_num,
          0 as payment_amount,
          refund_count,
          refund_num,
          refund_amount,
          0 as cart_count,
          0 as cart_num,
          0 as favor_count,
          0 as evaluation_good_count,
          0 as evaluation_mid_count,
          0 as evaluation_bad_count
      from refund_count
      union all
      select
          dt, sku_id, null as sku_name,
          0 order_count,
          0 order_num,
          0 order_amount,
          0 as payment_count,
          0 as payment_num,
          0 as payment_amount,
          0 as refund_count,
          0 as refund_num,
          0 as refund_amount,
          cart_count,
          cart_num,
          0 as favor_count,
          0 as evaluation_good_count,
          0 as evaluation_mid_count,
          0 as evaluation_bad_count
      from cart_count
      union all
      select
          dt, sku_id, null as sku_name,
          0 order_count,
          0 order_num,
          0 order_amount,
          0 as payment_count,
          0 as payment_num,
          0 as payment_amount,
          0 as refund_count,
          0 as refund_num,
          0 as refund_amount,
          0 as cart_count,
          0 as cart_num,
          favor_count,
          0 as evaluation_good_count,
          0 as evaluation_mid_count,
          0 as evaluation_bad_count
      from favor_count
      union all
      select
          dt, sku_id, null as sku_name,
          0 order_count,
          0 order_num,
          0 order_amount,
          0 as payment_count,
          0 as payment_num,
          0 as payment_amount,
          0 as refund_count,
          0 as refund_num,
          0 as refund_amount,
          0 as cart_count,
          0 as cart_num,
          0 as favor_count,
          evaluation_good_count,
          evaluation_mid_count,
          evaluation_bad_count
      from evaluation_count
  )
  
  select
      dt, sku_id, max(sku_name),
      sum(order_count),
      sum(order_num),
      sum(order_amount),
      sum(payment_count),
      sum(payment_num),
      sum(payment_amount),
      sum(refund_count),
      sum(refund_num),
      sum(refund_amount),
      sum(cart_count),
      sum(cart_num),
      sum(favor_count),
      sum(evaluation_good_count),
      sum(evaluation_mid_count),
      sum(evaluation_bad_count)
  from unionall
  group by dt, sku_id
  --order by dt, sku_id
  ;
  ```

-----

#### 知识点09：DWS层搭建--用户主题宽表--需求分析

- 主题需求

  - 指标

    ```properties
    登录次数、收藏店铺数、收藏商品数、加入购物车次数、加入购物车金额、下单次数、下单金额、支付次数、支付金额
    ```

  - 维度

    ```properties
    用户、日期
    ```

- 建表操作

  > 注意：建表操作需要在hive中执行，presto不支持hive的建表语法。

  ```sql
  create table yp_dws.dws_user_daycount
  (
      --dt STRING,
      user_id string comment '用户 id',
      login_count bigint comment '登录次数',
      store_collect_count bigint comment '店铺收藏数量',
      goods_collect_count bigint comment '商品收藏数量',
      cart_count bigint comment '加入购物车次数',
      cart_amount decimal(38,2) comment '加入购物车金额',
      order_count bigint comment '下单次数',
      order_amount    decimal(38,2)  comment '下单金额',
      payment_count   bigint      comment '支付次数',
      payment_amount  decimal(38,2) comment '支付金额'
  ) COMMENT '每日用户行为'
  PARTITIONED BY(dt STRING)
  ROW format delimited fields terminated BY '\t'
  stored AS orc tblproperties ('orc.compress' = 'SNAPPY');
  ```

- 确定字段与表关系

  ```shell
  #登录次数
  yp_dwd.fact_user_login
  	id
  	login_user
  
  #收藏店铺数、收藏商品数
  yp_dwd.fact_store_collect
  	id
  	user_id
  	
  #加入购物车次数、加入购物车金额
  yp_dwd.fact_shop_cart
  yp_dwb.dwb_goods_detail
  	#因为购物车中没有金额，因此需要和商品详情表进行关联
  	goods_promotion_price: 商品促销价格(售价)
  	buy_num(本次加入购物车数量)
  
  #下单次数、下单金额
  yp_dwd.fact_shop_order
  yp_dwd.fact_shop_order_address_detail
  	#通过订单主副表可以提供
  
  #支付次数、支付金额
  yp_dwd.fact_trade_record
  ```

- 最终实现合并的方式

  - 使用union all合并

> 用户主题的统计宽表，作为项目练习由大家自己完成。
>
> 所用到的技术点之前已经完成铺垫完毕。

------

#### 知识点10：扩展：Hive优化--Index索引问题

- 索引是什么

  - 在数据库中，索引指的是==提供指向存储在表的指定列中的数据值的指针（地址）==，数据库使用索引以找到特定值，然后顺指针找到包含该值的行。
  - 索引的作用相当于==新华字典的目录==，可以根据目录中的页码==快速找到所需的内容==。比如我们可以按拼音、笔画、偏旁部首等排序的目录（索引）快速查找到需要的字。

- mysql中的索引

  > 作为OLTP系统软件的典型代表，mysql也支持索引的使用。

  - 索引的优点

    - ==大大提高查询的效率==

  - 索引的缺点

    - ==降低更新表的速度==，如对表进行INSERT、UPDATE和DELETE。因为更新表时，MySQL不仅要保存数据，还要保存一下索引文件。==MySQL支持自动更新索引==。
    - 建立索引会==占用磁盘空间==的索引文件。

  - 索引使用的注意事项

    - 当创建索引之后，根据具有索引的字段查询，效率才能体现。

  - mysql的索引分类

    - 普通索引

      > 普通索引是最基本的索引，它没有任何限制。

      ```sql
      CREATE INDEX indexName ON table_name (column_name);
      
      --也可以在建表的时候同时指定索引
      CREATE TABLE `table` (
          `id` int(11) NOT NULL AUTO_INCREMENT ,
          `title` char(255) CHARACTER NOT NULL ,
          `content` text CHARACTER NULL ,
          `time` int(10) NULL DEFAULT NULL ,
          PRIMARY KEY (`id`),
           
      )
      
      --length为可选参数，表示索引的长度，只有字符串类型的字段才能指定索引长度
      ```

    - 唯一索引

      > 与普通索引类似，不同的就是：==索引列的值必须唯一==，但允许有空值。

      ```sql
      CREATE UNIQUE INDEX indexName ON table(column(length));
      
      --建表的时候创建索引
      CREATE TABLE `table` (
          `id` int(11) NOT NULL AUTO_INCREMENT ,
          `title` char(255) CHARACTER NOT NULL UNIQUE,
          `content` text CHARACTER NULL ,
          `time` int(10) NULL DEFAULT NULL ,
          UNIQUE indexName (title(length))
      );
      ```

    - 主键索引

      > 一种特殊的唯一索引，一个表只能有一个主键，不允许有空值。
      >
      > 一般是在建表的时候同时创建主键索引。

      ```sql
      CREATE TABLE `table` (
          `id` int(11) NOT NULL AUTO_INCREMENT ,
          `title` char(255) NOT NULL ,
          PRIMARY KEY (`id`)
      );
      ```

    - 组合索引

      > 指多个字段上创建的索引，只有在查询条件中使用了创建索引时的第一个字段，索引才会被使用。
      >
      > 使用组合索引时==**遵循最左前缀**==集合。

      ```sql
      ALTER TABLE mytable ADD INDEX name_city_age (name,city,age);
      
      --建表时，name长度为16，这里用10。这是因为一般情况下名字的长度不会超过10，这样会加速索引查询速度，还会减少索引文件的大小，提高INSERT的更新速度。
      
      --建立这样的组合索引，其实是相当于分别建立了下面三组组合MySQL数据库索引
      name,city,age
      name,city
      name 
      
      --为什么没有 city，age这样的组合索引呢？这是因为MySQL组合索引“最左前缀”的结果。简单的理解就是只从最左面的开始组合。并不是只要包含这三列的查询都会用到该组合索引，下面的几个SQL就会用到这个组合MySQL数据库索引：
      
      SELECT * FROM mytable WHERE name="admin" AND city="邯郸"; -- 组合索引会生效
      SELECT * FROM mytable WHERE name="admin"; -- 组合索引会生效
      
      --而下面几个则不会用到：
      SELECT * FROM mytable WHERE city="邯郸" AND name="admin";  -- 组合索引不会生效
      SELECT * FROM mytable WHERE age=20 AND city="邯郸";-- 组合索引不会生效
      SELECT * FROM mytable WHERE city="邯郸";-- 组合索引不会生效
      ```
  
- Hive中的索引与问题

  > Hive作为OLAP软件，应该说是要支持索引的，因为这样可以提高查询效率。
  >
  > Hive 0.7版本之后，开始支持索引，但是功能很弱。

  ```properties
  1、在hive表指定列上建立索引，会产生一张索引表（Hive的一张物理表），里面的字段包括，索引列的值、该值对应的HDFS文件路径、该值在文件中的偏移量;
  
  2、hive的索引是需要手动进行维护的,索引表不会自动rebuild，如果表有数据新增或删除，那么必须手动rebuild索引表数据;
  
  在执行索引字段查询时候，首先额外生成一个MR job，根据对索引列的过滤条件，从索引表中过滤出索引列的值对应的hdfs文件路径及偏移量，输出到hdfs上的一个文件中。
  
  然后根据生成的临时文件中的hdfs路径和偏移量，筛选原始input文件，生成新的split,作为整个job的split,这样就达到不用全表扫描的目的。
  
  每次查询时候都要先用一个job扫描索引表，如果索引列的值非常稀疏，那么索引表本身也会非常大；
  
  ```

> 因此在==**Hive3.0版本之后，直接移除了索引**==，即使使用hive低版本的，也不建议使用索引。

![image-20211015185907147](Day07_DWS层建设实战-2.assets/image-20211015185907147.png)

- Hive给的建议是：

  > - 使用物化视图  自动更新
  > - ==建议使用ORC、Parquet等格式文件==。**这些文件格式本身列式存储，内部特性支持查询效率的提高。**

  ![image-20211015210144113](Day07_DWS层建设实战-2.assets/image-20211015210144113.png)

----

#### 知识点11：扩展：Hive优化--ORC文件索引--Row Group Index

- ORC文件存储格式

  ![ORC file format | CDP Public Cloud](Day07_DWS层建设实战-2.assets/hive_orc_file_structure.png)

  ```properties
  一个ORC文件包含一个或多个stripes(groups of row data)；
  
  每个stripe中包含了每个column的min/max值的索引数据；
  
  当查询中有<,>,=的操作时，会根据min/max值，跳过扫描不包含的stripes。
  ```

- Row Group Index行组索引

  > ORC为每个stripe建立的包含min/max值的索引，就称为Row Group Index，也叫min-max Index，或者Storage Index。

  ```properties
  1、在建立ORC格式表时，指定表参数’orc.create.index’=’true’之后，便会建立Row Group Index；
  
  2、需要注意的是，为了使Row Group Index有效利用，向表中加载数据时，必须对需要使用索引的字段进行排序，否则，min/max会失去意义。
  
  3、另外，这种索引通常用于数值型字段的查询过滤优化上。
  ```

  ```sql
  --CTAS
  CREATE TABLE t_text_2 stored AS ORC 
  TBLPROPERTIES
      ('orc.compress'='SNAPPY',
      'orc.create.index'='true', --开启行组索引
      'orc.stripe.size'='10485760',
      'orc.row.index.stride'='10000') 	--索引条目之间的行数（必须> = 1000）
  AS 
  SELECT xxxx
  FROM t_text_1 
  DISTRIBUTE BY id sort BY id;
  
  --参数hive.optimize.index.filte 表示是否自动使用索引，默认为false（不使用）；
  --如果不设置该参数为true，那么ORC的索引当然也不会使用。
  
  --执行sql之前，为了使用索引，应该设置下面的参数
  set hive.optimize.index.filter=true;
  ```
------

####  知识点12：扩展：Hive优化--ORC文件索引--Bloom Filter Index

- 布隆过滤器

  - 概念

    > Bloom Filter是由Bloom在1970年提出的一种==多哈希函数映射的快速查找算法==。通常应用在一些需要==快速判断某个元素是否属于集合，但是并不严格==要求100%正确的场合。

  - 特点：**判断有不一定有，判断没有一定没有**。

  - 大致原理

    - 初始状态时，Bloom Filter是一个包含m位的==位数组(bit)==，每一位都置为0

      ![image-20211015211935483](Day07_DWS层建设实战-2.assets/image-20211015211935483.png)

    - 当来了一个元素 a，进行判断，使用两个哈希函数，计算出该元素对应的Hash值为1和5，然后到Bloom Filter中判断第1位和第5位的值，上面全部为1，因此a不在Bloom Filter内，将 a 添加进去:

      ![image-20211015211959613](Day07_DWS层建设实战-2.assets/image-20211015211959613.png)

    - 添加a之后，BloomFilter的第1位和第5位的值为1，之后的元素，要判断是不是在Bloom Filter内，也是同a一样的方法，只有对元素哈希后对应位置上都是 1 才认为这个元素在集合内（虽然这样可能会误判）

      ![image-20211015212019020](Day07_DWS层建设实战-2.assets/image-20211015212019020.png)

    - 随着元素的插入，Bloom filter 中修改的值变多，出现误判的几率也随之变大，当新来一个元素时，满足其在Bloom Filter内的条件，即所有对应位都是 1 ，这样就可能有两种情况，一是这个元素就在集合内，没有发生误判；还有一种情况就是发生误判，出现了==哈希碰撞==，这个元素本不在集合内。

      ![image-20211015212034179](Day07_DWS层建设实战-2.assets/image-20211015212034179.png)

- hive中布隆过滤器索引

  > 在建表时候，通过表参数==orc.bloom.filter.columns=”pcid”==来指定为那些字段建立BloomFilter索引；
  >
  > 这样，在生成数据的时候，会在每个stripe中，为该字段建立BloomFilter的数据结构；
  >
  > 当查询条件中包含==对该字段的=号过滤时候，先从BloomFilter中获取以下是否包含该值==，如果不包含，则跳过该stripe。

  ```sql
  CREATE TABLE t_text_2 stored AS ORC 
  TBLPROPERTIES
      ('orc.compress'='SNAPPY',
      'orc.create.index'='true',
      "orc.bloom.filter.columns"="pcid", --布隆过滤器 对pcid进行索引
      'orc.stripe.size'='10485760',
      'orc.row.index.stride'='10000') 
  AS 
  SELECT xxxx
  FROM t_text_1 
  DISTRIBUTE BY id sort BY id;
  
  
  -- 启用索引
  SET hive.optimize.index.filter=true;
  -- 当查询中有<,>,=的操作时，会根据min/max值，跳过扫描不包含的stripes。
  -- 执行查询
  SELECT COUNT(1) FROM t_text_2 
  WHERE id >= 1000 AND id <= 2000 --如果前面开启了索引,此处用到了行组索引
  AND pcid IN ('B','A');--如果前面开启了索引,此处用到了布隆过滤器索引
  ```

----

#### Hive相关配置参数

```shell
--分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.created.files=150000;
--hive压缩
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--写入时压缩生效
set hive.exec.orc.compression.strategy=COMPRESSION;
--分桶
set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;
set hive.optimize.bucketmapjoin = true;
set hive.auto.convert.sortmerge.join=true;
set hive.auto.convert.sortmerge.join.noconditionaltask=true;
--并行执行
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
--小文件合并
-- set mapred.max.split.size=2147483648;
-- set mapred.min.split.size.per.node=1000000000;
-- set mapred.min.split.size.per.rack=1000000000;
--矢量化查询
set hive.vectorized.execution.enabled=true;
--关联优化器
set hive.optimize.correlation=true;
--读取零拷贝
set hive.exec.orc.zerocopy=true;
--join数据倾斜
set hive.optimize.skewjoin=true;
-- set hive.skewjoin.key=100000;
set hive.optimize.skewjoin.compiletime=true;
set hive.optimize.union.remove=true;
-- group倾斜
set hive.groupby.skewindata=false;
```



























