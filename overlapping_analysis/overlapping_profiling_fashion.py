# Databricks notebook source
# MAGIC %run /utils/spark_utils

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Overlapping Customer List

# COMMAND ----------

import os
import pandas as pd
project_dir="/mnt/dev/overlapping_analysis/fashion" # developing environment

sales_tables=['imx_sales_scope',
              "lc_sales_scope"
]
demo_tables=["imx_demo_active",
             "lc_demo_active"
]
matched_tables=[
             "lc_imx_customer"
            ]

for t in sales_tables+demo_tables+matched_tables:
    load_parquet_to_temp_table(os.path.join(project_dir,t),t )

## select brand overlapping customers
lc_imx_customer_brand = spark.sql(
    "select distinct A.* from lc_imx_customer A \
        inner join  imx_sales_scope B on A.imx_key = B.vip_main_no \
        where brand_code in ('SC') \
    ")
lc_imx_customer_brand.createOrReplaceTempView("lc_imx_customer_brand")

# brand code: IZ isabel marant; SC sacai; CM club monaco
# buy brand cm in imx_fasion, and buy lc
imx_sales_scope_labeled = spark.sql(
    "select A.* , \
    case when B.lc_key is not null then 1 else 0 end as LC \
    from imx_sales_scope A    \
    left join lc_imx_customer_brand B on A.vip_main_no = B.imx_key \
    where brand_code in ('SC')\
    "
    )


# label lc table, who buy selected brand in IMX
lc_sales_scope_labeled = spark.sql(
    'select A.*,  \
    case when B.imx_key is not null then 1 else 0 end as IMX \
    from lc_sales_scope A  \
    left join lc_imx_customer_brand B on A.vip_no = B.lc_key \
    '
    )

imx_demo_active_labeled = spark.sql(
    'select A.*,  \
    case when B.lc_key is not null then 1 else 0 end as LC \
    from imx_demo_active A    \
    left join lc_imx_customer_brand B on A.vip_main_no = B.imx_key \
    '
    )

lc_demo_active_labeled = spark.sql(
    'select A.*,  \
    case when B.imx_key is not null then 1 else 0 end as IMX \
    from lc_demo_active A  \
    left join lc_imx_customer_brand B on A.vip_no = B.lc_key \
    ')
imx_sales_scope_labeled.createOrReplaceTempView("imx_sales_scope_labeled")
lc_sales_scope_labeled.createOrReplaceTempView("lc_sales_scope_labeled")
imx_demo_active_labeled.createOrReplaceTempView("imx_demo_active_labeled")
lc_demo_active_labeled.createOrReplaceTempView("lc_demo_active_labeled")


from pyspark.sql import functions as F
lc_label = "LC"
imx_label="IMX"

lc_sales_scope_labeled_fashion=lc_sales_scope_labeled.filter(F.col("bu_desc").isin(["WW","MW","LSA","MSA"]))
lc_sales_scope_labeled_nonfashion=lc_sales_scope_labeled.filter(~F.col("bu_desc").isin(["WW","MW","LSA","MSA"]))


# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select COUNT(distinct vip_main_no) from imx_sales_scope_labeled

# COMMAND ----------

# MAGIC %sql
# MAGIC select COUNT(distinct vip_no) from LC_sales_scope_labeled

# COMMAND ----------

# %sql
# select distinct  B.brand_code,brand_desc from imx_sales_scope

# COMMAND ----------

# %sql
# select * from imx_sales_scope
# where brand_desc="Isabel Marant"

# COMMAND ----------

# %sql
# select count(distinct vip_no) from imx_sales_scope
# where lower(brand_desc) like "%isabel%"

# COMMAND ----------

# %sql
# select * from lc_imx_customer_brand

# COMMAND ----------

# MAGIC %md
# MAGIC prod_brand	brand_desc
# MAGIC
# MAGIC SB    	Sacai Consignment
# MAGIC
# MAGIC IZ    	Isabel Marant
# MAGIC
# MAGIC PC    	Paul Smith Con
# MAGIC
# MAGIC SC    	Sacai
# MAGIC
# MAGIC CM    	Club Monaco
# MAGIC
# MAGIC IM    	Isabel Marant
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the # overlapping customers
# MAGIC select count(distinct lc_key),count(distinct imx_key) from lc_imx_customer_brand

# COMMAND ----------

# MAGIC %md
# MAGIC # Top 10 LC subclass in HL/COS for overlapping customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC distinct 
# MAGIC Concat(S.class_desc,"-",S.subclass_desc) as class_subclass,
# MAGIC count(distinct B.vip_no)
# MAGIC from lc_sales_scope_labeled A
# MAGIC right join  (select distinct vip_no from lc_demo_active_labeled where IMX=1) B using(vip_no)
# MAGIC left join lc_prd.crm_db_neo_silver.dbo_v_sales_dtl S using (order_no)
# MAGIC where A.bu_desc in ("COS")
# MAGIC and lower(A.class_desc) not like "%concession%"
# MAGIC group by 1
# MAGIC order by 2 desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC distinct 
# MAGIC Concat(S.class_desc,"-",S.subclass_desc) as class_subclass,
# MAGIC count(distinct B.vip_no)
# MAGIC from lc_sales_scope_labeled A
# MAGIC right join  (select distinct vip_no from lc_demo_active_labeled where IMX=1) B using(vip_no)
# MAGIC left join lc_prd.crm_db_neo_silver.dbo_v_sales_dtl S using (order_no)
# MAGIC where A.bu_desc in ("HL")
# MAGIC and lower(A.class_desc) not like "%concession%"
# MAGIC group by 1
# MAGIC order by 2 desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC # IMX SACAI & LC OTHER BRAND Customer

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view lc_cust_no_sacai as 
# MAGIC select distinct lc_key as vip_no from lc_imx_customer_brand -- LC customer who buy SACAI in IMX
# MAGIC except
# MAGIC select distinct vip_no from lc_sales_scope_labeled where lower(brand_desc) like "%sacai%" -- LC customer who not buy SACAI in LC

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC Concat(S.class_desc,"-",S.subclass_desc) as class_subclass,
# MAGIC count(distinct S.vip_no)
# MAGIC from lc_sales_scope_labeled 
# MAGIC right join lc_cust_no_sacai using (vip_no)
# MAGIC left join lc_prd.crm_db_neo_silver.dbo_v_sales_dtl S using (order_no)
# MAGIC where S.bu_desc !="COS"
# MAGIC group by 1
# MAGIC order by 2 desc
# MAGIC limit 11

# COMMAND ----------

n_unique_cust={'lc_imx':1283}

# COMMAND ----------

imx_sales_scope_labeled.columns

# COMMAND ----------

lc_sales_scope_labeled.columns

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Metrics Calculation

# COMMAND ----------




# total number of customer
def n_customer(prefix1,prefix2):
    key =f"{prefix1.lower()}_{prefix2.lower()}"
    return n_unique_cust[key]


# average order value
def aov(sales_scope_labeled,overlap_label,sales_amt_col,order_no_col):
    # select overlapping customers
    result = sales_scope_labeled.filter(sales_scope_labeled[overlap_label] == 1).agg(
    # calculate 
                (F.sum(sales_amt_col) / F.countDistinct(order_no_col)).alias("result") 
                )

    # Retrieve the result
    result_value = result.first()[0]
    return result_value


# average unit per transaction
def upt(sales_scope_labeled,overlap_label,sales_qty_col,order_no_col):
    # select overlapping customers
    result = sales_scope_labeled.filter(sales_scope_labeled[overlap_label] == 1) .agg(
    # calculate 
                (F.sum(sales_qty_col) / F.countDistinct(order_no_col)).alias("result") # calculate 
                )

    # Retrieve the result
    result_value = result.first()[0]
    return result_value

# average # of transaction per customers
def avg_trans_per_cust(sales_scope_labeled,overlap_label,vip_col,order_no_col,prefix1,prefix2):
    n=n_unique_cust [f'{prefix1}_{prefix2}']
    # select overlapping customers
    result = sales_scope_labeled.filter(sales_scope_labeled[overlap_label] == 1) .agg(
    # calculate 
                (F.countDistinct(order_no_col) / n ).alias("result") # calculate 
                )

    # Retrieve the result
    result_value = result.first()[0]
    return result_value

# average $ spending per customers
def avg_spend_per_cust(sales_scope_labeled,overlap_label,vip_col,sales_amt_col,prefix1,prefix2):
    n=n_unique_cust [f'{prefix1}_{prefix2}']
    # select overlapping customers
    result = sales_scope_labeled.filter(sales_scope_labeled[overlap_label] == 1) .agg(
    # calculate 
                (F.sum(sales_amt_col) / n).alias("result") # calculate 
                )

    # Retrieve the result
    result_value = result.first()[0]
    return result_value

# average price point
def avg_price_point(sales_scope_labeled,overlap_label,sales_amt_col,sales_qty_col):
    # select overlapping customers
    result = sales_scope_labeled.filter(sales_scope_labeled[overlap_label] == 1) .agg(
    # calculate 
                (F.sum(sales_amt_col) / F.sum(sales_qty_col)).alias("result") # calculate 
                )
    # Retrieve the result
    result_value = result.first()[0]
    return result_value



# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Table Summarize

# COMMAND ----------



def sales_behavior_table (sales_scope_labeled1, sales_scope_labeled2,overlap_label1,overlap_label2,vip_col1,vip_col2,sales_amt_col1,sales_amt_col2,order_no_col1,order_no_col2,qty_col1,qty_col2,prefix1,prefix2):
    index = ['n_customers','aov','upt','avg # trans per cust','avg spend per cust','avg price point']
    columns=[overlap_label1,overlap_label2]
    table= pd.DataFrame(columns=columns,index=index)
    # overlap_label1
    table.loc['n_customers',overlap_label1] = n_customer(prefix1=prefix1,prefix2=prefix2)

    table.loc['aov',overlap_label1] = aov(sales_scope_labeled=sales_scope_labeled1,overlap_label=overlap_label2,sales_amt_col=sales_amt_col1,order_no_col=order_no_col1)

    table.loc['upt',overlap_label1] = upt(sales_scope_labeled=sales_scope_labeled1,overlap_label=overlap_label2,sales_qty_col=qty_col1,order_no_col=order_no_col1)

    table.loc['avg # trans per cust',overlap_label1] = avg_trans_per_cust(sales_scope_labeled=sales_scope_labeled1,overlap_label=overlap_label2,vip_col=vip_col1,order_no_col=order_no_col1,prefix1=prefix1,prefix2=prefix2)

    table.loc['avg spend per cust',overlap_label1] = avg_spend_per_cust(sales_scope_labeled=sales_scope_labeled1,overlap_label=overlap_label2,vip_col=vip_col1,sales_amt_col=sales_amt_col1,prefix1=prefix1,prefix2=prefix2)

    table.loc['avg price point',overlap_label1]=avg_price_point(sales_scope_labeled=sales_scope_labeled1,overlap_label=overlap_label2,sales_amt_col=sales_amt_col1,sales_qty_col=qty_col1)
    #  overlap_label2

    table.loc['n_customers',overlap_label2] = n_customer(prefix1=overlap_label1,prefix2=overlap_label2)

    table.loc['aov',overlap_label2] = aov(sales_scope_labeled=sales_scope_labeled2,overlap_label=overlap_label1,sales_amt_col=sales_amt_col2,order_no_col=order_no_col2)

    table.loc['upt',overlap_label2] = upt(sales_scope_labeled=sales_scope_labeled2,overlap_label=overlap_label1,sales_qty_col=qty_col2,order_no_col=order_no_col2)

    table.loc['avg # trans per cust',overlap_label2] = avg_trans_per_cust(sales_scope_labeled=sales_scope_labeled2,overlap_label=overlap_label1,vip_col=vip_col2,order_no_col=order_no_col2,prefix1=prefix1,prefix2=prefix2)

    table.loc['avg spend per cust',overlap_label2] = avg_spend_per_cust(sales_scope_labeled=sales_scope_labeled2,overlap_label=overlap_label1,vip_col=vip_col2,sales_amt_col=sales_amt_col2,prefix1=prefix1,prefix2=prefix2)

    table.loc['avg price point',overlap_label2]=avg_price_point(sales_scope_labeled=sales_scope_labeled2,overlap_label=overlap_label1,sales_amt_col=sales_amt_col2,sales_qty_col=qty_col2)

    return table

# COMMAND ----------

lc_imx_sales = sales_behavior_table (sales_scope_labeled1=lc_sales_scope_labeled_fashion, 
                      sales_scope_labeled2=imx_sales_scope_labeled,
                      overlap_label1=lc_label,
                      overlap_label2=imx_label,
                      vip_col1='vip_no',
                      vip_col2='vip_main_no',
                      sales_amt_col1='amt_hkd',
                      sales_amt_col2='net_amt_HKD',
                      order_no_col1='order_no',
                      order_no_col2='sales_main_key',
                      qty_col1='dtl_qty',
                      qty_col2='sold_qty',
                      prefix1=lc_label.lower(),
                      prefix2=imx_label.lower())
lc_imx_sales

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Table Concatenated

# COMMAND ----------

# sales_concatenated = pd.concat([lc_imx_sales, lccos_imx_sales,lc_jb_sales,lccos_jb_sales,imx_jb_sales], keys=['LC-IMX', 'LCCOS - IMX','LC-JB','LCCOS-JB','IMX-JB'], axis=1)
# sales_concatenated

# COMMAND ----------

# MAGIC %md
# MAGIC # Demographic: Penetration and Tenure

# COMMAND ----------

# MAGIC %md
# MAGIC # Share of Wallet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summarize

# COMMAND ----------


def sw_table(sales_scope_labeled1,
             sales_scope_labeled2,
             overlap_label1,
             overlap_label2,
             sales_amt_col1,
             sales_amt_col2
             ):
    columns=[overlap_label1,overlap_label2]
    index=["$ Sales", "% of Share"] 
    table = pd.DataFrame(index=index,columns=columns)
    comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).agg(F.sum(sales_amt_col1)).first()[0]
    comp2_sales = sales_scope_labeled2.filter(sales_scope_labeled2[overlap_label1] == 1).agg(F.sum(sales_amt_col2)).first()[0]
    comp1_share = comp1_sales / (comp1_sales+comp2_sales)
    comp2_share = comp2_sales / (comp1_sales+comp2_sales)
    table.loc['$ Sales',overlap_label1]=comp1_sales
    table.loc['$ Sales',overlap_label2]=comp2_sales
    table.loc['% of Share',overlap_label1]=str(round(comp1_share*100,2))+"%"
    table.loc['% of Share',overlap_label2]=str(round(comp2_share*100,2))+"%"
    return table

# COMMAND ----------

lc_imx_sw=sw_table(sales_scope_labeled1=lc_sales_scope_labeled,
             sales_scope_labeled2=imx_sales_scope_labeled,
             overlap_label1=lc_label,
             overlap_label2=imx_label,
             sales_amt_col1='amt_hkd',
             sales_amt_col2='net_amt_hkd'
             )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concatenated

# COMMAND ----------

# sw_concatenated = pd.concat([lc_imx_sw, lccos_imx_sw,lc_jb_sw,lccos_jb_sw,imx_jb_sw], keys=['LC-IMX', 'LCCOS - IMX','LC-JB','LCCOS-JB','IMX-JB'], axis=1)
# sw_concatenated
lc_imx_sw

# COMMAND ----------

# MAGIC %md
# MAGIC # Price Point by Category

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metrics Calculation

# COMMAND ----------

from pyspark.sql.functions import sum, col

def pricepoint_by_cat(sales_scope_labeled,overlap_label,sales_amt_col,qty_col,prefix):
    overlapped = sales_scope_labeled.filter(sales_scope_labeled[overlap_label] == 1)
    # Calculate the total number of unique vip_no
    result_df = overlapped.groupBy('arti_cate_name').agg(sum(sales_amt_col).alias('sum_sales'), sum(qty_col).alias('sum_qty'))
    result_df = result_df.withColumn(prefix, col('sum_sales') / col('sum_qty'))
    # Show the result DataFrame
    return result_df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Summarize

# COMMAND ----------

def pricepoint_table(
    sales_scope_labeled1,
    sales_scope_labeled2,
    overlap_label1,
    overlap_label2,
    sales_amt_col1,
    sales_amt_col2,
    qty_col1,
    qty_col2,
):
    company1 = pricepoint_by_cat(
        sales_scope_labeled=sales_scope_labeled1,
        overlap_label=overlap_label2,
        sales_amt_col=sales_amt_col1,
        qty_col=qty_col1,
        prefix=overlap_label1,
    )
    company2 = pricepoint_by_cat(
        sales_scope_labeled=sales_scope_labeled2,
        overlap_label=overlap_label1,
        sales_amt_col=sales_amt_col2,
        qty_col=qty_col2,
        prefix=overlap_label2,
    )
    table = (
        company1.join(company2, on="arti_cate_name", how="outer")
        .toPandas()[["arti_cate_name", overlap_label1, overlap_label2]]
        .set_index("arti_cate_name")
    )
    return table.round(2)

# COMMAND ----------

lc_imx_pp = pricepoint_table(
                     sales_scope_labeled1=lc_sales_scope_labeled,
                     sales_scope_labeled2=imx_sales_scope_labeled,
                     overlap_label1=lc_label,
                     overlap_label2=imx_label,
                     sales_amt_col1='amt_hkd',
                     sales_amt_col2='net_amt_hkd',
                     qty_col1='dtl_qty',
                     qty_col2='sold_qty')

# COMMAND ----------

lc_imx_pp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concatenated

# COMMAND ----------

# pp_concatenated = pd.concat([lc_imx_pp, lccos_imx_pp,lc_jb_pp,lccos_jb_pp,imx_jb_pp], keys=['LC-IMX', 'LCCOS - IMX','LC-JB','LCCOS-JB','IMX-JB'], axis=1)
# pp_concatenated

# COMMAND ----------

# lccos_imx_lccoscat = pricepoint_by_cat(sales_scope_labeled=lccos_sales_scope_labeled,overlap_label=imx_label,sales_col='amt_hkd',qty_col='dtl_qty',prefix=lccos_label)
# lccos_imx_imxcat = pricepoint_by_cat(sales_scope_labeled=imx_sales_scope_labeled,overlap_label=lccos_label,sales_col='net_amt_hkd',qty_col='sold_qty',prefix=imx_label)
# lccos_imx_cat = lccos_imx_lccoscat.join(lccos_imx_imxcat, on='arti_cate_name', how='outer').toPandas()[['arti_cate_name',lccos_label,imx_label]].set_index("arti_cate_name")
# lccos_imx_cat

# COMMAND ----------

# MAGIC %md
# MAGIC # Top Brand

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def get_topbrand(sales_scope_labeled,overlap_label,sales_col,brand_col,vip1):
    overlapped = sales_scope_labeled.filter(sales_scope_labeled[overlap_label] == 1)
# Calculate the sum of sales_col for each brand 
    brand_sales_df = overlapped.groupBy(brand_col).agg(F.countDistinct(vip1).alias('count_customer'))
    # Calculate the grand total
    grand_total = overlapped.agg(F.countDistinct(vip1).alias('total')).first()[0]

    # Calculate the percentage of grand total for each brand
    percentage_df = brand_sales_df.withColumn('percentage', (F.col('count_customer') / grand_total) * 100)

    # Order the result by percentage in descending order
    result_df = percentage_df.orderBy(F.desc('percentage'))
    return result_df.toPandas()
def get_topbrand_fashion(sales_scope_labeled,overlap_label,sales_col,brand_col,vip1):
    overlapped = sales_scope_labeled.filter(sales_scope_labeled[overlap_label] == 1).filter(sales_scope_labeled['bu_desc'].isin(['WW','MW','LSA','MSA']))
# Calculate the sum of sales_col for each brand 
    brand_sales_df = overlapped.groupBy(brand_col).agg(F.countDistinct(vip1).alias('count_customer'))
    # Calculate the grand total
    grand_total = overlapped.agg(F.countDistinct(vip1).alias('total')).first()[0]

    # Calculate the percentage of grand total for each brand
    percentage_df = brand_sales_df.withColumn('percentage', (F.col('count_customer') / grand_total) * 100)

    # Order the result by percentage in descending order
    result_df = percentage_df.orderBy(F.desc('percentage'))
    return result_df.toPandas()

# COMMAND ----------

# lc fashion brand
lc_imx_lcbrand=get_topbrand(sales_scope_labeled=lc_sales_scope_labeled_fashion,overlap_label=imx_label,sales_col='amt_hkd',brand_col='brand_desc',vip1='vip_no')
lc_imx_lcbrand.iloc[0:10,:]

# COMMAND ----------

# lc non fashion
lc_imx_lcbrand=get_topbrand(sales_scope_labeled=lc_sales_scope_labeled_nonfashion,overlap_label=imx_label,sales_col='amt_hkd',brand_col='brand_desc',vip1='vip_no')
lc_imx_lcbrand.iloc[0:10,:]

# COMMAND ----------

lc_imx_imxbrand=get_topbrand(sales_scope_labeled=imx_sales_scope_labeled,overlap_label=lc_label,sales_col='net_amt_hkd',brand_col='brand_desc',vip1='vip_main_no')
lc_imx_imxbrand

# COMMAND ----------

# MAGIC %md
# MAGIC # Tier Matrix

# COMMAND ----------

from pyspark.sql import functions as F

def tiermatrix(df1, df2, vip1, vip2, matched_df, prefix1, prefix2, tier1, tier2):
    joined_df = matched_df.join(df1, df1[vip1] == matched_df[f"{prefix1}_key"]).join(df2, df2[vip2] == matched_df[f"{prefix2}_key"])

    # Step 3: Calculate the count of overlapping customers for each combination of Tier1 and Tier2
    overlap_counts = joined_df.groupBy(df1[f"{tier1}"], df2[f"{tier2}"]).agg(F.count("*").alias("overlap_count"))

    # Step 4: Calculate the total count of customers for each Tier1 and Tier2 combination
    total_counts = joined_df.count()

    # Step 5: Calculate the percentage of overlapping customers for each combination
    overlap_percentage = overlap_counts.withColumn("pct", F.col("overlap_count") / total_counts)

    # Pivot the table to get the desired matrix format
    tier_matrix = overlap_percentage.groupBy(df1[f"{tier1}"]).pivot(f"{tier2}").agg(F.first("pct"))

    # Display the resulting tier matrix
    return tier_matrix.toPandas()

# COMMAND ----------

tiermatrix(lc_demo_active_labeled,imx_demo_active_labeled,"vip_no","vip_main_no",lc_imx_customer_brand,"lc","imx","CARD_TYPE_updated","viptyp_desc_unique")

# COMMAND ----------

# vip1="vip_main_no"
# vip2="vip_main_no"
# matched_df=imx_jb_customer
# prefix1="imx"
# prefix2="jb"
# tier1="viptyp_desc_unique"
# tier2="viptyp_desc_unique"
# df1=imx_demo_active_labeled.withColumn("imx_tier",imx_demo_active_labeled.viptyp_desc_unique)
# df2=jb_demo_active_labeled.withColumn("jb_tier",jb_demo_active_labeled.viptyp_desc_unique)

# joined_df = matched_df.join(df1, df1[vip1] == matched_df[f"{prefix1}_key"]).join(df2, df2[vip2] == matched_df[f"{prefix2}_key"])

# # Step 3: Calculate the count of overlapping customers for each combination of Tier1 and Tier2
# overlap_counts = joined_df.groupBy('imx_tier','jb_tier').agg(F.count("*").alias("overlap_count"))

# # Step 4: Calculate the total count of customers for each Tier1 and Tier2 combination
# total_counts = joined_df.count()

# # Step 5: Calculate the percentage of overlapping customers for each combination
# overlap_percentage = overlap_counts.withColumn("pct", F.col("overlap_count") / total_counts)

# # Pivot the table to get the desired matrix format
# from pyspark.sql import functions as F

# tier_matrix = overlap_percentage.groupBy('imx_tier') \
#     .pivot('jb_tier') \
#     .agg(F.first("pct"))

# # Display the resulting tier matrix
# tier_matrix.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC # Tenure Matrix

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime

def assign_tenure_group(df,date,prefix):
    current_date = datetime.now().date()
    df = df.withColumn("tenure", F.floor(F.datediff(F.lit(current_date), F.col(date))/365))
    df = df.withColumn(f"{prefix}_tenure_group", F.when(F.col("tenure").between(0, 2), "tenure0-2")
                                           .when(F.col("tenure").between(3, 4), "tenure3-4")
                                           .when(F.col("tenure").between(5, 6), "tenure5-6")
                                           .when(F.col("tenure").between(7, 8), "tenure7-8")
                                           .otherwise("tenure>8"))
    # print(df.toPandas().head())
    return df



def tenurematrix(df1, df2, vip1, vip2, matched_df, prefix1, prefix2, date1, date2):
    df1 = assign_tenure_group(df1,date1,prefix1)
    df2= assign_tenure_group(df2,date2,prefix2)

    joined_df = matched_df.join(df1, df1[vip1] == matched_df[f"{prefix1}_key"]).join(df2, df2[vip2] == matched_df[f"{prefix2}_key"])

    # Step 3: Calculate the count of overlapping customers for each combination of date1 and date2
    overlap_counts = joined_df.groupBy(f"{prefix1}_tenure_group", f"{prefix2}_tenure_group").agg(F.count("*").alias("overlap_count"))

    # Step 4: Calculate the total count of customers for each date1 and date2 combination
    total_counts = joined_df.count()

    # Step 5: Calculate the percentage of overlapping customers for each combination
    overlap_percentage = overlap_counts.withColumn("pct", F.col("overlap_count") / total_counts)

    # Pivot the table to get the desired matrix format
    matrix = overlap_percentage.groupBy(f"{prefix1}_tenure_group").pivot(f"{prefix2}_tenure_group").agg(F.first("pct"))

    # Display the resulting tier matrix
    return matrix.toPandas()

# COMMAND ----------

# tenure lc imx
tenurematrix(lc_demo_active_labeled, imx_demo_active_labeled, "vip_no", "vip_main_no", lc_imx_customer_brand, 'lc', "imx","first_transaction_date",'VIP_FIRST_PUR_DATE')

# COMMAND ----------

# MAGIC %md
# MAGIC # Gender

# COMMAND ----------

lc_demo_active_labeled.select('sex').toPandas().value_counts()

# COMMAND ----------

imx_demo_active_labeled.select("VIP_SEX").toPandas().value_counts()

# COMMAND ----------

matched_df = lc_imx_customer_brand
df1=lc_demo_active_labeled
df2=imx_demo_active_labeled
prefix1="lc"
prefix2='imx'
vip1='vip_no'
vip2='vip_main_no'

joined_df = matched_df.join(df1, df1[vip1] == matched_df[f"{prefix1}_key"]).join(df2, df2[vip2] == matched_df[f"{prefix2}_key"])
count = joined_df.groupby('sex').agg(F.count("*").alias('overlap_count'))
total_count = joined_df.count()
pct=count.withColumn("pct",count['overlap_count']/total_count)
pct.toPandas()[['sex','pct']]

# COMMAND ----------

# MAGIC %md
# MAGIC # SOW by BU

# COMMAND ----------

sales_scope_labeled1=lc_sales_scope_labeled
overlap_label2=imx_label
sales_amt_col1='amt_hkd'
comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).groupby('bu_desc').agg(F.sum(sales_amt_col1).alias("amt"))
total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).agg(F.sum("amt_hkd")).collect()[0][0]
pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
pct.toPandas()[['bu_desc','pct']]

# COMMAND ----------

sales_scope_labeled1=imx_sales_scope_labeled
overlap_label2=lc_label
sales_amt_col1='net_amt_HKD'
comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).groupby('bu_desc').agg(F.sum(sales_amt_col1).alias("amt"))
total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).agg(F.sum(sales_amt_col1)).collect()[0][0]
pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
pct.toPandas()[['bu_desc','pct']]

# COMMAND ----------

# MAGIC %md
# MAGIC # SOW by Cat ??

# COMMAND ----------

# %sql
# select distinct arti_cate_name

# COMMAND ----------

# #lc-jb lc
# sales_scope_labeled1=lc_sales_scope_labeled
# overlap_label2=jb_label
# sales_amt_col1='amt_hkd'
# comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).filter(sales_scope_labeled1['bu_desc'] == "COS").groupby('arti_cate_name').agg(F.sum(sales_amt_col1).alias("amt"))
# total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).filter(sales_scope_labeled1['bu_desc'] == "COS").agg(F.sum(sales_amt_col1)).collect()[0][0]
# pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
# pct.toPandas()[['arti_cate_name','pct']].sort_values("arti_cate_name")

# COMMAND ----------

# #lc-jb jb
# jb_sales_scope_labeled2 = spark.sql("select * from jb_sales_scope_labeled2")
# sales_scope_labeled1=jb_sales_scope_labeled2
# overlap_label2=lc_label
# sales_amt_col1='net_amt_hkd'
# comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).groupby('arti_cate_name').agg(F.sum(sales_amt_col1).alias("amt"))
# total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).agg(F.sum(sales_amt_col1)).collect()[0][0]
# pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
# pct.toPandas()[['arti_cate_name','pct']].sort_values("arti_cate_name")

# COMMAND ----------

#lc-imx 
sales_scope_labeled1=lc_sales_scope_labeled
overlap_label2=imx_label
sales_amt_col1='amt_hkd'
comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).groupby('arti_cate_name').agg(F.sum(sales_amt_col1).alias("amt"))
total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).agg(F.sum(sales_amt_col1)).collect()[0][0]
lc_pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
lc_pct=lc_pct.toPandas()[['arti_cate_name','pct']].sort_values("arti_cate_name")


sales_scope_labeled1=imx_sales_scope_labeled
overlap_label2=lc_label
sales_amt_col1='net_amt_hkd'
comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).groupby('arti_cate_name').agg(F.sum(sales_amt_col1).alias("amt"))
total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).agg(F.sum(sales_amt_col1)).collect()[0][0]
imx_pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
imx_pct=imx_pct.toPandas()[['arti_cate_name','pct']].sort_values("arti_cate_name")


lc_pct.merge(imx_pct,how='outer',on='arti_cate_name',suffixes=("_lc","_imx"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select arti_cate_name,
# MAGIC LC,
# MAGIC sum(net_amt_hkd)
# MAGIC from imx_sales_scope_labeled
# MAGIC group by 1,2

# COMMAND ----------

# #imx-jb imx
# sales_scope_labeled1=imx_sales_scope_labeled
# overlap_label2=jb_label
# sales_amt_col1='net_amt_hkd'
# comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).groupby('arti_cate_name').agg(F.sum(sales_amt_col1).alias("amt"))
# total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).agg(F.sum(sales_amt_col1)).collect()[0][0]
# pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
# pct.toPandas()[['arti_cate_name','pct']].sort_values("arti_cate_name")

# COMMAND ----------

# #imx-jb jb
# sales_scope_labeled1=jb_sales_scope_labeled2
# overlap_label2=imx_label
# sales_amt_col1='net_amt_hkd'
# comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).groupby('arti_cate_name').agg(F.sum(sales_amt_col1).alias("amt"))
# total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).agg(F.sum(sales_amt_col1)).collect()[0][0]
# pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
# pct.toPandas()[['arti_cate_name','pct']].sort_values("arti_cate_name")

# COMMAND ----------

# MAGIC %md
# MAGIC # Top Class

# COMMAND ----------

imx_sales_scope_labeled.columns

# COMMAND ----------

lc_sales_scope_labeled.columns

# COMMAND ----------

def get_topproduct(sales_scope_labeled,overlap_label,class_desc_col,vip1):
    overlapped = sales_scope_labeled.filter(sales_scope_labeled[overlap_label] == 1)
    # Calculate the sum of sales_col for each brand 
    brand_sales_df = overlapped.groupBy(class_desc_col).agg(F.countDistinct(vip1).alias('count_customer'))
    # Calculate the grand total
    grand_total = overlapped.agg(F.countDistinct(vip1).alias('total')).first()[0]
    # Calculate the percentage of grand total for each brand
    percentage_df = brand_sales_df.withColumn('percentage', (F.col('count_customer') / grand_total) * 100)

    # Order the result by percentage in descending order
    result_df = percentage_df.orderBy(F.desc('percentage'))
    return result_df.toPandas()

# COMMAND ----------

# LC TOP CLASS
get_topproduct(sales_scope_labeled=lc_sales_scope_labeled_fashion,overlap_label=imx_label,class_desc_col='class_desc',vip1='vip_no')

# COMMAND ----------

# get top product of sacai

overlapped = lc_sales_scope_labeled.filter(F.col('brand_desc').like("%SACAI%"))
brand_sales_df = overlapped.groupBy('class_desc').agg(F.countDistinct('vip_no').alias('count_customer'))
# Calculate the grand total
grand_total = overlapped.agg(F.countDistinct('vip_no').alias('total')).first()[0]
# Calculate the percentage of grand total for each brand
percentage_df = brand_sales_df.withColumn('percentage', (F.col('count_customer') / grand_total) * 100)
# Order the result by percentage in descending order
result_df = percentage_df.orderBy(F.desc('percentage'))
result_df.toPandas()

# COMMAND ----------

# get top product of sacai

overlapped = lc_sales_scope_labeled.filter(F.col('brand_desc').like("%SACAI%")).filter(F.col(imx_label)==1)
brand_sales_df = overlapped.groupBy('class_desc').agg(F.countDistinct('vip_no').alias('count_customer'))
# Calculate the grand total
grand_total = overlapped.agg(F.countDistinct('vip_no').alias('total')).first()[0]
# Calculate the percentage of grand total for each brand
percentage_df = brand_sales_df.withColumn('percentage', (F.col('count_customer') / grand_total) * 100)
# Order the result by percentage in descending order
result_df = percentage_df.orderBy(F.desc('percentage'))
result_df.toPandas()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Penetration by Store

# COMMAND ----------

# lc imx
df1=imx_sales_scope_labeled
df1.groupBy(df1['shop_desc']).agg(F.mean(df1[lc_label]).alias('penetration')).toPandas().sort_values("penetration",ascending=False).iloc[:6,:]

# COMMAND ----------

# MAGIC %md
# MAGIC # Nationality

# COMMAND ----------

matched_df = lc_imx_customer_brand
df1=lc_demo_active_labeled
df2=imx_demo_active_labeled
prefix1="lc"
prefix2='imx'
vip1='vip_no'
vip2='vip_main_no'

joined_df = matched_df.join(df1, df1[vip1] == matched_df[f"{prefix1}_key"]).join(df2, df2[vip2] == matched_df[f"{prefix2}_key"])
count = joined_df.groupby(df2.nationality).agg(F.count("*").alias('overlap_count'))
total_count = joined_df.count()
pct=count.withColumn("pct",count['overlap_count']/total_count)
pct.toPandas()[['nationality','pct']]

# COMMAND ----------

# MAGIC %md
# MAGIC # Appendix

# COMMAND ----------

lc_sales_scope_labeled.filter(lc_sales_scope_labeled[imx_label]==1).filter(lc_sales_scope_labeled.brand_desc=='59015 - AVEDA').groupBy("prod_desc").agg(F.countDistinct('vip_no').alias("n_customers")).toPandas().sort_values('n_customers',ascending=False)

# COMMAND ----------

jb_sales_scope_labeled.select("arti_cate_name","maincat_desc").distinct().show()

# COMMAND ----------

imx_sales_scope_labeled.select("brand_desc").distinct().show()

# COMMAND ----------

imx_ab=imx_sales_scope_labeled.filter(F.col(jb_label)==1).filter(F.col("brand_desc")=="Augustinus Bader").select("vip_main_no").distinct()
imx_ab.count()

# COMMAND ----------

imx_nb=imx_sales_scope_labeled.filter(F.col(jb_label)==1).filter(F.col("brand_desc")=="Natura Bisse").select("vip_main_no").distinct()
imx_nb.count()

# COMMAND ----------

lccos_sales_scope_labeled.select("brand_desc").distinct().sort("brand_desc").toPandas()

# COMMAND ----------

jb_nb=jb_sales_scope_labeled.filter(F.col(imx_label)==1).filter(F.col("brand_desc")=="Natura Bisse".upper()).select("vip_main_no").distinct()
jb_nb.count()

# COMMAND ----------

jb_ab=jb_sales_scope_labeled.filter(F.col(imx_label)==1).filter(F.col("brand_desc")=="Augustinus Bader".upper()).select("vip_main_no").distinct()
jb_ab.count()

# COMMAND ----------

imx_jb_customer.join(imx_nb,[imx_jb_customer.imx_key == imx_nb.vip_main_no],how='inner').join(jb_nb,[imx_jb_customer.jb_key == jb_nb.vip_main_no],how='inner').distinct().count()

# COMMAND ----------

imx_jb_customer.join(imx_ab,[imx_jb_customer.imx_key == imx_ab.vip_main_no],how='inner').join(jb_ab,[imx_jb_customer.jb_key == jb_ab.vip_main_no],how='inner').distinct().count()

# COMMAND ----------

b="MAISON FRANCIS KURKD"

jb_mfk=jb_sales_scope_labeled.filter(F.col(lc_label)==1).filter(F.col("brand_desc")==b.upper()).select("vip_main_no").distinct()
print(jb_mfk.count())
lc_mfk=lc_sales_scope_labeled.filter(F.col(jb_label)==1).filter(F.col("brand_desc").like("%MAISON FRANCIS KURKD%")).select("vip_no").distinct()
print(lc_mfk.count())
lc_jb_customer.join(lc_mfk,[lc_jb_customer.lc_key == lc_mfk.vip_no],how='inner').join(jb_mfk,[lc_jb_customer.jb_key == jb_mfk.vip_main_no],how='inner').distinct().count()


# COMMAND ----------

# MAGIC %md
# MAGIC # Store

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct vip_main_no) from jb_sales_scope_labeled
# MAGIC where shop_code in ("JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11")
# MAGIC and LC =1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct vip_main_no) from 
# MAGIC (
# MAGIC select distinct vip_main_no from jb_sales_scope_labeled
# MAGIC where shop_code in ("JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11")
# MAGIC and LC =1
# MAGIC intersect
# MAGIC select distinct vip_main_no from jb_sales_scope_labeled
# MAGIC where shop_code not in ("JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11")
# MAGIC and LC =1
# MAGIC )

# COMMAND ----------

lc_sales_scope_labeled.filter(F.col(jb_label) == 1)\
    .groupBy("loc_code_neo") \
    .agg(F.countDistinct("vip_no")).toPandas()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lcjg.lcgold.store_dim

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct loc_code_neo ,store_name
# MAGIC from lc_sales_scope_labeled S
# MAGIC left join  lcjg.lcgold.store_dim D on S.loc_code_neo = D.store_id
# MAGIC where JB=1

# COMMAND ----------

jb_sales_scope_labeled.filter(F.col("brand_desc")=="Augustinus Bader".upper()).select("vip_main_no").distinct().count()
jb_sales_scope_labeled.filter(F.col("brand_desc")=="Natura Bisse".upper()).select("vip_main_no").distinct().count()
imx_sales_scope_labeled.filter(F.col("brand_desc")=="Natura Bisse").select("vip_main_no").distinct().count()
imx_sales_scope_labeled.filter(F.col("brand_desc")=="Augustinus Bader").select("vip_main_no").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC # JB Issue

# COMMAND ----------


issue = pd.read_csv( os.path.join("/dbfs"+project_dir , "jb_new_joiner_issue_loc.csv"))

spark_df = spark.createDataFrame(issue)
# Register the Spark DataFrame as a temporary table
spark_df.createOrReplaceTempView("issue")
issue

# COMMAND ----------

spark_df = spark.createDataFrame(issue)
# Register the Spark DataFrame as a temporary table
spark_df.createOrReplaceTempView("issue")

# COMMAND ----------

# %sql
# select * from jb_sales_scope_labeled
# where vip_main_no in (select `Member No` from issue)

# COMMAND ----------

# MAGIC %md
# MAGIC # Brand Overlap

# COMMAND ----------

# %sql
# select distinct brand_code,bu_desc, arti_cate_name, maincat_desc,item_desc
# from imx_sales_scope_labeled
# where brand_code="CM"
# and arti_cate_name in ("Accessories","Dress")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view SACAI_LCONLY_VIPNO AS
# MAGIC select distinct vip_no
# MAGIC from lc_sales_scope
# MAGIC where brand_desc like "%SACAI%"
# MAGIC and vip_no not in (select lc_key from lc_imx_customer_brand);
# MAGIC
# MAGIC create or replace temp view SACAI_IMXONLY_VIPNO AS
# MAGIC select distinct vip_main_no
# MAGIC from imx_sales_scope
# MAGIC where brand_code ="SC"
# MAGIC and vip_main_no not in (select imx_key from lc_imx_customer_brand);
# MAGIC
# MAGIC create or replace temp view SACAI_OVERLAP AS
# MAGIC select distinct vip_main_no,c.vip_no
# MAGIC from imx_sales_scope a 
# MAGIC inner join lc_imx_customer_brand b on a.vip_main_no = b.imx_key
# MAGIC inner join lc_sales_scope c on c.vip_no = b.lc_key
# MAGIC where a.brand_code ="SC"
# MAGIC and c.brand_desc like "%SACAI%";

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SACAI LC ONLY  avg spending
# MAGIC select 
# MAGIC bu_desc,
# MAGIC SUM(amt_hkd) / 35686494.7 as p_spending
# MAGIC from lc_sales_scope A
# MAGIC right join SACAI_LCONLY_VIPNO C using(vip_no)
# MAGIC where  A.brand_desc like "%SACAI%"
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SACAI LC ONLY CLASS TOP
# MAGIC select 
# MAGIC A.class_desc,
# MAGIC B.subclass_desc ,
# MAGIC count(distinct A.vip_no)
# MAGIC from lc_sales_scope A
# MAGIC left join lc_prd.crm_db_neo_silver.dbo_v_sales_dtl B using(order_no)
# MAGIC right join SACAI_LCONLY_VIPNO C using(vip_no)
# MAGIC where  A.brand_desc like "%SACAI%"
# MAGIC group by A.class_desc, subclass_desc
# MAGIC order by 3 desc
# MAGIC limit 13

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SACAI LC ONLY CLASS TOP
# MAGIC select 
# MAGIC A.class_desc,
# MAGIC count(distinct A.vip_no)
# MAGIC from lc_sales_scope A
# MAGIC left join lc_prd.crm_db_neo_silver.dbo_v_sales_dtl B using(order_no)
# MAGIC right join SACAI_LCONLY_VIPNO C using(vip_no)
# MAGIC where  A.brand_desc like "%SACAI%"
# MAGIC group by A.class_desc
# MAGIC order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC A.class_desc,
# MAGIC B.subclass_desc ,
# MAGIC count(distinct A.vip_no)
# MAGIC from lc_sales_scope A
# MAGIC left join lc_prd.crm_db_neo_silver.dbo_v_sales_dtl B using(order_no)
# MAGIC right join SACAI_OVERLAP C using(vip_no)
# MAGIC where  A.brand_desc like "%SACAI%"
# MAGIC group by A.class_desc, subclass_desc
# MAGIC order by 3 desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SACAI LC ONLY CLASS TOP. price point
# MAGIC select 
# MAGIC arti_cate_name,
# MAGIC sum(amt_hkd) / sum(dtl_qty) as pp
# MAGIC from lc_sales_scope A
# MAGIC right join SACAI_LCONLY_VIPNO C using(vip_no)
# MAGIC where  A.brand_desc like "%SACAI%"
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SACAI overlap. price point
# MAGIC select 
# MAGIC arti_cate_name,
# MAGIC sum(amt_hkd) / sum(dtl_qty) as pp
# MAGIC from lc_sales_scope A
# MAGIC right join SACAI_OVERLAP C using(vip_no)
# MAGIC where  A.brand_desc like "%SACAI%"
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lc_prd.crm_db_neo_silver.dbo_v_sales_dtl

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SACAI LC ONLY
# MAGIC -- store
# MAGIC select 
# MAGIC store_name10,
# MAGIC count(distinct A.vip_No)
# MAGIC from lc_sales_scope A
# MAGIC left join lcjg.lcgold.store_dim B on A.loc_code_neo=B.store_id
# MAGIC right join SACAI_LCONLY_VIPNO C using(vip_no)
# MAGIC where  A.brand_desc like "%SACAI%"
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SACAI LC overlap
# MAGIC -- store
# MAGIC select 
# MAGIC store_name3,
# MAGIC count(distinct A.vip_No)
# MAGIC from lc_sales_scope A
# MAGIC left join lcjg.lcgold.store_dim B on A.loc_code_neo=B.store_id
# MAGIC right join SACAI_OVERLAP C using(vip_no)
# MAGIC where  A.brand_desc like "%SACAI%"
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct item_desc, arti_cate_name, maincat_desc
# MAGIC from imx_sales_scope
# MAGIC where brand_code ="SC"
# MAGIC and arti_cate_name in ("Shoes","Bags")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CM

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC A.class_desc,
# MAGIC B.subclass_desc ,
# MAGIC count(distinct A.vip_no)
# MAGIC from lc_sales_scope_labeled A
# MAGIC left join lc_prd.crm_db_neo_silver.dbo_v_sales_dtl B using(order_no)
# MAGIC where IMX=1
# MAGIC and B.bu_desc in ("WW","MW","LSA","MSA")
# MAGIC group by A.class_desc, subclass_desc
# MAGIC order by 3 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CM overlap. price point
# MAGIC select 
# MAGIC arti_cate_name,
# MAGIC sum(amt_hkd) / sum(dtl_qty) as pp
# MAGIC from lc_sales_scope_labeled A
# MAGIC where IMX=1
# MAGIC and A.bu_desc in ("WW","MW","LSA","MSA")
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SACAI LC overlap
# MAGIC -- store
# MAGIC select 
# MAGIC store_name10,
# MAGIC count(distinct A.vip_No) 
# MAGIC
# MAGIC from lc_sales_scope_labeled A 
# MAGIC left join lcjg.lcgold.store_dim B on A.loc_code_neo=B.store_id
# MAGIC where IMX=1
# MAGIC group by 1
# MAGIC order by 2 desc
