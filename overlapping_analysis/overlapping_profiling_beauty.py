# Databricks notebook source
# MAGIC %run /utils/spark_utils

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Overlapping Customer List

# COMMAND ----------

import os
import pandas as pd
project_dir="/mnt/dev/overlapping_analysis/" # developing environment

sales_tables=['imx_sales_scope_labeled',
              "lc_sales_scope_labeled",
              "lccos_sales_scope_labeled",
              "jb_sales_scope_labeled"]
demo_tables=["imx_demo_active_labeled",
             "lc_demo_active_labeled",
             "lccos_demo_active_labeled",
             "jb_demo_active_labeled"]

matched_tables=[
             "lc_imx_customer",
             "lccos_imx_customer",
              "lc_jb_customer",
             "lccos_jb_customer",
             "imx_jb_customer"
            ]

for t in sales_tables+demo_tables+matched_tables:
    load_parquet_to_temp_table(os.path.join(project_dir,t),t )

imx_sales_scope_labeled = spark.sql('select * from imx_sales_scope_labeled')
lc_sales_scope_labeled = spark.sql('select * from lc_sales_scope_labeled')
lccos_sales_scope_labeled = spark.sql('select * from lccos_sales_scope_labeled')
jb_sales_scope_labeled = spark.sql('select * from jb_sales_scope_labeled')

imx_demo_active_labeled = spark.sql('select * from imx_demo_active_labeled')
lc_demo_active_labeled = spark.sql('select * from lc_demo_active_labeled')
lccos_demo_active_labeled = spark.sql('select * from lccos_demo_active_labeled')
jb_demo_active_labeled = spark.sql('select * from jb_demo_active_labeled')

lc_imx_customer = spark.sql('select * from lc_imx_customer')
lc_jb_customer = spark.sql('select * from lc_jb_customer')
lccos_imx_customer = spark.sql('select * from lccos_imx_customer')
lccos_jb_customer = spark.sql('select * from lccos_jb_customer')
imx_jb_customer = spark.sql('select * from imx_jb_customer')

# COMMAND ----------

project_path = "/dbfs/mnt/dev/overlapping_analysis/"
arti_cate_dim = pd.read_csv(os.path.join(project_path,"arti_cate_dim.csv"),index_col="arti_cate_code")
jb_cat_mapping = pd.read_csv(os.path.join(project_path,"jb_cat_mapping.csv"))
jb_cat_mapping= pd.merge(jb_cat_mapping, arti_cate_dim, on='arti_cate_code', how='left')
spark.createDataFrame(jb_cat_mapping).createOrReplaceTempView("jb_cat_mapping")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW jb_sales_scope_labeled1 AS
# MAGIC SELECT A.* EXCEPT (arti_cate_name) ,
# MAGIC B.item_subcat_desc
# MAGIC FROM jb_sales_scope_labeled A
# MAGIC LEFT JOIN imx_prd.dashboard_crm_gold.beauty_valid_tx0 B USING (sales_main_key);
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW jb_sales_scope_labeled2 AS
# MAGIC SELECT A.* ,
# MAGIC B.arti_cate_name
# MAGIC FROM jb_sales_scope_labeled1 A
# MAGIC LEFT JOIN jb_cat_mapping B USING (item_subcat_desc)
# MAGIC

# COMMAND ----------

jb_sales_scope_labeled.columns

# COMMAND ----------

import os
import json
path= "/dbfs/mnt/dev/overlapping_analysis/n_unique_cust.json"

with open(path, 'r') as file:
    n_unique_cust = file.read()
n_unique_cust=eval(n_unique_cust)
n_unique_cust

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


from pyspark.sql import functions as F
lc_label = "LC"
lccos_label="LCCOS"
imx_label="IMX"
jb_label="JB"

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

import pandas as pd

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

lc_imx_sales = sales_behavior_table (sales_scope_labeled1=lc_sales_scope_labeled, 
                      sales_scope_labeled2=imx_sales_scope_labeled,
                      overlap_label1=lc_label,
                      overlap_label2=imx_label,
                      vip_col1='vip_no',
                      vip_col2='vip_main_no',
                      sales_amt_col1='amt_hkd',
                      sales_amt_col2='net_amt_hkd',
                      order_no_col1='order_no',
                      order_no_col2='sales_main_key',
                      qty_col1='dtl_qty',
                      qty_col2='sold_qty',
                      prefix1=lc_label.lower(),
                      prefix2=imx_label.lower())

lccos_imx_sales = sales_behavior_table (sales_scope_labeled1=lccos_sales_scope_labeled, 
                      sales_scope_labeled2=imx_sales_scope_labeled,
                      overlap_label1=lccos_label,
                      overlap_label2=imx_label,
                      vip_col1='vip_no',
                      vip_col2='vip_main_no',
                      sales_amt_col1='amt_hkd',
                      sales_amt_col2='net_amt_hkd',
                      order_no_col1='order_no',
                      order_no_col2='sales_main_key',
                      qty_col1='dtl_qty',
                      qty_col2='sold_qty',
                      prefix1=lccos_label.lower(),
                      prefix2=imx_label.lower())

lc_jb_sales = sales_behavior_table (sales_scope_labeled1=lc_sales_scope_labeled, 
                      sales_scope_labeled2=jb_sales_scope_labeled,
                      overlap_label1=lc_label,
                      overlap_label2=jb_label,
                      vip_col1='vip_no',
                      vip_col2='vip_main_no',
                      sales_amt_col1='amt_hkd',
                      sales_amt_col2='net_amt_hkd',
                      order_no_col1='order_no',
                      order_no_col2='sales_main_key',
                      qty_col1='dtl_qty',
                      qty_col2='sold_qty',
                      prefix1=lc_label.lower(),
                      prefix2=jb_label.lower())

lccos_jb_sales = sales_behavior_table (sales_scope_labeled1=lccos_sales_scope_labeled, 
                      sales_scope_labeled2=jb_sales_scope_labeled,
                      overlap_label1=lccos_label,
                      overlap_label2=jb_label,
                      vip_col1='vip_no',
                      vip_col2='vip_main_no',
                      sales_amt_col1='amt_hkd',
                      sales_amt_col2='net_amt_hkd',
                      order_no_col1='order_no',
                      order_no_col2='sales_main_key',
                      qty_col1='dtl_qty',
                      qty_col2='sold_qty',
                      prefix1=lccos_label.lower(),
                      prefix2=jb_label.lower())

imx_jb_sales = sales_behavior_table (sales_scope_labeled1=imx_sales_scope_labeled, 
                      sales_scope_labeled2=jb_sales_scope_labeled,
                      overlap_label1=imx_label,
                      overlap_label2=jb_label,
                      vip_col1='vip_main_no',
                      vip_col2='vip_main_no',
                      sales_amt_col1='net_amt_hkd',
                      sales_amt_col2='net_amt_hkd',
                      order_no_col1='sales_main_key',
                      order_no_col2='sales_main_key',
                      qty_col1='sold_qty',
                      qty_col2='sold_qty',
                      prefix1=imx_label.lower(),
                      prefix2=jb_label.lower())

# COMMAND ----------

lc_jb_sales = sales_behavior_table (sales_scope_labeled1=lc_sales_scope_labeled, 
                      sales_scope_labeled2=jb_sales_scope_labeled,
                      overlap_label1=lc_label,
                      overlap_label2=jb_label,
                      vip_col1='vip_no',
                      vip_col2='vip_main_no',
                      sales_amt_col1='amt_hkd',
                      sales_amt_col2='net_amt_hkd',
                      order_no_col1='order_no',
                      order_no_col2='sales_main_key',
                      qty_col1='dtl_qty',
                      qty_col2='sold_qty',
                      prefix1=lc_label.lower(),
                      prefix2=jb_label.lower())

# COMMAND ----------

lc_jb_sales 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Table Concatenated

# COMMAND ----------

sales_concatenated = pd.concat([lc_imx_sales, lccos_imx_sales,lc_jb_sales,lccos_jb_sales,imx_jb_sales], keys=['LC-IMX', 'LCCOS - IMX','LC-JB','LCCOS-JB','IMX-JB'], axis=1)
sales_concatenated

# COMMAND ----------

# MAGIC %md
# MAGIC # Demographic: Penetration and Tenure

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metrics Calcualtion

# COMMAND ----------

# total number of customer
def p_overlap_cust(sales_scope_labeled,overlap_label,vip_col):
    # select overlapping customers with overlap_label
    overlapped_vip_count = sales_scope_labeled.filter(sales_scope_labeled[overlap_label] == 1).select(vip_col).distinct().count()
    # total number of customers
    total_vip_count = sales_scope_labeled.select(vip_col).distinct().count()
    # percentage of overlapping customer 
    p = overlapped_vip_count / total_vip_count
    return p

from pyspark.sql.functions import current_date, datediff, floor, avg

def tenure1_2(demo_active_labeled,overlap_label,vip_col,first_transaction_date_col):
    overlapped = demo_active_labeled.filter(demo_active_labeled[overlap_label] == 1)
    overlapped = overlapped.withColumn("tenure", floor(datediff(current_date(), overlapped[first_transaction_date_col]) / 365))

    tenure_perc = overlapped.filter(overlapped.tenure<=2).select(vip_col).distinct().count() / overlapped.select(vip_col).distinct().count()

    return tenure_perc

def tenure3_4(demo_active_labeled,overlap_label,vip_col,first_transaction_date_col):
    overlapped = demo_active_labeled.filter(demo_active_labeled[overlap_label] == 1)
    overlapped = overlapped.withColumn("tenure", floor(datediff(current_date(), overlapped[first_transaction_date_col]) / 365))

    tenure_perc = overlapped.filter((overlapped.tenure<=4) & (overlapped.tenure>=3)).select(vip_col).distinct().count() / overlapped.select(vip_col).distinct().count()

    return tenure_perc

def tenure5_6(demo_active_labeled,overlap_label,vip_col,first_transaction_date_col):
    overlapped = demo_active_labeled.filter(demo_active_labeled[overlap_label] == 1)
    overlapped = overlapped.withColumn("tenure", floor(datediff(current_date(), overlapped[first_transaction_date_col]) / 365))

    tenure_perc = overlapped.filter((overlapped.tenure<=6) & (overlapped.tenure>=5)).select(vip_col).distinct().count() / overlapped.select(vip_col).distinct().count()

    return tenure_perc

def tenure7_8(demo_active_labeled,overlap_label,vip_col,first_transaction_date_col):
    overlapped = demo_active_labeled.filter(demo_active_labeled[overlap_label] == 1)
    overlapped = overlapped.withColumn("tenure", floor(datediff(current_date(), overlapped[first_transaction_date_col]) / 365))

    tenure_perc = overlapped.filter((overlapped.tenure<=8) & (overlapped.tenure>=7)).select(vip_col).distinct().count() / overlapped.select(vip_col).distinct().count()

    return tenure_perc

def tenure8_(demo_active_labeled,overlap_label,vip_col,first_transaction_date_col):
    overlapped = demo_active_labeled.filter(demo_active_labeled[overlap_label] == 1)
    overlapped = overlapped.withColumn("tenure", floor(datediff(current_date(), overlapped[first_transaction_date_col]) / 365))

    tenure_perc = overlapped.filter( overlapped.tenure>8).select(vip_col).distinct().count() / overlapped.select(vip_col).distinct().count()

    return tenure_perc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summarize

# COMMAND ----------

from pyspark.sql.functions import to_date
import pandas as pd

def demo_table(sales_scope_labeled1,
               sales_scope_labeled2,
               demo_active_labeled1,
               demo_active_labeled2,
               overlap_label1,
               overlap_label2,
               vip_col1,
               vip_col2,
               first_transaction_date_col1,
               first_transaction_date_col2,
               ):
    index = ['% overlap customers','tenure 1-2', 'tenure 3-4','tenure 5-6','tenure 7-8','tenure >8']
    columns=[overlap_label1,overlap_label2]
    table = pd.DataFrame(columns=columns,index=index)

    table .loc['% overlap customers',overlap_label1] = p_overlap_cust(sales_scope_labeled=sales_scope_labeled1,overlap_label=overlap_label2,vip_col=vip_col1)
    table .loc['% overlap customers',overlap_label2] = p_overlap_cust(sales_scope_labeled=sales_scope_labeled2,overlap_label=overlap_label1,vip_col=vip_col2)


    table .loc['tenure 1-2',overlap_label1] = tenure1_2(demo_active_labeled=demo_active_labeled1,overlap_label=overlap_label2,vip_col=vip_col1,first_transaction_date_col=first_transaction_date_col1)

    table .loc['tenure 1-2',overlap_label2] = tenure1_2(demo_active_labeled=demo_active_labeled2,overlap_label=overlap_label1,vip_col=vip_col2,first_transaction_date_col=first_transaction_date_col2)


    table .loc['tenure 3-4',overlap_label1] = tenure3_4(demo_active_labeled=demo_active_labeled1,overlap_label=overlap_label2,vip_col=vip_col1,first_transaction_date_col=first_transaction_date_col1)

    table .loc['tenure 3-4',overlap_label2] = tenure3_4(demo_active_labeled=demo_active_labeled2,overlap_label=overlap_label1,vip_col=vip_col2,first_transaction_date_col=first_transaction_date_col2)

    table .loc['tenure 5-6',overlap_label1] = tenure5_6(demo_active_labeled=demo_active_labeled1,overlap_label=overlap_label2,vip_col=vip_col1,first_transaction_date_col=first_transaction_date_col1)

    table .loc['tenure 5-6',overlap_label2] = tenure5_6(demo_active_labeled=demo_active_labeled2,overlap_label=overlap_label1,vip_col=vip_col2,first_transaction_date_col=first_transaction_date_col2)

    table .loc['tenure 7-8',overlap_label1] = tenure7_8(demo_active_labeled=demo_active_labeled1,overlap_label=overlap_label2,vip_col=vip_col1,first_transaction_date_col=first_transaction_date_col1)

    table .loc['tenure 7-8',overlap_label2] = tenure7_8(demo_active_labeled=demo_active_labeled2,overlap_label=overlap_label1,vip_col=vip_col2,first_transaction_date_col=first_transaction_date_col2)

    table .loc['tenure >8',overlap_label1] = tenure8_(demo_active_labeled=demo_active_labeled1,overlap_label=overlap_label2,vip_col=vip_col1,first_transaction_date_col=first_transaction_date_col1)

    table .loc['tenure >8',overlap_label2] = tenure8_(demo_active_labeled=demo_active_labeled2,overlap_label=overlap_label1,vip_col=vip_col2,first_transaction_date_col=first_transaction_date_col2)

    table = table.applymap(lambda x:x*100).round(2).applymap(lambda x: str(x)+"%")
    return table

# COMMAND ----------

lc_imx_demo =demo_table(sales_scope_labeled1=lc_sales_scope_labeled,
               sales_scope_labeled2=imx_sales_scope_labeled,
               demo_active_labeled1=lc_demo_active_labeled,
               demo_active_labeled2=imx_demo_active_labeled,
               overlap_label1=lc_label,
               overlap_label2=imx_label,
               vip_col1="vip_no",
               vip_col2="vip_main_no",
               first_transaction_date_col1="first_transaction_date",
               first_transaction_date_col2='VIP_FIRST_PUR_DATE',
               )
lccos_imx_demo =demo_table(sales_scope_labeled1=lccos_sales_scope_labeled,
               sales_scope_labeled2=imx_sales_scope_labeled,
               demo_active_labeled1=lccos_demo_active_labeled,
               demo_active_labeled2=imx_demo_active_labeled,
               overlap_label1=lccos_label,
               overlap_label2=imx_label,
               vip_col1="vip_no",
               vip_col2="vip_main_no",
               first_transaction_date_col1="first_transaction_date",
               first_transaction_date_col2='VIP_FIRST_PUR_DATE',
               )


lc_jb_demo =demo_table(sales_scope_labeled1=lc_sales_scope_labeled,
               sales_scope_labeled2=jb_sales_scope_labeled,
               demo_active_labeled1=lc_demo_active_labeled,
               demo_active_labeled2=jb_demo_active_labeled,
               overlap_label1=lc_label,
               overlap_label2=jb_label,
               vip_col1="vip_no",
               vip_col2="vip_main_no",
               first_transaction_date_col1="first_transaction_date",
               first_transaction_date_col2='VIP_FIRST_PUR_DATE',
               )
lccos_jb_demo =demo_table(sales_scope_labeled1=lccos_sales_scope_labeled,
               sales_scope_labeled2=jb_sales_scope_labeled,
               demo_active_labeled1=lccos_demo_active_labeled,
               demo_active_labeled2=jb_demo_active_labeled,
               overlap_label1=lccos_label,
               overlap_label2=jb_label,
               vip_col1="vip_no",
               vip_col2="vip_main_no",
               first_transaction_date_col1="first_transaction_date",
               first_transaction_date_col2='VIP_FIRST_PUR_DATE',
               )

imx_jb_demo =demo_table(sales_scope_labeled1=imx_sales_scope_labeled,
               sales_scope_labeled2=jb_sales_scope_labeled,
               demo_active_labeled1=imx_demo_active_labeled,
               demo_active_labeled2=jb_demo_active_labeled,
               overlap_label1=imx_label,
               overlap_label2=jb_label,
               vip_col1="vip_main_no",
               vip_col2="vip_main_no",
               first_transaction_date_col1='VIP_FIRST_PUR_DATE',
               first_transaction_date_col2='VIP_FIRST_PUR_DATE',
               )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concatenated

# COMMAND ----------

demo_concatenated = pd.concat([lc_imx_demo, lccos_imx_demo,lc_jb_demo,lccos_jb_demo,imx_jb_demo], keys=['LC-IMX', 'LCCOS - IMX','LC-JB','LCCOS-JB','IMX-JB'], axis=1)
demo_concatenated

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
lccos_imx_sw=sw_table(sales_scope_labeled1=lccos_sales_scope_labeled,
             sales_scope_labeled2=imx_sales_scope_labeled,
             overlap_label1=lccos_label,
             overlap_label2=imx_label,
             sales_amt_col1='amt_hkd',
             sales_amt_col2='net_amt_hkd'
             )
lc_jb_sw=sw_table(sales_scope_labeled1=lc_sales_scope_labeled,
             sales_scope_labeled2=jb_sales_scope_labeled,
             overlap_label1=lc_label,
             overlap_label2=jb_label,
             sales_amt_col1='amt_hkd',
             sales_amt_col2='net_amt_hkd'
             )
lccos_jb_sw=sw_table(sales_scope_labeled1=lccos_sales_scope_labeled,
             sales_scope_labeled2=jb_sales_scope_labeled,
             overlap_label1=lccos_label,
             overlap_label2=jb_label,
             sales_amt_col1='amt_hkd',
             sales_amt_col2='net_amt_hkd'
             )

imx_jb_sw=sw_table(sales_scope_labeled1=imx_sales_scope_labeled,
             sales_scope_labeled2=jb_sales_scope_labeled,
             overlap_label1=imx_label,
             overlap_label2=jb_label,
             sales_amt_col1='net_amt_hkd',
             sales_amt_col2='net_amt_hkd'
             )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concatenated

# COMMAND ----------

sw_concatenated = pd.concat([lc_imx_sw, lccos_imx_sw,lc_jb_sw,lccos_jb_sw,imx_jb_sw], keys=['LC-IMX', 'LCCOS - IMX','LC-JB','LCCOS-JB','IMX-JB'], axis=1)
sw_concatenated

# COMMAND ----------

# MAGIC %md
# MAGIC # Tier

# COMMAND ----------

from pyspark.sql.functions import countDistinct, col



def tierPercent(demo_active_labeled,overlap_label,vip_col,tier_col):
    overlapped = demo_active_labeled.filter(demo_active_labeled[overlap_label] == 1)
    # Calculate the total number of unique vip_no
    total_count = overlapped.select(countDistinct(vip_col)).collect()[0][0]
    # Calculate the count of each card_type_updated and its percentage
    result_df = overlapped.groupBy(tier_col).agg(countDistinct(vip_col).alias('count'))
    result_df = result_df.withColumn('percentage', (col('count') / total_count)).toPandas().set_index(tier_col)
    # Show the result DataFrame
    return result_df

# COMMAND ----------

lc_imx_lctier = tierPercent(demo_active_labeled=lc_demo_active_labeled,overlap_label=imx_label,vip_col='vip_no',tier_col='CARD_TYPE_updated')
lc_imx_lctier['order']=pd.Series(index=["Beauty+","Privilege","Gold","Platinum","Diamond"],data = [1,2,3,4,5])
lc_imx_lctier .sort_values("order")

# COMMAND ----------

lc_imx_imxtier = tierPercent(demo_active_labeled=imx_demo_active_labeled,overlap_label=lc_label,vip_col='vip_main_no',tier_col='viptyp_desc_unique')
lc_imx_imxtier['order']=pd.Series(index=["ICARD Silver","ICARD Gold","ICARD Diamond","ICARD Pre-member","OTHERS"],data = [1,2,3,4,5])
lc_imx_imxtier.sort_values("order")

# COMMAND ----------

lccos_imx_lccostier = tierPercent(demo_active_labeled=lccos_demo_active_labeled,overlap_label=imx_label,vip_col='vip_no',tier_col='CARD_TYPE_updated')
lccos_imx_lccostier['order']=pd.Series(index=["Beauty+","Privilege","Gold","Platinum","Diamond"],data = [1,2,3,4,5])
lccos_imx_lccostier.sort_values("order")

# COMMAND ----------

lccos_imx_imxtier = tierPercent(demo_active_labeled=imx_demo_active_labeled,overlap_label=lccos_label,vip_col='vip_main_no',tier_col='viptyp_desc_unique')
lccos_imx_imxtier['order']=pd.Series(index=["ICARD Silver","ICARD Gold","ICARD Diamond","ICARD Pre-member","OTHERS"],data = [1,2,3,4,5])
lccos_imx_imxtier.sort_values("order")

# COMMAND ----------

lc_jb_lctier = tierPercent(demo_active_labeled=lc_demo_active_labeled,overlap_label=jb_label,vip_col='vip_no',tier_col='CARD_TYPE_updated')
lc_jb_lctier['order']=pd.Series(index=["Beauty+","Privilege","Gold","Platinum","Diamond"],data = [1,2,3,4,5])
lc_jb_lctier .sort_values("order")

# COMMAND ----------

lc_jb_jbtier = tierPercent(demo_active_labeled=jb_demo_active_labeled,overlap_label=lc_label,vip_col='vip_main_no',tier_col='viptyp_desc_unique')
lc_jb_jbtier['order']=pd.Series(index=["JB ESSENTIAL","JB ESTEEME","JB1","OTHERS"],data = [1,2,3,4])
lc_jb_jbtier.sort_values('order')

# COMMAND ----------

lccos_jb_lccostier = tierPercent(demo_active_labeled=lccos_demo_active_labeled,overlap_label=jb_label,vip_col='vip_no',tier_col='CARD_TYPE_updated')
lccos_jb_lccostier['order']=pd.Series(index=["Beauty+","Privilege","Gold","Platinum","Diamond"],data = [1,2,3,4,5])
lccos_jb_lccostier .sort_values("order")

# COMMAND ----------

lccos_jb_jbtier = tierPercent(demo_active_labeled=jb_demo_active_labeled,overlap_label=lccos_label,vip_col='vip_main_no',tier_col='viptyp_desc_unique')
lccos_jb_jbtier['order']=pd.Series(index=["JB ESSENTIAL","JB ESTEEME","JB1","OTHERS"],data = [1,2,3,4])
lccos_jb_jbtier.sort_values('order')

# COMMAND ----------

imx_jb_imxtier = tierPercent(demo_active_labeled=imx_demo_active_labeled,overlap_label=jb_label,vip_col='vip_main_no',tier_col='viptyp_desc_unique')
imx_jb_imxtier['order']=pd.Series(index=["ICARD Silver","ICARD Gold","ICARD Diamond","ICARD Pre-member","OTHERS"],data = [1,2,3,4,5])
imx_jb_imxtier.sort_values('order')

# COMMAND ----------

imx_jb_jbtier = tierPercent(demo_active_labeled=jb_demo_active_labeled,overlap_label=imx_label,vip_col='vip_main_no',tier_col='viptyp_desc_unique')
imx_jb_jbtier['order']=pd.Series(index=["JB ESSENTIAL","JB ESTEEME","JB1","OTHERS"],data = [1,2,3,4])
imx_jb_jbtier.sort_values('order')

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

lccos_imx_pp = pricepoint_table(
                     sales_scope_labeled1=lccos_sales_scope_labeled,
                     sales_scope_labeled2=imx_sales_scope_labeled,
                     overlap_label1=lccos_label,
                     overlap_label2=imx_label,
                     sales_amt_col1='amt_hkd',
                     sales_amt_col2='net_amt_hkd',
                     qty_col1='dtl_qty',
                     qty_col2='sold_qty')

lc_jb_pp = pricepoint_table(
                     sales_scope_labeled1=lc_sales_scope_labeled,
                     sales_scope_labeled2=jb_sales_scope_labeled2,
                     overlap_label1=lc_label,
                     overlap_label2=jb_label,
                     sales_amt_col1='amt_hkd',
                     sales_amt_col2='net_amt_hkd',
                     qty_col1='dtl_qty',
                     qty_col2='sold_qty')
lccos_jb_pp = pricepoint_table(
                     sales_scope_labeled1=lccos_sales_scope_labeled,
                     sales_scope_labeled2=jb_sales_scope_labeled2,
                     overlap_label1=lccos_label,
                     overlap_label2=jb_label,
                     sales_amt_col1='amt_hkd',
                     sales_amt_col2='net_amt_hkd',
                     qty_col1='dtl_qty',
                     qty_col2='sold_qty')

imx_jb_pp = pricepoint_table(
                     sales_scope_labeled1=imx_sales_scope_labeled,
                     sales_scope_labeled2=jb_sales_scope_labeled2,
                     overlap_label1=imx_label,
                     overlap_label2=jb_label,
                     sales_amt_col1='net_amt_hkd',
                     sales_amt_col2='net_amt_hkd',
                     qty_col1='sold_qty',
                     qty_col2='sold_qty')

# COMMAND ----------

lccos_imx_pp

# COMMAND ----------

lc_imx_pp

# COMMAND ----------

lccos_jb_pp

# COMMAND ----------

lc_jb_pp

# COMMAND ----------

imx_sales_scope_labeled.filter(imx_sales_scope_labeled[jb_label] == 1).select('arti_cate_name').distinct().show()

# COMMAND ----------

imx_jb_pp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concatenated

# COMMAND ----------

pp_concatenated = pd.concat([lc_imx_pp, lccos_imx_pp,lc_jb_pp,lccos_jb_pp,imx_jb_pp], keys=['LC-IMX', 'LCCOS - IMX','LC-JB','LCCOS-JB','IMX-JB'], axis=1)
pp_concatenated

# COMMAND ----------

lccos_imx_lccoscat = pricepoint_by_cat(sales_scope_labeled=lccos_sales_scope_labeled,overlap_label=imx_label,sales_col='amt_hkd',qty_col='dtl_qty',prefix=lccos_label)
lccos_imx_imxcat = pricepoint_by_cat(sales_scope_labeled=imx_sales_scope_labeled,overlap_label=lccos_label,sales_col='net_amt_hkd',qty_col='sold_qty',prefix=imx_label)
lccos_imx_cat = lccos_imx_lccoscat.join(lccos_imx_imxcat, on='arti_cate_name', how='outer').toPandas()[['arti_cate_name',lccos_label,imx_label]].set_index("arti_cate_name")
lccos_imx_cat

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

lc_imx_lcbrand=get_topbrand(sales_scope_labeled=lc_sales_scope_labeled,overlap_label=imx_label,sales_col='amt_hkd',brand_col='brand_desc',vip1='vip_no')
lc_imx_lcbrand.iloc[0:10,:]

# COMMAND ----------

lc_imx_lcbrand=get_topbrand_fashion(sales_scope_labeled=lc_sales_scope_labeled,overlap_label=imx_label,sales_col='amt_hkd',brand_col='brand_desc',vip1='vip_no')
lc_imx_lcbrand.iloc[0:10,:]

# COMMAND ----------

lc_imx_imxbrand=get_topbrand(sales_scope_labeled=imx_sales_scope_labeled,overlap_label=lc_label,sales_col='net_amt_hkd',brand_col='brand_desc',vip1='vip_main_no')
lc_imx_imxbrand

# COMMAND ----------

lccos_imx_lccosbrand=get_topbrand(sales_scope_labeled=lccos_sales_scope_labeled,overlap_label=imx_label,sales_col='amt_hkd',brand_col='brand_desc')
lccos_imx_lccosbrand.iloc[:10,:]

# COMMAND ----------

lccos_imx_imxbrand=get_topbrand(sales_scope_labeled=imx_sales_scope_labeled,overlap_label=lccos_label,sales_col='net_amt_hkd',brand_col='brand_desc')
lccos_imx_imxbrand

# COMMAND ----------

lc_jb_lcbrand=get_topbrand(sales_scope_labeled=lc_sales_scope_labeled,overlap_label=jb_label,sales_col='amt_hkd',brand_col='brand_desc',vip1='vip_no')
lc_jb_lcbrand.iloc[:10,:]

# COMMAND ----------

lc_jb_lcbrand=get_topbrand_fashion(sales_scope_labeled=lc_sales_scope_labeled,overlap_label=jb_label,sales_col='amt_hkd',brand_col='brand_desc',vip1='vip_no')
lc_jb_lcbrand.iloc[:10,:]

# COMMAND ----------

lc_jb_jbbrand=get_topbrand(sales_scope_labeled=jb_sales_scope_labeled,overlap_label=lc_label,sales_col='net_amt_hkd',brand_col='brand_desc',vip1='vip_main_no')
lc_jb_jbbrand

# COMMAND ----------

lccos_jb_lccosbrand=get_topbrand(sales_scope_labeled=lccos_sales_scope_labeled,overlap_label=jb_label,sales_col='amt_hkd',brand_col='brand_desc',vip1='vip_no')
lccos_jb_lccosbrand.iloc[:10,:]

# COMMAND ----------

lccos_jb_jbbrand=get_topbrand(sales_scope_labeled=jb_sales_scope_labeled,overlap_label=lccos_label,sales_col='net_amt_hkd',brand_col='brand_desc')
lccos_jb_jbbrand

# COMMAND ----------

imx_jb_imxbrand=get_topbrand(sales_scope_labeled=imx_sales_scope_labeled,overlap_label=jb_label,sales_col='net_amt_hkd',brand_col='brand_desc',vip1='vip_main_no')
imx_jb_imxbrand

# COMMAND ----------

imx_jb_jbbrand=get_topbrand(sales_scope_labeled=jb_sales_scope_labeled,overlap_label=imx_label,sales_col='net_amt_hkd',brand_col='brand_desc',vip1='vip_main_no')
imx_jb_jbbrand

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
    tier_matrix = overlap_percentage.groupBy(df1[f"{tier1}"]).pivot(df2[f"{tier2}"]).agg(F.first("pct"))

    # Display the resulting tier matrix
    return tier_matrix.toPandas()

# COMMAND ----------

tiermatrix(lc_demo_active_labeled,imx_demo_active_labeled,"vip_no","vip_main_no",lc_imx_customer,"lc","imx","CARD_TYPE_updated","viptyp_desc_unique")

# COMMAND ----------

tiermatrix(lc_demo_active_labeled,jb_demo_active_labeled,"vip_no","vip_main_no",lc_jb_customer,"lc","jb","CARD_TYPE_updated","viptyp_desc_unique")

# COMMAND ----------

vip1="vip_main_no"
vip2="vip_main_no"
matched_df=imx_jb_customer
prefix1="imx"
prefix2="jb"
tier1="viptyp_desc_unique"
tier2="viptyp_desc_unique"
df1=imx_demo_active_labeled.withColumn("imx_tier",imx_demo_active_labeled.viptyp_desc_unique)
df2=jb_demo_active_labeled.withColumn("jb_tier",jb_demo_active_labeled.viptyp_desc_unique)

joined_df = matched_df.join(df1, df1[vip1] == matched_df[f"{prefix1}_key"]).join(df2, df2[vip2] == matched_df[f"{prefix2}_key"])

# Step 3: Calculate the count of overlapping customers for each combination of Tier1 and Tier2
overlap_counts = joined_df.groupBy('imx_tier','jb_tier').agg(F.count("*").alias("overlap_count"))

# Step 4: Calculate the total count of customers for each Tier1 and Tier2 combination
total_counts = joined_df.count()

# Step 5: Calculate the percentage of overlapping customers for each combination
overlap_percentage = overlap_counts.withColumn("pct", F.col("overlap_count") / total_counts)

# Pivot the table to get the desired matrix format
from pyspark.sql import functions as F

tier_matrix = overlap_percentage.groupBy('imx_tier') \
    .pivot('jb_tier') \
    .agg(F.first("pct"))

# Display the resulting tier matrix
tier_matrix.toPandas()

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
    df = df.withColumn(f"{prefix}_tenure_group", F.when(F.col("tenure").between(1, 2), "tenure1-2")
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
tenurematrix(lc_demo_active_labeled, imx_demo_active_labeled, "vip_no", "vip_main_no", lc_imx_customer, 'lc', "imx","first_transaction_date",'VIP_FIRST_PUR_DATE')

# COMMAND ----------

# lc jb
tenurematrix(lc_demo_active_labeled, jb_demo_active_labeled, "vip_no", "vip_main_no", lc_jb_customer, 'lc', "jb","first_transaction_date",'VIP_FIRST_PUR_DATE')

# COMMAND ----------

tenurematrix(imx_demo_active_labeled, jb_demo_active_labeled, "vip_main_no", "vip_main_no", imx_jb_customer, 'imx', "jb",'VIP_FIRST_PUR_DATE','VIP_FIRST_PUR_DATE')

# COMMAND ----------

# MAGIC %md
# MAGIC # Gender

# COMMAND ----------

lc_demo_active_labeled.select('sex').toPandas().value_counts()

# COMMAND ----------

imx_demo_active_labeled.select("VIP_SEX").toPandas().value_counts()

# COMMAND ----------

matched_df = lc_imx_customer
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

matched_df = lc_jb_customer
df1=lc_demo_active_labeled
df2=jb_demo_active_labeled
prefix1="lc"
prefix2='jb'
vip1='vip_no'
vip2='vip_main_no'

joined_df = matched_df.join(df1, df1[vip1] == matched_df[f"{prefix1}_key"]).join(df2, df2[vip2] == matched_df[f"{prefix2}_key"])
count = joined_df.groupby('sex').agg(F.count("*").alias('overlap_count'))
total_count = joined_df.count()
pct=count.withColumn("pct",count['overlap_count']/total_count)
pct.toPandas()[['sex','pct']]

# COMMAND ----------

# imx jb
matched_df = imx_jb_customer
df1=imx_demo_active_labeled
df2=jb_demo_active_labeled
prefix1="imx"
prefix2='jb'
vip1='vip_main_no'
vip2='vip_main_no'

joined_df = matched_df.join(df1, df1[vip1] == matched_df[f"{prefix1}_key"]).join(df2, df2[vip2] == matched_df[f"{prefix2}_key"])
count = joined_df.groupby(df1['VIP_SEX']).agg(F.count("*").alias('overlap_count'))
total_count = joined_df.count()
pct=count.withColumn("pct",count['overlap_count']/total_count)
pct.toPandas()[['VIP_SEX','pct']]

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

#lc jb
sales_scope_labeled1=lc_sales_scope_labeled
overlap_label2=jb_label
sales_amt_col1='amt_hkd'
comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).groupby('bu_desc').agg(F.sum(sales_amt_col1).alias("amt"))
total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).agg(F.sum("amt_hkd")).collect()[0][0]
pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
pct.toPandas()[['bu_desc','pct']]

# COMMAND ----------

# MAGIC %md
# MAGIC # SOW by Cat ??

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct arti_cate_name

# COMMAND ----------

#lc-jb lc
sales_scope_labeled1=lc_sales_scope_labeled
overlap_label2=jb_label
sales_amt_col1='amt_hkd'
comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).filter(sales_scope_labeled1['bu_desc'] == "COS").groupby('arti_cate_name').agg(F.sum(sales_amt_col1).alias("amt"))
total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).filter(sales_scope_labeled1['bu_desc'] == "COS").agg(F.sum(sales_amt_col1)).collect()[0][0]
pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
pct.toPandas()[['arti_cate_name','pct']].sort_values("arti_cate_name")

# COMMAND ----------

#lc-jb jb
jb_sales_scope_labeled2 = spark.sql("select * from jb_sales_scope_labeled2")
sales_scope_labeled1=jb_sales_scope_labeled2
overlap_label2=lc_label
sales_amt_col1='net_amt_hkd'
comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).groupby('arti_cate_name').agg(F.sum(sales_amt_col1).alias("amt"))
total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).agg(F.sum(sales_amt_col1)).collect()[0][0]
pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
pct.toPandas()[['arti_cate_name','pct']].sort_values("arti_cate_name")

# COMMAND ----------

#lc-imx lc
sales_scope_labeled1=lc_sales_scope_labeled
overlap_label2=imx_label
sales_amt_col1='amt_hkd'
comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).filter(sales_scope_labeled1['bu_desc'] == "COS").groupby('arti_cate_name').agg(F.sum(sales_amt_col1).alias("amt"))
total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).filter(sales_scope_labeled1['bu_desc'] == "COS").agg(F.sum(sales_amt_col1)).collect()[0][0]
pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
pct.toPandas()[['arti_cate_name','pct']].sort_values("arti_cate_name")

# COMMAND ----------

#lc-imx imx
sales_scope_labeled1=imx_sales_scope_labeled
overlap_label2=lc_label
sales_amt_col1='net_amt_hkd'
comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).groupby('arti_cate_name').agg(F.sum(sales_amt_col1).alias("amt"))
total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).agg(F.sum(sales_amt_col1)).collect()[0][0]
pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
pct.toPandas()[['arti_cate_name','pct']].sort_values("arti_cate_name")

# COMMAND ----------

#imx-jb imx
sales_scope_labeled1=imx_sales_scope_labeled
overlap_label2=jb_label
sales_amt_col1='net_amt_hkd'
comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).groupby('arti_cate_name').agg(F.sum(sales_amt_col1).alias("amt"))
total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).agg(F.sum(sales_amt_col1)).collect()[0][0]
pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
pct.toPandas()[['arti_cate_name','pct']].sort_values("arti_cate_name")

# COMMAND ----------

#imx-jb jb
sales_scope_labeled1=jb_sales_scope_labeled2
overlap_label2=imx_label
sales_amt_col1='net_amt_hkd'
comp1_sales = sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).groupby('arti_cate_name').agg(F.sum(sales_amt_col1).alias("amt"))
total_sales=  sales_scope_labeled1.filter(sales_scope_labeled1[overlap_label2] == 1).agg(F.sum(sales_amt_col1)).collect()[0][0]
pct = comp1_sales.withColumn("pct",comp1_sales['amt']/total_sales)
pct.toPandas()[['arti_cate_name','pct']].sort_values("arti_cate_name")

# COMMAND ----------

# MAGIC %md
# MAGIC # Top COS Class

# COMMAND ----------

imx_sales_scope_labeled.columns

# COMMAND ----------

lc_sales_scope_labeled.columns

# COMMAND ----------

def get_topproduct(sales_scope_labeled,overlap_label,class_desc_col,vip1):
    overlapped = sales_scope_labeled.filter(sales_scope_labeled[overlap_label] == 1).filter(sales_scope_labeled['bu_desc']=="COS")
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

get_topproduct(sales_scope_labeled=lc_sales_scope_labeled,overlap_label=jb_label,class_desc_col='class_desc',vip1='vip_no')

# COMMAND ----------

get_topproduct(sales_scope_labeled=lc_sales_scope_labeled,overlap_label=imx_label,class_desc_col='class_desc',vip1='vip_no')

# COMMAND ----------

# MAGIC %md
# MAGIC # Penetration by Store

# COMMAND ----------

# lc imx
df1=imx_sales_scope_labeled
df1.groupBy(df1['shop_desc']).agg(F.mean(df1[lc_label]).alias('penetration')).toPandas().sort_values("penetration",ascending=False).iloc[:6,:]

# COMMAND ----------

# lc jb
df1=jb_sales_scope_labeled
df1.groupBy(df1['shop_desc']).agg(F.mean(df1[lc_label]).alias('penetration')).toPandas().sort_values("penetration",ascending=False).iloc[:6,:]

# COMMAND ----------

# imx jb
df1=jb_sales_scope_labeled
df1.groupBy(df1['shop_desc']).agg(F.mean(df1[imx_label]).alias('penetration')).toPandas().sort_values("penetration",ascending=False).iloc[:6,:]

# COMMAND ----------

# MAGIC %md
# MAGIC # Nationality

# COMMAND ----------

matched_df = lc_jb_customer
df1=lc_demo_active_labeled
df2=jb_demo_active_labeled
prefix1="lc"
prefix2='jb'
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

# MAGIC %sql
# MAGIC select * from jb_sales_scope_labeled
# MAGIC where vip_main_no in (select `Member No` from issue)
