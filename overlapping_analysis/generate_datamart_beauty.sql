-- Databricks notebook source
-- MAGIC %run /utils/spark_utils

-- COMMAND ----------

-- MAGIC %run /utils/gdm_utils

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Define Functions

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql.functions import trim
-- MAGIC import pandas as pd
-- MAGIC import os
-- MAGIC
-- MAGIC # preprocessing function to trim table
-- MAGIC def trim_df(source_table, output_table_name):
-- MAGIC     df = spark.sql(f"select * from {source_table}")
-- MAGIC     for column in df.columns:
-- MAGIC         df = df.withColumn(column, trim(df[column]))
-- MAGIC     df.createOrReplaceTempView(output_table_name)
-- MAGIC
-- MAGIC def update_tier(vip_no, join_bu_desc,tier):
-- MAGIC   if tier == "Tracking":
-- MAGIC     if vip_no[0] == '2':
-- MAGIC       return "Beauty+"
-- MAGIC     elif vip_no[0] =='3' and join_bu_desc == 'COS':
-- MAGIC       return "Beauty+"
-- MAGIC     elif vip_no[0] == '3' and join_bu_desc != 'COS':
-- MAGIC       return "Privilege"      
-- MAGIC     elif vip_no[0] != '2' and vip_no[0] != '3':
-- MAGIC       return "Privilege"
-- MAGIC   else:
-- MAGIC     return tier
-- MAGIC
-- MAGIC sqlContext.registerFunction('update_tier', update_tier)
-- MAGIC
-- MAGIC # function to clean phone number
-- MAGIC def cleanse_phone(p):
-- MAGIC   if p is None:
-- MAGIC     return ''
-- MAGIC   p = p.strip()
-- MAGIC   p = p.split("-")[-1]
-- MAGIC   if p.startswith("+852"):
-- MAGIC     p = p[4:]
-- MAGIC   if p.startswith("+86"):
-- MAGIC     p = p[3:]
-- MAGIC   if p.startswith("852"):
-- MAGIC     p = p[3:]
-- MAGIC   if p.startswith("86"):
-- MAGIC     p =p[2:]
-- MAGIC   return p
-- MAGIC
-- MAGIC sqlContext.registerFunction('CLEANSE_PHONE', cleanse_phone)
-- MAGIC
-- MAGIC
-- MAGIC # read arti category mapping file
-- MAGIC project_beauty_path = "/mnt/dev/overlapping_analysis/beauty/"
-- MAGIC load_csv_to_temp_table(os.path.join(project_beauty_path,"lc_beauty_mapping.csv"), "lc_beauty_mapping")
-- MAGIC load_csv_to_temp_table(os.path.join(project_beauty_path,"imx_beauty_mapping.csv"), "imx_beauty_mapping")
-- MAGIC load_csv_to_temp_table(os.path.join(project_beauty_path,"joyce_beauty_mapping.csv"), "joyce_beauty_mapping")
-- MAGIC project_fashion_path = "/mnt/dev/overlapping_analysis/fashion/"
-- MAGIC load_csv_to_temp_table(os.path.join(project_fashion_path,"lc_fashion_mapping.csv"), "lc_fashion_mapping")
-- MAGIC load_csv_to_temp_table(os.path.join(project_fashion_path,"imx_fashion_mapping.csv"), "imx_fashion_mapping")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Joyce Beauty

-- COMMAND ----------

-- Joyce beauty sales id
CREATE
OR REPLACE TEMPORARY VIEW jb_hk AS
SELECT
    DISTINCT a.sales_main_key
FROM
    imx_prd.dashboard_crm_gold.sales_sales_last3years a
WHERE
    shop_brand = "JB"
    AND region_key = 'HK'
    AND item_code != "JBDUMITMJBY"
    AND order_date != "2021-02-27"
    AND valid_tx_flag = 1
    AND isnull(void_flag) = 1
UNION

SELECT
    DISTINCT a.sales_main_key
FROM
    imx_prd.dashboard_crm_gold.sales_sales_last3years a
WHERE
    shop_brand = "JB"
    AND region_key = 'HK'
    AND item_code != "JBDUMITMJBY"
    AND order_date = "2021-02-27"
    AND invoice_remark LIKE "2021-02-27%"
    AND valid_tx_flag = 1
    AND isnull(void_flag) = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Sales Raw Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## IMX Beauty

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # select columns needed
-- MAGIC trim_df("imx_prd.dashboard_crm_gold.beauty_valid_tx0", "imx_beauty_sales_trimed")
-- MAGIC trim_df("imx_prd.imx_dw_train_silver.dbo_viw_lc_cs2k_item_item_maincat", "imx_cat_trimed") 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- add item_cat_desc
-- MAGIC create
-- MAGIC or replace temp view imx_sales_raw as
-- MAGIC select
-- MAGIC   a.*,
-- MAGIC   b.maincat_desc,
-- MAGIC   coalesce(c.arti_cate_name,"others") as arti_cate_name, -- self-defined category name. if null, then others
-- MAGIC   d.brand_desc
-- MAGIC from
-- MAGIC   imx_beauty_sales_trimed a
-- MAGIC   left join imx_cat_trimed b on a.item_cat = b.maincat_code
-- MAGIC   left join imx_beauty_mapping c using(maincat_desc)  
-- MAGIC   left join imx_prd.imx_dw_train_silver.dbo_viw_lc_xxx_brand_brand d on a.brand_code = d.brand_code;
-- MAGIC   ;
-- MAGIC   
-- MAGIC CREATE
-- MAGIC OR REPLACE TEMP VIEW imx_sales_raw_exclude_jb as -- exclude sales transaction in joyce beauty
-- MAGIC select
-- MAGIC   *
-- MAGIC from
-- MAGIC   imx_sales_raw
-- MAGIC where
-- MAGIC   sales_main_key not in (
-- MAGIC     select
-- MAGIC       distinct sales_main_key
-- MAGIC     from
-- MAGIC       jb_hk
-- MAGIC   )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Joyce Beauty

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW jb_sales_raw as -- select sales transaction in joyce beauty
select
  A.* EXCEPT (A.arti_cate_name),
  M.arti_cate_name
from
  imx_sales_raw A -- this table is imx table after preprocessing
  left join jb_cat_mapping M USING (item_subcat_desc) 
  right join jb_hk C using (sales_main_key) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## LC ALL EXCEPT JB

-- COMMAND ----------

-- MAGIC %py
-- MAGIC trim_df("lc_prd.crm_db_neo_silver.dbo_v_sales_dtl", "lc_sales_trimed")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create
-- MAGIC or replace temp view lcjg_sales_raw as
-- MAGIC select
-- MAGIC   a.*,
-- MAGIC   CONCAT(
-- MAGIC     SUBSTR(a.order_date, 1, 4),
-- MAGIC     '-',
-- MAGIC     SUBSTR(a.order_date, 5, 2),
-- MAGIC     '-',
-- MAGIC     SUBSTR(a.order_date, 7, 2)
-- MAGIC   ) AS formatted_order_date,
-- MAGIC   -- format order_date yyyyMMdd as yyyy-MM-dd
-- MAGIC   b.vat_region_name, -- add region name column
-- MAGIC   Case when bu_desc ="COS" then coalesce(c.arti_cate_name, "others") else bu_desc end as arti_cate_name
-- MAGIC from
-- MAGIC   lc_sales_trimed a
-- MAGIC   left join lcjg.lcgold.store_dim b on a.loc_code_neo = b.store_id
-- MAGIC   left join lc_cat_mapping c using (category_desc);
-- MAGIC   
-- MAGIC CREATE
-- MAGIC OR REPLACE TEMP VIEW lc_sales_raw_exclude_jb AS -- exclude sales transaction in joyce beauty
-- MAGIC SELECT
-- MAGIC   *
-- MAGIC FROM
-- MAGIC   lcjg_sales_raw
-- MAGIC WHERE
-- MAGIC   brand_desc not like '%JOYCE%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # LC exclude Aveda

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW lc_sales_raw_exclude_jb_aveda AS -- exclude sales transaction in joyce beauty
SELECT
  *
FROM
  lc_sales_raw_exclude_jb
WHERE
  brand_desc  not like "%AVEDA%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## LC Beauty

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW lccos_sales_raw AS -- only include cos
SELECT *  FROM lc_sales_raw_exclude_jb
where bu_desc="COS"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Demographic Raw-Cleaned table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## IMX

-- COMMAND ----------

-- MAGIC %py
-- MAGIC trim_df(
-- MAGIC     "imx_prd.imx_dw_train_silver.dbo_viw_lc_sales_vip", "imx_demo_member"
-- MAGIC )

-- COMMAND ----------

-- add vip type (tier) description
CREATE
OR REPLACE TEMP VIEW imx_demo_raw0 AS
SELECT DISTINCT
  a.* EXCEPT (VIP_TYPE),
  CASE
    WHEN b.viptyp_desc in 
    (
    -- for jb customer, JB viptype is dominating
    "JB1",
    "JB ESTEEME",
    "JB ESSENTIAL",
    -- for imx exclude jb customers, ICARD viptype is dominating. 
    "ICARD Pre-member",
    "ICARD Silver",
    "ICARD Gold",
    "ICARD Diamond",
    "ICARD Diamond - Taiwan")
    THEN viptyp_desc
    ELSE "OTHERS"
  END AS viptyp_desc_cleaned
FROM
  imx_demo_member a
  LEFT JOIN imx_prd.imx_dw_train_silver.dbo_viw_lc_sales_viptyp b ON a.vip_type = b.viptyp_code
WHERE
  b.REGION_KEY = "HK"; -- viptyp_code map with different viptyp_desc, so need to select region


-- add age group
CREATE
OR REPLACE TEMP VIEW imx_demo_raw AS
select 
a.* EXCEPT (VIP_AGEGRP),
b.VIP_AGEGRP
from imx_demo_raw0 a
left join imx_prd.imx_dw_train_silver.dbo_viw_lc_sales_vip b
USING ( VIP_MAIN_NO);

CREATE
OR REPLACE TEMP VIEW imx_demo_cleaned AS -- only keep 1 record of viptyp. If has multiple, then do not keep "Others"
SELECT DISTINCT
-- take MAX because 1 vip_main_no could has multiple records, with different birthday,email, vip_type data. Take MAX to take non-null value among them. 
  vip_main_no,
  CLEANSE_PHONE(MAX(vip_tel_2)) AS vip_tel_cleaned, --preprocessing for match
  LOWER(MAX(vip_email)) AS vip_email_lower,  --preprocessing for match
  MAX(vip_birth_IYY) AS vip_birth_IYY,
  MAX(vip_birth_IMM) AS vip_birth_IMM,
  MAX(vip_birth_IDD) AS vip_birth_IDD,
  MAX(VIP_SEX) AS VIP_SEX ,
  CAST(MAX(VIP_FIRST_PUR_DATE) as DATE) AS VIP_FIRST_PUR_DATE,
  CAST(MAX(VIP_ISSUE_DATE) as DATE) AS VIP_ISSUE_DATE,
  MIN(viptyp_desc_cleaned) as viptyp_desc_unique, -- take Min to choose ICARD>JB>OTHERS as final viptyp_desc
  MAX(REGION_KEY) as nationality
 FROM
  imx_demo_raw
GROUP BY
  vip_main_no --see vip_main_no = 01H0010822 fro example

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## LC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC trim_df(
-- MAGIC     "lcjg.lcsilver.adw_obiee_dmt_crm_member_reachable", "lcjg_demo_member"
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### preprocess tier

-- COMMAND ----------

-- replace 3999-12-31 as 1999-12-21
create
or replace temp view lcjg_demo_upd_card as
select distinct *, 
CASE WHEN card_start_date = "3999-12-31" then "1999-12-31" else  card_start_date end as card_start_date_updated
from
  lcjg_demo_member;


create
or replace temp view lcjg_demo_cleaned as
select distinct
   -- take MAX because 1 vip_no can has multiple records
    vip_no,
    CLEANSE_PHONE(MAX(phone_no)) as phone_no_cleaned,  -- preprocessing phone and email for matching
    LOWER(MAX(email)) as email_lower,
    MAX(sex) as sex,
    MAX(birth_date) as birth_date,
    MAX(card_start_date_updated) as card_start_date_updated , -- string: yyyy-mm-dd
    MAX(join_bu_desc) as join_bu_desc,
    MAX(first_transaction_date) as first_transaction_date, -- string: yyyy-mm-dd
    MAX(update_tier(vip_no,join_bu_desc,CARD_TYPE)) as CARD_TYPE_updated,
    MAX(COUNTRY_OF_RESIDENCE) As nationality
from
  lcjg_demo_upd_card
group by 
  vip_no

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Sales Scope Table
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from sqlalchemy import create_engine
-- MAGIC from sqlalchemy.orm import sessionmaker
-- MAGIC
-- MAGIC # select data within scope of time and region
-- MAGIC def scope_df(source_table, date_col, region_col, region, output_table_name):
-- MAGIC     df = spark.sql(
-- MAGIC         f"""
-- MAGIC                    select * from {source_table} 
-- MAGIC                    where {region_col} = {region} 
-- MAGIC                    and cast({date_col} as date) >="2022-04-01" 
-- MAGIC                    and cast({date_col} as date) <="2023-03-31" """
-- MAGIC     )
-- MAGIC     df.createOrReplaceTempView(output_table_name)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC scope_df(source_table='imx_sales_raw_exclude_jb', date_col='order_date',region_col="region_key",region="'HK'",output_table_name="imx_sales_scope")
-- MAGIC scope_df(source_table='lc_sales_raw_exclude_jb', date_col='formatted_order_date',region_col="vat_region_name",region="'HONG KONG'",output_table_name="lc_sales_scope")
-- MAGIC scope_df(source_table='lc_sales_raw_exclude_jb_aveda', date_col='formatted_order_date',region_col="vat_region_name",region="'HONG KONG'",output_table_name="lc_sales_scope_exclude_aveda")
-- MAGIC scope_df(source_table='lccos_sales_raw', date_col='formatted_order_date',region_col="vat_region_name",region="'HONG KONG'",output_table_name="lccos_sales_scope")
-- MAGIC scope_df(source_table='jb_sales_raw', date_col='order_date',region_col="region_key",region="'HK'",output_table_name="jb_sales_scope")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Demographic Active table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # select active member in scope
-- MAGIC def get_demo_active(demo_raw, sales_scope,join_key,output_table_name):
-- MAGIC     df=spark.sql(f"""
-- MAGIC     SELECT
-- MAGIC     DISTINCT A.*
-- MAGIC     FROM
-- MAGIC     {demo_raw} A
-- MAGIC     INNER JOIN {sales_scope} B using ({join_key})
-- MAGIC     """)
-- MAGIC     df.createOrReplaceTempView(output_table_name)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # get active member for jb
-- MAGIC get_demo_active(demo_raw="imx_demo_cleaned", sales_scope='jb_sales_scope',join_key='vip_main_no',output_table_name='jb_demo_active')
-- MAGIC # get active member for imx
-- MAGIC get_demo_active(demo_raw="imx_demo_cleaned", sales_scope='imx_sales_scope',join_key='vip_main_no',output_table_name='imx_demo_active')
-- MAGIC # get active member for lc
-- MAGIC get_demo_active(demo_raw="lcjg_demo_cleaned", sales_scope='lc_sales_scope',join_key='vip_no',output_table_name='lc_demo_active')
-- MAGIC # get active member for lc beauty
-- MAGIC get_demo_active(demo_raw="lcjg_demo_cleaned", sales_scope='lccos_sales_scope',join_key='vip_no',output_table_name='lccos_demo_active')
-- MAGIC # lc exclude aveda
-- MAGIC get_demo_active(demo_raw="lcjg_demo_cleaned", sales_scope='lc_sales_scope_exclude_aveda',join_key='vip_no',output_table_name='lc_demo_active_exclude_aveda')
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Overlap Demographic Table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import pyspark.sql.functions as F
-- MAGIC # match customer by either email or telephone
-- MAGIC def match( demo_df1,demo_df2,prefix1,prefix2,key1,key2,email_col1,email_col2,mobile_col1,mobile_col2): 
-- MAGIC   matched_df = spark.sql(
-- MAGIC     f"""
-- MAGIC   SELECT
-- MAGIC     DISTINCT A.{ key1 } AS { prefix1 }_key,
-- MAGIC     B.{ key2 } AS { prefix2 }_key,
-- MAGIC     "{prefix1}_{prefix2}" as  overlap_name,
-- MAGIC     A.{ email_col1 } AS { prefix1 }_email,
-- MAGIC     B.{ email_col2 } AS { prefix2 }_email,
-- MAGIC     A.{ mobile_col1 } AS { prefix1 }_mobile,
-- MAGIC     B.{ mobile_col2 } AS { prefix2 }_mobile
-- MAGIC   FROM
-- MAGIC     {demo_df1} A
-- MAGIC     INNER JOIN {demo_df2} B ON (
-- MAGIC       A.{ email_col1 } <> ''
-- MAGIC       AND A.{ email_col1 } IS NOT NULL
-- MAGIC       AND A.{ email_col1 } = B.{ email_col2 }
-- MAGIC     )
-- MAGIC     OR (
-- MAGIC       A.{mobile_col1} <> ''
-- MAGIC       AND A.{ mobile_col1 } IS NOT NULL
-- MAGIC       AND A.{mobile_col1} = B.{mobile_col2}
-- MAGIC     );
-- MAGIC   """)
-- MAGIC
-- MAGIC   matched_df.createOrReplaceTempView(f"{prefix1}_{prefix2}_customer")
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC def count_unique_customer(prefix1,prefix2):
-- MAGIC   matched_df = spark.sql(f"select * from {prefix1}_{prefix2}_customer")
-- MAGIC   # some customer may have multiple vip_no in a company. so drop the duplicate
-- MAGIC   matched_df_unique=matched_df.groupBy(f"{ prefix2 }_key").agg(F.min(f"{ prefix1 }_key"))
-- MAGIC   matched_df_unqiue=matched_df_unique.groupBy(f"min({ prefix1 }_key)").agg(F.min(f"{ prefix2 }_key"))
-- MAGIC   n_unique_customer = matched_df_unqiue.select(F.countDistinct(f"min({ prefix1 }_key)").alias('distinct_count')).first()['distinct_count']
-- MAGIC   print(f"{prefix1} - {prefix2} # unique active customer: {n_unique_customer}")
-- MAGIC   return n_unique_customer
-- MAGIC

-- COMMAND ----------

-- select * from lc_imx_customer

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # match lc and imx
-- MAGIC match( demo_df1='lc_demo_active',demo_df2='imx_demo_active',prefix1='lc',prefix2='imx',key1='vip_no',key2='vip_main_no',email_col1='email_lower',email_col2='vip_email_lower',mobile_col1='phone_no_cleaned',mobile_col2='vip_tel_cleaned')
-- MAGIC # match lc and jb
-- MAGIC match( demo_df1='lc_demo_active',demo_df2='jb_demo_active',prefix1='lc',prefix2='jb',key1='vip_no',key2='vip_main_no',email_col1='email_lower',email_col2='vip_email_lower',mobile_col1='phone_no_cleaned',mobile_col2='vip_tel_cleaned')
-- MAGIC # match imx and jb
-- MAGIC match( demo_df1='imx_demo_active',demo_df2='jb_demo_active',prefix1='imx',prefix2='jb',key1='vip_main_no',key2='vip_main_no',email_col1='vip_email_lower',email_col2='vip_email_lower',mobile_col1='vip_tel_cleaned',mobile_col2='vip_tel_cleaned')
-- MAGIC # match lc cos and imx
-- MAGIC match( demo_df1='lccos_demo_active',demo_df2='imx_demo_active',prefix1='lccos',prefix2='imx',key1='vip_no',key2='vip_main_no',email_col1='email_lower',email_col2='vip_email_lower',mobile_col1='phone_no_cleaned',mobile_col2='vip_tel_cleaned')
-- MAGIC # match lc cos and jb
-- MAGIC match( demo_df1='lccos_demo_active',demo_df2='jb_demo_active',prefix1='lccos',prefix2='jb',key1='vip_no',key2='vip_main_no',email_col1='email_lower',email_col2='vip_email_lower',mobile_col1='phone_no_cleaned',mobile_col2='vip_tel_cleaned')
-- MAGIC # match imx and lc_exclude_aveda
-- MAGIC match( demo_df1='lc_demo_active_exclude_aveda',demo_df2='imx_demo_active',prefix1='lcExc',prefix2='imx',key1='vip_no',key2='vip_main_no',email_col1='email_lower',email_col2='vip_email_lower',mobile_col1='phone_no_cleaned',mobile_col2='vip_tel_cleaned')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #count
-- MAGIC n_lc_imx = count_unique_customer(prefix1="lc",prefix2='imx')
-- MAGIC n_lc_jb = count_unique_customer(prefix1="lc",prefix2='jb')
-- MAGIC n_imx_jb = count_unique_customer(prefix1="imx",prefix2='jb')
-- MAGIC n_lccos_imx = count_unique_customer(prefix1="lccos",prefix2='imx')
-- MAGIC n_lccos_jb = count_unique_customer(prefix1="lccos",prefix2='jb')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC n_unique_cust ={
-- MAGIC "lc_imx":n_lc_imx,
-- MAGIC "lc_jb":n_lc_jb,
-- MAGIC "imx_jb":n_imx_jb,
-- MAGIC "lccos_imx":n_lccos_imx,
-- MAGIC "lccos_jb":n_lccos_jb
-- MAGIC }
-- MAGIC
-- MAGIC import os
-- MAGIC import json
-- MAGIC path= "/dbfs/mnt/dev/overlapping_analysis/n_unique_cust.json"
-- MAGIC
-- MAGIC json_string = json.dumps(n_unique_cust)
-- MAGIC
-- MAGIC # Write the JSON string to the file
-- MAGIC os.makedirs(os.path.dirname(path), exist_ok=True)
-- MAGIC with open(path, 'w') as file:
-- MAGIC     file.write(json_string)
-- MAGIC
-- MAGIC print(f"Dictionary stored in file: {path}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1 imx_key is possibly mapping with multiple lc_key

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Label Sales Scope and Demo Active Table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC # label overlapping customers in sales and demographic table
-- MAGIC
-- MAGIC def label_table(base_df, c1_c2_customer, left_key,right_key,label,output_table_name):  
-- MAGIC     df = spark.sql(f"""
-- MAGIC               WITH overlap AS ((SELECT DISTINCT {right_key} from {c1_c2_customer})) -- avoid duplicated key when joining with base_df
-- MAGIC               SELECT DISTINCT a.* ,
-- MAGIC               CASE WHEN b.{right_key} IS NOT NULL THEN 1 ELSE 0 END AS {label}
-- MAGIC               FROM {base_df} a 
-- MAGIC               LEFT JOIN overlap b
-- MAGIC               ON a.{left_key} = b.{right_key}
-- MAGIC               """)
-- MAGIC     df.createOrReplaceTempView(output_table_name)
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC lc_label = "LC"
-- MAGIC imx_label="IMX"
-- MAGIC jb_label="JB"
-- MAGIC lccos_label = "LCCOS"
-- MAGIC # label imx sales_scope table
-- MAGIC label_table(base_df='imx_sales_scope', c1_c2_customer='lc_imx_customer', left_key='vip_main_no',right_key="imx_key",label=lc_label,output_table_name="imx_sales_scope_labeled_lc")
-- MAGIC
-- MAGIC label_table(base_df='imx_sales_scope_labeled_lc', c1_c2_customer='imx_jb_customer', left_key='vip_main_no',right_key="imx_key",label=jb_label,output_table_name="imx_sales_scope_labeled_lc_jb")
-- MAGIC
-- MAGIC # label_table(base_df='imx_sales_scope_labeled_lc_jb', c1_c2_customer='lccos_imx_customer', left_key='vip_main_no',right_key="imx_key",label=lccos_label,output_table_name="imx_sales_scope_labeled")
-- MAGIC
-- MAGIC # label lc sales_scope table
-- MAGIC label_table(base_df='lc_sales_scope', c1_c2_customer='lc_imx_customer', left_key='vip_no',right_key="lc_key",label=imx_label,output_table_name="lc_sales_scope_labeled_imx")
-- MAGIC
-- MAGIC label_table(base_df='lc_sales_scope_labeled_imx', c1_c2_customer='lc_jb_customer', left_key='vip_no',right_key="lc_key",label=jb_label,output_table_name="lc_sales_scope_labeled")
-- MAGIC
-- MAGIC
-- MAGIC # label lccos sales_scope table
-- MAGIC # label_table(base_df='lccos_sales_scope', c1_c2_customer='lccos_imx_customer', left_key='vip_no',right_key="lccos_key",label=imx_label,output_table_name="lccos_sales_scope_labeled_imx")
-- MAGIC
-- MAGIC # label_table(base_df='lccos_sales_scope_labeled_imx', c1_c2_customer='lccos_jb_customer', left_key='vip_no',right_key="lccos_key",label=jb_label,output_table_name="lccos_sales_scope_labeled")
-- MAGIC
-- MAGIC # label jb sales_scope table
-- MAGIC label_table(base_df='jb_sales_scope', c1_c2_customer='lc_jb_customer', left_key='vip_main_no',right_key="jb_key",label=lc_label,output_table_name="jb_sales_scope_labeled_lc")
-- MAGIC
-- MAGIC label_table(base_df='jb_sales_scope_labeled_lc', c1_c2_customer='imx_jb_customer', left_key='vip_main_no',right_key="jb_key",label=imx_label,output_table_name="jb_sales_scope_labeled_lc_imx")
-- MAGIC
-- MAGIC # label_table(base_df='jb_sales_scope_labeled_lc_imx', c1_c2_customer='lccos_jb_customer', left_key='vip_main_no',right_key="jb_key",label=lccos_label,output_table_name="jb_sales_scope_labeled")
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # label imx demo_active table
-- MAGIC label_table(base_df='imx_demo_active', c1_c2_customer='lc_imx_customer', left_key='vip_main_no',right_key="imx_key",label=lc_label,output_table_name="imx_demo_active_labeled_lc")
-- MAGIC
-- MAGIC label_table(base_df='imx_demo_active_labeled_lc', c1_c2_customer='imx_jb_customer', left_key='vip_main_no',right_key="imx_key",label=jb_label,output_table_name="imx_demo_active_labeled")
-- MAGIC
-- MAGIC # label_table(base_df='imx_demo_active_labeled_lc_jb', c1_c2_customer='lccos_imx_customer', left_key='vip_main_no',right_key="imx_key",label=lccos_label,output_table_name="imx_demo_active_labeled")
-- MAGIC
-- MAGIC # label lc demo_active table
-- MAGIC label_table(base_df='lc_demo_active', c1_c2_customer='lc_imx_customer', left_key='vip_no',right_key="lc_key",label=imx_label,output_table_name="lc_demo_active_labeled_imx")
-- MAGIC
-- MAGIC label_table(base_df='lc_demo_active_labeled_imx', c1_c2_customer='lc_jb_customer', left_key='vip_no',right_key="lc_key",label=jb_label,output_table_name="lc_demo_active_labeled")
-- MAGIC
-- MAGIC
-- MAGIC # label lccos demo_active table
-- MAGIC # label_table(base_df='lccos_demo_active', c1_c2_customer='lccos_imx_customer', left_key='vip_no',right_key="lccos_key",label=imx_label,output_table_name="lccos_demo_active_labeled_imx")
-- MAGIC
-- MAGIC # label_table(base_df='lccos_demo_active_labeled_imx', c1_c2_customer='lccos_jb_customer', left_key='vip_no',right_key="lccos_key",label=jb_label,output_table_name="lccos_demo_active_labeled")
-- MAGIC
-- MAGIC # label jb demo_active table
-- MAGIC label_table(base_df='jb_demo_active', c1_c2_customer='lc_jb_customer', left_key='vip_main_no',right_key="jb_key",label=lc_label,output_table_name="jb_demo_active_labeled_lc")
-- MAGIC
-- MAGIC label_table(base_df='jb_demo_active_labeled_lc', c1_c2_customer='imx_jb_customer', left_key='vip_main_no',right_key="jb_key",label=imx_label,output_table_name="jb_demo_active_labeled")
-- MAGIC
-- MAGIC # label_table(base_df='jb_demo_active_labeled_lc_imx', c1_c2_customer='lccos_jb_customer', left_key='vip_main_no',right_key="jb_key",label=lccos_label,output_table_name="jb_demo_active_labeled")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Save Labeled Table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import os
-- MAGIC project_dir="/mnt/dev/overlapping_analysis/" # developing environment
-- MAGIC
-- MAGIC os.makedirs(project_dir,exist_ok=True)
-- MAGIC  
-- MAGIC
-- MAGIC sales_tables=[
-- MAGIC               # 'imx_sales_scope_labeled',
-- MAGIC               # "lc_sales_scope_labeled",
-- MAGIC             #   "lccos_sales_scope_labeled",
-- MAGIC             #   "jb_sales_scope_labeled"
-- MAGIC               ]
-- MAGIC demo_tables=[
-- MAGIC              "imx_demo_active_labeled",
-- MAGIC              "lc_demo_active_labeled",
-- MAGIC             #  "lccos_demo_active_labeled",
-- MAGIC              "jb_demo_active_labeled"
-- MAGIC             ]
-- MAGIC
-- MAGIC for t in demo_tables:
-- MAGIC     write_temp_table_to_parquet(t, os.path.join(project_dir,t), mode="overwrite")
-- MAGIC     print("save ["+t+"] in "+project_dir)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC for t in sales_tables:
-- MAGIC     write_temp_table_to_parquet(t, os.path.join(project_dir,t), mode="overwrite")
-- MAGIC     print("save ["+t+"] in "+project_dir)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC matched_tables=[
-- MAGIC             #  "lc_imx_customer",
-- MAGIC             #  "lccos_imx_customer",
-- MAGIC               # "lc_jb_customer",
-- MAGIC             #  "lccos_imx_customer",
-- MAGIC             #  "imx_jb_customer"
-- MAGIC             ]
-- MAGIC
-- MAGIC for t in matched_tables:
-- MAGIC     write_temp_table_to_parquet(t, os.path.join(project_dir,t), mode="overwrite")
-- MAGIC     print("save ["+t+"] in "+project_dir)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # list_alldf_size() # check the size of dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Appendix

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Overlap AB & NB

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC # brand_desc="NATURA BISSE"
-- MAGIC brand_desc="AUGUSTINUS BADER"
-- MAGIC
-- MAGIC spark.sql(
-- MAGIC f"""
-- MAGIC create or replace temp view cust_list_imx_2122 as
-- MAGIC select 
-- MAGIC distinct vip_main_no as vip_main_no_imx,
-- MAGIC 1 as imx_2122
-- MAGIC from imx_sales_raw_exclude_jb 
-- MAGIC where order_date >="2021-04-01" 
-- MAGIC and order_date <="2022-03-31"
-- MAGIC and upper(brand_desc) ="{brand_desc}"
-- MAGIC and region_key="HK"
-- MAGIC """
-- MAGIC )
-- MAGIC
-- MAGIC spark.sql(
-- MAGIC f"""
-- MAGIC create or replace temp view cust_list_jb_2122 as
-- MAGIC select 
-- MAGIC distinct vip_main_no as vip_main_no_jb,
-- MAGIC 1 as jb_2122
-- MAGIC from jb_sales_raw A
-- MAGIC where order_date >="2021-04-01" 
-- MAGIC and order_date <="2022-03-31"
-- MAGIC and upper(brand_desc) ="{brand_desc}"
-- MAGIC and region_key="HK"
-- MAGIC """
-- MAGIC )
-- MAGIC
-- MAGIC spark.sql(
-- MAGIC f"""
-- MAGIC create or replace temp view cust_list_imx_2223 as
-- MAGIC select 
-- MAGIC distinct vip_main_no as vip_main_no_imx,
-- MAGIC 1 as imx_2223
-- MAGIC from imx_sales_raw_exclude_jb 
-- MAGIC where order_date >="2022-04-01" 
-- MAGIC and order_date <="2023-03-31"
-- MAGIC and upper(brand_desc) ="{brand_desc}"
-- MAGIC and region_key="HK"
-- MAGIC """
-- MAGIC )
-- MAGIC
-- MAGIC spark.sql(
-- MAGIC f"""
-- MAGIC create or replace temp view cust_list_jb_2223 as
-- MAGIC select 
-- MAGIC distinct vip_main_no as vip_main_no_jb,
-- MAGIC 1 as jb_2223
-- MAGIC from jb_sales_raw A
-- MAGIC where order_date >="2022-04-01" 
-- MAGIC and order_date <="2023-03-31"
-- MAGIC and upper(brand_desc) ="{brand_desc}"
-- MAGIC and region_key="HK"
-- MAGIC """
-- MAGIC )

-- COMMAND ----------

-- MAGIC %py
-- MAGIC customer_list=spark.sql(
-- MAGIC """
-- MAGIC select 
-- MAGIC distinct
-- MAGIC B.vip_main_no_imx,
-- MAGIC C.vip_main_no_jb,
-- MAGIC CASE when imx_2122=1 and jb_2122=1 then "IMX & JB overlap"
-- MAGIC when imx_2122=1 and jb_2122 is null then "IMX only"
-- MAGIC when imx_2122 is null and jb_2122=1 then "JB only"
-- MAGIC else "IMX & JB inactive"
-- MAGIC end as label_2122,
-- MAGIC CASE when imx_2223=1 and jb_2223=1 then "IMX & JB overlap"
-- MAGIC when imx_2223=1 and jb_2223 is null then "IMX only"
-- MAGIC when imx_2223 is null and jb_2223=1 then "JB only"
-- MAGIC else "IMX & JB inactive"
-- MAGIC end as label_2223
-- MAGIC from
-- MAGIC imx_jb_customer A
-- MAGIC full outer join  cust_list_imx_2122 B on A.imx_key = B.vip_main_no_imx
-- MAGIC full outer join  cust_list_jb_2122 C on A.jb_key = C.vip_main_no_jb
-- MAGIC left join  cust_list_imx_2223 D on B.vip_main_no_imx = D.vip_main_no_imx -- how many 2122 customer remain in 2223
-- MAGIC left join  cust_list_jb_2223 E on C.vip_main_no_jb = E.vip_main_no_jb
-- MAGIC where (B.vip_main_no_imx is not null or C.vip_main_no_jb is not null)
-- MAGIC """
-- MAGIC ).toPandas()
-- MAGIC customer_list

-- COMMAND ----------

-- MAGIC %py
-- MAGIC customer_list['vip']=1
-- MAGIC output=customer_list[['label_2122','label_2223','vip']].groupby(['label_2122','label_2223']).count().unstack()
-- MAGIC output

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Issue

-- COMMAND ----------

-- MAGIC %py
-- MAGIC issue = pd.read_csv( os.path.join(project_path , "jb_new_joiner_issue_loc.csv"))
-- MAGIC
-- MAGIC spark_df = spark.createDataFrame(issue)
-- MAGIC # Register the Spark DataFrame as a temporary table
-- MAGIC spark_df.createOrReplaceTempView("issue")
-- MAGIC issue

-- COMMAND ----------

select distinct `Issue Location`, `Issue Location Name` from issue

-- COMMAND ----------

--("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") --lc jb store

-- COMMAND ----------

select count(distinct `Member No`) from issue

-- COMMAND ----------

select count(distinct vip_main_no) from jb_sales_raw S
right join issue on S.vip_main_no = issue.`Member No`
where order_date >="2022-04-01"
and order_date <="2023-03-31"
and `Issue Location` in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") -- join not in lC JB store

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import os
-- MAGIC import pandas as pd
-- MAGIC project_dir="/mnt/dev/overlapping_analysis/" # developing environment
-- MAGIC for t in ["jb_demo_active_labeled","jb_sales_scope_labeled"]:
-- MAGIC     load_parquet_to_temp_table(os.path.join(project_dir,t),t )
-- MAGIC
-- MAGIC jb_demo_active_labeled = spark.sql('select * from jb_demo_active_labeled')
-- MAGIC jb_sales_scope_labeled=spark.sql('select * from jb_sales_scope_labeled')

-- COMMAND ----------

select count(distinct vip_main_no) from jb_sales_scope_labeled S
right join issue on S.vip_main_no = issue.`Member No`

-- COMMAND ----------

select count(distinct vip_main_no) from jb_sales_scope_labeled S
right join issue on S.vip_main_no = issue.`Member No`
and `Issue Location` in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") -- join in lC JB store
and loc_code_neo in 

-- COMMAND ----------

select count(distinct vip_main_no) from jb_sales_scope_labeled S
right join issue on S.vip_main_no = issue.`Member No`
and `Issue Location` not in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") -- join not in lC JB store

-- COMMAND ----------

select count(distinct vip_main_no) from jb_sales_scope_labeled S
right join issue on S.vip_main_no = issue.`Member No`
and `Issue Location` not in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") -- join in JBY
and shop_code not in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") ---  shop in JBY

-- COMMAND ----------

select count(distinct vip_main_no) from jb_sales_scope_labeled S
right join issue on S.vip_main_no = issue.`Member No`
and `Issue Location` not in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") -- join in JBY
and shop_code in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") ---  shop in LCJB

-- COMMAND ----------

select count(*) from (
select distinct vip_main_no from jb_sales_scope_labeled S
right join issue on S.vip_main_no = issue.`Member No`
and `Issue Location` not in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") -- join in JBY
and shop_code not in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") ---  shop in JBY

intersect 

select distinct vip_main_no from jb_sales_scope_labeled S
right join issue on S.vip_main_no = issue.`Member No`
and `Issue Location` not in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") -- join in JBY
and shop_code in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") ---  shop in LCJB
)

-- COMMAND ----------

select count(distinct vip_main_no) from jb_sales_scope_labeled S
right join issue on S.vip_main_no = issue.`Member No`
and `Issue Location`  in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") -- join in LCJB
and shop_code  in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") ---  shop in LCJB

-- COMMAND ----------

select count(distinct vip_main_no) from jb_sales_scope_labeled S
right join issue on S.vip_main_no = issue.`Member No`
and `Issue Location`  in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") -- join in LCJB
and shop_code  not in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") ---  shop in JBY

-- COMMAND ----------

select count(*) from (
select distinct vip_main_no from jb_sales_scope_labeled S
right join issue on S.vip_main_no = issue.`Member No`
and `Issue Location`  in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") -- join in JBY
and shop_code not in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") ---  shop in JBY

intersect 

select distinct vip_main_no from jb_sales_scope_labeled S
right join issue on S.vip_main_no = issue.`Member No`
and `Issue Location`  in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") -- join in JBY
and shop_code in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") ---  shop in LCJB
)

-- COMMAND ----------

select count(distinct vip_main_no) from jb_sales_scope_labeled S
right join issue on S.vip_main_no = issue.`Member No`
and `Issue Location` not in ("FMSHKG01","FMSHKG02","JBSHKG10","JBSHKG05","JBSHKG09","JBSHKG11") -- join not in lC JB store
and imx=0 
and lc = 0
