-- Databricks notebook source
-- MAGIC %run /utils/spark_utils

-- COMMAND ----------

-- MAGIC %run /utils/gdm_utils

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
-- MAGIC ## IMX Fasion Sales

-- COMMAND ----------

select * from imx_prd.imx_dw_train_silver.dbo_viw_lc_sales_sales

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # trim
-- MAGIC trim_df("imx_prd.imx_dw_train_silver.dbo_viw_lc_sales_sales","imx_sales_trimed")
-- MAGIC trim_df("imx_prd.imx_dw_train_silver.dbo_viw_lc_xxx_brand_brand","imx_brand_trimed")
-- MAGIC trim_df("imx_prd.imx_dw_train_silver.dbo_viw_lc_sales_vip_masked","imx_vip_trimed")
-- MAGIC trim_df("imx_prd.imx_dw_train_silver.dbo_viw_lc_cs2k_item_sku","imx_item_trimed")
-- MAGIC trim_df("imx_prd.imx_dw_train_silver.dbo_viw_lc_cs2k_inv_location","imx_loc_trimed")
-- MAGIC trim_df("imx_prd.imx_dw_train_silver.dbo_viw_lc_cs2k_item_item_maincat ","imx_cat_trimed")
-- MAGIC

-- COMMAND ----------

-- cleaned sku table
CREATE
OR REPLACE TEMPORARY VIEW clean_sku0 AS
SELECT
    --sku_key,
    item_code,
    item_desc,
    item_desc_c,
    item_cat,
    item_sub_cat,
    item_product_line,
    item_product_line_desc,
    brand_code,
    retail_price_hk,
    retail_price_tw,
    CAST(last_modified_date AS DATE) AS last_modified_date
FROM
    imx_item_trimed;

-- take last_modified_date
CREATE
OR REPLACE TEMPORARY VIEW clean_sku1 AS
SELECT
    DISTINCT *
FROM
    clean_sku0;

CREATE
OR REPLACE TEMPORARY VIEW clean_sku AS
SELECT
    a.*
FROM
    clean_sku1 a
    INNER JOIN (
        SELECT
            item_code,
            MAX(last_modified_date) AS MaxDateTime
        FROM
            clean_sku1
        GROUP BY
            item_code
    ) b ON a.item_code = b.item_code
    AND a.last_modified_date = b.MaxDateTime;


-- cleaned sales_vip
CREATE
OR REPLACE TEMPORARY VIEW vip0 AS
SELECT
    DISTINCT vip_no,
    MIN(vip_main_no) as vip_main_no
FROM
    imx_vip_trimed
WHERE
    isnull(vip_main_no) = 0
GROUP BY
    vip_no;

CREATE
OR REPLACE TEMPORARY VIEW vip AS
SELECT
    DISTINCT *
FROM
    vip0;

-- COMMAND ----------

select distinct prod_brand,b.brand_desc 
from imx_prd.imx_dw_train_silver.dbo_viw_lc_sales_sales a
left join imx_prd.imx_dw_train_silver.dbo_viw_lc_xxx_brand_brand b on trim(a.prod_brand)=trim(b.brand_code)
-- where trim(brand_code) in ("CM","IM","IZ","SB","SC","PC")

-- COMMAND ----------

-- add brand_desc
-- add sales_main_key
--add vip_main_no
-- add shop_desc
-- add arti_cate_name
  create
  or replace temp view imx_sales_raw as
select
distinct 
  a.*,
  cast(a.sales_date as date) as sales_date_formatted,
  b.brand_desc,
  c.* except (item_code,last_modified_date),
  concat(A.shop_code, '-', A.invoice_no) AS sales_main_key,
  COALESCE(d.vip_main_no, a.vip_no) as vip_main_no, --if no vip_main_no, then use vip_no
  e.shop_desc,
  coalesce(g.arti_cate_name, "others") as arti_cate_name,
  g.`bu_desc ` as bu_desc,
  maincat_desc
from
  imx_sales_trimed a
  left join imx_brand_trimed b on 
  a.prod_brand=b.brand_code
  left join clean_sku c on a.item_code=c.item_code and a.prod_brand=b.brand_code
  LEFT JOIN vip d ON a.vip_no = d.vip_no
  left join imx_loc_trimed e using (shop_code)
  left join imx_cat_trimed f on c.item_cat = f.maincat_code
  left join imx_fashion_mapping g using(maincat_desc)

  where valid_tx_flag=1 -- select valid transaction
  and isnull(void_flag)=1
  ;


-- exclude jb
CREATE
OR REPLACE TEMP VIEW imx_sales_raw_exclude_jb as -- exclude sales transaction in joyce beauty
select
distinct 
  *
from
  imx_sales_raw
where
  sales_main_key not in (
    select
      distinct sales_main_key
    from
      jb_hk
  )

-- COMMAND ----------

select
distinct 
  *
from
  imx_sales_raw
where
  sales_main_key in (
    select
      distinct sales_main_key
    from
      jb_hk
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## LC Sales

-- COMMAND ----------

select * from lc_prd.crm_db_neo_silver.dbo_v_sales_dtl

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %sql
-- MAGIC   -- format order_date yyyyMMdd as yyyy-MM-dd
-- MAGIC -- add region
-- MAGIC -- add arti_cate_name
-- MAGIC create
-- MAGIC or replace temp view lcjg_sales0 as
-- MAGIC select
-- MAGIC distinct 
-- MAGIC   a.*,
-- MAGIC   to_date(order_date,"yyyyMMdd") AS formatted_order_date,
-- MAGIC   b.vat_region_name, 
-- MAGIC   coalesce(c.arti_cate_name, "others") as arti_cate_name
-- MAGIC from
-- MAGIC   lc_prd.crm_db_neo_silver.dbo_v_sales_dtl a
-- MAGIC   left join lcjg.lcgold.store_dim b on a.loc_code_neo = b.store_id
-- MAGIC   left join lc_fashion_mapping c using(bu_desc, category_desc)
-- MAGIC   ;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC trim_df("lcjg_sales0", "lcjg_sales_raw")

-- COMMAND ----------

-- exclude sales transaction in joyce beauty
CREATE
OR REPLACE TEMP VIEW lc_sales_raw_exclude_jb AS 
SELECT
  *
FROM
  lcjg_sales_raw
WHERE
  brand_desc not like '%JOYCE%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Demographic

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # # select and trim
-- MAGIC # imx_demo_cols = [
-- MAGIC #     "vip_main_no",
-- MAGIC #     "vip_tel_2",
-- MAGIC #     "vip_email",
-- MAGIC #     "vip_birth_IYY",
-- MAGIC #     "vip_birth_IMM",
-- MAGIC #     "vip_birth_IDD",
-- MAGIC #     "VIP_SEX",
-- MAGIC #     'VIP_FIRST_PUR_DATE', # to calculate tenure
-- MAGIC #     "VIP_ISSUE_DATE", # to calculate tenure
-- MAGIC #     "VIP_TYPE", # to calculate tier
-- MAGIC #     "REGION_KEY" # to calculate nationality
-- MAGIC # ]
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
    -- for imx customers, ICARD viptype is dominating. 
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
a.*,
VIP_AGEGRP
from imx_demo_raw0 a
left join imx_prd.imx_dw_train_silver.dbo_viw_lc_sales_vip
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
-- MAGIC # select and trim
-- MAGIC # LCJG_demo_cols = [
-- MAGIC #     "vip_no",
-- MAGIC #     "phone_no",
-- MAGIC #     "email",
-- MAGIC #     "sex",
-- MAGIC #     "birth_date",
-- MAGIC #     "phone_country_code",
-- MAGIC #     "card_start_date", ## has problem: some is 3999-12-31
-- MAGIC #     "CARD_TYPE",
-- MAGIC #     "join_bu_desc",
-- MAGIC #     "first_transaction_date", # string yyyy-mm-dd
-- MAGIC #     "COUNTRY_OF_RESIDENCE"
-- MAGIC # ]
-- MAGIC trim_df(
-- MAGIC     "lcjg.lcsilver.adw_obiee_dmt_crm_member_reachable", "lcjg_demo_member"
-- MAGIC )

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
-- MAGIC # Sales Scope

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
-- MAGIC                    where {region_col} in( {region}) 
-- MAGIC                    and cast({date_col} as date) >="2022-04-01" 
-- MAGIC                    and cast({date_col} as date) <="2023-03-31" """
-- MAGIC     )
-- MAGIC     df.createOrReplaceTempView(output_table_name)
-- MAGIC
-- MAGIC
-- MAGIC scope_df(source_table='imx_sales_raw_exclude_jb', date_col='sales_date_formatted',region_col="region_key",region=" 'HK','IH' ",output_table_name="imx_sales_scope")
-- MAGIC scope_df(source_table='lc_sales_raw_exclude_jb', date_col='formatted_order_date',region_col="vat_region_name",region="'HONG KONG'",output_table_name="lc_sales_scope")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Demographic Active

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
-- MAGIC
-- MAGIC
-- MAGIC # get active member for imx
-- MAGIC get_demo_active(demo_raw="imx_demo_cleaned", sales_scope='imx_sales_scope',join_key='vip_main_no',output_table_name='imx_demo_active')
-- MAGIC # get active member for lc
-- MAGIC get_demo_active(demo_raw="lcjg_demo_cleaned", sales_scope='lc_sales_scope',join_key='vip_no',output_table_name='lc_demo_active')

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
-- MAGIC match( demo_df1='lc_demo_active',demo_df2='imx_demo_active',prefix1='lc',prefix2='imx',key1='vip_no',key2='vip_main_no',email_col1='email_lower',email_col2='vip_email_lower',mobile_col1='phone_no_cleaned',mobile_col2='vip_tel_cleaned')
-- MAGIC
-- MAGIC # n_lc_imx = count_unique_customer(prefix1="lc",prefix2='imx')
-- MAGIC # n_unique_cust ={"lc_imx":n_lc_imx}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Save

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import os
-- MAGIC import json
-- MAGIC path= "/dbfs/mnt/dev/overlapping_analysis/fashion/n_unique_cust.json"
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

-- MAGIC %py
-- MAGIC import os
-- MAGIC import json
-- MAGIC
-- MAGIC print(f"Dictionary stored in file: {path}")
-- MAGIC
-- MAGIC ## save labeled sales and demo table
-- MAGIC project_dir="/mnt/dev/overlapping_analysis/fashion" # developing environment
-- MAGIC os.makedirs(project_dir,exist_ok=True)
-- MAGIC
-- MAGIC
-- MAGIC demo_tables=[
-- MAGIC              "imx_demo_active",
-- MAGIC             #  "lc_demo_active",
-- MAGIC              "lc_imx_customer"
-- MAGIC             ]
-- MAGIC
-- MAGIC for t in demo_tables:
-- MAGIC     write_temp_table_to_parquet(t, os.path.join(project_dir,t), mode="overwrite")
-- MAGIC     print("save ["+t+"] in "+project_dir)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC project_dir="/mnt/dev/overlapping_analysis/fashion" # developing environment
-- MAGIC sales_tables=[
-- MAGIC                'imx_sales_scope',
-- MAGIC             #    "lc_sales_scope",
-- MAGIC               ]
-- MAGIC for t in sales_tables:
-- MAGIC     write_temp_table_to_parquet(t, os.path.join(project_dir,t), mode="overwrite")
-- MAGIC     print("save ["+t+"] in "+project_dir)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Brand Info

-- COMMAND ----------

select 
brand_desc,
count(distinct vip_main_no) as n_member,
count(distinct sales_main_key) as n_trans,
min(cast(sales_date as date)) as min_date,
max(cast(sales_date as date)) as max_date
from imx_sales_raw_exclude_jb
where  brand_desc in ( 'Paul Smith Con' , 'Isabel Marant')
and region_key="HK"
group by brand_desc

-- COMMAND ----------

select 
brand_desc,
count(distinct vip_main_no) as n_member,
count(distinct sales_main_key) as n_trans,
min(cast(sales_date as date)) as min_date,
max(cast(sales_date as date)) as max_date,
year(cast(sales_date as date)) as year
from imx_sales_raw_exclude_jb
where  brand_desc in ('Isabel Marant')
and region_key="HK"
group by 1,6
