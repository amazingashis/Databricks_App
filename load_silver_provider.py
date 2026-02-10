# Databricks notebook source
# MAGIC %md
# MAGIC ###Create dm_service_provider table

# COMMAND ----------

from utils.Spark_Utils import *
from utils.DataFrame_Utils import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from configs.Env_Configs import database
from air.pipeline.stages import MatchSchema
from ng_data_quality import grammar
from air.pipeline.sinks import MergeSink
from utils.Column_Transformations import *
from pyspark.sql.types import *


# COMMAND ----------

schema = f"{database}.bronze"
spark = start_spark()
spark.sql("use " + schema)

# COMMAND ----------

# DBTITLE 1,list of tables available for provider
table_list = [
'provider_addr_stat_tab',
'provider_address1',
'provider_address1_usr',
'provider_addr_arr',
'provider_address1_usr_fieldname',
'provider_address1_usr_labels',
'provider_address2',
'provider_addrservices',
'provider_affi_arr',
'provider_contract_status',
'provider_credent',
'provider_dr_expert',
'provider_dr_lan',
'provider_dr_plan1',
'provider_dr_plan2',
'provider_dr_spec',
'provider_drname',
'provider_educ_arr',
'provider_educ_tab',
'provider_educate',
'provider_exp_tab',
'provider_facility',
'provider_hospital',
'provider_ins_plan',
'provider_lan_tab',
'provider_lic_arr',
'provider_loc_tab',
'provider_par_tab',
'provider_par_type',
'provider_rec_arr',
'provider_school',
'provider_services',
'provider_spec_tab',
'provider_sta_tab',
'provider_statelic',
'provider_stfstatu',
'provider_taxonomy',
'provider_train',
'provider_apos_tab',
'provider_u1_tab',
'provider_u2_tab',
'provider_u3_tab',
'provider_u3ins_tb',
'provider_u12_tab',
'provider_eth_tab',
'provider_taxo_arr'
]

# COMMAND ----------

dm_df = create_dataframe_from_table(spark, schema, table_list)

# COMMAND ----------

# DBTITLE 1,dataframe created for all the tables listed above
addr_stat_tab_df = dm_df["provider_addr_stat_tab" ]
address1_df = dm_df["provider_address1" ]
address1_usr_df = dm_df["provider_address1_usr" ]
addr_arr_df = dm_df["provider_addr_arr" ]
address1_usr_fieldname_df = dm_df["provider_address1_usr_fieldname" ]
address1_usr_labels_df = dm_df["provider_address1_usr_labels" ]
address2_df = dm_df["provider_address2"]
addrservices_df = dm_df["provider_addrservices" ]
affi_arr_df = dm_df["provider_affi_arr" ]
contract_status_df = dm_df["provider_contract_status" ]
credent_df = dm_df["provider_credent" ]
dr_expert_df = dm_df["provider_dr_expert" ]
dr_lan_df = dm_df["provider_dr_lan" ]
dr_plan1_df = dm_df["provider_dr_plan1" ]
dr_plan2_df = dm_df["provider_dr_plan2" ]
dr_spec_df = dm_df["provider_dr_spec" ]
drname_df = dm_df["provider_drname" ]
educ_arr_df = dm_df["provider_educ_arr" ]
educ_tab_df = dm_df["provider_educ_tab" ]
educate_df = dm_df["provider_educate" ]
exp_tab_df = dm_df["provider_exp_tab" ]
facility_df = dm_df["provider_facility" ]
hospital_df = dm_df["provider_hospital" ]
ins_plan_df = dm_df["provider_ins_plan" ]
lan_tab_df = dm_df["provider_lan_tab" ]
lic_arr_df = dm_df["provider_lic_arr" ]
loc_tab_df = dm_df["provider_loc_tab" ]
par_tab_df = dm_df["provider_par_tab" ]
par_type_df = dm_df["provider_par_type" ]
rec_arr_df = dm_df["provider_rec_arr" ]
school_df = dm_df["provider_school" ]
services_df = dm_df["provider_services" ]
spec_tab_df = dm_df["provider_spec_tab" ]
sta_tab_df = dm_df["provider_sta_tab" ]
statelic_df = dm_df["provider_statelic" ]
stfstatu_df = dm_df["provider_stfstatu"].filter(trim(col("active123")).isin(['!','.','1','7','A','AINN','C','D','PCNN','Q','SPI','V','W']))
taxonomy_df = dm_df["provider_taxonomy" ]
train_df = dm_df["provider_train" ]
apos_tab_df = dm_df["provider_apos_tab"]
u1_tab_df = dm_df["provider_u1_tab" ]
u2_tab_df = dm_df["provider_u2_tab"]
u3_tab_df = dm_df["provider_u3_tab"]
u3ins_tb_df = dm_df["provider_u3ins_tb"]
u12_tab_df = dm_df["provider_u12_tab"]
eth_tab_df = dm_df["provider_eth_tab"]
taxo_arr_df = dm_df["provider_taxo_arr"]

# COMMAND ----------

# DBTITLE 1,converting all the columns of above listed dataframe to lower case
addr_stat_tab_df = addr_stat_tab_df.toDF(*[c.lower() for c in addr_stat_tab_df.columns])
address1_df = address1_df.toDF(*[c.lower() for c in address1_df.columns])
address1_usr_df = address1_usr_df.toDF(*[c.lower() for c in address1_usr_df.columns])
addr_arr_df = addr_arr_df.toDF(*[c.lower() for c in addr_arr_df.columns])
address1_usr_fieldname_df = address1_usr_fieldname_df.toDF(*[c.lower() for c in address1_usr_fieldname_df.columns])
address1_usr_labels_df = address1_usr_labels_df.toDF(*[c.lower() for c in address1_usr_labels_df.columns])
address2_df = address2_df.toDF(*[c.lower() for c in address2_df.columns])
addrservices_df = addrservices_df.toDF(*[c.lower() for c in addrservices_df.columns])
affi_arr_df = affi_arr_df.toDF(*[c.lower() for c in affi_arr_df.columns])
contract_status_df = contract_status_df.toDF(*[c.lower() for c in contract_status_df.columns])
credent_df = credent_df.toDF(*[c.lower() for c in credent_df.columns])
dr_expert_df = dr_expert_df.toDF(*[c.lower() for c in dr_expert_df.columns])
dr_lan_df = dr_lan_df.toDF(*[c.lower() for c in dr_lan_df.columns])
dr_plan1_df = dr_plan1_df.toDF(*[c.lower() for c in dr_plan1_df.columns])
dr_plan2_df = dr_plan2_df.toDF(*[c.lower() for c in dr_plan2_df.columns])
dr_spec_df = dr_spec_df.toDF(*[c.lower() for c in dr_spec_df.columns])
drname_df = drname_df.toDF(*[c.lower() for c in drname_df.columns])
educ_arr_df = educ_arr_df.toDF(*[c.lower() for c in educ_arr_df.columns])
educ_tab_df = educ_tab_df.toDF(*[c.lower() for c in educ_tab_df.columns])
educate_df = educate_df.toDF(*[c.lower() for c in educate_df.columns])
exp_tab_df = exp_tab_df.toDF(*[c.lower() for c in exp_tab_df.columns])
facility_df = facility_df.toDF(*[c.lower() for c in facility_df.columns])
hospital_df = hospital_df.toDF(*[c.lower() for c in hospital_df.columns])
ins_plan_df = ins_plan_df.toDF(*[c.lower() for c in ins_plan_df.columns])
lan_tab_df = lan_tab_df.toDF(*[c.lower() for c in lan_tab_df.columns])
lic_arr_df = lic_arr_df.toDF(*[c.lower() for c in lic_arr_df.columns])
loc_tab_df = loc_tab_df.toDF(*[c.lower() for c in loc_tab_df.columns])
par_tab_df = par_tab_df.toDF(*[c.lower() for c in par_tab_df.columns])
par_type_df = par_type_df.toDF(*[c.lower() for c in par_type_df.columns])
rec_arr_df = rec_arr_df.toDF(*[c.lower() for c in rec_arr_df.columns])
school_df = school_df.toDF(*[c.lower() for c in school_df.columns])
services_df = services_df.toDF(*[c.lower() for c in services_df.columns])
spec_tab_df = spec_tab_df.toDF(*[c.lower() for c in spec_tab_df.columns])
sta_tab_df = sta_tab_df.toDF(*[c.lower() for c in sta_tab_df.columns])
statelic_df = statelic_df.toDF(*[c.lower() for c in statelic_df.columns])
stfstatu_df = stfstatu_df.toDF(*[c.lower() for c in stfstatu_df.columns])
taxonomy_df = taxonomy_df.toDF(*[c.lower() for c in taxonomy_df.columns])
train_df = train_df.toDF(*[c.lower() for c in train_df.columns])
apos_tab_df = apos_tab_df.toDF(*[c.lower() for c in apos_tab_df.columns])
u1_tab_df = u1_tab_df.toDF(*[c.lower() for c in u1_tab_df.columns])
u2_tab_df = u2_tab_df.toDF(*[c.lower() for c in u2_tab_df.columns])
u3_tab_df= u3_tab_df.toDF(*[c.lower() for c in u3_tab_df.columns])
u3ins_tb_df = u3ins_tb_df.toDF(*[c.lower() for c in u3ins_tb_df.columns])
u12_tab_df = u12_tab_df.toDF(*[c.lower() for c in u12_tab_df.columns])
eth_tab_df = eth_tab_df.toDF(*[c.lower() for c in eth_tab_df.columns])
taxo_arr_df = taxo_arr_df.toDF(*[c.lower() for c in taxo_arr_df.columns])

# COMMAND ----------

def create_renamed_df_with_alias(df, alias):
    # Generate new column names using the alias and the original column names
    new_column_names = [f"{alias}__{col}" for col in df.columns]
    # Create a new DataFrame with renamed columns
    renamed_df = df.toDF(*new_column_names)
    return renamed_df

# COMMAND ----------

drname_df = trim_string_columns(create_renamed_df_with_alias(drname_df, "drname_df"))
dr_expert_df = trim_string_columns(create_renamed_df_with_alias(dr_expert_df, "dr_expert_df"))
address1_df = trim_string_columns(create_renamed_df_with_alias(address1_df, "address1_df"))
address2_df = trim_string_columns(create_renamed_df_with_alias(address2_df, "address2_df"))
addr_arr_df = trim_string_columns(create_renamed_df_with_alias(addr_arr_df, "addr_arr_df"))
stfstatu_df = trim_string_columns(create_renamed_df_with_alias(stfstatu_df, "stfstatu_df"))
sta_tab_df = trim_string_columns(create_renamed_df_with_alias(sta_tab_df, "sta_tab_df"))
dr_plan1_df = trim_string_columns(create_renamed_df_with_alias(dr_plan1_df, "dr_plan1_df"))
contract_status_df = trim_string_columns(create_renamed_df_with_alias(contract_status_df, "contract_status_df"))
ins_plan_df = trim_string_columns(create_renamed_df_with_alias(ins_plan_df, "ins_plan_df"))
taxonomy_df = trim_string_columns(create_renamed_df_with_alias(taxonomy_df, "taxonomy_df"))
lic_arr_df = trim_string_columns(create_renamed_df_with_alias(lic_arr_df, "lic_arr_df"))
credent_df = trim_string_columns(create_renamed_df_with_alias(credent_df, "credent_df"))
statelic_df = trim_string_columns(create_renamed_df_with_alias(statelic_df, "statelic_df"))
educate_df = trim_string_columns(create_renamed_df_with_alias(educate_df, "educate_df"))
educ_tab_df = trim_string_columns(create_renamed_df_with_alias(educ_tab_df, "educ_tab_df"))
educ_arr_df = trim_string_columns(create_renamed_df_with_alias(educ_arr_df, "educ_arr_df"))
exp_tab_df = trim_string_columns(create_renamed_df_with_alias(exp_tab_df, "exp_tab_df"))
train_df = trim_string_columns(create_renamed_df_with_alias(train_df, "train_df"))
dr_lan_df = trim_string_columns(create_renamed_df_with_alias(dr_lan_df, "dr_lan_df"))
hospital_df = trim_string_columns(create_renamed_df_with_alias(hospital_df, "hospital_df"))
dr_spec_df = trim_string_columns(create_renamed_df_with_alias(dr_spec_df, "dr_spec_df"))
spec_tab_df = trim_string_columns(create_renamed_df_with_alias(spec_tab_df, "spec_tab_df"))
school_df = trim_string_columns(create_renamed_df_with_alias(school_df, "school_df"))
loc_tab_df = trim_string_columns(create_renamed_df_with_alias(loc_tab_df, "loc_tab_df"))
lan_tab_df =  trim_string_columns(create_renamed_df_with_alias(lan_tab_df, "lan_tab_df"))
par_tab_df =  trim_string_columns(create_renamed_df_with_alias(par_tab_df, "par_tab_df"))
u1_tab_df = trim_string_columns(create_renamed_df_with_alias(u1_tab_df, "u1_tab_df"))
u2_tab_df = trim_string_columns(create_renamed_df_with_alias(u2_tab_df, "u2_tab_df"))
u3_tab_df= trim_string_columns(create_renamed_df_with_alias(u3_tab_df, "u3_tab_df"))
u3ins_tb_df= trim_string_columns(create_renamed_df_with_alias(u3ins_tb_df, "u3ins_tb_df"))
rec_arr_df = trim_string_columns(create_renamed_df_with_alias(rec_arr_df, "rec_arr_df"))
apos_tab_df = trim_string_columns(create_renamed_df_with_alias(apos_tab_df, "apos_tab_df"))
affi_arr_df = trim_string_columns(create_renamed_df_with_alias(affi_arr_df, "affi_arr_df"))
u12_tab_df = trim_string_columns(create_renamed_df_with_alias(u12_tab_df, "u12_tab_df"))
eth_tab_df = trim_string_columns(create_renamed_df_with_alias(eth_tab_df, "eth_tab_df"))
taxo_arr_df = trim_string_columns(create_renamed_df_with_alias(taxo_arr_df, "taxo_arr_df"))

# COMMAND ----------

# MAGIC %md
# MAGIC Column-level Transformation udf functions

# COMMAND ----------

# DBTITLE 1,create map of define struct type
identifier_struct_type = StructType([
    StructField("reference_id", StringType()),
    StructField("identifier_type", StringType()),
    StructField("identifier", StringType()),
    StructField("identifier_source", StringType()),
    StructField("eff_date", StringType()),
    StructField("exp_date", StringType()),
])

# Define a UDF to create the map
def udf_create_map(identifiers):
    map_dict = {}
    # creates key based on the length of array, if less than 1 creates col else col_{array_index+1}
    if len(identifiers) == 1: 
        identifier = identifiers[0]
        map_dict[identifier.identifier_type] = identifier
    else:
        for i, identifier in enumerate(identifiers):
            map_dict[f"{identifier.identifier_type}_{i+1}"] = identifier
    return map_dict

# Register the UDF
map_udf = udf(udf_create_map, MapType(StringType(), identifier_struct_type))

# COMMAND ----------

# DBTITLE 1,PCP_ind and service_provider_role mapping
pcp_ind_struct = StructType([
    StructField("pcp_ind", BooleanType(), True),
    StructField("service_provider_role", StringType(), True)
])

def udf_pcp_ind(pcp_ind_array):
    pcp_ind = None
    service_provider_role = None

    if len(pcp_ind_array) == 1:
        pcp_ind_code = pcp_ind_array[0]
        if pcp_ind_code == "P":
            pcp_ind= True
            service_provider_role = "PCP"
        elif pcp_ind_code == "S":
            pcp_ind= False
            service_provider_role = "Specialist"
        elif pcp_ind_code == "B":
            pcp_ind= True
            service_provider_role = "Both PCP and Specialist"
        else:
            service_provider_role = None
    elif len(pcp_ind_array) > 1:
        pcp_ind = True
        service_provider_role = "Both PCP and Specialist"

    # Return an array of struct 
    return [(pcp_ind, service_provider_role)]

# Register the UDF
map_pcp_ind_role = udf(udf_pcp_ind, ArrayType(pcp_ind_struct))


# COMMAND ----------

address1_df = address1_df.withColumn("address1_df__zip_extension", substring('address1_df__zip', 7, 10))
address2_df = address2_df.withColumn("address2_df__substring_tax_id",substring("address2_df__tax_id",1,10))
address1_df = address1_df.withColumn("address1_df__zip", substring('address1_df__zip', 1, 5)) 

# COMMAND ----------

# DBTITLE 1,Join with address lookup table, addr_addr df
joined_addr1_arr_df = address1_df.join(
        addr_arr_df.select(
            col("addr_arr_df__cd"),
            col("addr_arr_df__txt")
        ), 
        (
            col("address1_df__sch_type") == col("addr_arr_df__cd")
        ), 
        "inner"
)

# COMMAND ----------

joined_addr1_arr_df = joined_addr1_arr_df.withColumn(
    "address_reference_id", 
        md5(concat
            (coalesce("address1_df__dr_id",lit('NA')),
             coalesce("address1_df__sch_type",lit('NA')),
             coalesce("address1_df__addr",lit('NA')),
             coalesce("address1_df__addr2",lit('NA')),
             coalesce("address1_df__city",lit('NA')),
             coalesce("address1_df__state",lit('NA')),
             coalesce("address1_df__zip",lit('NA')),
             coalesce("address1_df__link",lit('NA'))
                )
        ) 
)

# COMMAND ----------

joined_addr2_addr1_tax_df = joined_addr1_arr_df.join(
    address2_df.select(
        col("address2_df__substring_tax_id"),
        col("address2_df__l_address")
    ), 
    (
        col("address1_df__link") == col("address2_df__l_address")
    ), 
    "inner"
)
# .filter(col("address1_df__sch_type") == 'Y')

# COMMAND ----------

general_contact_details_array_df = joined_addr2_addr1_tax_df.groupBy(col("address1_df__dr_id").alias("general_contact_details__dr_id")).agg(
     sort_array(
            collect_list(
                struct(
                    col("address_reference_id").alias("reference_id"),
                    col("addr_arr_df__txt").alias("id"),
                    lit(None).alias("entity_type"),
                    col("address1_df__fax").alias("telecom_value"),
                    lit("fax").alias("telecom_type"),
                    lit(None).alias("telecom_use"),
                    lit(None).alias("telecom_rank"),
                    lit(None).alias("telecom_eff_date"),
                    lit(None).alias("telecom_exp_date"),
                    lit(None).alias("contact_name"),
                    lit(None).alias("contact_address"),
                    lit(None).alias("contact_telecom_value"),
                    lit(None).alias("contact_telecom_type"),
                    lit(None).alias("available_all_day"),
                    lit("sun").alias("available_days_of_week"),
                    when(col("address1_df__sun_s1") != "", col("address1_df__sun_s1")).otherwise(col("address1_df__sun_s2")).alias("available_start_time"),
                    when(col("address1_df__sun_e1") != "", col("address1_df__sun_e1")).otherwise(col("address1_df__sun_e2")).alias("available_end_time"),
                    lit(None).alias("intemediary_ind"),
                    lit(None).alias("intermediary_value"),
                    lit(None).alias("intermediary_type")
                )
            ), asc = False
        ).alias("Sunday"),
     sort_array(
            collect_list(
                struct(
                    col("address_reference_id").alias("reference_id"),
                    col("addr_arr_df__txt").alias("id"),
                    lit(None).alias("entity_type"),
                    col("address1_df__fax").alias("telecom_value"),
                    lit("fax").alias("telecom_type"),
                    lit(None).alias("telecom_use"),
                    lit(None).alias("telecom_rank"),
                    lit(None).alias("telecom_eff_date"),
                    lit(None).alias("telecom_exp_date"),
                    lit(None).alias("contact_name"),
                    lit(None).alias("contact_address"),
                    lit(None).alias("contact_telecom_value"),
                    lit(None).alias("contact_telecom_type"),
                    lit(None).alias("available_all_day"),
                    lit("mon").alias("available_days_of_week"),
                    when(col("address1_df__mon_s1") != "", col("address1_df__mon_s1")).otherwise(col("address1_df__mon_s2")).alias("available_start_time"),
                    when(col("address1_df__mon_e1") != "", col("address1_df__mon_e1")).otherwise(col("address1_df__mon_e2")).alias("available_end_time"),
                    lit(None).alias("intemediary_ind"),
                    lit(None).alias("intermediary_value"),
                    lit(None).alias("intermediary_type")
                )
            ), asc = False
        ).alias("Monday"),
     sort_array(
            collect_list(
                struct(
                    col("address_reference_id").alias("reference_id"),
                    col("addr_arr_df__txt").alias("id"),
                    lit(None).alias("entity_type"),
                    col("address1_df__fax").alias("telecom_value"),
                    lit("fax").alias("telecom_type"),
                    lit(None).alias("telecom_use"),
                    lit(None).alias("telecom_rank"),
                    lit(None).alias("telecom_eff_date"),
                    lit(None).alias("telecom_exp_date"),
                    lit(None).alias("contact_name"),
                    lit(None).alias("contact_address"),
                    lit(None).alias("contact_telecom_value"),
                    lit(None).alias("contact_telecom_type"),
                    lit(None).alias("available_all_day"),
                    lit("tue").alias("available_days_of_week"),
                    when(col("address1_df__tues_s1") != "", col("address1_df__tues_s1")).otherwise(col("address1_df__tues_s2")).alias("available_start_time"),
                    when(col("address1_df__tues_e1") != "", col("address1_df__tues_e1")).otherwise(col("address1_df__tues_e2")).alias("available_end_time"),
                    lit(None).alias("intemediary_ind"),
                    lit(None).alias("intermediary_value"),
                    lit(None).alias("intermediary_type")
                )
            ), asc = False
        ).alias("Tuesday"),
     sort_array(
            collect_list(
                struct(
                    col("address_reference_id").alias("reference_id"),
                    col("addr_arr_df__txt").alias("id"),
                    lit(None).alias("entity_type"),
                    col("address1_df__fax").alias("telecom_value"),
                    lit("fax").alias("telecom_type"),
                    lit(None).alias("telecom_use"),
                    lit(None).alias("telecom_rank"),
                    lit(None).alias("telecom_eff_date"),
                    lit(None).alias("telecom_exp_date"),
                    lit(None).alias("contact_name"),
                    lit(None).alias("contact_address"),
                    lit(None).alias("contact_telecom_value"),
                    lit(None).alias("contact_telecom_type"),
                    lit(None).alias("available_all_day"),
                    lit("wed").alias("available_days_of_week"),
                    when(col("address1_df__wednes_s1") != "", col("address1_df__wednes_s1")).otherwise(col("address1_df__wednes_s2")).alias("available_start_time"),
                    when(col("address1_df__wednes_e1") != "", col("address1_df__wednes_e1")).otherwise(col("address1_df__wednes_e2")).alias("available_end_time"),
                    lit(None).alias("intemediary_ind"),
                    lit(None).alias("intermediary_value"),
                    lit(None).alias("intermediary_type")
                )
            ), asc = False
        ).alias("Wednesday"),
     sort_array(
            collect_list(
                struct(
                    col("address_reference_id").alias("reference_id"),
                    col("addr_arr_df__txt").alias("id"),
                    lit(None).alias("entity_type"),
                    col("address1_df__fax").alias("telecom_value"),
                    lit("fax").alias("telecom_type"),
                    lit(None).alias("telecom_use"),
                    lit(None).alias("telecom_rank"),
                    lit(None).alias("telecom_eff_date"),
                    lit(None).alias("telecom_exp_date"),
                    lit(None).alias("contact_name"),
                    lit(None).alias("contact_address"),
                    lit(None).alias("contact_telecom_value"),
                    lit(None).alias("contact_telecom_type"),
                    lit(None).alias("available_all_day"),
                    lit("thu").alias("available_days_of_week"),
                    when(col("address1_df__thurs_s1") != "", col("address1_df__thurs_s1")).otherwise(col("address1_df__thurs_s2")).alias("available_start_time"),
                    when(col("address1_df__thurs_e1") != "", col("address1_df__thurs_e1")).otherwise(col("address1_df__thurs_e2")).alias("available_end_time"),
                    lit(None).alias("intemediary_ind"),
                    lit(None).alias("intermediary_value"),
                    lit(None).alias("intermediary_type")
                )
            ), asc = False
        ).alias("Thursday"),
     sort_array(
            collect_list(
                struct(
                    col("address_reference_id").alias("reference_id"),
                    col("addr_arr_df__txt").alias("id"),
                    lit(None).alias("entity_type"),
                    col("address1_df__fax").alias("telecom_value"),
                    lit("fax").alias("telecom_type"),
                    lit(None).alias("telecom_use"),
                    lit(None).alias("telecom_rank"),
                    lit(None).alias("telecom_eff_date"),
                    lit(None).alias("telecom_exp_date"),
                    lit(None).alias("contact_name"),
                    lit(None).alias("contact_address"),
                    lit(None).alias("contact_telecom_value"),
                    lit(None).alias("contact_telecom_type"),
                    lit(None).alias("available_all_day"),
                    lit("fri").alias("available_days_of_week"),
                    when(col("address1_df__fri_s1") != "", col("address1_df__fri_s1")).otherwise(col("address1_df__fri_s2")).alias("available_start_time"),
                    when(col("address1_df__fri_e1") != "", col("address1_df__fri_e1")).otherwise(col("address1_df__fri_e2")).alias("available_end_time"),
                    lit(None).alias("intemediary_ind"),
                    lit(None).alias("intermediary_value"),
                    lit(None).alias("intermediary_type")
                )
            ), asc = False
        ).alias("Friday"),
     sort_array(
            collect_list(
                struct(
                    col("address_reference_id").alias("reference_id"),
                    col("addr_arr_df__txt").alias("id"),
                    lit(None).alias("entity_type"),
                    col("address1_df__fax").alias("telecom_value"),
                    lit("fax").alias("telecom_type"),
                    lit(None).alias("telecom_use"),
                    lit(None).alias("telecom_rank"),
                    lit(None).alias("telecom_eff_date"),
                    lit(None).alias("telecom_exp_date"),
                    lit(None).alias("contact_name"),
                    lit(None).alias("contact_address"),
                    lit(None).alias("contact_telecom_value"),
                    lit(None).alias("contact_telecom_type"),
                    lit(None).alias("available_all_day"),
                    lit("sat").alias("available_days_of_week"),
                    when(col("address1_df__satur_s1") != "", col("address1_df__satur_s1")).otherwise(col("address1_df__satur_s2")).alias("available_start_time"),
                    when(col("address1_df__satur_e1") != "", col("address1_df__satur_e1")).otherwise(col("address1_df__satur_e2")).alias("available_end_time"),
                    lit(None).alias("intemediary_ind"),
                    lit(None).alias("intermediary_value"),
                    lit(None).alias("intermediary_type")
                )
            ), asc = False
        ).alias("Saturday")
)

# COMMAND ----------

general_contact_details_df = general_contact_details_array_df.select(
    sort_array(
        concat(
            coalesce(col("Sunday"), lit([])),
            coalesce(col("Monday"), lit([])),
            coalesce(col("Tuesday"), lit([])),
            coalesce(col("Wednesday"), lit([])),
            coalesce(col("Thursday"), lit([])),
            coalesce(col("Friday"), lit([])),
            coalesce(col("Saturday"), lit([]))
        )
    ).alias("address_general_contact_details"), 
    col("general_contact_details__dr_id")
)

# COMMAND ----------

# DBTITLE 1,training position
joined_train_hospital_apos_affi_df = train_df.join(
    hospital_df.select(
    col("hospital_df__cd"),
    col("hospital_df__txt"),
    col("hospital_df__subname"),
    col("hospital_df__addr"),
    col("hospital_df__addr2"),
    col("hospital_df__city"),
    col("hospital_df__state"),
    col("hospital_df__zip"),
    col("hospital_df__nation"),
    col("hospital_df__phone"),
    col("hospital_df__contact1"),
    col("hospital_df__contact2"),
    col("hospital_df__fax"),
    col("hospital_df__email"),
    col("hospital_df__note")
    ),
    (
        (col("train_df__cd") == col("hospital_df__cd"))
    ),
    "left"
).join(
    apos_tab_df.select(
    col("apos_tab_df__cd"),
    col("apos_tab_df__txt")
    ),
    (
        (col("train_df__position") == col("apos_tab_df__cd"))
    ),
    "left" 
).join(
    affi_arr_df.select(
    col("affi_arr_df__cd"),
    col("affi_arr_df__txt")
    ),
    (
        (col("train_df__sch_type") == col("affi_arr_df__cd"))
    ),
    "left"  
)

# COMMAND ----------

# DBTITLE 1,Aggregation of Train
joined_train_hospital_apos_affi_grouped_df = joined_train_hospital_apos_affi_df.groupBy(col("train_df__dr_id")).agg(
   sort_array(
        collect_list(
        struct(
                md5(concat(col("train_df__position"),col("train_df__cd"))).alias("reference_id"),
                lit("Training").alias("identifier_type"),
                col("train_df__position").alias("identifier"),
                lit("Provider_Train").alias("identifier_source"),             
                date_format(col("train_df__tdate"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("exp_date"),
                date_format(col("train_df__fdate"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("eff_date"),
            )
            ), asc=False
        ).alias("train_identifiers"),
    sort_array(
        collect_list(
            struct(
                 md5(concat(col("train_df__position"),col("train_df__cd"))).alias("reference_id"),
                lit(None).alias("code_id"),
                lit("Training Position").alias("code_type"),
                col("train_df__position").alias("code_value"),
                col("apos_tab_df__txt").alias("code_description")
                )
            ), asc=False
    ).alias("apos_variable"), 
    sort_array(
        collect_list(
            struct(
                 md5(concat(col("train_df__position"),col("train_df__cd"))).alias("reference_id"),
                lit(None).alias("code_id"),
                lit("Place of Training").alias("code_type"),
                col("hospital_df__cd").alias("code_value"),
                col("hospital_df__txt").alias("code_description")
                )
            ), asc=False
    ).alias("hospital_variable"), 
    sort_array(
        collect_list(
            struct(
                 md5(concat(col("train_df__position"),col("train_df__cd"))).alias("reference_id"),
                lit(None).alias("code_id"),
                lit("Program Type").alias("code_type"),
                lit(None).alias("code_value"),
                col("affi_arr_df__txt").alias("code_description")
                )
            ), asc=False
    ).alias("affi_arr_variable")
)

# Add the mapped array column to MapType column with udf function
joined_train_hospital_apos_affi_grouped_df = joined_train_hospital_apos_affi_grouped_df.withColumns(
    {
        "train_identifiers": map_udf(col("train_identifiers"))
    }
)

# COMMAND ----------

joined_addr2_addr1_tax_loc_df = joined_addr2_addr1_tax_df.join(
    loc_tab_df.select(
        col("loc_tab_df__cd"),
        col("loc_tab_df__txt")
    ),
    (
        trim(col("address1_df__loc_code")) == trim(col("loc_tab_df__cd"))
    ),
    "left"
)

# COMMAND ----------

# DBTITLE 1,Aggregation of address fields
joined_addr2_addr1_arr_grouped_df = joined_addr2_addr1_tax_loc_df.groupBy(col("address1_df__dr_id")).agg(
    sort_array(
            collect_list(
                struct(
                    col("address_reference_id").alias("reference_id"),
                    lit("physical").alias("address_type"), # default address_type is physical and there is no PO or BOX in address description to map as postal
                    when(
                        col("addr_arr_df__txt").isin(
                            "MFC Home Address",              
                            "Home"                          
                        ),
                        lit("home")
                    ).when(
                        col("addr_arr_df__txt").isin(
                            "MFC Part Time Clinic",
                            "MFC Part Time Clinic Bill",
                            "MFC W-9 Address",
                            "MFC Hospitalist Address  ",
                            "MFC Mailing Address",
                            "MFC Primary Office Address",
                            "MFC Previous Address",
                            "MFC Previous Billing Address",
                            "MFC Credentialing Address",
                            "MedStar Inpatient Term Address",
                            "Prov Group Contract Address",
                            "MFC Urgent Care Part Time",
                            "MedStar Inpatient Location",
                            "MFC Practice Location",
                            "MFC Telemedicine Address",
                            "Provider Group Previous Add",
                            "Additional Office",
                            "Primary",
                            "Prov Grp Mailing Addr",
                            "Prov Grp Addr",
                            "Primary Organization",
                        ),
                        lit("work")
                    ).when(
                        col("addr_arr_df__txt").isin(
                            "MFC Billing Address",
                            "MedStar Inpatient Billing",
                            "PHO-Fourth Billing Address",
                            "Billing",
                            "Mailing",
                            "Prov Grp Bill Addr",
                            "Prov Grp IRS Addr"
                        ),
                        lit("billing").alias("address_use")
                    ).when(
                        col("addr_arr_df__txt").isin(
                            "UNSPECIFIED"                            
                        ),
                        lit("temp")
                    ).when(
                        col("addr_arr_df__txt").isin(
                            "MFC Do Not Print",
                            "Previous Billing Address",
                            "Previous Billing Org",
                            "Previous Office Address",
                            "Previous",
                        ),
                        lit("old")
                    ).otherwise(
                        lit(None)
                    ).alias("address_use"),
                    col("address1_df__addr").alias("address_line_1"),
                    col("address1_df__addr2").alias("address_line_2"),
                    col("address1_df__city").alias("city"),
                    col("address1_df__county").alias("county"),
                    col("address1_df__state").alias("state_province"),
                    col("address1_df__zip").alias("postal_code"),
                    col("address1_df__zip_extension").alias("zip_code_extension"),
                    col("address1_df__nation").alias("country"),
                    col("address1_df__longitude").alias("longitude"),
                    col("address1_df__latitude").alias("latitude"),
                    col("address1_df__effdate").alias("eff_date"),
                    col("address1_df__termdate").alias("exp_date"),
                    lit(None).alias("confidential_status"),
                    col("address1_df__handicap").alias("is_handicap_accessible"),
                    col("address1_df__loc_code").alias("locality_code"),
                    col("addr_arr_df__cd").alias("address_sch_type"),
                    when(col("address1_df__udf3") == 'Y',lit(True))
                        .when(col("address1_df__udf3") == 'N',lit(False))
                        .otherwise(lit(None)).alias("print_in_directory_ind")
                )
            ), asc = False
        ).alias("other_addresses"), sort_array(
            collect_list(
                struct(
                    col("address_reference_id").alias("reference_id"),
                    lit("phone").alias("phone_type"),
                    col("address1_df__phone").alias("number"),
                    lit(None).alias("country_code"),
                    lit(None).alias("area_code"),
                    col("address1_df__extension").alias("extension"),
                    lit(None).alias("confidential_status")
                )
            ), asc = False
    ).alias("other_phones"), sort_array(
            collect_list(
                struct(
                    col("addr_arr_df__cd").alias("reference_id"),
                    col("address_reference_id").alias("address_reference_id"),
                    col("address2_df__substring_tax_id").alias("tax_id")
                )
            ), asc = False
        ).alias("other_tax_id"), sort_array(
            collect_list(
                struct(
                    col("address_reference_id").alias("reference_id"),
                    lit("County_Code").alias("source_column_name"),
                    col("address1_df__countycd").alias("source_column_value")
                )
            ), asc = False
        ).alias("address_county_extension"), sort_array(
            collect_list(
                struct(
                    col("address_reference_id").alias("reference_id"),
                    lit("Address_Link").alias("source_column_name"),
                    col("address1_df__link").alias("source_column_value")
                )
            ), asc = False
        ).alias("address_link_extension"), sort_array(
            collect_list(
                struct(
                    col("addr_arr_df__cd").alias("reference_id"),
                    col("address_reference_id").alias("address_reference_id"),
                    col("address1_df__phone").alias("primary_phone")
                )
            ), asc = False
        ).alias("primary_phone_array"),
        sort_array(
            collect_set(
                struct(
                    col("address_reference_id").alias("reference_id"),
                    col("addr_arr_df__txt").alias("id_type"),
                    col("addr_arr_df__cd").alias("id_value"),
                    lit(None).alias("id_concept"),
                    lit(None).alias("id_status"),
                    lit("cd").alias("input_field_name")
                )
            ), asc = False
        ).alias("address_reference_ids"),
        sort_array(
        collect_list(
            struct(
                md5(
                concat(
                    coalesce(col("address1_df__dr_id"),lit('NA')),
                    coalesce(col("address1_df__sch_type"),lit('NA')),
                    coalesce(col("address1_df__addr"),lit('NA')),
                    coalesce(col("address1_df__addr2"),lit('NA')),
                    coalesce(col("address1_df__city"),lit('NA')),
                    coalesce(col("address1_df__state"),lit('NA')),
                    coalesce(col("address1_df__zip"),lit('NA')),
                    coalesce(col("address1_df__link"),lit('NA'))
                )).alias("reference_id"),
                lit(None).alias("code_id"),
                lit("Location").alias("code_type"),
                col("loc_tab_df__cd").alias("code_value"),
                col("loc_tab_df__txt").alias("code_description")
            )
        ), asc = False
    ).alias("loc_tab_cd_variable"),
    )

# COMMAND ----------

joined_addr2_addr1_arr_grouped_df = joined_addr2_addr1_arr_grouped_df.join(
    general_contact_details_df.select(
    col("address_general_contact_details"),
    col("general_contact_details__dr_id")
    ),
    (
        (col("general_contact_details__dr_id") == col("address1_df__dr_id"))
    ),
    "left"
)      

# COMMAND ----------

stfstatu_sta_tab_rec_arr_df = stfstatu_df.join(
    sta_tab_df.select(
    col("sta_tab_df__cd"),
    col("sta_tab_df__txt")
    ),
    (
        (col("stfstatu_df__staff_cd") == col("sta_tab_df__cd"))
    ),
    "left"
).join( 
       rec_arr_df.select(
    col("rec_arr_df__cd"),
    col("rec_arr_df__txt")
       ),
       (
        (col("stfstatu_df__active123") == col("rec_arr_df__cd"))
       ),
       "left"
)

# COMMAND ----------

stfstatu_df = stfstatu_sta_tab_rec_arr_df.groupBy(col("stfstatu_df__dr_id")).agg(
    sort_array(
        collect_list(
            struct(
                lit(None).alias("reference_id"),
                lit(None).alias("code_id"),
                lit("Active123").alias("code_type"),
                col("stfstatu_df__active123").alias("code_value"),
                col("rec_arr_df__txt").alias("code_description")
                )
            ), asc=False
    ).alias("status_active_variable"), 
    sort_array(
        collect_list(
            struct(
                lit(None).alias("reference_id"),
                lit("Rec_Date").alias("source_column_name"),
                col("stfstatu_df__rec_date").alias("source_column_value")
                )
            ), asc=False
        ).alias("status_rec_date_extension"),
    sort_array(
        collect_list(
            struct(
                lit(None).alias("reference_id"),
                lit("Rea_Date").alias("source_column_name"),
                col("stfstatu_df__rea_date").alias("source_column_value")
                )
            ), asc=False
        ).alias("status_rea_date_extension"),
    sort_array(
        collect_list(
            struct(
                lit(None).alias("reference_id"),
                lit("l_secfile").alias("source_column_name"),
                col("stfstatu_df__l_secfile").alias("source_column_value")
                )
            ), asc=False
    ).alias("status_l_secfile_extension"),
    sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit("Stfstatu_Medicare").alias("identifier_type"),
                col("stfstatu_df__medicare").alias("identifier"),
                lit("stfstatu").alias("identifier_source"),
                lit(None).alias("exp_date"),
                lit(None).alias("eff_date")
            )

            ), asc=False
        ).alias("status_medicare_identifiers"),
    sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit("Stfstatu_Medicaid").alias("identifier_type"),
                col("stfstatu_df__medicaid").alias("identifier"),
                lit("stfstatu").alias("identifier_source"),
                lit(None).alias("exp_date"),
                lit(None).alias("eff_date")
            )
            ), asc=False
        ).alias("status_medicaid_identifiers"),
    sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit("udf1").alias("code_id"),
                lit("EPSDT").alias("code_type"),
                lit("stfstatu_df__udf1").alias("code_value"),
                when(col("stfstatu_df__udf1")=='1',lit('MD Medicaid EPSDT Certified'))
                     .when(col("stfstatu_df__udf1")=='2',lit('DO NOT USE DC Medicaid EPSDT'))
                     .when(col("stfstatu_df__udf1")=='3',lit('DO NOT USE MD&DC Medicaid EPSDT'))
                     .when(col("stfstatu_df__udf1")=='NOC',lit('Not Certified'))
                     .when(col("stfstatu_df__udf1")=='CE',lit('Certification Ended'))
                     .when(col("stfstatu_df__udf1")=='NA',lit('N/A')).otherwise(lit('')).alias("code_description")
            )
            ), asc=False
        ).alias("status_udf1_variable"),
    sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit("udf21").alias("code_id"),
                lit("DC Healthcheck").alias("code_type"),
                lit("stfstatu_df__udf21").alias("code_value"),
                when(col("stfstatu_df__udf21")=='1',lit('DC Healthcheck EPSDT Certified'))
                     .when(col("stfstatu_df__udf21")=='NOC',lit('Not Certified'))
                     .when(col("stfstatu_df__udf21")=='NA',lit('N/A')).otherwise(lit('')).alias("code_description")
            )
            ), asc=False
        ).alias("status_udf21_variable"),
    sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit("udf22").alias("code_id"),
                lit("Flouride Varnish").alias("code_type"),
                lit("stfstatu_df__udf22").alias("code_value"),
                when(col("stfstatu_df__udf22")=='TNC',lit('Training Not Completed'))
                     .when(col("stfstatu_df__udf22")=='NOC',lit('Not Certified'))
                     .when(col("stfstatu_df__udf22")=='NA',lit('N/A')).otherwise(lit('')).alias("code_description")
            )
            ), asc=False
        ).alias("status_udf22_variable"),
    sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit(None).alias("code_id"),
                lit("Record Status").alias("code_type"),
                col("sta_tab_df__cd").alias("code_value"),
                col("sta_tab_df__txt").alias("code_description")
            )
            ), asc=False
        ).alias("sta_tab_variable"),
        sort_array(
            collect_set(
                struct(
                    lit(None).alias("reference_id"),                   
                    lit(None).alias("board_id"),
                    lit(None).alias("certificate_id"),
                    lit("Flouride Varnish").alias("description"), 
                    lit(None).alias("issue_date"),
                    lit(None).alias("exp_date"), 
                    lit(None).alias("issuer"),
                    lit(None).alias("issuer_display"),                   
                    lit(None).alias("jurisdiction"),
                    lit(None).alias("license_category"),
                    lit(None).alias("license_number"), 
                    lit(None).alias("qualification_code"),
                    lit("udf22").alias("qualification_code_display"), 
                    lit(None).alias("qualification_code_system"),
                    lit(None).alias("qualification_number"), 
                    lit(None).alias("qualification_type"),
                    lit(None).alias("renew_date"), 
                    lit(None).alias("setting_type"),
                    col("stfstatu_df__udf22").alias("status"),
                    lit(None).alias("taxonomy")
                    )
                ), asc = False
            ).alias("status_udf22_qualifications"),
            sort_array(
                collect_set(
                    struct(
                        lit(None).alias("reference_id"),                   
                        lit(None).alias("board_id"),
                        lit(None).alias("certificate_id"),                        
                        lit("DC Health Check").alias("description"), 
                        lit(None).alias("issue_date"),
                        lit(None).alias("exp_date"), 
                        lit(None).alias("issuer"),
                        lit(None).alias("issuer_display"),                   
                        lit(None).alias("jurisdiction"),
                        lit(None).alias("license_category"),
                        lit(None).alias("license_number"), 
                        lit(None).alias("qualification_code"),
                        lit("udf21").alias("qualification_code_display"), 
                        lit(None).alias("qualification_code_system"),
                        lit(None).alias("qualification_number"), 
                        lit(None).alias("qualification_type"),
                        lit(None).alias("renew_date"), 
                        lit(None).alias("setting_type"),
                        col("stfstatu_df__udf21").alias("status"),
                        lit(None).alias("taxonomy")
                        )
                    ), asc = False
                ).alias("status_udf21_qualifications"),
            sort_array(
                collect_set(
                    struct(
                        lit(None).alias("reference_id"),                   
                        lit(None).alias("board_id"),
                        lit(None).alias("certificate_id"),
                        lit("EPDST").alias("description"), 
                        lit(None).alias("issue_date"),
                        lit(None).alias("exp_date"), 
                        lit(None).alias("issuer"),
                        lit(None).alias("issuer_display"),                   
                        lit(None).alias("jurisdiction"),
                        lit(None).alias("license_category"),
                        lit(None).alias("license_number"), 
                        lit(None).alias("qualification_code"),
                        lit("udf1").alias("qualification_code_display"), 
                        lit(None).alias("qualification_code_system"),
                        lit(None).alias("qualification_number"), 
                        lit(None).alias("qualification_type"),
                        lit(None).alias("renew_date"), 
                        lit(None).alias("setting_type"),
                        col("stfstatu_df__udf1").alias("status"),
                        lit(None).alias("taxonomy")
                        )
                    ), asc = False
                ).alias("status_udf1_qualifications"),   
            sort_array(
                collect_set(
                    col("stfstatu_df__pcp_spec")
            )).alias("pcp_ind_array") 
    )

# Add the mapped array column to MapType column with udf function
stfstatu_df = stfstatu_df.withColumns(
    {
        "status_medicare_identifiers": map_udf(col("status_medicare_identifiers")),
        "status_medicaid_identifiers": map_udf(col("status_medicaid_identifiers")),
        "pcp_ind_role":map_pcp_ind_role(col("pcp_ind_array")),


    }
).withColumns(
    {
        "pcp_ind":col("pcp_ind_role").getItem(0)["pcp_ind"],
        "service_provider_role": col("pcp_ind_role").getItem(0)["service_provider_role"]
    }

)
       

# COMMAND ----------

# DBTITLE 1,Join with stfstatu to segregate suppliers data
joined_agg_addr_stfstatu_df = joined_addr2_addr1_arr_grouped_df.join(
    stfstatu_df.select(
        col("stfstatu_df__dr_id"),        
        col("status_rec_date_extension"),
        col("status_rea_date_extension"),
        col("status_l_secfile_extension"),
        map_concat(
            col("status_medicare_identifiers"),
            col("status_medicaid_identifiers")
        ).alias("stfstatu_identifiers"),
        col("status_udf22_qualifications"),
        col("status_udf21_qualifications"),
        col("status_udf1_qualifications"),
        col("pcp_ind"),
        col("service_provider_role"),
        col("status_udf1_variable"),
        col("status_udf21_variable"),
        col("status_udf22_variable"),
        col("sta_tab_variable"),
        col("status_active_variable")
    ),
    (
        col("address1_df__dr_id") == col("stfstatu_df__dr_id")
    ),
    "inner"
)

# COMMAND ----------

dr_plan1_ins_plan_u3ins_tb_joined_df = dr_plan1_df.join(
    ins_plan_df.select(
        col("ins_plan_df__cd"),
        col("ins_plan_df__txt"),
        col("ins_plan_df__subname"),
    ),
    (
        col("dr_plan1_df__plan_cd") == col("ins_plan_df__cd")
    ),
    "left"
).join(
    u3ins_tb_df.select(
        col("u3ins_tb_df__cd"),
        col("u3ins_tb_df__txt")
        ),
    (
        col("dr_plan1_df__l_u3ins_tb") == col("u3ins_tb_df__cd")
    ),
    "left"
)

# COMMAND ----------

# DBTITLE 1,Aggregation of plans to populate networks array
dr_plan1_grouped_df = dr_plan1_ins_plan_u3ins_tb_joined_df.groupBy(col("dr_plan1_df__dr_id"), col("dr_plan1_df__status")).agg(
    sort_array(
        collect_set(
            struct(
                md5(
                    concat(
                        coalesce(col("dr_plan1_df__plan_cd"), lit("")),
                        coalesce(col("dr_plan1_df__vstart").cast("string"), lit("")),
                        coalesce(col("dr_plan1_df__eend").cast("string"), lit(""))
                    )).alias("reference_id"),           
                col("dr_plan1_df__plan_cd").alias("plan_id"),
                col("ins_plan_df__txt").alias("name"),            
                col("ins_plan_df__subname").alias("type"),
                col("dr_plan1_df__l_par_code").alias("parent_payer_network"),            
                col("dr_plan1_df__vstart").alias("eff_date"),
                col("dr_plan1_df__eend").alias("exp_date"),
                lit(None).alias("participation_code"), 
                lit(None).alias("payer_network_name")
                )
            ), asc = False
        ).alias("networks"),
    sort_array(
        collect_set(
            struct(
                lit(None).alias("reference_id"),
                lit("Plan_Status").alias("source_column_name"),
                col("dr_plan1_df__status").alias("source_column_value")
                )
            ), asc=False
    ).alias("drplan1_status_extension"),
    sort_array(
        collect_set(
            struct(
                lit(None).alias("reference_id"),
                lit("pcp_spec").alias("source_column_name"),
                col("dr_plan1_df__pcp_spec").alias("source_column_value")
                )
            ), asc=False
    ).alias("drplan1_pcp_spec_extension"), 
    sort_array(
        collect_list(
            struct(
                md5(
                    concat(
                        coalesce(col("dr_plan1_df__plan_cd"), lit("")),
                        coalesce(col("dr_plan1_df__vstart").cast("string"), lit("")),
                        coalesce(col("dr_plan1_df__eend").cast("string"), lit(""))
                    )).alias("reference_id"),
                lit(None).alias("code_id"),
                lit("Plan Term Reason").alias("code_type"),
                col("u3ins_tb_df__cd").alias("code_value"),
                col("u3ins_tb_df__txt").alias("code_description")            
            )
            ), asc=False
        ).alias("drplan1_u3ins_variable")
)

# COMMAND ----------

# DBTITLE 1,Join with plan table to identify the group affiliation plans
joined_agg_addr_stfstatu_plan_df = joined_agg_addr_stfstatu_df.join(
    dr_plan1_grouped_df.select(
        col("dr_plan1_df__dr_id"),
        col("dr_plan1_df__status"),
        col("networks"),
        col("drplan1_status_extension"),
        col("drplan1_pcp_spec_extension"),
        col("drplan1_u3ins_variable")
    ),
    (
        col("address1_df__dr_id") == col("dr_plan1_df__dr_id")
    ),
    "inner"
)

# COMMAND ----------

# DBTITLE 1,Join with contract status and filtering out inactive status
joined_agg_addr_stfstatu_plan_contract_df = joined_agg_addr_stfstatu_plan_df.join(
    contract_status_df.select(
        col("contract_status_df__link"),
        col("contract_status_df__cd"),
        col("contract_status_df__txt")
    ),
    (
        col("dr_plan1_df__status") == col("contract_status_df__cd")
    ),
    "inner"
).filter(lower(col("contract_status_df__txt")) == 'active')

# COMMAND ----------

# DBTITLE 1,Join of credent and lic_arr df and statelic df
credent_lic_arr_statelic_join_df = credent_df.join(
    lic_arr_df.select(
        col("lic_arr_df__cd"),
        col("lic_arr_df__txt"),
        col("lic_arr_df__lic_udf1")
    ),
    (
        col("credent_df__sch_type") == col("lic_arr_df__cd")
    ),
    "inner"
).join(
    statelic_df.select(
        col("statelic_df__cd"),
        col("statelic_df__txt"),
        col("statelic_df__addr"),
        col("statelic_df__addr2"),
        col("statelic_df__city"),
        col("statelic_df__zip"),
        col("statelic_df__ver_elec"),
        col("statelic_df__state"),
        col("statelic_df__email")
    ),
    (
        col("credent_df__cd") == col("statelic_df__cd")
    ),
    "inner"
)

# COMMAND ----------

# DBTITLE 1,Aggregation to populate licenses column
credent_lic_arr_statelic_grouped_join_df = credent_lic_arr_statelic_join_df.groupBy(col("credent_df__dr_id")).agg(
    sort_array(
        collect_set(
            struct(
                lit(None).alias("reference_id"),                   
                lit(None).alias("board_id"),
                lit(None).alias("certificate_id"),
                col("credent_df__title").alias("description"), 
                col("credent_df__issue_dt").alias("issue_date"),
                col("credent_df__lic_date").alias("exp_date"), 
                col("statelic_df__txt").alias("issuer"),
                col("statelic_df__cd").alias("issuer_display"),                   
                lit(None).alias("jurisdiction"),
                col("lic_arr_df__cd").alias("license_category"),
                col("credent_df__lic_num").alias("license_number"), 
                lit(None).alias("qualification_code"),
                lit(None).alias("qualification_code_display"), 
                col("lic_arr_df__txt").alias("qualification_code_system"),
                lit(None).alias("qualification_number"), 
                lit(None).alias("qualification_type"),
                lit(None).alias("renew_date"), 
                lit(None).alias("setting_type"),
                col("credent_df__status").alias("status"),
                lit(None).alias("taxonomy")
                )
            ), asc = False
        ).alias("credent_lic_arr_statelic_qualifications"), sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("Credent_Code").alias("source_column_name"),
                    col("credent_df__cd").alias("source_column_value")
                )
            ), asc = False
        ).alias("credent_code_extension"), sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("Credent_Link").alias("source_column_name"),
                    col("credent_df__link").alias("source_column_value")
                )
            ), asc = False
        ).alias("credent_link_extension"),
        sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("Credent_Notes").alias("source_column_name"),
                    col("credent_df__notes").alias("source_column_value")
                )
            ), asc = False
        ).alias("credent_notes"),
        sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("Credent_Verification").alias("source_column_name"),
                    col("statelic_df__ver_elec").cast("String").alias("source_column_value")
                )
            ), asc = False
        ).alias("credent_verification"),
        sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("Credent_State").alias("source_column_name"),
                    col("statelic_df__state").alias("source_column_value")
                )
            ), asc = False
        ).alias("credent_state"),
        sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("Credent_Email").alias("source_column_name"),
                    col("statelic_df__email").alias("source_column_value")
                )
            ), asc = False
        ).alias("credent_email")
    )

# COMMAND ----------

# DBTITLE 1,Join of license array with above df
joined_agg_addr_stfstatu_plan_contract_credent_licarr_df = joined_agg_addr_stfstatu_plan_contract_df.join(
    credent_lic_arr_statelic_grouped_join_df.select(
        col("credent_df__dr_id"),
        col("credent_lic_arr_statelic_qualifications"),
        col("credent_code_extension"),
        col("credent_link_extension"),
        col("credent_notes"),
        col("credent_verification"),
        col("credent_state"),
        col("credent_email")
    ),
    (
        col("address1_df__dr_id") == col("credent_df__dr_id")
    ),
    "left"
)

# COMMAND ----------

# DBTITLE 1,Join of educate and edu_arr df
educate_arr_join_df = educate_df.join(
    educ_arr_df.select(
        col("educ_arr_df__cd"),
        col("educ_arr_df__txt")
    ),
    (
        col("educate_df__sch_type") == col("educ_arr_df__cd")
    ),
    "inner"
).join(
    school_df.select(
        col("school_df__cd"),
        col("school_df__txt"),
        col("school_df__subname"),
        col("school_df__contact1"),
        col("school_df__contact2"),
        col("school_df__addr"),
        col("school_df__addr2"),
        col("school_df__city"),
        col("school_df__nation"),
        col("school_df__zip"),
        col("school_df__email"),
        col("school_df__state")
    ),
    (
        col("educate_df__cd") == col("school_df__cd")
    ),
    "inner"
).join(
    educ_tab_df.select(
        col("educ_tab_df__cd"),
        col("educ_tab_df__txt")
    ),
    (
        col("educate_df__degree") == col("educ_tab_df__cd")
    ),
    "inner"
).join(
    hospital_df.select(
        col("hospital_df__cd"),
        col("hospital_df__txt"),
        col("hospital_df__subname"),
        col("hospital_df__addr"),
        col("hospital_df__addr2"),
        col("hospital_df__city"),
        col("hospital_df__state"),
        col("hospital_df__zip"),
        col("hospital_df__nation"),
        col("hospital_df__phone"),
        col("hospital_df__contact1"),
        col("hospital_df__fax"),
        col("hospital_df__email"),
        col("hospital_df__homehtml"),
        col("hospital_df__note")
    ),
    (
        col("educate_df__cd") == col("hospital_df__cd")
    ),
    "inner"
)

# COMMAND ----------

# DBTITLE 1,Aggregation of educate df
educate_arr_grouped_join_df = educate_arr_join_df.groupBy(col("educate_df__dr_id")).agg(
    sort_array(
        collect_set(
            struct(
                col("educate_df__dr_id").alias("reference_id"),                   
                lit(None).alias("cumulative_years_of_training"),
                col("educate_df__degree").alias("degree"),
                col("educ_arr_df__txt").alias("education_type"), 
                year(col("educate_df__tdate")).alias("end_year"),
                col("school_df__subname").alias("department"),
                lit(None).alias("gpa"), 
                lit(None).alias("active"),
                lit(None).alias("graduated_ind"),                   
                col("school_df__txt").alias("school_name"),
                year(col("educate_df__fdate")).alias("start_year"),
                lit(None).alias("year_of_graduation"), 
                lit(None).alias("years_in_program")
                )
            ), asc = False
        ).alias("education"), sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("Educate_Code").alias("source_column_name"),
                    col("educate_df__cd").alias("source_column_value")
                )
            ), asc = False
        ).alias("educate_code_extension"), 
        sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("cmt1").alias("source_column_name"),
                    col("educate_df__cmt1").alias("source_column_value")
                )
            ), asc = False
        ).alias("educate_cmt1_extension"),
        sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("Educate_Link").alias("source_column_name"),
                    col("educate_df__link").alias("source_column_value")
                )
            ), asc = False
        ).alias("educate_link_extension"),
        sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("School_Code").alias("source_column_name"),
                    col("school_df__cd").alias("source_column_value")
                )
            ), asc = False
        ).alias("school_code_extension"),
        concat(
            sort_array(
                collect_list(
                    struct(
                        lit(None).alias("reference_id"),
                        lit(None).alias("id"),
                        lit("School").alias("entity_type"),
                        lit(None).alias("telecom_value"),
                        lit(None).alias("telecom_type"),
                        lit(None).alias("telecom_use"),
                        lit(None).alias("telecom_rank"),
                        lit(None).alias("telecom_eff_date"),
                        lit(None).alias("telecom_exp_date"),
                        col("school_df__contact1").alias("contact_name"),
                        lit(None).alias("contact_address"),
                        lit(None).alias("contact_telecom_value"),
                        lit(None).alias("contact_telecom_type"),
                        lit(None).alias("available_all_day"),
                        lit(None).alias("available_days_of_week"),
                        lit(None).alias("available_start_time"),
                        lit(None).alias("available_end_time"),
                        lit(None).alias("intemediary_ind"),
                        lit(None).alias("intermediary_value"),
                        lit(None).alias("intermediary_type")
                    )
                ), asc = False
            ),
             sort_array(
                collect_list(
                    struct(
                        lit(None).alias("reference_id"),
                        lit(None).alias("id"),
                        lit("School").alias("entity_type"),
                        lit(None).alias("telecom_value"),
                        lit(None).alias("telecom_type"),
                        lit(None).alias("telecom_use"),
                        lit(None).alias("telecom_rank"),
                        lit(None).alias("telecom_eff_date"),
                        lit(None).alias("telecom_exp_date"),
                        col("school_df__contact2").alias("contact_name"),
                        lit(None).alias("contact_address"),
                        lit(None).alias("contact_telecom_value"),
                        lit(None).alias("contact_telecom_type"),
                        lit(None).alias("available_all_day"),
                        lit(None).alias("available_days_of_week"),
                        lit(None).alias("available_start_time"),
                        lit(None).alias("available_end_time"),
                        lit(None).alias("intemediary_ind"),
                        lit(None).alias("intermediary_value"),
                        lit(None).alias("intermediary_type")
                    )
                ), asc = False
            )
        ).alias("school_general_contact_details"),
        sort_array(
            collect_list(
                struct(
                        lit(None).alias("reference_id"),
                        lit(None).alias("code_id"),
                        lit("Education").alias("code_type"),
                        col("educ_tab_df__cd").alias("code_value"),
                        col("educ_tab_df__txt").alias("code_description")
                     )
            ), asc=False
        ).alias("educ_tab_cd_variable"),
        sort_array(
            collect_list(
                struct(
                        col("educate_df__dr_id").alias("reference_id"),
                        lit("education_notes").alias("source_column_name"),
                        col("educate_df__notes").alias("source_column_value")
                     )
            ), asc=False
        ).alias("educate_df__notes_extension"),
        sort_array(
            collect_list(
                struct(
                        col("educate_df__dr_id").alias("reference_id"),
                        lit("education_program").alias("source_column_name"),
                        col("educate_df__cmt1").alias("source_column_value")
                     )
            ), asc=False
        ).alias("educate_df__program_extension"),
        sort_array(
            collect_list(
                struct(
                        col("educate_df__dr_id").alias("reference_id"),
                        lit("school_email").alias("source_column_name"),
                        col("school_df__email").alias("source_column_value")
                     )
            ), asc=False
        ).alias("school_df__email_extension"),
        sort_array(
            collect_list(
                struct(
                        lit(None).alias("reference_id"),
                        lit("hospital_address_line_1").alias("source_column_name"),
                        col("hospital_df__addr").alias("source_column_value")
                     )
            ), asc=False
        ).alias("hospital_df__address_line_1_extension"),
        sort_array(
            collect_list(
                struct(
                        lit(None).alias("reference_id"),
                        lit("hospital_address_line_2").alias("source_column_name"),
                        col("hospital_df__addr2").alias("source_column_value")
                     )
            ), asc=False
        ).alias("hospital_df__address_line_2_extension"),
        sort_array(
            collect_list(
                struct(
                        lit(None).alias("reference_id"),
                        lit("hospital_city").alias("source_column_name"),
                        col("hospital_df__city").alias("source_column_value")
                     )
            ), asc=False
        ).alias("hospital_df__city_extension"),
        sort_array(
            collect_list(
                struct(
                        lit(None).alias("reference_id"),
                        lit("hospital_state").alias("source_column_name"),
                        col("hospital_df__state").alias("source_column_value")
                     )
            ), asc=False
        ).alias("hospital_df__state_extension"),
        sort_array(
            collect_list(
                struct(
                        lit(None).alias("reference_id"),
                        lit("hospital_postal_code").alias("source_column_name"),
                        col("hospital_df__zip").alias("source_column_value")
                     )
            ), asc=False
        ).alias("hospital_df__postal_code_extension"),
        sort_array(
            collect_list(
                struct(
                        lit(None).alias("reference_id"),
                        lit("hospital_country_code").alias("source_column_name"),
                        col("hospital_df__nation").alias("source_column_value")
                     )
            ), asc=False
        ).alias("hospital_df__country_code_extension"),
        sort_array(
            collect_list(
                struct(
                        lit(None).alias("reference_id"),
                        lit("hospital_url").alias("source_column_name"),
                        col("hospital_df__homehtml").alias("source_column_value")
                     )
            ), asc=False
        ).alias("hospital_df__url_extension"),
        sort_array(
            collect_list(
                struct(
                        lit(None).alias("reference_id"),
                        lit("hospital_contact1").alias("source_column_name"),
                        col("hospital_df__contact1").alias("source_column_value")
                     )
            ), asc=False
        ).alias("hospital_df__contact1_extension"),
        sort_array(
            collect_list(
                struct(
                        lit(None).alias("reference_id"),
                        lit("hospital_phone").alias("source_column_name"),
                        col("hospital_df__phone").alias("source_column_value")
                     )
            ), asc=False
        ).alias("hospital_df__phone_extension"),
        sort_array(
            collect_list(
                struct(
                        lit(None).alias("reference_id"),
                        lit("hospital_fax").alias("source_column_name"),
                        col("hospital_df__fax").alias("source_column_value")
                     )
            ), asc=False
        ).alias("hospital_df__fax_extension"),
        sort_array(
            collect_list(
                struct(
                        lit(None).alias("reference_id"),
                        lit("hospital_email").alias("source_column_name"),
                        col("hospital_df__email").alias("source_column_value")
                     )
            ), asc=False
        ).alias("hospital_df__email_extension"),
        sort_array(
            collect_list(
                struct(
                        lit(None).alias("reference_id"),
                        lit("hospital_comments").alias("source_column_name"),
                        col("hospital_df__note").alias("source_column_value")
                     )
            ), asc=False
        ).alias("hospital_df__comments_extension"),
        sort_array(
            collect_set(
                struct(
                    col("educate_df__dr_id").alias("reference_id"),
                    lit(None).alias("address_type"), 
                    lit(None).alias("address_use"),
                    col("school_df__addr").alias("address_line_1"),
                    col("school_df__addr2").alias("address_line_2"),
                    col("school_df__city").alias("city"),
                    lit(None).alias("county"),
                    col("school_df__state").alias("state_province"),
                    substring(col("school_df__zip"), 1, 5).alias("postal_code"),
                    lit(None).alias("zip_code_extension"),
                    col("school_df__nation").alias("country"),
                    lit(None).alias("longitude"),
                    lit(None).alias("latitude"),
                    lit(None).alias("eff_date"),
                    lit(None).alias("exp_date"),
                    lit(None).alias("confidential_status"),
                    lit(None).alias("is_handicap_accessible"),
                    lit(None).alias("locality_code"),
                    lit(None).alias("address_sch_type"),
                    lit(None).alias("print_in_directory_ind")
                )
            ), asc = False
        ).alias("other_addresses_school")
)

# COMMAND ----------

# DBTITLE 1,Join of education_df with main dfs
joined_agg_addr_stfstatu_plan_contract_credent_licarr_edu_df = joined_agg_addr_stfstatu_plan_contract_credent_licarr_df.join(
    educate_arr_grouped_join_df.select(
        col("educate_df__dr_id"),
        col("education"),
        col("educate_code_extension"),
        col("educate_cmt1_extension"),
        col("educate_link_extension"),
        col("school_code_extension"),
        col("school_general_contact_details"),
        col("educ_tab_cd_variable"),
        col("school_df__email_extension"),
        col("other_addresses_school"),
        col("educate_df__notes_extension"),
        col("educate_df__program_extension"),
        col("hospital_df__address_line_1_extension"),
        col("hospital_df__address_line_2_extension"),
        col("hospital_df__city_extension"),
        col("hospital_df__state_extension"),
        col("hospital_df__postal_code_extension"),
        col("hospital_df__country_code_extension"),
        col("hospital_df__url_extension"),
        col("hospital_df__contact1_extension"),
        col("hospital_df__phone_extension"),
        col("hospital_df__fax_extension"),
        col("hospital_df__email_extension"),
        col("hospital_df__comments_extension")
    ),
    (
        col("educate_df__dr_id") == col("address1_df__dr_id")
    ),
    "left"
)

# COMMAND ----------

dr_spec_tab_join_df = dr_spec_df.join(
    spec_tab_df.select(
        col("spec_tab_df__txt"),
        col("spec_tab_df__cd")
    ),
    (
        col("spec_tab_df__cd") == col("dr_spec_df__specialty")
    ),
    "left"
).join(
    taxonomy_df.select(
        col("taxonomy_df__dr_id"),
        col("taxonomy_df__cd")
    ),
    (
      col("dr_spec_df__dr_id") == col("taxonomy_df__dr_id")  
    ),
    "left"
).join(
  taxo_arr_df.select(
    col("taxo_arr_df__cd"),
    col("taxo_arr_df__descript")
  ).distinct(),
  (
    col("taxonomy_df__cd") == col("taxo_arr_df__cd")
  ),
  "left"
)

# COMMAND ----------

# DBTITLE 1,ra
dr_spec_tab_grouped_join_df = dr_spec_tab_join_df.groupBy(col("dr_spec_df__dr_id")).agg(
    sort_array(
        collect_set(
            struct(
                col("dr_spec_df__dr_id").alias("reference_id"),                  
                lit(None).alias("id"),
                lit("Specialty").alias("type"),
                lit(None).alias("type_desc"), 
                col("spec_tab_df__cd").alias("code_value"),
                col("spec_tab_df__txt").alias("specialty_description"), 
                lit(None).alias("vendor_name"),
                col("dr_spec_df__primspec").alias("primary_ind")
                )
            ), asc = False
        ).alias("specialty"),
    sort_array(
            collect_set(
                struct(
                    col("dr_spec_df__dr_id").alias("reference_id"),                  
                    lit(None).alias("id"),
                    lit("Taxonomy").alias("type"),
                    lit(None).alias("type_desc"), 
                    col("taxonomy_df__cd").alias("code_value"),
                    col("taxo_arr_df__descript").alias("specialty_description"), 
                    lit(None).alias("vendor_name"),
                    lit(None).alias("primary_ind")
                )
            ), asc = False
    ).alias("taxonomy_spec"),
     sort_array(
        collect_set(
            struct(
                lit(None).alias("reference_id"),                   
                lit(None).alias("board_id"),
                lit(None).alias("certificate_id"),
                lit("Board Certified").alias("description"), 
                col("dr_spec_df__year_cert").cast(StringType()).alias("issue_date"),
                lit(None).alias("exp_date"), 
                lit(None).alias("issuer"),
                lit(None).alias("issuer_display"),                   
                lit(None).alias("jurisdiction"),
                lit(None).alias("license_category"),
                lit(None).alias("license_number"), 
                lit(None).alias("qualification_code"),
                lit(None).alias("qualification_code_display"), 
                lit(None).alias("qualification_code_system"),
                lit(None).alias("qualification_number"), 
                lit(None).alias("qualification_type"),
                col("dr_spec_df__year_recert").cast(StringType()).alias("renew_date"), 
                lit(None).alias("setting_type"),
                col("dr_spec_df__bd_cert").cast(StringType()).alias("status"),
                lit(None).alias("taxonomy")
                )
            ), asc = False
        ).alias("dr_spec_tab_join_qualifications"),
    sort_array(
        collect_set(
            col("taxonomy_df__cd")
        )
    ).alias("taxonomy_codes")

)

# COMMAND ----------

joined_agg_addr_stfstatu_plan_contract_credent_licarr_edu_spec_df = joined_agg_addr_stfstatu_plan_contract_credent_licarr_edu_df. join(
    dr_spec_tab_grouped_join_df.select(
        col("dr_spec_df__dr_id"),
        col("specialty"),
        col("taxonomy_spec"),
        col("dr_spec_tab_join_qualifications"),
        col("taxonomy_codes")
    ),
    (
        col("dr_spec_df__dr_id") == col("address1_df__dr_id")
    ),
    "left"
)

# COMMAND ----------

# DBTITLE 1,expertise join
joined_dr_expert_exp_tab_df=dr_expert_df.join(
    exp_tab_df,
    (
        col("dr_expert_df__expertise") == col("exp_tab_df__cd")
    ),
    "left"
)

# COMMAND ----------

# DBTITLE 1,expertise aggregate
joined_agg_dr_expert_exp_tab_df = joined_dr_expert_exp_tab_df.groupBy(col("dr_expert_df__dr_id")).agg(
   sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit(None).alias("code_id"),
                lit("Expertise").alias("code_type"),
                col("dr_expert_df__expertise").alias("code_value"),
                col("exp_tab_df__txt").alias("code_description")
            )
            ), asc=False
        ).alias("dr_expert_expertise_variable"), 
   sort_array(
            collect_set(
                struct(
                    col("dr_expert_df__dr_id").alias("reference_id"),                  
                    lit(None).alias("id"),
                    lit("Expertise").alias("type"),
                    lit(None).alias("type_desc"), 
                    col("dr_expert_df__expertise").alias("code_value"),
                    col("exp_tab_df__txt").alias("specialty_description"), 
                    lit(None).alias("vendor_name"),
                    lit(None).alias("primary_ind")
                )
            ), asc = False
    ).alias("expertise_spec"),
)

# COMMAND ----------

# DBTITLE 1,Join with the main drname table of the above dfs & join drname table and u1_tab &u2_tab(min_max_age_reference_table)
joined_agg_addr_stfstatu_plan_contract_credent_licarr_edu_spec_drname_df = joined_agg_addr_stfstatu_plan_contract_credent_licarr_edu_spec_df.join(
    drname_df,
    (
        col("drname_df__dr_id") == col("address1_df__dr_id")
    ),
    "inner"
).join(
    u1_tab_df, #min_age reference table
    (
        col("u1_tab_df__cd") == col("drname_df__ud1")
    ),
    "left"
).join(
    u2_tab_df, #max_age reference table
    (
        col("u2_tab_df__cd") == col("drname_df__ud2")
    ),
    "left"
).join(
    u3_tab_df, # accept new patient reference table
    (
        col("u3_tab_df__cd") == col("drname_df__ud3")
    ),
    "left"
).join(
    par_tab_df, 
    (
        col("par_tab_df__cd") == col("drname_df__pract_cd1")
    ),
    "left"
).join(
    u12_tab_df, 
    (
        col("u12_tab_df__cd") == col("drname_df__ud12")
    ),
    "left"
).join(
    eth_tab_df, 
    (
        col("eth_tab_df__cd") == col("drname_df__ethnic")
    ),
    "left"
)

# COMMAND ----------

joined_agg_addr_stfstatu_plan_contract_credent_licarr_edu_spec_drname_grouped_df = joined_agg_addr_stfstatu_plan_contract_credent_licarr_edu_spec_drname_df.groupBy("drname_df__dr_id").agg(
    sort_array(
        concat(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),            
                    lit("URL").alias("email_type"),
                    col("drname_df__email").alias("email"),
                    lit(None).alias("confidential_status")
                    )
                ),
            collect_list(
                struct(
                    lit(None).alias("reference_id"),            
                    lit("App_Module").alias("email_type"),
                    col("drname_df__appmodule_email").alias("email"),
                    lit(None).alias("confidential_status")
                )
            )
        )).alias("other_emails"), 
    sort_array(
            concat(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),            
                    lit("Facebook").alias("type"),
                    col("drname_df__facebook").alias("url"),
                    lit(None).alias("active_profile")
                )
            ),
            collect_list(
                struct(
                    lit(None).alias("reference_id"),            
                    lit("Twitter").alias("type"),
                    col("drname_df__twitter").alias("url"),
                    lit(None).alias("active_profile")
                )
            )
        )).alias("other_social_profiles"), 
    # extension_source_value fields
    sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("Dr_Title").alias("source_column_name"),
                    col("drname_df__drtitle").alias("source_column_value")
                )
            ), asc = False
        ).alias("drname_title_extension"), 
    sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("pict1").alias("source_column_name"),
                    col("drname_df__pict1").cast("string").alias("source_column_value")
                )
            ), asc = False
        ).alias("drname_pict1_extension"), 
    sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("ud7").alias("source_column_name"),
                    col("drname_df__ud7").cast("string").alias("source_column_value")
                )
            ), asc = False
        ).alias("drname_ud7_extension"), 
        sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("ud7_date").alias("source_column_name"),
                    col("drname_df__ud7_date").cast("date").alias("source_column_value")
                )
            ), asc = False
        ).alias("drname_ud7_date_extension"), 
    sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("pict2").alias("source_column_name"),
                    col("drname_df__pict2").cast("string").alias("source_column_value")
                )
            ), asc = False
        ).alias("drname_pict2_extension"), 
    sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("ud3").alias("source_column_name"),
                    col("drname_df__ud3").cast("string").alias("source_column_value")
                )
            ), asc = False
        ).alias("drname_ud3_extension"), 
    sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("ud9").alias("source_column_name"),
                    when(col("drname_df__ud9")== "C", lit("1099/Contracted")).when(col("drname_df__ud9")== "E",lit("W-2/Employed")).otherwise(col("drname_df__ud9")).cast("string").alias("source_column_value")
                )
            ), asc = False
        ).alias("drname_ud9_extension"), 
    sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("Verfac").alias("source_column_name"),
                    col("drname_df__verfac").cast("string").alias("source_column_value")
                )
            ), asc = False
        ).alias("drname_verfac_extension"), 
    sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("Beeper").alias("source_column_name"),
                    col("drname_df__beeper").alias("source_column_value")
                )
            ), asc = False
        ).alias("drname_beeper_extension"), 
    sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("Beeper2").alias("source_column_name"),
                    col("drname_df__beeper2").alias("source_column_value")
                )
            ), asc = False
        ).alias("drname_beeper2_extension"), 
    sort_array(
            collect_list(
                struct(
                    lit(None).alias("reference_id"),
                    lit("Cellphone").alias("source_column_name"),
                    col("drname_df__cellphone").alias("source_column_value")
                )
            ), asc = False
        ).alias("drname_cellphone_extension"),
    sort_array(
        collect_list(
            struct(
                lit(None).alias("reference_id"),
                lit("ud13_date").alias("source_column_name"),
                col("drname_df__ud13_date").alias("source_column_value")
            )
        ), asc = False
    ).alias("drname_ud13_date_extension"),
    sort_array(
        collect_list(
            struct(
                lit(None).alias("reference_id"),
                lit("ud14_date").alias("source_column_name"),
                col("drname_df__ud14_date").alias("source_column_value")
            )
        ), asc = False
    ).alias("drname_ud14_date_extension"),
    sort_array(
        collect_list(
            struct(
                lit(None).alias("reference_id"),
                lit("ud15_date").alias("source_column_name"),
                col("drname_df__ud15_date").alias("source_column_value")
            )
        ), asc = False
    ).alias("drname_ud15_date_extension"),
    sort_array(
        collect_list(
            struct(
                lit(None).alias("reference_id"),
                lit("Min_Patient_Age").alias("source_column_name"),
                col("u1_tab_df__txt").alias("source_column_value")
            )
        ), asc = False
    ).alias("drname_ud1_extension"),
    sort_array(
        collect_list(
            struct(
                lit(None).alias("reference_id"),
                lit("Max_Patient_Age").alias("source_column_name"),
                col("u2_tab_df__txt").alias("source_column_value")
            )
        ), asc = False
    ).alias("drname_ud2_extension"),
    sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit("Drname_Medicare").alias("identifier_type"),
                col("drname_df__medicare").alias("identifier"),
                lit("drname").alias("identifier_source"),
                lit(None).alias("exp_date"),
                lit(None).alias("eff_date")

            )

            ), asc=False
        ).alias("drname_medicare_identifiers"),
    sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit("Drname_Medicaid").alias("identifier_type"),
                col("drname_df__medicaid").alias("identifier"),
                lit("drname").alias("identifier_source"),
                lit(None).alias("exp_date"),
                lit(None).alias("eff_date")
            )
            ), asc=False
        ).alias("drname_medicaid_identifiers"),
      sort_array(
        collect_list(
     struct(
            lit(None).alias("reference_id"),
            lit("Visa").alias("identifier_type"),
            col("drname_df__visa").alias("identifier"),
            lit("DrName").alias("identifier_source"),
            lit(None).alias("eff_date"),
            col("drname_df__expdate").cast(StringType()).alias("exp_date")            
        ),
   )).alias("drname_df__visa_identifiers"),
   sort_array(
        collect_list(
            struct(
            lit(None).alias("reference_id"),
            lit("UPIN").alias("identifier_type"),
            col("drname_df__upin").alias("identifier"),
            lit("DrName").alias("identifier_source"),
            lit(None).alias("exp_date"),
            lit(None).alias("eff_date")
        ),
        )).alias("drname_df__upin_identifiers"),
    sort_array(
        collect_list(
     struct(
            lit(None).alias("reference_id"),
            lit("CAQH").alias("identifier_type"),
            col("drname_df__nudf1").alias("identifier"),
            lit("DrName").alias("identifier_source"),
            lit(None).alias("exp_date"),
            lit(None).alias("eff_date")
        ),
        )).alias("drname_df__nudf1_identifiers"),
    sort_array(
        collect_list(
     struct(
            lit(None).alias("reference_id"),
            lit("efmg number").alias("identifier_type"),
            col("drname_df__efmg_no").alias("identifier"),
            lit("DrName").alias("identifier_source"),
            lit(None).alias("exp_date"),
            lit(None).alias("eff_date")
        )
        )).alias("drname_df__efmg_no_identifiers"),
    sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit("NPI").alias("identifier_type"),
                col("drname_df__nationalid").alias("identifier"),         
                lit("NPI").alias("identifier_source"),
                lit(None).alias("exp_date"),
                lit(None).alias("eff_date")
            )
            ), asc=False
        ).alias("drname_df__npi_identifiers"),
                                        
    sort_array(
        collect_set(
            struct(
                lit(None).alias("reference_id"),                   
                lit(None).alias("board_id"),
                lit(None).alias("certificate_id"),
                lit("Cultural Competency Training").alias("description"), 
                lit(None).alias("issue_date"),
                lit(None).alias("exp_date"), 
                lit(None).alias("issuer"),
                lit(None).alias("issuer_display"),                   
                lit(None).alias("jurisdiction"),
                lit(None).alias("license_category"),
                lit(None).alias("license_number"), 
                lit(None).alias("qualification_code"),
                lit("ud8").alias("qualification_code_display"), 
                lit(None).alias("qualification_code_system"),
                col("drname_df__npdb").alias("qualification_number"), 
                col("drname_df__npdbdesc").alias("qualification_type"),
                lit(None).alias("renew_date"), 
                lit(None).alias("setting_type"),
                col("drname_df__ud8").alias("status"),
                lit(None).alias("taxonomy")
                )
            ), asc = False
        ).alias("drname_ud8_qualifications"),
    sort_array(
        collect_set(
            when(col("u3_tab_df__txt") == 'Accepting New Patients', lit('Accepting New Patients'))
                .when(col("u3_tab_df__txt") == 'Not Accepting New Patients', lit('Not Accepting New Patients'))
            .otherwise(lit(None))
            ), asc = False
    ).alias("drname_accept_new_patients"),
    sort_array(
        collect_set(
            when(col("u3_tab_df__txt") == 'Accepting New Patients', lit(None))
                .when(col("u3_tab_df__txt") == 'Not Accepting New Patients', lit(None))
            .otherwise(col("u3_tab_df__txt"))            
            ), asc = False
    ).alias("drname_accept_new_patient_from_network"),
    sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit(None).alias("code_id"),
                lit("Ethinicity").alias("code_type"),
                col("eth_tab_df__cd").alias("code_value"),
                col("eth_tab_df__txt").alias("code_description")
            )
            ), asc=False
        ).alias("drname_ethinic_variable"),
    sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit(None).alias("code_id"),
                lit("Race").alias("code_type"),
                col("u12_tab_df__cd").alias("code_value"),
                col("u12_tab_df__txt").alias("code_description")
            )
            ), asc=False
        ).alias("drname_race_variable"), 
    sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit(None).alias("code_id"),
                lit("FMG").alias("code_type"),
                col("drname_df__fmg").cast(StringType()).alias("code_value"),
                lit("Qualification Type/Status").alias("code_description")
            )
            ), asc=False
        ).alias("drname_fmg_variable"), 
    sort_array(
        collect_list(
        struct(
                lit(None).alias("reference_id"),
                lit(None).alias("code_id"),
                lit("Practice Code").alias("code_type"),
                col("drname_df__pract_cd1").cast(StringType()).alias("code_value"),
                col("par_tab_df__txt").alias("code_description")
            )
            ), asc=False
        ).alias("drname_pract_variable"),
    sort_array(
        collect_list(
        struct(
                col("drname_df__npdb").alias("qualification_number"),
                col("drname_df__npdbdesc").alias("qualification_type")
        )
        ), asc=False
        ).alias("npdb_list")
)

# Add the mapped array column to MapType column with udf function
joined_agg_addr_stfstatu_plan_contract_credent_licarr_edu_spec_drname_grouped_df = joined_agg_addr_stfstatu_plan_contract_credent_licarr_edu_spec_drname_grouped_df.withColumns(
    {
        "drname_medicare_identifiers": map_udf(col("drname_medicare_identifiers")),
        "drname_medicaid_identifiers": map_udf(col("drname_medicaid_identifiers")),
        "accept_new_patient": col("drname_accept_new_patients").getItem(0),
        "accept_new_patient_from_network": col("drname_accept_new_patient_from_network").getItem(0),
        "drname_df__visa_identifiers":map_udf(col("drname_df__visa_identifiers")),
        "drname_df__upin_identifiers":map_udf(col("drname_df__upin_identifiers")),
        "drname_df__nudf1_identifiers":map_udf(col("drname_df__nudf1_identifiers")),
        "drname_df__efmg_no_identifiers":map_udf(col("drname_df__efmg_no_identifiers")),
        "drname_df__npi_identifiers":map_udf(col("drname_df__npi_identifiers")),
    }
)

# COMMAND ----------

joined_lan_tab_dr_lan_df = dr_lan_df.join(
    lan_tab_df.select(
        col("lan_tab_df__cd"),
        col("lan_tab_df__txt")
    ),
    (
        col("dr_lan_df__lan_cd") == col("lan_tab_df__cd")
    ),
    "left"
)

# COMMAND ----------

joined_lan_tab_dr_lan_grouped_df = joined_lan_tab_dr_lan_df.groupBy(col("dr_lan_df__dr_id")).agg(
    sort_array(
        collect_list(
            struct(
                lit(None).alias("reference_id"),
                col("dr_lan_df__lan_cd").alias("language_code"),
                col("lan_tab_df__txt").alias("language_code_display"),
                lit(None).alias("language_text"),
                col("dr_lan_df__seq").alias("language_proficiency"),
            )
        ), asc=True
    ).alias("other_languages"),
)

# COMMAND ----------

final_df = joined_agg_addr_stfstatu_plan_contract_credent_licarr_edu_spec_drname_df.join(
    joined_agg_addr_stfstatu_plan_contract_credent_licarr_edu_spec_drname_grouped_df,
    (
        ["drname_df__dr_id"]
    ),
    "inner"
).join(
    joined_lan_tab_dr_lan_grouped_df,
    (
        col("drname_df__dr_id") == col("dr_lan_df__dr_id")
    ),
    "left"
).join(
    joined_agg_dr_expert_exp_tab_df,
    (
        col("drname_df__dr_id") == col("dr_expert_df__dr_id")
    ),
     "left"
).join(
    joined_train_hospital_apos_affi_grouped_df,
    (
        col("drname_df__dr_id") == col("train_df__dr_id")
    ),
     "left"
)


# COMMAND ----------

# DBTITLE 1,Map taxonomy code
qualification_transform_expr = f"""
transform(
    taxonomy_codes,
    t ->  transform(qualifications, q -> struct(
        q.reference_id as reference_id,
        q.board_id as board_id,
        q.certificate_id as certificate_id,
        q.description as description,
        q.issue_date as issue_date,
        q.exp_date as exp_date,
        q.issuer as issuer,
        q.issuer_display as issuer_display,
        q.jurisdiction as jurisdiction,
        q.license_category as license_category,
        q.license_number as license_number,
        t as qualification_code,
        q.qualification_code_display as qualification_code_display,
        q.qualification_code_system as qualification_code_system,
        q.qualification_number as qualification_number,
        q.qualification_type as qualification_type,
        q.renew_date as renew_date,
        q.setting_type as setting_type,
        q.status as status,
        t as taxonomy
    ))
)
"""

# COMMAND ----------

# DBTITLE 1,map npdb
qualification_npdb_list_transform_expr = f"""
transform(
    npdb_list,
    t ->  transform(qualifications, q -> struct(
        q.reference_id as reference_id,
        q.board_id as board_id,
        q.certificate_id as certificate_id,
        q.description as description,
        q.issue_date as issue_date,
        q.exp_date as exp_date,
        q.issuer as issuer,
        q.issuer_display as issuer_display,
        q.jurisdiction as jurisdiction,
        q.license_category as license_category,
        q.license_number as license_number,
        q.qualification_code as qualification_code,
        q.qualification_code_display as qualification_code_display,
        q.qualification_code_system as qualification_code_system,
        t.qualification_number as qualification_number,
        t.qualification_type as qualification_type,
        q.renew_date as renew_date,
        q.setting_type as setting_type,
        q.status as status,
        q.qualification_code as taxonomy
    ))
)
"""

# COMMAND ----------

# DBTITLE 1,identifiers and extension_source_value
final_df = final_df.withColumn("identifiers",
                            map_concat(
                                coalesce(col("drname_df__visa_identifiers"), create_map()),
                                coalesce(col("drname_df__upin_identifiers"), create_map()),
                                coalesce(col("drname_df__nudf1_identifiers"), create_map()),
                                coalesce(col("drname_df__efmg_no_identifiers"), create_map()),
                                coalesce(col("drname_df__npi_identifiers"),create_map()),
                                coalesce(col("stfstatu_identifiers"), create_map()),
                                coalesce(col("drname_medicare_identifiers"), create_map()),
                                coalesce(col("drname_medicaid_identifiers"), create_map()),
                                coalesce(col("train_identifiers"), create_map())
                            )
                                 
).withColumn("extension_source_values", concat(                
                coalesce(col("drname_verfac_extension"), lit([])),
                coalesce(col("drname_title_extension"), lit([])),
                coalesce(col("drname_ud7_extension"), lit([])),
                coalesce(col("drname_ud7_date_extension"), lit([])),
                coalesce(col("drname_pict1_extension"), lit([])),
                coalesce(col("drname_pict2_extension"), lit([])),
                coalesce(col("drname_ud3_extension"), lit([])),
                coalesce(col("drname_ud9_extension"), lit([])),
                coalesce(col("drname_ud1_extension"), lit([])),
                coalesce(col("drname_ud2_extension"), lit([])),
                coalesce(col("drname_ud13_date_extension"),lit([])), # credentailing date, 
                coalesce(col("drname_ud14_date_extension"), lit([])),
                coalesce(col("drname_ud15_date_extension"), lit([])),
                coalesce(col("status_rec_date_extension"), lit([])),
                coalesce(col("address_county_extension"), lit([])),
                coalesce(col("address_link_extension"), lit([])),
                coalesce(col("educate_code_extension"), lit([])),
                coalesce(col("educate_cmt1_extension"), lit([])),
                coalesce(col("educate_link_extension"), lit([])),
                coalesce(col("credent_code_extension"), lit([])),
                coalesce(col("credent_link_extension"), lit([])),
                coalesce(col("credent_notes"), lit([])),
                coalesce(col("credent_state"), lit([])),
                coalesce(col("credent_email"), lit([])),
                coalesce(col("credent_verification"), lit([])),
                coalesce(col("drname_beeper_extension"), lit([])),
                coalesce(col("drname_beeper2_extension"), lit([])),
                coalesce(col("school_code_extension"), lit([])),
                coalesce(col("drplan1_status_extension"), lit([])),
                coalesce(col("drplan1_pcp_spec_extension"), lit([])),
                coalesce(col("status_rea_date_extension"), lit([])),
                coalesce(col("status_l_secfile_extension"), lit([])),
                coalesce(col("school_df__email_extension"), lit([])),
                coalesce(col("educate_df__program_extension"), lit([])),
                coalesce(col("educate_df__notes_extension"), lit([])),
                coalesce(col("hospital_df__address_line_1_extension"), lit([])),
                coalesce(col("hospital_df__address_line_2_extension"), lit([])),
                coalesce(col("hospital_df__city_extension"), lit([])),
                coalesce(col("hospital_df__state_extension"), lit([])),
                coalesce(col("hospital_df__postal_code_extension"), lit([])),
                coalesce(col("hospital_df__country_code_extension"), lit([])),
                coalesce(col("hospital_df__url_extension"), lit([])),
                coalesce(col("hospital_df__contact1_extension"), lit([])),
                coalesce(col("hospital_df__phone_extension"), lit([])),
                coalesce(col("hospital_df__fax_extension"), lit([])),
                coalesce(col("hospital_df__email_extension"), lit([])),
                coalesce(col("hospital_df__comments_extension"), lit([]))
            )
).withColumn(
   "variable_fields",
    concat(
        coalesce(col("status_active_variable"), lit([])),
        coalesce(col("status_udf1_variable"), lit([])),
        coalesce(col("status_udf21_variable"), lit([])),
        coalesce(col("status_udf22_variable"), lit([])),
        coalesce(col("sta_tab_variable"), lit([])),        
        coalesce(col("drname_ethinic_variable"), lit([])),
        coalesce(col("drname_race_variable"), lit([])),
        coalesce(col("drname_fmg_variable"), lit([])),
        coalesce(col("drplan1_u3ins_variable"), lit([])),
        coalesce(col("loc_tab_cd_variable"), lit([])) ,
        coalesce(col("educ_tab_cd_variable"), lit([])),
        coalesce(col("dr_expert_expertise_variable"), lit([])),
        coalesce(col("drname_pract_variable"), lit([])),
        coalesce(col("apos_variable"), lit([])),
        coalesce(col("hospital_variable"), lit([])),
        coalesce(col("affi_arr_variable"), lit([]))                                    
    )
).withColumn(
   "qualifications",
    concat(
        coalesce(col("status_udf22_qualifications"), lit([])),
        coalesce(col("status_udf21_qualifications"), lit([])),
        coalesce(col("status_udf1_qualifications"), lit([])),
        coalesce(col("dr_spec_tab_join_qualifications"), lit([])),
        coalesce(col("credent_lic_arr_statelic_qualifications"), lit([])),
        coalesce(col("drname_ud8_qualifications"), lit([]))
    )  
).withColumn(
    "general_contact_details",
    concat(
        coalesce(col("address_general_contact_details"), lit([])),
        coalesce(col("school_general_contact_details"), lit([]))
    )
).withColumn(
    "qualifications",
    #Transform expr maps null to entire struct fields of qualifications if taxonomy_codes array is empty
    when(
        size(col("taxonomy_codes")) != 0,
        flatten(array_distinct(expr(qualification_transform_expr))) # qualification_transform_expr transformation creates nested arrays, this flattens and create on single array
    ).otherwise(col("qualifications"))
).withColumn(
    "qualifications",
    #Transform expr maps null to entire struct fields of qualifications if nbdb_list array is empty
    flatten(array_distinct(expr(qualification_npdb_list_transform_expr)))

).withColumn(
    "specialty",
    concat(coalesce(col("specialty"), lit([])),
    coalesce(col("taxonomy_spec"), lit([])),
    coalesce(col("expertise_spec"), lit([])))
)

# COMMAND ----------

# DBTITLE 1,Updated list based on dm version 25.1.24
colList =[
    col("drname_df__nationalid").alias("service_provider_id"),
    col("drname_df__dr_no_ext").alias("source_service_provider_id"),
    lit("HERO").alias("service_provider_id_system"),
    lit(None).alias("service_provider_id_type"),
    lit(None).alias("service_provider_id_type_system"),
    lit('usual').alias("service_provider_id_use"), # default provider use is usual
    lit(None).alias("service_provider_id_type_display"),
    lit(None).alias("service_provider_id_period_start"),
    lit(None).alias("service_provider_id_period_end"),
    lit(None).alias("service_provider_id_assigner"),
    lit(None).alias("service_provider_id_assigner_display"),
    col("drname_df__dr_fname").alias("name_first_name"),
    col("drname_df__dr_iname").alias("name_middle_name"),
    col("drname_df__dr_lname").alias("name_last_name"),
    col("drname_df__drsal").alias("name_prefix_name"),
    col("drname_df__drsuffix").alias("name_suffix_name"),
    lit(None).alias("service_provider_name_use"),
    lit(None).alias("service_provider_name_period_start"),
    lit(None).alias("service_provider_name_period_end"),
    concat(
        coalesce(col("drname_df__dr_fname"), lit('')),
        when(col("drname_df__dr_fname").isNotNull() & col("drname_df__dr_iname").isNotNull(), lit(' ')).otherwise(lit('')),
        coalesce(col("drname_df__dr_iname"), lit('')),
        when((col("drname_df__dr_fname").isNotNull() | col("drname_df__dr_iname").isNotNull()) & col("drname_df__dr_lname").isNotNull(), lit(' ')).otherwise(lit('')),
        coalesce(col("drname_df__dr_lname"), lit(''))
    ).alias("alternate_name_name"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["address_type"].alias("address_address_type"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["address_use"].alias("address_address_use"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["address_line_1"].alias("address_address_line_1"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["address_line_2"].alias("address_address_line_2"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["city"].alias("address_city"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["county"].alias("address_county"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["state_province"].alias("address_state_province"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["postal_code"].alias("address_postal_code"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["zip_code_extension"].alias("address_zip_code_extension"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["country"].alias("address_country"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["longitude"].alias("address_longitude"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["latitude"].alias("address_latitude"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["eff_date"].alias("address_eff_date"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["exp_date"].alias("address_exp_date"),
    lit(None).alias("address_confidential_status"),
    expr("filter(other_addresses,   x -> x.address_sch_type = 'HFC2')").getItem(0)["locality_code"].alias("address_locality_code"),
    concat(
    coalesce(col('other_addresses'),lit([])),
    coalesce(col('other_addresses_school'),lit([]))
    ).alias("other_addresses"),
    expr("filter(primary_phone_array,   x -> x.reference_id = 'HFC2')").getItem(0)["primary_phone"].alias("primary_phone_number"),
    lit(None).alias("secondary_phone_number"),
    col("other_phones").alias("other_phones"),
    col("drname_df__email").alias("email_email"),
    col("other_emails").alias("other_emails"),
    col("general_contact_details").alias("general_contact_details"),
    lit("Facebook").alias("social_profile_social_profile_type"),
    col("drname_df__facebook").alias("social_profile_profile_url"),
    col("other_social_profiles").alias("other_social_profiles"),
    col("pcp_ind").alias("pcp_ind"),
    col("service_provider_role").alias("service_provider_role"),
    col("accept_new_patient").alias("accept_new_patient"),
    col("accept_new_patient_from_network").alias("accept_new_patient_from_network"),
    lit(None).alias("new_patient_characteristic"),
    col("drname_df__gender").alias("base_demographic_gender"),
    lit(None).alias("base_demographic_sex"),
    col("drname_df__ud12").alias("base_demographic_race"),
    lit(None).alias("base_demographic_religion"),
    col("drname_df__ethnic").alias("base_demographic_ethnicity"),
    col("drname_df__marital").alias("base_demographic_marital_status"),
    col("drname_df__citisen").alias("base_demographic_nationality"),
    lit(None).alias("base_demographic_mothers_maiden_name"),
    col("drname_df__citisen").alias("base_demographic_citizenship"),
    col("drname_df__ss_no").alias("national_id"),
    col("drname_df__nationalid").alias("npi"),
    lit(None).alias("dea_id"),
    lit(None).alias("service_provider_alternate_id"),
    lit(None).alias("service_provider_alternate_id_type"),
    lit(None).alias("service_provider_alternate_id_type_system"),
    lit(None).alias("service_provider_alternate_id_type_display"),
    lit(None).alias("alternate_id_npi_value"),
    lit(None).alias("alternate_id_npi_use"),
    lit(None).alias("alternate_id_npi_type"),
    lit(None).alias("alternate_id_npi_type_system"),
    lit(None).alias("alternate_id_clia_value"),
    lit(None).alias("alternate_id_clia_use"),
    lit(None).alias("alternate_id_clia_type"),
    lit(None).alias("alternate_id_clia_type_system"),
    expr("filter(other_tax_id,   x -> x.reference_id = 'HFC2')").getItem(0)["tax_id"].alias("tin"),
    lit(None).alias("practice_type"),
    lit(None).alias("practice_type_display"),
    lit(None).alias("location"),
    lit(None).alias("location_display"),
    lit(True).alias("active"),
    col("drname_df__birthdate").alias("birth_date_of_birth"),
    year(col("drname_df__birthdate")).alias("birth_year_of_birth"),
    col("drname_df__birthplace").alias("birth_birthplace"),
    lit(None).alias("deceased_deceased_ind"),
    lit(None).alias("deceased_deceased_date"),
    col("other_languages").getItem(0)["language_code"].alias("language_language_code"),
    col("other_languages").alias("other_languages"),
    lit(None).alias("privacy_preferences"),
    col("qualifications").alias("qualifications"),
    lit(None).alias("credentialing_practice_type"),
    lit(None).alias("credentialing_practice_status"),
    lit(None).alias("credentialing_provider_status"),
    lit(None).alias("credentialing_provider_type"),
    col("education").alias("education"),
    lit(None).alias("reviews"),
    lit(None).alias("provider_participating_code"),
    col("networks").alias("networks"),
    lit(None).alias("network_tiers"),
    lit(None).alias("fee_variation_value"),
    col("specialty").alias("specialty"),
    lit(None).alias("eft"),
    lit(None).alias("provider_endpoint"),
    col("address_reference_ids").alias("reference_ids"),
    col("variable_fields").alias("variable_fields"),
    lit(None).alias("source_extension_record_id"),
    lit(None).alias("original_source_value"),
    lit("HERO").alias("source_name"),
    col("drname_df__dr_id").alias("source_id"),
    lit(None).alias("source_authoritative_ind"),
    lit(None).alias("source_timestamp"),
    col("identifiers").alias("identifiers"),
    lit(None).alias("additional_source_identifiers"),
    col("extension_source_values").alias("extension_source_values"),
    lit(None).alias("data_quality_result"),
]

# COMMAND ----------

mapping_df = final_df.select(colList)

# COMMAND ----------

# DBTITLE 1,setting the DQ rules
dq_rule_string =  """
Service_Provider
    service_provider_id must not be null
    length(npi) must be 10
    npi must not be null
    birth_date_of_birth should be less than now
    birth_date_of_birth should be greater than '1900-01-01'
    length(birth_year_of_birth) must be 4
    base_demographic_gender should be in ['M','F']
    source_id must be service_provider_id
"""


# COMMAND ----------

# DBTITLE 1,applying the DQ rules
from ng_data_quality import grammar
df_with_rules = grammar.apply_dq_rules(dq_rule_string, mapping_df)
mapping_df = grammar.flatten_dq_results(df_with_rules)

# COMMAND ----------

mapping_df = (
    mapping_df.withColumns({
        "source_authoritative_ind":lit(True)   
    })
)

# COMMAND ----------

target_table = f'{database}.silver.service_provider'
silver_df = MatchSchema(ref_table_name = target_table, strict = True).apply(mapping_df)

# COMMAND ----------

silver_df.write.mode("overwrite").insertInto(target_table)
