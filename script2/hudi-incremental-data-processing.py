import sys
import os
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.context import SparkConf
from awsglue.job import Job
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when,year,month,dayofmonth,from_unixtime,lit,concat
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DateType,DoubleType,BooleanType
import pyspark.sql.functions as sqlFunction
from datetime import datetime
import fnmatch

s3_client = boto3.client("s3")

"""
    Created by  :   Krishna Priya Bandi && Siddartha Rao Chennur
    Description :   This is a glue job template to load data in 'raw_processed' layer.
                    This job uses Hudi to do full loads and change data captures on tables provided
                    The same job can be used to run with different parameters for different processes.
                    To build any new data load process developers needs to just create/update a config file no code change needed.
"""


"""
    Task Level Arguments: Configurations that are exclusive for group of tables in a given database.
    Job Level Arguments : Arguments that are exclusive for the etl job

    *** Task Level Arguments will be given in Task Trigger file (job-def/task_jobtrigger.json) ***
    task_name                   : task name
    source_system_name          : source_system_name

    
    *** Job Level Arguments will be given in Job Config file (job-def/jobcreate.json) ***
    config_file_bucket          : source bucket
    checkpoint_file_bucket      : checkpoint bucket
    audit_table_name            : audit table name
    audit_db_name               : audit database name
    config_prefix               : prefix where task config files are stored in a given config_file_bucket
    checkpoint_prefix           : prefix where checkpoint config files are stored in a given checkpoint_file_bucket
    target_bucket               : target data bucket
"""
args = getResolvedOptions(sys.argv, ['JOB_NAME'
        , 'task_name'
        , 'source_system_name'
        , 'config_file_bucket'
        , 'checkpoint_file_bucket'
        , 'target_bucket'
        , 'audit_table_name'
        , 'audit_db_name'
        , 'checkpoint_prefix'
        , 'config_prefix'
        ])

# setting up Spark and Glue context parameters
config = SparkConf().setAll([('spark.serializer','org.apache.spark.serializer.KryoSerializer')]) 
sc = SparkContext(conf=config)  
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# variable declaration from arguments
job_run_id = args['JOB_RUN_ID']
load_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
load_date = datetime.now().strftime("%Y-%m-%d")
load_hour = datetime.now().strftime("%H")
load_min = datetime.now().strftime("%M")
load_sec = datetime.now().strftime("%S")
audit_db_name = args['audit_db_name']
audit_table_name = args['audit_table_name']
config_bucket = args['config_file_bucket']
checkpoint_bucket = args['checkpoint_file_bucket']
task_name= args['task_name']
source_system_name= args['source_system_name']
s3_config_prefix=args['config_prefix']+source_system_name+"/"+task_name+"-jobconfig.json"
s3_checkpoint_prefix = args['checkpoint_prefix']+source_system_name+"/"+task_name+"-checkpoint.json"
target_bucket = args['target_bucket']
source_db_name = ''
target_db_name = ''
source_table_name = ''
target_table_name = ''
record_count = 0



def write_json_to_s3(bucket,json_data,s3_prefix):
    '''
    this functions writes json_data to s3 prefix options
    params: 
        bucket: s3_bucket_name
        json_data: json data to be written to s3
        s3_prefix: s3 prefix to write data
    
    return:
        none
    '''
    try:
        s3_client.put_object(Bucket = bucket, Body = str(json.dumps(json_data)), Key = s3_prefix)
    except Exception as e:
        logError(str(e),'write_json_to_s3','Write JSON to S3')
        raise Exception(e)


def logError(error_message, function_name, process_name):
    '''
        Logging error details like load_start_time, load_end_time, error_message
        params :
            error_message   :   actual error message
            funtion_name    :   source location of error
    '''
    insertAuditLog(process_name, 1, 'ERROR occurred in : ' + function_name + ' , Error Message : ' + error_message,0)
        

def write_hudi_data(dataframe,hudi_config,process_name,partition_keys,target_db):
    '''
    this function writes processed data to target bucket
    params:
        dataframe: data to be written to target
        hudi_config: hudi configuration parameters in a dictionary format
        dbname: database name
        tablename: table name
        process_name: full load or cdc for logging purpose
    return:
        none
    '''
    try:
        record_count = dataframe.count()
        if partition_keys != "":
            distinct_partitions = dataframe.select("partition_string").distinct().collect()
            dataframe.drop("partition_string")
        glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(dataframe, glueContext, "dataframe"), connection_type = "marketplace.spark", connection_options = hudi_config)
        # insert to audit log
        if partition_keys != "":
            spark.sql('use '+target_db+"_access")
            for i in distinct_partitions:
                print(i[0]+"\n")
                spark.sql(i[0])
        insertAuditLog(process_name, 0, process_name+' on '+source_table_name+' is SUCCESSFUL',record_count)
        return True
    except Exception as e:
        print("Error writing data to target_db"+ str(e))
        logError(str(e),'write_hudi_data',process_name)
        return False


def partition_management(df,partition_keys,partition_field,process_name,table_name,target_db):
    '''
    this function creates partition columns if partition keys exists for a table
    params:
        df: dataframe containing data
        partition_keys: partition keys for the data provided
        process_name: full load or cdc for logging purpose
    
    return:
        df : dataframe with new partition columns added to the input dataframe
    '''
    try:
        allPartitionKeys = 'datekey,hour,min,sec'
        partitionKeys = partition_keys.split(',')
        df = df.withColumn('datekey', df[partition_field].substr(1, 10))
        df = df.withColumn('hour', df[partition_field].substr(12, 2))
        df = df.withColumn('min', df[partition_field].substr(14, 2))
        df = df.withColumn('sec', df[partition_field].substr(16, 2))
        df = df.withColumn('columns_available', lit(partition_keys))
        df = df.withColumn('partition_string',\
            concat(lit('alter table '+table_name+' add if not exists partition'),\
            when(df["columns_available"].contains("datekey")=="true",concat(lit('(datekey="'), df['datekey'],lit('"'))).otherwise(lit("")),\
            when(df["columns_available"].contains("hour")=="true",concat(lit(',hour="'), df['hour'],lit('"'))).otherwise(lit("")),\
            when(df["columns_available"].contains("min")=="true",concat(lit(',min="'), df['min'],lit('"'))).otherwise(lit("")),\
            when(df["columns_available"].contains("sec")=="true",concat(lit(',sec="'), df['sec'],lit('"'))).otherwise(lit("")),\
            lit(') location "s3://'+target_bucket+'/'+target_db+'/'+table_name+'/'),\
            when(df["columns_available"].contains("datekey")=="true",concat(lit('datekey='), df['datekey'],lit('/'))).otherwise(lit("")),\
            when(df["columns_available"].contains("hour")=="true",concat(lit('hour='), df['hour'],lit('/'))).otherwise(lit("")),\
            when(df["columns_available"].contains("min")=="true",concat(lit('min='), df['min'],lit('/'))).otherwise(lit("")),\
            when(df["columns_available"].contains("sec")=="true",concat(lit('sec='), df['sec'],lit('/'))).otherwise(lit("")),\
            lit('"')
            ))
        for part_col in allPartitionKeys.split(","):
            if part_col not in partitionKeys:
                df=df.drop(part_col)
        df.drop("columns_available")
        return df
    except Exception as e:
        print('error in partition_management function' + str(e))
        logError(str(e),'partition_management',process_name)
        return "fail"


def insertAuditLog(process_name, has_failed, error_message,record_count):
    '''
        Logging Audit details like record_count,load_start_time, load_end_time, error_message
        params:
            jobRunId    :   glue job run id
            processName :   name of the process for data load
            sourceTableName    :   source glue table names from config file
            targetDBName        :   target glue database name from config file
            targetTableName     :   target glue table name from config file
            loadStart           :   load start time
            loadEnd             :   load end time
            recordCount         :   no of records loading
            auditDBName         :   audit glue database name from config file
            auditTableName      :   audit glue table name from config file
            loadHour,loadMin,loadSec : partition keys
            hasFailed           :   if the load has failed or not
            errorMessage        :   load error message
    '''

    print('Start inserting audit log details')

    try:
        schema = StructType([
            StructField('job_run_id', StringType(), True),
            StructField('process_name', StringType(), True),
            #StructField('source_db_name', StringType(), True),#no
            StructField('source_table_name', StringType(), True),
            StructField('target_db_name', StringType(), True),
            StructField('target_table_name', StringType(), True),
            StructField('load_start', StringType(), True),
            StructField('load_end', StringType(), True),
            StructField('record_count', IntegerType(), True),
            StructField('has_failed', IntegerType(), True),
            StructField('error_message', StringType(), True)
        ])

        # List
        load_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("insert audit logging record count: {}".format(record_count))

        data = [(job_run_id, task_name , source_table_name, target_db_name, target_table_name, load_start, load_end,
                    record_count, has_failed, error_message)]
        
        # ???
        rdd = spark.sparkContext.parallelize(data)

        # Create data frame
        df = spark.createDataFrame(rdd, schema)
        df.drop_duplicates()
        
        # default settings  we want to maintain the same for all process
        df = df.withColumn("datekey", sqlFunction.lit(str(load_date)))
        df = df.withColumn("hour", sqlFunction.lit(int(load_hour)))
        dfLog = DynamicFrame.fromDF(df, glueContext, "enriched")
        additionalOptions = {"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}
        additionalOptions["partitionKeys"] = ["datekey", "hour"]
        glueContext.write_dynamic_frame_from_catalog(frame=dfLog, database=audit_db_name
                                                            , table_name=audit_table_name
                                                            , transformation_ctx="write_data_sink"
                                                            , additional_options=additionalOptions)

        print('End inserting audit log details')
    except Exception as e:
        print('error loading audit log data' + str(e))
    

#function for building hudi configuration
def create_hudi_config(write_option,table_iterator,target_db):
    '''
    this function creates hudi configuration dictionary as per inputs params 
    params:
        write_option: bulk_insert or upsert or delete
        table_iterator: individual table information from config file
        target_db: target database to be 
    '''
    record_key = table_iterator['record_key']
    precombinedkey = table_iterator['precombined_key']
    tablename = table_iterator['table_name']
    clustering_key = table_iterator['record_key']
    partition_keys = table_iterator['partition_path']
    s3_path = "s3://"+target_bucket+"/"+target_db+"/"+tablename+"/"
    print(tablename)
    print(partition_keys)
    print(s3_path)
    
    commonConfig = {
        'className' : 'org.apache.hudi', 
        'hoodie.datasource.write.precombine.field': precombinedkey,
        'hoodie.datasource.write.recordkey.field': record_key,
        'hoodie.table.name': tablename,
        'hoodie.consistency.check.enabled': 'true',
        'hoodie.datasource.write.insert.drop.duplicates': 'true',
        'path': s3_path
        }
    clusteringConfig = {
        'hoodie.parquet.small.file.limit': 0,
        #'hoodie.datasource.write.keygenerator.class':'org.apache.hudi.keygen.NonPartitionedKeyGenerator',
        'hoodie.datasource.write.keygenerator.class':'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
        'hoodie.datasource.hive_sync.partition_extractor_class':'org.apache.hudi.hive.NonPartitionedExtractor',
        'hoodie.clustering.plan.strategy.sort.columns': clustering_key,
        'hoodie.clustering.plan.strategy.max.bytes.per.group' : 10737418240, #10GB
        'hoodie.clustering.plan.strategy.max.num.groups' : 10
        }
    partitionConfig = {
        'hoodie.datasource.write.partitionpath.field' : "datekey,hour",
        'hoodie.datasource.write.hive_style_partitioning' : 'true',
        'hoodie.datasource.write.keygenerator.class':'org.apache.hudi.keygen.ComplexKeyGenerator'
        }
    bulkinsertConfig = {
        'hoodie.bulkinsert.shuffle.parallelism': 50, 
        'hoodie.datasource.write.operation': 'bulk_insert'
        }
    deleteConfig = {
        'hoodie.delete.shuffle.parallelism': 50, 
        'hoodie.datasource.write.operation': 'delete',
        'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS', 
        'hoodie.cleaner.commits.retained': 10
        }
    upsertConfig = {
        'hoodie.upsert.shuffle.parallelism': 50,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
        'hoodie.cleaner.commits.retained': 10
        }
    if partition_keys=="":
        if write_option=="bulk_insert":
            combinedConf = {**commonConfig, **clusteringConfig, **bulkinsertConfig}
        if write_option=="upsert":
            combinedConf = {**commonConfig, **clusteringConfig, **upsertConfig}
        if write_option=="delete":
            combinedConf = {**commonConfig, **clusteringConfig, **deleteConfig}
    else:
        if write_option=="bulk_insert":
            combinedConf = {**commonConfig, **partitionConfig, **bulkinsertConfig}
        if write_option=="upsert":
            combinedConf = {**commonConfig, **partitionConfig, **upsertConfig}
        if write_option=="delete":
            combinedConf = {**commonConfig, **partitionConfig, **deleteConfig}
    return combinedConf
   

def full_load(table_iterator,source_db,target_db):
    '''
    this function performs full load for an individual table provided
    params:
        table_iterator: individual table information from config file
        source_db: source database to fetch input table data from
        target_db: target database to load data into
    return:
        last_processed_partition
    '''
    try:
        '''
        FULL LOAD FUNCTIONALITY
        '''
        # fetch hudi config for 'bulk insert' setup
        hudi_config = create_hudi_config("bulk_insert",table_iterator,target_db)

        # read data from input table using glue context
        #df = glueContext.create_dynamic_frame.from_catalog(database = source_db, table_name = table_iterator['table_name'], transformation_ctx = "df")
        #dfout = df.toDF()
        print("select * from "+source_db+"."+table_iterator['table_name'])
        if table_iterator['data_file_type']=="sequence":
            dfout = spark.sql("select * from "+source_db+"."+table_iterator['table_name'])
        else:
            dfout = glueContext.create_dynamic_frame.from_catalog(database = source_db, table_name = table_iterator['table_name'], transformation_ctx = "df").toDF()
        if table_iterator['partition_path']!="":
            #if the target table has partitions. Partition keys must be one or more of these given formats - datekey,hour,min,sec
            dfout = partition_management(dfout,table_iterator['partition_path'],table_iterator['partition_field'],'full_load',table_iterator['table_name'],target_db)
        if dfout!="fail":
            # write data to s3 bucket  
            result = write_hudi_data(dfout,hudi_config,'full_load',table_iterator['partition_path'],target_db)
            if result == True:
                # default last processed partition setup for full load
                last_processed_partition='00000101T000000_00000101T000000'
                last_processed_sequence='0'
            else:
                last_processed_partition=''
                last_processed_sequence=''
            return last_processed_partition,last_processed_sequence
        else:
            return "",""

    except Exception as e:
        print('error in full_load function' + str(e))
        logError(str(e),'full_load','full_load')
        return "",""

        

def change_data_capture(table_iterator,source_db,target_db,last_processed_partition,last_processed_sequence):
    '''
    this function performs CDC for an individual table provided
    params:
        table_iterator: individual table information from config file
        source_db: source database to fetch input table data from
        target_db: target database to load data into
        last_processed_partition: needed to fetch change data from "last_processed_partition"
        last_processed_sequence:
    return:
        max_partition_processed: used for next CDC run
    
    '''
    try:
        # derive 'ct' table for given input table
        ct_table_name=table_iterator['table_name']+"__ct"

        # fetch max_partition_processed for ct_table
        if table_iterator['data_file_type']=="sequence":
            max_processed_df = spark.sql("select max(header__partition_name),max(header__change_seq) from "+source_db+'.'+ct_table_name)
            max_partition_processed = max_processed_df.collect()[0][0]
            max_sequence_processed = max_processed_df.collect()[0][1]
        else:
            ct_table_df = glueContext.create_dynamic_frame.from_catalog(database = source_db, table_name = ct_table_name, transformation_ctx="ct_table_df")
            ct_tableDF = ct_table_df.toDF()
            ct_tableDF.show(10)
            ct_tableDF.createOrReplaceTempView("ct_temp_view")
            spark.sql("select * from ct_temp_view").show(10)
            max_processed_df = spark.sql("select max(header__partition_name),max(header__change_seq) from ct_temp_view")
            max_partition_processed = max_processed_df.collect()[0][0]
            max_sequence_processed = max_processed_df.collect()[0][1]
        
        print("max_partition_processed {0},max_sequence_processed {1}".format(max_partition_processed,max_sequence_processed))

        '''
        UPSERT FUNCTIONALITY
        '''
        # fetch hudi config for 'upsert' setup
        hudi_config = create_hudi_config("upsert",table_iterator,target_db)

        # read data from input table using glue context
        #df = glueContext.create_dynamic_frame.from_catalog(database = source_db, table_name = ct_table_name,pushDownPredicate = "(header__partition_name > '"+last_processed_partition+"' and header__partition_name <= '"+max_partition_processed+"' and (header__change_oper=='U' or header__change_oper=='I'))", transformation_ctx = "df")
        #dfout = df.toDF()
        if table_iterator['data_file_type']=="sequence":
            dfout = spark.sql("select * from "+source_db+'.'+ct_table_name+" where header__partition_name >= '"+last_processed_partition+"' and header__partition_name <= '"+max_partition_processed+"' and header__change_seq >= '"+last_processed_sequence+"' and header__change_seq <= '"+max_sequence_processed+"' and (header__change_oper=='U' or header__change_oper=='I')")
        else:
            dfout = spark.sql("select * from ct_temp_view where header__partition_name >= '"+last_processed_partition+"' and header__partition_name <= '"+max_partition_processed+"' and header__change_seq >= '"+last_processed_sequence+"' and header__change_seq <= '"+max_sequence_processed+"' and (header__change_oper=='U' or header__change_oper=='I')")
        
        # filter out compose inserted columns from __ct table 
        cols = [column for column in dfout.columns if column.startswith("header__")]

        if table_iterator['partition_path']!="":
            #if the target table has partitions. Partition keys must be one or more of these given formats - datekey,hour,min,sec
            dfout = partition_management(dfout,table_iterator['partition_path'],table_iterator['partition_field'],'upsert',table_iterator['table_name'],target_db)
        if dfout!="fail":
        #drop compose inserted columns
            dfout.drop(*cols)
    
            # write data to s3 bucket  ???
            result = write_hudi_data(dfout,hudi_config,'upsert',table_iterator['partition_path'],target_db)
            if result == True:
                return max_partition_processed,max_sequence_processed
            else:
                return last_processed_partition,last_processed_sequence
        else:
            return last_processed_partition,last_processed_sequence
        
        '''
        DELETE FUNCTIONALITY
        '''
        # fetch hudi config for 'delete' setup
        hudi_config = create_hudi_config('delete',table_iterator,target_db)

        # read data from input table using glue context
        #df = glueContext.create_dynamic_frame.from_catalog(database = source_db, table_name = ct_table_name,pushDownPredicate = "(header__partition_name > '"+last_processed_partition+"' and header__partition_name <= '"+max_partition_processed+"' and header__change_oper=='D')", transformation_ctx = "df")
        #dfout = df.toDF()
        if table_iterator['data_file_type']=="sequence":
            dfout = spark.sql("select * from "+source_db+'.'+ct_table_name+" where header__partition_name >= '"+last_processed_partition+"' and header__partition_name <= '"+max_partition_processed+"' and header__change_seq > '"+last_processed_sequence+"' and header__change_seq <= '"+max_sequence_processed+"' and header__change_oper=='D'")
        else:
            dfout = spark.sql("select * from ct_temp_view where header__partition_name >= '"+last_processed_partition+"' and header__partition_name <= '"+max_partition_processed+"' and header__change_seq > '"+last_processed_sequence+"' and header__change_seq <= '"+max_sequence_processed+"' and header__change_oper=='D'")    
        
        if table_iterator['partition_path']!="":
            #if the target table has partitions. Partition keys must be one or more of these given formats - datekey,hour,min,sec
            dfout = partition_management(dfout,table_iterator['partition_path'],table_iterator['partition_field'],'delete',table_iterator['table_name'],target_db)
        if dfout!="fail":
        # drop compose inserted columns
            dfout.drop(*cols)
    
            # write data to s3 bucket  
            result = write_hudi_data(dfout,hudi_config,'delete',table_iterator['partition_path'],target_db)
            if result == True:
                return max_partition_processed,max_sequence_processed
            else:
                return last_processed_partition,last_processed_sequence
        else:
            return last_processed_partition,last_processed_sequence
    except Exception as e:
        print('error in change_data_capture function' + str(e))
        logError(str(e),'change_data_capture','CDC')
        return last_processed_partition,last_processed_sequence
 
if __name__ == "__main__":
    try:
        config_file_chk = s3_client.list_objects_v2(
                Bucket=config_bucket,
                Prefix=s3_config_prefix,
                Delimiter='/'
                )
    except Exception as e:
            print('error in listing objects in config bucket' + str(e))
            logError(str(e),'list_objects_v2','Listing objects in config bucket')
            raise Exception(e)     

    try:
        checkpoint_file_chk = s3_client.list_objects_v2(
                Bucket=checkpoint_bucket,
                Prefix=s3_checkpoint_prefix,
                Delimiter='/'
                )
    except Exception as e:
            print('error in listing objects in checkpoint bucket' + str(e))
            logError(str(e),'list_objects_v2','Listing objects in checkpoint bucket')
            raise Exception(e)

    if config_file_chk['KeyCount'] == 0:
        '''
        if task_config.json file does not exist - Exception Raised
        '''
        try:
            print(s3_config_prefix)
        except Exception as e: 
            error_msg = "Error: There is no config file in s3 location: " + s3_config_prefix
            logError(str(e),'KeyCount','No config file exists for the task' + task_name)
            raise Exception(error_msg)
        
    else:
        '''
        if task_config.json exists
        '''
        # Read the content from the file and convert to json object
        config_obj = s3_client.get_object(Bucket=config_bucket, Key=s3_config_prefix)
        config_str = config_obj['Body'].read().decode(encoding="utf-8")
        config_json = json.loads(config_str)

        source_db_name = config_json['source_db']
        target_db_name = config_json['target_db']
        
        if checkpoint_file_chk['KeyCount'] == 0:
            '''
            checkpoint.json file does not exist
            '''
            print("full load needed, checkpoint file does not exist or Force Full load requested")
            checkpoint_dict = {}
            
            for table_iterator in config_json['tables_list']:
                print("Starting full load for table : {}".format(table_iterator['table_name']))
                source_table_name = table_iterator['table_name']
                target_table_name = table_iterator['table_name']
                last_processed_partition,last_processed_sequence = full_load(table_iterator,config_json['source_db'] ,config_json['target_db'])
                
                if (last_processed_partition != ""):
                    checkpoint_tbl_dict = {}
                    checkpoint_tbl_dict['source_db'] = config_json['source_db'] 
                    checkpoint_tbl_dict['target_db'] = config_json['target_db']
                    checkpoint_tbl_dict['table_name'] = table_iterator['table_name']
                    checkpoint_tbl_dict['last_processed_partition'] = last_processed_partition
                    checkpoint_tbl_dict['last_processed_sequence'] = last_processed_sequence
                    checkpoint_dict[table_iterator['table_name']] = checkpoint_tbl_dict

            write_json_to_s3(checkpoint_bucket,checkpoint_dict,s3_checkpoint_prefix)
        else:
            '''
            if checkpoint.json file exists - perform CDC
            '''
            print("CDC process started as checkpoint file exists")
            checkpoint_obj = s3_client.get_object(Bucket=checkpoint_bucket, Key=s3_checkpoint_prefix)
            checkpoint_str = checkpoint_obj['Body'].read().decode(encoding="utf-8")
            checkpoint_json = json.loads(checkpoint_str)

            for table_iterator in config_json['tables_list']:
                table_name=table_iterator['table_name']
                source_table_name = table_iterator['table_name']+'__ct'
                target_table_name = table_iterator['table_name']
                
                if(table_name in list(checkpoint_json.keys())):
                    # if table exists in checkpoint json, do CDC
                    print("Starting CDC for table : {}".format(table_name))
                    last_processed_partition = checkpoint_json[table_name]['last_processed_partition']
                    last_processed_sequence = checkpoint_json[table_name]['last_processed_sequence']

                    max_partition_processed,max_sequence_processed = change_data_capture(table_iterator,config_json['source_db'],config_json['target_db'],last_processed_partition,last_processed_sequence)
                    
                    # update last_processed_partition and last_processed_sequence in checkpoint file
                    checkpoint_json[table_name]['last_processed_partition']= max_partition_processed
                    checkpoint_json[table_name]['last_processed_sequence']= max_sequence_processed
                else:
                    # if new table is added in config json , do full load for that particular table
                    print("Starting Full Load for newly added table : {}".format(table_name))

                    last_processed_partition,last_processed_sequence = full_load(table_iterator,config_json['source_db'] ,config_json['target_db'])
                    
                    # update last_processed_partition in checkpoint file 
                    # need to add all parameters in checkpoint file ???
                    if(last_processed_partition != ""):
                        checkpoint_tbl_dict = {}
                        checkpoint_tbl_dict['source_db'] = config_json['source_db'] 
                        checkpoint_tbl_dict['target_db'] = config_json['target_db']
                        checkpoint_tbl_dict['table_name'] = table_iterator['table_name']
                        checkpoint_tbl_dict['last_processed_partition'] = last_processed_partition
                        checkpoint_tbl_dict['last_processed_sequence'] = last_processed_sequence
                        checkpoint_json[table_iterator['table_name']] = checkpoint_tbl_dict

            print(checkpoint_json)
            write_json_to_s3(checkpoint_bucket,checkpoint_json,s3_checkpoint_prefix)
