
import subprocess
import shlex
import os

# mysql query to fetch Hive metadata information
query = "use HIVE;select TBL_NAME, OWNER, LOCATION, TBL_TYPE, NAME from DBS join TBLS on DBS.DB_ID=TBLS.DB_ID join SDS on SDS.SD_ID=TBLS.SD_ID where DBS.NAME='db1' and (TBL_NAME not like 'tb1%' );"

# Loading mysql connection info from conf file
#conf_path = os.getcwd() + "/" + "my.cnf"
query_string = "mysql --host=aws_host_name --port=3306 --user=username --password=pwd -e \"{query}\"".format(query=query )
#query_string = "mysql --defaults-extra-file={conf_path} -e \"{query}\"".format(query=query, conf_path=conf_path )
split_cmd = shlex.split(query_string)

# Executing the metadata query and storing the result set
query_result = subprocess.Popen(split_cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
result, err = query_result.communicate()
print("mysql query executed")

result_set = result.split('\n')[1:][:-1]
counter_managed = 0
counter_external = 0
counter_view = 0

# generating drop statements and hdfs remove statements as per result set from mysql query
drop_statements = ""
for i in result_set:
    table_name = tuple(map(lambda x: x.strip(), i.split('\t')))[0]
    owner = tuple(map(lambda x: x.strip(), i.split('\t')))[1]
    location = tuple(map(lambda x: x.strip(), i.split('\t')))[2]
    table_type = tuple(map(lambda x: x.strip(), i.split('\t')))[3]
    database_name = tuple(map(lambda x: x.strip(), i.split('\t')))[4]

    if(table_type == "MANAGED_TABLE"):
        print("DROP TABLE IF EXISTS "+ database_name + "." + table_name + ";") # check if u need database here
        drop_statements = drop_statements + "DROP TABLE "+ database_name + "." + table_name + ";"
        counter_managed = counter_managed + 1

    if(table_type == "EXTERNAL_TABLE"): 
        print("DROP TABLE IF EXISTS " + database_name + "." + table_name + ";")
        drop_statements = drop_statements + "DROP TABLE "+ database_name + "." + table_name + ";"
        print("hdfs dfs -rm -r " + location.replace("hdfs://url",""))
        rm_external_file = ("hdfs dfs -rm -r " + location.replace("hdfs://url",""))
        #os.system(rm_external_file)
        counter_external = counter_external + 1

    if(table_type == "VIRTUAL_VIEW"):
        print("DROP VIEW IF EXISTS "+ database_name + "." + table_name + ";")
        drop_statements = drop_statements + "DROP VIEW "+ database_name + "." + table_name + ";"
        counter_view = counter_view + 1

# .hql file to store drop statements , will be executed in beeline
filename = "drop_hive_table.hql"
print("view",counter_view)
print("managed",counter_managed)
print("external",counter_external)
with open(filename, 'w') as writ_obj:
    writ_obj.write(drop_statements)

print("Running hql file generated")
JDBC_URL = "jdbc:hive2://jdbc_url"
print(JDBC_URL)
query_string = "beeline -u '{JDBC_URL}' --force -f {filename}".format(JDBC_URL=JDBC_URL, filename=filename)
print("query_string")
print(query_string)
split_cmd = shlex.split(query_string)
query_result = subprocess.Popen(split_cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
result, err = query_result.communicate()
