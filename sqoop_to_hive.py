import sys
import datetime
reload(sys)
sys.setdefaultencoding('utf8')

def sqoop_shell(db_name, table_name, input_path, sqoop_shell_path):
    output_path = '/Users/zqert/Codes/Codes_poke/poketraining/sqoop/bin/' + sqoop_shell_path
    s = '#!/bin/bash' + '\n' +'set -ux\n' + 'work_dir=$(readlink -f $(dirname $0))/..\n\n' + 'source $work_dir/conf/sqoop.conf\n' + 'source $work_dir/bin/hadoop.rc\n\n' + 'if [ $# -eq 1 ]; then\n' + '\tdate_key_start=$1\n' + 'else\n' + '''\tdate_key_start=$(date -d '1 day ago' +"%Y-%m-%d 00:00:00")\n''' + 'fi\n' + '''date_key_end=$(date -d "$date_key_start 1 day" +"%Y-%m-%d 00:00:00")\n'''
    a = 0
    with open(input_path, 'r') as f:
        while True:
            line = f.readline()
            if line:
                arr = line.strip().split('`', -1)
                if a ==0:
                    s = s + 'table="' + arr[1] + '"\n'
                    s = s + 'poke_table_dir="$hdfs_db_home/poke_' + db_name + '_' + arr[1] + '"\n'
                    s = s + 'hrmr $poke_table_dir\n\n'
                    s = s + 'sqoop import \\' + '\n\t-Dmapreduce.job.queuename="$job_queue"\\' + '\n\t--connect "$poke_member_address" \\' + '\n\t--username "$poke_member_user" \\' + '\n\t--password "$poke_member_password" \\' + '\n\t--table "$table" \\' + '\n\t--target-dir "$poke_table_dir" \\' + '\n\t--driver "com.mysql.jdbc.Driver" \\' + "\n\t--fields-terminated-by '' \\" + '''\n\t--where "ctime >= '$date_key_start' and ctime < '$date_key_end'" \\''' + '\n\t--as-parquetfile \\' + '\n\t-m 4'
                    break;
    return s

def scala_shell(db_name, table_name, input_path, scala_shell_file, scala_object, hive_table_name):
    output_path = '/Users/zqert/Codes/Codes_poke/poketraining/bin/' + scala_shell_file
    s = '#!/bin/bash' + '\n' +'set -ux\n' + 'work_dir=$(readlink -f $(dirname $0))/..\n\n' + 'if [ $# -eq 1 ]; then\n' + '\tdate=$1\n' + 'else\n' + '''\tdate=$(date -d "1 day ago" +"%Y-%m-%d 00:00:00")\n''' + 'fi\n' + 'source $work_dir/bin/hadoop.conf' + '\n\nspark-submit \\' + '\n\t--driver-memory 1G\\' + '\n\t--executor-memory 2G\\' + '\n\t--num-executors 1\\' + '\n\t--executor-cores 4\\' + '\n\t--conf spark.yarn.queue="production"\\' + '\n\t--class com.ipusic.poke.data.' + scala_object + '\\' + '\n\t$poke_training_jar\\' + '\n\t"$poke_db_home/poke_' + db_name + '_' + table_name + '" "$' + hive_table_name + '_one_day"' + '\n\n\nif [ $? -ne 0 ]; then' + '\n\tmsg="poke job (' + scala_shell_file + ') failed in ' + scala_object + ' at $date"' + '\n\techo $msg' + '\n\t$work_dir/monitor/notification.py "send" "$msg"' + '\nelse' + '\n\thive -e "MSCK REPAIR TABLE poke_log.' + hive_table_name + ';"' + '\n\timpala-shell -q "REFRESH poke_log.' + hive_table_name + ';"' + '\nfi'
    return s

def hive_sql(db_name, table_name, input_path, hive_table_name):
    datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    out_put_path = '/Users/zqert/Codes/Codes_poke/poketraining/hive/' + datetime.datetime.now().strftime('%Y%m%d%H%M') + '.create_' + table_name + '.sql'
    s = 'use poke_log;\n\n'
    a = 0
    with open(input_path, 'r') as f:
        while True:
            line = f.readline()
            if line:
                arr = line.strip().split('`', -1)
                if a ==0:
                    s = s + "CREATE TABLE IF NOT EXISTS " + hive_table_name + arr[2] + '\n'
                elif arr[0] == '':
                    val1 = arr[2]
                    if 'bigint' in val1:
                        s = s + arr[1] + '\tBIGINT,\n'
                    elif 'varchar' in val1 or 'datetime' in val1 or 'timestamp' in val1 or 'text' in val1:
                        s = s + arr[1] + '\tSTRING,\n'
                    elif 'int' in val1:
                        s = s + arr[1] + '\tINT,\n'
                else:
                    s = s[:-2] +'\n)\nPARTITIONED BY (year INT, month INT, day INT)\nSTORED AS PARQUET;'
                    break;
                a = a + 1
            else:
                break;
    return s

def hive_schema(input_path, hive_table_name_upper):
    s = 'val ' + hive_table_name_upper + '_SCHEMA = StructType(Array(\n'
    a = 0
    with open(input_path, 'r') as f:
        while True:
            line = f.readline()
            if line:
                arr = line.strip().split('`', -1)
                if a ==0:
					a = 0
                elif arr[0] == '':
                    val1 = arr[2]
                    if 'bigint' in val1:
                        s = s + 'StructField("' + arr[1] + '", LongType),\n'
                    elif 'varchar' in val1 or 'datetime' in val1 or 'timestamp' in val1 or 'text' in val1:
                        s = s + 'StructField("' + arr[1] + '", StringType),\n'
                    elif 'int' in val1:
                        s = s + 'StructField("' + arr[1] + '", IntegerType),\n'
                else:
                    s = s[:-2] + '\n))\n'
                    break;
                a = a + 1
            else:
                break;
    return s

def column_with_quote_marks(input_path):
    s = ''
    a = 0
    with open(input_path, 'r') as f:
        while True:
            line = f.readline()
            if line:
                arr = line.strip().split('`', -1)
                if a ==0:
					a = 0
                elif arr[0] == '':
                    s = s + arr[1] + ', '
                else:
                    break;
                a = a + 1
            else:
                break;
    return s[:-2]
    
def column_with_double_quotes(input_path):
    s = ''
    a = 0
    with open(input_path, 'r') as f:
        while True:
            line = f.readline()
            if line:
                arr = line.strip().split('`', -1)
                if a ==0:
					a = 0
                elif arr[0] == '':
                    s = s + '"' + arr[1] + '", '
                else:
                    break;
                a = a + 1
            else:
                break;
    return s[:-2]

def scala_code(scala_onject, hive_table_name, column_str_with_quote, column_str_with_double_quotes, hive_table_name_upper):
    s = 'package com.ipusic.poke.data\n\n' + 'import java.util.TimeZone\nimport com.ipusic.poke.common.HiveSchemas\nimport org.apache.spark.sql.{Row, SparkSession}\nimport org.joda.time.format.DateTimeFormat\nimport org.joda.time.{DateTime, DateTimeZone}' + '\n\nobject ' + scala_onject + ' {\n' + '  def main(args: Array[String]) {\n' + '    val inputPath = args(0)\n    val outputPath = args(1)\n\n' + '    val spark = SparkSession.builder().appName("' + scala_object + '").getOrCreate()\n\n' + '    val dataInfo = spark.read.format("parquet").load(inputPath)\n' + '      .select(' + column_str_with_double_quotes + ')\n      .rdd\n      .map(row => {\n'
    a = 0
    flag = 0
    with open(input_path, 'r') as f:
        while True:
            line = f.readline()
            if line:
                arr = line.strip().split('`', -1)
                if a ==0:
					a = 0
                elif arr[0] == '':
                    val1 = arr[2]
                    if 'bigint' in val1:
                        s = s + '        val ' + arr[1] + ' = row.getLong(' + str(a-1) + ')\n'
                    elif 'varchar' in val1 or 'datetime' in val1 or 'timestamp' in val1 or 'text' in val1:
                        if 'ctime' in arr[1] or 'mtime' in arr[1]:
                            if flag == 0:
                                s = s + '        val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")\n'
                            s = s + '        val ' + arr[1] + ' = new DateTime(row.getLong(' + str(a-1) + '), DateTimeZone.forTimeZone(TimeZone.getTimeZone("JST"))).toString(dateFormat)\n'
                            flag = flag + 1
                        else:
                            s = s + '        val ' + arr[1] + ' = row.getString(' + str(a-1) + ')\n'
                    elif 'int' in val1:
                        s = s + '        val ' + arr[1] + ' = row.getInt(' + str(a-1) + ')\n'
                else:
                    break;
                a = a + 1
            else:
                break;
    s = s + '        Row(' + column_str_with_quote + ')\n' + '      })\n\n' + '    spark.createDataFrame(dataInfo, HiveSchemas.' + hive_table_name_upper + '_SCHEMA)\n' + '      .write.mode("overwrite").parquet(outputPath)\n    spark.stop()\n  }\n}'
    return s


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: db_name, table_name, input_path")
        exit(-1)
    print sys.argv[1:]
    db_name, table_name, input_path = sys.argv[1:]
    sqoop_shell_file = 'poke_' + db_name + '_' + table_name + '.sh'
    scala_shell_file = 'server_' + db_name + '_' + table_name + '_to_hive.sh'
    scala_object = 'Server' + db_name.title() + table_name.title().replace('_','')
    hive_table_name = 'server_db_' + db_name + '_' + table_name
    hive_table_name_upper = hive_table_name.upper()
    sqoop_shell_str = sqoop_shell(db_name, table_name, input_path, sqoop_shell_file)
    sqoop_shell_output_path = '/Users/zqert/Codes/Codes_poke/poketraining/sqoop/bin/' + sqoop_shell_file
    scala_shell_str = scala_shell(db_name, table_name, input_path, scala_shell_file, scala_object, hive_table_name)
    scala_shell_output_path = '/Users/zqert/Codes/Codes_poke/poketraining/bin/' + scala_shell_file
    hive_sql_str = hive_sql(db_name, table_name, input_path, hive_table_name)
    datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    hive_sql_out_put_path = '/Users/zqert/Codes/Codes_poke/poketraining/hive/' + datetime.datetime.now().strftime('%Y%m%d%H%M') + '.create_' + table_name + '.sql'
    hive_schema_str = hive_schema(input_path, hive_table_name_upper)
    hadoop_conf_str = hive_table_name + '_one_day="$poke_hive_base/' + hive_table_name + '/$hive_path_one_day"'
    column_str_with_quote = column_with_quote_marks(input_path)
    column_str_with_double_quotes = column_with_double_quotes(input_path)
    scala_code_str = scala_code(scala_object, hive_table_name, column_str_with_quote, column_str_with_double_quotes, hive_table_name_upper)
    scala_code_output_path = '/Users/zqert/Codes/Codes_poke/poketraining/src/main/scala/com/ipusic/poke/data/' + scala_object + '.scala'
    print "-"*40,"sqoop_shell_str","-"*40
    print sqoop_shell_str
    print sqoop_shell_output_path
    print "-"*40,"scala_shell_str","-"*40
    print scala_shell_str
    print scala_shell_output_path
    print "-"*40,"hive_sql_str","-"*40
    print hive_sql_str
    print hive_sql_out_put_path
    print "-"*40,"hive_schema_str","-"*40
    print hive_schema_str
    print "-"*40,"hadoop_conf_str","-"*40
    print hadoop_conf_str
    print "-"*40,"column_str_with_quote","-"*40
    print column_str_with_quote
    print "-"*40,"column_str_with_double_quotes","-"*40
    print column_str_with_double_quotes
    print "-"*40,"scala_code_str","-"*40
    print scala_code_str
    print scala_code_output_path
    print "git add " + sqoop_shell_output_path + " " + scala_shell_output_path + " " + hive_sql_out_put_path + " " + scala_code_output_path
    print "chmod 755 " + sqoop_shell_output_path
    print "chmod 755 " + scala_shell_output_path
    '''
    with open(sqoop_shell_output_path,'wb') as f:
        f.write(sqoop_shell_str)
    with open(scala_shell_output_path,'wb') as f:
        f.write(scala_shell_str)
    with open(hive_sql_out_put_path,'wb') as f:
        f.write(hive_sql_str)
    with open(scala_code_output_path,'wb') as f:
        f.write(scala_code_str)
    '''

