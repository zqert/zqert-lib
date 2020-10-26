# -*- coding: utf-8 -*-

filename='./tables/user_lottery_history.txt'
a = 0
str0 = ''
str1 = ''
str2 = ''
str3 = ''
str4 = ''
str5 = ''
with open(filename, 'r') as f:
    while True:
        line = f.readline()
        if line:
            arr = line.strip().split('`', -1)
            if a == 0:
                print arr[0] + arr[1] + arr[2]
            elif arr[0] == '':
                val1 = arr[2]
                if 'bigint' in val1:
                    str0 = str0 + arr[1] + '\tBIGINT,\n'
                    str1 = str1 + arr[1] + ', '
                    str2 = str2 + '"' + arr[1] + '", '
                    str3 = str3 + 'val ' + arr[1] + ' = row.getLong(' + str(a-1) + ')\n'
                    str4 = str4 + 'StructField("' + arr[1] + '", LongType),\n'
                    str5 = str5 + 'val ' + arr[1] + ' = TextUtil.parseLong(arr(' + str(a-1) + '), 0L)\n'
                elif 'varchar' in val1 or 'datetime' in val1 or 'timestamp' in val1 or 'text' in val1:
                    str0 = str0 + arr[1] + '\tSTRING,\n'
                    str1 = str1 + arr[1] + ', '
                    str2 = str2 + '"' + arr[1] + '", '
                    str3 = str3 + 'val ' + arr[1] + ' = row.getString(' + str(a-1) + ')\n'
                    str4 = str4 + 'StructField("' + arr[1] + '", StringType),\n'
                    str5 = str5 + 'val ' + arr[1] + ' = arr(' + str(a-1) + ')\n'
                elif 'int' in val1:
                    str0 = str0 + arr[1] + '\tINT,\n'
                    str1 = str1 + arr[1] + ', '
                    str2 = str2 + '"' + arr[1] + '", '
                    str3 = str3 + 'val ' + arr[1] + ' = row.getInt(' + str(a-1) + ')\n'
                    str4 = str4 + 'StructField("' + arr[1] + '", IntegerType),\n'
                    str5 = str5 + 'val ' + arr[1] + ' = TextUtil.parseInt(arr(' + str(a-1) + '), 0L)\n'
            else:
                str0 = str0 +')\nPARTITIONED BY (year INT, month INT, day INT)\nSTORED AS PARQUET;'
                print str0
                print str1
                print str2
                print str3
                print str4
                print str5
                break;
            a = a + 1
        else:
            break;
