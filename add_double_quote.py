# -*- coding: utf-8 -*-
str="id, uid, coin_amount, coin_amount_buy, coin_amount_nonbuy, gem_amount, ctime, mtime"
print str
str_arr = str.split(', ')
str1=''
for each in str_arr:
    str1 += '\"'
    str1 += each
    str1 += '\", '

print str1[:-2]
