# -*- coding: utf-8 -*-

str='"up_cur_artist", "up_cur_artist_sim", "up_cur_song", "up_cur_song_sim", "up_long_artist", "up_long_artist_sim", "up_long_song", "up_long_song_sim","request_type", "is_record_started", "artist_id", "song_table_id", "artist_table_id", "view", "rec_view", "record_start", "rec_record_start", "record", "rec_record",\
"cur_day_view", "cur_day_rec_view", "cur_day_record_start", "cur_day_rec_record_start", "cur_day_record", "cur_day_rec_record", "last_day_view",\
"last_day_rec_view", "last_day_record_start", "last_day_rec_record_start", "last_day_record", "last_day_rec_record", "song_opt", "hotScore", "alsScore",\
"simScore", "simUserScore", "can_score", "duration", "decade", "isFollows", "isFriends"'
print str
str_arr = str.split('\"')
str1=""
for each in str_arr:
    str1 += each
print str1
