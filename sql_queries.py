# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id varchar PRIMARY KEY, start_time time NOT NULL, user_id varchar NOT NULL, level Boolean, song_id varchar, artist_id varchar, session_id varchar, location varchar, user_agent varchar);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id varchar PRIMARY KEY, first_name varchar, last_name varchar, gender Boolean, level Boolean);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar PRIMARY KEY, title varchar NOT NULL, artist_id varchar NOT NULL, year int, duration float);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY, name varchar NOT NULL, location varchar, latitude int, longitude int);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time time PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int);""")

# INSERT RECORDS

songplay_table_insert = """INSERT INTO songplays(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

user_table_insert = """INSERT INTO users(user_id, first_name, last_name, gender, level)  VALUES (%s, %s, %s, %s, %s)"""

song_table_insert = """INSERT INTO songs(song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)"""


artist_table_insert = """INSERT INTO artists(artist_id, name, location, latitude, longitude)  VALUES (%s, %s, %s, %s, %s)"""


time_table_insert = """INSERT INTO time(start_time, hour, day, week, month, year, weekday)  VALUES (%s, %s, %s, %s, %s, %s, %s)"""

# FIND SONGS

song_select = """SELECT * FROM songs"""

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]