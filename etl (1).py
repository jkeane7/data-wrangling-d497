import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values.tolist()
    song_data = list(song_data[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist() 
    artist_data = list(artist_data[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong'].reset_index()

    # convert timestamp column to datetime
    t = df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    df['hour'] = df['ts'].dt.hour.astype(int)
    df['day'] = df['ts'].dt.day.astype(int)
    df['week'] = df['ts'].dt.week.astype(int)
    df['month'] = df['ts'].dt.month.astype(int)
    df['year'] = df['ts'].dt.year.astype(int)
    df['weekday'] = df['ts'].dt.dayofweek.astype(int)
    
       
    # tie data to columns
    time_dict = {c:[] for c in column_labels}

    # use loop to combine column_labels and time_data values into the dictionary
    for row in time_data[1:]:
        for c,v in zip(column_labels, row):
            time_dict[c].append(v)      

    
    # insert time data records
    time_data = df[['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']].values.tolist()
    column_labels = ['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame.from_dict(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()