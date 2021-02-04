import glob
import os
from io import StringIO

import pandas as pd

import psycopg2
import psycopg2.extras as extras

from sql_queries import artist_table_insert, songplay_table_batch_insert, song_select,  song_table_insert


def insert_df_into_table(df, cur, table):
    """
    Bulk insert a dataframe(df) into a table
    """

    # create temp table eg tmp_tab_users
    temp_table_create_query = """
    CREATE TEMP TABLE %s 
    (LIKE %s INCLUDING DEFAULTS)
    ON COMMIT DROP;
    """ % ('tmp_tab_'+table, table )

    cur.execute(temp_table_create_query)

    # copy data into temp table (including duplicates if present)
    buffer = StringIO()
    df.to_csv(buffer, mode='w', index=False, header=False)
    buffer.seek(0)
    cur.copy_from(buffer, 'tmp_tab_'+table, sep=',')

    # insert into main table excluding duplicates
    insert_query = """
    INSERT INTO %s
    SELECT *
    FROM %s
    ON CONFLICT DO NOTHING;
    """ % (table, 'tmp_tab_'+table)
    
    cur.execute(insert_query)


def process_song_file(cur, filepath):
    """
    Read and process a JSON file containing song data and insert into relevant tables
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_longitude', 'artist_latitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Read and process a JSON file containing log data and insert into relevant tables
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page.eq('NextSong')]

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    time_data = (df.ts, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time','hour','day','week','month','year','weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    insert_df_into_table(time_df, cur, 'time')

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    insert_df_into_table(user_df, cur, 'users')

    # insert songplay records
    songplay_data = []
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        
        songplay_data.append((row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent ))

    extras.execute_values(cur, songplay_table_batch_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Processes all files in the given filepath using the given function (func)
    """
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