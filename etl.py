import os
import glob
import psycopg2
import json
from datetime import datetime, timedelta

from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Reads, transforms and uploads song files from a given filepath into the songs and artists tables
    
    Parameters:
        cur: A Postgres Cursor object available from Connection.cursor()
        filepath: A string of the json file object to be loaded
    '''
    # open song file
    with open(filepath) as _file_:
        song_json = json.load(_file_)

    # insert song record
    song_data = [
        song_json["song_id"],
        song_json["title"],
        song_json["artist_id"],
        song_json["year"],
        song_json["duration"]
    ]
    
    #Catches Duplication Inserts
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.IntegrityError:
        cur.execute('rollback;')
    
    # insert artist record
    artist_data = [
        song_json["artist_id"],
        song_json["artist_name"],
        song_json["artist_location"],
        song_json["artist_latitude"],
        song_json["artist_longitude"]
    ]
    
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.IntegrityError:
        cur.execute('rollback;')

    
def epoch_time_to_time_data(epoch_time):
    '''
    Returns a list of strings of extracted columns for the time table from an epoch time string
    
    Parameters:
        epoch_time: A python Datetime object in epoch millisecond format
    '''
    return [
        epoch_time.strftime("%Y-%m-%d %H:%M:%S"), 
        epoch_time.strftime("%H"),
        epoch_time.strftime("%d"),
        epoch_time.strftime("%U"),
        epoch_time.strftime("%m"),
        epoch_time.strftime("%Y"),
        epoch_time.strftime("%w")       
    ]


def process_log_file(cur, filepath):
    '''
    Reads, transforms and uploads log files from a given filepath into the users, time and songlplays tables.
    
    NOTE: The songplays table requires the artists and songs table have already been built in order to query that those tables can be queried to obtain the song and artist ID from the supplied name in the log file.
    
    Parameters:
        cur: A Postgres Cursor object available from Connection.cursor()
        filepath: A string of the json file object to be loaded
    '''
    # open log file
    with open(filepath) as _file_:
        logs = [json.loads(log_string) for log_string in _file_.read().split("\n")]
        
    logs = list(filter(lambda log : log['page'] == 'NextSong', logs))
    
    
    for log in logs:
        ##### INSERTING TIME DATA #####
        epoch_time = datetime(1970, 1, 1) + timedelta(milliseconds = log['ts'])
        time_data = epoch_time_to_time_data(epoch_time)
        
        try:
            cur.execute(time_table_insert, time_data)
        except psycopg2.IntegrityError:
            cur.execute('rollback;')
        
        ##### INSERTING USER DATA #####
        user_data = [
            log['userId'],
            log['firstName'],
            log['lastName'],
            log['gender'],
            log['level']
        ]
        
        try:
            cur.execute(user_table_insert, user_data)
        except psycopg2.IntegrityError:
            cur.execute('rollback;')

        ##### GETTING AND INSERTING SONGPLAY DATA #####
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (log['artist'], log['song'], log['length']))
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = [
            time_data[0],
            log['userId'],
            log['level'],
            song_id,
            artist_id,
            log['sessionId'],
            log['location'],
            log['userAgent']
        ]
        
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Walks through a given directory to load json files that are found then sending them to the desired function for execution
    
    Parameters:
        cur: A Postgres Cursor object
        conn: A Postgres Connection object
        filepath: A string containing the root filepath for the directory to walk through
        func: A function to execute, must contain two parameters cur and filepath for the information to be passed to
    '''
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
    '''
    Builds the Postgres connection to the sparkifydb database and sends song_data and log_data to be executed by their appropriate processes.
    Closes the database after completion.
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
    
