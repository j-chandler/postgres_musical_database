# Postgres Data Modeling

## Project Introduction
This project builds a Postgres Dataset using musical data and user activity logs. The project loads songs and artists from a directory of JSON files and transforms log level activity data into a songplays table which is designed to have analytical queries ran against it.

## Running The Project
Install the requirements first.
    
    pip install -r requirements.txt

Then make sure the tables by running

    python create_tables.py

And finally running
    
    python etl.py

will run the transforms and the data will be available in the tables below for querying. Data should be in a folder at the root of the project in a folder named "data". Additions to the data should follow the same structure in order to guarantee results are as expected.

## Data Structure
Data is in two formats depending on whether it is log data or songs data.

Log data is a series of JSON newline files found in the log_data folder. Each JSON line contains the following keys:

    - artist, The artist name or null if page not NextSong
    - auth, 
    - firstName, of the Current User
    - gender, of the Current User, character either M or F
    - itemInSession, int of the number of connected events
    - lastName, of the Current User
    - length, of the Song or null if page not NextSong
    - level, tier level of the Current User (free / paid)
    - location
    - method
    - page, this dictates whether a song is played, for pipeline purposes we want logs where this is equal to NextSong
    - registration
    - sessionId
    - song, title of the song or null if page not NextSong
    - status
    - ts, timestamp in epoch milliseconds
    - userAgent, browser data
    - userId

Song data is also in JSON newline format but each song is it's own file found within the song_data hierarchy.

Song data files have the following keys:
    
    - num_songs
    - artist_id
    - artist_latitude
    - artist_longitude
    - artist_location
    - artist_name
    - song_id
    - title
    - duration
    - year


## Table Formats
The dataset is structured into a star schema with the following tables:

Fact Tables:

    Table Name: songplays
    Table Schema:
        songplay_id SERIAL PRIMARY KEY,
        start_time timestamp,
        user_id int,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int,
        location varchar,
        user_agent varchar

Dimension Tables:

    Table Name: songs
    Table Schema: 
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar,
        year int,
        duration numeric

    Table Name: artists
    Table Schema: 
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar,
        latitude numeric,
        longitude numeric

    Table Name: time
    Table Schema:
        start_time timestamp PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int

    Table Name: users
    Table Schema: 
        user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender char(1),
        level varchar

## Sample Queries
### Finding All Songs From An Artist

    SELECT
        * 
    FROM songs
    JOIN artists
    ON artists.artist_id = songs.artist_id

### Average number of songplays by users

    SELECT
        COUNT(songplays.songplay_id) AS songs_played
    FROM songplays
    GROUP BY user_id
    ORDER BY songs_played DESC


