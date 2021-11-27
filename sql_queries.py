# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS
    songplays(
        songplay_id SERIAL PRIMARY KEY,
        start_time text null,
        user_id integer not null,
        level text not null,
        song_id text null,
        artist_id text null,
        session_id integer not null,
        location text null,
        user_agent text not null,
        CONSTRAINT fk_time
            FOREIGN KEY(start_time)
                REFRENCES start(start_time),
        CONSTRAINT fk_users
            FOREIGN KEY(user_id)
                REFRENCES users(user_id),
        CONSTRAINT fk_songs
            FOREIGN KEY(song_id)
                REFRENCES songs(song_id),
        CONSTRAINT fk_songs
            FOREIGN KEY(song_id)
                REFRENCES songs(song_id),
        CONSTRAINT fk_artist
            FOREIGN KEY(artist_id)
                REFRENCES artists(artist_id),
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS
    users(
        user_id integer primary key,
        first_name text null,
        last_name text null,
        gender text null,
        level text null
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS
    songs(
        song_id text primary key,
        title text not null,
        artist_id text not null,
        year smallint not null,
        duration numeric null
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS
    artists(
        artist_id text primary key,
        name text null,
        location text null,
        latitude numeric null,
        longitude numeric null
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS
    time(
        start_time text primary key,
        hour smallint,
        day smallint,
        week smallint,
        month smallint,
        year smallint,
        weekday smallint
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) values(%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""
INSERT INTO users(user_id,first_name,last_name,gender,level) values(%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING
""")

song_table_insert = ("""
INSERT INTO songs(song_id,title,artist_id,year,duration) values(%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, longitude, latitude) values(%s, %s, %s, %s, %s) on CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday) values (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, s.artist_id FROM songs s 
JOIN artists a on a.artist_id=s.artist_id 
where s.title=%s and a.name=%s and s.duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]


# EXTRA QUERY
songplay_table_batch_insert = ("""
INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) values %s
""")
