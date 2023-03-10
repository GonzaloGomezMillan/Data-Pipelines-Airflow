class SqlQueries:
    create_tables = """
    CREATE TABLE IF NOT EXISTS public.artists (
        artistid varchar(256) NOT NULL,
        name varchar(256),
        location varchar(256),
        lattitude numeric(18,0),
        longitude numeric(18,0)
    );

    CREATE TABLE IF NOT EXISTS public.songplays (
        playid varchar(32) NOT NULL,
        start_time timestamp NOT NULL,
        userid int4 NOT NULL,
        "level" varchar(256),
        songid varchar(256),
        artistid varchar(256),
        sessionid int4,
        location varchar(256),
        user_agent varchar(256),
        CONSTRAINT songplays_pkey PRIMARY KEY (playid)
    );

    CREATE TABLE IF NOT EXISTS public.songs (
        songid varchar(256) NOT NULL,
        title varchar(256),
        artistid varchar(256),
        "year" int4,
        duration numeric(18,0),
        CONSTRAINT songs_pkey PRIMARY KEY (songid)
    );

    CREATE TABLE IF NOT EXISTS public.staging_events (
        artist varchar(256),
        auth varchar(256),
        firstname varchar(256),
        gender varchar(256),
        iteminsession int4,
        lastname varchar(256),
        length numeric(18,0),
        "level" varchar(256),
        location varchar(256),
        "method" varchar(256),
        page varchar(256),
        registration numeric(18,0),
        sessionid int4,
        song varchar(256),
        status int4,
        ts int8,
        useragent varchar(256),
        userid int4
    );

    CREATE TABLE IF NOT EXISTS public.staging_songs (
        num_songs int4,
        artist_id varchar(256),
        artist_name varchar(256),
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0),
        artist_location varchar(256),
        song_id varchar(256),
        title varchar(256),
        duration numeric(18,0),
        "year" int4
    );

    CREATE TABLE IF NOT EXISTS public."time" (
        start_time timestamp NOT NULL,
        "hour" int4,
        "day" int4,
        week int4,
        "month" varchar(256),
        "year" int4,
        weekday varchar(256),
        CONSTRAINT time_pkey PRIMARY KEY (start_time)
    );

    CREATE TABLE IF NOT EXISTS public.users (
        userid int4 NOT NULL,
        first_name varchar(256),
        last_name varchar(256),
        gender varchar(256),
        "level" varchar(256),
        CONSTRAINT users_pkey PRIMARY KEY (userid)
    );
"""
    
    songplay_table_insert = ("""
        (
        playid,
        start_time,
        userid,
        level,
        songid,
        artistid,
        sessionid,
        location,
        user_agent
        )
    
        SELECT
                md5(events.sessionid || events.start_time) playid,
                events.start_time as start_time, 
                events.userid as userid, 
                events.level as level, 
                songs.song_id as songid, 
                songs.artist_id as artistid, 
                events.sessionid as sessionid, 
                events.location as location, 
                events.useragent as user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        (
        userid,
        first_name,
        last_name,
        gender,
        level
        )
    
        SELECT  distinct userid,
                firstname,
                lastname,
                gender,
                level
        FROM staging_events
        WHERE page='NextSong'
    """)


    song_table_insert = ("""
        (
        songid,
        title,
        artistid,
        year,
        duration
        )
    
        SELECT  distinct song_id,
                title, 
                artist_id, 
                year, 
                duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        (
        artistid,
        name,
        location,
        lattitude,
        longitude
        )
        
        SELECT  distinct artist_id, 
                artist_name, 
                artist_location, 
                artist_latitude, 
                artist_longitude
        FROM staging_songs
    """)
    
    time_table_insert = ("""
        (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday        
        )
        
        SELECT  start_time,
                extract(hour from start_time) as hour,
                extract(day from start_time) as day,
                extract(week from start_time) as week, 
                extract(month from start_time) as month, 
                extract(year from start_time) as year, 
                extract(dayofweek from start_time) as weekday
        FROM songplays
    """)