
# A script to rebuild the movies database. As with the
# hospital database, we will use a schema to gather
# all the movies functionality together.

import psycopg2 as pg
import psycopg2.extensions as pge

import os

import pandas as pd
import numpy as np


# First set up a connection using the authentication
# data passed from the original call

# This sets up a connection to the database

tm351_admin_conn = pg.connect(dbname=DB_USER_MOVIES,
                              host='127.0.0.1',
                              user=DB_USER_MOVIES,
                              password=DB_PWD_MOVIES,
                              port=5432)

tm351_admin_conn.autocommit = True


# Next, set up a cursor, and DROP the movies
# schema if it exists.
c=tm351_admin_conn.cursor()

c.execute('DROP SCHEMA IF EXISTS movies CASCADE')

# Then recreate a clean schema

c.execute('CREATE SCHEMA movies')

# We'll define and populate the tables first, then add the
# foreign keys.

# Set up the path for the csv files
data_path=os.path.join(os.path.dirname(os.path.realpath(__file__)),
                       'sql_movie_data')


# and create a connection string

DB_MOVIES_SETUP_CONNECTION = '{engine}://{user}:{pwd}@{host}:{port}/{name}'.format(engine='postgresql',
                                                               user=DB_USER_MOVIES,
                                                               pwd=DB_PWD_MOVIES,
                                                               host='localhost',
                                                               port=5432,
                                                               name=DB_USER_MOVIES)


## Define the also_known_as table

print("Building movie.also_known_as table")

df=pd.read_csv(os.path.join(data_path, 'also_known_as.csv'))

df.to_sql('also_known_as',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.also_known_as
                ALTER COLUMN person_id TYPE INTEGER,
                ALTER COLUMN name TYPE VARCHAR(200);
          ''')

c.execute('''
            ALTER TABLE movies.also_known_as
                ADD CONSTRAINT also_known_as_pk
                    PRIMARY KEY (person_id, name);
          ''')

## Define the cast_member table

print("Building movies.cast_member table")

df=pd.read_csv(os.path.join(data_path, 'cast_members.csv'))

df.to_sql('cast_member',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.cast_member
                ALTER COLUMN credit_id TYPE CHAR(24),
                ALTER COLUMN movie_id TYPE INTEGER,
                ALTER COLUMN person_id TYPE INTEGER,
                ALTER COLUMN cast_id TYPE INTEGER,
                ALTER COLUMN character TYPE VARCHAR(500),
                ALTER COLUMN gender TYPE INTEGER,
                ALTER COLUMN cast_order TYPE INTEGER;
          ''')

c.execute('''
            ALTER TABLE movies.cast_member
                ADD CONSTRAINT cast_member_pk
                    PRIMARY KEY (credit_id);
          ''')

## Define the collection table

print("Building movies.collection table")

df=pd.read_csv(os.path.join(data_path, 'collections.csv'))

df.to_sql('collection',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.collection
                ALTER COLUMN id TYPE INTEGER,
                ALTER COLUMN name TYPE VARCHAR(100);
          ''')

c.execute('''
            ALTER TABLE movies.collection
                ADD CONSTRAINT collection_pk
                    PRIMARY KEY (id);
          ''')

## Define the crew table

print("Building movies.crew table")

df=pd.read_csv(os.path.join(data_path, 'crew.csv'))

df.to_sql('crew',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.crew
                ALTER COLUMN credit_id TYPE CHAR(24),
                ALTER COLUMN movie_id TYPE INTEGER,
                ALTER COLUMN person_id TYPE INTEGER,
                ALTER COLUMN department TYPE VARCHAR(100),
                ALTER COLUMN job TYPE VARCHAR(100),
                ALTER COLUMN gender TYPE INTEGER;
          ''')

c.execute('''
            ALTER TABLE movies.crew
                ADD CONSTRAINT crew_pk
                    PRIMARY KEY (credit_id);
          ''')

## Define the gender table

print("Building movies.gender table")

df=pd.read_csv(os.path.join(data_path, 'gender.csv'))

df.to_sql('gender',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.gender
                ALTER COLUMN id TYPE INTEGER,
                ALTER COLUMN gender TYPE VARCHAR(10);
          ''')

c.execute('''
            ALTER TABLE movies.gender
                ADD CONSTRAINT gender_pk
                    PRIMARY KEY (id);
          ''')


## Define the genre table

print("Building movies.genre table")

df=pd.read_csv(os.path.join(data_path, 'genres.csv'))

df.to_sql('genre',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.genre
                ALTER COLUMN id TYPE INTEGER,
                ALTER COLUMN name TYPE VARCHAR(100);
          ''')

c.execute('''
            ALTER TABLE movies.genre
                ADD CONSTRAINT genre_pk
                    PRIMARY KEY (id);
          ''')

## Define the movie_genre table

print("Building movies.movie_genre table")

df=pd.read_csv(os.path.join(data_path, 'movie_genres.csv'))

df.to_sql('movie_genre',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.movie_genre
                ALTER COLUMN movie_id TYPE INTEGER,
                ALTER COLUMN genre_id TYPE INTEGER;
          ''')

c.execute('''
            ALTER TABLE movies.movie_genre
                ADD CONSTRAINT movie_genre_pk
                    PRIMARY KEY (movie_id, genre_id);
          ''')


## Define the movie_language table

print("Building movies.movie_language table")

df=pd.read_csv(os.path.join(data_path, 'movie_languages.csv'))

df.to_sql('movie_language',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.movie_language
                ALTER COLUMN movie_id TYPE INTEGER,
                ALTER COLUMN language_id TYPE CHAR(2);
          ''')

c.execute('''
            ALTER TABLE movies.movie_language
                ADD CONSTRAINT movie_language_pk
                    PRIMARY KEY (movie_id, language_id);
          ''')

## Define the movie_production_company table

print("Building movies.movie_production_company table")

df=pd.read_csv(os.path.join(data_path, 'movie_production_companies.csv'))

df.to_sql('movie_production_company',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.movie_production_company
                ALTER COLUMN movie_id TYPE INTEGER,
                ALTER COLUMN production_company_id TYPE INTEGER;

          ''')

c.execute('''
            ALTER TABLE movies.movie_production_company
                ADD CONSTRAINT movie_production_company_pk
                    PRIMARY KEY (movie_id, production_company_id);
          ''')

## Define the production_company table

print("Building movies.production_company table")

df=pd.read_csv(os.path.join(data_path, 'production_companies.csv'),
               keep_default_na=False)

df.to_sql('production_company',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.production_company
                ALTER COLUMN id TYPE INTEGER,
                ALTER COLUMN company_name TYPE VARCHAR(100);
          ''')

c.execute('''
            ALTER TABLE movies.production_company
                ADD CONSTRAINT production_company_pk
                    PRIMARY KEY (id);
          ''')


## Define the movie_production_country table

print("Building movies.movie_production_country table")

df=pd.read_csv(os.path.join(data_path, 'movie_production_countries.csv'),
               keep_default_na=False)

df.to_sql('movie_production_country',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.movie_production_country
                ALTER COLUMN movie_id TYPE INTEGER,
                ALTER COLUMN production_country_id TYPE CHAR(2);

          ''')

c.execute('''
            ALTER TABLE movies.movie_production_country
                ADD CONSTRAINT movie_production_country_pk
                    PRIMARY KEY (movie_id, production_country_id);
          ''')



## Define the production_country table

print("Building movies.production_country table")

df=pd.read_csv(os.path.join(data_path, 'production_countries.csv'),
               keep_default_na=False)

df.to_sql('production_country',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.production_country
                ALTER COLUMN id TYPE CHAR(2),
                ALTER COLUMN country_name TYPE VARCHAR(50);
          ''')

c.execute('''
            ALTER TABLE movies.production_country
                ADD CONSTRAINT production_country_pk
                    PRIMARY KEY (id);
          ''')


## Define the spoken_language table

print("Building movies.spoken_language table")

df=pd.read_csv(os.path.join(data_path, 'spoken_languages.csv'),
               keep_default_na=False)

df.to_sql('spoken_language',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.spoken_language
                ALTER COLUMN id TYPE CHAR(2),
                ALTER COLUMN language TYPE VARCHAR(50);
          ''')

c.execute('''
            ALTER TABLE movies.spoken_language
                ADD CONSTRAINT spoken_language_pk
                    PRIMARY KEY (id);
          ''')



## Define the movie table

print("Building movies.movie table")

df=pd.read_csv(os.path.join(data_path, 'movies.csv'))


# Before exporting to postgresql, convert the dates to
# pandas datetime format

z=[pd.to_datetime(z) for z in df['release_date']]
df['release_date']=z

df.to_sql('movie',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )

c.execute('''
            ALTER TABLE movies.movie
                ALTER COLUMN id TYPE INTEGER,
                ALTER COLUMN adult TYPE BOOLEAN,
                ALTER COLUMN belongs_to_collection TYPE INTEGER,
                ALTER COLUMN budget TYPE BIGINT,
                ALTER COLUMN imdb_id TYPE CHAR(9),
                ALTER COLUMN original_language TYPE CHAR(2),
                ALTER COLUMN original_title TYPE VARCHAR(200),
                ALTER COLUMN overview TYPE VARCHAR(1000),
                ALTER COLUMN popularity TYPE DOUBLE PRECISION,
                ALTER COLUMN release_date TYPE DATE,
                ALTER COLUMN revenue TYPE BIGINT ,
                ALTER COLUMN rt_all_critics_num_fresh TYPE INTEGER,
                ALTER COLUMN rt_all_critics_num_reviews TYPE INTEGER,
                ALTER COLUMN rt_all_critics_num_rotten TYPE INTEGER,
                ALTER COLUMN rt_all_critics_rating TYPE FLOAT,
                ALTER COLUMN rt_all_critics_score TYPE INTEGER,
                ALTER COLUMN rt_audience_num_ratings TYPE INTEGER,
                ALTER COLUMN rt_audience_rating TYPE FLOAT,
                ALTER COLUMN rt_audience_score TYPE INTEGER,
                ALTER COLUMN rt_ID TYPE VARCHAR(100),
                ALTER COLUMN rt_top_critics_rating TYPE FLOAT,
                ALTER COLUMN rt_top_critics_num_fresh TYPE INTEGER,
                ALTER COLUMN rt_top_critics_num_reviews TYPE INTEGER,
                ALTER COLUMN rt_top_critics_num_rotten TYPE INTEGER,
                ALTER COLUMN rt_top_critics_score TYPE INTEGER,
                ALTER COLUMN runtime TYPE INTEGER,
                ALTER COLUMN spanish_title TYPE VARCHAR(200),
                ALTER COLUMN status TYPE VARCHAR(50),
                ALTER COLUMN tagline TYPE VARCHAR(500),
                ALTER COLUMN title TYPE VARCHAR(200),
                ALTER COLUMN tmdb_id TYPE INTEGER,
                ALTER COLUMN video TYPE BOOLEAN,
                ALTER COLUMN vote_average TYPE FLOAT,
                ALTER COLUMN vote_count TYPE INTEGER,
                ALTER COLUMN year TYPE INTEGER;
          ''')

#                ALTER COLUMN rt_all_critics_rating TYPE NUMERIC(5, 2),
#                ALTER COLUMN rt_audience_rating TYPE NUMERIC(5, 2),
             #   ALTER COLUMN rt_top_critics_rating TYPE NUMERIC(5, 2),
#                ALTER COLUMN vote_average TYPE NUMERIC(5, 2)

c.execute('''
            ALTER TABLE movies.movie
                ADD CONSTRAINT movie_pk
                    PRIMARY KEY (id);
          ''')


## Define the person table

print("Building movies.person table")

df=pd.read_csv(os.path.join(data_path, 
                            'people.csv'))

# Before exporting to postgresql, convert the dates to
# pandas datetime format
#
# In fact, there are some dates which don't parse, so
# I'm just going to hack those in afterwards

unparsed_birthdays_dict={}
parsed_birthdays_ls=[]

for (i, d) in enumerate(df['birthday']):
    try:
        parsed_birthdays_ls.append(pd.to_datetime(d))
    except pd.errors.OutOfBoundsDatetime:
        unparsed_birthdays_dict[df['id'].iloc[i]]=d
        parsed_birthdays_ls.append(np.nan)
        
df['birthday']=parsed_birthdays_ls

unparsed_deathdays_dict={}
parsed_deathdays_ls=[]

for (i, d) in enumerate(df['deathday']):
    try:
        parsed_deathdays_ls.append(pd.to_datetime(d))
    except pd.errors.OutOfBoundsDatetime:
        unparsed_deathdays_dict[df['id'].iloc[i]]=d
        parsed_deathdays_ls.append(np.nan)
        
df['deathday']=parsed_deathdays_ls


# Uncomment these to show problem dates.
# print(unparsed_birthdays_dict)
# print(unparsed_deathdays_dict)


df.to_sql('person',
          DB_MOVIES_SETUP_CONNECTION,
          schema='movies',
          index=False
         )


c.execute('''
            ALTER TABLE movies.person
                ALTER COLUMN id TYPE INTEGER,
                ALTER COLUMN adult TYPE BOOLEAN,
                ALTER COLUMN biography TYPE VARCHAR(5000),
                ALTER COLUMN birthday TYPE DATE,
                ALTER COLUMN deathday TYPE DATE,
                ALTER COLUMN gender TYPE INTEGER,
                ALTER COLUMN homepage TYPE VARCHAR(200),
                ALTER COLUMN imdb_id TYPE CHAR(10),
                ALTER COLUMN name TYPE VARCHAR(200),
                ALTER COLUMN place_of_birth TYPE VARCHAR(200),
                ALTER COLUMN popularity TYPE DOUBLE PRECISION;
          ''')

c.execute('''
            ALTER TABLE movies.person
                ADD CONSTRAINT person_pk
                    PRIMARY KEY (id);
          ''')


# Replace the problematic dates

c.execute('''
            UPDATE movies.person
            SET birthday='1628-01-12'
            WHERE id=44217;
            ''')

c.execute('''
            UPDATE movies.person
            SET deathday='1616-04-23'
            WHERE id=6210;
            ''')

c.execute('''
            UPDATE movies.person
            SET deathday='1937-03-17 00:00:00'
            WHERE id=1181173;
            ''')

print("Tables defined and populated")
print("Defining foreign key constraints")
      
c.execute('''
    ALTER TABLE movies.movie
    ADD CONSTRAINT movie_collection_fk
        FOREIGN KEY (belongs_to_collection) REFERENCES movies.collection;
    ''')

c.execute('''
    ALTER TABLE movies.person
    ADD CONSTRAINT person_gender_fk
        FOREIGN KEY (gender) REFERENCES movies.gender;
    ''')

c.execute('''
    ALTER TABLE movies.also_known_as
    ADD CONSTRAINT also_known_as_person_fk
        FOREIGN KEY (person_id) REFERENCES movies.person;
    ''')

c.execute('''
    ALTER TABLE movies.cast_member
    ADD CONSTRAINT cast_member_movie_fk
        FOREIGN KEY (movie_id) REFERENCES movies.movie;
    ''')

c.execute('''
    ALTER TABLE movies.cast_member
    ADD CONSTRAINT cast_member_person_fk
        FOREIGN KEY (person_id) REFERENCES movies.person;
    ''')

c.execute('''
    ALTER TABLE movies.cast_member
    ADD CONSTRAINT cast_member_gender_fk
        FOREIGN KEY (gender) REFERENCES movies.gender;
    ''')

c.execute('''
    ALTER TABLE movies.crew
    ADD CONSTRAINT crew_movie_fk
        FOREIGN KEY (movie_id) REFERENCES movies.movie;
    ''')

c.execute('''
    ALTER TABLE movies.crew
    ADD CONSTRAINT crew_person_fk
        FOREIGN KEY (person_id) REFERENCES movies.person;
    ''')

c.execute('''
    ALTER TABLE movies.crew
    ADD CONSTRAINT crew_gender_fk
        FOREIGN KEY (gender) REFERENCES movies.gender;
    ''')

c.execute('''
    ALTER TABLE movies.movie_genre
    ADD CONSTRAINT movie_genre_movie_fk
        FOREIGN KEY (movie_id) REFERENCES movies.movie;
    ''')

c.execute('''
    ALTER TABLE movies.movie_genre
    ADD CONSTRAINT movie_genre_genre_fk
        FOREIGN KEY (genre_id) REFERENCES movies.genre;
    ''')

c.execute('''
    ALTER TABLE movies.movie_language
    ADD CONSTRAINT movie_language_movie_fk
        FOREIGN KEY (movie_id) REFERENCES movies.movie;
    ''')

c.execute('''
    ALTER TABLE movies.movie_language
    ADD CONSTRAINT movie_language_spoken_language_fk
        FOREIGN KEY (language_id) REFERENCES movies.spoken_language;
    ''')

c.execute('''
    ALTER TABLE movies.movie_production_company
    ADD CONSTRAINT movie_production_company_movie_fk
        FOREIGN KEY (movie_id) REFERENCES movies.movie;
    ''')

c.execute('''
    ALTER TABLE movies.movie_production_company
    ADD CONSTRAINT movie_production_company_production_company_fk
        FOREIGN KEY (production_company_id) REFERENCES movies.production_company;
    ''')

c.execute('''
    ALTER TABLE movies.movie_production_country
    ADD CONSTRAINT movie_production_country_movie_fk
        FOREIGN KEY (movie_id) REFERENCES movies.movie;
    ''')

c.execute('''
    ALTER TABLE movies.movie_production_country
    ADD CONSTRAINT movie_production_country_production_country_fk
        FOREIGN KEY (production_country_id) REFERENCES movies.production_country;
    ''')
      
print("done")

c.close()
tm351_admin_conn.commit()







