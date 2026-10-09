from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='meetup_initial_load',
    default_args=default_args,
    description='Carga inicial de datos proveniente de los CSV',
    schedule='*/15 * * * *',
    catchup=False,
    tags=['meetup', 'snowflake'],
) as dag:

    

    create_tables_meetup = SQLExecuteQueryOperator(
        task_id='create_tables_meetup',
        conn_id='conn_snowflake_rappi',
        sql="""

        CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.BRONZE.CATEGORIES (
            category_id VARCHAR,
            category_name VARCHAR,
            shortname VARCHAR,
            sort_name VARCHAR
        );

        CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.BRONZE.CITIES (
            city VARCHAR,
            city_id VARCHAR,
            country VARCHAR,
            distance VARCHAR,
            latitude VARCHAR,
            l       ocalized_country_name VARCHAR,
            longitude VARCHAR,
            member_count VARCHAR,
            ranking VARCHAR,
            state VARCHAR,
            zip VARCHAR
        );

        CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.BRONZE.EVENTS (
            event_id VARCHAR,
            created VARCHAR,
            description VARCHAR,
            duration VARCHAR,
            event_url VARCHAR,
            "fee.accepts" VARCHAR,
            "fee.amount" VARCHAR,
            "fee.currency" VARCHAR,
            "fee.description" VARCHAR,
            "fee.label" VARCHAR,
            "fee.required" VARCHAR,
            "group.created" VARCHAR,
            "group.group_lat" VARCHAR,
            "group.group_lon" VARCHAR,
            group_id VARCHAR,
            "group.join_mode" VARCHAR,
            "group.name" VARCHAR,
            "group.urlname" VARCHAR,
            "group.who" VARCHAR,
            headcount VARCHAR,
            how_to_find_us VARCHAR,
            maybe_rsvp_count VARCHAR,
            event_name VARCHAR,
            photo_url VARCHAR,
            "rating.average" VARCHAR,
            "rating.count" VARCHAR,
            rsvp_limit VARCHAR,
            event_status VARCHAR,
            event_time VARCHAR,
            updated VARCHAR,
            utc_offset VARCHAR,
            "venue.address_1" VARCHAR,
            "venue.address_2" VARCHAR,
            "venue.city" VARCHAR,
            "venue.country" VARCHAR,
            venue_id VARCHAR,
            "venue.lat" VARCHAR,
            "venue.localized_country_name" VARCHAR,
            "venue.lon" VARCHAR,
            "venue.name" VARCHAR,
            "venue.phone" VARCHAR,
            "venue.repinned" VARCHAR,
            "venue.state" VARCHAR,
            "venue.zip" VARCHAR,
            visibility VARCHAR,
            waitlist_count VARCHAR,
            why VARCHAR,
            yes_rsvp_count VARCHAR
        );

        CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS (
            group_id VARCHAR,
            category_id VARCHAR,
            "category.name" VARCHAR,
            "category.shortname" VARCHAR,
            city_id VARCHAR,
            city VARCHAR,
            country VARCHAR,
            created VARCHAR,
            description VARCHAR,
            "group_photo.base_url" VARCHAR,
            "group_photo.highres_link" VARCHAR,
            "group_photo.photo_id" VARCHAR,
            "group_photo.photo_link" VARCHAR,
            "group_photo.thumb_link" VARCHAR,
            "group_photo.type" VARCHAR,
            join_mode VARCHAR,
            lat VARCHAR,
            link VARCHAR,
            lon VARCHAR,
            members VARCHAR,
            group_name VARCHAR,
            "organizer.member_id" VARCHAR,
            "organizer.name" VARCHAR,
            "organizer.photo.base_url" VARCHAR,
            "organizer.photo.highres_link" VARCHAR,
            "organizer.photo.photo_id" VARCHAR,
            "organizer.photo.photo_link" VARCHAR,
            "organizer.photo.thumb_link" VARCHAR,
            "organizer.photo.type" VARCHAR,
            rating VARCHAR,
            state VARCHAR,
            timezone VARCHAR,
            urlname VARCHAR,
            utc_offset VARCHAR,
            visibility VARCHAR,
            who VARCHAR
        );

        CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS_TOPICS (
            topic_id VARCHAR,
            topic_key VARCHAR,
            topic_name VARCHAR,
            group_id VARCHAR
        );

        CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.BRONZE.MEMBERS (
            member_id VARCHAR,
            bio VARCHAR,
            city VARCHAR,
            country VARCHAR,
            hometown VARCHAR,
            joined VARCHAR,
            lat VARCHAR,
            link VARCHAR,
            lon VARCHAR,
            member_name VARCHAR,
            state VARCHAR,
            member_status VARCHAR,
            visited VARCHAR,
            group_id VARCHAR
        );

        CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.BRONZE.MEMBERS_TOPICS (
            topic_id VARCHAR,
            topic_key VARCHAR,
            topic_name VARCHAR,
            member_id VARCHAR
        );

        CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.BRONZE.TOPICS (
            topic_id VARCHAR,
            description VARCHAR,
            link VARCHAR,
            members VARCHAR,
            topic_name VARCHAR,
            urlkey VARCHAR,
            main_topic_id VARCHAR
        );

        CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.BRONZE.VENUES (
            venue_id VARCHAR,
            address_1 VARCHAR,
            city VARCHAR,
            country VARCHAR,
            distance VARCHAR,
            lat VARCHAR,
            localized_country_name VARCHAR,
            lon VARCHAR,
            venue_name VARCHAR,
            rating VARCHAR,
            rating_count VARCHAR,
            state VARCHAR,
            zip VARCHAR,
            normalised_rating VARCHARhttps://stackoverflow.com/questions/29973357/how-do-you-format-code-in-visual-studio-code-vscode
        );
        """
    )

    load_data_bronze = SQLExecuteQueryOperator(
        task_id='load_data_bronze',
        conn_id='conn_snowflake_rappi',
        sql="""

            COPY INTO PRUEBA_TECNICA_RAPPI.BRONZE.CATEGORIES
            FROM @PRUEBA_TECNICA_RAPPI.BRONZE.STAGING_CSV/categories.csv
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = 'CONTINUE';

            COPY INTO PRUEBA_TECNICA_RAPPI.BRONZE.CITIES
            FROM @PRUEBA_TECNICA_RAPPI.BRONZE.STAGING_CSV/cities.csv
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = 'CONTINUE';

            COPY INTO PRUEBA_TECNICA_RAPPI.BRONZE.EVENTS
            FROM @PRUEBA_TECNICA_RAPPI.BRONZE.STAGING_CSV/events.csv
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = 'CONTINUE';

            COPY INTO PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS
            FROM @PRUEBA_TECNICA_RAPPI.BRONZE.STAGING_CSV/groups.csv
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = 'CONTINUE';

            COPY INTO PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS_TOPICS
            FROM @PRUEBA_TECNICA_RAPPI.BRONZE.STAGING_CSV/groups_topics.csv
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = 'CONTINUE';

            COPY INTO PRUEBA_TECNICA_RAPPI.BRONZE.MEMBERS
            FROM @PRUEBA_TECNICA_RAPPI.BRONZE.STAGING_CSV/members.csv
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = 'CONTINUE';

            COPY INTO PRUEBA_TECNICA_RAPPI.BRONZE.MEMBERS_TOPICS
            FROM @PRUEBA_TECNICA_RAPPI.BRONZE.STAGING_CSV/members_topics.csv
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = 'CONTINUE';

            COPY INTO PRUEBA_TECNICA_RAPPI.BRONZE.TOPICS
            FROM @PRUEBA_TECNICA_RAPPI.BRONZE.STAGING_CSV/topics.csv
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = 'CONTINUE';


            COPY INTO PRUEBA_TECNICA_RAPPI.BRONZE.VENUES
            FROM @PRUEBA_TECNICA_RAPPI.BRONZE.STAGING_CSV/venues.csv
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = 'CONTINUE';
        """
    )

create_tables_meetup >> load_data_bronze