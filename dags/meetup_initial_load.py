from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from dags.utils.slack_notifier import (
    on_start_task_callback,
    on_failure_callback,
    on_success_task_callback,
    on_success_dag_callback
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': on_failure_callback,
    'on_success_callback': on_success_task_callback,
}

with DAG(
    dag_id='meetup_initial_load',
    default_args=default_args,
    description='Carga inicial de datos proveniente de los CSV',
    schedule=None,
    catchup=False,
    max_active_runs=1,
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
            localized_country_name VARCHAR,
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
            normalised_rating VARCHAR
        );
        """,
        on_execute_callback=on_start_task_callback
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
    load_data_silver = SQLExecuteQueryOperator(
            task_id='load_data_silver',
            conn_id='conn_snowflake_rappi',
            sql="""   
            CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.SILVER.CITIES AS
            SELECT 
                TRY_CAST(city_id AS INT) AS city_id,
                TRIM(NULLIF(city, '')) AS city,
                TRIM(NULLIF(country, '')) AS country,
                TRY_CAST(distance AS FLOAT) AS distance,
                TRY_CAST(latitude AS FLOAT) AS latitude,
                TRIM(NULLIF(localized_country_name, '')) AS localized_country_name,
                TRY_CAST(longitude AS FLOAT) AS longitude,
                TRY_CAST(member_count AS INT) AS member_count,
                TRY_CAST(ranking AS INT) AS ranking,
                TRIM(NULLIF(state, '')) AS state,
                TRIM(NULLIF(zip, '')) AS zip
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.CITIES
            WHERE TRY_CAST(city_id AS INT) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY TRY_CAST(city_id AS INT) 
                ORDER BY TRY_CAST(member_count AS INT) DESC NULLS LAST
            ) = 1;

            CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.SILVER.CATEGORIES AS
            SELECT 
                TRY_CAST(category_id AS INT) AS category_id,
                TRIM(NULLIF(category_name, '')) AS category_name,
                TRIM(NULLIF(shortname, '')) AS shortname,
                TRIM(NULLIF(sort_name, '')) AS sort_name
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.CATEGORIES
            WHERE TRY_CAST(category_id AS INT) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY TRY_CAST(category_id AS INT) 
                ORDER BY category_name ASC
            ) = 1;

            CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.SILVER.VENUES AS
            SELECT 
                TRY_CAST(venue_id AS BIGINT) AS venue_id,
                TRIM(NULLIF(venue_name, '')) AS venue_name,
                TRIM(NULLIF(address_1, '')) AS address_1,
                TRIM(NULLIF(city, '')) AS city,
                TRIM(NULLIF(state, '')) AS state,
                TRIM(NULLIF(country, '')) AS country,
                TRIM(NULLIF(localized_country_name, '')) AS localized_country_name,
                TRIM(NULLIF(zip, '')) AS zip,
                TRY_CAST(lat AS FLOAT) AS latitude,
                TRY_CAST(lon AS FLOAT) AS longitude,
                TRY_CAST(distance AS FLOAT) AS distance,
                TRY_CAST(rating AS FLOAT) AS rating,
                TRY_CAST(rating_count AS INT) AS rating_count,
                TRY_CAST(normalised_rating AS FLOAT) AS normalised_rating
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.VENUES
            WHERE TRY_CAST(venue_id AS BIGINT) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY TRY_CAST(venue_id AS BIGINT) 
                ORDER BY TRY_CAST(rating_count AS INT) DESC NULLS LAST
            ) = 1;

            INSERT INTO PRUEBA_TECNICA_RAPPI.SILVER.MEMBERS (
                member_id,
                member_name,
                bio,
                hometown,
                city,
                state,
                country,
                link,
                latitude,
                longitude
            )
            SELECT 
                TRY_CAST(member_id AS BIGINT) AS member_id,
                TRIM(NULLIF(member_name, '')) AS member_name,
                TRIM(NULLIF(bio, '')) AS bio,
                TRIM(NULLIF(hometown, '')) AS hometown,
                TRIM(NULLIF(city, '')) AS city,
                TRIM(NULLIF(state, '')) AS state,
                TRIM(NULLIF(country, '')) AS country,
                TRIM(NULLIF(link, '')) AS link,
                TRY_CAST(lat AS FLOAT) AS latitude,
                TRY_CAST(lon AS FLOAT) AS longitude
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.MEMBERS
            WHERE TRY_CAST(member_id AS BIGINT) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY TRY_CAST(member_id AS BIGINT) 
                ORDER BY member_name ASC
            ) = 1;

            CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.SILVER.TOPICS AS
            SELECT 
                TRY_CAST(topic_id AS INT) AS topic_id,
                TRIM(NULLIF(topic_name, '')) AS topic_name,
                TRIM(NULLIF(description, '')) AS description,
                TRIM(NULLIF(link, '')) AS link,
                TRIM(NULLIF(urlkey, '')) AS urlkey,
                TRY_CAST(members AS INT) AS members_count,
                TRY_CAST(NULLIF(main_topic_id, '-1') AS INT) AS main_topic_id
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.TOPICS
            WHERE TRY_CAST(topic_id AS INT) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY TRY_CAST(topic_id AS INT) 
                ORDER BY TRY_CAST(members AS INT) DESC NULLS LAST
            ) = 1;

            INSERT INTO PRUEBA_TECNICA_RAPPI.SILVER.GROUPS (
                group_id,
                group_name,
                created_at,
                description,
                join_mode,
                link,
                urlname,
                visibility,
                who,
                members_count,
                rating,
                latitude,
                longitude,
                timezone,
                utc_offset,
                group_photo_id,
                group_photo_base_url,
                group_photo_link,
                group_photo_highres_link,
                group_photo_thumb_link,
                group_photo_type,
                organizer_id,
                organizer_photo_id,
                organizer_photo_base_url,
                organizer_photo_link,
                organizer_photo_highres_link,
                organizer_photo_thumb_link,
                organizer_photo_type,
                category_id,
                city_id
            )
            SELECT 
                TRY_CAST(group_id AS BIGINT) AS group_id,
                TRIM(NULLIF(group_name, '')) AS group_name,
                TRY_TO_TIMESTAMP_NTZ(created) AS created_at,
                TRIM(NULLIF(description, '')) AS description,
                TRIM(NULLIF(join_mode, '')) AS join_mode,
                TRIM(NULLIF(link, '')) AS link,
                TRIM(NULLIF(urlname, '')) AS urlname,
                TRIM(NULLIF(visibility, '')) AS visibility,
                TRIM(NULLIF(who, '')) AS who,
                TRY_CAST(members AS INT) AS members_count,
                TRY_CAST(rating AS FLOAT) AS rating,
                TRY_CAST(lat AS FLOAT) AS latitude,
                TRY_CAST(lon AS FLOAT) AS longitude,
                TRIM(NULLIF(timezone, '')) AS timezone,
                TRY_CAST(utc_offset AS INT) AS utc_offset,
                TRY_CAST("group_photo.photo_id" AS BIGINT) AS group_photo_id,
                TRIM(NULLIF("group_photo.base_url", '')) AS group_photo_base_url,
                TRIM(NULLIF("group_photo.photo_link", '')) AS group_photo_link,
                TRIM(NULLIF("group_photo.highres_link", '')) AS group_photo_highres_link,
                TRIM(NULLIF("group_photo.thumb_link", '')) AS group_photo_thumb_link,
                TRIM(NULLIF("group_photo.type", '')) AS group_photo_type,
                TRY_CAST("organizer.member_id" AS BIGINT) AS organizer_id,
                TRY_CAST("organizer.photo.photo_id" AS BIGINT) AS organizer_photo_id,
                TRIM(NULLIF("organizer.photo.base_url", '')) AS organizer_photo_base_url,
                TRIM(NULLIF("organizer.photo.photo_link", '')) AS organizer_photo_link,
                TRIM(NULLIF("organizer.photo.highres_link", '')) AS organizer_photo_highres_link,
                TRIM(NULLIF("organizer.photo.thumb_link", '')) AS organizer_photo_thumb_link,
                TRIM(NULLIF("organizer.photo.type", '')) AS organizer_photo_type,
                TRY_CAST(category_id AS INT) AS category_id,
                TRY_CAST(city_id AS INT) AS city_id
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS
            WHERE TRY_CAST(group_id AS BIGINT) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY TRY_CAST(group_id AS BIGINT) 
                ORDER BY TRY_TO_TIMESTAMP_NTZ(created) DESC NULLS LAST
            ) = 1;

            INSERT INTO PRUEBA_TECNICA_RAPPI.SILVER.EVENTS (
                event_id,
                event_name,
                created_at,
                event_time,
                updated_at,
                utc_offset,
                duration_ms,
                event_status,
                visibility,
                description,
                event_url,
                photo_url,
                how_to_find_us,
                why,
                yes_rsvp_count,
                maybe_rsvp_count,
                waitlist_count,
                headcount,
                rsvp_limit,
                rating_average,
                rating_count,
                fee_amount,
                fee_currency,
                fee_accepts,
                fee_label,
                fee_description,
                fee_required,
                group_id,
                venue_id
            )
            SELECT 
                TRIM(event_id) AS event_id,
                TRIM(NULLIF(event_name, '')) AS event_name,
                TRY_TO_TIMESTAMP_NTZ(created) AS created_at,
                TRY_TO_TIMESTAMP_NTZ(event_time) AS event_time,
                TRY_TO_TIMESTAMP_NTZ(updated) AS updated_at,
                TRY_CAST(utc_offset AS INT) AS utc_offset,
                TRY_CAST(duration AS BIGINT) AS duration_ms,
                TRIM(NULLIF(event_status, '')) AS event_status,
                TRIM(NULLIF(visibility, '')) AS visibility,
                TRIM(NULLIF(description, '')) AS description,
                TRIM(NULLIF(event_url, '')) AS event_url,
                TRIM(NULLIF(photo_url, '')) AS photo_url,
                TRIM(NULLIF(how_to_find_us, '')) AS how_to_find_us,
                TRIM(NULLIF(why, '')) AS why,
                TRY_CAST(yes_rsvp_count AS INT) AS yes_rsvp_count,
                TRY_CAST(maybe_rsvp_count AS INT) AS maybe_rsvp_count,
                TRY_CAST(waitlist_count AS INT) AS waitlist_count,
                TRY_CAST(headcount AS INT) AS headcount,
                TRY_CAST(rsvp_limit AS INT) AS rsvp_limit,
                TRY_CAST("rating.average" AS FLOAT) AS rating_average,
                TRY_CAST("rating.count" AS INT) AS rating_count,
                TRY_CAST("fee.amount" AS FLOAT) AS fee_amount,
                TRIM(NULLIF("fee.currency", '')) AS fee_currency,
                TRIM(NULLIF("fee.accepts", '')) AS fee_accepts,
                TRIM(NULLIF("fee.label", '')) AS fee_label,
                TRIM(NULLIF("fee.description", '')) AS fee_description,
                TRY_CAST("fee.required" AS BOOLEAN) AS fee_required,
                TRY_CAST(group_id AS BIGINT) AS group_id,
                TRY_CAST(venue_id AS BIGINT) AS venue_id
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.EVENTS
            WHERE NULLIF(TRIM(event_id), '') IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY TRIM(event_id) 
                ORDER BY TRY_TO_TIMESTAMP_NTZ(event_time) DESC NULLS LAST
            ) = 1;

            CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.SILVER.GROUP_TOPICS AS
            SELECT DISTINCT
                TRY_CAST(group_id AS BIGINT) AS group_id,
                TRY_CAST(topic_id AS INT) AS topic_id
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS_TOPICS
            WHERE TRY_CAST(group_id AS BIGINT) IS NOT NULL
            AND TRY_CAST(topic_id AS INT) IS NOT NULL;

            INSERT INTO PRUEBA_TECNICA_RAPPI.SILVER.GROUP_MEMBERS (
                group_id,
                member_id,
                joined_at,
                member_status,
                last_visited_at
            )
            SELECT 
                TRY_CAST(group_id AS BIGINT) AS group_id,
                TRY_CAST(member_id AS BIGINT) AS member_id,
                TRY_TO_TIMESTAMP_NTZ(joined) AS joined_at,
                TRIM(NULLIF(member_status, '')) AS member_status,
                TRY_TO_TIMESTAMP_NTZ(visited) AS last_visited_at
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.MEMBERS
            WHERE TRY_CAST(group_id AS BIGINT) IS NOT NULL
            AND TRY_CAST(member_id AS BIGINT) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY TRY_CAST(group_id AS BIGINT), TRY_CAST(member_id AS BIGINT)
                ORDER BY TRY_TO_TIMESTAMP_NTZ(joined) DESC NULLS LAST
            ) = 1;

            CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.SILVER.MEMBER_TOPICS AS
            SELECT DISTINCT
                TRY_CAST(member_id AS BIGINT) AS member_id,
                TRY_CAST(topic_id AS INT) AS topic_id
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.MEMBERS_TOPICS
            WHERE TRY_CAST(member_id AS BIGINT) IS NOT NULL
            AND TRY_CAST(topic_id AS INT) IS NOT NULL;
            """
        )
    load_data_gold = SQLExecuteQueryOperator(
            task_id='load_data_gold',
            conn_id='conn_snowflake_rappi',
            sql="""   
            CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.GOLD.FACT_EVENT_DEMAND AS
            SELECT 
                e.event_id,
                e.event_name,
                e.event_time,
                DATE(e.event_time) AS event_date,
                g.group_id,
                g.group_name,
                c.category_name,
                v.venue_id,
                v.venue_name,
                v.city AS venue_city,
                v.country AS venue_country,
                e.yes_rsvp_count AS confirmed_rsvps,
                e.waitlist_count,
                e.rsvp_limit,
                CASE 
                    WHEN e.rsvp_limit IS NOT NULL AND e.rsvp_limit > 0 THEN 
                        ROUND((e.yes_rsvp_count / e.rsvp_limit) * 100, 2)
                    ELSE NULL 
                END AS projected_occupancy_pct,
                CASE 
                    WHEN e.waitlist_count > 0 THEN 'Sobrevendido'
                    WHEN e.rsvp_limit IS NOT NULL AND e.rsvp_limit > 0 AND (e.yes_rsvp_count / e.rsvp_limit) >= 0.8 THEN 'Casi Lleno'
                    WHEN e.rsvp_limit IS NULL OR e.rsvp_limit <= 0 THEN 'Sin Limite de Capacidad'
                    ELSE 'Asientos disponibles'
                END AS demand_status,
                e.fee_required,
                e.fee_amount,
                e.fee_currency,
                CASE 
                    WHEN UPPER(e.fee_required) = 'TRUE' AND e.fee_amount > 0 THEN (e.yes_rsvp_count * e.fee_amount)
                    ELSE 0 
                END AS projected_gross_revenue
            FROM PRUEBA_TECNICA_RAPPI.SILVER.EVENTS e
            LEFT JOIN PRUEBA_TECNICA_RAPPI.SILVER.GROUPS g 
                ON e.group_id = g.group_id
            LEFT JOIN PRUEBA_TECNICA_RAPPI.SILVER.CATEGORIES c 
                ON g.category_id = c.category_id
            LEFT JOIN PRUEBA_TECNICA_RAPPI.SILVER.VENUES v 
                ON e.venue_id = v.venue_id;

            CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.GOLD.FACT_AGG_GROUP_ENGAGEMENT_MONTHLY AS
            SELECT 
                TO_NUMBER(TO_VARCHAR(e.event_time, 'YYYYMM')) AS activity_month,
                g.group_id,
                g.group_name,
                g.members_count AS total_group_members,
                c.category_name,
                ci.city AS group_city,
                ci.state AS group_state,
                ci.country AS group_country,
                COUNT(DISTINCT e.event_id) AS total_events_created,
                SUM(e.yes_rsvp_count) AS total_rsvps_generated,
                SUM(e.waitlist_count) AS total_waitlist_generated,
                ROUND(SUM(e.yes_rsvp_count) / NULLIF(COUNT(DISTINCT e.event_id), 0), 2) AS avg_rsvps_per_event,
                SUM(
                    CASE 
                        WHEN UPPER(e.fee_required) = 'TRUE' AND e.fee_amount > 0 THEN (e.yes_rsvp_count * e.fee_amount)
                        ELSE 0 
                    END
                ) AS monthly_projected_revenue
            FROM PRUEBA_TECNICA_RAPPI.SILVER.GROUPS g
            LEFT JOIN PRUEBA_TECNICA_RAPPI.SILVER.CATEGORIES c 
                ON g.category_id = c.category_id
            LEFT JOIN PRUEBA_TECNICA_RAPPI.SILVER.CITIES ci 
                ON g.city_id = ci.city_id
            -- Traemos los eventos asociados a cada grupo
            INNER JOIN PRUEBA_TECNICA_RAPPI.SILVER.EVENTS e 
                ON g.group_id = e.group_id
            GROUP BY 
                TO_NUMBER(TO_VARCHAR(e.event_time, 'YYYYMM')),
                g.group_id,
                g.group_name,
                g.members_count,
                c.category_name,
                ci.city,
                ci.state,
                ci.country;

            CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.GOLD.DIM_MEMBER_360 AS
            SELECT 
                m.member_id,
                m.member_name,
                m.city AS member_city,
                m.state AS member_state,
                m.country AS member_country,
                COUNT(DISTINCT gm.group_id) AS total_groups_joined,
                MIN(gm.joined_at)::DATE AS first_group_joined_date,
                MAX(gm.joined_at)::DATE AS last_group_joined_date,
                DATEDIFF('day', MIN(gm.joined_at), CURRENT_DATE()) AS member_tenure_days,
                CASE 
                    WHEN COUNT(DISTINCT gm.group_id) >= 5 THEN 'Alta participación'
                    WHEN COUNT(DISTINCT gm.group_id) BETWEEN 2 AND 4 THEN 'Mediana participación'
                    ELSE 'Miembro de un solo grupo'
                END AS member_segment
            FROM PRUEBA_TECNICA_RAPPI.SILVER.MEMBERS m
            INNER JOIN PRUEBA_TECNICA_RAPPI.SILVER.GROUP_MEMBERS gm 
                ON m.member_id = gm.member_id
            GROUP BY 
                m.member_id,
                m.member_name,
                m.city,
                m.state,
                m.country;
            """
        )
    load_data_s3 = SQLExecuteQueryOperator(
            task_id='load_data_s3',
            conn_id='conn_snowflake_rappi',
            sql="""   
            COPY INTO @PRUEBA_TECNICA_RAPPI.GOLD.S3_GOLD_STAGE/DIM_MEMBER_360_{{ data_interval_end.strftime('%Y%m%d_%H%M') }}/
            FROM PRUEBA_TECNICA_RAPPI.GOLD.DIM_MEMBER_360
            FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
            OVERWRITE = TRUE;
            
            COPY INTO @PRUEBA_TECNICA_RAPPI.GOLD.S3_GOLD_STAGE/FACT_AGG_GROUP_ENGAGEMENT_MONTHLY_{{ data_interval_end.str('%Y%m%d_%H%M') }}/
            FROM PRUEBA_TECNICA_RAPPI.GOLD.FACT_AGG_GROUP_ENGAGEMENT_MONTHLY
            FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
            OVERWRITE = TRUE;
            
            COPY INTO @PRUEBA_TECNICA_RAPPI.GOLD.S3_GOLD_STAGE/FACT_EVENT_DEMAND_{{ data_interval_end.str('%Y%m%d_%H%M') }}/
            FROM PRUEBA_TECNICA_RAPPI.GOLD.FACT_EVENT_DEMAND
            FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
            OVERWRITE = TRUE;
            """,
            on_success_callback=on_success_dag_callback
        )
    

create_tables_meetup >> load_data_bronze >> load_data_silver  >> load_data_gold >> load_data_s3