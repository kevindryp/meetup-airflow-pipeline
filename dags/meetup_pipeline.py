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
    dag_id='meetup_pipeline',
    default_args=default_args,
    description='Ejecuta el flujo completo de datos hasta la respetiva capa analitica',
    schedule='*/15 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['meetup', 'snowflake'],
) as dag:

    generate_data_delta = SQLExecuteQueryOperator(
        task_id='generate_data_delta',
        conn_id='conn_snowflake_rappi',
        sql="""

        CREATE TABLE IF NOT EXISTS PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS_DELTA LIKE PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS;
        ALTER TABLE PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS_DELTA ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP_NTZ;
        INSERT INTO PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS_DELTA (
            group_id, 
            group_name,
            created, 
            visibility,
            "organizer.member_id",
            category_id,
            city_id,
            ingested_at
        )
        SELECT 
            UNIFORM(30000000, 99999999, RANDOM())::VARCHAR AS group_id,
            'Community Group ' || UNIFORM(1, 1000, RANDOM())::VARCHAR AS group_name,
            CURRENT_TIMESTAMP()::VARCHAR AS created,
            'public' AS visibility,
            m.organizer_id AS "organizer.member_id",
            cat.category_id,
            c.city_id,
            CURRENT_TIMESTAMP() AS ingested_at
        FROM (
            SELECT member_id AS organizer_id 
            FROM PRUEBA_TECNICA_RAPPI.SILVER.MEMBERS 
            SAMPLE (2 ROWS)
        ) m
        CROSS JOIN (
            SELECT category_id 
            FROM PRUEBA_TECNICA_RAPPI.SILVER.CATEGORIES 
            SAMPLE (1 ROWS)
        ) cat
        CROSS JOIN (
            SELECT city_id 
            FROM PRUEBA_TECNICA_RAPPI.SILVER.CITIES 
            SAMPLE (1 ROWS)
        ) c;

        CREATE TABLE IF NOT EXISTS PRUEBA_TECNICA_RAPPI.BRONZE.MEMBERS_DELTA 
        LIKE PRUEBA_TECNICA_RAPPI.BRONZE.MEMBERS;
        ALTER TABLE PRUEBA_TECNICA_RAPPI.BRONZE.MEMBERS_DELTA 
        ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP_NTZ;
        INSERT INTO PRUEBA_TECNICA_RAPPI.BRONZE.MEMBERS_DELTA (
            member_id,
            member_name,
            bio,
            city,
            country,
            hometown,
            joined,
            lat,
            link,
            lon,
            state,
            member_status,
            visited,
            group_id,
            ingested_at
        )
        SELECT 
            UNIFORM(80000000, 89999999, RANDOM())::VARCHAR AS member_id,
            'User_' || UUID_STRING() AS member_name,
            'Apasionado por la tecnología, ciencia de datos y eventos comunitarios.' AS bio,
            c.city,
            c.country,
            c.city AS hometown,
            CURRENT_TIMESTAMP()::VARCHAR AS joined,
            '4.6097' AS lat,
            'https://www.meetup.com/members/' || UNIFORM(100000, 999999, RANDOM())::VARCHAR AS link,
            '-74.0817' AS lon,
            'Cundinamarca' AS state,
            'active' AS member_status,
            CURRENT_TIMESTAMP()::VARCHAR AS visited,
            g.group_id,
            CURRENT_TIMESTAMP() AS ingested_at
        FROM (
            SELECT city, country 
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.CITIES SAMPLE (3 ROWS)
            WHERE city IS NOT NULL 
        ) c
        CROSS JOIN (
            SELECT group_id 
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS SAMPLE (1 ROWS)
        ) g;

        CREATE TABLE IF NOT EXISTS PRUEBA_TECNICA_RAPPI.BRONZE.EVENTS_DELTA LIKE PRUEBA_TECNICA_RAPPI.BRONZE.EVENTS;
        ALTER TABLE PRUEBA_TECNICA_RAPPI.BRONZE.EVENTS_DELTA ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP_NTZ;
        INSERT INTO PRUEBA_TECNICA_RAPPI.BRONZE.EVENTS_DELTA (
            event_id,
            group_id,
            venue_id,
            event_name,
            description,
            event_status,
            rsvp_limit,
            yes_rsvp_count,
            maybe_rsvp_count,
            waitlist_count,
            created,
            event_time,
            duration,
            event_url,
            visibility,
            how_to_find_us,
            ingested_at
        )
        SELECT 
            'EVT_' || UUID_STRING() AS event_id,
            g.group_id,
            v.venue_id,
            'Meetup Session ' || UNIFORM(100, 999, RANDOM())::VARCHAR AS event_name,
            'Encuentro dinámico para discutir temas de innovación, arquitectura de datos y comunidad.' AS description,
            'upcoming' AS event_status,
            UNIFORM(20, 200, RANDOM())::VARCHAR AS rsvp_limit,
            UNIFORM(5, 50, RANDOM())::VARCHAR AS yes_rsvp_count,
            UNIFORM(0, 10, RANDOM())::VARCHAR AS maybe_rsvp_count,
            '0' AS waitlist_count,
            CURRENT_TIMESTAMP()::VARCHAR AS created,
            DATEADD(day, UNIFORM(1, 30, RANDOM()), CURRENT_TIMESTAMP())::VARCHAR AS event_time,
            '7200000' AS duration,
            'https://www.meetup.com/events/' || UUID_STRING() AS event_url,
            'public' AS visibility,
            'En la entrada principal del auditorio.' AS how_to_find_us,
            CURRENT_TIMESTAMP() AS ingested_at
        FROM (
            SELECT group_id 
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS 
            SAMPLE (3 ROWS)
        ) g
        CROSS JOIN (
            SELECT venue_id 
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.VENUES SAMPLE (1 ROWS)
            WHERE venue_id IS NOT NULL 
        ) v;
        """,
        on_execute_callback=on_start_task_callback
    )

    load_data_silver = SQLExecuteQueryOperator(
            task_id='load_data_silver',
            conn_id='conn_snowflake_rappi',
            sql="""
    
            MERGE INTO PRUEBA_TECNICA_RAPPI.SILVER.GROUPS AS target
            USING (
                SELECT 
                    group_id,
                    group_name,
                    created::TIMESTAMP_NTZ AS created_at,
                    visibility,
                    "organizer.member_id" AS organizer_id,
                    category_id,
                    city_id
                FROM PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS_DELTA
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY group_id 
                    ORDER BY COALESCE(ingested_at, CURRENT_TIMESTAMP()) DESC
                ) = 1
            ) AS source
            ON target.group_id = source.group_id
            WHEN MATCHED THEN
                UPDATE SET 
                    target.group_name = source.group_name,
                    target.visibility = source.visibility,
                    target.organizer_id = source.organizer_id,
                    target.category_id = source.category_id,
                    target.city_id = source.city_id
            WHEN NOT MATCHED THEN
                INSERT (group_id, group_name, created_at, visibility, organizer_id, category_id, city_id)
                VALUES (source.group_id, source.group_name, source.created_at, source.visibility, source.organizer_id, source.category_id, source.city_id);

            MERGE INTO PRUEBA_TECNICA_RAPPI.SILVER.MEMBERS AS target
            USING (
                SELECT 
                    member_id, 
                    member_name, 
                    bio, 
                    city, 
                    country, 
                    hometown, 
                    lat::FLOAT AS latitude, 
                    link, 
                    lon::FLOAT AS longitude, 
                    state
                FROM PRUEBA_TECNICA_RAPPI.BRONZE.MEMBERS_DELTA
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY member_id 
                    ORDER BY COALESCE(ingested_at, CURRENT_TIMESTAMP()) DESC
                ) = 1
            ) AS source
            ON target.member_id = source.member_id
            WHEN MATCHED THEN
                UPDATE SET 
                    target.member_name = source.member_name, 
                    target.bio = source.bio, 
                    target.city = source.city, 
                    target.country = source.country, 
                    target.hometown = source.hometown,
                    target.latitude = source.latitude,
                    target.link = source.link,
                    target.longitude = source.longitude,
                    target.state = source.state
            WHEN NOT MATCHED THEN
                INSERT (
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
                VALUES (
                    source.member_id, 
                    source.member_name, 
                    source.bio, 
                    source.hometown, 
                    source.city, 
                    source.state, 
                    source.country, 
                    source.link, 
                    source.latitude, 
                    source.longitude
                );

            MERGE INTO PRUEBA_TECNICA_RAPPI.SILVER.GROUP_MEMBERS AS target
            USING (
                SELECT 
                    group_id,
                    member_id,
                    joined::TIMESTAMP_NTZ AS joined_at
                FROM PRUEBA_TECNICA_RAPPI.BRONZE.MEMBERS_DELTA
                WHERE group_id IS NOT NULL AND member_id IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY group_id, member_id 
                    ORDER BY COALESCE(ingested_at, CURRENT_TIMESTAMP()) DESC
                ) = 1
            ) AS source
            ON target.group_id = source.group_id 
            AND target.member_id = source.member_id
            WHEN MATCHED THEN
                UPDATE SET 
                    target.joined_at = source.joined_at
            WHEN NOT MATCHED THEN
                INSERT (group_id, member_id, joined_at)
                VALUES (source.group_id, source.member_id, source.joined_at);

            MERGE INTO PRUEBA_TECNICA_RAPPI.SILVER.EVENTS AS target
            USING (
                SELECT 
                    event_id,
                    group_id,
                    venue_id,
                    event_name,
                    description,
                    event_status,
                    rsvp_limit::INT AS rsvp_limit,
                    yes_rsvp_count::INT AS yes_rsvp_count,
                    maybe_rsvp_count::INT AS maybe_rsvp_count,
                    waitlist_count::INT AS waitlist_count,
                    created::TIMESTAMP_NTZ AS created_at,
                    event_time::TIMESTAMP_NTZ AS event_time,
                    duration::INT AS duration_ms,
                    event_url,
                    visibility,
                    how_to_find_us
                FROM PRUEBA_TECNICA_RAPPI.BRONZE.EVENTS_DELTA
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY event_id 
                    ORDER BY COALESCE(ingested_at, CURRENT_TIMESTAMP()) DESC
                ) = 1
            ) AS source
            ON target.event_id = source.event_id
            WHEN MATCHED THEN
                UPDATE SET 
                    target.group_id = source.group_id,
                    target.venue_id = source.venue_id,
                    target.event_name = source.event_name,
                    target.description = source.description,
                    target.event_status = source.event_status,
                    target.rsvp_limit = source.rsvp_limit,
                    target.yes_rsvp_count = source.yes_rsvp_count,
                    target.maybe_rsvp_count = source.maybe_rsvp_count,
                    target.waitlist_count = source.waitlist_count,
                    target.event_time = source.event_time,
                    target.duration_ms = source.duration_ms,
                    target.event_url = source.event_url,
                    target.visibility = source.visibility,
                    target.how_to_find_us = source.how_to_find_us
            WHEN NOT MATCHED THEN
                INSERT (
                    event_id,
                    group_id,
                    venue_id,
                    event_name,
                    description,
                    event_status,
                    rsvp_limit,
                    yes_rsvp_count,
                    maybe_rsvp_count,
                    waitlist_count,
                    created_at,
                    event_time,
                    duration_ms,
                    event_url,
                    visibility,
                    how_to_find_us
                )
                VALUES (
                    source.event_id,
                    source.group_id,
                    source.venue_id,
                    source.event_name,
                    source.description,
                    source.event_status,
                    source.rsvp_limit,
                    source.yes_rsvp_count,
                    source.maybe_rsvp_count,
                    source.waitlist_count,
                    source.created_at,
                    source.event_time,
                    source.duration_ms,
                    source.event_url,
                    source.visibility,
                    source.how_to_find_us
                );
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

            CREATE OR REPLACE TABLE PRUEBA_TECNICA_RAPPI.SILVER.GROUP_TOPICS AS
            SELECT DISTINCT
                TRY_CAST(group_id AS BIGINT) AS group_id,
                TRY_CAST(topic_id AS INT) AS topic_id
            FROM PRUEBA_TECNICA_RAPPI.BRONZE.GROUPS_TOPICS
            WHERE TRY_CAST(group_id AS BIGINT) IS NOT NULL
            AND TRY_CAST(topic_id AS INT) IS NOT NULL;

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
    
    load_gold_to_s3 = SQLExecuteQueryOperator(
                    task_id='load_gold_to_s3',
                    conn_id='conn_snowflake_rappi',
                    sql="""
            
                    COPY INTO @PRUEBA_TECNICA_RAPPI.GOLD.S3_GOLD_STAGE/DIM_MEMBER_360_{{ data_interval_end.strftime('%Y%m%d_%H%M') }}/
                    FROM PRUEBA_TECNICA_RAPPI.GOLD.DIM_MEMBER_360
                    FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
                    OVERWRITE = TRUE;

                    COPY INTO @PRUEBA_TECNICA_RAPPI.GOLD.S3_GOLD_STAGE/FACT_AGG_GROUP_ENGAGEMENT_MONTHLY_{{ data_interval_end.strftime('%Y%m%d_%H%M') }}/
                    FROM PRUEBA_TECNICA_RAPPI.GOLD.FACT_AGG_GROUP_ENGAGEMENT_MONTHLY
                    FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
                    OVERWRITE = TRUE;

                    COPY INTO @PRUEBA_TECNICA_RAPPI.GOLD.S3_GOLD_STAGE/FACT_EVENT_DEMAND_{{ data_interval_end.strftime('%Y%m%d_%H%M') }}/
                    FROM PRUEBA_TECNICA_RAPPI.GOLD.FACT_EVENT_DEMAND
                    FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
                    OVERWRITE = TRUE;

                    """,
                    on_success_callback=on_success_dag_callback
                )

generate_data_delta >> load_data_silver >> load_data_gold >> load_gold_to_s3