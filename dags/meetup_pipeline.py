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
    dag_id='meetup_pipeline',
    default_args=default_args,
    description='Pipeline Medallion Incremental de Meetup para Events, Groups y Members (Paso 4)',
    schedule='*/15 * * * *',
    catchup=False,
    tags=['meetup', 'snowflake', 'rappipay'],
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
        """
    )
