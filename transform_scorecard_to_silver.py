# transform_scorecard_to_silver_final_working.py
import snowflake.connector
from snowflake.connector import ProgrammingError

CONFIG = {
    "user": "aryansarda7",
    "password": "SheetalGovind1412*",
    "account": "CJB15274.us-east-1",
    "warehouse": "COMPUTE_WH",
    "database": "DB_IPL_PROJECT",
    "schema": "RAW"
}

def get_snowflake_connection():
    return snowflake.connector.connect(**CONFIG)

def create_silver_tables(cursor):
    """Create silver tables with corrected URL parsing"""
    
    # First extract match IDs using the correct pattern
    cursor.execute("""
    CREATE OR REPLACE TEMPORARY TABLE STG_MATCH_IDS AS
    SELECT 
      data,
      -- Corrected ID extraction from the actual URL format
      SPLIT_PART(SPLIT_PART(PARSE_JSON(data):appIndex.webURL::STRING, '/', -2), '-', 1)::INT AS match_id,
      PARSE_JSON(data):appIndex.seoTitle::STRING AS match_desc,
      PARSE_JSON(data):status::STRING AS match_status
    FROM RAW.RAW_SCORECARD
    WHERE match_id IS NOT NULL
    """)
    
    # SILVER_BATTING
    cursor.execute("""
    CREATE OR REPLACE TABLE SILVER.SILVER_BATTING AS
    SELECT
      m.match_id,
      m.match_desc,
      sc.value:inningsId::INT as innings_id,
      sc.value:batTeamName::STRING as batting_team,
      batsman.value:id::INT as batsman_id,
      batsman.value:name::STRING as batsman_name,
      NVL(batsman.value:runs::INT, 0) as runs_scored,
      NVL(batsman.value:balls::INT, 0) as balls_faced,
      NVL(batsman.value:fours::INT, 0) as fours,
      NVL(batsman.value:sixes::INT, 0) as sixes,
      batsman.value:outDec::STRING as dismissal_desc,
      CASE WHEN batsman.value:outDec::STRING = 'not out' THEN TRUE ELSE FALSE END as is_not_out,
      CASE 
        WHEN NVL(batsman.value:balls::INT, 0) > 0 
        THEN ROUND(NVL(batsman.value:runs::INT, 0) * 100.0 / NULLIF(batsman.value:balls::INT, 0), 2)
        ELSE 0 
      END as strike_rate
    FROM STG_MATCH_IDS m,
    LATERAL FLATTEN(input => PARSE_JSON(m.data):scorecard) sc,
    LATERAL FLATTEN(input => sc.value:batsman) batsman
    """)

    # SILVER_BOWLING
    cursor.execute("""
    CREATE OR REPLACE TABLE SILVER.SILVER_BOWLING AS
    WITH match_teams AS (
      SELECT 
        m.match_id,
        LISTAGG(DISTINCT sc.value:batTeamName::STRING, ',') WITHIN GROUP (ORDER BY sc.value:batTeamName::STRING) AS teams
      FROM STG_MATCH_IDS m,
      LATERAL FLATTEN(input => PARSE_JSON(m.data):scorecard) sc
      GROUP BY m.match_id
    )
    SELECT
      m.match_id,
      sc.value:inningsId::INT as innings_id,
      CASE 
        WHEN SPLIT(mt.teams, ',')[0]::STRING = sc.value:batTeamName::STRING THEN SPLIT(mt.teams, ',')[1]::STRING
        ELSE SPLIT(mt.teams, ',')[0]::STRING
      END as bowling_team,
      bowler.value:id::INT as bowler_id,
      bowler.value:name::STRING as bowler_name,
      TRY_TO_DOUBLE(bowler.value:overs::STRING) as overs_bowled,
      NVL(bowler.value:maidens::INT, 0) as maidens,
      NVL(bowler.value:runs::INT, 0) as runs_given,
      NVL(bowler.value:wickets::INT, 0) as wickets,
      COALESCE(TRY_TO_DOUBLE(bowler.value:economy::STRING), 0.0) as economy,
      NVL(sc.value:extras.wides::INT, 0) as wides,
      NVL(sc.value:extras.noBalls::INT, 0) as no_balls
    FROM STG_MATCH_IDS m,
    LATERAL FLATTEN(input => PARSE_JSON(m.data):scorecard) sc,
    LATERAL FLATTEN(input => sc.value:bowler) bowler
    JOIN match_teams mt ON m.match_id = mt.match_id
    WHERE bowler.value:name IS NOT NULL
    """)

    # SILVER_MATCH_SUMMARY
    cursor.execute("""
    CREATE OR REPLACE TABLE SILVER.SILVER_MATCH_SUMMARY AS
    WITH match_teams AS (
      SELECT 
        m.match_id,
        LISTAGG(DISTINCT sc.value:batTeamName::STRING, ',') WITHIN GROUP (ORDER BY sc.value:batTeamName::STRING) AS teams
      FROM STG_MATCH_IDS m,
      LATERAL FLATTEN(input => PARSE_JSON(m.data):scorecard) sc
      GROUP BY m.match_id
    ),
    innings_stats AS (
      SELECT
        m.match_id,
        sc.value:inningsId::INT as innings_id,
        sc.value:batTeamName::STRING as batting_team,
        sc.value:score::INT as total_runs,
        sc.value:wickets::INT as wickets,
        sc.value:overs::INT as overs
      FROM STG_MATCH_IDS m,
      LATERAL FLATTEN(input => PARSE_JSON(m.data):scorecard) sc
    )
    SELECT 
      m.match_id,
      m.match_desc,
      'IPL' AS series_name,
      'T20' AS match_format,
      SPLIT(mt.teams, ',')[0]::STRING AS team1,
      SPLIT(mt.teams, ',')[1]::STRING AS team2,
      REGEXP_SUBSTR(m.match_status, '^(.*) won by') AS winner,
      REGEXP_SUBSTR(m.match_status, 'won by (.*) (wkts|runs)') AS margin,
      MAX(CASE WHEN is1.innings_id = 1 THEN is1.total_runs END) AS team1_score,
      MAX(CASE WHEN is1.innings_id = 1 THEN is1.wickets END) AS team1_wickets,
      MAX(CASE WHEN is2.innings_id = 2 THEN is2.total_runs END) AS team2_score,
      MAX(CASE WHEN is2.innings_id = 2 THEN is2.wickets END) AS team2_wickets
    FROM STG_MATCH_IDS m
    JOIN match_teams mt ON m.match_id = mt.match_id
    LEFT JOIN innings_stats is1 ON m.match_id = is1.match_id AND is1.innings_id = 1
    LEFT JOIN innings_stats is2 ON m.match_id = is2.match_id AND is2.innings_id = 2
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    """)

def verify_transformation(cursor):
    """Enhanced verification with better debugging"""
    print("\n🔍 Verification Results:")
    
    # Check staging table first
    cursor.execute("SELECT COUNT(*) FROM STG_MATCH_IDS")
    stg_count = cursor.fetchone()[0]
    print(f" - Matches in staging: {stg_count}")
    
    if stg_count == 0:
        print("\n⚠️ Running advanced diagnostics...")
        cursor.execute("""
        SELECT 
          PARSE_JSON(data):appIndex.webURL::STRING AS url,
          SPLIT_PART(SPLIT_PART(PARSE_JSON(data):appIndex.webURL::STRING, '/', -2), '-', 1) AS extracted_id
        FROM RAW.RAW_SCORECARD
        LIMIT 5
        """)
        print("\nURL Extraction Samples:")
        for row in cursor.fetchall():
            print(f"URL: {row[0]}\nExtracted ID: {row[1]}\n")
        return
    
    # Main verification
    checks = [
        ("Record Counts", """
        SELECT 'BATTING' AS source, COUNT(*) FROM SILVER.SILVER_BATTING
        UNION ALL SELECT 'BOWLING', COUNT(*) FROM SILVER.SILVER_BOWLING
        UNION ALL SELECT 'MATCH_SUMMARY', COUNT(*) FROM SILVER.SILVER_MATCH_SUMMARY
        """),
        ("Unique Matches", """
        SELECT 'BATTING' AS source, COUNT(DISTINCT match_id) FROM SILVER.SILVER_BATTING
        UNION ALL SELECT 'BOWLING', COUNT(DISTINCT match_id) FROM SILVER.SILVER_BOWLING
        UNION ALL SELECT 'MATCH_SUMMARY', COUNT(DISTINCT match_id) FROM SILVER.SILVER_MATCH_SUMMARY
        """),
        ("Sample Matches", "SELECT match_id, match_desc FROM SILVER.SILVER_MATCH_SUMMARY LIMIT 5")
    ]
    
    for desc, query in checks:
        try:
            cursor.execute(query)
            print(f"\n{desc}:")
            for row in cursor.fetchall():
                print(f" - {row[0]}: {row[1]}")
        except Exception as e:
            print(f"⚠️ Check failed: {desc}\nError: {str(e)}")

def main():
    conn = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        print("🔌 Connected to Snowflake")
        cursor.execute("USE SCHEMA SILVER")
        
        print("🔄 Starting transformations...")
        create_silver_tables(cursor)
        
        print("✅ Verifying results...")
        verify_transformation(cursor)
        
        conn.commit()
        print("\n🎯 Transformation completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()