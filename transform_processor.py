# transform_processor.py
import re
import json
from datetime import datetime
from fuzzywuzzy import fuzz
from mysql.connector import Error
from config import MYSQL_CONFIG
import mysql.connector

class TransformProcessor:
    def __init__(self):
        self.connection = None
        self._create_db_connection()

    def _create_db_connection(self):
        """Create MySQL database connection"""
        try:
            self.connection = mysql.connector.connect(**MYSQL_CONFIG)
            print("✅ TRANSFORM: Connected to MySQL")
        except Error as e:
            print(f"❌ TRANSFORM: Connection error: {e}")
            raise

    def _execute_sql(self, query, params=None, multi=False):
        """Execute SQL query with error handling"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            self.connection.commit()
        except Error as e:
            print(f"❌ TRANSFORM: SQL Error: {e}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def transform_raw_to_silver(self):
        """Transform RAW → SILVER"""
        try:
            self._clear_silver_tables()
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute("SELECT match_id, json_data FROM raw_scorecard")
            
            processed = 0
            for row in cursor.fetchall():
                self._process_match(row['match_id'], json.loads(row['json_data']))
                processed += 1
            
            print(f"\n🔄 TRANSFORM: Processed {processed} matches to SILVER")
            return processed
            
        except Exception as e:
            print(f"❌ TRANSFORM: Silver processing error: {e}")
            raise

    def _clear_silver_tables(self):
        """Clear existing SILVER data"""
        self._execute_sql("TRUNCATE TABLE silver_match_summary")
        self._execute_sql("TRUNCATE TABLE silver_batting")
        self._execute_sql("TRUNCATE TABLE silver_bowling")
        print("🧹 TRANSFORM: Cleared SILVER tables")

    def _process_match(self, match_id, scorecard):
        """Process individual match to SILVER"""
        try:
            # Match summary logic
            self._process_match_summary(match_id, scorecard)
            
            # Process innings
            for innings in scorecard.get("scorecard", []):
                self._process_batting(match_id, innings)
                self._process_bowling(match_id, innings)
            
        except Exception as e:
            print(f"❌ TRANSFORM: Error processing {match_id}: {str(e)}")
            raise

    def _process_match_summary(self, match_id, scorecard):
        """Insert match summary data"""
        status = scorecard.get("status", "")[:250]
        is_tie = "tie" in status.lower()
        is_no_result = "no result" in status.lower()
        
        # Team extraction logic
        team1, team2 = self._extract_teams(scorecard)
        
        # Winner determination
        match_winner = self._determine_winner(status, team1, team2, is_tie, is_no_result)
        
        # Insert summary
        self._execute_sql("""
            INSERT INTO silver_match_summary (
                match_id, match_desc, series_name, team1_name, team2_name,
                match_winner, match_status, is_tie, is_no_result
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            match_id,
            scorecard.get("appIndex", {}).get("seoTitle", "No Description")[:255],
            "Indian Premier League 2025",
            team1,
            team2,
            match_winner,
            status,
            is_tie,
            is_no_result
        ))

    def _extract_teams(self, scorecard):
        """Extract team names from scorecard"""
        if 'scorecard' in scorecard and len(scorecard['scorecard']) > 0:
            team1 = scorecard['scorecard'][0].get('batTeamName', 'Unknown')
            team2 = scorecard['scorecard'][0].get('oppTeamName', 'Unknown')
            return team1, team2
        return 'Unknown', 'Unknown'

    def _determine_winner(self, status, team1, team2, is_tie, is_no_result):
        """Determine match winner from status text"""
        if is_tie or is_no_result:
            return None
        if "won by" in status.lower():
            return team1 if team1.lower() in status.split("won by")[0].lower() else team2
        return None

    def _process_batting(self, match_id, innings):
        """Process batting data"""
        bat_team = innings.get("batTeamName", "Unknown")
        for batsman in innings.get("batsman", []):
            self._execute_sql("""
                INSERT INTO silver_batting (
                    batsman_id, batsman_name, runs_scored, balls_faced,
                    fours, sixes, strike_rate, match_id, innings_id,
                    batting_team, out_status, wickets
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                batsman.get("id"),
                batsman.get("fullName") or batsman.get("name"),
                batsman.get("runs", 0),
                batsman.get("balls", 0),
                batsman.get("fours", 0),
                batsman.get("sixes", 0),
                float(batsman.get("strkRate", "0").replace(',', '') or 0),
                match_id,
                innings.get("inningsId", 1),
                bat_team,
                batsman.get("outDec") or "not out",
                0 if "not out" in (batsman.get("outDec") or "").lower() else 1
            ))

    def _process_bowling(self, match_id, innings):
        """Process bowling data"""
        bowl_team = innings.get("bowlTeamName", "Unknown")
        for bowler in innings.get("bowler", []):
            self._execute_sql("""
                INSERT INTO silver_bowling (
                    bowler_id, bowler_name, overs_bowled, maidens,
                    runs_given, wickets, economy, match_id, innings_id, bowling_team
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                bowler.get("id"),
                bowler.get("fullName") or bowler.get("name"),
                float(bowler.get("overs", 0)),
                bowler.get("maidens", 0),
                bowler.get("runs", 0),
                bowler.get("wickets", 0),
                float(bowler.get("economy", 0)),
                match_id,
                innings.get("inningsId", 1),
                bowl_team
            ))

    def transform_silver_to_gold(self):
        """Transform SILVER → GOLD"""
        try:
            self._clear_gold_tables()
            self._process_top_batsmen()
            self._process_top_bowlers()
            self._process_team_stats()
            print("\n✅ TRANSFORM: GOLD layer completed")
            
        except Exception as e:
            print(f"❌ TRANSFORM: Gold processing error: {e}")
            raise

    def _clear_gold_tables(self):
        """Clear existing GOLD data"""
        self._execute_sql("TRUNCATE TABLE gold_top_batsmen")
        self._execute_sql("TRUNCATE TABLE gold_top_bowlers")
        self._execute_sql("TRUNCATE TABLE gold_team_stats")
        print("🧹 TRANSFORM: Cleared GOLD tables")

    def _process_top_batsmen(self):
        """Create gold_top_batsmen"""
        self._execute_sql("""
            INSERT INTO gold_top_batsmen (...)  # Keep original batting SQL
            # [Keep the original SQL query from your code]
        """)

    def _process_top_bowlers(self):
        """Create gold_top_bowlers"""
        self._execute_sql("""
            INSERT INTO gold_top_bowlers (...)  # Keep original bowling SQL
            # [Keep the original SQL query from your code]
        """)

    def _process_team_stats(self):
        """Compute team stats with NRR"""
        # [Keep your original compute_gold_team_stats_dynamic() logic]
        # (Too long to include here, but keep it as-is)