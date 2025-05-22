# update_mysql_tables.py
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import logging
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowException

class MySQLTablesUpdater(LoggingMixin):
    def __init__(self):
        self.mysql_config = {
            'host': '   ',
            'database': '   ',
            'user': '   ',
            'password': '   ',
            'port':
        }
        self.connection = None
        self._create_db_connection()

    def _create_db_connection(self):
        try:
            if self.connection is None or not self.connection.is_connected():
                self.connection = mysql.connector.connect(**self.mysql_config)
                self.log.info("✅ Successfully connected to MySQL database")
        except Error as e:
            self.log.error(f"❌ Error connecting to MySQL: {e}")
            raise AirflowException(f"MySQL connection failed: {e}")

    def _execute_multi_sql(self, queries):
        """Execute multiple SQL statements safely"""
        cursor = None
        try:
            cursor = self.connection.cursor()
            for query in queries:
                try:
                    for statement in query.split(';'):
                        if statement.strip():
                            cursor.execute(statement)
                            self.log.info(f"Executed: {statement[:100]}...")
                except Error as e:
                    self.log.error(f"❌ Partial execution error: {e}")
                    raise
            self.connection.commit()
        finally:
            if cursor:
                cursor.close()
  
    def update_tables(self):
        try:
            queries = [
                # PURPLE CAP (Top Bowlers)
                """
                DROP TABLE IF EXISTS purple_cap;
                CREATE TABLE purple_cap AS
                SELECT position, player_name, team, total_wickets, matches_played,
                       innings_bowled, overs_bowled, runs_conceded, best_bowling_fig,
                       bowling_average, economy, four_wickets, five_wickets
                FROM gold_top_bowlers ORDER BY position;
                """,
                # POINTS TABLE
                """
                DROP TABLE IF EXISTS points_table;
                CREATE TABLE points_table AS
                SELECT position, team_name, matches_played AS Pld, matches_won AS Won,
                       matches_lost AS Lost, matches_no_result AS NR, points AS Pts,
                       ROUND(net_run_rate, 3) AS NRR
                FROM gold_team_stats ORDER BY position;
                """,
                # ORANGE CAP (Top Batsmen)
                """
                DROP TABLE IF EXISTS orange_cap;
                CREATE TABLE orange_cap AS
                SELECT position, player_name, team, total_runs, matches_played,
                       innings_played, highest_score, average_runs, strike_rate,
                       centuries, half_centuries, fours, sixes
                FROM gold_top_batsmen ORDER BY position;
                """,
                # INNINGS BREAKDOWN
                """
                DROP TABLE IF EXISTS innings_1;
                CREATE TABLE innings_1 AS
                SELECT team1 AS TEAM_A, team1_score AS TEAM_A_SCORE,
                       top_batsman_team1 AS TEAM_A_TOP_BATSMAN,
                       top_batsman_team1_runs AS TEAM_A_TOP_BATSMAN_RUNS,
                       top_batsman_team1_balls AS TEAM_A_TOP_BATSMAN_BALLS,
                       top_batsman_team1_sr AS TEAM_A_TOP_BATSMAN_SR,
                       team2 AS TEAM_B, top_bowler_team2 AS TEAM_B_TOP_BOWLER,
                       top_bowler_team2_wickets AS TEAM_B_TOP_BOWLER_WICKETS,
                       top_bowler_team2_runs AS TEAM_B_TOP_BOWLER_RUNS,
                       top_bowler_team2_econ AS TEAM_B_TOP_BOWLER_ECONOMY
                FROM gold_latest_match_summary;
                """,
                """
                DROP TABLE IF EXISTS innings_2;
                CREATE TABLE innings_2 AS
                SELECT team2 AS TEAM_B, team2_score AS TEAM_B_SCORE,
                       top_batsman_team2 AS TEAM_B_TOP_BATSMAN,
                       top_batsman_team2_runs AS TEAM_B_TOP_BATSMAN_RUNS,
                       top_batsman_team2_balls AS TEAM_B_TOP_BATSMAN_BALLS,
                       top_batsman_team2_sr AS TEAM_B_TOP_BATSMAN_SR,
                       team1 AS TEAM_A, top_bowler_team1 AS TEAM_A_TOP_BOWLER,
                       top_bowler_team1_wickets AS TEAM_A_TOP_BOWLER_WICKETS,
                       top_bowler_team1_runs AS TEAM_A_TOP_BOWLER_RUNS,
                       top_bowler_team1_econ AS TEAM_A_TOP_BOWLER_ECONOMY
                FROM gold_latest_match_summary;
                """,
                # CATCH TAKEN
                """
                DROP TABLE IF EXISTS catch_taken;
                CREATE TABLE catch_taken AS
                SELECT
                fielder_name,
                team_name,
                total_catches_taken
                FROM gold_fielder_catch_stats ORDER BY total_catches_taken DESC;
                """,
                # POWERPLAY TEAM STATS
                """
                DROP TABLE IF EXISTS powerplay_team_stats;
                CREATE TABLE powerplay_team_stats AS
                SELECT
                team_name,
                total_powerplay_innings,
                total_powerplay_runs,
                average_powerplay_score,
                last_updated
                FROM gold_team_powerplay_stats;
                """,
                # TOP SCORER BOUNDARIES RATIO
                """
                DROP TABLE IF EXISTS top_scorer_boundaries_ratio;
                CREATE TABLE top_scorer_boundaries_ratio AS
                SELECT player_id, player_name, team_name, total_runs, total_balls_faced, boundary_dominance_ratio, last_updated FROM gold_batsman_performance_metrics 
                ORDER BY total_runs DESC;
                """,
                # BOWLER CLEAN BOWLED STATS
                """
                DROP TABLE IF EXISTS bowler_clean_bowled;
                CREATE TABLE bowler_clean_bowled AS
                SELECT
                bowler_name,
                team_name,
                total_clean_bowled_wickets,
                economy
                FROM gold_bowler_clean_bowled_stats
                ORDER BY total_clean_bowled_wickets DESC LIMIT 20;
                """,
                # WICKET DISTRIBUTION
                """
                DROP TABLE IF EXISTS wicket_distribution;
                CREATE TABLE wicket_distribution AS
                SELECT 
                    team, 
                    SUM(total_wickets) AS team_wickets
                FROM gold_top_bowlers
                GROUP BY team;
                """,
                # BOWLER EFFECTIVENESS
                """
                DROP TABLE IF EXISTS bowler_effectiveness;
                CREATE TABLE bowler_effectiveness AS
                SELECT
                player_name,
                team_name,
                total_wickets,
                effectiveness_ratio
                FROM gold_bowler_performance_metrics 
                WHERE total_wickets >= 5
                ORDER BY effectiveness_ratio DESC;
                """
            ]

            self._execute_multi_sql(queries)
            self.log.info("✅ All tables updated successfully")

        except Exception as e:
            self.connection.rollback()
            self.log.error(f"❌ Failed to update tables: {e}")
            raise AirflowException(f"Table update failed: {e}")

    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.log.info("MySQL connection closed")


# Airflow-compatible function
def update_mysql_tables():
    processor = MySQLTablesUpdater()
    try:
        processor.update_tables()
    finally:
        processor.close_connection()
