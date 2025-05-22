# transform_processor.py
import json
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import re
import logging
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowException
from fuzzywuzzy import fuzz

class TransformProcessor:
    KNOWN_TEAM_NAME_MAP = {
        "Mumbai Indians": ["MI", "Mumbai"],
        "Chennai Super Kings": ["CSK", "Chennai"],
        "Royal Challengers Bengaluru": ["RCB", "Bangalore", "Royal Challengers Bangalore"],
        "Delhi Capitals": ["DC", "Delhi"],
        "Gujarat Titans": ["GT"],
        "Lucknow Super Giants": ["LSG", "Lucknow"],
        "Kolkata Knight Riders": ["KKR", "Kolkata"],
        "Punjab Kings": ["PBKS", "Kings XI Punjab", "Punjab"],
        "Rajasthan Royals": ["RR", "Rajasthan"],
        "Sunrisers Hyderabad": ["SRH", "Hyderabad", "Sunrisers H"]
    }

    def __init__(self, mysql_config = None):
       self.log = logging.getLogger(__name__)
       self.mysql_config = mysql_config or {
       'host': '   ',
       'database': '   ',
       'user': '   ',
       'password': '   ',
       'port':   # Add if needed
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

    def _execute_sql(self, query, params=None, multi=False):
        if not self.connection or not self.connection.is_connected():
            self._create_db_connection()
        
        cursor = self.connection.cursor(buffered=True)
        is_primarily_select_query = query.strip().upper().startswith("SELECT")

        try:
            if multi:
                for result_iterator in cursor.execute(query, params, multi=True): # type: ignore
                    if result_iterator.with_rows:
                        self.log.info(f"(TransformProcessor) Executed (multi-part with rows): {result_iterator.statement}")
                    else:
                        self.log.info(f"(TransformProcessor) Executed (multi-part, no rows/DML): {result_iterator.statement} - Rows affected: {result_iterator.rowcount}")
                self.connection.commit()
            else:
                cursor.execute(query, params)
                if not is_primarily_select_query:
                    self.connection.commit()
            return cursor
        except Error as e:
            self.log.error(f"❌ (TransformProcessor) SQL Error: {e}\nQuery: {query}\nParams: {params}")
            try:
                if (not is_primarily_select_query and not multi) or multi:
                    self.log.info("Attempting rollback due to error...")
                    self.connection.rollback()
                    self.log.info("(TransformProcessor) Rollback successful.")
            except Error as rb_err:
                self.log.error(f"❌ (TransformProcessor) Error during rollback: {rb_err}")
            raise
    
    def _extract_teams_from_filename(self, match_id):
        """Extract team names from standardized match_id format: {id}_{team1}_vs_{team2}"""
        try:
            parts = match_id.split('_')
            if len(parts) >= 4 and "_vs_" in match_id:
                team1 = re.sub(r"([a-z])([A-Z])", r"\1 \2", parts[1])
                team2 = re.sub(r"([a-z])([A-Z])", r"\1 \2", parts[3])
                return (
                    self._normalize_team_name(team1),
                    self._normalize_team_name(team2)
                )
        except Exception as e:
            self.log.error(f"Error extracting teams from filename {match_id}: {e}")
        return ("Unknown", "Unknown")

    def _normalize_team_name(self, name_variant):
        if not name_variant or not isinstance(name_variant, str):
            return "Unknown"
        
        name_variant_clean = name_variant.strip()
        name_variant_lower = name_variant_clean.lower()

        if not name_variant_clean or name_variant_lower == "unknown":
            return "Unknown"

        for canonical_name in self.KNOWN_TEAM_NAME_MAP.keys():
            if canonical_name.lower() == name_variant_lower:
                return canonical_name

        for canonical_name, variants in self.KNOWN_TEAM_NAME_MAP.items():
            for variant in variants:
                if variant.lower() == name_variant_lower:
                    return canonical_name 

        best_match_score = 0
        best_canonical_name = None
        for canonical_name in self.KNOWN_TEAM_NAME_MAP.keys():
            score = fuzz.ratio(name_variant_lower, canonical_name.lower())
            if score > best_match_score:
                best_match_score = score
                best_canonical_name = canonical_name
        
        if best_match_score > 85:
            return best_canonical_name

        return name_variant_clean if name_variant_clean else "Unknown"


    def create_silver_gold_tables(self):
        try:
            # SILVER layer tables
            self._execute_sql("""
                CREATE TABLE IF NOT EXISTS silver_batting (
                    id INT AUTO_INCREMENT PRIMARY KEY, batsman_id INT, batsman_name VARCHAR(100),
                    runs_scored INT, balls_faced INT, fours INT, sixes INT, strike_rate FLOAT,
                    match_id VARCHAR(100), innings_id INT, batting_team VARCHAR(100),
                    out_status VARCHAR(100), wickets INT DEFAULT 0, load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_match (match_id), INDEX idx_batsman (batsman_id)
                )""")
            self._execute_sql("""
                CREATE TABLE IF NOT EXISTS silver_bowling (
                    id INT AUTO_INCREMENT PRIMARY KEY, bowler_id INT, bowler_name VARCHAR(100),
                    overs_bowled FLOAT, maidens INT, runs_given INT, wickets INT, economy FLOAT,
                    match_id VARCHAR(100), innings_id INT, bowling_team VARCHAR(100),
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, INDEX idx_match (match_id), INDEX idx_bowler (bowler_id)
                )""")
            self._execute_sql("""
                CREATE TABLE IF NOT EXISTS silver_match_summary (
                    id INT AUTO_INCREMENT PRIMARY KEY, 
                    match_id VARCHAR(100), 
                    match_sequence_number INT,  # New column for the match number
                    match_desc VARCHAR(255),
                    series_name VARCHAR(100), 
                    match_type VARCHAR(50), 
                    match_format VARCHAR(50),
                    team1_name VARCHAR(100), 
                    team2_name VARCHAR(100), 
                    toss_winner VARCHAR(100),
                    toss_decision VARCHAR(50), 
                    match_winner VARCHAR(100), 
                    winning_margin INT,
                    win_by_runs BOOLEAN, 
                    match_status VARCHAR(255), 
                    is_tie BOOLEAN DEFAULT FALSE,
                    is_no_result BOOLEAN DEFAULT FALSE, 
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_match (match_id),
                    INDEX idx_match_seq_num (match_sequence_number) # Optional: index for faster querying
                )""")
            # GOLD layer tables
            self._execute_sql("""
                CREATE TABLE IF NOT EXISTS gold_top_batsmen (
                    id INT AUTO_INCREMENT PRIMARY KEY, position INT, player_name VARCHAR(100), team VARCHAR(100),
                    total_runs INT, matches_played INT, innings_played INT, highest_score INT,
                    average_runs FLOAT, strike_rate FLOAT, centuries INT, half_centuries INT,
                    fours INT, sixes INT, last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_player (player_name), INDEX idx_team (team)
                )""")
            self._execute_sql("""
                CREATE TABLE IF NOT EXISTS gold_top_bowlers (
                    id INT AUTO_INCREMENT PRIMARY KEY, position INT, player_name VARCHAR(100), team VARCHAR(100),
                    total_wickets INT, matches_played INT, innings_bowled INT, overs_bowled FLOAT,
                    runs_conceded INT, best_bowling_fig VARCHAR(50), bowling_average FLOAT, economy FLOAT,
                    four_wickets INT, five_wickets INT, last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_player (player_name), INDEX idx_team (team)
                )""")
            self._execute_sql("""
                CREATE TABLE IF NOT EXISTS gold_team_stats (
                    id INT AUTO_INCREMENT PRIMARY KEY, position INT, team_name VARCHAR(100), matches_played INT,
                    matches_won INT, matches_lost INT, matches_tied INT, matches_no_result INT,
                    points INT, net_run_rate FLOAT DEFAULT 0.0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_team (team_name)
                )""")
            self.log.info("✅ (TransformProcessor) SILVER and GOLD tables created/verified successfully")
        except Error as e:
            self.log.error(f"❌ (TransformProcessor) Error creating SILVER/GOLD tables: {e}")
            raise

    def get_extras_from_raw(self, match_id):
        db_cursor = None
        extras_map = {}
        try:
            db_cursor = self._execute_sql("SELECT json_data FROM raw_scorecard WHERE match_id = %s", (match_id,))
            result = db_cursor.fetchone()
            if not result: return extras_map
            json_data = json.loads(result[0])
            for innings in json_data.get("scorecard", []):
                team = innings.get("batTeamName")
                if team:
                    team = self._normalize_team_name(team)
                extras_data = innings.get("extras", {}) 
                total_extras = extras_data.get("total", 0)
                try:
                    total_extras = int(total_extras)
                except (ValueError, TypeError): total_extras = 0
                if team and team.lower() != "unknown":
                    extras_map[team] = extras_map.get(team, 0) + total_extras
        finally:
            if db_cursor: db_cursor.close()
        return extras_map


    def transform_raw_to_silver(self):
        """Transform data to SILVER layer with all required tables"""
        try:
            self._execute_sql("TRUNCATE TABLE silver_match_summary")
            self._execute_sql("TRUNCATE TABLE silver_batting")
            self._execute_sql("TRUNCATE TABLE silver_bowling")
            self.log.info("🧹 (TransformProcessor) Cleared existing SILVER data")
            
            db_cursor_raw = self._execute_sql("SELECT match_id, json_data FROM raw_scorecard")
            records = db_cursor_raw.fetchall()
            db_cursor_raw.close() 
            
            processed_count = 0
            skipped_count = 0
            
            self.log.info(f"\n🔄 (TransformProcessor) Processing {len(records)} matches to SILVER layer...")
            
            for row_tuple in records:
                match_folder_name = row_tuple[0] 
                scorecard_json_str = row_tuple[1]
                scorecard = json.loads(scorecard_json_str)
                
                try:
                    match_id = match_folder_name 
                    app_index = scorecard.get("appIndex", {})
                    status_text = scorecard.get("status", "") 
                    
                    is_no_result = (
                        "no result" in status_text.lower() 
                        or "abandoned" in status_text.lower()
                        or not status_text.strip()  # Empty status
                        or not any(innings.get("batsman") for innings in scorecard.get("scorecard", []))  # No batting data
                    )
                    
                    is_tie = "tie" in status_text.lower()
                    
                    seo_title = app_index.get("seoTitle", "")
                    match_sequence_num = None 
                    if seo_title:
                        match_num_search = re.search(r'(\d+)(?:st|nd|rd|th)\s+Match', seo_title, re.IGNORECASE)
                        if match_num_search:
                            try:
                                match_sequence_num = int(match_num_search.group(1))
                            except ValueError:
                                self.log.error(f"⚠️ Match {match_id}: Could not parse match number from seoTitle: '{seo_title}'")
                    
                    match_info = scorecard.get("matchInfo", {}) 
                    
                    match_desc_val = app_index.get("seoTitle", "No Description")[:255]
                    series_name_val = match_info.get("series", {}).get("name", "Indian Premier League")[:100]
                    
                    match_type_val = match_info.get("matchTypeActualKey", "Unknown")[:50] 
                    match_format_val = match_info.get("matchFormatActualKey", "T20")[:50] 
                    
                    toss_winner_raw = match_info.get("tossWinnerActualKey") 
                    toss_winner_val = self._normalize_team_name(toss_winner_raw) if toss_winner_raw else None
                    if toss_winner_val: toss_winner_val = toss_winner_val[:100]

                    toss_decision_val = match_info.get("tossDecisionActualKey")
                    if toss_decision_val: toss_decision_val = toss_decision_val[:50]

                    winning_margin_val = None
                    win_by_runs_val = None

                    if not is_tie and not is_no_result and "won by" in status_text.lower():
                        try:
                            margin_text_part = status_text.lower().split("won by", 1)[1].strip()
                            if "run" in margin_text_part:
                                margin_search = re.search(r'(\d+)\s+run', margin_text_part)
                                if margin_search:
                                    winning_margin_val = int(margin_search.group(1))
                                    win_by_runs_val = True
                            elif "wicket" in margin_text_part:
                                margin_search = re.search(r'(\d+)\s+wicket', margin_text_part)
                                if margin_search:
                                    winning_margin_val = int(margin_search.group(1))
                                    win_by_runs_val = False
                        except Exception as e_margin:
                            self.log.error(f"ℹ️ Match {match_id}: Could not parse winning margin/method from status: '{status_text}'. Error: {e_margin}")
                    
                    team1_for_match, team2_for_match = self._extract_teams_from_filename(match_id)

                    if team1_for_match == "Unknown" or team2_for_match == "Unknown":
                        match_info_teams_list = match_info.get("teams", [])
                        candidate_teams_from_match_info = []
                        if isinstance(match_info_teams_list, list):
                            for team_entry in match_info_teams_list:
                                if isinstance(team_entry, dict):
                                    name = team_entry.get("name")
                                    s_name = team_entry.get("shortName")
                                    chosen_name = name if name and name.strip() else s_name
                                    if chosen_name and chosen_name.strip():
                                        normalized = self._normalize_team_name(chosen_name)
                                        if normalized.lower() != "unknown":
                                            candidate_teams_from_match_info.append(normalized)
                        
                        distinct_match_info_teams = sorted(list(set(candidate_teams_from_match_info)))
                        if len(distinct_match_info_teams) >= 1 and team1_for_match == "Unknown":
                            team1_for_match = distinct_match_info_teams[0]
                        if len(distinct_match_info_teams) >= 2 and team2_for_match == "Unknown":
                            team2_for_match = distinct_match_info_teams[1]

                    # Special case: If we have valid teams but no gameplay data, force no-result
                    if (team1_for_match != "Unknown" and team2_for_match != "Unknown" and 
                        not any(innings.get("batsman") for innings in scorecard.get("scorecard", []))):
                        is_no_result = True
                        self.log.info(f"🔀 Match {match_id}: Forced No-Result due to valid teams but no gameplay data")

                    if team1_for_match.lower() != "unknown" and team1_for_match.lower() == team2_for_match.lower():
                        team2_for_match = "Unknown" 
                    elif team1_for_match.lower() == "unknown" and team2_for_match.lower() == "unknown" and not is_no_result:
                        self.log.info(f"⚠️ Match {match_id}: Could not identify any valid team names. Both are 'Unknown'. Status: '{status_text[:250]}'") # Use sliced status_text for print
                    
                    # --- Match Winner Identification ---
                    match_winner = None 
                    if not is_tie and not is_no_result and status_text:
                        if team1_for_match.lower() != "unknown" and team2_for_match.lower() != "unknown":
                            # Prefer "won by" as it's more definitive for the winner
                            winner_part_status = status_text.split("won by")[0].strip() if "won by" in status_text.lower() else \
                                                (status_text.split("beat")[0].strip() if "beat" in status_text.lower() else None)

                            if winner_part_status:
                                normalized_winner_text = self._normalize_team_name(winner_part_status)
                                if fuzz.ratio(normalized_winner_text.lower(), team1_for_match.lower()) > 80: # Using ratio for potentially closer names
                                    match_winner = team1_for_match
                                elif fuzz.ratio(normalized_winner_text.lower(), team2_for_match.lower()) > 80:
                                    match_winner = team2_for_match
                    
                    # --- Insert match summary ---
                    sql_insert_summary = """
                        INSERT INTO silver_match_summary (
                            match_id, match_sequence_number, match_desc, series_name, 
                            match_type, match_format, team1_name, team2_name, 
                            toss_winner, toss_decision, match_winner, 
                            winning_margin, win_by_runs, match_status, 
                            is_tie, is_no_result
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                        """
                    params_summary = (
                        match_id, 
                        match_sequence_num, 
                        match_desc_val,
                        series_name_val,
                        match_type_val,
                        match_format_val,
                        team1_for_match[:100], 
                        team2_for_match[:100], 
                        toss_winner_val,
                        toss_decision_val,
                        match_winner[:100] if match_winner else None, 
                        winning_margin_val,
                        win_by_runs_val,
                        status_text[:255],
                        is_tie, 
                        is_no_result
                    )
                    summary_ins_cursor = self._execute_sql(sql_insert_summary, params_summary)
                    if summary_ins_cursor: summary_ins_cursor.close()
                    
                    # --- Batting and Bowling Data ---
                    for innings_data in scorecard.get("scorecard", []):
                        innings_id = innings_data.get("inningsId", 1)
                        bat_team_raw = innings_data.get("batTeamName")
                        bat_team_normalized = self._normalize_team_name(bat_team_raw) if bat_team_raw else "Unknown"

                        bowl_team = "Unknown"
                        if bat_team_normalized.lower() != "unknown" and \
                        team1_for_match.lower() != "unknown" and \
                        team2_for_match.lower() != "unknown" and \
                        team1_for_match.lower() != team2_for_match.lower():
                            if bat_team_normalized == team1_for_match: bowl_team = team2_for_match
                            elif bat_team_normalized == team2_for_match: bowl_team = team1_for_match
                            else: 
                                if fuzz.partial_ratio(bat_team_normalized.lower(), team1_for_match.lower()) > 85: bowl_team = team2_for_match
                                elif fuzz.partial_ratio(bat_team_normalized.lower(), team2_for_match.lower()) > 85: bowl_team = team1_for_match
                        elif bat_team_normalized.lower() != "unknown": 
                            if team1_for_match.lower() != "unknown" and bat_team_normalized != team1_for_match : bowl_team = team1_for_match
                            elif team2_for_match.lower() != "unknown" and bat_team_normalized != team2_for_match : bowl_team = team2_for_match
                        
                        if bat_team_normalized.lower() == "unknown" and not is_no_result:
                            self.log.info(f"⚠️ Match {match_id}, Innings {innings_id}: Batting team is 'Unknown'. Batting/bowling stats might be misattributed or skipped.")

                        for batsman in innings_data.get("batsman", []):
                            strike_rate_str = batsman.get("strkRate", "0"); strike_rate = 0.0
                            try: strike_rate = float(str(strike_rate_str).replace(',', '')) if strike_rate_str else 0.0
                            except: pass
                            out_status = batsman.get("outDesc") or batsman.get("outDec") or "not out"
                            is_out = 0 if "not out" in out_status.lower() else 1
                            batsman_id_val = batsman.get("id")

                            bat_ins_cursor = self._execute_sql("""
                                INSERT INTO silver_batting (batsman_id, batsman_name, runs_scored, balls_faced, fours, sixes, strike_rate, match_id, innings_id, batting_team, out_status, wickets)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                                (batsman_id_val, 
                                (batsman.get("fullName") or batsman.get("name"))[:100] if (batsman.get("fullName") or batsman.get("name")) else "Unknown Batsman", 
                                batsman.get("r", batsman.get("runs", 0)),
                                batsman.get("b", batsman.get("balls", 0)), 
                                batsman.get("4s", batsman.get("fours", 0)), 
                                batsman.get("6s", batsman.get("sixes", 0)),
                                strike_rate, match_id, innings_id, bat_team_normalized[:100], out_status[:100], is_out))
                            if bat_ins_cursor: bat_ins_cursor.close()
                        
                        for bowler in innings_data.get("bowler", []):
                            if is_no_result and bowl_team.lower() == "unknown":
                                continue 
                            overs_str = bowler.get("ov", bowler.get("overs", "0")); overs = 0.0
                            if isinstance(overs_str, str):
                                try: overs = float(overs_str)
                                except ValueError:
                                    if "." in overs_str: overs = float(overs_str)
                                    else: overs = float(f"{overs_str}.0")
                            elif isinstance(overs_str, (int, float)): overs = float(overs_str)
                            
                            economy_str = bowler.get("econ", bowler.get("economy", "0")); economy = 0.0
                            if isinstance(economy_str, str):
                                try: economy = float(economy_str.replace(',', '')) if economy_str else 0.0
                                except ValueError: economy = 0.0
                            elif isinstance(economy_str, (int, float)): economy = float(economy_str)

                            bowler_id_val = bowler.get("id")

                            bowl_ins_cursor = self._execute_sql("""
                                INSERT INTO silver_bowling (bowler_id, bowler_name, overs_bowled, maidens, runs_given, wickets, economy, match_id, innings_id, bowling_team)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                                (bowler_id_val, 
                                (bowler.get("fullName") or bowler.get("name") or "Unknown Bowler")[:100],
                                overs, 
                                bowler.get("m", bowler.get("maidens", 0)), 
                                bowler.get("r", bowler.get("runs", 0)),
                                bowler.get("w", bowler.get("wickets", 0)), 
                                economy, match_id, innings_id, bowl_team[:100]))
                            if bowl_ins_cursor: bowl_ins_cursor.close()
                    processed_count += 1
                except Exception as e:
                    self.log.error(f"❌ (TransformProcessor) Error processing SILVER for {match_folder_name}: {str(e)}")
                    import traceback; traceback.print_exc()
                    skipped_count += 1
            self.log.info(f"\n(TransformProcessor) SILVER Transformation complete: {processed_count} processed, {skipped_count} skipped.")
            return processed_count
        except Exception as e:
            self.log.error(f"❌ (TransformProcessor) General SILVER transformation error: {e}")
            raise

    def compute_gold_team_stats_dynamic(self):
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute("SELECT * FROM silver_match_summary ORDER BY match_id")
            matches = cursor.fetchall()
            team_stats = {}
            for match in matches:
                match_id = match['match_id']
                team1 = self._normalize_team_name(match['team1_name']) if match['team1_name'] else "Unknown"
                team2 = self._normalize_team_name(match['team2_name']) if match['team2_name'] else "Unknown"
                winner_raw = match['match_winner']
                winner = self._normalize_team_name(winner_raw) if winner_raw else None
                is_tie = match['is_tie']
                is_no_result = match['is_no_result']
                status = match['match_status'] or ""
                status_clean = status.lower()
            
                # If match was no_result and teams are still unknown, try original filename derivation
                if is_no_result and (team1.lower() == "unknown" or team2.lower() == "unknown"):
                    parts = match_id.split('_')
                    if len(parts) >= 3: 
                        match_name_part = '_'.join(parts[1:]) 
                        if "_vs_" in match_name_part:
                            t1_raw, t2_raw = match_name_part.split('_vs_', 1)
                            # Simple camel/pascal case to space
                            def normalize_from_filename(name):
                                s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', name)
                                return self._normalize_team_name(re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1).replace('-', ' '))

                            if team1.lower() == "unknown": team1 = normalize_from_filename(t1_raw)
                            if team2.lower() == "unknown": team2 = normalize_from_filename(t2_raw)
                
                # Initialize team stats with normalized names
                for team_name_loop in [team1, team2]:
                    if team_name_loop and team_name_loop.lower() != "unknown" and team_name_loop not in team_stats:
                        team_stats[team_name_loop] = {'matches_played': 0,'matches_won': 0,'matches_lost': 0,'matches_tied': 0,'matches_no_result': 0,'points': 0,'runs_scored': 0.0,'balls_faced': 0.0,'runs_conceded': 0.0,'balls_bowled': 0.0}
                
                for team_name_loop in [team1, team2]:
                    if team_name_loop and team_name_loop.lower() != "unknown": team_stats[team_name_loop]['matches_played'] += 1
                
                if is_no_result:
                    for team_name_loop in [team1, team2]:
                        if team_name_loop and team_name_loop.lower() != "unknown":
                            team_stats[team_name_loop]['matches_no_result'] += 1
                            team_stats[team_name_loop]['points'] += 1  # IPL rules: 1 point each
                            self.log.info(f"➕ Match {match_id}: Awarded 1 point each to {team_name_loop} (No-Result)")
                elif "super over" in status_clean:
                    so_winner_match_re = re.search(r'\((.*?) won the super over\)', status, re.IGNORECASE)
                    if so_winner_match_re:
                        so_winner_name_raw = so_winner_match_re.group(1).strip()
                        so_winner_name = self._normalize_team_name(so_winner_name_raw)
                        winner_found_so = False
                        if so_winner_name == team1:
                            team_stats[team1]['matches_won'] += 1; team_stats[team1]['points'] += 2
                            if team2.lower() != "unknown": team_stats[team2]['matches_lost'] += 1
                            winner_found_so = True
                        elif so_winner_name == team2:
                            team_stats[team2]['matches_won'] += 1; team_stats[team2]['points'] += 2
                            if team1.lower() != "unknown": team_stats[team1]['matches_lost'] += 1
                            winner_found_so = True
                        
                        if not winner_found_so and so_winner_name.lower() != "unknown":
                            self.log.info(f"ℹ️ Super Over Winner '{so_winner_name_raw}' (normalized to '{so_winner_name}') for match {match_id} did not match team1 ('{team1}') or team2 ('{team2}'). Treating as tie for points.")
                            for team_name_loop in [team1, team2]: 
                                if team_name_loop and team_name_loop.lower() != "unknown": team_stats[team_name_loop]['matches_tied'] += 1; team_stats[team_name_loop]['points'] += 1
                        elif not winner_found_so :
                             for team_name_loop in [team1, team2]: 
                                if team_name_loop and team_name_loop.lower() != "unknown": team_stats[team_name_loop]['matches_tied'] += 1; team_stats[team_name_loop]['points'] += 1
                    else:
                        for team_name_loop in [team1, team2]:
                            if team_name_loop and team_name_loop.lower() != "unknown": team_stats[team_name_loop]['matches_tied'] += 1; team_stats[team_name_loop]['points'] += 1
                elif is_tie:
                    for team_name_loop in [team1, team2]:
                        if team_name_loop and team_name_loop.lower() != "unknown": team_stats[team_name_loop]['matches_tied'] += 1; team_stats[team_name_loop]['points'] += 1
                else:
                    if winner and winner.lower() != "unknown":
                        actual_winner_team = winner
                        if actual_winner_team == team1:
                            team_stats[team1]['matches_won'] += 1; team_stats[team1]['points'] += 2
                            if team2.lower() != "unknown": team_stats[team2]['matches_lost'] += 1
                        elif actual_winner_team == team2:
                            team_stats[team2]['matches_won'] += 1; team_stats[team2]['points'] += 2
                            if team1.lower() != "unknown": team_stats[team1]['matches_lost'] += 1

                if not is_no_result:
                    nrr_cursor = self.connection.cursor(dictionary=True)
                    nrr_cursor.execute("SELECT batting_team, SUM(runs_scored) as runs, SUM(balls_faced) as balls, SUM(wickets) as wickets_lost FROM silver_batting WHERE match_id = %s AND innings_id IN (1, 2) GROUP BY batting_team", (match_id,))
                    batting_data_for_nrr = {}
                    for row in nrr_cursor.fetchall():
                        team_name_nrr = self._normalize_team_name(row['batting_team'])
                        if team_name_nrr.lower() == "unknown": continue

                        raw_balls = float(row['balls'] or 0)
                        adj_balls = 120.0 if (row['wickets_lost'] or 0) >= 10 and raw_balls < 120 else raw_balls
                        batting_data_for_nrr[team_name_nrr] = {'runs': float(row['runs'] or 0), 'adjusted_balls': adj_balls, 'wickets': int(row['wickets_lost'] or 0)}
                    nrr_cursor.close()
                    extras_for_match = self.get_extras_from_raw(match_id)

                    for team_name_loop in [team1, team2]:
                        if not team_name_loop or team_name_loop.lower() == "unknown": continue
                        opponent_team = team2 if team_name_loop == team1 else team1
                        if not opponent_team or opponent_team.lower() == "unknown": continue

                        if team_name_loop in batting_data_for_nrr and opponent_team in batting_data_for_nrr:
                            team_bat_runs = batting_data_for_nrr[team_name_loop]['runs']
                            team_extras_val = extras_for_match.get(team_name_loop, 0) 
                            total_runs_scored_by_team = team_bat_runs + team_extras_val
                            opp_bat_runs = batting_data_for_nrr[opponent_team]['runs']
                            opp_extras_val = extras_for_match.get(opponent_team, 0)
                            total_runs_conceded_by_team = opp_bat_runs + opp_extras_val

                            team_stats[team_name_loop]['runs_scored'] += total_runs_scored_by_team
                            team_stats[team_name_loop]['balls_faced'] += batting_data_for_nrr[team_name_loop]['adjusted_balls']
                            team_stats[team_name_loop]['runs_conceded'] += total_runs_conceded_by_team
                            team_stats[team_name_loop]['balls_bowled'] += batting_data_for_nrr[opponent_team]['adjusted_balls']
            cursor.close()

            self._execute_sql("DELETE FROM gold_team_stats") 
            final_standings_data = []
            for team_name_final, stats_data in team_stats.items():
                overs_faced = stats_data['balls_faced'] / 6.0 if stats_data['balls_faced'] > 0 else 0.1 
                overs_bowled = stats_data['balls_bowled'] / 6.0 if stats_data['balls_bowled'] > 0 else 0.1
                nrr = 0.0
                if overs_faced > 0 and overs_bowled > 0 : 
                     nrr = round((stats_data['runs_scored'] / max(overs_faced, 0.1)) - (stats_data['runs_conceded'] / max(overs_bowled, 0.1)), 3)
                final_standings_data.append((
                    team_name_final, stats_data['matches_played'], stats_data['matches_won'], stats_data['matches_lost'], 
                    stats_data['matches_tied'], stats_data['matches_no_result'], stats_data['points'], nrr ))
            final_standings_data.sort(key=lambda x: (-x[6], -x[7])) 
            for pos, team_data_tuple in enumerate(final_standings_data, start=1):
                self._execute_sql("""INSERT INTO gold_team_stats (position, team_name, matches_played, matches_won, matches_lost, matches_tied, matches_no_result, points, net_run_rate) 
                                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", (pos, *team_data_tuple))
            self.log.info("✅ (TransformProcessor) GOLD team standings updated with accurate NRR including extras.")
        except Exception as e:
            self.log.error(f"❌ (TransformProcessor) Error computing gold_team_stats: {e}")
            import traceback; traceback.print_exc()
            raise


    def transform_silver_to_gold(self):
        try:
            self._execute_sql("TRUNCATE TABLE gold_top_batsmen")
            self._execute_sql("TRUNCATE TABLE gold_top_bowlers")
            self.log.info("🧹 (TransformProcessor) Cleared existing GOLD player data")
            self.log.info("\n🔄 (TransformProcessor) Transforming to GOLD layer...")

            batsmen_cursor = self._execute_sql("""
                INSERT INTO gold_top_batsmen (position, player_name, team, total_runs, matches_played, innings_played, highest_score, average_runs, strike_rate, centuries, half_centuries, fours, sixes)
                WITH batting_stats AS (
                    SELECT batsman_id, SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT batsman_name ORDER BY LENGTH(batsman_name) DESC SEPARATOR '|'), '|', 1) AS player_name,
                        batting_team, SUM(runs_scored) AS total_runs, COUNT(DISTINCT match_id) AS matches_played,
                        COUNT(DISTINCT CONCAT(match_id, '-', innings_id)) AS innings_played, MAX(runs_scored) AS highest_score,
                        ROUND(SUM(runs_scored)/NULLIF(SUM(wickets), 0), 2) AS average_runs,
                        ROUND((SUM(runs_scored)/NULLIF(SUM(balls_faced), 0))*100, 2) AS strike_rate,
                        SUM(CASE WHEN runs_scored >= 100 THEN 1 ELSE 0 END) AS centuries,
                        SUM(CASE WHEN runs_scored >= 50 AND runs_scored < 100 THEN 1 ELSE 0 END) AS half_centuries,
                        SUM(fours) AS fours, SUM(sixes) AS sixes
                    FROM silver_batting WHERE batsman_id IS NOT NULL AND batsman_name IS NOT NULL AND batsman_name != 'Unknown' AND batting_team IS NOT NULL AND batting_team != 'Unknown'
                    GROUP BY batsman_id, batting_team )
                SELECT ROW_NUMBER() OVER (ORDER BY total_runs DESC, average_runs DESC, strike_rate DESC) AS position, player_name, batting_team AS team,
                    total_runs, matches_played, innings_played, highest_score, average_runs, strike_rate, centuries, half_centuries, fours, sixes
                FROM batting_stats WHERE total_runs > 0 ORDER BY total_runs DESC, average_runs DESC, strike_rate DESC LIMIT 20; """)
            if batsmen_cursor: batsmen_cursor.close()
            self.log.info("✅ (TransformProcessor) GOLD top batsmen stats updated.")

            bowlers_cursor = self._execute_sql("""
                INSERT INTO gold_top_bowlers (position, player_name, team, total_wickets, matches_played, innings_bowled, overs_bowled, runs_conceded, best_bowling_fig, bowling_average, economy, four_wickets, five_wickets)
                WITH bowling_stats_agg AS (
                    SELECT bowler_id, SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT bowler_name ORDER BY LENGTH(bowler_name) DESC SEPARATOR '|'), '|', 1) AS player_name,
                        ANY_VALUE(bowling_team) AS derived_bowling_team, SUM(wickets) AS total_wickets, COUNT(DISTINCT match_id) AS matches_played,
                        COUNT(DISTINCT CONCAT(match_id, '-', innings_id)) AS innings_bowled, SUM(overs_bowled) AS total_overs_bowled_decimal,
                        SUM(runs_given) AS total_runs_conceded, SUM(CASE WHEN wickets >= 4 AND wickets < 5 THEN 1 ELSE 0 END) AS four_wickets,
                        SUM(CASE WHEN wickets >= 5 THEN 1 ELSE 0 END) AS five_wickets
                    FROM silver_bowling WHERE bowler_id IS NOT NULL AND bowler_name IS NOT NULL AND bowler_name NOT LIKE 'Unknown%' AND bowling_team IS NOT NULL AND bowling_team != 'Unknown'
                    GROUP BY bowler_id ),
                bowling_stats_calculated AS ( SELECT *, ROUND(total_runs_conceded / NULLIF(total_wickets, 0), 2) AS bowling_average,
                        ROUND(total_runs_conceded / NULLIF( (FLOOR(total_overs_bowled_decimal) + ( ( (total_overs_bowled_decimal - FLOOR(total_overs_bowled_decimal)) * 10 ) / 6 ) ), 0), 2) AS economy_rate 
                    FROM bowling_stats_agg ),
                best_figures AS ( SELECT bowler_id, CONCAT(wickets, '/', runs_given) AS best_bowling_fig FROM (
                        SELECT bowler_id, wickets, runs_given, ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY wickets DESC, runs_given ASC ) AS rn
                        FROM silver_bowling WHERE wickets > 0 ) ranked WHERE rn = 1)
                SELECT ROW_NUMBER() OVER (ORDER BY bs.total_wickets DESC, bs.economy_rate ASC, bs.bowling_average ASC ) AS position,
                    bs.player_name, bs.derived_bowling_team AS team, bs.total_wickets, bs.matches_played, bs.innings_bowled,
                    bs.total_overs_bowled_decimal AS overs_bowled, bs.total_runs_conceded AS runs_conceded, bf.best_bowling_fig,
                    bs.bowling_average, bs.economy_rate AS economy, bs.four_wickets, bs.five_wickets
                FROM bowling_stats_calculated bs LEFT JOIN best_figures bf ON bs.bowler_id = bf.bowler_id
                WHERE bs.total_wickets > 0 ORDER BY position ASC LIMIT 20; """)
            if bowlers_cursor: bowlers_cursor.close()
            self.log.info("✅ (TransformProcessor) GOLD top bowlers stats updated.")
            
            self.compute_gold_team_stats_dynamic()
            
            final_cursor = self._execute_sql("""SELECT position, team_name, matches_played, matches_won, matches_lost, matches_tied, matches_no_result, points, COALESCE(net_run_rate, 0.0) AS net_run_rate FROM gold_team_stats ORDER BY position""")
            self.log.info("\n🏆 (TransformProcessor) Final Team Standings:")
            print("-"*110); self.log.info(f"{'Pos':<4} {'Team':<30} {'Pld':<5} {'Won':<5} {'Lost':<5} {'Tied':<5} {'NR':<5} {'Pts':<5} {'NRR':>8}"); print("-"*110)
            for row_dict in final_cursor.fetchall():
                print(f"{row_dict[0]:<4} {row_dict[1]:<30} {row_dict[2]:<5} {row_dict[3]:<5} {row_dict[4]:<5} {row_dict[5]:<5} {row_dict[6]:<5} {row_dict[7]:<5} {float(row_dict[8]):>8.3f}")
            final_cursor.close()
            print("-"*110); self.log.info("\n✅ (TransformProcessor) GOLD layer transformation completed successfully")
        except Exception as e:
            self.log.error(f"❌ (TransformProcessor) GOLD transformation error: {e}")
            import traceback; traceback.print_exc()
            raise

    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            if hasattr(self, 'log'):
                self.log.info("MySQL connection closed")

    def __del__(self):
        try:
            self.close_connection()
        except Exception:
            pass
