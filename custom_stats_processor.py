# custom_stats_processor.py
import json
from collections import defaultdict
import re
import mysql.connector
from mysql.connector import Error
from transform_processor import TransformProcessor
from fuzzywuzzy import fuzz
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowException

class CustomStatsProcessor:
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

    def __init__(self, mysql_config=None):
        self.mysql_config = mysql_config or {
        'host': '  ',
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
                self.log.info("✅ (CustomStatsProcessor) Successfully connected to MySQL database")
        except Error as e:
            self.log.error(f"❌ (CustomStatsProcessor) Error connecting to MySQL: {e}")
            raise
    
    def close_connection(self):
        """Close the database connection if it exists"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.log.info("✅ (CustomStatsProcessor) MySQL connection closed")

    def _execute_sql(self, query, params=None, multi=False):
        if not self.connection or not self.connection.is_connected():
             self._create_db_connection()
        cursor = self.connection.cursor(buffered=True)
        is_primarily_select_query = query.strip().upper().startswith("SELECT")
        try:
            if multi:
                for result_iterator in cursor.execute(query, params, multi=True):
                     if result_iterator.with_rows:
                         pass
                     else:
                         pass
                self.connection.commit()
            else:
                cursor.execute(query, params)
                if not is_primarily_select_query:
                    self.connection.commit()
            return cursor
        except Error as e:
            self.log.error(f"❌ (CustomStatsProcessor) SQL Error: {e}\nQuery: {query}\nParams: {params}")
            try:
                if (not is_primarily_select_query and not multi) or multi:
                    self.connection.rollback()
            except Error as rb_err:
                self.log.error(f"❌ (CustomStatsProcessor) Error during rollback: {rb_err}")
            raise

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
        best_canonical_name = "Unknown"
        for canonical_name in self.KNOWN_TEAM_NAME_MAP.keys():
            score = fuzz.ratio(name_variant_lower, canonical_name.lower())
            if score > best_match_score:
                best_match_score = score
                best_canonical_name = canonical_name
        if best_match_score > 85:
            return best_canonical_name
        return "Unknown"

    def create_custom_gold_tables(self):
        queries = [
            """
            CREATE TABLE IF NOT EXISTS gold_bowler_clean_bowled_stats (
                bowler_id INT, 
                bowler_name VARCHAR(100), 
                team_name VARCHAR(100),
                total_clean_bowled_wickets INT DEFAULT 0,
                economy DECIMAL(5,2) DEFAULT NULL, 
                total_runs_conceded_for_econ INT DEFAULT NULL,
                total_overs_bowled_for_econ DECIMAL(7,3) DEFAULT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (bowler_id, team_name)
            )""",
            """
            CREATE TABLE IF NOT EXISTS gold_team_powerplay_stats (
                team_name VARCHAR(100) PRIMARY KEY, total_powerplay_innings INT DEFAULT 0,
                total_powerplay_runs INT DEFAULT 0, average_powerplay_score FLOAT DEFAULT 0.0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )""",
            """
            CREATE TABLE IF NOT EXISTS gold_batsman_performance_metrics (
                player_id INT, player_name VARCHAR(100), team_name VARCHAR(100),
                total_runs INT DEFAULT 0, total_balls_faced INT DEFAULT 0,
                boundary_dominance_ratio FLOAT DEFAULT 0.0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (player_id, team_name)
            )""",
            """
            CREATE TABLE IF NOT EXISTS gold_bowler_performance_metrics (
                player_id INT, player_name VARCHAR(100), team_name VARCHAR(100),
                total_wickets INT DEFAULT 0, total_runs_conceded INT DEFAULT 0,
                effectiveness_ratio FLOAT DEFAULT 0.0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (player_id, team_name)
            )""",
            """
            CREATE TABLE IF NOT EXISTS gold_team_head_to_head_stats (
                id INT AUTO_INCREMENT PRIMARY KEY, team1_name VARCHAR(100), team2_name VARCHAR(100),
                team1_wins INT DEFAULT 0, team2_wins INT DEFAULT 0, ties_or_no_result INT DEFAULT 0,
                total_matches INT DEFAULT 0, team1_win_percentage FLOAT DEFAULT 0.0, team2_win_percentage FLOAT DEFAULT 0.0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_h2h (team1_name, team2_name)
            )""",
            """
            CREATE TABLE IF NOT EXISTS gold_latest_match_summary (
                match_id VARCHAR(50) PRIMARY KEY,
                team1 VARCHAR(100) NOT NULL,
                team2 VARCHAR(100) NOT NULL,
                team1_score VARCHAR(20),
                team2_score VARCHAR(20),
                result VARCHAR(255) NOT NULL,
                top_batsman_team1 VARCHAR(100),
                top_batsman_team1_runs INT,
                top_batsman_team1_balls INT,
                top_batsman_team1_sr FLOAT,
                top_batsman_team2 VARCHAR(100),
                top_batsman_team2_runs INT,
                top_batsman_team2_balls INT,
                top_batsman_team2_sr FLOAT,
                top_bowler_team1 VARCHAR(100),
                top_bowler_team1_wickets INT,
                top_bowler_team1_runs INT,
                top_bowler_team1_econ FLOAT,
                top_bowler_team2 VARCHAR(100),
                top_bowler_team2_wickets INT,
                top_bowler_team2_runs INT,
                top_bowler_team2_econ FLOAT,
                match_date TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )""",
            """
            CREATE TABLE IF NOT EXISTS gold_fielder_catch_stats (
                fielder_id INT,
                fielder_name VARCHAR(100),
                team_name VARCHAR(100),
                total_catches_taken INT DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (fielder_id, team_name)
            )"""
        ]
        db_cursor = None
        try:
            for query in queries:
                db_cursor = self._execute_sql(query)
                if db_cursor: db_cursor.close(); db_cursor = None
        finally:
            if db_cursor: db_cursor.close()
        self.log.info("✅ (CustomStatsProcessor) Custom GOLD tables created/verified successfully")

    def _get_innings_list(self, scorecard_json):
        if not scorecard_json: return []
        innings_list = scorecard_json.get('scoreCard')
        if innings_list is None: innings_list = scorecard_json.get('scorecard')
        return innings_list if isinstance(innings_list, list) else []

    def _parse_fielder_from_outdec(self, out_dec_str):
        if not out_dec_str: return None
        match_c = re.match(r"c\s+([^b(]+?)\s*(?:b|run out|\(sub\)|st\s|\W*$)", out_dec_str.strip(), re.IGNORECASE)
        if match_c:
            fielder_name = match_c.group(1).strip()
            if fielder_name.lower() == "&":
                 match_c_and_b_bowler = re.match(r"c\s+&\s+b\s+([\w\s.-]+)", out_dec_str.strip(), re.IGNORECASE)
                 if match_c_and_b_bowler: return match_c_and_b_bowler.group(1).strip()
                 return None
            return fielder_name
        match_caught_by = re.search(r"caught by\s+([\w\s.-]+?)(?:\s+b\s+|$)", out_dec_str.strip(), re.IGNORECASE)
        if match_caught_by: return match_caught_by.group(1).strip()
        match_c_and_b = re.match(r"c & b\s+([\w\s.-]+)", out_dec_str.strip(), re.IGNORECASE)
        if match_c_and_b: return match_c_and_b.group(1).strip()
        return None

    def _build_player_map_from_scorecard(self, scorecard_json, match_id_for_log="UnknownMatch"):
        player_map = {}
        name_to_id_map = {}
        if not scorecard_json: return player_map, name_to_id_map

        match_header = scorecard_json.get("matchHeader", {}) 
        
        effective_team1_name_norm = "Unknown"
        effective_team2_name_norm = "Unknown"

        if match_header:
            team1_info_mh = match_header.get("team1", {})
            team2_info_mh = match_header.get("team2", {})
            if team1_info_mh and isinstance(team1_info_mh, dict) and team1_info_mh.get("name"):
                effective_team1_name_norm = self._normalize_team_name(team1_info_mh.get("name"))
            if team2_info_mh and isinstance(team2_info_mh, dict) and team2_info_mh.get("name"):
                candidate_t2_norm = self._normalize_team_name(team2_info_mh.get("name"))
                if candidate_t2_norm != "Unknown" and candidate_t2_norm != effective_team1_name_norm:
                    effective_team2_name_norm = candidate_t2_norm
                elif candidate_t2_norm != "Unknown" and effective_team1_name_norm == "Unknown": 
                    effective_team1_name_norm = candidate_t2_norm

            if (effective_team1_name_norm == "Unknown" or effective_team2_name_norm == "Unknown" or effective_team1_name_norm == effective_team2_name_norm):
                mti_list = match_header.get("matchTeamInfo")
                if isinstance(mti_list, list) and len(mti_list) > 0:
                    current_teams_found = []
                    if effective_team1_name_norm != "Unknown": current_teams_found.append(effective_team1_name_norm)
                    if effective_team2_name_norm != "Unknown" and effective_team2_name_norm not in current_teams_found: current_teams_found.append(effective_team2_name_norm)

                    for team_info_mti in mti_list:
                        if len(current_teams_found) == 2: break
                        if isinstance(team_info_mti, dict):
                            team_name_raw_mti = team_info_mti.get("teamName", team_info_mti.get("battingTeamName", team_info_mti.get("bowlingTeamName")))
                            if team_name_raw_mti:
                                norm_team_mti = self._normalize_team_name(team_name_raw_mti)
                                if norm_team_mti != "Unknown" and norm_team_mti not in current_teams_found:
                                    current_teams_found.append(norm_team_mti)
                    
                    if len(current_teams_found) >= 1: effective_team1_name_norm = current_teams_found[0]
                    if len(current_teams_found) >= 2: effective_team2_name_norm = current_teams_found[1]
        else:
            self.log.info(f"DBUG Player Map Build: No matchHeader found for match {match_id_for_log}. Relying on innings for team info.")

        needs_discovery_from_innings = (
            effective_team1_name_norm == "Unknown" or 
            effective_team2_name_norm == "Unknown" or
            (effective_team1_name_norm != "Unknown" and effective_team1_name_norm == effective_team2_name_norm)
        )

        innings_list_for_team_discovery = self._get_innings_list(scorecard_json)
        if needs_discovery_from_innings and innings_list_for_team_discovery:
            discovered_teams_set = set()
            if effective_team1_name_norm != "Unknown":
                discovered_teams_set.add(effective_team1_name_norm)
            if effective_team2_name_norm != "Unknown" and effective_team2_name_norm != effective_team1_name_norm : 
                discovered_teams_set.add(effective_team2_name_norm)

            for i_data in innings_list_for_team_discovery:
                if len(discovered_teams_set) >= 2: 
                    break
                if isinstance(i_data, dict):
                    bat_team_name_raw_disc = None
                    if 'batTeamDetails' in i_data: 
                        bat_team_name_raw_disc = i_data.get('batTeamDetails',{}).get('batTeamName')
                    elif 'batTeamName' in i_data: 
                        bat_team_name_raw_disc = i_data.get('batTeamName')
                    
                    if bat_team_name_raw_disc:
                        norm_team = self._normalize_team_name(bat_team_name_raw_disc)
                        if norm_team != "Unknown":
                            discovered_teams_set.add(norm_team) 
            
            discovered_list = list(discovered_teams_set)
            if len(discovered_list) >= 1:
                effective_team1_name_norm = discovered_list[0]
            if len(discovered_list) >= 2:
                effective_team2_name_norm = discovered_list[1]
            elif len(discovered_list) == 1: 
                effective_team2_name_norm = "Unknown"

        innings_list = self._get_innings_list(scorecard_json) 
        for innings_data in innings_list:
            if not isinstance(innings_data, dict): continue
            
            current_bat_team_norm_ing = "Unknown"
            current_bowl_team_norm_ing = "Unknown"

            bat_team_name_raw_ing = None
            if 'batTeamDetails' in innings_data: bat_team_name_raw_ing = innings_data.get('batTeamDetails', {}).get('batTeamName')
            elif 'batTeamName' in innings_data: bat_team_name_raw_ing = innings_data.get('batTeamName')
            
            if bat_team_name_raw_ing:
                current_bat_team_norm_ing = self._normalize_team_name(bat_team_name_raw_ing)

            if current_bat_team_norm_ing != "Unknown":
                if effective_team1_name_norm != "Unknown" and effective_team2_name_norm != "Unknown" and effective_team1_name_norm != effective_team2_name_norm:
                    if current_bat_team_norm_ing == effective_team1_name_norm:
                        current_bowl_team_norm_ing = effective_team2_name_norm
                    elif current_bat_team_norm_ing == effective_team2_name_norm:
                        current_bowl_team_norm_ing = effective_team1_name_norm
                elif effective_team1_name_norm != "Unknown" and current_bat_team_norm_ing != effective_team1_name_norm:
                    current_bowl_team_norm_ing = effective_team1_name_norm
                elif effective_team2_name_norm != "Unknown" and current_bat_team_norm_ing != effective_team2_name_norm:
                    current_bowl_team_norm_ing = effective_team2_name_norm

            player_sources_configs = [
                {'key_in_innings': 'batTeamDetails', 'player_list_path': ['batsmenData'], 'team_norm': current_bat_team_norm_ing, 'id_key': 'batId', 'name_keys': ['fullName', 'batName', 'name']}, # Prioritize fullName
                {'key_in_innings': 'bowlTeamDetails', 'player_list_path': ['bowlersData'], 'team_norm': current_bowl_team_norm_ing, 'id_key': 'bowlerId', 'name_keys': ['fullName', 'bowlName', 'name']}, # Prioritize fullName
                {'key_in_innings': 'batsman', 'player_list_path': None, 'team_norm': current_bat_team_norm_ing, 'id_key': 'id', 'name_keys': ['fullName', 'name']}, # Prioritize fullName
                {'key_in_innings': 'bowler', 'player_list_path': None, 'team_norm': current_bowl_team_norm_ing, 'id_key': 'id', 'name_keys': ['fullName', 'name']}  # Prioritize fullName
            ]
            
            for config in player_sources_configs:
                player_list_data = None
                if config['player_list_path'] is None: 
                    if config['key_in_innings'] in innings_data:
                        player_list_data = innings_data.get(config['key_in_innings'])
                else: 
                    if config['key_in_innings'] in innings_data:
                        current_level = innings_data.get(config['key_in_innings'], {})
                        for part in config['player_list_path']: 
                            if not isinstance(current_level, dict) : current_level = {}; break 
                            current_level = current_level.get(part, {})
                        if isinstance(current_level, dict): player_list_data = list(current_level.values()) 
                        elif isinstance(current_level, list): player_list_data = current_level

                if not player_list_data or not isinstance(player_list_data, list): continue

                team_for_this_list = config['team_norm']
                id_key = config['id_key']
                name_keys = config['name_keys']

                for p_data in player_list_data:
                    if not isinstance(p_data, dict): continue
                    p_id = p_data.get(id_key)
                    
                    p_name_candidate = ""
                    for nk in name_keys:
                        name_val = p_data.get(nk, "").strip()
                        if name_val:
                            p_name_candidate = name_val
                            break 
                    
                    if p_id and p_name_candidate:
                        is_new_name_generic = (p_name_candidate == "Unknown" or 
                                               p_name_candidate.startswith("Fielder ID") or 
                                               p_name_candidate.startswith("Player ID"))

                        if p_id in player_map:
                            if player_map[p_id]['team_name_normalized'] == "Unknown" and team_for_this_list != "Unknown":
                                player_map[p_id]['team_name_normalized'] = team_for_this_list
                            
                            current_name_in_map = player_map[p_id]['name']
                            is_current_name_generic = (not current_name_in_map or 
                                                       current_name_in_map == "Unknown" or 
                                                       current_name_in_map.startswith(f"Fielder ID {p_id}") or 
                                                       current_name_in_map.startswith(f"Player ID {p_id}"))

                            if not is_new_name_generic:
                                if is_current_name_generic:
                                    player_map[p_id]['name'] = p_name_candidate
                                elif len(p_name_candidate) > len(current_name_in_map):
                                    player_map[p_id]['name'] = p_name_candidate
                                elif len(p_name_candidate) == len(current_name_in_map) and \
                                     p_name_candidate.count(' ') > current_name_in_map.count(' ') and \
                                     not is_current_name_generic :
                                     player_map[p_id]['name'] = p_name_candidate


                        else:
                            player_map[p_id] = {
                                'name': p_name_candidate if not is_new_name_generic else f"Player ID {p_id}", 
                                'team_name_normalized': team_for_this_list
                            }
                        
                        if not is_new_name_generic:

                            if p_name_candidate.lower() not in name_to_id_map :
                                name_to_id_map[p_name_candidate.lower()] = p_id

        return player_map, name_to_id_map

    def _extract_fielder_from_dropped_catch(self, comm_text, commentary_formats, name_to_id_map, player_map):
        """Improved dropped catch extraction with comprehensive patterns"""
        DROP_KEYWORDS = ["dropped!", "spills", "drops", "put down", "missed chance", "spill", "misfield"]
        DROP_PATTERNS = [
            r"dropped by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:drops\b|spills|puts down|misfields)",
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+and\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+converge,\s+(?:the former|the latter)",
            r"missed chance\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+couldn't\s+hold\s+on",
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+shells\s+it"
        ]

        comm_text_lower = comm_text.lower()
        if not any(kw in comm_text_lower for kw in DROP_KEYWORDS):
            return None

        if commentary_formats and 'bold' in commentary_formats:
            bold_values = commentary_formats['bold'].get('formatValue', [])
            for bold_val in bold_values:
                if any(kw in bold_val.lower() for kw in ["drop", "spill"]):
                    for pattern in DROP_PATTERNS:
                        match = re.search(pattern, comm_text, re.IGNORECASE)
                        if match:
                            return self._resolve_fielder_from_match(match, comm_text)

        for pattern in DROP_PATTERNS:
            match = re.search(pattern, comm_text, re.IGNORECASE)
            if match:
                return self._resolve_fielder_from_match(match, comm_text)

        return self._fuzzy_match_fallback(comm_text, name_to_id_map)

    def _resolve_fielder_from_match(self, match, comm_text):
        """Determine fielder from regex match groups"""
        groups = match.groups()
        if len(groups) > 1 and 'former' in comm_text:
            return groups[0].strip()
        elif len(groups) > 1 and 'latter' in comm_text:
            return groups[1].strip()
        return groups[0].strip()

    def _fuzzy_match_fallback(self, comm_text, name_to_id_map):
        """Final attempt using fuzzy matching"""
        potential_names = re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b", comm_text)
        best_score, best_name = 80, None

        for name in potential_names:
            for known_name_lower in name_to_id_map:
                score = fuzz.ratio(name.lower(), known_name_lower)
                if score > best_score: 
                    best_score, best_name = score, name 
        return best_name if best_score >= 85 else None

    def calculate_fielder_catches(self):
        self.log.info("ℹ️ (CustomStatsProcessor) Calculating fielder stats (taken)...")

        aggregated_stats = defaultdict(lambda: {
            'name': "Unknown",
            'catches_taken': 0,
            'matches_present_in': set()
        })

        db_cursor_sc = None
        scorecards_data = []
        try:
            db_cursor_sc = self._execute_sql("SELECT match_id, json_data FROM raw_scorecard")
            scorecards_data = db_cursor_sc.fetchall()
        except Error as e:
             self.log.error(f"❌ Error fetching scorecard data: {e}"); return
        finally:
            if db_cursor_sc: db_cursor_sc.close()

        if not scorecards_data:
            self.log.info("⚠️ No scorecard data found in raw_scorecard table."); return

        match_player_maps = {}

        for match_id_raw, scorecard_json_str in scorecards_data:
            match_id = str(match_id_raw)
            scorecard_json = None
            try:
                scorecard_json = json.loads(scorecard_json_str)
                player_map, name_to_id_map = self._build_player_map_from_scorecard(scorecard_json, match_id)
                match_player_maps[match_id] = {'player_map': player_map, 'name_to_id_map': name_to_id_map}

                if not player_map: 
                    self.log.info(f"DBUG (calculate_fielder_catches): Empty player_map for match {match_id}. Skipping catch processing for this match.")
                    continue
                
                innings_list = self._get_innings_list(scorecard_json)

                for innings_data in innings_list:
                    if not isinstance(innings_data, dict): continue
                    
                    batsmen_list_source = []
                    is_structure_A = 'batTeamDetails' in innings_data
                    
                    if is_structure_A:
                        s_data = innings_data.get('batTeamDetails', {}).get('batsmenData', {})
                        if isinstance(s_data, dict): batsmen_list_source = list(s_data.values())
                    else:
                        batsmen_list_source = innings_data.get('batsman', [])
                        if not isinstance(batsmen_list_source, list): batsmen_list_source = []


                    for batsman_details in batsmen_list_source:
                        if not isinstance(batsman_details, dict): continue
                        
                        fielder_id = None; is_catch = False
                        batsman_name_debug = batsman_details.get('batName', batsman_details.get('name', 'N/A'))


                        if is_structure_A:
                            wc = batsman_details.get('wicketCode')
                            fid = batsman_details.get('fielderId1') 
                            if wc and wc.upper() == "CAUGHT" and fid is not None and fid != 0:
                                is_catch, fielder_id = True, fid
                        else:
                            od = batsman_details.get('outDec', "")
                            if od and (od.lower().startswith("c ") or "caught by" in od.lower()) and \
                               "run out" not in od.lower() and "stumped" not in od.lower() and "hit wicket" not in od.lower():
                                fn = self._parse_fielder_from_outdec(od)
                                if fn:
                                    fid = name_to_id_map.get(fn.lower())
                                    if fid: is_catch, fielder_id = True, fid
                                    else: 
                                        best_s = 0; temp_id = None; matched_n = ""
                                        for n_key_lower, p_id_val in name_to_id_map.items():
                                            s = fuzz.ratio(fn.lower(), n_key_lower)
                                            if s > best_s: best_s, temp_id, matched_n = s, p_id_val, n_key_lower
                                        if best_s >= 90: 
                                            is_catch, fielder_id = True, temp_id
                                            
                        if is_catch and fielder_id:
                            f_info = player_map.get(fielder_id)
                            if f_info:
                                p_name, team_norm = f_info['name'], f_info['team_name_normalized']
                                
                                if team_norm == "Unknown":
                                     self.log.info(f"DBUG (calculate_fielder_catches): Fielder ID {fielder_id} ({p_name}) has 'Unknown' team in player_map for match {match_id} (Batsman: {batsman_name_debug}). Check source data or _build_player_map.")

                                key = (fielder_id, team_norm)
                                stat = aggregated_stats[key]
                                if stat['name'] == "Unknown" or stat['name'].startswith("Fielder ID"): stat['name'] = p_name
                                stat['catches_taken'] += 1
                                stat['matches_present_in'].add(match_id)
                            else: 
                                self.log.info(f"DBUG (calculate_fielder_catches): Fielder ID {fielder_id} from scorecard event (Batsman: {batsman_name_debug}) not found in player_map for match {match_id}. Will be marked as 'Unknown Fielder Team'.")
                                key = (fielder_id, "Unknown Fielder Team") 
                                aggregated_stats[key]['name'] = f"Fielder ID {fielder_id}"
                                aggregated_stats[key]['catches_taken'] += 1
                                aggregated_stats[key]['matches_present_in'].add(match_id)

            except Exception as e:
                self.log.error(f"⚠️ Error processing scorecard for catches taken (match {match_id}): {type(e).__name__} - {e} (Line: {e.__traceback__.tb_lineno if e.__traceback__ else 'N/A'})")


        if not aggregated_stats:
            self.log.info("ℹ️ No fielder stats (taken) to insert/update.")
            return

        self.log.info(f"ℹ️ Inserting/Updating gold_fielder_catch_stats for {len(aggregated_stats)} fielder-team combinations...")
        success_count, fail_count = 0, 0
        try:
            for (fielder_id, team_name), data in aggregated_stats.items():
                if team_name == "Unknown" or team_name == "Unknown Fielder Team":
                    self.log.info(f"SKIPPING insert for Fielder ID {fielder_id} (Name: {data['name']}) due to unresolved team: '{team_name}'. Matches: {data['matches_present_in']}")
                    continue

                player_final_name = data['name']
                if player_final_name == "Unknown" or player_final_name.startswith("Fielder ID"):
                     player_final_name = f"Fielder ID {fielder_id}"

                catches_taken = data['catches_taken']

                insert_query = """
                    INSERT INTO gold_fielder_catch_stats
                        (fielder_id, fielder_name, team_name, total_catches_taken)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        fielder_name = VALUES(fielder_name),
                        total_catches_taken = VALUES(total_catches_taken)
                """
                params = (fielder_id, player_final_name, team_name, catches_taken)

                current_insert_cursor = None
                try:
                    current_insert_cursor = self._execute_sql(insert_query, params)
                    success_count += 1
                except Error as ins_err:
                     self.log.error(f"❌ Failed to insert/update fielder {fielder_id} ({player_final_name}) for team {team_name}: {ins_err}")
                     fail_count += 1
                finally:
                    if current_insert_cursor: current_insert_cursor.close()
        finally:
            pass
        self.log.info(f"✅ (CustomStatsProcessor) Fielder catch stats calculation completed. Inserted/Updated: {success_count}, Skipped (Unknown Team): {len(aggregated_stats) - success_count - fail_count}, Failed DB: {fail_count}")
    
    def update_latest_match_summary(self):
        self.log.info("ℹ️ Updating latest match summary with detailed stats...")
        
        try:
            cursor = self._execute_sql(
                "SELECT match_id, json_data FROM raw_scorecard "
                "ORDER BY match_id DESC LIMIT 1"
            )
            latest_match = cursor.fetchone()
            cursor.close()
        except Error as e:
            self.log.error(f"❌ Error fetching latest match: {e}")
            return

        if not latest_match:
            self.log.info("ℹ️ No matches found in raw_scorecard")
            return

        match_id, json_str = latest_match
        try:
            scorecard = json.loads(json_str)
            header = scorecard.get('matchHeader', {})
            

            team1 = self._normalize_team_name(header.get('team1', {}).get('name', 'Unknown'))
            team2 = self._normalize_team_name(header.get('team2', {}).get('name', 'Unknown'))

            if team1 == "Unknown" or team2 == "Unknown":
                match_id_str = str(match_id)
                if '_vs_' in match_id_str:
                    parts = match_id_str.split('_vs_')
                    if len(parts) >= 2:
                        team1_from_id = re.sub(r"([A-Z])", r" \1", parts[0].split('_')[-1]).strip()
                        team2_from_id = re.sub(r"([A-Z])", r" \1", parts[1].split('_')[0]).strip()
                        team1 = self._normalize_team_name(team1_from_id) if team1 == "Unknown" else team1
                        team2 = self._normalize_team_name(team2_from_id) if team2 == "Unknown" else team2

            result = scorecard.get('status', 'Result not available')
            summary_data = {
                'match_id': match_id,
                'team1': team1,
                'team2': team2,
                'result': result,
                'team1_score': '',
                'team2_score': '',
                'top_batsman_team1': '',
                'top_batsman_team1_runs': 0,
                'top_batsman_team1_balls': 0,
                'top_batsman_team1_sr': 0.0,
                'top_batsman_team2': '',
                'top_batsman_team2_runs': 0,
                'top_batsman_team2_balls': 0,
                'top_batsman_team2_sr': 0.0,
                'top_bowler_team1': '',
                'top_bowler_team1_wickets': 0,
                'top_bowler_team1_runs': 0,
                'top_bowler_team1_econ': 0.0,
                'top_bowler_team2': '',
                'top_bowler_team2_wickets': 0,
                'top_bowler_team2_runs': 0,
                'top_bowler_team2_econ': 0.0
            }

            for innings in scorecard.get('scorecard', []):
                is_structure_A = 'batTeamDetails' in innings
                bat_team = "Unknown"
                if is_structure_A:
                    bat_team = self._normalize_team_name(innings.get('batTeamDetails', {}).get('batTeamName', 'Unknown'))
                else:
                    bat_team = self._normalize_team_name(innings.get('batTeamName', 'Unknown'))
                is_team1 = bat_team == team1

                if is_team1:
                    summary_data['team1_score'] = f"{innings.get('score', 'N/A')}/{innings.get('wickets', 'N/A')}"
                else:
                    summary_data['team2_score'] = f"{innings.get('score', 'N/A')}/{innings.get('wickets', 'N/A')}"

                batsmen = []
                if is_structure_A:
                    batsmen_data = innings.get('batTeamDetails', {}).get('batsmenData', {})
                    batsmen = list(batsmen_data.values()) if isinstance(batsmen_data, dict) else []
                else:
                    batsmen = innings.get('batsman', [])

                if batsmen:
                    top_batsman = max(batsmen, key=lambda x: int(x.get('runs', 0)))
                    runs = int(top_batsman.get('runs', 0))
                    balls = int(top_batsman.get('balls', 0))
                    sr = round((runs / max(1, balls)) * 100, 2) if balls > 0 else 0.0
                    
                    if is_team1:
                        summary_data.update({
                            'top_batsman_team1': top_batsman.get('batName', top_batsman.get('name', 'N/A')),
                            'top_batsman_team1_runs': runs,
                            'top_batsman_team1_balls': balls,
                            'top_batsman_team1_sr': sr
                        })
                    else:
                        summary_data.update({
                            'top_batsman_team2': top_batsman.get('batName', top_batsman.get('name', 'N/A')),
                            'top_batsman_team2_runs': runs,
                            'top_batsman_team2_balls': balls,
                            'top_batsman_team2_sr': sr
                        })

                bowlers = []
                if is_structure_A:
                    bowlers_data = innings.get('bowlTeamDetails', {}).get('bowlersData', {})
                    bowlers = list(bowlers_data.values()) if isinstance(bowlers_data, dict) else []
                else:
                    bowlers = innings.get('bowler', [])

                if bowlers:
                    top_bowler = max(
                        bowlers,
                        key=lambda x: (
                            int(x.get('wickets', 0)),
                            -float(x.get('economy', 999))
                        )
                    )
                    if is_team1:
                        summary_data.update({
                            'top_bowler_team2': top_bowler.get('bowlName', top_bowler.get('name', 'N/A')),
                            'top_bowler_team2_wickets': int(top_bowler.get('wickets', 0)),
                            'top_bowler_team2_runs': int(top_bowler.get('runs', 0)),
                            'top_bowler_team2_econ': float(top_bowler.get('economy', 0))
                        })
                    else:
                        summary_data.update({
                            'top_bowler_team1': top_bowler.get('bowlName', top_bowler.get('name', 'N/A')),
                            'top_bowler_team1_wickets': int(top_bowler.get('wickets', 0)),
                            'top_bowler_team1_runs': int(top_bowler.get('runs', 0)),
                            'top_bowler_team1_econ': float(top_bowler.get('economy', 0))
                        })

            self._execute_sql("""
                INSERT INTO gold_latest_match_summary (
                    match_id, team1, team2, team1_score, team2_score, result,
                    top_batsman_team1, top_batsman_team1_runs, top_batsman_team1_balls, top_batsman_team1_sr,
                    top_batsman_team2, top_batsman_team2_runs, top_batsman_team2_balls, top_batsman_team2_sr,
                    top_bowler_team1, top_bowler_team1_wickets, top_bowler_team1_runs, top_bowler_team1_econ,
                    top_bowler_team2, top_bowler_team2_wickets, top_bowler_team2_runs, top_bowler_team2_econ
                ) VALUES (
                    %(match_id)s, %(team1)s, %(team2)s, %(team1_score)s, %(team2_score)s, %(result)s,
                    %(top_batsman_team1)s, %(top_batsman_team1_runs)s, %(top_batsman_team1_balls)s, %(top_batsman_team1_sr)s,
                    %(top_batsman_team2)s, %(top_batsman_team2_runs)s, %(top_batsman_team2_balls)s, %(top_batsman_team2_sr)s,
                    %(top_bowler_team1)s, %(top_bowler_team1_wickets)s, %(top_bowler_team1_runs)s, %(top_bowler_team1_econ)s,
                    %(top_bowler_team2)s, %(top_bowler_team2_wickets)s, %(top_bowler_team2_runs)s, %(top_bowler_team2_econ)s
                )
                ON DUPLICATE KEY UPDATE
                    team1_score = VALUES(team1_score),
                    team2_score = VALUES(team2_score),
                    result = VALUES(result),
                    top_batsman_team1 = VALUES(top_batsman_team1),
                    top_batsman_team1_runs = VALUES(top_batsman_team1_runs),
                    top_batsman_team1_balls = VALUES(top_batsman_team1_balls),
                    top_batsman_team1_sr = VALUES(top_batsman_team1_sr),
                    top_batsman_team2 = VALUES(top_batsman_team2),
                    top_batsman_team2_runs = VALUES(top_batsman_team2_runs),
                    top_batsman_team2_balls = VALUES(top_batsman_team2_balls),
                    top_batsman_team2_sr = VALUES(top_batsman_team2_sr),
                    top_bowler_team1 = VALUES(top_bowler_team1),
                    top_bowler_team1_wickets = VALUES(top_bowler_team1_wickets),
                    top_bowler_team1_runs = VALUES(top_bowler_team1_runs),
                    top_bowler_team1_econ = VALUES(top_bowler_team1_econ),
                    top_bowler_team2 = VALUES(top_bowler_team2),
                    top_bowler_team2_wickets = VALUES(top_bowler_team2_wickets),
                    top_bowler_team2_runs = VALUES(top_bowler_team2_runs),
                    top_bowler_team2_econ = VALUES(top_bowler_team2_econ),
                    last_updated = CURRENT_TIMESTAMP
            """, summary_data)

            self.log.info(f"✅ Updated detailed match summary for {team1} vs {team2}")

        except Exception as e:
            self.log.error(f"❌ Error processing latest match: {str(e)}")

    def calculate_bowler_clean_bowled_stats(self):
        self.log.info("ℹ️ (CustomStatsProcessor) Calculating bowler clean bowled stats and economy...")
        
        bowler_aggregated_data = defaultdict(lambda: {
            'name': "Unknown",
            'team': "Unknown",
            'clean_bowled_wickets': 0, 
            'total_runs_for_economy': 0,
            'total_balls_for_economy': 0,
            'economy': 0.0
        })
        
        db_cursor_sc = None
        scorecards_data = []
        try:
            db_cursor_sc = self._execute_sql("SELECT match_id, json_data FROM raw_scorecard")
            scorecards_data = db_cursor_sc.fetchall()
        except Error as e:
            self.log.info(f"❌ Error fetching scorecard data: {e}")
            return
        finally:
            if db_cursor_sc: db_cursor_sc.close()

        if not scorecards_data:
            self.log.info("⚠️ No scorecard data found for bowler stats.")
            return

        total_bowled_dismissals_identified_debug = 0

        for match_id_raw_db, scorecard_json_str in scorecards_data:
            match_id_for_logs = str(match_id_raw_db)
            scorecard_json = None
            try:
                scorecard_json = json.loads(scorecard_json_str)
                player_map, name_to_id_map = self._build_player_map_from_scorecard(scorecard_json, match_id_for_logs)

                if not player_map:
                    continue

                innings_list = self._get_innings_list(scorecard_json)
                
                match_playing_team1_norm = "Unknown"
                match_playing_team2_norm = "Unknown"
                match_header = scorecard_json.get("matchHeader", {})
                if match_header:
                    team1_mh_raw = match_header.get("team1", {}).get("name")
                    team2_mh_raw = match_header.get("team2", {}).get("name")
                    if team1_mh_raw: match_playing_team1_norm = self._normalize_team_name(team1_mh_raw)
                    if team2_mh_raw:
                        cand_t2 = self._normalize_team_name(team2_mh_raw)
                        if cand_t2 != "Unknown":
                            if match_playing_team1_norm == "Unknown": match_playing_team1_norm = cand_t2
                            elif cand_t2 != match_playing_team1_norm: match_playing_team2_norm = cand_t2
                
                still_needs_teams = (match_playing_team1_norm == "Unknown" or match_playing_team2_norm == "Unknown" or \
                                    (match_playing_team1_norm != "Unknown" and match_playing_team1_norm == match_playing_team2_norm))
                if still_needs_teams and isinstance(match_id_raw_db, str) and "_vs_" in match_id_raw_db:
                    id_parts = match_id_raw_db.split('_'); team_name_section_of_id = '_'.join(id_parts[1:]) if len(id_parts) > 1 and id_parts[0].isdigit() else match_id_raw_db
                    if "_vs_" in team_name_section_of_id:
                        name_parts = team_name_section_of_id.split("_vs_")
                        if len(name_parts) == 2:
                            cand_t1_from_id = self._normalize_team_name(name_parts[0].replace("_", " "))
                            cand_t2_from_id = self._normalize_team_name(name_parts[1].replace("_", " "))
                            if match_playing_team1_norm == "Unknown" and cand_t1_from_id != "Unknown": match_playing_team1_norm = cand_t1_from_id
                            if match_playing_team2_norm == "Unknown" and cand_t2_from_id != "Unknown" and cand_t2_from_id != match_playing_team1_norm: match_playing_team2_norm = cand_t2_from_id
                            if match_playing_team1_norm == "Unknown" and cand_t1_from_id != "Unknown" and cand_t1_from_id != match_playing_team2_norm: match_playing_team1_norm = cand_t1_from_id
                
                still_needs_teams = (match_playing_team1_norm == "Unknown" or match_playing_team2_norm == "Unknown" or \
                                    (match_playing_team1_norm != "Unknown" and match_playing_team1_norm == match_playing_team2_norm))
                if still_needs_teams and innings_list:
                    discovered_teams = set()
                    if match_playing_team1_norm != "Unknown": discovered_teams.add(match_playing_team1_norm)
                    if match_playing_team2_norm != "Unknown" and match_playing_team2_norm != match_playing_team1_norm: discovered_teams.add(match_playing_team2_norm)
                    for i_data in innings_list:
                        if len(discovered_teams) >= 2 and len(set(list(discovered_teams)[:2])) == 2: break
                        bat_team_name_raw_ing = i_data.get('batTeamDetails',{}).get('batTeamName') or i_data.get('batTeamName')
                        if bat_team_name_raw_ing:
                            norm_team_ing = self._normalize_team_name(bat_team_name_raw_ing)
                            if norm_team_ing != "Unknown": discovered_teams.add(norm_team_ing)
                    temp_list_from_set = list(d for d in discovered_teams if d != "Unknown") 
                    if len(temp_list_from_set) == 1: match_playing_team1_norm, match_playing_team2_norm = temp_list_from_set[0], "Unknown"
                    elif len(temp_list_from_set) >= 2:
                        match_playing_team1_norm, match_playing_team2_norm = temp_list_from_set[0], temp_list_from_set[1]
                        if match_playing_team1_norm == match_playing_team2_norm: 
                            match_playing_team2_norm = temp_list_from_set[2] if len(temp_list_from_set) > 2 and temp_list_from_set[2] != match_playing_team1_norm else "Unknown"

                if match_playing_team1_norm == "Unknown" or match_playing_team2_norm == "Unknown" or match_playing_team1_norm == match_playing_team2_norm:
                    continue 
                if not innings_list: continue

                for innings_data in innings_list: 
                    if not isinstance(innings_data, dict): continue

                    is_structure_A = 'batTeamDetails' in innings_data
                    current_bat_team_name_ing_raw = innings_data.get('batTeamDetails', {}).get('batTeamName') if is_structure_A else innings_data.get('batTeamName')
                    current_bat_team_norm = self._normalize_team_name(current_bat_team_name_ing_raw)
                    if current_bat_team_norm == "Unknown": continue

                    current_bowling_team_norm = "Unknown"
                    if current_bat_team_norm == match_playing_team1_norm: current_bowling_team_norm = match_playing_team2_norm
                    elif current_bat_team_norm == match_playing_team2_norm: current_bowling_team_norm = match_playing_team1_norm
                    if current_bowling_team_norm == "Unknown": continue 

                    bowler_list_source = innings_data.get('bowler', [])
                    if not bowler_list_source and 'bowlTeamDetails' in innings_data:
                        bowler_list_source = list(innings_data.get('bowlTeamDetails', {}).get('bowlersData', {}).values())

                    for bowler_perf in bowler_list_source:
                        if not isinstance(bowler_perf, dict): continue
                        bowler_id_econ = bowler_perf.get('id', bowler_perf.get('bowlerId'))
                        if bowler_id_econ is None: continue

                        bowler_name_econ_raw = bowler_perf.get('name') or bowler_perf.get('fullName') or bowler_perf.get('bowlName')
                        bowler_name_econ = player_map.get(bowler_id_econ, {}).get('name', bowler_name_econ_raw or f"Player ID {bowler_id_econ}")

                        runs_str = bowler_perf.get('r', bowler_perf.get('runs', "0")) 
                        overs_str = bowler_perf.get('ov', bowler_perf.get('overs', "0"))
                        balls_bowled_direct = bowler_perf.get('balls')

                        try: runs_conceded = int(runs_str)
                        except (ValueError, TypeError): runs_conceded = 0
                        
                        current_balls_for_spell = 0
                        if balls_bowled_direct is not None:
                            try: current_balls_for_spell = int(balls_bowled_direct)
                            except (ValueError, TypeError): pass
                        elif isinstance(overs_str, str) and '.' in overs_str:
                            parts = overs_str.split('.')
                            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                                main_overs, balls_in_over = int(parts[0]), int(parts[1])
                                if 0 <= balls_in_over <= 5: current_balls_for_spell = (main_overs * 6) + balls_in_over
                        elif isinstance(overs_str, (str, int, float)) and str(overs_str).replace('.', '', 1).isdigit():
                            try: current_balls_for_spell = int(float(overs_str)) * 6
                            except ValueError: pass
                        
                        if current_balls_for_spell > 0:
                            key_econ = (bowler_id_econ, current_bowling_team_norm)
                            agg_data = bowler_aggregated_data[key_econ]
                            if agg_data['name'] == "Unknown" or agg_data['name'].startswith("Player ID"):
                                agg_data['name'] = bowler_name_econ 
                            agg_data['team'] = current_bowling_team_norm
                            agg_data['total_runs_for_economy'] += runs_conceded
                            agg_data['total_balls_for_economy'] += current_balls_for_spell

                    batsmen_list_for_wickets = list(innings_data.get('batTeamDetails', {}).get('batsmenData', {}).values()) if is_structure_A \
                                   else innings_data.get('batsman', [])
                    if not isinstance(batsmen_list_for_wickets, list): batsmen_list_for_wickets = []

                    for batsman_details in batsmen_list_for_wickets:
                        if not isinstance(batsman_details, dict): continue
                        
                        is_bowled_dismissal = False
                        bowler_id_wicket = None
                        
                        if is_structure_A:
                            wicket_code = batsman_details.get('wicketCode', "").upper()
                            if wicket_code == "BOWLED":
                                is_bowled_dismissal = True
                                bowler_id_wicket = batsman_details.get('bowlerId') 
                        else:
                            out_dec = batsman_details.get('outDec', "")
                            if "c & b" not in out_dec.lower() and \
                               ("run out" not in out_dec.lower()) and \
                               ("stumped" not in out_dec.lower()) and \
                               ("hit wicket" not in out_dec.lower()) and \
                               (out_dec.lower().startswith("b ") or " bowled " in out_dec.lower() or re.search(r"\sb\s(?!\S)", out_dec.lower())):
                                

                                match_b = re.search(r"(?:^b\s+|^bowled\s+|\sb\s+)([\w\s.'-]+?)(?:\s*\(|\s*$|\[)", out_dec, re.IGNORECASE)
                                if not match_b: 
                                     match_b = re.search(r"(?:b|bowled)\s+([\w\s.'-]+)", out_dec, re.IGNORECASE)

                                if match_b:
                                    bowler_name_from_desc = match_b.group(1).strip()
                                    bowler_id_wicket_temp = name_to_id_map.get(bowler_name_from_desc.lower())
                                    if bowler_id_wicket_temp is not None: 
                                        bowler_id_wicket = bowler_id_wicket_temp
                                        is_bowled_dismissal = True
                                    else: 
                                        best_s, temp_id_fuzzy, matched_n = 0, None, ""
                                        for n_key_lower, p_id_val in name_to_id_map.items(): 
                                            s = fuzz.ratio(bowler_name_from_desc.lower(), n_key_lower)
                                            if s > best_s: 
                                                best_s, temp_id_fuzzy, matched_n = s, p_id_val, n_key_lower
                                        if best_s >= 90: 
                                            bowler_id_wicket = temp_id_fuzzy
                                            is_bowled_dismissal = True

                        if is_bowled_dismissal and bowler_id_wicket is not None:
                            total_bowled_dismissals_identified_debug += 1
                            bowler_info = player_map.get(bowler_id_wicket) 
                            if bowler_info:
                                key_wicket = (bowler_id_wicket, current_bowling_team_norm)
                                current_stat_entry = bowler_aggregated_data[key_wicket]
                                
                                if current_stat_entry['name'] == "Unknown" or current_stat_entry['name'].startswith("Player ID"):
                                    current_stat_entry['name'] = bowler_info['name']
                                current_stat_entry['team'] = current_bowling_team_norm 
                                current_stat_entry['clean_bowled_wickets'] += 1 

            except json.JSONDecodeError as je:
                self.log.error(f"⚠️ JSON Decode Error for scorecard match_id {match_id_for_logs}: {je}")
            except Exception as e:
                self.log.error(f"⚠️ Error processing stats for match {match_id_for_logs}: {type(e).__name__} - {e} (Line: {e.__traceback__.tb_lineno if e.__traceback__ else 'N/A'})")
        
        self.log.info(f"DEBUG: Total 'Clean Bowled' dismissals identified for processing: {total_bowled_dismissals_identified_debug}")

        for key, data_entry in bowler_aggregated_data.items():
            if data_entry['total_balls_for_economy'] > 0:
                data_entry['economy'] = round((data_entry['total_runs_for_economy'] * 6.0) / data_entry['total_balls_for_economy'], 2)
            else:
                data_entry['economy'] = 0.0 

        if not bowler_aggregated_data:
            self.log.info("ℹ️ No bowler stats derived (clean bowled/economy) to insert/update.")
            return

        self.log.info(f"ℹ️ Inserting/Updating gold_bowler_clean_bowled_stats for {len(bowler_aggregated_data)} bowler-team combinations...")
        try:
            for (key_bowler_id, key_team_name), data_to_insert in bowler_aggregated_data.items():
                if not data_to_insert['name'] or data_to_insert['name'] == "Unknown" or data_to_insert['name'].startswith(("Bowler ID", "Player ID")) or \
                   not data_to_insert['team'] or data_to_insert['team'] == "Unknown":
                    continue
                if key_bowler_id is None: continue
                
                if data_to_insert['clean_bowled_wickets'] == 0 and data_to_insert['total_balls_for_economy'] == 0:
                    continue


                current_op_cursor = None
                try:
                    current_op_cursor = self._execute_sql("""
                        INSERT INTO gold_bowler_clean_bowled_stats 
                            (bowler_id, bowler_name, team_name, total_clean_bowled_wickets, economy) 
                        VALUES (%s, %s, %s, %s, %s) 
                        ON DUPLICATE KEY UPDATE 
                            bowler_name = VALUES(bowler_name), 
                            team_name = VALUES(team_name), 
                            total_clean_bowled_wickets = VALUES(total_clean_bowled_wickets),
                            economy = VALUES(economy)
                    """, (key_bowler_id, data_to_insert['name'], data_to_insert['team'], 
                          data_to_insert['clean_bowled_wickets'], data_to_insert['economy']))
                finally:
                    if current_op_cursor: current_op_cursor.close()
        except Error as db_batch_err:
            self.log.error(f"❌ DB Error during batch insert/update for clean bowled stats: {db_batch_err}")
        except Exception as batch_exc:
             self.log.error(f"❌ Unexpected error during batch insert/update for clean bowled stats: {batch_exc}")
        self.log.info("✅ (CustomStatsProcessor) Bowler clean bowled stats (with economy) calculation completed.")

    def calculate_team_avg_powerplay_score(self):
        self.log.info("ℹ️ (CustomStatsProcessor) Calculating team average powerplay scores...")
        team_stats = defaultdict(lambda: {'total_runs': 0, 'innings_count': 0})
        db_cursor = None; scorecards_data = []
        try:
            db_cursor = self._execute_sql("SELECT match_id, json_data FROM raw_scorecard")
            scorecards_data = db_cursor.fetchall()
        finally:
            if db_cursor: db_cursor.close()

        for match_id, json_data_str in scorecards_data:
            try:
                scorecard = json.loads(json_data_str)
                innings_list = self._get_innings_list(scorecard)

                for innings_data in innings_list:
                    bat_team_name_raw = None
                    pp_runs = 0
                    found_pp = False

                    if 'batTeamDetails' in innings_data and 'ppData' in innings_data:
                        bat_team_name_raw = innings_data.get("batTeamDetails", {}).get("batTeamName")
                        pp_info_source = innings_data.get("ppData", {}).get("pp_1", {})
                        if isinstance(pp_info_source, dict) and \
                           pp_info_source.get("ppType") == "mandatory" and \
                           pp_info_source.get("ppOversTo") == 6.0 and \
                           "runsScored" in pp_info_source:
                            pp_runs = pp_info_source.get("runsScored", 0)
                            found_pp = True

                    elif 'pp' in innings_data and 'batTeamName' in innings_data:
                        bat_team_name_raw = innings_data.get("batTeamName")
                        pp_obj = innings_data.get("pp", {})
                        if isinstance(pp_obj, dict) and 'powerPlay' in pp_obj:
                            for pp_segment in pp_obj.get("powerPlay", []):
                                if isinstance(pp_segment, dict) and \
                                   pp_segment.get("ppType") == "mandatory" and \
                                   pp_segment.get("ovrTo") == 6.0 and \
                                   "run" in pp_segment:
                                    pp_runs = pp_segment.get("run", 0)
                                    found_pp = True
                                    break

                    if found_pp and bat_team_name_raw:
                        bat_team_name = self._normalize_team_name(bat_team_name_raw)
                        if bat_team_name != "Unknown":
                            if isinstance(pp_runs, (int, float)):
                                team_stats[bat_team_name]['total_runs'] += int(pp_runs)
                                team_stats[bat_team_name]['innings_count'] += 1
                            else:
                                self.log.info(f"⚠️ Invalid PP runs type '{type(pp_runs)}' for match {match_id}, team {bat_team_name}")


            except json.JSONDecodeError as je: self.log.error(f"⚠️ JSON Decode Error (PP) for match_id {match_id}: {je}")
            except Exception as e: self.log.error(f"⚠️ Error processing PP for match {match_id}: {type(e).__name__} - {e} (Line: {e.__traceback__.tb_lineno if e.__traceback__ else 'N/A'})")

        db_ins_cursor = None
        try:
            for team_name, data in team_stats.items():
                avg_score = (data['total_runs'] / data['innings_count']) if data['innings_count'] > 0 else 0.0
                db_ins_cursor = self._execute_sql("""INSERT INTO gold_team_powerplay_stats (team_name, total_powerplay_innings, total_powerplay_runs, average_powerplay_score)
                    VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE total_powerplay_innings = VALUES(total_powerplay_innings),
                    total_powerplay_runs = VALUES(total_powerplay_runs), average_powerplay_score = VALUES(average_powerplay_score)
                """, (team_name, data['innings_count'], data['total_runs'], round(avg_score, 2)))
                if db_ins_cursor: db_ins_cursor.close(); db_ins_cursor=None
        finally:
            if db_ins_cursor: db_ins_cursor.close()
        self.log.info("✅ (CustomStatsProcessor) Team average powerplay scores calculated.")

    def calculate_batsman_performance_metrics(self):
        self.log.info("ℹ️ (CustomStatsProcessor) Calculating batsman performance metrics (Boundary Dominance)...")
        db_cursor = None; column_names = []; results = []
        try:
            db_cursor = self._execute_sql("""
                SELECT batsman_id AS player_id,
                    SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT batsman_name ORDER BY LENGTH(batsman_name) DESC SEPARATOR '|'), '|', 1) AS player_name,
                    batting_team AS team_name_raw, SUM(runs_scored) AS total_runs, SUM(balls_faced) AS total_balls_faced,
                    SUM(fours) AS total_fours, SUM(sixes) AS total_sixes
                FROM silver_batting WHERE batsman_id IS NOT NULL AND batting_team IS NOT NULL AND batting_team <> 'Unknown'
                AND batsman_name IS NOT NULL AND batsman_name <> 'Unknown' GROUP BY batsman_id, batting_team""")
            if db_cursor and db_cursor.description: column_names = [col[0] for col in db_cursor.description]
            results = db_cursor.fetchall() if db_cursor else []
        finally:
            if db_cursor: db_cursor.close()

        inserted_count = 0
        for row_tuple in results:
            if not column_names: continue
            row = dict(zip(column_names, row_tuple))
            normalized_team_name = self._normalize_team_name(row.get('team_name_raw'))
            if normalized_team_name == "Unknown": continue

            runs_from_boundaries = (row.get('total_fours', 0) * 4) + (row.get('total_sixes', 0) * 6)
            total_runs = row.get('total_runs', 0)
            bdr = (runs_from_boundaries / total_runs * 100) if total_runs > 0 else 0.0
            db_ins_cursor = None
            try:
                db_ins_cursor = self._execute_sql("""
                    INSERT INTO gold_batsman_performance_metrics (player_id, player_name, team_name, total_runs, total_balls_faced, boundary_dominance_ratio)
                    VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE player_name = VALUES(player_name), team_name = VALUES(team_name),
                    total_runs = VALUES(total_runs), total_balls_faced = VALUES(total_balls_faced), boundary_dominance_ratio = VALUES(boundary_dominance_ratio)
                """, (row.get('player_id'), row.get('player_name'), normalized_team_name, total_runs, row.get('total_balls_faced', 0), round(bdr, 2)))
                if db_ins_cursor: db_ins_cursor.close(); db_ins_cursor=None
                inserted_count +=1
            except Error as e: self.log.error(f"Failed BDR insert for {row.get('player_id')}: {e}")
            finally:
                if db_ins_cursor: db_ins_cursor.close()
        self.log.info(f"✅ (CustomStatsProcessor) Batsman performance metrics calculated. Inserted/Updated: {inserted_count}")

    def calculate_bowler_performance_metrics(self):
        self.log.info("ℹ️ (CustomStatsProcessor) Calculating bowler performance metrics (Effectiveness Ratio)...")
        db_cursor = None; column_names = []; results = []
        try:
            db_cursor = self._execute_sql("""
                SELECT bowler_id AS player_id,
                    SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT bowler_name ORDER BY LENGTH(bowler_name) DESC SEPARATOR '|'), '|', 1) AS player_name,
                    bowling_team AS team_name_raw, SUM(wickets) AS total_wickets, SUM(runs_given) AS total_runs_conceded
                FROM silver_bowling WHERE bowler_id IS NOT NULL AND bowling_team IS NOT NULL AND bowling_team <> 'Unknown'
                AND bowler_name IS NOT NULL AND bowler_name <> 'Unknown' GROUP BY bowler_id, bowling_team""")
            if db_cursor and db_cursor.description: column_names = [col[0] for col in db_cursor.description]
            results = db_cursor.fetchall() if db_cursor else []
        finally:
            if db_cursor: db_cursor.close()

        inserted_count = 0
        for row_tuple in results:
            if not column_names: continue
            row = dict(zip(column_names, row_tuple))
            normalized_team_name = self._normalize_team_name(row.get('team_name_raw'))
            if normalized_team_name == "Unknown": continue

            effectiveness_ratio = (row.get('total_wickets', 0) * 100) / (row.get('total_runs_conceded', 0) + 1)
            db_ins_cursor = None
            try:
                db_ins_cursor = self._execute_sql("""
                    INSERT INTO gold_bowler_performance_metrics (player_id, player_name, team_name, total_wickets, total_runs_conceded, effectiveness_ratio)
                    VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE player_name = VALUES(player_name), team_name = VALUES(team_name),
                    total_wickets = VALUES(total_wickets), total_runs_conceded = VALUES(total_runs_conceded), effectiveness_ratio = VALUES(effectiveness_ratio)
                """, (row.get('player_id'), row.get('player_name'), normalized_team_name, row.get('total_wickets', 0), row.get('total_runs_conceded', 0), round(effectiveness_ratio, 4)))
                if db_ins_cursor: db_ins_cursor.close(); db_ins_cursor=None
                inserted_count += 1
            except Error as e: self.log.error(f"Failed Bowler ER insert for {row.get('player_id')}: {e}")
            finally:
                if db_ins_cursor: db_ins_cursor.close()
        self.log.info(f"✅ (CustomStatsProcessor) Bowler performance metrics calculated. Inserted/Updated: {inserted_count}")

    def calculate_team_head_to_head(self):
        self.log.info("ℹ️ (CustomStatsProcessor) Calculating team head-to-head stats...")
        h2h_stats = defaultdict(lambda: {'team1_wins': 0, 'team2_wins': 0, 'ties_or_nr': 0, 'total_matches': 0})
        db_cursor = None; column_names = []; matches = []
        try:
            db_cursor = self._execute_sql("""
                SELECT team1_name, team2_name, match_winner, is_no_result, is_tie FROM silver_match_summary
                WHERE team1_name IS NOT NULL AND team1_name <> 'Unknown' AND team2_name IS NOT NULL AND team2_name <> 'Unknown'""")
            if db_cursor and db_cursor.description: column_names = [col[0] for col in db_cursor.description]
            matches = db_cursor.fetchall() if db_cursor else []
        finally:
            if db_cursor: db_cursor.close()

        processed_count = 0
        for row_tuple in matches:
            processed_count += 1
            if not column_names: continue
            row = dict(zip(column_names, row_tuple))

            t1_orig_norm = self._normalize_team_name(str(row['team1_name']))
            t2_orig_norm = self._normalize_team_name(str(row['team2_name']))
            match_winner_norm = self._normalize_team_name(str(row['match_winner'])) if row.get('match_winner') else "Unknown"

            if t1_orig_norm == "Unknown" or t2_orig_norm == "Unknown" or t1_orig_norm == t2_orig_norm : continue

            key_t1, key_t2 = sorted([t1_orig_norm, t2_orig_norm])
            key = (key_t1, key_t2)
            h2h_stats[key]['total_matches'] += 1

            is_nr = row.get('is_no_result', False)
            is_tie = row.get('is_tie', False)

            if is_nr or (is_tie and match_winner_norm == "Unknown"):
                h2h_stats[key]['ties_or_nr'] += 1
            elif match_winner_norm != "Unknown":
                if match_winner_norm == key_t1 : h2h_stats[key]['team1_wins'] +=1
                elif match_winner_norm == key_t2 : h2h_stats[key]['team2_wins'] +=1
                else:
                    self.log.info(f"⚠️ H2H Warning: Winner '{match_winner_norm}' doesn't match key teams '{key_t1}', '{key_t2}'. Match outcome ambiguous.")
                    h2h_stats[key]['ties_or_nr'] += 1
            else:
                 h2h_stats[key]['ties_or_nr'] += 1

        db_ins_cursor = None
        inserted_count = 0
        try:
            for (t1_key, t2_key), data in h2h_stats.items():
                decided = data['total_matches'] - data['ties_or_nr']
                t1_wp = (data['team1_wins'] / decided * 100) if decided > 0 else 0.0
                t2_wp = (data['team2_wins'] / decided * 100) if decided > 0 else 0.0
                db_ins_cursor = self._execute_sql("""
                    INSERT INTO gold_team_head_to_head_stats (team1_name, team2_name, team1_wins, team2_wins, ties_or_no_result, total_matches, team1_win_percentage, team2_win_percentage)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE team1_wins = VALUES(team1_wins), team2_wins = VALUES(team2_wins),
                    ties_or_no_result = VALUES(ties_or_no_result), total_matches = VALUES(total_matches),
                    team1_win_percentage = VALUES(team1_win_percentage), team2_win_percentage = VALUES(team2_win_percentage)
                """, (t1_key, t2_key, data['team1_wins'], data['team2_wins'], data['ties_or_nr'], data['total_matches'], round(t1_wp,2), round(t2_wp,2)))
                if db_ins_cursor: db_ins_cursor.close(); db_ins_cursor = None
                inserted_count +=1
        except Error as e: self.log.error(f"Failed H2H insert for {t1_key} vs {t2_key}: {e}")
        finally:
            if db_ins_cursor: db_ins_cursor.close()
        self.log.info(f"✅ (CustomStatsProcessor) Team head-to-head stats calculated. Processed {processed_count} matches, Inserted/Updated {inserted_count} H2H records.")

    def run_all_custom_stats(self):
        self.log.info("\n--- Custom Stats Processing Started ---")
        tables_to_truncate = [
            "gold_bowler_clean_bowled_stats",
            "gold_team_powerplay_stats", 
            "gold_batsman_performance_metrics",
            "gold_bowler_performance_metrics", 
            "gold_team_head_to_head_stats",
            "gold_fielder_catch_stats",
            "gold_latest_match_summary"
        ]

        self.log.info("Creating/Verifying GOLD tables...")
        self.create_custom_gold_tables()

        self.log.info("\nClearing old custom GOLD data...")
        truncated_count = 0
        for table in tables_to_truncate:
            db_trunc_cursor = None
            try:
                self.log.info(f"Truncating {table}...")
                db_trunc_cursor = self._execute_sql(f"TRUNCATE TABLE {table}")
                truncated_count += 1
            except Error as e:
                if e.errno == 1146: 
                    self.log.error(f"⚠️ Table {table} does not exist, skipping truncate.")
                else: 
                    self.log.error(f"❌ SQL Error during truncate of {table}: {e}")
            finally:
                if db_trunc_cursor: 
                    db_trunc_cursor.close()
        self.log.info(f"Truncated {truncated_count} tables.")

        self.log.info("\nCalculating custom GOLD stats...")
        stats_methods = [
            self.calculate_bowler_clean_bowled_stats,
            self.calculate_team_avg_powerplay_score,
            self.calculate_batsman_performance_metrics,
            self.calculate_bowler_performance_metrics,
            self.calculate_team_head_to_head,
            self.calculate_fielder_catches,
            self.update_latest_match_summary
        ]

        for method in stats_methods:
            try:
                method()
            except Exception as e:
                self.log.error(f"❌ Error in {method.__name__}: {str(e)}")

        self.log.info("\n--- Custom GOLD stats calculation complete ---")
