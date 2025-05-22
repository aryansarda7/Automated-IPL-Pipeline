import http.client
import json
import boto3
from datetime import datetime

# -------- Your API and AWS Settings --------
RAPIDAPI_HOST = "  "
RAPIDAPI_KEY = "  "
SERIES_ID = "  "  # IPL 2025 series ID (based on uploaded schedule)

AWS_ACCESS_KEY = '  '
AWS_SECRET_KEY = '  '
AWS_REGION = '  '
BUCKET_NAME = '  '

# -------- Boto3 S3 Client --------
s3 = boto3.client('s3',
                  aws_access_key_id=AWS_ACCESS_KEY,
                  aws_secret_access_key=AWS_SECRET_KEY,
                  region_name=AWS_REGION)

# -------- Helper Functions --------
def fetch_series_matches():
    conn = http.client.HTTPSConnection(RAPIDAPI_HOST)
    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': RAPIDAPI_HOST
    }

    endpoint = f"/series/v1/{SERIES_ID}"
    conn.request("GET", endpoint, headers=headers)
    res = conn.getresponse()
    data = res.read()

    try:
        parsed = json.loads(data)
        return parsed.get("matchDetails", [])
    except Exception as e:
        print(f"❌ Error parsing series match list: {e}")
        return []

def get_completed_match_ids(match_details_list):
    match_ids = []
    for group in match_details_list:
        match_map = group.get("matchDetailsMap")
        if not match_map:
            continue  # Skip ads or malformed
        for match in match_map.get("match", []):
            match_info = match.get("matchInfo", {})
            if match_info.get("state", "") == "Complete":
                team1 = match_info.get("team1", {}).get("teamName", "Team1")
                team2 = match_info.get("team2", {}).get("teamName", "Team2")
                match_ids.append((match_info.get("matchId"), team1, team2))
    return match_ids

def load_processed_matches():
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key="processed_matches.json")
        processed = json.loads(obj['Body'].read())
        return processed
    except Exception:
        print("🔵 No processed_matches.json found, creating new.")
        return []

def save_processed_matches(processed_ids):
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key="processed_matches.json",
        Body=json.dumps(processed_ids, indent=4),
        ContentType='application/json'
    )

# -------- Main Script --------

def get_ipl_matches():
    print("\n🔵 Fetching list of completed matches from IPL series...")
    match_details = fetch_series_matches()

    if not match_details:
        print("⚠️ No matches found, exiting.")
        return

    completed_matches = get_completed_match_ids(match_details)
    print(f"✅ Found {len(completed_matches)} completed matches.")

    processed_matches = load_processed_matches()
    new_matches = [match for match in completed_matches if str(match[0]) not in processed_matches]

    print(f"🟡 {len(new_matches)} new matches to process.")

    for match_id, team1, team2 in new_matches:
        try:
            match_folder = f"{match_id}_{team1.replace(' ', '')}_vs_{team2.replace(' ', '')}"

            # Fetch Scorecard
            conn = http.client.HTTPSConnection(RAPIDAPI_HOST)
            headers = {'x-rapidapi-key': RAPIDAPI_KEY, 'x-rapidapi-host': RAPIDAPI_HOST}
            conn.request("GET", f"/mcenter/v1/{match_id}/scard", headers=headers)
            res = conn.getresponse()
            scard_data = json.loads(res.read().decode("utf-8"))

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=f"{match_folder}/{match_folder}_scard.json",
                Body=json.dumps(scard_data, indent=4),
                ContentType='application/json'
            )
            print(f"✅ Uploaded {match_folder}_scard.json to S3")

            # Fetch Commentary
            conn = http.client.HTTPSConnection(RAPIDAPI_HOST)
            conn.request("GET", f"/mcenter/v1/{match_id}/comm", headers=headers)
            res = conn.getresponse()
            comm_data = json.loads(res.read().decode("utf-8"))

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=f"{match_folder}/{match_folder}_comm.json",
                Body=json.dumps(comm_data, indent=4),
                ContentType='application/json'
            )
            print(f"✅ Uploaded {match_folder}_comm.json to S3")

            # Update processed list
            processed_matches.append(str(match_id))

        except Exception as e:
            print(f"❌ Error processing match {match_id}: {e}")

    save_processed_matches(processed_matches)
    print("\n🎯 All new matches processed and uploaded!")

if __name__ == "__main__":
    get_ipl_matches()
