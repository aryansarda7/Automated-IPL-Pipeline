import requests

SUPERSET_URL = "   "
USERNAME = "   "
PASSWORD = "   "
CHART_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]  # Your chart IDs

def refresh_superset_charts():
    # 1. Get access token
    res = requests.post(f"{SUPERSET_URL}/api/v1/security/login", json={
        "username": USERNAME,
        "password": PASSWORD,
        "provider": "db"
    })
    token = res.json().get("access_token")
    headers = {"Authorization": f"Bearer {token}"}

    # 2. Hit each chart endpoint to refresh it
    for chart_id in CHART_IDS:
        url = f"{SUPERSET_URL}/api/v1/chart/{chart_id}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print(f"✅ Refreshed chart ID {chart_id}")
        else:
            print(f"❌ Failed to refresh chart ID {chart_id}: {response.status_code}")
