from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import mysql.connector
import time
import requests
import traceback
import os

# --- SETUP CHROME DRIVER ---
chrome_options = Options()
chrome_options.add_argument("--headless")
service = Service("./chromedriver.exe")
driver = webdriver.Chrome(service=service, options=chrome_options)

# --- DB CONNECTION ---
connection = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="saber1108",
    database="crawl_data",
    use_unicode=True,
    autocommit=False
)
cursor = connection.cursor()

# --- TOKENS CONFIG ---
TOKENS = [
    os.getenv("TOKEN1"),
    os.getenv("TOKEN2"),
    os.getenv("TOKEN3"),
    # Add thêm nếu có
]

current_token_index = 0
request_counter = 0
MAX_REQUESTS_PER_TOKEN = 5000

HEADERS = {
    "Accept": "application/vnd.github.v3+json",
    "Authorization": f"token {TOKENS[current_token_index]}"
}

def switch_token():
    global current_token_index, HEADERS
    current_token_index += 1
    if current_token_index >= len(TOKENS):
        print("⏰ Hết lượt request cho tất cả token. Ngủ 3 tiếng (10,800 giây)...")
        time.sleep(10800)  # 3 tiếng
        current_token_index = 0
    new_token = TOKENS[current_token_index]
    HEADERS["Authorization"] = f"token {new_token}"
    print(f"⚠️ Đổi sang token thứ {current_token_index + 1}")

def make_request(url, page):
    global request_counter
    if request_counter >= MAX_REQUESTS_PER_TOKEN:
        switch_token()
        request_counter = 0

    try:
        resp = requests.get(
            url,
            headers=HEADERS,
            params={"per_page": 100, "page": page},
            timeout=10
        )
        request_counter += 1
        return resp
    except requests.RequestException as e:
        print(f"   [Network Error] {e}")
        traceback.print_exc()
        return None

# --- SQL QUERIES ---
insert_release = """
INSERT IGNORE INTO `release` (id, version, content, repoID)
VALUES (%s, %s, %s, %s)
"""
query_select_all = "SELECT id, user, name FROM repo"

# --- CRAWL RELEASE DATA ---
cursor.execute(query_select_all)
for repo_id, owner, name in cursor.fetchall():
    print(f"\n▶ Starting {owner}/{name} (repo_id={repo_id})")
    page = 1

    while True:
        url = f"https://api.github.com/repos/{owner}/{name}/releases"
        resp = make_request(url, page)
        if not resp:
            break

        print(f"   → Rate‑limit remaining: {resp.headers.get('x-ratelimit-remaining', '?')}")
        if resp.status_code != 200:
            print(f"   [HTTP {resp.status_code}] skipping remaining pages")
            break

        releases = resp.json()
        if not releases:
            print("   [Done] no more releases here.")
            break

        success, failed = 0, 0
        for rel in releases:
            rel_id = rel.get("id")
            tag = rel.get("tag_name", "")
            body = rel.get("body") or ""
            try:
                cursor.execute(insert_release, (rel_id, tag, body, repo_id))
                success += cursor.rowcount
            except mysql.connector.Error as sql_err:
                failed += 1
                print(f"   [SQL Error] repo={owner}/{name} rel_id={rel_id}: {sql_err}")
                snippet = repr(body[:200])
                print("      body snippet:", snippet)
                traceback.print_exc()

        try:
            connection.commit()
        except mysql.connector.Error as commit_err:
            print(f"   [Commit Error] page {page}: {commit_err}")
            traceback.print_exc()

        print(f"   [Page {page}] inserted={success}, skipped={failed}")
        time.sleep(3)
        page += 1

# --- CLEANUP ---
cursor.close()
connection.close()
driver.quit()
print("\n✅ All repos processed.")
