from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import mysql.connector
import time

chrome_options = Options()
chrome_options.add_argument("--headless")  

service = Service("./chromedriver.exe")

driver = webdriver.Chrome(service=service, options=chrome_options)


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

""" 
! CRAWL RELEASE DATA
"""

import requests
import traceback
import os


GITHUB_TOKEN = os.getenv("TOKEN1")

HEADERS = {
    "Accept": "application/vnd.github.v3+json",
    "Authorization": f"token {GITHUB_TOKEN}"
}

insert_release = """
INSERT IGNORE INTO `release` (id, version, content, repoID)
VALUES (%s, %s, %s, %s)
"""

query_select_all = "SELECT id, user, name FROM repo"
cursor.execute(query_select_all)

for repo_id, owner, name in cursor.fetchall():
    print(f"\n▶ Starting {owner}/{name} (repo_id={repo_id})")
    page = 1

    while True:
        url = f"https://api.github.com/repos/{owner}/{name}/releases"
        try:
            resp = requests.get(
                url,
                headers=HEADERS,
                params={"per_page": 100, "page": page},
                timeout=10
            )
        except requests.RequestException as e:
            print(f"   [Network Error] {e}")
            traceback.print_exc()
            break

        print(f"   → Rate‑limit remaining: {resp.headers.get('x-ratelimit-remaining','?')}")
        if resp.status_code != 200:
            print(f"   [HTTP {resp.status_code}] skipping remaining pages")
            break

        releases = resp.json()
        if not releases:
            print("   [Done] no more releases here.")
            break

        success, failed = 0, 0
        for rel in releases:
            rel_id   = rel.get("id")
            tag      = rel.get("tag_name", "")
            body     = rel.get("body") or ""        # never None
            try:
                cursor.execute(insert_release, (rel_id, tag, body, repo_id))
                success += cursor.rowcount
            except mysql.connector.Error as sql_err:
                failed += 1
                print(f"   [SQL Error] repo={owner}/{name} rel_id={rel_id}: {sql_err}")
                # show first 200 chars so you can inspect quoting issues
                snippet = repr(body[:200])
                print("      body snippet:", snippet)
                traceback.print_exc()
                # skip this release
                
        # commit per‐page
        try:
            connection.commit()
        except mysql.connector.Error as commit_err:
            print(f"   [Commit Error] page {page}: {commit_err}")
            traceback.print_exc()

        print(f"   [Page {page}] inserted={success}, skipped={failed}")
        time.sleep(3)
        page += 1

cursor.close()
connection.close()
print("\n✅ All repos processed.")






