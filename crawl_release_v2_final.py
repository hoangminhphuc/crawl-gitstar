import requests
import mysql.connector
import time
from concurrent.futures import ThreadPoolExecutor
import os
from dotenv import load_dotenv
load_dotenv()

class TokenManager:
    def __init__(self, tokens):
        """
        Khởi tạo TokenManager với danh sách các token GitHub.
        :param tokens: Danh sách các token.
        """
        self.tokens = tokens
        self.current_token_index = 0
        self.token_usage = {token: {"count": 0, "reset_time": 0} for token in tokens}  # Lưu trữ số lượng yêu cầu và thời gian reset

    def get_token(self):
        """
        Lấy token theo vòng quay.
        :return: token GitHub.
        """
        token = self.tokens[self.current_token_index]
        self.current_token_index = (self.current_token_index + 1) % len(self.tokens)
        return token

    def check_rate_limit(self):
        """
        Kiểm tra rate limit của tất cả các token.
        :return: True nếu tất cả các token đã chạm rate limit, False nếu không.
        """
        for token in self.tokens:
            headers = {"Authorization": f"token {token}"}
            url = "https://api.github.com/rate_limit"
            try:
                resp = requests.get(url, headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    remaining = data.get("resources", {}).get("core", {}).get("remaining", 0)
                    reset_time = data.get("resources", {}).get("core", {}).get("reset", 0)
                    self.token_usage[token] = {"count": 5000 - remaining, "reset_time": reset_time}
                    if remaining > 0:
                        return False  # Nếu còn token nào chưa hết rate limit
                else:
                    print(f"✘ Error checking rate limit for token: {token}")
            except requests.RequestException as e:
                print(f"✘ Error while checking rate limit: {e}")
        return True  # Nếu tất cả các token đều đã hết rate limit

    def wait_for_reset(self):
        """
        Chờ cho đến khi tất cả các token reset lại rate limit.
        """
        # Tính thời gian còn lại cho tất cả các token
        reset_times = [self.token_usage[token]["reset_time"] for token in self.tokens]
        sleep_time = max(reset_times) - time.time()
        if sleep_time > 0:
            print(f"All tokens hit rate limit. Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)  # Chờ cho đến khi tất cả token được reset

# Kết nối DB
# connection = mysql.connector.connect(
#     host="localhost",
#     port=3306,
#     user="root",  
#     password="saber1108",  
#     database="crawl_data",
#     use_unicode=True,
#     autocommit=True
# )
# cursor = connection.cursor()

# SQL query để insert release và commit vào DB
insert_release_query = """
INSERT IGNORE INTO `release` (id, version, content, repoID)
VALUES (%s, %s, %s, %s)
"""

insert_commit_query = """
INSERT IGNORE INTO `commit` (hash, message, releaseID)
VALUES (%s, %s, %s)
"""
# insert_commit_query = """
# INSERT IGNORE INTO commit (sha, commit_message, release_id)
# VALUES (%s, %s, %s)
# """
from mysql.connector import pooling

dbconfig = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "saber1108",
    "database": "crawl_data",
    "autocommit": True,
    "use_unicode": True,
}

# create a pool of 10 connections
cnxpool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=30, **dbconfig)

def save_commit_to_db(cursor, sha, commit_message, release_id):
    try:
        cursor.execute(insert_commit_query, (sha, commit_message, release_id))
    except mysql.connector.Error as e:
        print(f"✘ Error saving commit {sha}: {e}")

def crawl_commit(owner, repo, release_tag, release_id, page=1):
    """
    Crawl commit data của release (dựa vào SHA hoặc tag).
    """

    # 1. Borrow one connection+cursor for this entire thread
    conn   = cnxpool.get_connection()
    cursor = conn.cursor()
    try :
        url = f"https://api.github.com/repos/{owner}/{repo}/commits"
        token = token_manager.get_token()
        headers = {"Authorization": f"token {token}"}
        
        # params = {"sha": release_tag, "page": page, "per_page": 100}
        
        while True:
            try:
                params = {"sha": release_tag, "page": page, "per_page": 100}
                # Kiểm tra rate limit trước khi gửi yêu cầu
                if token_manager.check_rate_limit():
                    token_manager.wait_for_reset()  # Nếu tất cả token chạm rate limit, chờ reset

                resp = requests.get(url, headers=headers, params=params)
                if resp.status_code == 200:
                    commits = resp.json()
                    if commits:
                        print(f"✔ Crawled {len(commits)} commits for release {release_tag} (page {page})")
                        # Insert commit data vào DB
                        for commit in commits:
                            sha_commit = commit['sha']
                            commit_message = commit['commit']['message']
                            # time.sleep(0.5)
                            save_commit_to_db(cursor, sha_commit, commit_message, release_id)
                        # Nếu còn nhiều commit, crawl tiếp
                        page += 1

                        if len(commits) < 100:
                            print("Reached final commits page.")
                            break
                    else:
                        print(f"No more commits for {release_tag} (page {page})")
                        break
                elif resp.status_code == 403:  # Nếu rate limit chạm
                    print(f"Rate limit reached for token: {token}. Sleeping for 1 hour...")
                    time.sleep(3600)  # Chờ 1 giờ
                else:
                    print(f"✘ Failed to crawl commit for {release_tag} | {resp.status_code}")
                    break
            except requests.RequestException as e:
                print(f"✘ Error while fetching commits: {e}")
                break

    finally:
        cursor.close()
        conn.close()

def save_release_to_db(cursor, release_id, release_tag, body, repo_id):
    """
    Lưu release vào DB.
    """
    try:
        cursor.execute(insert_release_query, (release_id, release_tag, body, repo_id))
        print(f"✔ Saved release {release_tag}")
        time.sleep(0.5) 
    except mysql.connector.Error as e:
        print(f"✘ Error saving release {release_tag}: {e}")

def crawl_release(owner, repo, repo_id):
    """
    Crawl tất cả release của 1 repo.
    """
    conn = cnxpool.get_connection()
    cursor = conn.cursor()
    try :
        print(f"▶ Crawling releases for {owner}/{repo}")
        url = f"https://api.github.com/repos/{owner}/{repo}/releases?per_page=100"
        token = token_manager.get_token()
        headers = {"Authorization": f"token {token}"}
        
        page = 1
        while True:
            params = {"page": page, "per_page": 100} # maximum for per_page is 100 in api github
            try:
                # Kiểm tra rate limit trước khi gửi yêu cầu
                if token_manager.check_rate_limit():
                    token_manager.wait_for_reset()  # Nếu tất cả token chạm rate limit, chờ reset

                resp = requests.get(url, headers=headers, params=params)
                if resp.status_code != 200:
                    print(f"✘ Failed to fetch releases for {owner}/{repo}")
                    break

                releases = resp.json()
                if not releases:
                    print(f"✘ No more releases for {owner}/{repo}")
                    break

                # Gọi multi-thread crawl commit cho từng release
                with ThreadPoolExecutor(max_workers=5) as commit_executor:
                    for release in releases:
                        release_tag = release.get("tag_name")
                        # sha = release.get("target_commitish")  # Thường là SHA hoặc branch
                        release_id = release.get("id")
                        body = release.get("body") or ""
                        
                        # Lưu release vào DB
                        save_release_to_db(cursor, release_id, release_tag, body, repo_id)

                        # Gọi crawl commit cho release này
                        commit_executor.submit(crawl_commit, owner, repo, release_tag, release_id)

                print(f"   ▶ Crawled page {page} of releases for {owner}/{repo}")
                time.sleep(1)
                page += 1
            except requests.RequestException as e:
                print(f"✘ Error while fetching releases: {e}")
                break

    finally:
        cursor.close()
        conn.close()

def crawl_repo():
    """
    Crawl tất cả các repo trong DB (hoặc từ danh sách repo đã có).
    """
    conn = cnxpool.get_connection()
    cursor = conn.cursor()


    query_select_all = "SELECT user, name, id FROM repo where id = 9"
    cursor.execute(query_select_all)
    
    repos = cursor.fetchall()  # Giả sử bạn đã có bảng 'repos' chứa thông tin repo cần crawl
    print(f"Found {len(repos)} repos to crawl.")
    cursor.close()
    conn.close()

    futures = []
    with ThreadPoolExecutor(max_workers=5) as repo_executor:
        for user, name, id in repos:
            # repo_executor.submit(crawl_release, user, name, id)
            future = repo_executor.submit(crawl_release, user, name, id)
            futures.append(future)

        for future in futures:
            future.result()
    print("✅ All repos processed.")

def github_token_manager():
    GITHUB_TOKENS = []

    i = 1
    while True:
        token = os.getenv(f"GITHUB_TOKEN_{i}")
        print(f"Token {i}: {token}")
        if not token:
            break
        GITHUB_TOKENS.append(token)
        i += 1
    
    return GITHUB_TOKENS

if __name__ == "__main__":
    GITHUB_TOKENS = github_token_manager() # List of tokens
    token_manager = TokenManager(GITHUB_TOKENS)
    crawl_repo()




