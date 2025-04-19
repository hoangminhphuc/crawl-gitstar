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
    password="1234",  
    database="crawl_data"  
)

cursor = connection.cursor()

# query_check = "SELECT COUNT(*) FROM repo WHERE user = %s AND name = %s"
# query_insert = "INSERT INTO repo (user, name) VALUES (%s, %s)"


# # Loop through the first 50 pages of the GitStar ranking
# for page_number in range(1, 51): 
#     url = f"https://gitstar-ranking.com/repositories?page={page_number}"
#     driver.get(url)
#     time.sleep(3)  
#     owner_link = driver.find_elements(By.CSS_SELECTOR, 'a.list-group-item.paginated_item')
#     for on in owner_link:
#         href = on.get_attribute('href')
#         parts = href.split('/')
#         owner = parts[3]  
#         name = parts[4]  
#         # repo_data.append({"owner": owner, "name": name})
#         cursor.execute(query_check, (owner, name))
#         result = cursor.fetchone()
#         if result[0] == 0:
#             cursor.execute(query_insert, (owner, name))
#         else:
#             print(f"Data already exists for {owner}/{name}")
#     connection.commit()

# driver.quit()
# print("Data insertion completed.")


repo_data = []
query_select_all = "SELECT user, name FROM repo"
cursor.execute(query_select_all)
rows = cursor.fetchall()
for row in rows:
    repo_data.append({"owner": row[0], "name": row[1]})



cursor.close()
connection.close()







