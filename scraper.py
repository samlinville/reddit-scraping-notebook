from os.path import exists
# from IPython.display import clear_output
import requests
from datetime import datetime
import traceback
import time
import json
import sys
import os
import pandas as pd
import shutil

posts = pd.read_csv('posts-i-imgur.csv', dtype={'Title': 'str',
                                        'ID': 'str',
                                        'Date': 'str',
                                        'Upvotes': 'int',
                                        'URL': 'str',
                                        'Status': 'str'
                                       })
# # posts = posts.iloc[: , 1:]
# posts = posts.sort_values(by=['Upvotes'], ascending=False)
# header_list = ["Title", "ID", "Date", "Upvotes", "URL", "Status"]
# posts = posts.reindex(columns = header_list)
# posts = posts[posts['URL'].str.contains("//i.imgur.com/")]
# posts = posts.reset_index(drop=True)
# posts.to_csv("posts-i-imgur.csv")

current_directory = os.getcwd()
new_dir = "pics-test" # -" + datetime.now().strftime("%m%d%Y%H%M%S")
final_directory = os.path.join(current_directory, new_dir)
if not os.path.exists(final_directory):
   os.makedirs(final_directory)

processed = 0
skipped = 0
failed = 0
success = 0
exceptions = 0

for index, row in posts.iterrows():
    url =  row['URL']
    filename = row['ID']
    
    if row['Status'] == "Failed":
        failed += 1
        processed += 1
        continue
        
    if row['Status'] == "BadURL":
        skipped += 1
        processed += 1
        continue
        
    if row['Status'] == "Downloaded":
        success += 1
        processed += 1
        continue
        
    if exists(final_directory + '/' + filename + '.jpg'):
        success += 1
        processed += 1
        continue
        
    matches = ["://i.redd.it", "://i.imgur.com"]
    if not any(x in url for x in matches):
        skipped += 1
        processed += 1  
        posts.at[index,'Status'] = "BadURL"
        continue
    try:
        res = requests.get(url, stream = True, timeout=20)
    except:
        skipped += 1
        processed += 1
        exceptions += 1
        posts.at[index,'Status'] = "Exception"
        continue
    if res.status_code == 200:
        with open(final_directory + '/' + filename + '.jpg','wb') as f:
            shutil.copyfileobj(res.raw, f)
        success += 1
        posts.at[index,'Status'] = "Downloaded"
        
    else:
        failed += 1
        posts.at[index,'Status'] = "Failed"
        
        
    processed += 1
    # clear_output(wait=True)
    total = 95620
    percentage = (processed / total) * 100
    
    # clear_output(wait=True)
    # print(str(processed) + " / " + str(total) + " processed, " + str(percentage) + "%")
    # print(str(success) + " successful" + " (" + str((success/total)*100) + "%)")
    # print(str(failed) + " failed" + " (" + str((failed/total)*100) + "%)")
    # print(str(skipped) + " skipped" + " (" + str((skipped/total)*100) + "%)")
    # print("Exceptions: " + str(exceptions))
    # print("Index: " + str(index))
    
    time.sleep(1)
    if (index % 100 == 0):
        posts.to_csv("posts-i-imgur.csv")
