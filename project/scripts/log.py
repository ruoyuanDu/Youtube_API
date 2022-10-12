# -*-coding:utf-8-*-
from datetime import datetime, date
import time
import os

def log(msg):
    # today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    today = date.today().strftime('%Y-%m-%d')
    # path = os.getcwd()
    log_file_path = os.path.join('../log/', today)
    current_time = time.strftime('%Y-%m-%d %X')
    with open(log_file_path, 'a', encoding='utf-8') as f:
        f.write(current_time + '-'*3 + msg +'\n')
# log('Total results of %i, recorded to page: %i, recorded results: %i, left results %i, last video_id in this page %s, last channel_id %s'%(100, 2, 24, 100-24, 'lkwjgk123', 'sadgasd'))