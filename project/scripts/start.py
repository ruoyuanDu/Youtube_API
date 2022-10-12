from search import search
from video import get_video
from comments import get_comments
import yb
from pprint import pprint
from db.dataframe import to_dataframe
import pandas as pd


if __name__ == '__main__':
    api = 'AIzaSyA6caAj-Jpjr2G59mB83jrz46NDfhMqcQk'
    youtube = yb.get_youtube(api_key=api)
    Search_df = pd.DataFrame()

    next_page = None
    # while True:
    for i in range(5):
        #extract search data
        next_page, search_results = search(youtube_server=youtube, page_token=next_page)
        search_df = to_dataframe(search_results, from_search=True)
        Search_df = pd.concat([Search_df, search_df])
        # if not next_page:
        #     break
    Search_df.reset_index(inplace=True, drop=True)
    print(Search_df)

    # #extract video data
    # next_page = None
    # Combined_video_df = pd.DataFrame()
    # for id in Search_df['videoId']:
    #     next_page, video_results = get_video(youtube_server=youtube, channel_id=id, page_token=next_page)
    #     video_df = to_dataframe(video_results, from_video=True)
    #     Combined_video_df = pd.concat([Combined_video_df, video_df])
    # print(Combined_video_df)

    #extract comments
    next_page = None