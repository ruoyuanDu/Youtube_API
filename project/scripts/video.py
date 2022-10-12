import pandas as pd
import os
from dataframe import to_dataframe
from log import log
import yb


def get_video(youtube_server=None, video_id=None, page_token=None):
    """
    Get data for specific video from Youtube API
    :param youtube_server: Google client API connection object from yb.get_youtube
    :param video_id: str, id for a specific video with default to None
    :param page_token: str, The page token for current search with default to None
    :return:
            next_page: str, Page token for next page if the search returns multiple pages results
            response: dicts, Search results in json format
    """

    request = youtube_server.videos().list(
        part="snippet, statistics",
        id=video_id,
        pageToken=page_token
    )
    try:
        response = request.execute()
        next_page = response.get('nextPageToken')
        return next_page, response
    except:
        print('connection failed...')
        print(response)
        log('connection failed')
        return False, False


if __name__ == '__main__':
    api = 'AIzaSyA6caAj-Jpjr2G59mB83jrz46NDfhMqcQk'
    # use yb.py method get_youtube to set up the connection to youtube API
    youtube = yb.get_youtube(api_key=api)

    # extract video data
    next_page = None
    par_dir = os.path.dirname(os.getcwd()) # get parent dir of current path
    # read in a parquet file which contains all videos ids needed
    video_search_df = pd.read_parquet(par_dir + '/db/search_data/2022-01_2022-10.parquet')
    video_ids = video_search_df.loc[:, 'videoId'].to_list() # list of video ids
    n_videos = len(video_ids) # total number of videos
    print('there are total of %i number of video ids to be recorded' % (n_videos))
    video_batch = [video_ids[i:i+101] for i in range(0, n_videos, 101)]
    i = 0
    for batch_num, batch in enumerate(video_batch):
        Combined_video_df = pd.DataFrame()
        for video in batch:
            next_page, video_results = get_video(youtube_server=youtube, video_id=video, page_token=next_page)
            if not video_results: # break while loop if the connection fails
                log('video %i %s is not recorded due to error' % (i, video_ids[i]))
                break
            # convert jason result from one video to dataframe and keep only specified columns
            video_df = to_dataframe(video_results, from_video=True,
                                    cols=['channelId', 'videoId', 'categoryId', 'title', 'publishedAt'])
            # concat dataframes of all videos
            Combined_video_df = pd.concat([Combined_video_df, video_df], ignore_index=True)
            # write in log info
            log('Recorded %ith videos, video id is %s' % (i, video_ids[i]))
            i += 1
        Combined_video_df.reset_index(inplace=True, drop=True)
        # set the destination dir path for to save the dataframe
        dest_dir = os.path.abspath(os.path.join(os.pardir, 'db/video_data'))
        if not os.path.exists(dest_dir): # make the dir named video_data if not existed
            os.makedirs(dest_dir)
        # join destination dir path with file name
        file_name = '2022-01_2022-10.batch%s'%(batch_num)
        path = os.path.join(dest_dir, file_name)
        # save dataframe to parquet file
        Combined_video_df.to_parquet(path + '.parquet', compression='gzip')

