import os
from pprint import pprint
import yb
from dataframe import to_dataframe
import pandas as pd
from log import log
import time
import googleapiclient.errors


def search(youtube_server=None, page_token=None,
           published_after_date=None, published_before_date=None,
           max_results=50, channel_id=None, language='en', order=None, search_term=None):
    """
    Get results of Search resource from Youtube API

    :param youtube_server: Google client API connection object from yb.get_youtube
    :param page_token: str, The page token for current search with default to None
    :param published_after_date: date with format "%Y-%m-%d", Search videos published after this date, formate , default is None
    :param published_before_date: date with format "%Y-%m-%d", Gearch videos published before this date, formate "%Y-%m-%d", default is None
    :param max_results: int, Number of results returned per search, maximum is 50, default is 50
    :param channel_id: str, ChannelId to which an video belongs to, default is None
    :param language: str, ISO 639-1 two-letter language code, default is 'en'
    :return:
            next_page: str, Page token for next page if the search returns multiple pages results
            response: dicts, Search results in json format
    """
    # if not published_before_date:
    #     print('please put in the published_before_date with format YYYY-mm-dd')
    # if published_before_date:
    request = youtube_server.search().list(
        part='snippet',
        publishedAfter=published_after_date,
        publishedBefore=published_before_date,
        maxResults=max_results,
        channelId=channel_id,
        type='video',
        safeSearch='strict',
        pageToken=page_token,
        relevanceLanguage=language,
        order=order,
        q=search_term,
        fields="nextPageToken,pageInfo,regionCode,items(id(videoId),snippet(publishedAt,channelId,channelTitle,title,description))"
    )
    try:
        response = request.execute()  # execute the connection, return json format results
        return response
    # return False if connection error
    except googleapiclient.errors.HttpError as e:
        print(e)
        log(str(e))
        return False


def main_daily(published_year, published_month, search_term, order, d1, d2):
    api = 'AIzaSyA6caAj-Jpjr2G59mB83jrz46NDfhMqcQk'
    # use yb.py method get_youtube to set up the connection to youtube API
    youtube = yb.get_youtube(api_key=api)

    Search_df = pd.DataFrame()
    if not os.path.exists('../db/search_data'):
        os.mkdir('../db/search_data')
    # #create a folder to store data
    # loop through the total number of days in the desired month
    for a in range(d1, d2):
        if a <= 30:
            publishedAfterDate = str(published_year) + '-' + str(published_month).zfill(2) + '-' + str(a).zfill(2)+'T00:00:00Z'
            publishedBeforeDate = str(published_year) + '-' + str(published_month).zfill(2) + '-' + str(a+1).zfill(2)+'T00:00:00Z'
        else:
            publishedAfterDate = str(published_year) + '-' + str(published_month).zfill(2) + '-' + str(a).zfill(2) + 'T00:00:00Z'
            publishedBeforeDate = str(published_year) + '-' + str(published_month+1).zfill(2) + '-' + str(1).zfill(2) + 'T00:00:00Z'
        print('start search on date %s' % (publishedAfterDate))
        log('start search on date %s' % (publishedAfterDate))
        print('search before date %s' % (publishedBeforeDate))
        # set the number of results returned per search
        max_res = 50
        next_page = None
        pages = 0
        while True:
            search_results = search(youtube_server=youtube,
                                    page_token=next_page,
                                    max_results=max_res,
                                    published_after_date=publishedAfterDate,
                                    published_before_date=publishedBeforeDate,
                                    search_term=search_term,
                                    order=order
                                    )
            next_page = search_results.get('nextPageToken')  # page token for the next page
            # previous_page_tok = search_results.get('prevPageToken')
            if not search_results:
                break
            # use dataframe.py method to_dataframe to put extracted json data into a pandas DataFrame
            search_df = to_dataframe(input_file=search_results, from_search=True)
            Search_df = pd.concat([Search_df, search_df])
            # write log file
            total_search_results = search_results['pageInfo']['totalResults']
            total_res_items = len(search_results['items'])
            log('total search results %i, actual returned results %i' % (total_search_results, total_res_items))
            time.sleep(3)
            if not next_page:
                log('next_page is None, moved on to next date')
                break
        # continue
        Search_df.reset_index(inplace=True, drop=True)
        # cur_dir = os.getcwd()
        path = os.path.join('../db', 'search_data/daily', publishedAfterDate[:10])
        # save dataframe to parquet file
        Search_df.to_parquet(path + '.parquet', compression='gzip')
        print('save file %s.parquet' % (path))
        log('write dataframe to parquet file name %s' % (path + '.parquet'))


def main_token(published_after_Date, published_before_Date, search_term, order, day1, day2):
    api = 'AIzaSyA6caAj-Jpjr2G59mB83jrz46NDfhMqcQk'
    # use yb.py method get_youtube to set up the connection to youtube API
    youtube = yb.get_youtube(api_key=api)
    Search_df = pd.DataFrame()  # store results from multiple pages and days
    if not os.path.exists('../db/search_data'):
        os.mkdir('../db/search_data')
    # #create a folder to store data
    # loop through the total number of days in the desired month
    for a in range(day_in, day_out):
        print('start search on date %s' % (published_after_Date))
        log('start search on date %s' % (published_before_Date))
        #
        # set the number of results returned per search
        max_res = 50
        next_page_list = [None, 'CDIQAA', 'CGQQAA', 'CJYBEAA', 'CMgBEAA', 'CPoBEAA']
        # next_page_list = ['CKwCEAA', 'CN4CEAA', 'CJADEAA', 'CMIDEAA']
        # next_page_list = ['CPQDEAA', 'CKYEEAA', 'CMIDEAA']
        # next_page_list = ['CPQDEAA', 'CKYEEAA']
        # pre_page = [None, 'CDIQAQ', 'CGQQAQ', 'CJYBEAE', 'CMgBEAE', 'CPoBEAE', 'CKwCEAE', 'CN4CEAE', 'CJADEAE',
        #             'CMIDEAE', 'CPQDEAE', 'CPQDEAE']
        # page_list = []
        # for i in range(len(next_page)):
        #     page_list.append(pre_page[i])
        #     page_list.append(next_page[i])
        pages = 0
        for token in next_page_list:
            search_results = search(youtube_server=youtube,
                                    page_token=token,
                                    max_results=max_res,
                                    published_after_date=published_after_Date,
                                    published_before_date=published_before_Date,
                                    search_term=search_term,
                                    order=order
                                    )
            pprint(search_results)
            if not search_results:
                break
            print('current page token is:%s' % (token))
            log('current page token is:%s' % (token))
            total_search_results = search_results['pageInfo']['totalResults']
            total_res_items = len(search_results['items'])
            log('total search results %i, actual returned results %i' % (total_search_results, total_res_items))
            # cols_to_keep = ['channelId', 'videoId', 'channelTitle', 'title', 'publishedAt', 'regionCode', 'description']
            # convert json format data to pandas dataframe
            search_df = to_dataframe(input_file=search_results, from_search=True)
            Search_df = pd.concat([Search_df, search_df])  # concat search results
            time.sleep(2)
            # if not token:
            #     log('token used out!! moved on to next date')
            #     break

        Search_df.reset_index(inplace=True, drop=True)
        # cur_dir = os.getcwd()
        par_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        data_folder = os.path.join(par_dir, 'db', 'search_data', 'monthly_data')
        if not os.path.exists(data_folder):
            os.mkdir(data_folder)
        file_name = published_after_Date[:10]
        # save dataframe to parquet file
        Search_df.to_parquet(data_folder + '/' + file_name + '.token.nextonly.parquet', compression='gzip')
        print('save file %s.parquet' % (data_folder + file_name))
        log('write dataframe to parquet file name %s' % (data_folder + file_name + '.token.nextonly.parquet'))


if __name__ == '__main__':
    #monthly data extraction
    day_in = 1
    day_out = 2
    date = '2022-11-%sT00:00:00Z' % (str(day_in).zfill(2))
    date_before = '2022-12-%sT00:00:00Z' % (str(day_in).zfill(2))
    order = 'date'
    search_term = 'review'
    main_token(published_after_Date=date, published_before_Date=date_before, order=order, search_term=search_term,
               day1=day_in, day2=day_out)

    # #daily data extraction
    # day_in = 1
    # day_out = 31
    # order = 'date'
    # search_term = 'review'
    # main_daily(published_year=2022, published_month=4, search_term=search_term, order=order, d1=day_in, d2=day_out)

# try daily search data for March and concat them and compare to the no of results from March where I extracted an entire month

# data extracted by daily have almost similar number of data extracted by month with pre-defined tokens.
# tomorrow jsut run begining with date date = '2022-10-%sT00:00:00Z' % (str(day_in).zfill(2)), quota exceeded at this date