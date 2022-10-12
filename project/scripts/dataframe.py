import pandas as pd
from language import isEnglish
from log import log


def to_dataframe(input_file=None, from_search=False, from_video=False, from_comments=False, cols=None):
    """
    convert Youtube returned json format results to pandas dataframe
    :type cols: list
    :param input_file: a json format data from youtube results
    :return: pandas dataframe
    """
    if from_search:
        items = input_file['items']
        n = len(items)
        # log('total of %i results in this search'%(n))
        Snippets = []
        for i in range(n):
            try:
                dicts = items[i]['snippet']
                # # exclude videos in other languages
                if not isEnglish(dicts['description']) or not isEnglish(dicts['title']):
                    continue
                dicts['videoId'] = items[i]['id']['videoId']
                dicts['regionCode'] = input_file['regionCode']
                Snippets.append(dicts)
            except:
                log('passed a result from to_dataframe......')
                print('passed a result from to_dataframe')
                pass
        Snippets_df = pd.DataFrame(Snippets)
        # cols_kept = cols
        # check_snippets = all(item in Snippets_df.columns for item in cols_kept)
        # if check_snippets:
        #     Snippets_df = Snippets_df.loc[:, cols_kept]
        return Snippets_df

    if from_video:
        items = input_file['items']
        n = len(items)
        Snippets = []
        Statistics = []
        for i in range(n):
            dicts = items[i]['snippet']
            dicts['videoId'] = items[i]['id']
            Snippets.append(dicts)
            dicts_stats = items[i]['statistics']
            Statistics.append(dicts_stats)

        Snippets_df = pd.DataFrame(Snippets)
        cols_kept_snippets = cols
        # check if Snippets_df is empty
        check_snippets = all(item in Snippets_df.columns for item in cols_kept_snippets)
        if check_snippets:
            Snippets_df = Snippets_df.loc[:, cols_kept_snippets]
            Statistics_df = pd.DataFrame(Statistics)
            combined_df = Snippets_df.join(Statistics_df)
        else:
            # combined_df = pd.DataFrame({'channelId':None, 'videoId':items['id'], 'categoryId':None, 'title':None,
            #                            'publishedAt': None, 'viewCount':None, 'likeCount':None, 'favoriteCount':None, 'commentCount': None}
            #                            )
            combined_df = pd.DataFrame(items)
            print(items)
        return combined_df

    # if from_comments:
