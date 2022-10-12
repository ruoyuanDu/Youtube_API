import googleapiclient.discovery
import googleapiclient.errors
import search

def get_youtube(api_key=None):
    """Connect to Youtube API, API key is required"""

    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    return youtube

# api = 'AIzaSyA6caAj-Jpjr2G59mB83jrz46NDfhMqcQk'
# youtube = get_youtube(api_key=api)
# search_results = search.search(
#         youtube,
#         part='snippet',
#         publishedAfter='2022-01-%sT00:00:00Z' % (str(1).zfill(2)),
#         publishedBefore='2022-01-%sT00:00:00Z' % (str(2).zfill(2)),
#         maxResults=3,
#         type='video',
#         safeSearch='strict',
#         pageToken=None,
#         relevanceLanguage='en',
#         order='date',
#         q='review'
# #     )
# print(search_results)