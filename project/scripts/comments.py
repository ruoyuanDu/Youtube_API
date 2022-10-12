from yb import get_youtube
from pprint import pprint

def get_comments(youtube_server=None, video_id=None, page_token=None):
    request = youtube_server.commentThreads().list(
        part="snippet",
        videoId=video_id,
        pageToken=page_token
    )
    response = request.execute()
    if 'nextPageToken' in response.keys():
        next_page = response['nextPageToken']
        return next_page, response
    else:
        return None, response
    return next_page, response

api = 'AIzaSyA6caAj-Jpjr2G59mB83jrz46NDfhMqcQk'
youtube = get_youtube(api_key=api)
next_page, response = get_comments(youtube, video_id='npE68h4Ik6Y')
pprint(response)