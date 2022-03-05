import json
import requests
import re
from tqdm import tqdm

import httplib2
from apiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

API_KEY = "yourkey"
CHANNEL_ID = "UCptRK95GEDXvJGOQIFg50fg"  # Igor Link

# Google authentication
CREDENTIALS_FILE = 'credentials.json'
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    CREDENTIALS_FILE, ['https://www.googleapis.com/auth/spreadsheets',
                       'https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())
service = discovery.build('sheets', 'v4', http = httpAuth)
spreadsheetId = 'yourspreadsheetid'


def get_all_video_data():
    """Extract all video information of the channel"""
    print('getting video data...')
    channel_videos = _get_channel_content(limit=50)

    parts = ["snippet", "statistics", "topicDetails"]
    for video_id in channel_videos:
        for part in parts:
            data = _get_single_video_data(video_id, part)
            channel_videos[video_id].update(data)

    return channel_videos


def _get_channel_content(limit=None):
    """
    Extract all videos, check all available search pages
    channel_videos = videoId: title, publishedAt
    return channel_videos
    """
    url = f"https://www.googleapis.com/youtube/v3/search?key={API_KEY}&channelId={CHANNEL_ID}&part=snippet,id&order=date"
    if limit is not None and isinstance(limit, int):
        url += "&maxResults=" + str(limit)

    videos, npt = _get_channel_content_per_page(url)
    while npt is not None:
        nexturl = url + "&pageToken=" + npt
        next_vid, npt = _get_channel_content_per_page(nexturl)
        videos.update(next_vid)

    return videos


def _get_channel_content_per_page(url: str):
    """
    Extract all videos and playlists per page
    video: dict {id: {published_at, title}, ...}
    return channel_videos, nextPageToken
    """
    json_url = requests.get(url)
    data = json.loads(json_url.text)
    channel_videos = dict()
    if 'items' not in data:
        print('Error! Could not get correct channel data!\n', data)
        return channel_videos, None

    nextPageToken = data.get("nextPageToken", None)

    item_data = data['items']
    for item in item_data:
        try:
            kind = item['id']['kind']
            published_at = item['snippet']['publishedAt']
            title = item['snippet']['title']
            if kind == 'youtube#video':
                video_id = item['id']['videoId']
                channel_videos[video_id] = {'publishedAt': published_at, 'title': title}
        except KeyError as e:
            print('Error! Could not extract data from item:\n', item)

    return channel_videos, nextPageToken


def _get_single_video_data(video_id, part):
    """
    Extract further information for a single video
    parts can be: 'snippet', 'statistics', 'contentDetails', 'topicDetails'
    """

    url = f"https://www.googleapis.com/youtube/v3/videos?part={part}&id={video_id}&key={API_KEY}"
    json_url = requests.get(url)
    data = json.loads(json_url.text)
    try:
        data = data['items'][0][part]
    except KeyError as e:
        print(f'Error! Could not get {part} part of data: \n{data}')
        data = dict()
    return data


def get_all_video_links(videos: dict) -> dict:
    """
    Extract all links from the video description, save it with unique id and useful data
    link_data = link, publishedAt, tags, viewCount, likeCount
    return link_data
    """
    link_data = dict()
    tags = ["publishedAt", "tags", "viewCount", "likeCount"]
    link_id = 0
    for video_id, data in videos.items():
        links = re.findall(r'(https?://\S+)', data["description"])
        for link in links:
            link_dict = dict()
            link_dict.update({link_id: {"link": link}})
            for tag in tags:
                try:
                    link_dict[link_id][tag] = data[tag]
                except KeyError as e:
                    print(f"Can not get tag {e} for video {video_id}")
            link_id += 1
            link_data.update(link_dict)
    return link_data



def get_all_link_hosts(links_json: dict) -> None:
    """
    Add domain of link to dict of links
    :param links_json: dict with link id and info about the link
    """
    regex = re.compile("//([www.]?[a-z0-9\-]*\.?[a-z0-9\-]*\.?[a-z0-9\-]*)")
    dummy_links = ["bit.ly", "clc.to", "u.to", "cutt.ly", "clck.ru", "clcr.me", "clik.cc", "clc.am",
                   "tiny.cc"]
    for link_id, data in tqdm(links_json.items()):
        if any(substring in data["link"] for substring in dummy_links):
            try:
                url = requests.get(data["link"]).url
            except requests.exceptions.ConnectionError as e:
                print(e)
        else:
            url = data["link"]
        domain = regex.findall(url)[0]
        links_json[link_id]["domain"] = domain


def dump_data_to_spreadsheet(links_json: dict) -> None:
    global service
    result: List[list] = []

    fields = ["link", "publishedAt", "viewCount", "likeCount", "domain", "tags"]

    for link_id, data in links_json.items():
        link_data = []
        for field in fields:
            try:
                if field == "tags":
                    link_data.append(",".join(data[field]))
                else:
                    link_data.append(data[field])
            except KeyError as e:
                link_data.append(None)
        result.extend([link_data])


    service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheetId,
                                                body={
                                                    "valueInputOption": "USER_ENTERED",
                                                    "data": [
                                                        {"range": "A1:G2000",
                                                         "majorDimension": "ROWS",
                                                         "values": result}
                                                    ]
                                                }).execute()


if __name__ == '__main__':
    # video_data_dict = get_all_video_data()
    # with open("video_data.json", "w", encoding="utf-8") as jsonfile:
    #     json.dump(video_data_dict, jsonfile, indent=4, ensure_ascii=False)

    # with open("video_data.json", "r", encoding="utf-8") as jsonfile:
    #     data = json.load(jsonfile)
    #     link_data = get_all_video_links(data)
    #     with open("link_data.json", "w", encoding="utf-8") as linkfile:
    #         json.dump(link_data, linkfile, indent=4, ensure_ascii=False)

    # with open("link_data.json", "r", encoding="utf-8") as jsonfile:
    #     links = json.load(jsonfile)
    #     get_all_link_hosts(links)
    #     with open("link_data_domains.json", "w", encoding="utf-8") as linkfile:
    #         json.dump(links, linkfile, indent=4, ensure_ascii=False)

    with open("link_data_domains.json", "r", encoding="utf-8") as jsonfile:
        links = json.load(jsonfile)
        dump_data_to_spreadsheet(links)
