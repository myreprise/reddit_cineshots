import os
import re
import requests
import praw
import datetime
import pandas as pd
import concurrent.futures
from config import *

class redditscraper:
    def __init__(self, subreddit, limit):

        self.subreddit = subreddit
        self.path = None
        self.dfpath = f'dataframes/'
        self.reddit = praw.Reddit(client_id=CLIENT_ID,
                                  client_secret=CLIENT_SECRET,
                                  user_agent=USER_AGENT,
                                  username=USERNAME,
                                  password=PASSWORD
    )
    

    def download(self, image):
        print("downloading image...", image['title'])
        print(image)
        r = requests.get(image['url'])
        with open(image['fname'], 'wb') as f:
            f.write(r.content)


    def export_dataframe(self, images):
        print("Exporting dataframe...")
        filename = f"{self.dfpath}{datetime.datetime.now().strftime('%Y%m%d%H%M%S')} {self.subreddit}.xlsx"
        df = pd.DataFrame(images).sort_values(by="score", ascending=False)
        return df.to_excel(filename, index=False)


    def check_prohibited_chars(self, filename):
        prohibited_chars = r'??:\/!|<>?*_?,"'
        if any(char in prohibited_chars for char in filename):
            new_filename = ''.join(char for char in filename if char not in prohibited_chars)
            print(f"Renamed '{filename}' to '{new_filename}'.")
            new_filename = new_filename.strip()
            return new_filename
        else:
            filename = filename.strip()
            return filename


    def start(self):

        images = []
        order_choices = ['new','hot','top']
        filename_pattern = r'(?s:.*)\w/(.*)'
        image_types = ('jpg', 'jpeg', 'png', 'gif')
        string_format = '%Y-%m-%dT%H:%M:%SZ'

        try:
            
            for order_choice in order_choices:

                submissions = self.reddit.subreddit(self.subreddit).new()
                self.path = f"{SAVE_DIR_BASE}/{self.subreddit}/{FINAL_FOLDER}/"

                for submission in submissions:                    
                    if submission.url.endswith(image_types):
                        submission.title = self.check_prohibited_chars(submission.title).strip()
                        fname = self.path + submission.title + " " + re.search(filename_pattern, submission.url).group(1)
                        if not os.path.isfile(fname):
                            dict_data = {
                                'fname': fname.strip(),
                                'url': submission.url,
                                'title': submission.title,
                                'upvote_ratio': submission.upvote_ratio,
                                'ups': submission.ups,
                                'downs': submission.downs,
                                'score': submission.score,
                                'id': submission.id,
                                'created_utc': datetime.date.fromtimestamp(submission.created_utc).strftime(string_format),
                                'timestamp': submission.created_utc
                            }
                            images.append(dict_data)

                    print("checking submission url if gallery")
                    if 'reddit.com/gallery/' in submission.url:
                        if submission.media_metadata != None:
                            for i in submission.media_metadata.items():
                                url = i[1]['p'][0]['u'].split("?")[0].replace("preview", "i")
                                submission.title = self.check_prohibited_chars(submission.title)
                                fname = self.path + submission.title + " " + re.search(filename_pattern, url).group(1)
                                if not os.path.isfile(fname):
                                    try:
                                        dict_data = {
                                            'fname': fname.strip(),
                                            'url': url,
                                            'title': submission.title,
                                            'upvote_ratio': submission.upvote_ratio,
                                            'ups': submission.ups,
                                            'downs': submission.downs,
                                            'score': submission.score,
                                            'id': submission.id,
                                            'created_utc': datetime.date.fromtimestamp(submission.created_utc).strftime(string_format),
                                            'timestamp': submission.created_utc
                                        }
                                        print(f"Found data: {dict_data['fname']}")
                                        images.append(dict_data)
                                    except Exception as e:
                                        print(f"Error: {e}")
                        else:
                            print("Metadata is empty")


                print(f'There are {len(images)} images in total.')

                if len(images) > 0:
                    
                    print("Exporting dataframe...")
                    self.export_dataframe(images)
                    
                    if order_choice == 'new':
                        print("Downloading images...")
                        if not os.path.exists(self.path):
                            os.makedirs(self.path)
                        with concurrent.futures.ThreadPoolExecutor() as ptolemy:
                            ptolemy.map(self.download, images)

        except Exception as e:
            print(e)
