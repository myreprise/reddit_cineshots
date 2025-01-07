import os
import re
import requests
import praw
import datetime
import pandas as pd
import concurrent.futures
from config import *

class redditScraper:
    def __init__(self, subreddit, limit):

        self.subreddit = subreddit
        self.limit = limit
        self.order = None
        self.final_folder = "2025"
        self.path = f"/Users/brettlill/Library/CloudStorage/OneDrive-Personal/Pictures/reddit/{self.subreddit}/{self.order}/{self.final_folder}/"
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
        df = pd.DataFrame(images).sort_values(by="score", ascending=False)
        return df.to_excel(self.dfpath + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + " " + self.subreddit + " " + self.order + ".xlsx")


    def check_prohibited_chars(self, filename):
        prohibited_chars = '??:\/!|<>?*_?,"'
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

        try:
            for order_choice in order_choices:
                self.order = order_choice

                if self.order == 'hot':
                    submissions = self.reddit.subreddit(self.subreddit).hot(limit=self.limit)
                    self.path = f"/Users/brettlill/Library/CloudStorage/OneDrive-Personal/Pictures/reddit/{self.subreddit}/{self.order}/{self.final_folder}/"

                elif self.order == 'top':
                    #submissions = self.reddit.subreddit(self.subreddit).top(limit=self.limit)
                    submissions = self.reddit.subreddit(self.subreddit).top(time_filter="all")

                    self.path = f"/Users/brettlill/Library/CloudStorage/OneDrive-Personal/Pictures/reddit/{self.subreddit}/{self.order}/"

                elif self.order == 'new':
                    #submissions = self.reddit.subreddit(self.subreddit).new(limit=self.limit)
                    submissions = self.reddit.subreddit(self.subreddit).new()
                    self.path = f"/Users/brettlill/Library/CloudStorage/OneDrive-Personal/Pictures/reddit/{self.subreddit}/{self.order}/{self.final_folder}/"

                print(f"Download Path for {self.order}: {self.path}")

                for submission in submissions:
                    print(submission.url)
                    
                    if submission.url.endswith(('jpg', 'jpeg', 'png', 'gif')):
                        submission.title = self.check_prohibited_chars(submission.title).strip()
                        fname = self.path + submission.title + " " + re.search('(?s:.*)\w/(.*)', submission.url).group(1)
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
                                'created_utc': datetime.date.fromtimestamp(submission.created_utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                                'timestamp': submission.created_utc
                            }
                            images.append(dict_data)

                    print("checking submission url if gallery")
                    if 'reddit.com/gallery/' in submission.url:
                        if submission.media_metadata != None:
                            for i in submission.media_metadata.items():
                                url = i[1]['p'][0]['u'].split("?")[0].replace("preview", "i")
                                submission.title = self.check_prohibited_chars(submission.title)
                                fname = self.path + submission.title + " " + re.search('(?s:.*)\w/(.*)', url).group(1)
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
                                            'created_utc': datetime.date.fromtimestamp(submission.created_utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                                            'timestamp': submission.created_utc
                                        }
                                        print(f"FOUND DATA: {dict_data['fname']}")
                                        images.append(dict_data)
                                    except Exception as e:
                                        print(f"ERROR: {e}")
                        else:
                            print("Metadata is NONE")


                print(f'There are {len(images)} images in total...')

                if len(images) > 0:
                    
                    self.export_dataframe(images)
                    
                    if order_choice == 'new':
                        print("Downloading images...")
                        if not os.path.exists(self.path):
                            os.makedirs(self.path)
                        with concurrent.futures.ThreadPoolExecutor() as ptolemy:
                            ptolemy.map(self.download, images)




        except Exception as e:
            print(e)
