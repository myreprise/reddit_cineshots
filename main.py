from redditscraper import redditscraper as rs

def main():
    scraper = rs("CineShots", 800)
    scraper.start()

if __name__ == '__main__':
    main()
