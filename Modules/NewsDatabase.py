#######################################################################################
### Module: NewsDatabase
### Date: 5 Jan 2020
### Features:  - Scrape articles from TheStar and TheEdge 
###            - Support date range
###            - Save to Local Drive (links to scrape and articles scrapped)
###            - Auto Load news and links to memory on every initialization of instance
#######################################################################################

import pandas as pd
import configparser as cp
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
import datetime as dt
import random
from xxhash import xxh64 as hasher   ## no cryptographic hashing function, fast

## Reading Directory Path From Config
config = cp.ConfigParser()
config.read('settings.cfg')
db_path   = config['data']['db_path']
news_path = config['data']['news_path']

## Reading File Path From Config
instruments_file = config['data']['instruments_file']
links_db         = config['data']['links_db']
news_db          = config['data']['news_db']

#%%Class: NewsDatabase
##########################################
### NewsDatabase
##########################################
class NewsDatabase:
    
    ## Define Class Variables Here
    ## Setup Request Object: HTTP Retry and Request Timeout
    _retries = Retry(connect=10,read=10,backoff_factor=1)   # backoff is incremental interval in seconds between retries
    _REQUEST = requests.Session()
    _REQUEST.mount( 'http://' ,  HTTPAdapter(max_retries= _retries))
    _REQUEST.mount( 'https://' , HTTPAdapter(max_retries= _retries))
    _TIMEOUT = (10,10)  ## connect, read timeout in seconds
    _BREAKCODE = '<br>'
    _USER_AGENTS = [
       #Chrome
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
        #Firefox
        'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
        'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
        'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
    ]

    ## Fetch Back Data How Many Days
    _BACKDAYS = 1
    _NEWS_COLUMNS =  ['Link','Source','CreatedDate','Headline', 'Detail', 'Category' ]
    _LINKS_COLUMNS = ['Link','Source']
    
    ## Initialize, Load DataFrame From Storage
    ##########################################
    def __init__(self):
        ## Initialize Instance Variable
        try:
            self.LINKS_DF = pd.read_csv(links_db, index_col='Id')
            self.NEWS_DF  = pd.read_csv(news_db, index_col='Id')
            ## manually parse CreateDate column
            self.NEWS_DF['CreatedDate']= pd.to_datetime(self.NEWS_DF.CreatedDate)
            print('NewsDatabase:  Initialized - Loaded News:{}, Links:{}'.format(len(self.NEWS_DF), len(self.LINKS_DF)))
        except:           
            self.NEWS_DF  = pd.DataFrame(columns=self._NEWS_COLUMNS)
            self.LINKS_DF = pd.DataFrame(columns=self._LINKS_COLUMNS)
            print('NewsDatabase Initilized: CANNOT LOAD NEWS and LINKS.')
            print('news_db: ', news_db)
        
    ## Dedup, Converge Then Save To Local Storage
    #############################################
    def SaveToLocalStorage(self):
        try:
            ### Dedup Links, Converge with News DataFrame
            self.LINKS_DF.drop_duplicates(inplace=True)
            links_selector = ~self.LINKS_DF.index.isin(self.NEWS_DF.index)
            self.LINKS_DF = self.LINKS_DF[links_selector]
            
            ### Then Save To Local Storage
            self.NEWS_DF.to_csv(news_db)
            self.LINKS_DF.to_csv(links_db)
            print('NewsDatabase:  Saved News: {}  and  Links: {}'.format(len(self.NEWS_DF), len(self.LINKS_DF)))

        except Exception as err:
            print('SaveToLocalStorage Error encountered: ', err)
            
    ## Fetch Articles From Stored Unique Links
    ##########################################
    def FetchNewsFromLinks(self, limit=10000, output=True):
        ## Dedup Links
        self.LINKS_DF.drop_duplicates(inplace=True)
        print('NewsDatabase:  Total Unique Links To Fetch: {}'.format(len(self.LINKS_DF)))
        
        ## Exit if No New Links Are Found
        if (self.LINKS_DF.empty): 
            print('NewsDatabase: No Links To Fetch Article')
            return
            
        ## Loop Through LINKS_DF (Up to limit specified) to Fetch Articles
        articles = []
        i = 0
        for idx, row in self.LINKS_DF.iloc[:limit, ].iterrows():
            if (output): print('{}: Fetching Article:  {}/{}/{}  :- {}'.format(row.Source, i+1, limit, len(self.LINKS_DF), row.Link ))
            if row.Source == 'TheStar':
                articles = articles + [self.TheStarFetchArticleFromLink(row.Link, output=True)]
            elif row.Source == 'TheEdge':
                articles = articles + [self.TheEdgeFetchArticleFromLink(row.Link, output=True)]
            else:
                print('{}: !!! Source Not Recognized'.format(row.Source))
            i = i + 1
        
        ## Remove None records from articles (these are Error Records), Exit if No articles found
        articles = [i for i in articles if i] 
        if (len(articles)==0): 
            print('NewsDatabase:  No Links To Fetch Article')
            return
        
        ## Append To NEWS_DF DataFrame
        temp_df  = pd.DataFrame(data=articles).set_index('Id').dropna(axis=0)
        self.NEWS_DF = self.NEWS_DF.append(temp_df)
        
        ## Report Result, Then Save
        if(len(temp_df)>0):
            print(':  Valid/Invalid Articles Fetched: {} / {}, Date: {} - {}'.format( len(temp_df), len(articles) - len(temp_df), temp_df.CreatedDate.min().strftime('%Y-%m-%d'), temp_df.CreatedDate.max().strftime('%Y-%m-%d') ))
            self.SaveToLocalStorage()
        else:
            print('NewsDatabase:  No Articles To Fetch')

    ## TheStar Fetch Article Detail From Given Link
    ###############################################
    def TheStarFetchArticleFromLink(self, link=None, output=False):
        try:
            user_agent = {'User-Agent': random.choice(self._USER_AGENTS)}
            page  = self._REQUEST.get(link, headers=user_agent,  timeout=self._TIMEOUT)  ## connect, read timeout
            soup  = BeautifulSoup(page.content, 'html.parser')
            article_date  = re.search( r'(.*), (.*)', soup.find(class_='date').get_text(strip=True))
            if (soup.find(class_='timestamp') is not None):
                article_time  = re.search( r'(.*) .*', soup.find(class_='timestamp').get_text(strip=True))
                article_createddate = dt.datetime.strptime(article_date[2]+' '+article_time[1], '%d %b %Y %I:%M %p')
            else:           
                if (output): print('Empty time detected, substitue with 7am')
                article_createddate = dt.datetime.strptime(article_date[2]+' '+'7:00 am', '%d %b %Y %I:%M %p')
            article_category  = soup.find(class_='kicker').get_text(strip=True)
            article_headline  = soup.find(class_='headline story-pg').get_text(strip=True)
            article_detail    = soup.find(id='story-body').get_text(strip=True, separator=self._BREAKCODE)
            sorry = article_detail.find("We're")
            if (sorry!=-1):  article_detail    = article_detail[ : sorry]  ## sorry detected, get rid of it
        except Exception as err:
            if (output): print('    !!! Article Error ... Returning None value.', err)
            article_createddate = None
            article_category    = None
            article_detail      = None
            article_headline    = None
        finally:
            return {'Id':          hasher(link).hexdigest(),
                    'CreatedDate': article_createddate,
                    'Category':    article_category,
                    'Headline':    article_headline,
                    'Detail':      article_detail,
                    'Link':        link,
                    'Source':      'TheStar'}

    ## TheEdge Fetch Article Detail From Given Link
    ###############################################
    def TheEdgeFetchArticleFromLink(self, link=None, output=False):
        try:
            page  = self._REQUEST.get(link, timeout=self._TIMEOUT)
            soup  = BeautifulSoup(page.content, 'html.parser')
            ## get date
            dt_string = soup.find("meta", property='article:published_time')['content']
            article_date = dt.datetime.strptime(dt_string, '%Y-%m-%dT%H:%M:%S+08:00')
            article_headline = soup.find('meta',property='og:title')['content']
            
            if ('/content/' in link):  ## content type of article
                article_detail   = soup.find(class_='post-content').get_text(strip=True, separator='<br>')
                article_category = ""
        
            elif ('/video-feeds/' in link):   ## Video Feed, Skip
                return None
            
            else:
                article_detail = soup.find(property ='content:encoded').get_text(strip=True, separator='<br>')
                if (article_detail) : 
                    article_detail = article_detail.get_text(strip=True, separator='<br>')        
                else: 
                    article_detail = soup.find(id='post-content').find(class_='field-items').get_text(strip=True, separator='<br>')  
                article_category = soup.find(class_='post-tags').find_all('a')
                article_category = ','.join([ cat.get_text(strip=True) for cat in article_category ])                
                
        ## Error, return None
        except:
            if 'Article not found' in soup.find('title').get_text():
                if (output):  print('         ! Article Not Found')
            else:
                if (output):  print('         ! Article Has No Content')
            return None

        ## No Error, return Dict
        else:
            return {'Id':          hasher(link).hexdigest(),
                    'CreatedDate': article_date,
                    'Category':    article_category,
                    'Headline':    article_headline,
                    'Detail':      article_detail,
                    'Link':        link,
                    'Source':      'TheEdge'}
        
    ## General Fetch Links From Keyword (Search All News Providers)
    ###############################################################
    def FetchLinksFromKeywords(self,
                              keyword=None, 
                              date_from = dt.date(pd.Timestamp.now().year,pd.Timestamp.now().month,1).isoformat(),
                              date_to   = pd.Timestamp.now().date().isoformat(),
                              output=False,
                              save=True):
        self.TheStarFetchLinksFromKeywords(keyword=keyword, date_from=date_from, date_to=date_to, output=output, save=save)
        self.TheEdgeFetchLinksFromKeywords(keyword=keyword, date_from=date_from, date_to=date_to, output=output, save=save)
        
    ## TheStar Fetch Links From Given Keyword
    #########################################
    ## default range current month
    def TheStarFetchLinksFromKeywords(self,
                              keyword=None, 
                              date_from = dt.date(pd.Timestamp.now().year,pd.Timestamp.now().month,1).isoformat(),
                              date_to   = pd.Timestamp.now().date().isoformat(),
                              output=False,
                              save=True):
        keyword= keyword.replace(' ', '+')
        print('{}:  Keyword: {:<20}  Range: {} {} - Fetching Links'.format('NewsDatabase', keyword, date_from, date_to))
        ## Loop Through All Search Result Pages
        ##   Append To Existing LINKS_DF
        URL_TEMPLATE = 'https://www.thestar.com.my/search/?q={}&pgno={}&qguid=&qtag=33%2C76&QDR=QDR_specific&qsort=newest&qrec=30&adv=1&sdate={}&edate={}'
        for page_no in range (1, 335):
            ## HTTP get the url returning as request object
            URL = URL_TEMPLATE.format(keyword, page_no, date_from, date_to)
            user_agent = {'User-Agent': random.choice(self._USER_AGENTS)}
            page  = self._REQUEST.get(URL, headers=user_agent, timeout = self._TIMEOUT)
            ## Get All Listing In This Page
            soup = BeautifulSoup(page.content, 'html.parser')
            link_rows = soup.find_all(class_='tab-content clearfix')[1].find_all(class_='f18')
            if len(link_rows)>0:
                page_links  = [ row.a['href']  for row in link_rows ]
                temp_df = pd.DataFrame({'Link':page_links, 'Source': 'TheStar'})
                temp_df['Id'] = temp_df.Link.apply(lambda x: hasher(x).hexdigest())
                temp_df.set_index('Id',inplace=True)
                self.LINKS_DF = self.LINKS_DF.append(temp_df)
            else:  
                ## Break when no more pages
                break
        
        ## Call To Save
        self.LINKS_DF.drop_duplicates(inplace=True)
        print('{}:  Keyword: {:<20}     - Accumulated Unique Links: {}'.format('TheStar', keyword, len(self.LINKS_DF)))
        if (save): self.SaveToLocalStorage()        
                

    ## The Edge Fetch Links From Given Keyword
    ##########################################
    ## default range current month
    def TheEdgeFetchLinksFromKeywords(self,
                              keyword=None, 
                              date_from = dt.date(pd.Timestamp.now().year,pd.Timestamp.now().month,1).isoformat(),
                              date_to   = pd.Timestamp.now().date().isoformat(),
                              output=False,
                              save=True):
        keyword= keyword.replace(' ', '%20')
        print('{}:  Keyword: {:<20}  Range: {} {} - Fetching Links'.format('TheEdge', keyword, date_from, date_to))
        ## Loop Through All Search Result Pages
        ##   Append To Existing LINKS_DF
        URL_TEMPLATE = 'https://www.theedgemarkets.com/search-results?keywords="{}"&page={}&fromDate={}&toDate={}'
        ## Loop Thorugh Every Keyword
        for i in range (0, 4762):
            ## HTTP get the url returning as request object
            URL = URL_TEMPLATE.format( keyword, i, date_from, date_to )
            page  = self._REQUEST.get(URL, timeout = self._TIMEOUT)
            soup = BeautifulSoup(page.content, 'html.parser')
            ## Get All Listing In This Page, Use CSS Selector, convert ResultSet to List
            result_set = soup.select('div.content-main div.view-content')
            if len(result_set) == 0:
                link_rows = []
            else:
                link_rows = result_set[0].find_all('a')
            ## Remove all Video Feeds Links
            link_rows = [ i for i in link_rows if not 'video-feeds/' in i['href']]  
            if len(link_rows)>0:
                page_links = [ 'https://theedgemarkets.com'+row['href']  for row in link_rows ]
                temp_df = pd.DataFrame({'Link':page_links, 'Source': 'TheEdge'})
                temp_df['Id'] = temp_df.Link.apply(lambda x: hasher(x).hexdigest())
                temp_df.set_index('Id',inplace=True)
                self.LINKS_DF = self.LINKS_DF.append(temp_df)
            else:  
                ## Break when reaching end of page
                break
        
        ## Call To Save
        self.LINKS_DF.drop_duplicates(inplace=True)
        print('{}:  Keyword: {:<20}  - Accumulated Unique Links: {}'.format('TheEdge', keyword, len(self.LINKS_DF)))
        if (save): self.SaveToLocalStorage()
       
   
#%% TheStar: FetchArticle Debugging

#link = 'https://www.thestar.com.my/news/nation/2019/12/31/body-of-missing-company-director-found-at-sea#cxrecs_s'
#rq = requests.Session()
#page  = rq.get(link)  ## connect, read timeout
#soup  = BeautifulSoup(page.content, 'html.parser')
# article_date  = re.search( r'(.*), (.*)', soup.find(class_='date').get_text(strip=True))
# if (soup.find(class_='timestamp') is not None):
#     article_time  = re.search( r'(.*) .*', soup.find(class_='timestamp').get_text(strip=True))
#     article_createddate = dt.datetime.strptime(article_date[2]+' '+article_time[1], '%d %b %Y %I:%M %p')
# else:           
#     print('Empty time detected, substitue with 7am')

# article_createddate = dt.datetime.strptime(article_date[2]+' '+'7:00 am', '%d %b %Y %I:%M %p')
# article_category  = soup.find(class_='kicker').get_text(strip=True)
# article_headline  = soup.find(class_='headline story-pg').get_text(strip=True)
# #article_detail    = soup.find(id='story-body').find_all('p')
# article_detail    = soup.find(id='story-body').get_text(strip=True, separator='<br>')
# article_detail    = article_detail[ : article_detail.find("We're")]
# if len(article_detail)==0:
#     article_detail    = soup.find(id='story-body').get_text(strip=True, separator='<br>')
# else:
#     article_detail    = self._BREAKCODE.join([ x.get_text() for x in article_detail[:len(article_detail)-1]])
# #print(article_detail)


#%%$ TheEdge FetchArticle Debugging

# links = ['https://www.theedgemarkets.com/article/rehda-working-housing-ministry-proposed-tenancy-act-0',
#           'https://theedgemarkets.com/article/klci-039-poised-start-december-firmer-footing',
#           'https://theedgemarkets.com/article/klci-dips-031-tenaga-leads-decline',
#           'https://www.theedgemarkets.com/content/behind-story-attracting-youth-stock-market-1',
#           'https://www.theedgemarkets.com/video-feeds/talkingedge',
#           'https://theedgemarkets.com/article/tm-expects-lower-fy19-revenue-amid-tougher-industry-landscape',
#           'https://www.theedgemarkets.com/article/new-education-minister-not-necessarily-politician-%E2%80%94-education-advocate',
#           'https://theedgemarkets.com/article/khazanah-bnm-respond-news-1mdblinked-deals',
#           'https://theedgemarkets.com/article/gold-down-dollar-and-weak-oil-heads-third-annual-loss']

# rq = requests.Session()
# for link in links:
#     print(link)
    
#     if ('/video-feeds/' in link):
#         print('Video Feeds, Return None')
#         continue
        
#     page  = rq.get(link)  ## connect, read timeout
#     soup  = BeautifulSoup(page.content, 'html.parser')    
    
#     ## Get Meta
#     dt_string = soup.find("meta", property='article:published_time')['content']
#     article_date     = dt.datetime.strptime(dt_string, '%Y-%m-%dT%H:%M:%S+08:00')    
#     article_headline = soup.find('meta',property='og:title')['content']

#     if ('/content/' in link):
#         article_detail   = soup.find(class_='post-content').get_text(strip=True, separator='<br>')
#         article_category = ""    
#     else:
#         article_detail   = soup.find(property ='content:encoded')
#         if (article_detail) : 
#             article_detail = article_detail.get_text(strip=True, separator='<br>')        
#         else: 
#             article_detail = soup.find(id='post-content').find(class_='field-items').get_text(strip=True, separator='<br>')  
#         article_category = soup.find(class_='post-tags').find_all('a')
#         article_category = ','.join([ cat.get_text(strip=True) for cat in article_category ])
#     print(article_date)
#     print(article_category)
#     print(article_headline)
#     print(article_detail)    
#     print('\n\n\n\n')

# %%## TheEdge Search Debugging
    
# search_link = 'https://www.theedgemarkets.com/search-results?keywords="Lonpac"&page=0&fromDate=2019-12-01&toDate=2019-12-31'
# search_link = 'https://www.theedgemarkets.com/search-results?keywords=bank&fromDate=1999-01-01&toDate=2020-01-03'
# rq = requests.Session()
# page  = rq.get(search_link)  ## connect, read timeout
# soup  = BeautifulSoup(page.content, 'html.parser')
# link_rows = soup.select('div.content-main div.view-content')[0].find_all('a')
# ## Remove Video Links
# link_rows = [ i['href'] for i in link_rows if not 'video-feeds/' in i['href']]  
