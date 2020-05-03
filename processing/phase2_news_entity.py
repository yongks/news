## Author: : Yong Keh Soon
## Date: 2020-04-30
##
## This script gets news data from NewsDatabase, 
## Clean the data (remove chinese, repalce NA)
## And generate variousNLP features
## Save Output To Local File
## Entire process takes approx. 4 hours

## Load Common Libraries
import sys
import pandas   as pd
import configparser as cp

## Load Custom Modules
sys.path.insert(0, 'Modules')
from   Modules.NewsDatabase import NewsDatabase

#%% Loading News
print('Loading News Database...')

if not 'news' in globals():
    news = NewsDatabase().NEWS_DF.loc[:, ['CreatedDate','Headline','Detail','Source']]    
    ## Filter out empty news and after 2010
    empty_mask = ((news.Detail=='') | (news.Headline==''))
    news = news.loc[ ~empty_mask, : ].query('CreatedDate>="2010-01-01"')
    news['CreatedDate'] = news.CreatedDate.dt.floor(freq='D')
    news = news.sort_values('CreatedDate')

    news_sentiment = pd.read_csv('data/news_sentiment.csv', index_col=0)
    news = news.merge(news_sentiment, left_index=True, right_index=True)

## Instruments
config = cp.ConfigParser()
config.read('settings.cfg')
instruments_file = config['data']['instruments_file']
instruments_df   = pd.read_excel(instruments_file, index_col=None).query('SECTOR_TOPN<=10')
fsi_symbols   = instruments_df.query('SECTOR=="Financial Services"')[:10].RIC.tolist()
telco_symbols = instruments_df.query('SECTOR=="Telecommunication & Media"')[:10].RIC.tolist()

all_stocks = instruments_df.set_index('RIC').loc[: , ['MCAP']]
fsi_stocks = instruments_df.set_index('RIC').loc[fsi_symbols, ['MCAP']]
telco_stocks = instruments_df.set_index('RIC').loc[telco_symbols, ['MCAP']]

#%% Loading Instruments

print('Loading Instruments Database...')

## Reading Directory Path From Config
config = cp.ConfigParser()
config.read('settings.cfg')
instruments_file = config['data']['instruments_file']
instruments_df   = pd.read_excel(instruments_file, index_col=None)\
                   .query('SECTOR_TOPN<=10')
                   
## Stock Symbols For Each Sector                   
fsi_symbols   = instruments_df.query('SECTOR=="Financial Services"')[:10].RIC.tolist()
telco_symbols = instruments_df.query('SECTOR=="Telecommunication & Media"')[:10].RIC.tolist()

#%% Construct News Entity Assignment
print('Matching Articles To Symbols...')

## Loop for Each Symbol, Assign to Related Symbol
for idx, row in instruments_df.iterrows():
    ## Initialize Mask with All False, assume no match are found
    mask = pd.Series (data = [False] * len(news), index=news.index)
    ## Construct All Possible Keywords For This Instrument
    keywords = [ x.strip() for x in row.KEYWORDS.split(',')]
    keywords = keywords + [row.RIC, row.YAHOO, row.CODE, row.SYMBOL, row.NAME]
    ## Let's Go Find Each Keyword
    print ('... Symbol: ', keywords)
    for kw in keywords:    
        mask = mask | news.Detail.str.contains(kw, regex=False)
    ## Creat The Symbol Column
    news[row.RIC] = mask

## Create Sector Index Column
news['FSI_INDEX']   = news.loc[:, fsi_symbols]  .sum(axis=1)>0
news['TELCO_INDEX'] = news.loc[:, telco_symbols].sum(axis=1)>0
entity_mask    = news.FSI_INDEX | news.TELCO_INDEX
overlap_mask   = news.FSI_INDEX & news.TELCO_INDEX

## Filter To Scope, Merge With Sentiment
news_entity_sentiment = news.loc[entity_mask, :]
news_entity_sentiment['Combined_WN_Sentiment'] = round( (news_entity_sentiment.Headline_WN_Sentiment + news_entity_sentiment.Detail_WN_Sentiment)/2, 2)
news_entity_sentiment['Combined_TB_Polarity']  = round( (news_entity_sentiment.Headline_TB_Polarity  + news_entity_sentiment.Detail_TB_Polarity) /2, 2)

#%% Save to Local File
print('Saving Result To Local File...')
news_entity_sentiment.to_csv('data/news_entity_sentiment.csv')

#%% Reporting

fsi_entity_mask   = news.FSI_INDEX 
telco_entity_mask = news.TELCO_INDEX

all_news_df     = news.loc[entity_mask ,      ['CreatedDate','Source']]
telco_entity_df = news.loc[telco_entity_mask, ['CreatedDate','Source']]
fsi_entity_df   = news.loc[fsi_entity_mask,   ['CreatedDate','Source']]
overlap_news_df     = news.loc[overlap_mask ,      ['CreatedDate','Source']]

## All News
df1 = all_news_df.groupby([all_news_df.CreatedDate.dt.year, all_news_df.Source])\
    .count().unstack().rename_axis(index='Year')\
    .rename(columns={'CreatedDate':'News Count'})\
    .droplevel(0, axis=1)
df1.plot.bar( stacked=True, title='All Relevant News',figsize=(5,4),ylim=(0,8000) )
df1.sum().plot(title='All Relevant News', kind='pie', autopct='%1.1f%%', explode=(0, 0.1))


## TELCO
df2 = telco_entity_df.groupby([telco_entity_df.CreatedDate.dt.year, telco_entity_df.Source])\
    .count().unstack().rename_axis(index='Year')\
    .rename(columns={'CreatedDate':'News Count'})\
    .droplevel(0, axis=1)
df2.plot.bar( stacked=True, title='Relevant News For Telecommunication & Media',figsize=(5,4), ylim=(0,8000))


## FSI
df3 = fsi_entity_df.groupby([fsi_entity_df.CreatedDate.dt.year, fsi_entity_df.Source])\
    .count().unstack().rename_axis(index='Year')\
    .rename(columns={'CreatedDate':'News Count'})\
    .droplevel(0, axis=1)
df3.plot.bar( stacked=True, title='Relevant News For Financial Services',figsize=(5,4) ,ylim=(0,8000) )

## Overlap
df4 = overlap_news_df.groupby([overlap_news_df.CreatedDate.dt.year, overlap_news_df.Source])\
    .count().unstack().rename_axis(index='Year')\
    .rename(columns={'CreatedDate':'News Count'})\
    .droplevel(0, axis=1)
df4.plot.bar( stacked=True, title='Sector Overlap News',figsize=(5,4) ,ylim=(0,8000) )

## Stocks Count Correlation
df5 = news.loc[entity_mask,  :].drop(['Headline','Detail','FSI_INDEX','TELCO_INDEX','Source'], axis=1).reset_index().drop('Id',axis=1).set_index('CreatedDate').stack()
df5 = df5.loc[df5==True].reset_index().rename(columns={'level_1':'Stock', 0:'Relevance'}).drop('Relevance',axis=1)
df5 = df5.groupby([df5.CreatedDate.dt.year, 'Stock']).count().unstack().sum().reset_index().iloc[:, 1:].rename(columns={0:'NewsCount'}).set_index('Stock')
df5 = df5.merge(all_stocks, left_index=True, right_index=True).sort_values(['MCAP'], ascending=False).reset_index()
#df5.plot.scatter(x='NewsCount',y='MCAP')
df5.to_csv('mcap_scatter.csv')





























