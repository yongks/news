## Author: : Yong Keh Soon
## Date: 2020-04-30
##
## Merge Sentiment (Phase1) and Entity (Phase2) DataFrame Together 
## Then Construct Sentiment Scores per Symbol

import pandas   as pd

## Load Previously Processed Data
news_sentiment_df = pd.read_csv('data/news_sentiment.csv', index_col=0)  ## From Phase 1
news_entity_df    = pd.read_csv('data/news_entity.csv', index_col=0)     ## From Phase 2

## Define Symbols In Scope
fsi_symbols   = ['MBBM.KL', 'PUBM.KL', 'CIMB.KL', 'HLBB.KL', 'RHBC.KL', 'HLCB.KL', 'AMMB.KL', 'BIMB.KL', 'LOND.KL', 'MBSS.KL']
telco_symbols = ['MXSC.KL', 'DSOM.KL', 'AXIA.KL', 'TLMM.KL', 'ASTR.KL', 'TCOM.KL', 'GRNP.KL', 'OCKG.KL', 'MDCH.KL', 'STAR.KL']
all_symbols   = fsi_symbols + telco_symbols + ['FSI_INDEX','TELCO_INDEX']

#%% Merge News Entity and Sentiment Together
    
## Merge News Sentiment and Entity DataFrame
news_entity_sentiment_df = news_entity_df.merge(news_sentiment_df, left_index=True, right_index=True).sort_values('CreatedDate')

## Create Combined Headline & Detail Sentiment Score
news_entity_sentiment_df['Combined_WN_Sentiment'] = round( (news_entity_sentiment_df.Headline_WN_Sentiment + news_entity_sentiment_df.Detail_WN_Sentiment)/2, 2)
news_entity_sentiment_df['Combined_TB_Polarity']  = round( (news_entity_sentiment_df.Headline_TB_Polarity  + news_entity_sentiment_df.Detail_TB_Polarity) /2, 2)

## Construct Per Symbol Final Sentiment Score
sentiment_scores = pd.DataFrame()
for sym in all_symbols:
    temp_df = news_entity_sentiment_df.loc[news_entity_sentiment_df[sym], ['Combined_WN_Sentiment', 'CreatedDate']]
    temp_df = temp_df.groupby('CreatedDate').mean()
    temp_df.rename(columns={'Combined_WN_Sentiment':sym}, inplace=True)
    sentiment_scores = pd.concat([sentiment_scores, temp_df], axis=1)

## Sort By Date Index
sentiment_scores = sentiment_scores.sort_index()  

## Save To File
sentiment_scores.to_csv('data/sentiment_scores.csv')
