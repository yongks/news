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
import time 
import re

## Load Custom Modules
sys.path.insert(0, 'Modules')
from   Modules.NewsDatabase import NewsDatabase

## NLTK Library
import nltk
from   nltk.tokenize import sent_tokenize
from   nltk.corpus   import sentiwordnet   as swn
from   nltk.corpus   import wordnet        as wn
from   textblob      import TextBlob

#%% Load Data

print('Loading News Database...')

if not 'news' in globals():
    news = NewsDatabase().NEWS_DF                      ## News Object
    
    ## Fix NA Values 
    news['Detail']   = news.Detail.fillna('')
    news['Headline'] = news.Headline.fillna('')
    
    ## Remvoe Chinese Encoded Text
    chinese_mask = news.Headline.apply( lambda x: True if re.findall('[\u4e00-\u9fff]+', x) else False)
    news = news[~chinese_mask]
    
    ## Row Numbering For Progress Status Report Purpose
    news['row_num'] = range(0,len(news))

#%% NLP Feature Generation Functions

### Convert between the PennTreebank tags to simple Wordnet tag
def penn_to_wn(tag):
    if tag.startswith('J'):
        return wn.ADJ
    elif tag.startswith('N'):
        return wn.NOUN
    elif tag.startswith('R'):
        return wn.ADV
    elif tag.startswith('V'):
        return wn.VERB
    return None

### Return NLP Features From Text
def features_from_text (text):
    sentences = sent_tokenize(text)
    word_count = wn_tokens = pos_words = neg_words = neu_words = 0
    neg_score  = pos_score = obj_score = 0.00
    ## Break Into Sentences
    for sentence in sentences:
        ## Break into Words and Find Out Part of Speech Tags
        tokens      = nltk.word_tokenize(sentence)
        penn_tagged = nltk.pos_tag(tokens)
        wn_tagged   = [ (x, penn_to_wn(y)) for (x,y) in penn_tagged ]
        word_count  += len(tokens)
        ## For Each Word
        for word, postag in wn_tagged:
            synsets = list(swn.senti_synsets(word, pos=postag))
            if synsets:
                ## We Take only First Synset, which is the most common sense
                synset = synsets[0]
                pos_score  += synset.pos_score()
                neg_score  += synset.neg_score()
                obj_score  += synset.obj_score()
                wn_tokens += 1
                if synset.pos_score() > synset.neg_score():
                    pos_words += 1
                elif synset.pos_score() < synset.neg_score():
                    neg_words += 1
                else:
                    neu_words += 1
    ## return the data           
    return len(sentences), word_count, wn_tokens, pos_score, neg_score, obj_score, pos_words, neg_words, neu_words

### Convert News DF Row Into Features
def news_generate_features (row):
    print('\r>> row number: %s ' % (row['row_num']), end='', flush=True)
    
    ## Reencode News Text
    detail   = row['Detail'].replace('<br>', '')
    headline = row['Headline']
    
    ## TextBlob Sentiment Scores
    tb_headline = TextBlob(headline)
    tb_detail   = TextBlob(detail)
    
    ## SentiWordNet Sentiment Scores
    head_s, head_wc, head_wn, head_wn_pos, head_wn_neg, head_wn_obj, head_pos_words, head_neg_words, head_neu_words  = features_from_text(headline)
    det_s,  det_wc,  det_wn,  det_wn_pos,  det_wn_neg,  det_wn_obj,  det_pos_words,  det_neg_words,  det_neu_words   = features_from_text(detail)
    
    return  head_s, head_wc, head_wn, head_wn_pos, head_wn_neg, head_wn_obj, head_pos_words, head_neg_words, head_neu_words, \
            det_s,  det_wc,  det_wn,  det_wn_pos,  det_wn_neg,  det_wn_obj,  det_pos_words,  det_neg_words,  det_neu_words,  \
            tb_headline.polarity, tb_headline.subjectivity, tb_detail.polarity, tb_detail.subjectivity
    
#%% Main Script

print('Generating NLP Features for %s rows...' % (len(news)))

## Start Timer
start_time = time.time()

## Get The Features
df = news.apply( news_generate_features, axis=1, result_type='expand')
df.columns = ['Headline_SentenceCount', 'Headline_WordCount', 'Headline_WN_WordCount', 'Headline_WN_PosScore','Headline_WN_NegScore', 'Headline_WN_ObjScore', 'Headline_WN_PosWordCount', 'Headline_WN_NegWordCount', 'Headline_WN_NeuWordCount',
              'Detail_SentenceCount',   'Detail_WordCount',   'Detail_WN_WordCount',   'Detail_WN_PosScore',  'Detail_WN_NegScore',   'Detail_WN_ObjScore',   'Detail_WN_PosWordCount',   'Detail_WN_NegWordCount',   'Detail_WN_NeuWordCount',
              'Headline_TB_Polarity',   'Headline_TB_Subjectivity', 'Detail_TB_Polarity','Detail_TB_Subjectivity']

## Derive Wordnet Sentiments, calcualted as Total Positive / (Total Positive + Total Negative), range: 0-1
df['Headline_WN_Sentiment'] = (df.Headline_WN_PosWordCount / (df.Headline_WN_PosWordCount + df.Headline_WN_NegWordCount)).fillna(0.5)
df['Detail_WN_Sentiment']   = (df.Detail_WN_PosWordCount / (df.Detail_WN_PosWordCount + df.Detail_WN_NegWordCount)).fillna(0.5)

## Display Time Taken (in seconds) To Complete The Feature Generation
print("--- NLP Features Generation Took: %s minutes ---" % ((time.time() - start_time)/60))

## Save To Local File
df.to_csv('data/phase1_news_sentiment.csv')