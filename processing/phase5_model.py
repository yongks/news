## Author: : Yong Keh Soon
## Date: 2020-04-30
##
## Map Sentiment Score to DOWN/STAY/UP
## Construct Baseline Performance
## Construct Model with Confusion Table
## Calcualte Accuracy, Precision, Recall and F1 Means
## Average Metric
## Evaluate Model Performance 
## Process will take about 20m

### Import Remaining Library
import pandas as pd
from numpy import arange
import time 

## Evaluation
from sklearn.metrics import confusion_matrix

# Change Directory to Project Root
import os
os.chdir('../')

## Display Environment
pd.set_option( 'display.notebook_repr_html', True)  # render Series and DataFrame as text, not HTML
pd.set_option( 'display.max_column', 25)    # number of columns
pd.set_option( 'display.max_rows', 100)      # number of rows
pd.set_option( 'display.width', 300)        # number of characters per row

## Turn Off All Warnings
import warnings
warnings.filterwarnings('ignore')

#%% Load Data

fsi_symbols   = ['MBBM.KL', 'PUBM.KL', 'CIMB.KL', 'HLBB.KL', 'RHBC.KL', 'HLCB.KL', 'AMMB.KL', 'BIMB.KL', 'LOND.KL', 'MBSS.KL']
telco_symbols = ['MXSC.KL', 'DSOM.KL', 'AXIA.KL', 'TLMM.KL', 'ASTR.KL', 'TCOM.KL', 'GRNP.KL', 'OCKG.KL', 'MDCH.KL', 'STAR.KL']
fsi_symbols_index   = fsi_symbols    + ['FSI_INDEX']
telco_symbols_index = telco_symbols  + ['TELCO_INDEX']
all_symbols   = fsi_symbols + telco_symbols + ['FSI_INDEX','TELCO_INDEX']

## Read EOD Returns From Local CSV File
returns_df    = pd.read_csv('data/phase4_returns.csv', header=[0,1], index_col=0).filter(regex='RD00|RD01|RD02|RD03|RD04|RD05')
fsi_returns   = returns_df.loc[:, fsi_symbols_index]
telco_returns = returns_df.loc[:, telco_symbols_index]
#.filter(regex='RD00|RD01|RD02|RD03|RD04|RD05|OPEN|CLOSE')  .reindex(['OPEN','CLOSE', 'RD00','RD01','RD02','RD03','RD04','RD05'], level=1, axis=1)

## Read Sentiment Scores From Local File
sentiment_df    = pd.read_csv('data/phase3_sentiment_scores.csv', index_col=0)
fsi_sentiment   = sentiment_df.loc[:, fsi_symbols_index]
telco_sentiment = sentiment_df.loc[:, telco_symbols_index]

return_cols = ['RD00','RD01','RD02','RD03','RD04','RD05']

#%% Create Labels

def ReturnDirection(num, th=0.5):
    if pd.isna(num):   return num
    elif num > th:     return 'UP'
    elif num < -1*th:  return 'DOWN'
    else:              return 'STAY'
    
returns_dir       = returns_df.loc[:,    (slice(None), ('RD00','RD01','RD02','RD03','RD04','RD05'))].applymap(ReturnDirection)
fsi_returns_dir   = returns_dir[fsi_symbols_index]
telco_returns_dir = returns_dir[telco_symbols_index]

#%% Prediction Modeling Through Grid Search

## senti_score is within range of 0 to 1
def SentiDirection(senti_score, th, center):
    upper_boundary = center + th
    lower_boundary = center - th
    if pd.isna(senti_score): return senti_score
    elif senti_score > upper_boundary: return 'UP'
    elif senti_score < lower_boundary: return 'DOWN'
    else:                              return 'STAY'

def cm_eval(actual, predicted):
    result = confusion_matrix(actual, predict)
    ACCURACY       = (result[0,0] + result[1,1] + result[2,2])/ result.sum()
    DOWN_PRECISION =  result[0,0] / (result[0,0] + result[1,0] + result[2,0])
    DOWN_RECALL    =  result[0,0] / (result[0,0] + result[0,1] + result[0,2])
    STAY_PRECISION =  result[1,1] / (result[0,1] + result[1,1] + result[2,1])
    STAY_RECALL    =  result[1,1] / (result[1,0] + result[1,1] + result[1,2])
    UP_PRECISION   =  result[2,2] / (result[0,2] + result[1,2] + result[2,2])
    UP_RECALL      =  result[2,2] / (result[2,0] + result[2,1] + result[2,2])
    DOWN_F1        =  2* (DOWN_PRECISION*DOWN_RECALL) / (DOWN_PRECISION+DOWN_RECALL)
    STAY_F1        =  2* (STAY_PRECISION*STAY_RECALL) / (STAY_PRECISION+STAY_RECALL)
    UP_F1          =  2* (UP_PRECISION*UP_RECALL) / (UP_PRECISION+UP_RECALL)
    return ACCURACY.round(3), DOWN_PRECISION.round(3), DOWN_RECALL.round(3), \
           STAY_PRECISION.round(3), STAY_RECALL.round(3), UP_PRECISION.round(3), UP_RECALL.round(3),\
           DOWN_F1.round(3), STAY_F1.round(3), UP_F1.round(3)

## Grid Search
centers    = arange(0.1, 1.0, 0.1)  ## From 0.1 to 0.9
thresholds = arange(0.01, 1.0, 0.01)  ## From 0.01 to 0.99

result = []
start_time = time.time()
## For Every Centers
for c in centers.tolist():
    ## For Every Threshold
    for t in thresholds.tolist():
        PARAM = 'C%.2f'%c + '-T%.2f'%t
        CENTER = c
        THRESHOLD = t
        print('Processing Model: ', PARAM)
        sentiment_dir = sentiment_df.applymap(lambda x: SentiDirection(senti_score=x, th=t, center=c))
        ## For Every Symbols
        for sym in all_symbols:
            for col in return_cols:
                actual    = returns_dir[sym][col]
                predicted = sentiment_dir[sym]
                if sym in fsi_symbols:
                    SECTOR = 'FS'
                if sym in telco_symbols:
                    SECTOR = 'TELCO'
                if sym in ['FSI_INDEX']:
                    SECTOR = 'FS_INDEX'
                if sym in ['TELCO_INDEX']:
                    SECTOR = 'TELCO_INDEX'
                model    = pd.concat([actual, predicted], axis=1).dropna()
                actual   = model.iloc[:,0]
                predict  = model.iloc[:,1]
                ACCURACY, DOWN_PRECISION, DOWN_RECALL, STAY_PRECISION, STAY_RECALL, UP_PRECISION, UP_RECALL, DOWN_F1, STAY_F1, UP_F1 = cm_eval( actual, predict)
                result_dict  = { 
                    'PARAM'  : PARAM, 'CENTER': c, 'THRESHOLD':t,
                    'SYMBOL' : sym, 'SECTOR': SECTOR,
                    'MODEL': col,     'ACCURACY':    ACCURACY, 
                    'DOWN_PRECISION': DOWN_PRECISION, 'DOWN_RECALL': DOWN_RECALL,
                    'STAY_PRECISION': STAY_PRECISION, 'STAY_RECALL': STAY_RECALL, 
                    'UP_PRECISION':   UP_PRECISION,   'UP_RECALL':   UP_RECALL,
                    'DOWN_F1':        DOWN_F1,  'STAY_F1': STAY_F1, 'UP_F1': UP_F1}
                result = result + [result_dict]
                
print("--- Grid Search Parameters Took: %s minutes ---" % ((time.time() - start_time)/60))

complete_result = pd.DataFrame(result).dropna()
complete_result .to_csv('results/complete_result.csv')

#%% Construct Baseline For Individual Stock

## Get Frequency Count and Probability Of Labels
freq = returns_dir.unstack().droplevel(2).groupby(['RIC','COLUMN']).value_counts().swaplevel().unstack()
freq_total = freq.reset_index().groupby(['RIC']).sum()

## Calculating Baseline
f1_baseline       = freq.div(freq_total, axis=0).mean(axis=1).unstack().round(4).rename(columns={'DOWN': 'DOWN_F1', 'STAY': 'STAY_F1','UP': 'UP_F1'}).loc[all_symbols, :]*100
accuracy_baseline = freq.div(freq_total, axis=0).groupby('RIC').max().mean(axis=1).to_frame().rename(columns= {0:'Accuracy_Baseline'}).round(4)*100

#%% Get Best Results For Groups

group_result = complete_result.groupby(['PARAM','SECTOR'], as_index=False).mean()

metrics = ['ACCURACY','DOWN_F1','STAY_F1','UP_F1']
focus = ['FS','TELCO','FS_INDEX','TELCO_INDEX']

best_results = []
for f in focus:
    focus_df = group_result.query('SECTOR=="%s"'%f)
    for m in metrics:
        idmax = focus_df[m].idxmax()
        result_dict = { 
            'SECTOR': f,
            'METRIC': m, 
            'PARAM':  group_result.iloc[idmax].PARAM,
            'VALUE':  group_result.iloc[idmax][m]
        }
        best_results = best_results + [result_dict]

best_group_result = pd.DataFrame(best_results)\
                    .set_index(['SECTOR','METRIC'])\
                    .unstack()
                    
best_group_result.to_csv('results/best_group_result.csv')
print (best_group_result)

#%% Get Best Results For Individual Stock

stock_result = complete_result.groupby(['PARAM','SYMBOL'], as_index=False).mean()

metrics = ['ACCURACY','DOWN_F1','STAY_F1','UP_F1']
focus = ['FS','TELCO','FS_INDEX','TELCO_INDEX']

best_results = []
for sym in all_symbols:
    focus_df = stock_result.query('SYMBOL=="%s"'%sym)
    for m in metrics:
        idmax = focus_df[m].idxmax()
        result_dict = { 
            'SYMBOL': sym,
            'METRIC': m, 
            'PARAM':  stock_result.iloc[idmax].PARAM,
            'VALUE':  stock_result.iloc[idmax][m]
        }
        best_results = best_results + [result_dict]

best_stock_result = pd.DataFrame(best_results)\
                    .set_index(['SYMBOL','METRIC'])\
                    .unstack()\
                    .sort_index()\
                    .rename(columns={'VALUE':'MODEL_PERFORMANCE'})\
                    .loc[all_symbols, :]

## Conver Metrics to to 100%
best_stock_result.MODEL_PERFORMANCE = best_stock_result.MODEL_PERFORMANCE.round(4)*100

## Consolidate report with Baseline
best_stock_result_with_baseline = best_stock_result.join(accuracy_baseline).join(f1_baseline)
best_stock_result_with_baseline.to_csv('results/best_stock_result_with_baseline.csv')
print( best_stock_result_with_baseline)

