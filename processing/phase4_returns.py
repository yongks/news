## Author: : Yong Keh Soon
## Date: 2020-04-30
##
## Calculate Index Price For FS and TELCO
## Build Returns for RD00 to RD05

import sys
sys.path.insert(0, 'Modules')
from Modules.EikonDatabase import EikonDatabase
import pandas as pd

## Change Directory to Project Root
import os
os.chdir('../')

## Define Symbols In Scope
fsi_symbols   = ['MBBM.KL', 'PUBM.KL', 'CIMB.KL', 'HLBB.KL', 'RHBC.KL', 'HLCB.KL', 'AMMB.KL', 'BIMB.KL', 'LOND.KL', 'MBSS.KL']
telco_symbols = ['MXSC.KL', 'DSOM.KL', 'AXIA.KL', 'TLMM.KL', 'ASTR.KL', 'TCOM.KL', 'GRNP.KL', 'OCKG.KL', 'MDCH.KL', 'STAR.KL']
all_symbols   = fsi_symbols + telco_symbols + ['FSI_INDEX','TELCO_INDEX']

## Pricing Database
eik = EikonDatabase()

#%% Create Sector Index Price

date_from   = '2010-01-01'  # Last 10 years
date_to     = '2020-01-08'  # Overshot 8 days into 2020

### Construct FSI INDEX
fsi = eik.GetFinancialByColumns (
        symbols = fsi_symbols, 
        columns=['OPENPRICE','CLOSEPRICE','MARKETCAP'],
        date_from=date_from, date_to=date_to)

fsi_mcap_weight = fsi.MARKETCAP.div(fsi.MARKETCAP.sum(axis=1), axis=0)
fsi_open  = (fsi.OPENPRICE * fsi_mcap_weight).sum(axis=1)
fsi_close = (fsi.CLOSEPRICE * fsi_mcap_weight).sum(axis=1)
fsi_index = pd.concat([fsi_open, fsi_close], axis=1)
fsi_index.columns = [('FSI_INDEX','OPENPRICE'), ('FSI_INDEX','CLOSEPRICE')]

### Construct TELCO INDEX
telco = eik.GetFinancialByColumns (
        symbols = telco_symbols, 
        columns=['OPENPRICE','CLOSEPRICE','MARKETCAP'], 
        date_from=date_from, date_to=date_to)

telco_mcap_weight = telco.MARKETCAP.div(telco.MARKETCAP.sum(axis=1), axis=0)
telco_open  = (telco.OPENPRICE * telco_mcap_weight).sum(axis=1)
telco_close = (telco.CLOSEPRICE * telco_mcap_weight).sum(axis=1)
telco_index = pd.concat([telco_open, telco_close], axis=1)
telco_index.columns = [('TELCO_INDEX','OPENPRICE'), ('TELCO_INDEX','CLOSEPRICE')]

## Combine FSI and Telco Index Price into DataFrame
returns = eik.GetFinancialBySymbols(
        symbols = all_symbols, 
        columns=['OPENPRICE','CLOSEPRICE','MARKETCAP'], 
        date_from=date_from, date_to=date_to)

returns = pd.concat([returns, fsi_index, telco_index], axis=1)

#%% Create Return Columns

future_days = 5

## Create Future Return Columns into Each Symbol (Percentage)
for sym in all_symbols + ['FSI_INDEX', 'TELCO_INDEX']:
    temp_df = returns.loc[ :, sym ]
    for i in range (0,future_days+1):
        returns[sym, 'RD{:02d}'.format(i)] = ((temp_df.CLOSEPRICE.shift(-1*i) - temp_df.OPENPRICE) / temp_df.OPENPRICE)*100

## Sort On Symbol Name (Level 0)
returns = returns.sort_index(axis=1, level=0).round(2)\
      .loc[ date_from : date_to, : ]

returns.to_csv('data/phase4_returns.csv')
print('Returns Generated')