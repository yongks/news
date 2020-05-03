#######################################################################################
### Module: EikonDatabase
### Date: 5 Jan 2020
### Features:  - Fetch Stock Price, Outstanding Stocks Info from Thomson Reuters Eikon  
###            - Support Corporate Action Records
###            - Minimize Eikon API call to Single call per Symbol
###            - Support selective Symbol update, with optional save to disk
###            - Price and Outstanding Stocks are Adjusted
###            - Support date range fetch and filter
###            - Save to Local Disk
#######################################################################################

import pandas as pd
import datetime as dt
import configparser as cp
import eikon as ek

#import warnings
#warnings.simplefilter(action='ignore', category=FutureWarning)
import warnings
warnings.filterwarnings("ignore")

## Reading Directory Path From Config
config = cp.ConfigParser()
config.read('../settings.cfg')

## Reading File Path From Config
instruments_file    = config['data']['instruments_file']
listing_db          = config['data']['listing_db']
eod_db_adjusted     = config['data']['eod_db_adjusted']
eod_db_unadjusted   = config['data']['eod_db_unadjusted']
corp_act_db         = config['data']['corp_act_db']
financial_db        = config['data']['financial_db']
eikon_api           = config['eikon']['api_key']

#%%
class EikonDatabase():
    
    UNIVERSE          = None
    EOD_DF            = pd.DataFrame()
    EOD_DF_UNADJUSTED = pd.DataFrame()
    FINANCIAL_DF      = pd.DataFrame()
    CORPACT_DF        = pd.DataFrame()
    LISTING_DF        = pd.DataFrame()

    TR_COLUMNS       = ['COMPANYSHARESOUTSTANDING','PE','PRICETOBVPERSHARE','VOLUME','OPENPRICE','HIGHPRICE','LOWPRICE','CLOSEPRICE']
    CALC_COLUMNS     = ['MARKETCAP']
    ALL_COLUMNS      = TR_COLUMNS + CALC_COLUMNS
    
    
    ################################################################
    ###   INIT
    ################################################################
    def __init__(self):
        
        try:
            print('EikonDatabase:  Loading Data From Hard Disk. Please Wait ...')
            #self.EOD_DF            = pd.read_csv(eod_db_adjusted ,  header=[0,1], index_col=0, parse_dates=True)
            #self.EOD_DF_UNADJUSTED = pd.read_csv(eod_db_unadjusted, header=[0,1], index_col=0, parse_dates=True)
            self.FINANCIAL_DF= pd.read_csv(financial_db ,  header=[0,1],    index_col=0, parse_dates=True)
            self.UNIVERSE    = self.FINANCIAL_DF.columns.get_level_values(0).unique().to_list()
            self.CORPACT_DF  = pd.read_csv(corp_act_db, index_col=0, parse_dates=['CACORPACTDATE'])
            self.LISTING_DF  = pd.read_csv(listing_db,  index_col=0)            
            self.ReportDatabaseStatus()
        except:
            print('EikonDatabase:  Unable to Load EOD Data')
            
    ################################################################
    ###   Report Database Overall Status
    ################################################################
    def ReportDatabaseStatus(self):
        print('')
        # sym_count = len(self.EOD_DF.columns.levels[0])            
        # min_date  = self.EOD_DF.index.min().date().isoformat()
        # max_date  = self.EOD_DF.index.max().date().isoformat()
        # print('EikonDatabase:  Adjusted EOD (EOD_DF)              - Symbols #: {:>5d}  {} > {}'.format(sym_count, min_date, max_date))

        # sym_count = len(self.EOD_DF_UNADJUSTED.columns.levels[0])
        # min_date  = self.EOD_DF_UNADJUSTED.index.min().date().isoformat()
        # max_date  = self.EOD_DF_UNADJUSTED.index.max().date().isoformat()
        # print('EikonDatabase:  UnAdjusted EOD (EOD_DF_UNADJUSTED) - Symbols #: {:>5d}  {} > {}'.format(sym_count, min_date, max_date))
        
        sym_count = len(self.FINANCIAL_DF.columns.levels[0])
        min_date  = self.FINANCIAL_DF.index.min().date().isoformat()
        max_date  = self.FINANCIAL_DF.index.max().date().isoformat()
        print('EikonDatabase:  Financial Data (FINANCIAL_DF)      - Symbols #: {:>5d}  {} > {}'.format(sym_count, min_date, max_date))

        sym_count = len(self.CORPACT_DF.RIC.unique())
        min_date = self.CORPACT_DF['CACORPACTDATE'].min().date().isoformat()
        max_date = self.CORPACT_DF['CACORPACTDATE'].max().date().isoformat()
        print('EikonDatabase:  Corp Act Data  (CORPACT_DF)        - Symbols #: {:>5d}  {} > {}'.format(sym_count, min_date, max_date))
    
        sym_count = len(self.LISTING_DF.index.unique())
        exc_count = len(self.LISTING_DF.EXCHANGEMARKETIDCODE.unique())
        print('EikonDatabase:  Stock Listing  (LISTING_DF)        - Symbols #: {:>5d}  Exchange #: {}'.format(sym_count, exc_count))
    
    
    ################################################################
    ###   Refresh CorpAct Data (CORPACT_DF)
    ################################################################
    def RefreshCorpAct(self, 
            symbols   = [], 
            date_from = '2000-01-01',
            date_to   = dt.date.today().isoformat(), 
            save=False,
            overwrite=False):        

        ## If single stock, convert to list first
        if (not type(symbols)==list): symbols = [symbols]
        
        current_universe = self.FINANCIAL_DF.columns.get_level_values(0).unique().to_list() +\
                           self.CORPACT_DF.RIC.to_list()
        
        ## Default to all Universe
        if len(symbols)==0:
            symbols = current_universe        
        
        ## Consider current universe for update
        elif (not overwrite):          
            symbols = list(set(symbols + current_universe))

        print('EikonDatabase:  Getting Corp Act Data.')
        df, err = ek.get_data( symbols,
                    ['TR.CACorpActDate','TR.CAExDate','TR.CAEffectiveDate',
                     'TR.CACorpActDesc','TR.CACorpActEventType','TR.CAAdjustmentType','TR.CAAdjustmentFactor',
                     'TR.CATermsOldShares','TR.CATermsNewShares','TR.CAOfferPrice'],
                    {'SDate':'%s'%date_from,'EDate':'%s'%date_to}, 
                    field_name=True)

        ## Tidy Up Column Names. Remove 'TR.' 
        df.columns = [ x[3:] if 'TR.' in x else x for x in df.columns]
        df.rename(columns={'Instrument':'RIC'}, inplace=True)
        
        ## Convert Date Columns, otherwise Report will fail
        date_cols = [ x for x in df.columns.to_list() if 'DATE' in x]
        df.loc[:, date_cols ] = df.loc[:, date_cols].applymap(pd.to_datetime)
        
        if (save==True):
            print('EikonDatabase:  Saved Corp Act.')
            self.CORPACT_DF = df
            self.CORPACT_DF.to_csv(corp_act_db)
            self.ReportDatabaseStatus()
        else:
            return df
  
    
    ################################################################
    ###   Get Corp Act Data  (CORPACT_DF)
    ################################################################
    def GetCorpAct(self, 
           symbols=None, 
           date_from = (dt.date.today()-dt.timedelta(days=365*20)).isoformat(),
           date_to   = dt.date.today().isoformat()):
        
        ## Return Entire DataFrame
        if (symbols==None):  
            return self.CORPACT_DF
        
        ## If single stock, convert to list first
        if (not type(symbols)==list): symbols = [symbols]
        
        return self.CORPACT_DF.query('RIC == @symbols')
    
    
    ################################################################
    ###   Refresh Lisiting Data For An Exchange (LISTING_DF)
    ###     Included Dual Listing Counters
    ################################################################    
    def RefreshListing(
            self, exchanges=['XKLS'], 
            save=True, 
            overwrite=False):
             
        tr_company_fields = ['TR.RIC','TR.CommonName','TR.ISIN','OFFCL_CODE','TR.SEDOLCODE','TR.ExchangeMarketIdCode','TR.ExchangeTicker',
                             'TR.TRBCEconomicSector', 'TR.TRBCBusinessSector','TR.TRBCIndustry',
                             'TR.GICSSector','TR.GICSIndustryGroup','TR.GICSIndustry','TR.GICSSubindustry',
                             'TR.TRBCIndustryGroup', 'TR.TRBusinessSummary', 'CF_EXCHNG', 
                             'TR.CompanyIncorpDate','TR.IPODate','TR.CompanyPublicSinceDate','TR.FirstTradeDate',
                             'TR.CompanySharesOutstanding','TR.CompanyMarketCap','TR.SharesFreeFloat', 'CF_CLOSE','CF_DATE']
            
        if (not type(exchanges)==list):
            exchanges = [exchanges]
        
        ## If not overwriting, Fetch Listing for Existing Exchanges Too
        if (not overwrite):
            exchanges = list(set(exchanges + self.LISTING_DF.EXCHANGEMARKETIDCODE.unique().tolist()))
        
        ## Fetch Listing for Each Exchange
        df = pd.DataFrame()
        for exc in exchanges:
            print('EikonDatabase:  Fetching Listing Data  -  Exchange: {}'.format(exc))   
            temp_df, err = ek.get_data('SCREEN(U(IN(Equity(active,public,primary,countryprimaryquote))/*UNV:Public*/), \
                        IN(TR.ExchangeMarketIdCode,"{}"))'.format(exc),
                        tr_company_fields, field_name = True)
            df = pd.concat([df, temp_df]  , axis=0)

        ## Remove 'TR.' from column names
        df.columns = [ x[3:] if 'TR.' in x else x for x in df.columns]
        ## remove First column 'Instrument', as it is redundant at 'RIC' column, make it index
        df = df.iloc[:, 1:].set_index('RIC')
    
        if(save):
            ## Assign to Memory then save
            print('EikonDatabase:  Saving Listing Data.')
            self.LISTING_DF = df
            df.to_csv(listing_db)
            self.ReportDatabaseStatus()
        else:
            return df
    
    
    ################################################################
    ###   Refresh Financial Data
    ###     overwrite : discard all existing data, fetch new one
    ################################################################
    def RefreshFinancial(self,
            symbols   = None, 
            date_from = '2010-01-01',
            date_to   = dt.date.today().isoformat(), 
            adjusted  = '1',
            save      = False,
            overwrite = False):    
    
        ## Connect to Eikon 
        ek.set_app_key(eikon_api)
        
        ## If single stock, convert to list first
        if (not type(symbols)==list): symbols = [symbols]
        
        ## Loop Through Every Symbol
        df = pd.DataFrame()
        i = 1
        for sym in symbols:
            print('EikonDatabase:  Fetching Financial Data  {:>4d}/{:<4d}  :  {:7<}  From {} > {}'.format(i, len(symbols), sym, date_from, date_to))
            i = i + 1

            ## Fetch The Data
            temp_df, err = ek.get_data([sym],
                      ['TR.CompanySharesOutstanding','TR.CompanySharesOutstanding.date',
                       'TR.PE.date',               'TR.PE',
                       'TR.PriceToBVPerShare',     'TR.PriceToBVPerShare.date',
                       'TR.VOLUME.date',           'TR.VOLUME',
                       'TR.OPENPRICE.date',        'TR.OPENPRICE', 
                       'TR.HIGHPRICE.date',        'TR.HIGHPRICE',
                       'TR.LOWPRICE.date',         'TR.LOWPRICE',
                       'TR.CLOSEPRICE.date',       'TR.CLOSEPRICE'],
                      {'SDate':date_from,'EDate':date_to,  'Adjusted':adjusted}, field_name=True)
           
            ## Fix Header: Remove 'TR'
            temp_df.columns = [ x[3:] if 'TR.' in x else x for x in temp_df.columns]
            #temp_df.columns = [ x[1:] if '.'== x[0] else x for x in temp_df.columns]
            ## Loop Through Each Columns For This Stock
            temp_df1 = pd.DataFrame()
            for col in self.TR_COLUMNS:
                temp_df2         = temp_df.loc[ : , [col+'.DATE', col]]           ## Get the Data For This Column
                temp_df2         = temp_df2.rename(columns={col+'.DATE':'Date'})  ## Rename Column 'Date' 
                if (temp_df2.isna().sum()[0] == len(temp_df2)):
                    ## This Symbol does not have this column
                    continue
                else:
                    temp_df2['Date'] = temp_df2['Date'].apply(lambda x: x[:10])       ## Keep Date String Only - Ommit Time
                    temp_df2         = temp_df2.dropna().drop_duplicates().groupby('Date').first()                      ## Remove duplicates
                    temp_df1         = temp_df1.merge(temp_df2,                       ## Append To Overall DataFrame For This Stock
                                         left_index  = True,
                                         right_index = True, how='outer')
            
            ## All Comlumns For This Tock Has Been Added. Remove Rows without Pricing
            clean_mask = temp_df1.loc[:, 'OPENPRICE':'CLOSEPRICE'].dropna(how='all').index
            temp_df1 = temp_df1.loc[clean_mask, : ]
            
            ## Treat Missing Value, and Calculate Additional Columns
            if 'COMPANYSHARESOUTSTANDING' in temp_df1.columns:
                temp_df1['COMPANYSHARESOUTSTANDING'] = temp_df1.COMPANYSHARESOUTSTANDING.ffill()
                temp_df1['MARKETCAP'] = temp_df1.CLOSEPRICE * temp_df1.COMPANYSHARESOUTSTANDING
            
            ## Add RIC code into Header, Then Merge Into Final DataFrame
            #full_columns = self.TR_COLUMNS + self.CALC_COLUMNS
            full_columns = temp_df1.columns
            temp_df1.columns = pd.MultiIndex.from_arrays([ [sym]*len(full_columns), full_columns])
            df = df.merge(temp_df1, left_index=True, right_index=True, how='outer')

        ## All Stocks Data Are Complete            
        df.columns = pd.MultiIndex.from_tuples(df.columns, names=('RIC','COLUMN'))
        ## Replace will construct a Complete DataFrame with Existing and New Data
        if (overwrite):
            print('EikonDatabase:  Financial Data - Removing All Existing Symbols From Memory')
            self.FINANCIAL_DF = df
        else:
            print('EikonDatabase:  Financial Data - Removing Overlapping Symbols From Memory')
            ## remove overlapping symbols from existing DataFrame
            existing_set   = set(self.FINANCIAL_DF.columns.get_level_values(0).to_list())
            remove_symbols = list(existing_set.intersection(symbols))
            ## Remove Overlapping Symbols if detected
            if len(remove_symbols)>0:
                self.FINANCIAL_DF = self.FINANCIAL_DF.drop(columns=remove_symbols, level=0)
                #df = pd.merge(temp_df, df, how='outer', left_index=True, right_index=True)
            self.FINANCIAL_DF = self.FINANCIAL_DF.merge(df, how='outer',left_index=True, right_index=True)
            
        ## If Save, update the DataFrame and save to Local File
        ##   Else return as DataFrame
        if(save):
            ## Assign to Memory
            print('EikonDatabase:  Financial Data - Saving To Local Disk')
            self.UNIVERSE = self.FINANCIAL_DF.columns.get_level_values(0).unique()
            self.FINANCIAL_DF.to_csv(financial_db)
        
        self.ReportFinancialStatus()
        
             
    ################################################################
    ###   Return Financial By Symbol(s))
    ################################################################
    def GetFinancialBySymbols(self, 
               symbols   = [], 
               date_from = '2000-01-01',
               date_to   = dt.date.today().isoformat(), 
               columns   = []):
                  
        ## Convert Symbols to List if it is not
        if (not type(symbols)==list): symbols = [symbols]

        ## Default Symbols to All Tickers if not specified
        if len(symbols)==0:
            symbols = self.FINANCIAL_DF.columns.get_level_values(0).unique().to_list()
        
        ## Convert Columns to List if it is not
        if (not type(columns)==list): columns = [columns]

        ## Default Columns if not specified
        if len(columns)==0:
            columns = self.ALL_COLUMNS
        
        ## Reorder Symbols and Return
        df = self.FINANCIAL_DF.loc[date_from:date_to, pd.IndexSlice[symbols,columns]]\
                 .reindex(symbols, level=0, axis=1)\
                 .reindex(columns, level=1, axis=1)\
                 .dropna(how='all')
           
        ## return final data
        return df
    
    
    ################################################################
    ###   Return Financial By Column(s)
    ################################################################  
    def GetFinancialByColumns(self,
                columns   = [],
                date_from = '2000-01-01',
                date_to   = dt.date.today().isoformat(),
                symbols   = []):
        
        ## Get The Data
        df = self.GetFinancialBySymbols(symbols, date_from, date_to, columns)
        ## Swap Columns 0,1 and Return
        df.columns = df.columns.swaplevel(0,1)
        ## Reorder Columns and Return        
        return df.reindex(columns, level=0, axis=1)\
                 .reindex(symbols, level=1, axis=1)
    
    
    ################################################################
    ###   Return All Financial Symbols
    ################################################################  
    def GetFinancialSymbols(self):
        ## Get The Data
        symbols = self.FINANCIAL_DF.columns.get_level_values(0).to_list()
        print('EikonDatabase:  Financial Data - Total Symbols: {:4d}'.format(len(symbols)))
        return symbols
    
    
    ################################################################
    ###   Report Database Status
    ################################################################       
    def ReportFinancialStatus(self):
        
        ## Calculate First and Last Date
        df1 = self.FINANCIAL_DF.stack(level=0).swaplevel().reset_index().groupby('RIC').agg(
            min_date= ('Date', lambda x: x[x.dropna().index[0]]),
            max_date= ('Date', lambda x: x[x.dropna().index[-1]]))
        ## Calcuate None NA Rows
        df2 = self.FINANCIAL_DF.stack(level=0).swaplevel().reset_index().groupby('RIC')\
                    .apply(lambda x: (~x.isna()).sum())\
                    .filter(items=self.ALL_COLUMNS)
        ## return merged data
        return df1.merge(df2, left_index=True, right_index=True, how='inner')
    
    
    ## Update Database with list of symbols for both Adjusted and Unadjusted
    ##  default to last 10 years from current date
    ##  
    def RefreshPricing(self, 
            symbols   = None, 
            date_from = '2000-01-01',
            date_to   = dt.date.today().isoformat(), 
            save      = False,
            replace   = False):
        
        ## Connect to Eikon 
        ek.set_app_key(eikon_api)

        ## If single stock, convert to list first
        if (not type(symbols)==list): symbols = [symbols]
    
        ## Loop through each symbol to get the final DataFrame
        df_adjusted = pd.DataFrame()
        df_unadjusted = pd.DataFrame()
        
        ## Build The Pair Of Dates (In String) with 10Y Gap
        ##  replace last date with current day, To Avoid Count=-1 column output
        decades = (dt.date.fromisoformat(date_to).year - 2000) // 10 + 1  ## how many periods of 10 years, since 2000
        dt_range1 = pd.date_range('2000-01-01', freq='10YS', periods=decades).format()
        dt_range2 = pd.date_range('2000-01-01', freq='10Y',  periods=decades).shift(periods=9, freq='Y').format()
        dt_range2[-1] = date_to

        ## Initialize
        unrecoverable_symbols = []
        i = 0

        for sym in symbols:
            i = i+1  

            df_range_adjusted   = pd.DataFrame()
            df_range_unadjusted = pd.DataFrame()
            
            ## Loop N times fore ach symbol, N is gap of 10 years
            for decade in range(0,decades):    
                dt1 = dt_range1[decade]
                dt2 = dt_range2[decade]
                
                ## Attempt 10 times getting data from Eikon, catering for error Retry
                temp_range_adjusted = pd.DataFrame()
                temp_range_unadjusted = pd.DataFrame()
                
                ## Try To Fetch Data 10x on this symbol
                for attempt in range(10):   
                    try:
                        print('EikonDatabase:  Fetching EOD Data  {:4>}/{:4>}  :  {:7<}  From {} > {}'.format(i, len(symbols), sym, dt1, dt2))
                        temp_range_adjusted   = ek.get_timeseries(sym, start_date=dt1, end_date=dt2, corax=  "adjusted")
                        temp_range_unadjusted = ek.get_timeseries(sym, start_date=dt1, end_date=dt2, corax="unadjusted")        
                    
                    ## If failure due to No Available Data, move to next time range, else try again
                    except Exception as err:
                        
                        if ('No data available for the requested date range' in err.message or
                            'Invalid RIC' in err.message):
                            break
                        else:
                            continue

                    ## Attempt successful, acquire the data for this symbol
                    else: 
                        df_range_adjusted   = pd.concat([df_range_adjusted,   temp_range_adjusted]  , axis=0)
                        df_range_unadjusted = pd.concat([df_range_unadjusted, temp_range_unadjusted], axis=0)
                        break

                else:
                    ## All 10 loops completed successfully, 
                    ###  Meaning we failed all the attempts - deal with the consequences
                    unrecoverable_symbols += [sym]
                    print('EikonDatabase:  {} Unrecoverable Error Processing Symbol- Skipping It'.format(sym))
                    continue
            
            ### Print Error Symbols if any
            if len(unrecoverable_symbols)>0:
                print('EikonDatabase:  Symbols Failure : {}'.format(unrecoverable_symbols))
                
            ## Consolidate multiple date range for this Symbol
            df_range_adjusted.columns   = pd.MultiIndex.from_product([[sym], df_range_adjusted.columns])
            df_range_unadjusted.columns = pd.MultiIndex.from_product([[sym], df_range_unadjusted.columns])
            
            ## Consolidate this Symbol into main dataframe
            df_adjusted   = pd.concat([df_adjusted,   df_range_adjusted]  , axis=1)
            df_unadjusted = pd.concat([df_unadjusted, df_range_unadjusted], axis=1)
        
        ## All Symbols Completed
        ## Rename the Headers, Save to local storage if chosen, and overwrite memory
        ##   Else return as a dictionary of both adjusted and unadjusted DF
        df_adjusted.columns.rename("RIC", level=0, inplace=True)
        df_unadjusted.columns.rename("COLUMN", level=1, inplace=True)

        ## Replace will construct A Complete DataFrame with Existing and New Data
        if (replace):
            ## remove relevant symbols from existing DataFrame
            eod_df            = self.EOD_DF.drop(columns=symbols, level=0)
            eod_df_unadjusted = self.EOD_DF_UNADJUSTED.drop(columns=symbols, level=0)
            ## merge with newly acquired dataframe columns
            df_adjusted   = pd.merge(eod_df,            df_adjusted,   how='outer', left_index=True, right_index=True)
            df_unadjusted = pd.merge(eod_df_unadjusted, df_unadjusted, how='outer', left_index=True, right_index=True)

        ## If Save, update the DataFrame and save to Local File
        ##   Else return as DataFrame
        if(save):
            ## Assign to Memory
            print('EikonDatabase:  Saving EOD Data with Full Replacement')
            self.EOD_DF            = df_adjusted
            self.EOD_DF_UNADJUSTED = df_unadjusted
            self.UNIVERSE          = self.EOD_DF.columns.get_level_values(0).unique()
            df_adjusted  .to_csv(eod_db_adjusted)
            df_unadjusted.to_csv(eod_db_unadjusted)
            self.ReportDatabaseStatus()
        else:
            return ({'EOD_ADJUSTED': df_adjusted, 'EOD_UNADJUSTED': df_unadjusted})

    ### Return Pricing of Symbols(s)
    ###  DataFrame returned always has a Symbol as Level1 Header
    def GetPricingBySymbols(self, 
               symbols=None, 
               date_from = '2000-01-01',
               date_to   = dt.date.today().isoformat(), 
               columns   = ['OPEN','CLOSE','HIGH','LOW','VOLUME'],
               adjusted  = True):
        
        ## Default Symbols to All Tickers if not specified
        if (symbols==None):
            if adjusted: symbols = self.EOD_DF.columns.get_level_values(0).to_list()
            else:        symbols = self.EOD_DF_UNADJUSTED.columns.get_level_values(0).to_list()
        
        ## Convert Symbols to List if it is not
        if (not type(symbols)==list):
            symbols = [symbols]
        
        #eikon.EOD_DF.loc[:, pd.IndexSlice[:,['CLOSE','OPEN']]]
        ## Select Adjusted or Non-Adjusted
        if adjusted:
            #df = self.EOD_DF.loc[date_from:date_to, symbols]
            df = self.EOD_DF.loc[date_from:date_to, pd.IndexSlice[symbols,columns]]
        else:
            df = self.EOD_DF_UNADJUSTED.loc[date_from:date_to, symbols]
            
        return df 
    
    ### Call GetPricingBySymbos
    ###  Then Swap The Two Headers
    ###  DataFrame returned always has a Symbol as Level 2 Header
    def GetPricingByColumns(self,
                symbols=None, 
                date_from = '2000-01-01',
                date_to   = dt.date.today().isoformat(),
                columns=['OPEN','CLOSE','HIGH','LOW','VOLUME'],
                adjusted = True):
        
        ## Get The Data
        df = self.GetPricingBySymbols(symbols, date_from, date_to, columns, adjusted)
        
        ## Swap Columns 0,1 and Return
        df.columns = df.columns.swaplevel(0,1)
        df.sort_index(axis=1, inplace=True)
        return df
    
