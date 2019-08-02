import sys
import os;
import datetime;
import pandas as pd
import numpy as np
import urllib.request


FOOTBALL_DATA_FIXTURES_URL = 'http://www.football-data.co.uk/fixtures.csv'
FOOTBALL_DATA_RESULTS_BASE_URL = 'http://www.football-data.co.uk/mmz4281/'
DATA_RAW_PATH = '../content/'
DATA_PREPROCESSED_PATH = '../content/preprocessed/'
DATA_PREPROCESSED_DEFAULT_FILE = '../content/preprocessed/preprocessed.csv'

def fetch_file( url, file ) :
    "Downloads the url to the specified local file"
    print("Fecthing File Url: " + url + " To File: " + file )
    urllib.request.urlretrieve( url, file)
    return;

def fetch_files( leagues, startyear, endyear):
    "Downloads the football data leagues specified for the start and end year"
    for i in range( startyear, endyear) :
        season = format( i, '02d') + format( i+1, '02d')
        for league in leagues :
            url = common.FOOTBALL_DATA_RESULTS_BASE_URL + season + "/" + league + ".csv"
            file = common.DATA_RAW_PATH + league + "_" + season + ".csv"
            fetch_file(url, file)
    return;

def delete_files( directory ):
    "Deletes the files in the specified directory"
    print("Deleting previous fetched files: " + directory )
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".csv")]
    for file in files:
        os.remove( file )
    return;

def fetch( leagues = ["E0", "E1", "E2", "E3"], startyear = 0, endyear = 16) :
    "Fetch the dataset for all English leagues between 2000 - 2016 ( by default) "
    try:
        print("Fetching File for Leagues: "  + str(leagues) +  " Start Year: " + str(startyear) + " End year: " + str(endyear) )
        delete_files( DATA_RAW_PATH )
        fetch_files( leagues, startyear, endyear)
    except Exception as e:
        print( e )
    return;
    
def season_year( row ) : 
    year = row.MatchDate.year
    month = row.MatchDate.month
    if month < 8 :
        return year - 1;
    else:
        return year;
    
# Make need to take an itrerative approach at sonme point, slow but may be required 
#def add_football_features( df ) :
#    df["F_FTHomeWin"]= np.nan
#    df["F_FTDraw"]= np.nan
#    df["F_FTAwayWin"]= np.nan
    
#   for (i, row) in df.iterrows():
#        df.loc[i,"F_FTHomeWin"]= 1 if row.F_FTResult == "H" else 0
#        df.loc[i,"F_FTDraw"]= 1 if row.F_FTResult == "D" else 0
#        df.loc[i,"F_FTAwayWin"]= 1 if row.F_FTResult == "A" else 0
#    return df
    
def correct_football_dataframe( df ): 
    "corrects the data in the dataframe"
    # Full column names
    df.rename(columns={
                       "Date": "MatchDate",
                       "FTHG": "F_FTHomeGoals",
                       "FTAG": "F_FTAwayGoals",
                       "FTR":  "F_FTResult",
                       "HTHG": "F_HTHomeGoals",
                       "HTAG": "F_HTAwayGoals",
                       "HTR":  "F_HTResult",
                       "WHH":  "OddsHomeWin",    # Taking odds from William Hill as consistant in all years dataset
                       "WHD": "OddsDraw",
                       "WHA":  "OddsAwayWin",
                       }, inplace=True)
    
    
    # Add extra label data (breaking down some complex types to simple labels)
    df["SeasonYear"] = df.apply( lambda row: season_year(row), axis=1)
    df["SeasonMatchNo"] = range( 1, len(df) + 1 )
    df["OddsWinDiff"] = df.apply( lambda row: abs(row.OddsHomeWin-row.OddsAwayWin), axis=1)
    df["MatchNo"] = df.apply( lambda row: row.SeasonYear*10000+row.SeasonMatchNo, axis=1)
    df["FDIndex"] = df.apply( lambda row: row.Div+str(row.MatchNo), axis=1)
    df.set_index( "FDIndex", inplace=True)
    
    # Correct some misspelt teams
    df["HomeTeam"] = df["HomeTeam"].apply( lambda team: "Middlesbrough" if team == "Middlesboro" else team )
    df["AwayTeam"] = df["AwayTeam"].apply( lambda team: "Middlesbrough" if team == "Middlesboro" else team )
  
    # Add features
    df["F_FTHomeWin"] = df.apply(lambda row: 1 if row.F_FTResult == "H" else 0, axis=1)
    df["F_FTDraw"]    = df.apply(lambda row: 1 if row.F_FTResult == "D" else 0, axis=1)
    df["F_FTAwayWin"] = df.apply(lambda row: 1 if row.F_FTResult == "A" else 0, axis=1)
    df["F_HTHomeWin"] = df.apply(lambda row: 1 if row.F_HTResult == "H" else 0, axis=1)
    df["F_HTDraw"]    = df.apply(lambda row: 1 if row.F_HTResult == "D" else 0, axis=1)
    df["F_HTAwayWin"] = df.apply(lambda row: 1 if row.F_HTResult == "A" else 0, axis=1)
    
    
    #df = add_football_features( df )
  
    return df

def read_csv_date( x ):
    try:
        if len(x) > 8:
            return pd.datetime.strptime(x, "%d/%m/%Y")
        else:
            return pd.datetime.strptime(x, "%d/%m/%y")
    except:
        return datetime.datetime(1970,1,1,0,0)
    
def read_footballdata_csv_file(file="../data/raw/E0_1011.csv" ) :
    "Read the Football data csv file"
    print("Reading Football Data Csv file: " + file )
    fields = ["Div","Date","HomeTeam","AwayTeam","FTHG","FTAG","FTR","HTHG","HTAG","HTR","WHH","WHD","WHA"]
    #dateparselambda = lambda x: pd.datetime.strptime(x, "%d/%m/%Y") if len(x) > 8 else pd.datetime.strptime(x, "%d/%m/%y")
    dateparselambda  = lambda x : (read_csv_date(x))
    df = pd.read_csv( file, error_bad_lines = False, keep_default_na = True, na_filter = True, na_values = '', skip_blank_lines = True, parse_dates = ["Date"], date_parser=dateparselambda, encoding = "ISO-8859-1", usecols=fields)
    df.dropna(inplace=True)
    correct_football_dataframe(df)
    return df

def read_footballdata_csv_directory(directory= DATA_RAW_PATH ) :
    "Read Football data csv directory of file"
    print("Reading Football Data Csv Directory: " + directory )
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".csv")]
    df = pd.DataFrame()
    for file in files:
        df = df.append( read_footballdata_csv_file( file ))
    return df

def write_preprocessed_result( dataframe, file = DATA_PREPROCESSED_DEFAULT_FILE ) :
    "Writes the preprocessed results to the specified file"
    print("Writing Preprocessed Results File: " + file )
    dataframe.to_csv( file )
    return

def preprocess ( raw_directory = DATA_RAW_PATH, result_file = DATA_PREPROCESSED_DEFAULT_FILE) :
    "processes the specified raw directory path and puts the results in the results file"
    df = read_footballdata_csv_directory( raw_directory )
    print( "Sorting Data...")
    df.sort_values( by = ["MatchDate", "Div"], ascending = [1,1], inplace = True)
    print( "Re-Order...") # NOTE Div is considered the index so not included
    df = df[[  "Div", "MatchNo", "SeasonYear", "SeasonMatchNo", "MatchDate","HomeTeam","AwayTeam","OddsHomeWin","OddsDraw","OddsAwayWin","OddsWinDiff","F_FTHomeGoals","F_FTAwayGoals","F_FTResult","F_FTHomeWin","F_FTDraw", "F_FTAwayWin", "F_HTHomeGoals","F_HTAwayGoals","F_HTResult", "F_HTHomeWin","F_HTDraw", "F_HTAwayWin"]]
    print( "Writing Output...")
    write_preprocessed_result( df, result_file)
    return
