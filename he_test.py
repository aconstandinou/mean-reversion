# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 14:06:28 2018

@author: antonio constandinou
"""

# HURST EXPONENT

from numpy import cumsum, polyfit, log, sqrt, std, subtract
import numpy as np
import psycopg2
import pandas as pd
import datetime
import os
import matplotlib.pyplot as plt

def load_db_credential_info(f_name_path):
    """
    load text file holding our database credential info and the database name
    args:
        f_name_path: name of file preceded with "\\", type string
    returns:
        array of 4 values that should match text file info
    """
    cur_path = os.getcwd()
    # lets load our database credentials and info
    f = open(cur_path + f_name_path, 'r')
    lines = f.readlines()[1:]
    lines = lines[0].split(',')
    return lines


def load_db_tickers_start_date(start_date, conn):
    """
    return a list of stock tickers that have data on the start_date arg provided
    args:
        start_date: datetime object to be used to query or PostgreSQL database
        conn: a Postgres DB connection object
    returns:
        list of tuples
    """
    # convert start_date to string for our SQL query
    date_string = start_date.strftime("%Y-%m-%d")
    
    cur = conn.cursor()
    SQL =   """
            SELECT ticker FROM symbol
            WHERE id IN
              (SELECT DISTINCT(stock_id) 
               FROM daily_data
               WHERE date_price = %s)
            """
    cur.execute(SQL, (date_string,))        
    data = cur.fetchall()
    return data


def write_results_text_file(f_name, sub_array):
    """
    write an array to text file
    args:
        f_name: name of our file to be written with extension (.txt), type string
        sub_array: array of our data
    returns:
        None
    """
    # lets write elements of array to a file
    file_to_write = open(f_name, 'w')

    for ele in sub_array:
        file_to_write.write("%s\n" % ele) 


def hurst_exponent(data):
    """
    calculate HE on our time series data
    args:
        data: pandas dataframe of data
        lags: array of our lag range, default 2 to 20
    returns:
        float, HE value
    """
    lags = range(2,100)
    tau = [sqrt(std(subtract(data[lag:], data[:-lag]))) for lag in lags]
    m = np.polyfit(log(lags), log(tau), 1)
    hurst = m[0]*2
    return hurst
 
       
def main():
    # name of our database credential files (.txt)
    db_credential_info = "database_info.txt"
    # create a path version of our text file
    db_credential_info_p = "\\" + db_credential_info
    
    # create our instance variables for host, username, password and database name
    db_host, db_user, db_password, db_name = load_db_credential_info(db_credential_info_p)
    conn = psycopg2.connect(host=db_host,database=db_name, user=db_user, password=db_password)
    cur = conn.cursor()
    
    # we will need to filter our results to create a test sample
    start_date = datetime.date(2004,12,30)
    end_date = datetime.date(2010,12,30)
    
    list_of_stocks = load_db_tickers_start_date(start_date, conn)
    
    # collections of tickers that have HE < 0.5
    mean_revert_ticker_arr = []
    he_dist_all = []
    he_dist_mr = []
    
    for stock_ticker in list_of_stocks:
        # simple string version for our ticker
        ticker = stock_ticker[0]
        SQL = """
              SELECT date_price, adj_close_price 
              FROM daily_data 
              INNER JOIN symbol ON symbol.id = daily_data.stock_id 
              WHERE symbol.ticker LIKE %s
              """        
        cur.execute(SQL, (ticker,))
        # will return a list of tuples
        results = cur.fetchall()
        
        # convert query results to pandas dataframe
        stock_data = pd.DataFrame(results, columns=['Date', 'Adj_Close'])
        # change our data type in our Adj Close column
        stock_data['Adj_Close'] = stock_data['Adj_Close'].astype(float)
                
        mask = (stock_data['Date'] > start_date) & (stock_data['Date'] <= end_date)
        stock_data = stock_data.loc[mask]
        
        # calculate our Hurst Exponent on Adj. Close data as an array of values
        result = hurst_exponent(stock_data['Adj_Close'].values)
        he_dist_all.append(result)
        
        if result < 0.5:
            mean_revert_ticker_arr.append(ticker)
            he_dist_mr.append(result)
            print('{0} exhibits mean reversion with HE {1}'.format(ticker, result))
        else:
            print('{0} does not exhibit mean reversion.'.format(ticker))
    
    # write our list of MR tickers to our text file
    f_name = 'he_stock_list_' + end_date.strftime('%Y_%m_%d') + '.txt'
    write_results_text_file(f_name, mean_revert_ticker_arr)
    
    # output distribution of all HE
    plt.hist(he_dist_all, edgecolor='black')
    plt.title('Distribution of HE for all stocks')
    plt.xlabel('HE')
    plt.ylabel('Count')
    plt.show()

    # output distribution of all mean reverting HE
    plt.hist(he_dist_mr, edgecolor='black')
    plt.title('Distribution of HE for mean reverting stocks')
    plt.xlabel('HE')
    plt.ylabel('Count')
    plt.show()
    
    # calculate stats on all stocks
    mean_all = np.mean(he_dist_all)
    median_all = np.median(he_dist_all)
    count_all = len(he_dist_all)
    stdv_all = np.std(he_dist_all)
    print("Total stocks: {0}".format(count_all))
    print("Median: {0}".format(median_all))
    print("Mean: {0}".format(mean_all))
    print("St. Dev: {0}".format(stdv_all))
    
    print("-------------------------------------")
    # calculate stats on mean reverting stocks only
    mean_mr = np.mean(he_dist_mr)
    median_mr = np.median(he_dist_mr)
    count_mr = len(he_dist_mr)
    stdv_mr = np.std(he_dist_mr)
    print("Total MR stocks: {0}".format(count_mr))
    print("Median MR: {0}".format(median_mr))
    print("Mean MR: {0}".format(mean_mr))
    print("St. Dev: {0}".format(stdv_mr))
    
if __name__ == "__main__":
    main()