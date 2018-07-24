# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 20:32:54 2018

@author: antonio constandinou
"""

# CALCULATE HALF LIFE FOR HURST MEAN REVERTING STOCKS

import datetime
import numpy as np
import pandas as pd
import os
import psycopg2
import statsmodels.api as sm
import math
import matplotlib.pyplot as plt


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


def load_txt_file_array(f_name):
    """
    return an array of strings from input text file
    args:
        f_name: file name as string
    returns:
        array
    """
    cur_path = os.getcwd() + f_name
    lines = open(cur_path).read().splitlines()
    
    return lines


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
    
    # load our hurst exponent output file for stocks that passed < 0.5
    file_name = "\\" + "he_stock_list_2010_12_30.txt"
    list_of_stocks = load_txt_file_array(file_name)
    
    # stocks whos half life is less than a criteria
    halflife_value_arr = []
    failed_tickers = []
    passed_tickers = []
    min_hf_var = 50.0
    
    for ticker in list_of_stocks:
        # our SQL statement
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

        # create a lag of our data
        try:
            stock_data.replace([np.inf, -np.inf], np.nan)
            stock_data.fillna(0, inplace=True)
            stock_lag = stock_data['Adj_Close'].shift(1)
            stock_lag.at[1] = 1.0
            stock_returns = stock_data['Adj_Close'] - stock_lag
            stock_returns.at[1] = 1.0
            
            # run OLS regression
            # add constant to predictor
            stock_lag2 = sm.add_constant(stock_lag)
            # independent var (X) = stock_lag2, dependent var (Y) = stock_returns
            # speed of MR = slope
            model = sm.OLS(stock_returns, stock_lag2)
            res = model.fit()
            
            # divide by speed of MR
            halflife = round(-np.log(2))/res.params[1]
            
            print("{0} Halflife: {1}".format(ticker, halflife))
            if halflife > 0.0:
                halflife_value_arr.append(halflife)
                if halflife > min_hf_var:
                    failed_tickers.append(ticker)
                else:
                    passed_tickers.append(ticker)
        except:
            print("Failed at {}".format(ticker))
            
    # output our results of those above or below our threshold
    write_results_text_file('halfL_failed_tickers.txt', failed_tickers)
    write_results_text_file('halfL_passed_tickers.txt', passed_tickers)
    
    mean = np.mean(halflife_value_arr)
    median = np.median(halflife_value_arr)
    count = len(halflife_value_arr)
    std_dev = np.std(halflife_value_arr)
    
    print("Total: {}".format(count))
    print("Median: {}".format(median))
    print("Mean: {}".format(mean))
    print("Std. Dev: {}".format(std_dev))
    
    plt.hist(halflife_value_arr, bins = 20, edgecolor='black')

if __name__ == "__main__":
    main()