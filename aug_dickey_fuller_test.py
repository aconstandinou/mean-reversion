# -*- coding: utf-8 -*-
"""
Created on Mon Jul 16 19:21:12 2018

@author: antonio constandinou
"""

# AUGMENTED DICKER FULLEY TEST

import psycopg2
import pandas as pd
import datetime
import statsmodels.tsa.stattools as ts
import os


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


def print_results(result):
    """
    print augmented dickey fuller results
    args:
        result: return value from Augmented Dickey Fuller test, tuple
    returns:
        None
    """
    # calculated test-statistic
    print('ADF Statistic: %f' % result[0])
    # p-value
    print('p-value: %f' % result[1])
    # output other critical values at 1%, 5%, 10%
    print('Critical Values:')
    for key, value in result[4].items():
        print('\t%s: %.3f' % (key, value))    

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
    
    list_of_stocks = load_db_tickers_start_date(start_date, conn)
    
    # collections of tickers that pass our ADF test at various statistical levels
    mean_revert_ticker_arr1 = []
    mean_revert_ticker_arr5 = []

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
        
        # output results of Augemnted Dicky-Fuller test for each stock with lag order of 1
        # lags: 1 = daily, 4 = quarterly, 12 = monthly
        result = ts.adfuller(stock_data['Adj_Close'], 1)
    
        # Simple statement to reject or not reject null hypothesis
        if result[0] < result[4]['1%']:
            mean_revert_ticker_arr1.append(ticker)
            print('{0} passed on 1% statistic {1}'.format(ticker, result[4]['1%']))
        elif (result[0] < result[4]['5%']):
            mean_revert_ticker_arr5.append(ticker)
            print('{0} passed on 5% statistic {1}'.format(ticker, result[4]['5%']))
        else:
            print('{0} failed.'.format(ticker))
    
    write_results_text_file('mr_stocks_adf_1.txt', mean_revert_ticker_arr1)
    write_results_text_file('mr_stocks_adf_5.txt', mean_revert_ticker_arr5)
      
        
if __name__ == "__main__":
    main()