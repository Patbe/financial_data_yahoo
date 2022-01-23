import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup

# needed to deal with SSL Problems:
import ssl

import re
import datetime
import time
import sqlite3

text = str()
lst = list()
ticker = str()
url_new = str()


def create_database():

    # define some global variables

    global cur
    global conn
    global soup
    global lst
    global ticker
    

    conn = sqlite3.connect("stock_db.sqlite")
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS Counts")
    cur.execute("""
    CREATE TABLE Counts (stock_ticker TEXT, date TEXT, open INTEGER, high INTEGER, low INTEGER, close INTEGER, adj_close INTEGER, volume INTEGER)
    """)
    cur.close()

    index = 0

    for i in lst:


        i = i.split(",")
        print(i)
        date = i[0]
        open = i[1]
        high = i[2]
        low = i[3]
        close = i[4]
        adj_close = i[5]
        volume = i[6]
        index = index + 1
        print(index)

        cur = conn.cursor()
        cur.execute("INSERT INTO Counts (stock_ticker, date,open,high,low,close,adj_close,volume) VALUES (?,?,?,?,?,?,?,?)", (ticker,date,open,high,low,close,adj_close,volume))

        # commit writes the data to the database

        conn.commit()
        index = index + 1
    cur.close()


# Ignore SSL Certificate Errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE




def choose_ticker():
    global ticker
    print("Example stock tickers: SAP.DE, ADDYY, ADBE, VNM, MYTE, SPLK")
    ## kurze pause
    time.sleep(0.25)


    ticker = input("Please insert ticker")
    ticker = str(ticker)




## anchor_date
# This is the start date, in this case i called the variable anchor_date for lack of a better word at the time
anchor_date = datetime.date(1970,1,1)

## Startdate

d1 = input("type start date in the following format: YYYY-MM-DD")

# Split up string and put the elements into integer variables

d1 = d1.split("-")

year = d1[0]
year = int(year)
month = d1[1]
month = int(month)
day = d1[2]
day = int(day)

## Enddate

d2 = input("type in end date in the following format: YYYY-MM-DD")

# Split up string and put the elements into integer variables

d2 = d2.split("-")

year_endDate = d2[0]
year_endDate = int(year_endDate)
month_endDate = d2[1]
month_endDate = int(month_endDate)
day_endDate = d2[2]
day_endDate = int(day_endDate)

# put start and end dates into variables

date = datetime.date(year,month,day)

date_endDate = datetime.date(year_endDate,month_endDate,day_endDate)


## Difference from startdate to Timestamp 01.01.1970 in seconds

delta_start = abs((date - anchor_date).days)

#convert to days

delta_start = (int(delta_start)) * 24 * 60 * 60
#print("startdate:",delta_start)

## Difference from enddate to Timestamp 01.01.1970 in seconds

delta_end = abs((date_endDate - anchor_date).days)

## I added 64800 seconds to get the time after market close (around 6 pm), otherwise the last day would be missing

delta_end = ((int(delta_end)) * 24 * 60 * 60)+64800


## Next I need to destructure the string of the url

## URL to yahoo finance, save as string variable

url = "https://query1.finance.yahoo.com/v7/finance/download/SAP.DE?period1=1582886604&period2=1614509004&interval=1d&events=history&includeAdjustedClose=true"

# this variable contains the end of the url string, later I need to put it at the end of any new url string

end_of_url_string ="&interval=1d&events=history&includeAdjustedClose=true"


## Splitting up of the string into three parts to replace the parts that are needed for new tickers - using regular expressions

## String 1: use regular expressions \S - > single character other than white space, + -> one or more times, ? -> non-greedy, until download
first_text_part = re.findall("https:\S+?download/", url)
for i in first_text_part:
    text1 = i
##String 2: start with literal ?, any character one or more times
third_text = re.findall("\?.+", url)
for i in third_text:
    text2 = i


## third text (text 2 needs to split again). Start with literal S until &

period1 = re.findall("^\S+[0-9]+?&", text2)

## find the part with the numbers
for i in period1:
    period1string = str(i)

## split the string into two periods using the & in between

    periodsplit = period1string.split("&")

## split the first number part in the number and string component

    first_string = periodsplit[0]
    index = first_string.find("=")
    strPart_1 = first_string[:index+1]
    numPart_1 = first_string[index+1:]


## split the second  number part in the number and string component

    second_string = periodsplit[1]
    index2 = second_string.find("=")
    strPart_2 = second_string[:index2+1]
    numPart_2 = second_string[index2+1:]


## Recreate the date string with the user's dates

## Startperiod

new_start = strPart_1+str(delta_start)
new_end = strPart_2+str(delta_end)

# execute choose_ticker function with the ticker of the user

choose_ticker()

## Recreate the whole string

hp =text1+ticker+new_start+"&"+new_end+end_of_url_string

## set the new url
url_new = hp
print(url_new)

#download_stock_data()


html = urllib.request.urlopen(url_new, context=ctx).read()

soup = BeautifulSoup(html, "html.parser")

for line in soup:
    line = line.split("\n")
    #line = line.split(",")
    for item in line:
        print(item)

        lst.append(item)


create_database()
