import urllib
import requests
import re
import csv
import os
import datetime
from bs4 import BeautifulSoup
from pathlib import Path
workdir = 'F:\\Travis\\Documents\\pylearn\\pitchbook\\csvdata\\'
###EVERYTHING BELOW THIS LINE AND ABOVE THE OTHER SIMILAR LINE IS SPECIFIC TO THE DB WRITER
######### parameters
import pymysql
sql_db = pymysql.connect(host='webcrawl.cssprimgj6xm.us-west-2.rds.amazonaws.com',user='user',password='password',db='webcrawl',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
rowzero = True
readcsv = 'databox-2016-09-13 20-44.csv'   # when adding to web-crawl program, this will need to change
db_location_r = os.path.join(workdir,readcsv)
fr =  open(db_location_r,'r')
reader = csv.reader(fr,delimiter = ',')
db_keys = list()
#### Everything above this line will need to be planted into the web-crawl program for it to work
#this is where i live, for now all the files are being saved there as well
workdir = 'F:\\Travis\\Documents\\pylearn\\pitchbook\\csvdata\\'
db_location_r = os.path.join(workdir,readcsv)
### keys used to initally add data
keys = ('sitenum','Website_URL','HTTP_response_code','Contact_link_URL','Contact_link_text','Management_link_URL','Management_link_text','Manager_name','Manager_title','access_time')

def insert_into_contact(row, table_name):

    # Insert into contact table

    contact_link_url = row[3]
    contact_link_text = row[4]

    insert_sql = "INSERT IGNORE INTO "+ "contact (Contact_link_URL, Contact_link_text) VALUES ('" + contact_link_url + "', '" + contact_link_text + "')"
    select_sql = "SELECT LAST_INSERT_ID()"

    print insert_sql
    print select_sql

    with sql_db as cursor :
        try :
            cursor.execute(insert_sql)
            sql_txt_2 = "Select id FROM "+ table_name +" WHERE contact_link_url = '" + contact_link_url + "'"
            print 'SQL Text 2 is: ' + sql_txt_2
            cursor.execute(sql_txt_2)
            result = cursor.fetchone()
            print result
            print 'the primary key for this '+ table_name +' is '+str(result["id"])
            return result["id"]

        finally :
            sql_db.commit

def insert_into_management(row, table_name):
    # Insert into management table

    Management_link_URL = row[5]
    Management_link_text = row[6]
    insert_sql = "INSERT IGNORE INTO "+ "management (Management_link_URL, Management_link_text) VALUES ('" + Management_link_URL + "', '" + Management_link_text + "')"
    select_sql = "SELECT LAST_INSERT_ID()"

    print insert_sql
    print select_sql

    with sql_db as cursor :
        try :
            cursor.execute(insert_sql)

            sql_txt_2 = "Select id FROM "+ table_name +" WHERE Management_link_URL = '" + Management_link_URL + "'"
            print 'SQL Text 2 is: ' + sql_txt_2
            cursor.execute(sql_txt_2)
            result = cursor.fetchone()
            print result
            print 'the primary key for this '+ table_name +' is '+str(result["id"])
            return result["id"]


        finally :
            sql_db.commit


def insert_into_manager(row, table_name):
    Manager_name = row[7]
    Manager_title = row[8]

    insert_sql = "INSERT IGNORE INTO "+ table_name + " (Manager_name, Manager_title) VALUES ('" + Manager_name + "', '" + Manager_title + "')"
    select_sql = "SELECT LAST_INSERT_ID()"

    print insert_sql
    print select_sql

    with sql_db as cursor :
        try :
            cursor.execute(insert_sql)
            sql_txt_2 = "Select id FROM "+ table_name +" WHERE Manager_name = '" + Manager_name + "'"
            print 'SQL Text 2 is: ' + sql_txt_2
            cursor.execute(sql_txt_2)
            result = cursor.fetchone()
            print result
            print 'the primary key for this '+ table_name +' is '+str(result["id"])
            return result["id"]


        finally :
            sql_db.commit

def insert_into_http(row, table_name):
    print row[2]

    HTTP_response_code = row[2]

    insert_sql = "INSERT IGNORE INTO "+ table_name + " (HTTP_response_code) VALUES (" + HTTP_response_code + ")"
    select_sql = "SELECT LAST_INSERT_ID()"

    print insert_sql
    print select_sql

    with sql_db as cursor :
        try :
            cursor.execute(insert_sql)
            sql_txt_2 = "Select id FROM "+ table_name +" WHERE  HTTP_response_code = " + HTTP_response_code
            print 'SQL Text 2 is: ' + sql_txt_2
            cursor.execute(sql_txt_2)
            result = cursor.fetchone()
            print result
            print 'the primary key for this '+ table_name +' is '+str(result["id"])
            return result["id"]

        finally :
            sql_db.commit


def insert_into_website(row, table_name):
    print row[1]

    Website_URL = row[1]

    insert_sql = "INSERT IGNORE INTO "+ table_name + " (Website_URL) VALUES ('" + Website_URL + "')"
    select_sql = "SELECT LAST_INSERT_ID()"

    print insert_sql
    print select_sql

    with sql_db as cursor :
        try :
            cursor.execute(insert_sql)
            sql_txt_2 = "Select id FROM "+ table_name +" WHERE Website_URL = '" + Website_URL + "'"
            print 'SQL Text 2 is: ' + sql_txt_2
            cursor.execute(sql_txt_2)
            result = cursor.fetchone()
            print result
            print 'the primary key for this '+ table_name +' is '+str(result["id"])
            return result["id"]


        finally :
            sql_db.commit



for row in reader :
    website_id = insert_into_website(row, "website")

    http_id = insert_into_http(row, "http")
    contact_id = insert_into_contact(row, "contact")

    management_id = insert_into_management(row, "management")

    manager_id = insert_into_manager(row, "manager")

    insert_sql = "INSERT IGNORE INTO "+ "access" + " (access_time, http_id, website_id, management_id, contact_id, manager_id) VALUES ('" + row[9] + "', " + str(http_id) + ", " + str(website_id) + ", " + str(management_id) + ", " + str(contact_id) + ", " + str(manager_id) + " )"

    print insert_sql


    with sql_db as cursor :
        try :
            cursor.execute(insert_sql)
        finally :
            sql_db.commit

print  "DONE"

