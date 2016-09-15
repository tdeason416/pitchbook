import urllib
import requests
import re
import csv
import os
import datetime
from bs4 import BeautifulSoup
from pathlib import Path
import pymysql
#this is where i live, for now all the files are being saved there as well
sitenum = 1
label = datetime.datetime.now()
labeln = label.time()
labeln = str(label).split('.')
labeln = labeln[0].split(':')
timetag = str()
for time in labeln[0:2]:
    timetag = timetag + '-' + time
workdir = raw_input('enter directory where list of websites (.txt) are stored:')
if len workdir < 1 :
    workdir = 'F:\\Travis\\Documents\\pylearn\\pitchbook\\csvdata\\'
readfile = 'webcrawl.txt'
writefile = 'databox'+timetag+'.csv'
location_r = os.path.join(workdir,readfile)
location_w = os.path.join(workdir,writefile)
#list of management terms
contact_search = ['contact']
management = ['people','management',' leadership','staff','everything','team','about','directors']
#list of http-tags which may contain names of managers
pertag = ['ahref','strong','p','h3','em','h2','h4','small']
#list of titles which managers may
titlelist = ['CEO','CTO','Developer','Manager','Journalist','Director','VP','Engineer','Engineering','Development','Founder','Partner','President','Officer','Senior']
#list of .csv columns to organize data
keys = ('sitenum','Website_URL','HTTP_response_Code','Contact_link_URL','Contact_link_text','Management_link_URL','Management_link_text','Manager_name','Manager_title','access_time')
## creates dictionary which will allow strings to be called by name, instead of thier location in the keys tuple
hasdata = dict()
col = dict()
for key in keys :
    col[key] = key
websites = open(location_r,'r')
###creates inital .csv file which only contains column headers (keys)
with open(location_w,'wb') as fl :
    a = csv.writer(fl,delimiter=',')
    a.writerow(keys)
## defines function to write information to .csv file
def rowdump(datadict,dkeys,callfile):
    'this function requires import csv module to be active'
    output = list()
    for key in dkeys:
        print key+' = '+str(datadict[key])+ '  ' + str(type(datadict[key]))
        if type(datadict[key]) is str or type(datadict[key]) is unicode :
            output.append(str(datadict[key]).strip())
        else :
            output.append(datadict[key])
    with open(callfile,'ab') as fd:  # top tip... 'a' means append here
        wr = csv.writer(fd,dialect='excel')
        wr.writerow(output)
##stolen function to get status codes will return None if no data is returned.
def http_status_code(weburl) :
    try :
        r = requests.head(weburl.rstrip())
        return r.status_code
    except :
        requests.ConnectionError
        return 404
######################################
##### now time for real data grab#####
######################################
##iterates through websites to collect pertinant data
for line in websites :
    publine = 0
    mancount = False
    #resetting datadump variable to null
    for key in keys :
        hasdata[key] = 'None'
    url = 'http://'+line
    hasdata[col['sitenum']] = sitenum
    sitenum = sitenum + 1
    hasdata[col['access_time']] = datetime.datetime.utcnow()
    hasdata[col['Website_URL']] = url.rstrip()
    hasdata[col['HTTP_response_Code']] = http_status_code(url)
    try :
        html = urllib.urlopen(url).read()
    except:
        print 'inoperable website: '+url
        rowdump(hasdata,keys,location_w)
        continue
    try : soup = BeautifulSoup(html.decode('utf-8'),'html.parser')
    except : soup = BeautifulSoup(html,'html.parser')
    tags = soup('a')
    for tag in tags :
        newrl = tag.get('href',None)
        try :
            space = tag.string
            space = re.sub('[^A-Z0-9a-z ]','',space)
            space = space.lower()
        except:
            space = 'noval'
        if newrl is not None and not newrl.startswith('http') :
            if newrl.startswith('/') :
                newrl = url + 'newrl'
            else :
                newrl = 'http://' + newrl
        for term in contact_search :
            if term in space :
                hasdata[col['Contact_link_URL']] = newrl
                hasdata[col['Contact_link_text']] = space
        #print 'contents = '+str(space)+ '  url =  '+ str(newrl)
        for part in management :
            if part in space and len(space) < 20 and publine < 1 :
                hasdata[col['Management_link_text']] = space
                hasdata[col['Management_link_URL']] = newrl
                hasdata[col['access_time']] = datetime.datetime.utcnow()
                namehas = False
                print newrl
                try :
                    html_s = urllib.urlopen(newrl).read()
                except :
                    print 'bad link'
                    continue
                soup_s = BeautifulSoup(html_s.decode('utf-8'),'html.parser')
                linksnum = True
                ptags = soup_s.findAll(pertag)
                for itag in ptags:
                    istitle = False
                    try :
                        manname = itag.string
                        if manname is None :
                            manname = 'nodata'
                    except :
                        manname = itag.contents()
                    for title in titlelist :
                        if title in manname :
                            print 'istitle triggered'
                            istitle =  True
                    mansplit = manname.split()
                    if not istitle and len(mansplit) == 2 and re.search('^[A-Z][a-z]+ [A-Z][a-z]+...',manname):
                        print '------------------------------------------'
                        #manname = re.sub('[^a-zA-Z0-9,.- ]','',manname)
                        employee = manname
                        namehas = True
                        try :
                            print employee
                            hasdata[col['Manager_name']] = employee
                        except :
                            print itag
                            hasdata[col['Manager_name']] = 'bad-data'
                            namehas = False
                    elif namehas and istitle :
                        print 'does this ever print'
                        manname = re.sub('[^A-Za-z0-9,.;: ]','',manname)
                        if len(manname) < 65 :
                            man_title = re.sub('[^A-Za-z0-9,.;: ]','',manname)
                            hasdata[col['Manager_title']] = man_title.rstrip()
                            hasdata[col['Manager_name']] = employee.rstrip()
                            hasdata[col['access_time']] = datetime.datetime.utcnow()
                            rowdump(hasdata,keys,location_w)
                            print '------------------------------------------'
                        namehas = False
    if not mancount:
        print 'no mgmt here'
        hasdata[col['access_time']] = datetime.datetime.utcnow()
        hasdata[col['Management_link_URL']] = 'None'
        rowdump(hasdata,keys,location_w)
    print 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

print '.CSV file write complete now moving onto DB upload'

import pymysql
aws_user = raw_input('Enter AWS account user name: ')
aws_password = raw_input('Enter AWS account password: ')
sql_db = pymysql.connect(host='webcrawl.cssprimgj6xm.us-west-2.rds.amazonaws.com',user=aws_user,password=aws_password,db='webcrawl',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
rowzero = True
db_location_r = location_w
fr =  open(db_location_r,'r')
reader = csv.reader(fr,delimiter = ',')
### keys used to initally add data

def insert_into_contact(row, table_name):

    # Insert into contact table

    print 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx START OF CONTACT xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

    print row[3]
    print row[4]

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


    print 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx DONE WITH CONTACT xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'


def insert_into_management(row, table_name):
    # Insert into management table

    print 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx START OF management xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

    print row[5]
    print row[6]

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

    print 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx DONE WITH management xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'


def insert_into_manager(row, table_name):

    # Insert into contact table

    print 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx START OF Manager xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

    print row[7]
    print row[8]

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


    print 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx DONE WITH Manager xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

def insert_into_http(row, table_name):

    # Insert into contact table

    print 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx START OF HTTP response code xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

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


    print 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx DONE HTTP response code xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'


def insert_into_website(row, table_name):

    # Insert into contact table

    print 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx START OF website xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

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

    print 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx Done with website xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'



for row in reader :
    firstcol = True
    colcount = 0
    # hasdata = dict()
    forkeys = list()
    ###first row only
    if rowzero :
        for item in row :
            if item is not 'sitenum' :
                db_keys.append(item)
        rowzero = False
        print 'omitted 1st row'
        continue

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

