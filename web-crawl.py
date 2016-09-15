import urllib
import requests
import re
import csv
import os
import datetime
from bs4 import BeautifulSoup
from pathlib import Path
#this is where i live, for now all the files are being saved there as well
sitenum = 1
label = datetime.datetime.now()
labeln = label.time()
labeln = str(label).split('.')
labeln = labeln[0].split(':')
timetag = str()
for time in labeln[0:2]:
    timetag = timetag + '-' + time
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

                        namehas = False
    if not mancount:
        print 'no mgmt here'
        hasdata[col['access_time']] = datetime.datetime.utcnow()
        hasdata[col['Management_link_URL']] = 'None'
        rowdump(hasdata,keys,location_w)

