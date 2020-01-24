import numpy as np
import pandas as pd
import requests
import datetime as dt
from matplotlib import pyplot as pl

# This python script is copied directly from Warren Helgason's MATLAB script for downloading WISKI data
# Andrew Ireson, March 16, 2016
#
# Tracked in git since May 16, 2016

def searchStation(MatchStr):
    '''Use this function to find the station name you are interested in. Syntax is \n Station_Name('SearchString')'''
    url_spec="http://giws.usask.ca:8080/KiWIS/KiWIS?service=kisters&type=queryServices&request=getStationList&datasource=0&format=ascii"
    if MatchStr==[]: 
        MatchStr=''

    # Fetch the list of available parameters from the Wiski server
    #f = urllib.request.urlopen(url_spec)
    f= iter(requests.get(url_spec).text.split('\n'))
    ts=[]
    parameters=[]
    output=[]
    f.__next__()
    i=0
    for line in f:
        if not len(line)==0:
            StationStr = line.split('\t')[0].strip()
            StationNo = line.split('\t')[1].strip()
            if MatchStr in StationStr or MatchStr in StationNo:
                print("%d \t %s" % (i,StationStr))
                i+=1
                output.append(StationStr)
    return output

def searchTimeseries(MatchStr,station_name):
    ts_name_list = ['56.*','04.*', '02.*'] # what type of TS? Wildcard ok (refer to Wiski TS types)

    ts=[]
    parameters=[]
    for ts_name in ts_name_list: 
        # Data selection parameters
        wiskiex = 'http://giws.usask.ca:8080/KiWIS/KiWIS'
        service='kisters'
        type='queryServices'
        request='getTimeseriesList'
        datasource='0'
        format='ascii'
        returnfields='station_name,ts_id,ts_name,stationparameter_name'

        # format the data selection parameters into a string
        parameter_list = 'service='+service+'&'
        parameter_list+= 'type='+type+'&'
        parameter_list+= 'request='+request+'&'
        parameter_list+= 'datasource='+datasource+'&'
        parameter_list+= 'format='+format+'&'
        parameter_list+= 'station_name='+station_name+'&'
        parameter_list+= 'ts_name='+ts_name+'&'
        parameter_list+= 'returnfields='+returnfields+'&'

        #url_spec = sprintf('#s?#s',wiskiex,parameter_list)
        url_spec=wiskiex + '?' + parameter_list

        # Fetch the list of available parameters from the Wiski server
        # f = urllib.request.urlopen(url_spec)
        f= iter(requests.get(url_spec).text.split('\n'))
        f.__next__()
        #dum = f.readline()
        for line in f:
            if not len(line)==0:
                ts.append(line.split('\t')[1].strip())
                parameters.append(line.split('\t')[3].strip())

    dum, i = np.unique(parameters,return_index=True)
    ts=np.asarray(ts)
    parameters=np.asarray(parameters)
    ts = ts[i]
    parameters = parameters[i]

    print("No # \t Timeseries \t Parameter")
    i=0
    output=np.array([])
    for (a,b) in zip(ts,parameters):
        print("%d \t %s \t %s" % (i,a,b))
        output = np.append(output,a + ',' + b)
        i+=1
    return output

def getTimeseries(ts,station_name,Start,End):

    # data extraction parameters
    wiskiex = 'http://giws.usask.ca:8080/KiWIS/KiWIS'
    service='kisters'
    type='queryServices'
    request='getTimeseriesValues'
    datasource='0'
    format='ascii'
    dateformat='yyyy-MM-dd%20HH:mm:ss'
    timezone = 'GMT-6'
    UTC_off = 0 #set the UTC offset the same as above
    returnfields='Timestamp,Value'

    # Create an empty dataframe:
    df=pd.DataFrame()

    print(Start)
    for tspar in ts:
        ts_id = str(tspar).split(',')[0].strip()
        par = str(tspar).split(',')[1].strip()

        parameter_list = 'service='+service+'&'
        parameter_list+= 'type='+type+'&'
        parameter_list+= 'request='+request+'&'
        parameter_list+= 'datasource='+datasource+'&'
        parameter_list+= 'format='+format+'&'
        parameter_list+= 'dateformat='+dateformat+'&'
        parameter_list+= 'timezone='+timezone+'&'
        parameter_list+= 'ts_id='+ts_id+'&'
        parameter_list+= 'returnfields='+returnfields+'&'
        parameter_list+= 'from='+Start+'&'
        parameter_list+= 'to='+End+'&'

        url_spec=wiskiex + '?' + parameter_list
        #print url_spec
        # Fetch the list of available parameters from the Wiski server
        # f = urllib.request.urlopen(url_spec)
        f= iter(requests.get(url_spec).text.split('\n'))
        # Skip 3 header lines:
        [next(f) for i in range(3)]
        t=[]
        data=[]
        for line in f:
            if not len(line)==0:
                line=line.replace('no value','NaN')
                t.append(line.split('\t')[0].strip())
                data.append(float(line.split('\t')[1].strip()))
        print(par, len(data))
        df[par]=data

    time=[]
    for i in t:
        time.append(dt.datetime.strptime(i, '%Y-%m-%d %H:%M:%S'))

    df=df.set_index(pd.DatetimeIndex(t))

    return df

def saveTS(df,fname):
    df.to_hdf('./' + fname + '.h5','df')
