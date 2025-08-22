# -*- coding: utf-8 -*-
"""
Created on Tue Jan  2 15:05:37 2024

@author: JW19712
"""

import time
import requests
import pandas as pd
import numpy as np
import datetime
import pyodbc
import WFMConfig
import threading
import queue
import warnings
import schedule
from sqlalchemy import create_engine
from sqlalchemy.sql import text

warnings.filterwarnings("ignore")
baseURL =       WFMConfig.baseURL
username =      WFMConfig.username
password =      WFMConfig.password
 
clientID =      WFMConfig.clientID
clientSecret =  WFMConfig.clientSecret
appKey =        WFMConfig.appKey

def getAccessToken():
    try:
        authURL = baseURL + "authentication/access_token"
        
       
        
        headers = {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'appkey':       appKey
            }
        
        payload = {
                        'username':     username,
                        'password':     password,
                        'client_id':    clientID,
                        'client_secret':clientSecret,
                        'grant_type':   'password',
                        'auth_chain':   'OAuthLdapService'
            }
        
        response = requests.post(
                        authURL,
                        data = payload,
                        headers = headers, 
                        verify = False
            )
        if response.ok:
            responseJSON = response.json()
            accessToken = responseJSON.get('access_token')
            #print("Access Token Response - - \t OK")
        else:
            print(f"Error with access token: {response.text}")
            return getAccessToken()
            
        return accessToken
    
    except requests.exceptions.RequestException as e:
        print('Access Token Request failed: ', e)
        time.sleep(5)
        return getAccessToken()
    
def getAllPersons():
    accessToken = getAccessToken()
    personURL = baseURL + "v1/commons/persons/apply_read"
    
    headers = {
                        'Content-Type': 'application/json',
                        'accept':       'application/json',
                        'appkey':       appKey,
                        'Authorization':f'Bearer {accessToken}'
        }
    
    payload = {
                        "where": {}
        }
    
    try:
        response = requests.post(personURL, json=payload, headers=headers, verify = False)
        
        if response.ok:
            print('All Person Response - - \t OK')
            responseJSON = response.json()
        else:
            print(f'Error retrieving persons: {response.text}')
            
        return responseJSON
    except requests.exceptions.RequestException as e:
        print('Person request failed: ', e)
        return getAllPersons()
    
def getAllPersonIDs():
    personJSON = getAllPersons()
    personIDs = []
    for i in range(len(personJSON['records'])):
        personIDs.append(personJSON['records'][i]['personId'])
    return personIDs

def getAllEmployeeIDs():
    personJSON = getAllPersons()
    employeeIDs = []
    for i in range(len(personJSON['records'])):
        employeeIDs.append(personJSON['records'][i]['personNumber'])
    return employeeIDs

def getAOID(identifier, accessToken):
    aoidURL = baseURL + f"v1/commons/persons/external_id/{identifier}"
    headers = {
                        'Content-Type': 'application/json',
                        'accept':       'application/json',
                        'appkey':       appKey,
                        'Authorization':f'Bearer {accessToken}'
        }
    try:
        response = requests.get(aoidURL, headers=headers, verify = False)
        response = response.json()
        if 'aoid' in response:
            return response['aoid']
        else:
            return 'NoAOID'
    except:
        print('AOID ERROR')
        return getAOID(identifier, accessToken)
    
def getTimeCard(personID, responseQueue, start, end):
    accessToken = getAccessToken()
    startTime = time.time()
    while True:
        try:
                aoid = getAOID(personID, accessToken)
                
                timecardURL = baseURL + f"v1/timekeeping/timecard?start_date={start}&end_date={end}&breakUpAtMidnight=true&include_phantom_punches=false&include_kind_of_time_segments=true&show_totals_with_hidden_paycodes=true&totals_include_combined_paycodes=true&aoid={aoid}&retrieve_full_labor_categories=false&return_true_cost_center_id=false&include_hidden_pay_code_edits=false"
            
                headers = {
                                'Content-Type': 'application/json',
                                'accept':       'application/json',
                                'appkey':       appKey,
                                'Authorization':f'Bearer {accessToken}'
                }
                timeJSON = {}
                print(f'Getting {aoid}')
                response = requests.get(timecardURL, headers=headers, verify = False)
                time.sleep(0.5)
                timeJSON = response.json()
                if response.ok:
                    
                    time.sleep(0)
                else:
                    response.raise_for_status()
        except Exception as e:
                print(f'Waiting for error: {e}')
                if 'errorCode' in timeJSON:
                    if timeJSON['errorCode'] == 'WTK-145027':
                        responseQueue.put((personID, response.json()))
                        break
                    time.sleep(5)
        else:
                responseQueue.put((personID, response.json()))
                break
    endTime = time.time()
    #print("\nExecution time: ", time.strftime("%H:%M:%S", time.gmtime(endTime - startTime)))

def processIDs():
    SQLserver = r'server'
    SQLdatabase = 'db'
    SQLusername = 'user'
    SQLpassword = 'pass'
    SQLcxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SQLserver+';DATABASE='+SQLdatabase+';TrustServerCertificate=yes;ENCRYPT=yes;UID='+SQLusername+';PWD='+SQLpassword)
    cursor = SQLcxn.cursor()
    responseQueue = queue.Queue()
    IDs = getAllPersonIDs()
    startTime = time.time()
    numThreads = 3
    idChunks = [IDs[i::numThreads] for i in range(numThreads)]
    threads = []
    updateTime = pd.read_sql("SELECT MAX(lastUpdated) as lastUpdated FROM SCH_ADP_Historical_Timecards", SQLcxn)
    dateUpdate = datetime.datetime.date(updateTime['lastUpdated'][0])
    start = dateUpdate
    end = datetime.date.today()
    
    while start < end:
        currentEnd = min(start + datetime.timedelta(days = 350), end)
        
        for chunk in idChunks:
            thread = threading.Thread(target = lambda ids: [getTimeCard(ID, responseQueue, start, currentEnd) for ID in chunk], args = (chunk,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
            
        start = currentEnd
        
    responses = []
    while not responseQueue.empty():
        responses.append(responseQueue.get()[1])
        
    endTime = time.time()
    print("\nExecution time: ", time.strftime("%H:%M:%S", time.gmtime(endTime - startTime)))
    return responses

def parseTimecards(timecards, index, queue):
    count = 0
    if 'errorCode' not in index:
            if len(index['processedSegments']) > 0:
                for segment in index['processedSegments']:
                    record = ['', segment['employee']['qualifier'], segment['employee']['id'], segment['itemId'], segment['orderNumber'], datetime.datetime.strptime(segment['startDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S'), datetime.datetime.strptime(segment['endDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S'), (lambda segment: segment['transfer']['transferString'] if 'transfer' in segment else '')(segment), '', '', '', '', segment['segmentTypeId'], (lambda segment: segment['paycode']['qualifier'] if 'paycode' in segment else '')(segment), '', segment['durationInSeconds'],  0]
                    queue.put(record)
            elif len(index['processedSegments']) < 1 and len(index['punches']) < 1 and len(index['hoursWorked']) < 1:
                count += 1
    if 'errorCode' not in index:
            if len(index['overtimeInfoForDates']) > 0:
                for overtimeDate in index['overtimeInfoForDates']:
                    for overtimeSegment in index['overtimeInfoForDates'][overtimeDate]['overtimeSegments']:
                        record = ['', index['activityTotals'][0]['employeeContext']['employee']['qualifier'], index['activityTotals'][0]['employeeContext']['employee']['id'], overtimeSegment['workItemId'], 0, datetime.datetime.strptime(overtimeSegment['startDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S'), datetime.datetime.strptime(overtimeSegment['endDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S'),'', '', '', '', '', 0, '', '', overtimeSegment['amount'], 0]
                        queue.put(record)
    #count += 1
    
def getPunches():
    timecards = processIDs()
    print('All timecards gotten')
    persons = getAllPersons()
    print('All persons gotten')
    timeStart = time.time()
    SQLserver = r'server'
    SQLdatabase = 'db'
    SQLusername = 'user'
    SQLpassword = 'pass'
    driver = 'ODBC+Driver+17+for+SQL+Server'
    
    connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SQLserver+';DATABASE='+SQLdatabase+';TrustServerCertificate=yes;ENCRYPT=yes;UID='+SQLusername+';PWD='+SQLpassword
    connectionURL = f'mssql+pyodbc:///?odbc_connect={connectionString}'
    engine = create_engine(connectionURL)
    
    groupStart = time.time()
    print('pulling established ADP Data')
    databaseData = pd.read_sql("SELECT DISTINCT fileNumber, homeDepartmentCode, jobTitleCode FROM dbo.SCH_Employee_Profiles_ADP", engine)
    groupEnd = time.time()
    print("Time to pull established ADP data: ", time.strftime("%H:%M:%S", time.gmtime(groupEnd - groupStart))) 
    
    groupStart = time.time()
    #count = 0
    allPunchesColumns = ['legalName', 'fileNumber', 'personNumber', 'segmentID', 'orderNumber', 'shiftStart', 'shiftEnd', 'transfer', 'homeDepartment', 'homeJobCode', 'transferDepartment', 'transferJobCode', 'segmentTypeID', 'paycodeQualifier', 'transferPaycode', 'durationInSeconds', 'overtimeAdjusted']
    
    timecardQueue = queue.Queue()
    numThreads = 160
    timecardChunks = [timecards[i::numThreads] for i in range(numThreads)]
    threads = []
    
    print('Parsing timecards')
    for chunk in timecardChunks:
        thread = threading.Thread(target = lambda rows: [parseTimecards(timecards, index, timecardQueue) for index in chunk], args = (chunk,))
        threads.append(thread)
        thread.start()
        
    for thread in threads:
        thread.join()
        
    outputList = []
    while not timecardQueue.empty():
        outputList.append(timecardQueue.get())
        
    allPunches = pd.DataFrame(outputList, columns = allPunchesColumns)
    # for index in range(len((timecards))):
    #     if 'errorCode' not in timecards[index]:
    #         if len(timecards[index]['processedSegments']) > 0:
    #             for segment in timecards[index]['processedSegments']:
    #                 allPunches.loc[len(allPunches.index)] = ['', segment['employee']['qualifier'], segment['employee']['id'], segment['itemId'], segment['orderNumber'], datetime.datetime.strptime(segment['startDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S'), datetime.datetime.strptime(segment['endDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S'), (lambda segment: segment['transfer']['transferString'] if 'transfer' in segment else '')(segment), '', '', '', '', segment['segmentTypeId'], (lambda segment: segment['paycode']['qualifier'] if 'paycode' in segment else '')(segment), '', segment['durationInSeconds'], 0]
    #         elif len(timecards[index]['processedSegments']) < 1 and len(timecards[index]['punches']) < 1 and len(timecards[index]['hoursWorked']) < 1:
    #             count += 1
    #     if 'errorCode' not in timecards[index]:
    #         if len(timecards[index]['overtimeInfoForDates']) > 0:
    #             for overtimeDate in timecards[index]['overtimeInfoForDates']:
    #                 for overtimeSegment in timecards[index]['overtimeInfoForDates'][overtimeDate]['overtimeSegments']:
    #                     allPunches.loc[len(allPunches.index)] = ['', timecards[index]['activityTotals'][0]['employeeContext']['employee']['qualifier'], timecards[index]['activityTotals'][0]['employeeContext']['employee']['id'], overtimeSegment['workItemId'], 0, datetime.datetime.strptime(overtimeSegment['startDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S'), datetime.datetime.strptime(overtimeSegment['endDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S'),'', '', '', '', '', 0, '', '', overtimeSegment['amount'], 0]
    #     count += 1
    groupEnd = time.time()
    print("Time to process timecards: ", time.strftime("%H:%M:%S", time.gmtime(groupEnd - groupStart)))   
    for i in range(len(persons['records'])):
        allPunches.loc[allPunches.personNumber == persons['records'][i]['personId'], 'legalName'] = persons['records'][i]['lastName'] + (lambda persons: (', ' + persons['records'][i]['firstName']) if 'firstName' in persons['records'][i] else '')(persons)
    
    segmentTypeMap = {
                        0: 'Overtime',
                        1: 'Regular',
                        4: 'SelfGenBreak',
                        6: 'AltPaycode',
                        9: 'SystemGen30',
                        11: 'Callback1hrPadding'
        }
    
    homeDeptDict = databaseData.set_index('fileNumber')['homeDepartmentCode'].to_dict()
    jobCodeDict = databaseData.set_index('fileNumber')['jobTitleCode'].to_dict()
    allPunches['homeDepartment'] = allPunches['fileNumber'].map(homeDeptDict)
    allPunches['homeJobCode'] = allPunches['fileNumber'].map(jobCodeDict)
    allPunches['segmentTypeID'] = allPunches['segmentTypeID'].map(segmentTypeMap)
    
    print('Start grouping session')
    groupStart = time.time()
    allPunchesGrouped = allPunches.groupby('fileNumber').apply(lambda x: x.to_dict(orient = 'records')).to_dict()
    allPunchesGroupedEnd = {}
    for item, itemData in allPunchesGrouped.items():
        endGrouped = pd.DataFrame(itemData).groupby('shiftEnd').apply(lambda x: x.to_dict(orient = 'records')).to_dict()
        allPunchesGroupedEnd[item] = endGrouped
    groupEnd = time.time()
    print("Time to group by fileNumber and shiftEnd: ", time.strftime("%H:%M:%S", time.gmtime(groupEnd - groupStart)))
    
    groupStart = time.time()
    for i in range(len(allPunches)):
        if allPunches['transfer'][i]:
            if '/' in allPunches['transfer'][i]:
                allPunches['transferDepartment'][i] = allPunches['transfer'][i].split('/')[1]
                allPunches['transferJobCode'][i] = allPunches['transfer'][i].split('/')[2].split(';')[0]
                if len(allPunches['transfer'][i].split(';')) > 1:
                    allPunches['transferPaycode'][i] = allPunches['transfer'][i].split(';')[1]
            if not '/' in allPunches['transfer'][i]:
                allPunches['transferPaycode'][i] = allPunches['transfer'][i].split(';')[1]
                
    groupEnd = time.time()
    print("Time to adjust transfers: ", time.strftime("%H:%M:%S", time.gmtime(groupEnd - groupStart)))
    
    groupStart = time.time()
    segmentGrouped = allPunches.groupby('segmentID').apply(lambda x: x.to_dict(orient = 'records')).to_dict()
    groupEnd = time.time()
    print("Time to group by segment ID: ", time.strftime("%H:%M:%S", time.gmtime(groupEnd - groupStart)))
    
    groupStart = time.time()
    last = True
    while last:
        readyToBreak = True
        for key in segmentGrouped:
            for i in range(len(segmentGrouped[key])):
                fixed = False
                if segmentGrouped[key][i]['shiftEnd'].time() == datetime.datetime.strptime("01JAN2001000000", "%d%b%Y%H%M%S").time() and segmentGrouped[key][i]['segmentTypeID'] == 'Regular':
                    for j in range(len(segmentGrouped[key])):
                        if segmentGrouped[key][j]['shiftStart'].time() == datetime.datetime.strptime("01JAN2001000000", "%d%b%Y%H%M%S").time() and segmentGrouped[key][j]['segmentTypeID'] == 'Regular':
                            #print("Fixed " + str(key))
                            segmentGrouped[key][i]['shiftEnd'] = segmentGrouped[key][j]['shiftEnd']
                            segmentGrouped[key][i]['durationInSeconds'] = segmentGrouped[key][i]['durationInSeconds'] + segmentGrouped[key][j]['durationInSeconds']
                            del segmentGrouped[key][j]
                            fixed = True
                            readyToBreak = False
                            break
                if fixed:
                    break
        if readyToBreak:
            last = False
    groupEnd = time.time()
    print("Time to fix midnights: ", time.strftime("%H:%M:%S", time.gmtime(groupEnd - groupStart)))
    
    groupStart = time.time()
    last = True
    while last:
        readyToBreak = True
        for key in segmentGrouped:
            for i in range(len(segmentGrouped[key])):
                fixed = False
                if segmentGrouped[key][i]['segmentTypeID'] == 'Regular':
                    timeToFix = segmentGrouped[key][i]['shiftEnd']
                    for j in range(len(segmentGrouped[key])):
                        if segmentGrouped[key][j]['segmentTypeID'] == 'Regular' and segmentGrouped[key][j]['shiftStart'] == timeToFix and not segmentGrouped[key][i] == segmentGrouped[key][j]:
                            #print("Fixed Regular Break for " + str(key))
                            segmentGrouped[key][i]['shiftEnd'] = segmentGrouped[key][j]['shiftEnd']
                            segmentGrouped[key][i]['durationInSeconds'] = segmentGrouped[key][i]['durationInSeconds'] + segmentGrouped[key][j]['durationInSeconds']
                            del segmentGrouped[key][j]
                            fixed = True
                            readyToBreak = False
                            break
                if fixed:
                    break
        if readyToBreak:
            last = False
    groupEnd = time.time()
    print("Time to fix regular breaks: ", time.strftime("%H:%M:%S", time.gmtime(groupEnd - groupStart)))
            
    for segID in segmentGrouped:
        segmentGrouped[segID] = sorted(segmentGrouped[segID], key = lambda x: x['shiftStart'])
    
    holidayFixes = []
    groupStart = time.time()
    print("Holiday Fixes")
    holCount = 0
    last = True
    while last:
        readyToBreak = True
        for key in segmentGrouped:
            print(f'{holCount}/' + str(len(segmentGrouped)), end = '\r')
            for i in range(len(segmentGrouped[key])):
                fixed = False
                if segmentGrouped[key][i]['segmentTypeID'] == 'Overtime':
                    if (segmentGrouped[key][i-1]['segmentTypeID'] == 'Regular' or segmentGrouped[key][i-1]['segmentTypeID'] == 'AltPaycode') and not segmentGrouped[key][i-1]['overtimeAdjusted']:
                        if segmentGrouped[key][i]['shiftEnd'] > segmentGrouped[key][i-1]['shiftEnd']:
                            segmentGrouped[key][i-1]['shiftEnd'] = segmentGrouped[key][i]['shiftEnd']
                            segmentGrouped[key][i-1]['overtimeAdjusted'] = 1
                        elif segmentGrouped[key][i]['shiftEnd'] < segmentGrouped[key][i-1]['shiftEnd']:
                            segmentGrouped[key][i]['shiftEnd'] = segmentGrouped[key][i-1]['shiftEnd']
                            segmentGrouped[key][i-1]['overtimeAdjusted'] = 1
                        segmentGrouped[key][i-1]['shiftEnd'] = segmentGrouped[key][i]['shiftStart']
                        segmentGrouped[key][i-1]['overtimeAdjusted'] = 1
                    elif (i-1) < 0:
                        if (segmentGrouped[key][i+1]['segmentTypeID'] == 'Regular' or segmentGrouped[key][i+1]['segmentTypeID'] == 'AltPaycode') and not segmentGrouped[key][i+1]['overtimeAdjusted']:
                            if segmentGrouped[key][i]['shiftEnd'] > segmentGrouped[key][i+1]['shiftEnd']:
                                segmentGrouped[key][i+1]['shiftEnd'] = segmentGrouped[key][i]['shiftEnd']
                                segmentGrouped[key][i+1]['overtimeAdjusted'] = 1
                            elif segmentGrouped[key][i]['shiftEnd'] < segmentGrouped[key][i+1]['shiftEnd']:
                                segmentGrouped[key][i]['shiftEnd'] = segmentGrouped[key][i+1]['shiftEnd']
                                segmentGrouped[key][i+1]['overtimeAdjusted'] = 1
                            elif segmentGrouped[key][i]['shiftEnd'] == segmentGrouped[key][i+1]['shiftEnd']:
                                segmentGrouped[key][i+1]['shiftStart'] = segmentGrouped[key][i]['shiftStart']
                                segmentGrouped[key][i+1]['shiftEnd'] = segmentGrouped[key][i]['shiftStart']
                            segmentGrouped[key][i+1]['overtimeAdjusted'] = 1
                if segmentGrouped[key][i]['segmentTypeID'] == 'AltPaycode' and segmentGrouped[key][i]['paycodeQualifier'] == 'Holiday':
                     #print("HOLIDAY TIME !!!")
                     timecardIndex = searchQualifier(segmentGrouped[key][i]['fileNumber'], timecards)
                     date = segmentGrouped[key][i]['shiftStart'].date()
                     if 'scheduleShifts' in timecards[timecardIndex]:
                         #print('\tscheduleShifts in ' + str(timecardIndex))
                         if len(timecards[timecardIndex]['scheduleShifts']) > 0:
                             #print('\t\tand it has length!!')
                             for cardI in range(len(timecards[timecardIndex]['scheduleShifts'])):
                                 if datetime.datetime.strptime(timecards[timecardIndex]['scheduleShifts'][cardI]['startDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S').date() == date:
                                     #print("\t\t\tDATE IS THE SAME !!!!")
                                     #time.sleep(1)
                                     holidayFixes.append([segmentGrouped[key][i]['legalName'], 
                                                          segmentGrouped[key][i]['fileNumber'], 
                                                          segmentGrouped[key][i]['personNumber'], 
                                                          segmentGrouped[key][i]['segmentID'], 
                                                          segmentGrouped[key][i]['orderNumber'], 
                                                          datetime.datetime.strptime(timecards[timecardIndex]['scheduleShifts'][cardI]['startDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S'), 
                                                          datetime.datetime.strptime(timecards[timecardIndex]['scheduleShifts'][cardI]['endDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S'), 
                                                          (lambda timecards: timecards[timecardIndex]['scheduleShifts'][cardI]['segments'][0]['orgJobRef']['qualifier'] if 'orgJobRef' in timecards[timecardIndex]['scheduleShifts'][cardI]['segments'][0] else '')(timecards),
                                                          segmentGrouped[key][i]['homeDepartment'], 
                                                          segmentGrouped[key][i]['homeJobCode'], 
                                                          parseTransferDept((lambda timecards: timecards[timecardIndex]['scheduleShifts'][cardI]['segments'][0]['orgJobRef']['qualifier'] if 'orgJobRef' in timecards[timecardIndex]['scheduleShifts'][cardI]['segments'][0] else '')(timecards)), 
                                                          parseTransferJob((lambda timecards: timecards[timecardIndex]['scheduleShifts'][cardI]['segments'][0]['orgJobRef']['qualifier'] if 'orgJobRef' in timecards[timecardIndex]['scheduleShifts'][cardI]['segments'][0] else '')(timecards)), 
                                                          (lambda timecards: timecards[timecardIndex]['scheduleShifts'][cardI]['segments'][0]['segmentTypeRef']['qualifier'] if 'segmentTypeRef' in timecards[timecardIndex]['scheduleShifts'][cardI]['segments'][0] else '')(timecards), 
                                                          'RectifiedHolHours', 
                                                          0, 
                                                          (datetime.datetime.strptime(timecards[timecardIndex]['scheduleShifts'][cardI]['endDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S')-datetime.datetime.strptime(timecards[timecardIndex]['scheduleShifts'][cardI]['startDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S')).total_seconds(),
                                                          0
                                                          ])
                                 else:
                                     #print(str(datetime.datetime.strptime(timecards[timecardIndex]['scheduleShifts'][cardI]['startDateTime'].replace('T', ' '), '%Y-%m-%d %H:%M:%S').date()) + " != " + str(date))
                                     time.sleep(0)
                if fixed:
                    holCount += 1
                    break
        if readyToBreak:
            last = False
    groupEnd = time.time()
    print("Time to fix shift ends/shift starts: ", time.strftime("%H:%M:%S", time.gmtime(groupEnd - groupStart)))
    groupStart = time.time()
    records = []
    for key in segmentGrouped:
        for segment in segmentGrouped[key]:
            records.append([segment['legalName'], segment['fileNumber'], segment['personNumber'], segment['segmentID'], segment['orderNumber'], segment['shiftStart'], segment['shiftEnd'], segment['transfer'], segment['homeDepartment'], segment['homeJobCode'], segment['transferDepartment'], segment['transferJobCode'], segment['segmentTypeID'], segment['paycodeQualifier'], segment['transferPaycode'], segment['durationInSeconds'], segment['overtimeAdjusted']])
            
    records.extend(holidayFixes)
    unpacked = pd.DataFrame(records, columns = ['legalName', 'fileNumber', 'personNumber', 'segmentID', 'orderNumber', 'shiftStart', 'shiftEnd', 'transfer', 'homeDepartment', 'homeJobCode', 'transferDepartment', 'transferJobCode', 'segmentTypeID', 'paycodeQualifier', 'transferPaycode', 'durationInSeconds', 'overtimeAdjusted'])
    groupEnd = time.time()
    print("Time to unpack: ", time.strftime("%H:%M:%S", time.gmtime(groupEnd - groupStart)))
    
    for i in range(len(unpacked)):
        timeDiff = unpacked['shiftEnd'][i] - unpacked['shiftStart'][i]
        #print(str(i) + ": " + str(timeDiff))
        if timeDiff == None:
            unpacked['shiftStart'][i] = 0
            continue
        elif int(timeDiff.total_seconds()) < 0:
            unpacked['shiftStart'][i] = unpacked['shiftStart'][i-1]
            
    endTime = time.time()
    print("\nExecution time: ", time.strftime("%H:%M:%S", time.gmtime(endTime - timeStart)))
    unpacked['lastUpdated'] = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    unpacked.replace({np.nan: None}, inplace = True)
    return unpacked

def searchQualifier(qualifier, timecards):
    for i in range(len(timecards)):
        if 'activityTotals' in timecards[i]:
            if timecards[i]['activityTotals'][0]['employeeContext']['employee']['qualifier'] == qualifier:
                return i
                
def parseTransferDept(string):
    if '/' in string:
        department = string.split('/')[1]
        return department
    else:
        return ''
        
    

def parseTransferJob(string):
    if '/' in string:
        jobCode = string.split('/')[2]
        return jobCode
    else:
        return ''
    
def dropAndWriteTable():
    print("Locking drop and write")
    checkQuery = """SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'SCH_ADP_Historical_Timecards_Staging'"""
    SQLserver = r'server'
    SQLdatabase = 'db'
    SQLusername = 'user'
    SQLpassword = 'pass'
    SQLcxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SQLserver+';DATABASE='+SQLdatabase+';TrustServerCertificate=yes;ENCRYPT=yes;UID='+SQLusername+';PWD='+SQLpassword)
    cursor = SQLcxn.cursor()
    cursor.execute(checkQuery)
    result = cursor.fetchone()
    
    df = getPunches()
    
    insertStr = "INSERT INTO [dbo].[SCH_ADP_Historical_Timecards_Staging](legalName, fileNumber, personNumber, segmentID, orderNumber, shiftStart, shiftEnd, transfer, homeDepartment, homeJobCode, transferDepartment, transferJobCode, segmentTypeID, paycodeQualifier, transferPaycode, durationInSeconds, overtimeAdjusted,  lastUpdated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    if result:
        cursor.execute("DROP TABLE [dbo].[SCH_ADP_Historical_Timecards_Staging]")
    cursor.execute("""CREATE TABLE [dbo].[SCH_ADP_Historical_Timecards_Staging](
            	[legalName] [varchar](50) NULL,
            	[fileNumber] [varchar](50) NULL,
            	[personNumber] [varchar](50) NULL,
            	[segmentID] [varchar](50) NULL,
            	[orderNumber] [varchar](50) NULL,
            	[shiftStart] [datetime] NULL,
            	[shiftEnd] [datetime] NULL,
            	[transfer] [varchar](max) NULL,
            	[homeDepartment] [varchar](50) NULL,
            	[homeJobCode] [varchar](10) NULL,
            	[transferDepartment] [varchar](50)NULL,
            	[transferJobCode] [varchar](50) NULL,
            	[segmentTypeID] [varchar](50) NULL,
            	[paycodeQualifier] [varchar](50) NULL,
            	[transferPaycode] [varchar](50) NULL,
            	[durationInSeconds] [int] NULL,
            	[overtimeAdjusted] [int] NULL,
                [lastUpdated] [datetime] NULL
        )""")
    
    for i in range(10):
        print('Start writing staging table')
    time.sleep(60)
    count = 0
    for index in df.values:
        while True:
            percent = (count/len(df))*100
            print(str(count) + ' / ' + str(len(df)) + f' ({percent:.2f}%)')
            try:
                #print(insertStr, tuple(index))
                cursor.execute(insertStr, tuple(index))
                count += 1
                break
            except Exception as e:
                print(f'error inserting into staging table: {e}')
                time.sleep(60)
                SQLcxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SQLserver+';DATABASE='+SQLdatabase+';TrustServerCertificate=yes;ENCRYPT=yes;UID='+SQLusername+';PWD='+SQLpassword)
                cursor = SQLcxn.cursor()
                continue
            
    
    try:
        cursor.execute("SELECT * INTO SCH_ADP_Historical_Timecards FROM SCH_ADP_Historical_Timecards_Staging")
        cursor.execute("DROP TABLE [dbo].[SCH_ADP_Historical_Timecards_Staging]")
        cursor.commit()
        SQLcxn.close()
    except Exception as e:
        time.sleep(600)
        print(f'Error in select into: {e}')
        SQLcxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SQLserver+';DATABASE='+SQLdatabase+';TrustServerCertificate=yes;ENCRYPT=yes;UID='+SQLusername+';PWD='+SQLpassword)
        cursor = SQLcxn.cursor()
        cursor.execute("SELECT * INTO SCH_ADP_Historical_Timecards FROM SCH_ADP_Historical_Timecards_Staging")
        cursor.execute("DROP TABLE [dbo].[SCH_ADP_Historical_Timecards_Staging]")
        cursor.commit()
        SQLcxn.close()
        
try:
     dropAndWriteTable()
except Exception as e:
     print(f"Error: {e}")