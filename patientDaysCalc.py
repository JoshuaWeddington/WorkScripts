# -*- coding: utf-8 -*-
"""
Created on Mon Jan 15 08:09:54 2024

@author: JW19712
"""

import pandas as pd
import numpy as np
import datetime
import pyodbc
import calendar

SQLserver = r'server'
SQLdatabase = 'db'
SQLusername = 'user'
SQLpassword = 'pass'

SQLcxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SQLserver+';DATABASE='+SQLdatabase+';TrustServerCertificate=yes;ENCRYPT=yes;UID='+SQLusername+';PWD='+SQLpassword+';APP=Python - Patient Days')

janData = pd.DataFrame({'SourceID': [], 'VisitID': [], 'AccountNumber': [], 'LocationStartDate': [], 'LocationEndDate': [], 'TimeInLocMins': [], 'SortOrder': [], 'CurrentLocation': [], 'NewLocation': []})
febData = pd.DataFrame({'SourceID': [], 'VisitID': [], 'AccountNumber': [], 'LocationStartDate': [], 'LocationEndDate': [], 'TimeInLocMins': [], 'SortOrder': [], 'CurrentLocation': [], 'NewLocation': []})
marData = pd.DataFrame({'SourceID': [], 'VisitID': [], 'AccountNumber': [], 'LocationStartDate': [], 'LocationEndDate': [], 'TimeInLocMins': [], 'SortOrder': [], 'CurrentLocation': [], 'NewLocation': []})
aprData = pd.DataFrame({'SourceID': [], 'VisitID': [], 'AccountNumber': [], 'LocationStartDate': [], 'LocationEndDate': [], 'TimeInLocMins': [], 'SortOrder': [], 'CurrentLocation': [], 'NewLocation': []})
mayData = pd.DataFrame({'SourceID': [], 'VisitID': [], 'AccountNumber': [], 'LocationStartDate': [], 'LocationEndDate': [], 'TimeInLocMins': [], 'SortOrder': [], 'CurrentLocation': [], 'NewLocation': []})
junData = pd.DataFrame({'SourceID': [], 'VisitID': [], 'AccountNumber': [], 'LocationStartDate': [], 'LocationEndDate': [], 'TimeInLocMins': [], 'SortOrder': [], 'CurrentLocation': [], 'NewLocation': []})
julData = pd.DataFrame({'SourceID': [], 'VisitID': [], 'AccountNumber': [], 'LocationStartDate': [], 'LocationEndDate': [], 'TimeInLocMins': [], 'SortOrder': [], 'CurrentLocation': [], 'NewLocation': []})
augData = pd.DataFrame({'SourceID': [], 'VisitID': [], 'AccountNumber': [], 'LocationStartDate': [], 'LocationEndDate': [], 'TimeInLocMins': [], 'SortOrder': [], 'CurrentLocation': [], 'NewLocation': []})
sepData = pd.DataFrame({'SourceID': [], 'VisitID': [], 'AccountNumber': [], 'LocationStartDate': [], 'LocationEndDate': [], 'TimeInLocMins': [], 'SortOrder': [], 'CurrentLocation': [], 'NewLocation': []})
octData = pd.DataFrame({'SourceID': [], 'VisitID': [], 'AccountNumber': [], 'LocationStartDate': [], 'LocationEndDate': [], 'TimeInLocMins': [], 'SortOrder': [], 'CurrentLocation': [], 'NewLocation': []})
novData = pd.DataFrame({'SourceID': [], 'VisitID': [], 'AccountNumber': [], 'LocationStartDate': [], 'LocationEndDate': [], 'TimeInLocMins': [], 'SortOrder': [], 'CurrentLocation': [], 'NewLocation': []})
decData = pd.DataFrame({'SourceID': [], 'VisitID': [], 'AccountNumber': [], 'LocationStartDate': [], 'LocationEndDate': [], 'TimeInLocMins': [], 'SortOrder': [], 'CurrentLocation': [], 'NewLocation': []})

year = 2024
startRange = 9
endRange = 10
for monthInRange in range(startRange, endRange):
    year = 2024
    month = monthInRange%12+1
    if month <= startRange:
        year = year+1
    numDays = calendar.monthrange(year, month)[1]
    
#     databaseData = pd.read_sql(f"""select B.*, CASE WHEN r.RegistrationType_MisRegTypeID = 'INO' THEN 'Observation' ELSE 'Inpatient' END AS PT_Type from SCRView.dbo.SCH_ABS_Patient_Loc_Breakdown B
# 	LEFT JOIN Livefdb.dbo.RegAcct_Main r on r.VisitID = B.VisitID
#     where CurrentLocation in ('3N','3C', '4C', '5C', '6C', 'ED.VB', 'RHB', 'ICU', 'NUR') and r.RegistrationType_MisRegTypeID IN ('IN', 'INO', 'IRF', 'NB', 'SNF')
#     and ((YEAR(LocationStartDate)='{year}' and (MONTH(LocationStartDate)='{month}')) or (YEAR(LocationEndDate)='{year}' and MONTH(LocationEndDate)='{month}'))""", SQLcxn)
    
    databaseData = pd.read_sql(f"""select B.*, rc.FinalAbstractFinancialClass_MisAbsFinClassID, CASE WHEN r.RegistrationType_MisRegTypeID = 'INO' THEN 'Observation' ELSE 'Inpatient' END AS PT_Type from SCRView.dbo.SCH_ABS_Patient_Loc_Breakdown B
	LEFT JOIN Livefdb.dbo.RegAcct_Main r on r.VisitID = B.VisitID
    LEFT JOIN Livefdb.dbo.AbsAcct_Main rc on rc.VisitID = B.VisitID
    where CurrentLocation in ('3N','3C', '4C', '5C', '6C', 'ED.VB', 'ICU', 'NUR', 'RHB') and r.RegistrationType_MisRegTypeID IN ('IN', 'INO', 'IRF', 'NB', 'SNF')
    and ((YEAR(LocationStartDate)='{year}' and (MONTH(LocationStartDate)='{month}')) or (YEAR(LocationEndDate)='{year}' and MONTH(LocationEndDate)='{month}'))""", SQLcxn)
    
    monthStart = datetime.datetime(year, month, 1, 0, 0, 0, 0)
    monthEnd = datetime.datetime(year, month, numDays, 23, 59, 0, 0)
    
    databaseData['originalMinutes'] = 0
    databaseData['adjustedMinutes'] = 0
    databaseData = databaseData[databaseData.LocationEndDate.notnull()]
    databaseData = databaseData.reset_index(drop=True)
    for i in range(len(databaseData)):
        databaseData['originalMinutes'][i] = (databaseData['LocationEndDate'][i] - databaseData['LocationStartDate'][i]).total_seconds() / 60
    for i in range(len(databaseData)):
        if databaseData['LocationStartDate'][i] < monthStart:
            databaseData['adjustedMinutes'][i] = (databaseData['LocationEndDate'][i] - monthStart).total_seconds() / 60
        elif databaseData['LocationEndDate'][i] > monthEnd:
            databaseData['adjustedMinutes'][i] = (monthEnd - databaseData['LocationStartDate'][i]).total_seconds() / 60
        else:
            databaseData['adjustedMinutes'][i] = databaseData['originalMinutes'][i]
            
    databaseData['patientHours'] = 0
    databaseData['patientDays'] = 0
    for i in range(len(databaseData)):
        databaseData['patientHours'][i] = databaseData['adjustedMinutes'][i] / 60
    for i in range(len(databaseData)):
        databaseData['patientDays'][i] = databaseData['patientHours'][i] / 24
        
    decemberDays = sum(databaseData['patientDays'])

    databaseData['numberOfMidnights'] = 0
    databaseData['daysByCountingMidnights'] = 0
    
    for i in range(len(databaseData)):
        if databaseData['LocationStartDate'][i] < monthStart:
            startMidnight = monthStart
            endMidnight = datetime.datetime(databaseData['LocationEndDate'][i].year, databaseData['LocationEndDate'][i].month, databaseData['LocationEndDate'][i].day, 0, 0, 0)
        elif databaseData['LocationEndDate'][i] > monthEnd:
            endMidnight = datetime.datetime(databaseData['LocationEndDate'][i].year, databaseData['LocationEndDate'][i].month, 1, 0, 0)
            startMidnight = datetime.datetime(databaseData['LocationStartDate'][i].year, databaseData['LocationStartDate'][i].month, databaseData['LocationStartDate'][i].day, 0, 0, 0)
        else:
            startMidnight = datetime.datetime(databaseData['LocationStartDate'][i].year, databaseData['LocationStartDate'][i].month, databaseData['LocationStartDate'][i].day, 0, 0, 0)
            endMidnight = datetime.datetime(databaseData['LocationEndDate'][i].year, databaseData['LocationEndDate'][i].month, databaseData['LocationEndDate'][i].day, 0, 0, 0)
        
        midnightCount = (endMidnight - startMidnight).days
        
        if midnightCount > 0:
            databaseData['numberOfMidnights'][i] = midnightCount
            databaseData['daysByCountingMidnights'][i] = midnightCount
        else:
            databaseData['daysByCountingMidnights'][i] = databaseData['patientDays'][i]
    
    databaseData['roundHoursBeforeDividingIntoDays'] = databaseData['patientHours'].round() / 24
    databaseData['daysRounded'] = databaseData['patientDays'].round()
    
    if month == 1:
        janData = pd.concat([janData, databaseData], ignore_index=True)
        janData['Month'] = 'January'
        janData['Year'] = f'{year}'
    elif month == 2:
        febData = pd.concat([febData, databaseData], ignore_index=True)
        febData['Month'] = 'February'
        febData['Year'] = f'{year}'
    elif month == 3:
        marData = pd.concat([marData, databaseData], ignore_index=True)
        marData['Month'] = 'March'
        marData['Year'] = f'{year}'
    elif month == 4:
        aprData = pd.concat([aprData, databaseData], ignore_index=True)
        aprData['Month'] = 'April'
        aprData['Year'] = f'{year}'
    elif month == 5:
        mayData = pd.concat([mayData, databaseData], ignore_index=True)
        mayData['Month'] = 'May'
        mayData['Year'] = f'{year}'
    elif month == 6:
        junData = pd.concat([junData, databaseData], ignore_index=True)
        junData['Month'] = 'June'
        junData['Year'] = f'{year}'
    elif month == 7:
        julData = pd.concat([julData, databaseData], ignore_index=True)
        julData['Month'] = 'July'
        julData['Year'] = f'{year}'
    elif month == 8:
        augData = pd.concat([augData, databaseData], ignore_index=True)
        augData['Month'] = 'August'
        augData['Year'] = f'{year}'
    elif month == 9:
        sepData = pd.concat([sepData, databaseData], ignore_index=True)
        sepData['Month'] = 'September'
        sepData['Year'] = f'{year}'
    elif month == 10:
        octData = pd.concat([octData, databaseData], ignore_index=True)
        octData['Month'] = 'October'
        octData['Year'] = f'{year}'
    elif month == 11:
        novData = pd.concat([novData, databaseData], ignore_index=True)
        novData['Month'] = 'November'
        novData['Year'] = f'{year}'
    elif month == 12:
        decData = pd.concat([decData, databaseData], ignore_index=True)
        decData['Month'] = 'December'
        decData['Year'] = f'{year}'

totalData = pd.concat([janData, febData, marData, aprData, mayData, junData, julData, augData, sepData, octData, novData, decData], ignore_index=True)
with pd.ExcelWriter("Test SQL Data.xlsx") as writer:
    totalData.to_excel(writer, sheet_name = "Data", index = False)
    