# -*- coding: utf-8 -*-
"""
Created on Thu Aug  7 10:08:46 2025

@author: JW19712
"""

import pandas as pd
import numpy as np
import pyodbc
import hashlib

def hashRowPTID(row):
    return hashlib.sha256(row['PatientID'].encode('utf-8')).hexdigest()
def hashRowVisitID(row):
    return hashlib.sha256(row['VisitID'].encode('utf-8')).hexdigest()
def hashRowAccountNumber(row):
    return hashlib.sha256(row['AccountNumber'].encode('utf-8')).hexdigest()

SQLserver = r'server'
SQLdatabase = 'db'
SQLusername = 'user'
SQLpassword = 'pass'

SQLcxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SQLserver+';DATABASE='+SQLdatabase+';TrustServerCertificate=yes;ENCRYPT=yes;UID='+SQLusername+';PWD='+SQLpassword+';APP=Python - Travis Request for LOS Data')

cursor = SQLcxn.cursor()

LOSData = pd.read_csv('MichaelExport.csv')
MeditechData = pd.read_sql('''SELECT 
HRA.StateFree as [Patient State]
, MCM.Name as [Patient County]
, SRCA.PatientID, SRCA.VisitID, SRCA.AccountNumber
, '' as [Opportunity Days]
, '' as [Avoidable Days]
, CAST(SRCA.AdmitDateTime as DATE) as [Admission Date]
, SRCA.AdmitSource as [Admission Source]
, SRCA.Insurance as [Primary Payer] 
, CAST(SRCA.DischargeDateTime as DATE) as [Discharge Date]
, DISP.Name as [Discharge Status]
, '' as [LOS Observed]
, '' as [LOS Outlier]
, '' as [LOS Expected]
, SRCA.Location_MisLocID as PtLocation
, RegistrationTypeKey_MisRegTypeID

FROM SCRView.dbo.SCR_REG_Census_ALL SRCA
LEFT JOIN Livefdb.dbo.HimRec_Address HRA on HRA.PatientID = SRCA.PatientID
left join SCRView.dbo.SCR_ADM_Discharge SAD on SAD.VisitID = SRCA.VisitID
left join Livefdb.dbo.MisDischDispos_Main DISP on DISP.MisDischDisposID = SAD.DispositionID
LEFT JOIN Livefdb.dbo.MisCnty_Main MCM on MCM.MisCntyID = HRA.County_MisCntyID
left outer join SCRView.dbo.SCH_OM_Order_Info as DO on DO.SourceID = SRCA.SourceID
	and DO.VisitID = SRCA.VisitID
	and DO.Category_OmCatID = 'D/C'
	and DO.OmOrdDictID = 'DCPT'
	and DO.Status <> 'Cancelled'
	and DO.OrderDateTime = (Select Max(MX.OrderDateTime) From SCRView.dbo.SCH_OM_Order_Info as MX
		Where MX.SourceID = DO.SourceID and MX.VisitID = DO.VisitID
			and MX.Category_OmCatID = DO.Category_OmCatID and MX.OmOrdDictID = DO.OmOrdDictID
			and MX.Status <> 'Cancelled')
WHERE SRCA.DischargeDateTime BETWEEN '1/1/2023' AND '12/31/2024'
  AND InpatientOrOutpatient = 'I'
  AND RegistrationTypeKey_MisRegTypeID <> 'ER'
  ''', SQLcxn)

importantData = LOSData[['Account No.', 'LOS', 'MRA Expected LOS']]

mapping = importantData.set_index('Account No.')['MRA Expected LOS']
mapping2 = importantData.set_index('Account No.')['LOS']

MeditechData['LOS Expected'] = MeditechData['AccountNumber'].map(mapping)
MeditechData['LOS Observed'] = MeditechData['AccountNumber'].map(mapping2)

MeditechData['Avoidable Days'] = 0
MeditechData['Opportunity Days'] = 0

difference = MeditechData['LOS Observed'] - MeditechData['LOS Expected']

MeditechData['Avoidable Days'] = np.where(difference > 0, difference, 0)
MeditechData['Opportunity Days'] = np.where(difference < 0, difference, 0)

MeditechData['AnonymizedPatientID'] = MeditechData.apply(hashRowPTID, axis=1)
MeditechData['AnonymizedVisitID'] = MeditechData.apply(hashRowVisitID, axis=1)
MeditechData['AnonymizedAccountNumber'] = MeditechData.apply(hashRowAccountNumber, axis=1)

MeditechData.to_excel('LOS_Output.xlsx')