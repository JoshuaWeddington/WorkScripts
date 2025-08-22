# -*- coding: utf-8 -*-
"""
Created on Wed Mar 19 11:11:34 2025

@author: JW19712
"""

import pandas as pd
import pyodbc

SQLserver = r'server'
SQLdatabase = 'db'
SQLusername = 'user'
SQLpassword = 'pass'

SQLcxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SQLserver+';DATABASE='+SQLdatabase+';TrustServerCertificate=yes;ENCRYPT=yes;UID='+SQLusername+';PWD='+SQLpassword)

EDQueryRes = pd.read_sql("""WITH Ranked AS (
	SELECT
		RAQR.*,
		ROW_NUMBER() OVER (PARTITION BY RAQR.VisitID, RAQR.Query_MisQryID ORDER BY RAQR.DateTime ASC) AS rn
	FROM
		RegAcctQuery_Result RAQR
	WHERE 
		RAQR.Query_MisQryID IN ('VS.TEMP', 'VS.RESP', 'VS.BP', 'VS.PULSE')
	)
SELECT     
REGT.SourceID, 
REG.PatientID,
REGT.VisitID, 
REG.AccountNumber, 
HIM.Name, 
HIM.Birthdate,
HIM.Sex,
HIM.LegalSex_MisSexID,
HRD.Race_MisRaceID,
MRM.Mnemonic,
MRM.Name as 'RaceName',
REG.ArrivalDateTime as ServiceDateTime, 
REG.AdmitDateTime, 
REG.Location_MisLocID as AdmittedLocation,
DD.Name AS 'DischDispo', 
REGT.RegistrationTypeDischargeDateTime, 
ADM.Status_EdmEdStatusID, 
coalesce(MAX(DEP.DateTimeID),REGT.RegistrationTypeDischargeDateTime) AS 'DepDateTime', 
PROB.Problems,
MAX(CON.DateTimeID) as ConsultDateTime,
MAX(DIS.DateTimeID) as DecisionToDischargeDateTime,
MAX(RM.DateTimeID) as RoomDateTime,
MAX(DOC.DateTimeID) as ProviderDateTime,
MAX(TR.DateTimeID) as TriageDateTime,
ED.TriageDateTime as TriageDateTime2,
case when LWBS.VisitID is not null then 'LWBS' end as LWBS,
PROV.NameWithCredentialsStored as Provider,
MIDL.NameWithCredentialsStored as Midlevel,
CC.Name as ChiefComplaint,
RAQR1.ValueInfo as InitialTemperature,
RAQR2.ValueInfo as InitialRespiratoryRate,
RAQR3.ValueInfo as InitialBloodPressure,
RAQR4.ValueInfo as InitialPulseRate
FROM         Livefdb.dbo.RegAcct_RegistrationTypes AS REGT LEFT OUTER JOIN
                      Livefdb.dbo.MisDischDispos_Main AS DD ON DD.MisDischDisposID = REGT.RegistrationTypeDischargeDisposition_MisDischDisposID AND 
                      REGT.SourceID = DD.SourceID LEFT OUTER JOIN
                      Livefdb.dbo.RegAcct_Main AS REG ON REG.VisitID = REGT.VisitID AND REG.SourceID = REGT.SourceID 
					  left join Ranked RAQR1 ON RAQR1.VisitID = REG.VisitID AND RAQR1.Query_MisQryID = 'VS.TEMP' AND RAQR1.rn = 1
					  left join Ranked RAQR2 ON RAQR2.VisitID = REG.VisitID AND RAQR2.Query_MisQryID = 'VS.RESP' AND RAQR2.rn = 1
					  left join Ranked RAQR3 ON RAQR3.VisitID = REG.VisitID AND RAQR3.Query_MisQryID = 'VS.BP' AND RAQR3.rn = 1
					  left join Ranked RAQR4 ON RAQR4.VisitID = REG.VisitID AND RAQR4.Query_MisQryID = 'VS.PULSE' AND RAQR4.rn = 1
					  left join Livefdb.dbo.EdmAcct_Main ED on ED.VisitID=REGT.VisitID
					  left join Livefdb.dbo.EdmComplaint_Main CC on CC.EdmComplaintID=ED.ChiefComplaint_EdmComplaintID
					  left join Livefdb.dbo.MisPerson_Names PROV on PROV.UnvUserID=ED.Provider_UnvUserID
					  left join Livefdb.dbo.MisPerson_Names MIDL on MIDL.UnvUserID=ED.MidLevelProvider_UnvUserID
					  INNER JOIN
                      Livefdb.dbo.HimRec_Main AS HIM ON HIM.SourceID = REG.SourceID AND HIM.PatientID = REG.PatientID LEFT OUTER JOIN
					  Livefdb.dbo.HimRec_Data AS HRD ON HRD.SourceID = HIM.SourceID AND HRD.PatientID = HIM.PatientID LEFT OUTER JOIN
					  Livefdb.dbo.MisRace_Main AS MRM ON MRM.SourceID = HRD.SourceID AND MRM.MisRaceID = HRD.Race_MisRaceID LEFT OUTER JOIN
                      Livefdb.dbo.EdmActivity_Activity_ADMIT AS ADM ON ADM.VisitID = REGT.VisitID AND ADM.SourceID = REGT.SourceID LEFT OUTER JOIN
                      Livefdb.dbo.SCR_EMR_Pat_Problems AS PROB ON PROB.SourceID = REGT.SourceID AND PROB.VisitID = REGT.VisitID LEFT OUTER JOIN
                      Livefdb.dbo.EdmActivity_Activity_DEPARTED AS DEP ON DEP.VisitID = REGT.VisitID AND DEP.SourceID = REGT.SourceID
					  left join Livefdb.dbo.EmdActivity_Activity_CONSULT AS CON ON CON.VisitID = REGT.VisitID AND CON.SourceID = REGT.SourceID
					  left join Livefdb.dbo.EdmActivity_Activity_DISCHARGE AS DIS ON DIS.VisitID = REGT.VisitID AND DIS.SourceID = REGT.SourceID
					  left join Livefdb.dbo.EdmActivity_Activity_RECVED AS RM ON RM.VisitID = REGT.VisitID AND RM.SourceID = REGT.SourceID
					  left join Livefdb.dbo.EdmActivity_Activity_DOCTOR AS DOC ON DOC.VisitID = REGT.VisitID AND DOC.SourceID = REGT.SourceID
                                          left join Livefdb.dbo.EdmActivity_Activity_TRIAGE AS TR on TR.VisitID = REGT.VisitID AND TR.SourceID = REGT.SourceID
					  left join (select ED.VisitID from Livefdb.dbo.EdmAcct_Main ED inner join Livefdb.dbo.AbsAcct_Diagnoses DX on DX.VisitID=ED.VisitID and DX.DiagnosisCode_MisDxID in ('Z64.2','Z53.2','Z53.21','Z53.9')) LWBS on LWBS.VisitID=REG.VisitID
WHERE     (REGT.RegistrationTypeKey_MisRegTypeID = 'ER')
and cast(REG.ServiceDateTime as date)>='9/1/2024' and cast(REG.ServiceDateTime as date)<='4/2/2025'
--and REG.Facility_MisFacID='MEDCNTR'
--and DD.Name is not null
group by 
REGT.SourceID, 
REG.PatientID,
REGT.VisitID, 
REG.AccountNumber, 
HIM.Name, 
HIM.Birthdate,
HIM.Sex,
HIM.LegalSex_MisSexID,
HRD.Race_MisRaceID,
MRM.Mnemonic,
MRM.Name,
REG.ArrivalDateTime, 
ED.TriageDateTime,
REG.AdmitDateTime, 
DD.Name, 
REGT.RegistrationTypeDischargeDateTime, 
ADM.Status_EdmEdStatusID, 
PROB.Problems,
case when LWBS.VisitID is not null then 'LWBS' end,
PROV.NameWithCredentialsStored ,
MIDL.NameWithCredentialsStored,
REG.Location_MisLocID,
CC.Name,
RAQR1.ValueInfo,
RAQR2.ValueInfo,
RAQR3.ValueInfo,
RAQR4.ValueInfo""", SQLcxn)

print('Results obtained, sorting...')
EDResultSorted = EDQueryRes.sort_values(by=['PatientID', 'ServiceDateTime'])

EDResultSorted['Readmit_72H'] = 'No'
EDResultSorted['Readmit_30D'] = 'No'

print('Finding readmits...')
for i in range(len(EDResultSorted)):
    pid, dischTime = EDResultSorted.loc[i, ['PatientID', 'RegistrationTypeDischargeDateTime']]
    
    futureVisits = EDResultSorted[(EDResultSorted['PatientID']==pid) & (EDResultSorted['ServiceDateTime'] > dischTime)]
    
    if not futureVisits.empty and (futureVisits['ServiceDateTime'] <= dischTime + pd.Timedelta(hours=72)).any():
        EDResultSorted.at[i, 'Readmit_72H'] = 'Yes'
        
    if not futureVisits.empty and (futureVisits['ServiceDateTime'] <= dischTime + pd.Timedelta(days=30)).any():
        EDResultSorted.at[i, 'Readmit_30D'] = 'Yes'
        
EDResultSorted['AdmittedToHospital?'] = 'No'
        
print('Finding admissions to hospital...')
for i in range(len(EDResultSorted)):
    adm = EDResultSorted.loc[i, 'AdmittedLocation']
    
    if adm == 'ED':
        EDResultSorted.at[i, 'AdmittedToHospital?'] = 'No'
    else:
        EDResultSorted.at[i, 'AdmittedToHospital?'] = 'Yes'
        
print('Adjusting for time within hospital...')
# startDate = pd.to_datetime('2024-12-09')
# endDate = pd.to_datetime('2025-01-06 23:59:59')

# filteredDF = EDResultSorted[(EDResultSorted['ServiceDateTime'] >= startDate) & (EDResultSorted['ServiceDateTime'] <= endDate)]

# startTime = pd.to_datetime('10:00:00').time()
# endTime = pd.to_datetime('18:00:00').time()

# filteredDF = filteredDF[
#                             (filteredDF['ServiceDateTime'].dt.time >= startTime) &
#                             (filteredDF['ServiceDateTime'].dt.time <= endTime)
#                         ]

filteredDF = EDResultSorted
filteredDF['LOS_Hours'] = (filteredDF['RegistrationTypeDischargeDateTime'] - filteredDF['ServiceDateTime']).dt.total_seconds() / 3600
filteredDF[['InitialTemperatureC', 'InitialTemperatureF']] = filteredDF['InitialTemperature'].str.extract(r'\{([\d\.]+)\|([\d\.]+)\}')

filteredDF['AgeAtService'] = (filteredDF['ServiceDateTime'] - filteredDF['Birthdate']).dt.days // 365

def sampleNoDupe(df, n, random_state=None):
    sample = df.sample(n=n, random_state=random_state)
    
    while sample['PatientID'].duplicated().any():
        sample = df.sample(n=n, random_state=random_state)
    return sample

#sampleDFFullCol = sampleNoDupe(filteredDF, 194, random_state=None)

requestedAccounts = pd.read_excel('RequestedAccounts.xlsx')
print('Merging')
sampleDFFullCol = filteredDF[filteredDF['AccountNumber'].isin(requestedAccounts['AccountNumber'])]

sampleDF = sampleDFFullCol[
    ['RaceName', 
    'AgeAtService', 
    'LegalSex_MisSexID', 
    'LOS_Hours', 
    'AdmittedToHospital?', 
    'Readmit_72H', 
    'Readmit_30D', 
    'InitialTemperatureC', 
    'InitialTemperatureF', 
    'InitialPulseRate', 
    'InitialBloodPressure', 
    'InitialRespiratoryRate',
    'Name',
    'AccountNumber',
    'Birthdate',
    'ServiceDateTime',
    'RegistrationTypeDischargeDateTime',
    'AdmittedLocation']
    ]




sampleDF.to_excel("ED Accounts.xlsx", index = False)
