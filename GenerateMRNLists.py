# -*- coding: utf-8 -*-
"""
Created on Wed Jul 30 12:35:13 2025

@author: JW19712
"""
import re
import pandas as pd
import pyodbc

queryList = [
 '''select * from SCH_UMDPP_MRN
where Measure='WellChild3to6Years'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='TobaccoScreening'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='Hypertension'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='DepressionAdultNEW'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='BreastCancerScreening'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='WellChild0to15Months'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='BMI'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='WellChild15to30Months'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='HbA1c'
and Numerator='Y' ''',
'''select * from SCH_UMDPP_MRN
where Measure='ChildhoodImmunizationsCombo10'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='ColonCancerScreening'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='Statin'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='BMIAdolescents'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='ChildhoodImmunizationsCombo7'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='DepressionAdolescents'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='ImmunizationAdolescents'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='Opioids'
and Numerator='Y' ''',
'''select * from SCH_UMDPP_MRN
where Measure='PostPartum'
and Numerator is null''',
'''select * from SCH_UMDPP_MRN
where Measure='30DayReadmit'
and Numerator='Y' ''',
'''select * from SCH_UMDPP_MRN
where Measure='MedReconciliation'
and Numerator is null'''
 ]

SQLserver = r'server'
SQLdatabase = 'db'
SQLusername = 'user'
SQLpassword = 'pass'

SQLcxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SQLserver+';DATABASE='+SQLdatabase+';TrustServerCertificate=yes;ENCRYPT=yes;UID='+SQLusername+';PWD='+SQLpassword+';APP=Python - Generate Fallot CSVs')

cursor = SQLcxn.cursor()

for query in queryList:
    
    match = re.search(r"Measure\s*=\s*'([^']+)'", query)
    if not match:
        print(f'Measure not found in query: {query}')
        continue
    
    measure = match.group(1)
    
    df = pd.read_sql(query, SQLcxn)
    
    csvFilename = f'MRN_List_{measure}_August2025.csv'
    df['PrefixMedicalRecordNumber'].to_csv(csvFilename, index = False, header = False)
    print(f'Exported: {csvFilename}')
    
cursor.close()
SQLcxn.close()