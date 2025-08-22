# Python script to obtain an access token from ADP and extract AOID for future use
#   Author: Josh Weddington
#   Started: 5/3/2023

import json
import threading
import schedule
import time
import requests
import warnings
import pandas as pd
import numpy as np
import pyodbc
from sys import stdout
from datetime import datetime

warnings.filterwarnings("ignore")

def getAccessToken(cert_file, key_file):
    try:
        auth_url = "https://accounts.adp.com/auth/oauth/v2/token"

        client_id = "id"
        client_secret = "secret"
    
        headers = {
                 'user-agent': 'application/x-www-form-urlencoded'
         }

        payload = {
                 'grant_type': 'client_credentials',
                 'client_id': client_id,
                 'client_secret': client_secret
                 }

        response = requests.post(
                     auth_url,
                     cert = (cert_file, key_file),
                     data = payload,
                     headers = headers
         )
        
        time.sleep(0.25)
        
        response_json = response.json()

        if response.ok:
            response_json = response.json()
            access_token = response_json.get('access_token')
            print(f'Access token: {access_token}')
        else:
            print(f'Waiting for 10 seconds due to error obtaining access token: {response.text}')
            time.sleep(10)
            return getAccessToken(cert_file, key_file)
        
        return access_token
    
    except requests.exceptions.RequestException as e:
        print('Access Token Request failed: ', e)
        time.sleep(60)
        return getAccessToken(cert_file, key_file)

def getWorkers(access_token, cert_file, key_file, page_size = 50):
    page_size = 50
    workers_url = 'https://api.adp.com/hr/v2/workers'
    workers_data = []
    retries = 0
    
    headers = {
                    'Accept': 'application/json;masked=false',
                    'Authorization': f'Bearer {access_token}'
            }
    params = {
                    'limit': page_size,
                    'offset': 0
            }
    while True:
            try:
                response = requests.get(workers_url, cert = (cert_file, key_file), headers = headers, params = params)
                response.raise_for_status()
                page_data = response.json()
                
                time.sleep(0.25)
                
                workers_data.extend(page_data['workers'])
                
                if len(page_data['workers']) < page_size:
                    numWorkers = params['offset'] + len(page_data['workers'])
                    stdout.write("\r" + str(numWorkers))
                    stdout.flush()
                    break
                
                params['offset'] += page_size
                totalOffset = params['offset']
                stdout.write("\r" + str(params['offset']))
                stdout.flush()
            except requests.exceptions.RequestException as e:
                    totalOffset = params['offset']
                    print(f'Error retrieving workers, waiting to retry offset {totalOffset}: ', e)
                    retries += 1
                    if retries > 3:
                        break
                    time.sleep(5)
    stdout.write("\n")
    return workers_data
    
def getReportsToJobDescription(worker, workers_data, assignments):
    idToFind = worker['workAssignments'][assignments]['reportsTo'][0]['associateOID']
    for innerWorker in workers_data:
        if idToFind == innerWorker['associateOID']:
            if "jobCode" in innerWorker['workAssignments'][0]:
                if "shortName" in innerWorker['workAssignments'][0]['jobCode']:
                    return innerWorker['workAssignments'][0]['jobCode']['shortName'] 
                elif "longName" in innerWorker['workAssignments'][0]['jobCode']:
                    return innerWorker['workAssignments'][0]['jobCode']['longName']
            else:
                return "jobCodeDescriptionUnavailable"

def getNAICS(worker, assignments):
    if "occupationalClassifications" in worker['workAssignments'][assignments]:
        for classification in worker['workAssignments'][assignments]['occupationalClassifications']:
            if classification['nameCode']['codeValue'] == "NAICS":
                if 'shortName' in classification['classificationCode']:
                    return classification['classificationCode']['shortName']
                elif 'longName' in classification['classificationCode']:
                    return classification['classificationCode']['longName']
                else:
                    return ""
    else:
        return ""

def getJobClassDesc(worker, assignments):
    if "occupationalClassifications" in worker['workAssignments'][assignments]:
        for classification in worker['workAssignments'][assignments]['occupationalClassifications']:
            if classification['nameCode']['codeValue'] == "Job Class":
                if 'shortName' in classification['classificationCode']:
                    return classification['classificationCode']['shortName']
                elif 'longName' in classification['classificationCode']:
                    return classification['classificationCode']['longName']
                else:
                    return ""
    else:
        return ""
        
def getEEOC(worker, assignments):
    output = ""
    if "occupationalClassifications" in worker['workAssignments'][assignments]:
        for classification in worker['workAssignments'][assignments]['occupationalClassifications']:
            if classification['nameCode']['codeValue'] == "EEOC":
                if 'shortName' in classification['classificationCode']:
                    output = classification['classificationCode']['shortName']
                    break
                elif 'longName' in classification['classificationCode']:
                    output = classification['classificationCode']['longName']
                    break
                else:
                    output = ""
                    
    return output

def getAOIDs(workers_data):
    AOIDS = []
    for worker in workers_data:
        AOIDS.append(worker['associateOID'])
    return AOIDS

def extractLicenses(licenseData, licensesCertsExtract):
    count = 0
    
    now = datetime.now()
    updateTime = now.strftime("%Y/%m/%d %H:%M:%S")
    
    for worker in licenseData:
        if not worker[0] == "":
            for licenseListElement in worker[0]:
                singleLicense = pd.Series({
                        'fileNumber': worker[2],
                        'categoryCode': (lambda licenseListElement: licenseListElement['categoryCode']['codeValue'] if "categoryCode" in licenseListElement else "")(licenseListElement),
                        'categoryDescription': (lambda licenseListElement: licenseListElement['categoryCode']['shortName'] if "categoryCode" in licenseListElement else "")(licenseListElement),
                        'comments': (lambda licenseListElement: licenseListElement['comments'] if "comments" in licenseListElement else "")(licenseListElement),
                        'effectiveDate': (lambda licenseListElement: licenseListElement['firstIssueDate'] if "firstIssueDate" in licenseListElement else "")(licenseListElement),
                        'employeeLicenseDetailID': (lambda licenseListElement: licenseListElement['customFieldGroup']['stringFields'][0]['stringValue'] if "stringValue" in licenseListElement['customFieldGroup']['stringFields'][0] else "")(licenseListElement),
                        'expirationDate': (lambda licenseListElement: licenseListElement['expirationDate'] if "expirationDate" in licenseListElement else "")(licenseListElement),
                        'issuedBy': (lambda licenseListElement: licenseListElement['issuingParty']['nameCode']['longName'] if "issuingParty" in licenseListElement else "")(licenseListElement),
                        'licenseOrCertificationCode': (lambda licenseListElement: licenseListElement['licenseNameCode']['codeValue'] if "licenseNameCode" in licenseListElement else "")(licenseListElement),
                        'licenseOrCertificationDescription': (lambda licenseListElement: licenseListElement['licenseNameCode']['longName'] if "licenseNameCode" in licenseListElement else "")(licenseListElement),
                        'licenseOrCertificationID': (lambda licenseListElement: licenseListElement['licenseID']['idValue'] if "licenseID" in licenseListElement else "")(licenseListElement),
                        'renewalRequirement': "", #No examples found of having a renewal requirement
                        'lastUpdated': updateTime
                        })
                licensesCertsExtract.loc[len(licensesCertsExtract.index)] = singleLicense
        if not worker[1] == "":
            for licenseListElement in worker[1]:
                singleCert = pd.Series({
                        'fileNumber': worker[2],
                        'categoryCode': (lambda licenseListElement: licenseListElement['categoryCode']['codeValue'] if "categoryCode" in licenseListElement else "")(licenseListElement),
                        'categoryDescription': (lambda licenseListElement: licenseListElement['categoryCode']['shortName'] if "categoryCode" in licenseListElement else "")(licenseListElement),
                        'comments': (lambda licenseListElement: licenseListElement['comments'] if "comments" in licenseListElement else "")(licenseListElement),
                        'effectiveDate': (lambda licenseListElement: licenseListElement['firstIssueDate'] if "firstIssueDate" in licenseListElement else "")(licenseListElement),
                        'employeeLicenseDetailID': (lambda licenseListElement: licenseListElement['customFieldGroup']['stringFields'][0]['stringValue'] if "stringValue" in licenseListElement['customFieldGroup']['stringFields'][0] else "")(licenseListElement),
                        'expirationDate': (lambda licenseListElement: licenseListElement['expirationDate'] if "expirationDate" in licenseListElement else "")(licenseListElement),
                        'issuedBy': (lambda licenseListElement: licenseListElement['issuingParty']['nameCode']['longName'] if "issuingParty" in licenseListElement else "")(licenseListElement),
                        'licenseOrCertificationCode': (lambda licenseListElement: licenseListElement['licenseNameCode']['codeValue'] if "licenseNameCode" in licenseListElement else "")(licenseListElement),
                        'licenseOrCertificationDescription': (lambda licenseListElement: licenseListElement['licenseNameCode']['longName'] if "licenseNameCode" in licenseListElement else "")(licenseListElement),
                        'licenseOrCertificationID': (lambda licenseListElement: licenseListElement['certificationID']['idValue'] if "licenseID" in licenseListElement else "")(licenseListElement),
                        'renewalRequirement': "", #No examples found of having a renewal requirement
                        'lastUpdated': updateTime
                        })
                licensesCertsExtract.loc[len(licensesCertsExtract.index)] = singleCert
        stdout.write("\r" + str(count))
        count +=1  
    return licensesCertsExtract

def getLicenses(AOIDS, workers_data, access_token, cert_file, key_file):
    licensesAndCertsData = []
    count = 0
    startTime = time.time()
    initTime = time.time()
    print("\nSTART LICENSE AND CERTIFICATIONS AT", time.strftime("%H:%M:%S", time.localtime()))
    while count < len(AOIDS):
        currentTime = time.time()
        
        if currentTime - startTime > 3500:
            access_token = getAccessToken(cert_file, key_file)
            startTime = time.time()
        
        licensesUrl = f'https://api.adp.com/talent/v2/associates/{AOIDS[count]}/associate-licenses'
        certsUrl = f'https://api.adp.com/talent/v2/associates/{AOIDS[count]}/associate-certifications'
        try:
            headers = {
                        'Accept': 'application/json;masked=false',
                        'Authorization': f'Bearer {access_token}'
                }
            
            stdout.write("\r" + str(count) + " - " + time.strftime("%H:%M:%S", time.gmtime(currentTime - initTime)))
            stdout.flush()
            
            responseLicense = requests.get(licensesUrl, cert = (cert_file, key_file), headers = headers)
            responseLicense.raise_for_status()
            time.sleep(0.21)
            
            responseCerts = requests.get(certsUrl, cert = (cert_file, key_file), headers = headers)
            responseCerts.raise_for_status()
            time.sleep(0.21)
            
            fileNumber = ""
            
            for worker in workers_data:
                if worker['associateOID'] == AOIDS[count]:
                    fileNumber = worker['workerID']['idValue']
                    break
            
            if responseCerts.status_code == 204 and responseLicense.status_code == 204:
                count += 1
                continue
            
            if responseLicense.status_code == 204 and not responseCerts.status_code == 204:
                certsData = responseCerts.json()
                licensesAndCertsData.append(["", certsData['associateCertifications'], fileNumber])
                count += 1
                continue
            
            if not responseLicense.status_code == 204 and responseCerts.status_code == 204:
                licenseData = responseLicense.json()
                licensesAndCertsData.append([licenseData['associateLicenses'], "", fileNumber])
                count += 1
                continue
            
            if not responseLicense.status_code == 204 and not responseCerts.status_code == 204:
                certsData = responseCerts.json()
                licenseData = responseLicense.json()
                licensesAndCertsData.append([licenseData['associateLicenses'], certsData['associateCertifications'], fileNumber])
                count += 1
                continue
            
            
        except requests.exceptions.RequestException as e:
            print(f'\nError retrieving schedules, waiting to retry {count}: ', e)
            time.sleep(5)
    return licensesAndCertsData

def extractAssociates(workers_data):
    count = 0
    associateInfo = pd.DataFrame(columns = ['fileNumber', 'SCHUsername', 'positionID', 'legalLastName', 'legalFirstName', 'legalMiddleName', 'raceCode', 
                                            'genderForInsuranceCoverage', 'rehireDate', 'hireDate', 'birthDate', 'positionStatus', 
                                            'personalContactHomePhone', 'personalContactMobilePhone', 'benefitsElegibilityCode', 
                                            'benefitsElegibilityDescription', 'primaryAddressLineOne', 'primaryAddressCity',
                                            'primaryAddressZipCode', 'primaryAddressStateOrTerritory', 'positionStartDate',
                                            'jobTitleCode', 'jobTitleDescription', 'seniorityDate', 'adjustedServiceDate',
                                            'FLSADescription', 'FLSACode', 'ethnicity', 'workerCategoryCode',
                                            'workerCategoryDescription', 'hoursPeriod', 'scheduledHours', 'FTE',
                                            'assignedShiftDescription', 'homeDepartmentCode', 'homeDepartmentDescription',
                                            'payGradeCode', 'payGradeDescription', 'rateTwo', 'regularPayRateAmount',
                                            'annualSalary', 'WFMLogonProfile', 'WFMPayRule', 'WFMBadge', 'WFMAccrualProfile',
                                            'WFMHHEmployeeType', 'sixtyDayReviewDueDate', 'annualReviewDueDate', 'creditedServiceDate', 'associateID',
                                            'rehireReasonDescription', 'managementPosition', 'jobFunctionDescription',
                                            'jobClassDescription', 'NAICSWorkersCompDescription', 'EEOCJobClassification',
                                            'reportsToLegalFirstName', 'reportsToLegalLastName', 'reportsToJobTitleDescription',
                                            'standardHours', 'locationDescription', 'homeCostNumberDescription', 'workContactEmail',
                                            'workContactPhone', 'personalContactEmail', 'otherLastNamesUsed', 'preferredFirstName',
                                            'taxIDSSN', 'terminationDate', 'terminationReasonCode', 'terminationReasonDescription',
                                            'voluntaryTerminationFlag', 'rehireEligibility', 'taxID'])
    for worker in workers_data:
        count += 1
        for assignments in range(len(worker['workAssignments'])):
            if worker['workAssignments'][assignments]['primaryIndicator']:
                singleAssociate = pd.Series({
                'fileNumber': worker['workerID']['idValue'],
                'SCHUsername': (lambda worker: worker['customFieldGroup']['stringFields'][0]['stringValue'] if "stringValue" in worker['customFieldGroup']['stringFields'][0] else "")(worker),
                'positionID': (lambda worker: worker['workAssignments'][assignments]['positionID'] if 'positionID' in worker['workAssignments'][assignments] else "")(worker),
                'legalLastName': worker['person']['legalName']['familyName1'],
                'legalFirstName': worker['person']['legalName']['givenName'],
                'legalMiddleName': (lambda worker: worker['person']['legalName']['middleName'] if "middleName" in worker['person']['legalName'] else "")(worker), 
                
                    #lambda denotes an anonymous function with a simple definition --- useful for things like this where there are many different 
                    #   checks that need to be made in a declaration. Simpler to define than using a large branching if else tree.
                    #
                    #   Reads as follows: legalMiddleName is worker['person']['legalName']['middleName'] if middleName is present, 
                    #       otherwise it is an empty string
                    #   By defining lambda as a function with a variable named worker, we can pass worker as such: (lambda: worker)(worker)
                    
                'raceCode': (lambda worker: worker['person']['raceCode']['codeValue'] if "raceCode" in worker['person'] else "")(worker),
                'genderForInsuranceCoverage': worker['person']['genderCode']['longName'],
                'rehireDate': (lambda worker: worker['workerDates']['rehireDate'] if "rehireDate" in worker['workerDates'] else "")(worker),
                'hireDate': worker['workerDates']['originalHireDate'],
                'birthDate': worker['person']['birthDate'],
                'positionStatus': worker['workAssignments'][assignments]['assignmentStatus']['statusCode']['longName'],
                'personalContactHomePhone': (lambda worker: worker['person']['communication']['landlines'][0]['countryDialing'] + worker['person']['communication']['landlines'][0]['areaDialing'] + worker['person']['communication']['landlines'][0]['dialNumber'] if "communication" in worker['person'] and "landlines" in worker['person']['communication'] else "")(worker),
                'personalContactMobilePhone': (lambda worker: worker['person']['communication']['mobiles'][0]['countryDialing'] + worker['person']['communication']['mobiles'][0]['areaDialing'] + worker['person']['communication']['mobiles'][0]['dialNumber'] if "communication" in worker['person'] and "mobiles" in worker['person']['communication'] and "areaDialing" in worker['person']['communication']['mobiles'][0] else (worker['person']['communication']['mobiles'][0]['countryDialing'] + worker['person']['communication']['mobiles'][0]['dialNumber'] if "communication" in worker['person'] and "mobiles" in worker['person']['communication'] else ""))(worker),
                'benefitsElegibilityCode': (lambda worker: worker['workAssignments'][assignments]['workerGroups'][0]['groupCode']['codeValue'] if 'workerGroups' in worker['workAssignments'][assignments] else "")(worker),
                'benefitsElegibilityDescription': (lambda worker: worker['workAssignments'][assignments]['workerGroups'][0]['groupCode']['shortName'] if 'workerGroups' in worker['workAssignments'][assignments] and "shortName" in worker['workAssignments'][assignments]['workerGroups'][0]['groupCode'] else (worker['workAssignments'][assignments]['workerGroups'][0]['groupCode']['longName'] if 'workerGroups' in worker['workAssignments'][assignments] and "longName" in worker['workAssignments'][assignments]['workerGroups'][0]['groupCode'] else ""))(worker),
                'primaryAddressLineOne': (lambda worker: worker['person']['legalAddress']['lineOne'] if "legalAddress" in worker['person'] else "")(worker),
                'primaryAddressCity': (lambda worker: worker['person']['legalAddress']['cityName'] if "legalAddress" in worker['person'] else "")(worker),
                'primaryAddressZipCode': (lambda worker: worker['person']['legalAddress']['postalCode'] if "legalAddress" in worker['person'] else "")(worker),
                'primaryAddressStateOrTerritory': (lambda worker: worker['person']['legalAddress']['countrySubdivisionLevel1']['shortName'] if "legalAddress" in worker['person'] else "")(worker),
                'positionStartDate': worker['workAssignments'][assignments]['actualStartDate'],
                'jobTitleCode': (lambda worker: worker['workAssignments'][assignments]['jobCode']['codeValue'] if "jobCode" in worker['workAssignments'][assignments] else "")(worker),
                'jobTitleDescription': (lambda worker: worker['workAssignments'][assignments]['jobCode']['longName'] if "jobCode" in worker['workAssignments'][assignments] and "longName" in worker['workAssignments'][assignments]['jobCode'] else (worker['workAssignments'][assignments]['jobCode']['shortName'] if"jobCode" in worker['workAssignments'][assignments] and "shortName" in worker['workAssignments'][assignments]['jobCode'] else ""))(worker),
                'seniorityDate': (lambda worker: worker['workAssignments'][assignments]['seniorityDate'] if "seniorityDate" in worker['workAssignments'][assignments] else "")(worker),
                'adjustedServiceDate': (lambda worker: worker['workerDates']['adjustedServiceDate'] if "adjustedServiceDate" in worker['workerDates'] else "")(worker),
                'FLSADescription': (lambda worker: worker['workAssignments'][assignments]['wageLawCoverage']['coverageCode']['shortName'] if "wageLawCoverage" in worker['workAssignments'][assignments] else "")(worker),
                'FLSACode': (lambda worker: worker['workAssignments'][assignments]['wageLawCoverage']['coverageCode']['codeValue'] if "wageLawCoverage" in worker['workAssignments'][assignments] else "")(worker),
                'ethnicity': (lambda worker: worker['person']['ethnicityCode']['longName'] if "ethnicityCode" in worker['person'] else "")(worker),
                'workerCategoryCode': (lambda worker: worker['workAssignments'][assignments]['workerTypeCode']['codeValue'] if "workerTypeCode" in worker['workAssignments'][assignments] else "")(worker),
                'workerCategoryDescription': (lambda worker: worker['workAssignments'][assignments]['workerTypeCode']['shortName'] if "workerTypeCode" in worker['workAssignments'][assignments] else "")(worker),
                'hoursPeriod': (lambda worker: worker['workAssignments'][assignments]['standardHours']['unitCode']['shortName'] if "standardHours" in worker['workAssignments'][assignments] and "unitCode" in worker['workAssignments'][assignments]['standardHours'] else "")(worker),
                'scheduledHours': (lambda worker: worker['workAssignments'][assignments]['standardHours']['hoursQuantity'] if "standardHours" in worker['workAssignments'][assignments] and "hoursQuantity" in worker['workAssignments'][assignments]['standardHours'] else "")(worker),
                'FTE': (lambda worker: worker['workAssignments'][assignments]['fullTimeEquivalenceRatio'] if "fullTimeEquivalenceRatio" in worker['workAssignments'][assignments] else "")(worker),
                'assignedShiftDescription': (lambda worker: worker['workAssignments'][assignments]['workShiftCode']['shortName'] if "workShiftCode" in worker['workAssignments'][assignments] else "")(worker),
                'homeDepartmentCode': (lambda worker: worker['workAssignments'][assignments]['assignedOrganizationalUnits'][0]['nameCode']['codeValue'] if "assignedOrganizationalUnits" in worker['workAssignments'][assignments] else "")(worker),
                'homeDepartmentDescription': (lambda worker: worker['workAssignments'][assignments]['assignedOrganizationalUnits'][0]['nameCode']['shortName'] if "assignedOrganizationalUnits" in worker['workAssignments'][assignments] and "shortName" in worker['workAssignments'][assignments]['assignedOrganizationalUnits'][0]['nameCode'] else (worker['workAssignments'][assignments]['assignedOrganizationalUnits'][0]['nameCode']['longName'] if "assignedOrganizationalUnits" in worker['workAssignments'][assignments] and "longName" in worker['workAssignments'][assignments]['assignedOrganizationalUnits'][0]['nameCode'] else ""))(worker),
                'payGradeCode': (lambda worker: worker['workAssignments'][assignments]['payGradeCode']['codeValue'] if "payGradeCode" in worker['workAssignments'][assignments] else "")(worker),
                'payGradeDescription': (lambda worker: worker['workAssignments'][assignments]['payGradeCode']['shortName'] if "payGradeCode" in worker['workAssignments'][assignments] and "shortName" in worker['workAssignments'][assignments]['payGradeCode'] else (worker['workAssignments'][assignments]['payGradeCode']['longName'] if "payGradeCode" in worker['workAssignments'][assignments] and "longName" in worker['workAssignments'][assignments]['payGradeCode'] else ""))(worker),
                'rateTwo': (lambda worker: worker['workAssignments'][assignments]['additionalRemunerations'][0]['rate']['amountValue'] if "additionalRemunerations" in worker['workAssignments'][assignments] else None)(worker),
                'regularPayRateAmount': (lambda worker: worker['workAssignments'][assignments]['baseRemuneration']['hourlyRateAmount']['amountValue'] if "baseRemuneration" in worker['workAssignments'][assignments] and "hourlyRateAmount" in worker['workAssignments'][assignments]['baseRemuneration'] else None)(worker),
                'annualSalary': (lambda worker: worker['workAssignments'][assignments]['baseRemuneration']['annualRateAmount']['amountValue'] if "baseRemuneration" in worker['workAssignments'][assignments] else "0")(worker),
                'WFMLogonProfile': (lambda worker: worker['customFieldGroup']['codeFields'][3]['codeValue'] if "codeValue" in worker['customFieldGroup']['codeFields'][3] else "")(worker),
                'WFMPayRule': (lambda worker: worker['customFieldGroup']['codeFields'][4]['codeValue'] if "codeValue" in worker['customFieldGroup']['codeFields'][4] else "")(worker),
                'WFMBadge': (lambda worker: worker['customFieldGroup']['stringFields'][2]['stringValue'] if "stringValue" in worker['customFieldGroup']['stringFields'][2] else "")(worker),
                'WFMAccrualProfile': (lambda worker: worker['customFieldGroup']['codeFields'][1]['codeValue'] if "codeValue" in worker['customFieldGroup']['codeFields'][1] else "")(worker),
                'WFMHHEmployeeType': (lambda worker: worker['customFieldGroup']['codeFields'][0]['codeValue'] if "codeValue" in worker['customFieldGroup']['codeFields'][0] else "")(worker),
                'sixtyDayReviewDueDate': (lambda worker: worker['customFieldGroup']['dateFields'][1]['dateValue'] if "dateValue" in worker['customFieldGroup']['dateFields'][1] else None)(worker),
                'annualReviewDueDate': (lambda worker: worker['customFieldGroup']['dateFields'][0]['dateValue'] if "dateValue" in worker['customFieldGroup']['dateFields'][0] else None)(worker),
                'creditedServiceDate': "", # No examples found of having a credited service date
                'associateID': worker['workerID']['idValue'],
                'rehireReasonDescription': (lambda worker: worker['workAssignments'][assignments]['assignmentStatus']['reasonCode']['shortName'] if "reasonCode" in worker['workAssignments'][assignments]['assignmentStatus'] and worker['workAssignments'][assignments]['assignmentStatus']['statusCode']['shortName'] == "Active" else "")(worker),
                'managementPosition': (lambda worker: "Yes" if worker['workAssignments'][assignments]['managementPositionIndicator'] else "No")(worker),
                'jobFunctionDescription': (lambda worker: worker['workAssignments'][assignments]['jobFunctionCode']['shortName'] if "jobFunctionCode" in worker['workAssignments'][assignments] and "shortName" in worker['workAssignments'][assignments]['jobFunctionCode'] else (worker['workAssignments'][assignments]['jobFunctionCode']['longName'] if "jobFunctionCode" in worker['workAssignments'][assignments] and "longName" in worker['workAssignments'][assignments]['jobFunctionCode'] else ""))(worker),
                'jobClassDescription': getJobClassDesc(worker, assignments),#(lambda worker: worker['workAssignments'][assignments]['occupationalClassifications'][1]['classificationCode']['shortName'] if "occupationalClassifications" in worker['workAssignments'][assignments] and len(worker['workAssignments'][assignments]['occupationalClassifications']) > 1 and "shortName" in worker['workAssignments'][assignments]['occupationalClassifications'][1]['classificationCode'] else (worker['workAssignments'][assignments]['occupationalClassifications'][1]['classificationCode']['longName'] if "occupationalClassifications" in worker['workAssignments'][assignments] and len(worker['workAssignments'][assignments]['occupationalClassifications']) > 1 and "longName" in worker['workAssignments'][assignments]['occupationalClassifications'][1]['classificationCode'] else ""))(worker),
                'NAICSWorkersCompDescription': getNAICS(worker, assignments), #CHECK
                'EEOCJobClassification': getEEOC(worker, assignments), #(lambda worker: worker['workAssignments'][assignments]['occupationalClassifications'][0]['classificationCode']['shortName'] if "occupationalClassifications" in worker['workAssignments'][assignments] and "shortName" in worker['workAssignments'][assignments]['occupationalClassifications'][0]['classificationCode'] else (worker['workAssignments'][assignments]['occupationalClassifications'][0]['classificationCode']['longName'] if "occupationalClassifications" in worker['workAssignments'][assignments] and "longName" in worker['workAssignments'][assignments]['occupationalClassifications'][0]['classificationCode'] else ""))(worker),
                'reportsToLegalFirstName': (lambda worker: worker['workAssignments'][assignments]['reportsTo'][0]['reportsToWorkerName']['formattedName'].split()[0] if "reportsTo" in worker['workAssignments'][assignments] else "")(worker),   #****** - Needs to be parsed for First Name
                'reportsToLegalLastName': (lambda worker: worker['workAssignments'][assignments]['reportsTo'][0]['reportsToWorkerName']['formattedName'].split()[-1] if "reportsTo" in worker['workAssignments'][assignments] else "")(worker),     #****** - Needs to be parsed for Last Name
                
                'reportsToJobTitleDescription': (lambda worker: getReportsToJobDescription(worker, workers_data, assignments) if "reportsTo" in worker['workAssignments'][assignments] else "")(worker),  
                
                'standardHours': (lambda worker: worker['workAssignments'][assignments]['standardPayPeriodHours']['hoursQuantity'] if "standardPayPeriodHours" in worker['workAssignments'][assignments] else "")(worker),
                'locationDescription': (lambda worker: worker['workAssignments'][assignments]['homeWorkLocation']['nameCode']['longName'] if "homeWorkLocation" in worker['workAssignments'][assignments] and "longName" in worker['workAssignments'][assignments]['homeWorkLocation']['nameCode'] else (worker['workAssignments'][assignments]['homeWorkLocation']['nameCode']['shortName'] if "homeWorkLocation" in worker['workAssignments'][assignments] and "shortName" in worker['workAssignments'][assignments]['homeWorkLocation']['nameCode'] else ""))(worker),
                
                'homeCostNumberDescription': (lambda worker: worker['workAssignments'][assignments]['homeOrganizationalUnits'][1]['nameCode']['longName'] 
                                              if "homeOrganizationalUnits" in worker['workAssignments'][assignments] and len(worker['workAssignments'][assignments]['homeOrganizationalUnits']) > 1 and "longName" in worker['workAssignments'][assignments]['homeOrganizationalUnits'][1]['nameCode'] 
                                                  else (worker['workAssignments'][assignments]['homeOrganizationalUnits'][1]['nameCode']['shortName'] 
                                                        if ("homeOrganizationalUnits" in worker['workAssignments'][assignments] and len(worker['workAssignments'][assignments]['homeOrganizationalUnits']) > 1 and "shortName" in worker['workAssignments'][assignments]['homeOrganizationalUnits'][1]['nameCode']) 
                                                        else ""))(worker),
                
                'workContactEmail': (lambda worker: worker['businessCommunication']['emails'][0]['emailUri'] if "businessCommunication" in worker and "emails" in worker['businessCommunication'] else "")(worker),
                'workContactPhone': (lambda worker: worker['businessCommunication']['landlines'][0]['countryDialing'] + worker['businessCommunication']['landlines'][0]['areaDialing'] + worker['businessCommunication']['landlines'][0]['dialNumber'] if "businessCommunication" in worker and "landlines" in worker['businessCommunication'] else "")(worker),
                'personalContactEmail': (lambda worker: worker['person']['communication']['emails'][0]['emailUri'] if "communication" in worker['person'] and "emails" in worker['person']['communication']  else "")(worker),
                'otherLastNamesUsed': (lambda worker: worker['person']['birthName']['familyName1'] if "birthName" in worker['person'] else "")(worker),
                'preferredFirstName': (lambda worker: worker['person']['preferredName']['givenName'] if "givenName" in worker['person']['preferredName'] else "")(worker),
                'taxIDSSN': (lambda worker: worker['person']['governmentIDs'][0]['idValue'] if "governmentIDs" in worker['person'] else "")(worker),
                'terminationDate': (lambda worker: worker['workerDates']['terminationDate'] if "terminationDate" in worker['workerDates'] else "")(worker),
                'terminationReasonCode': (lambda worker: worker['workAssignments'][assignments]['assignmentStatus']['reasonCode']['codeValue'] if "reasonCode" in worker['workAssignments'][assignments]['assignmentStatus'] and worker['workAssignments'][assignments]['assignmentStatus']['statusCode']['longName'] == "Terminated" else "")(worker),
                'terminationReasonDescription': (lambda worker: worker['workAssignments'][assignments]['assignmentStatus']['reasonCode']['longName'] if "reasonCode" in worker['workAssignments'][assignments]['assignmentStatus'] and "longName" in worker['workAssignments'][assignments]['assignmentStatus']['reasonCode'] and worker['workAssignments'][assignments]['assignmentStatus']['statusCode']['longName'] == "Terminated" else (worker['workAssignments'][assignments]['assignmentStatus']['reasonCode']['shortName'] if "reasonCode" in worker['workAssignments'][assignments]['assignmentStatus'] and "shortName" in worker['workAssignments'][assignments]['assignmentStatus']['reasonCode'] and worker['workAssignments'][assignments]['assignmentStatus']['statusCode']['longName'] == "Terminated" else ""))(worker),
                'voluntaryTerminationFlag': (lambda worker: "Voluntary" if "voluntaryIndicator" in worker['workAssignments'][assignments] and worker['workAssignments'][assignments]['voluntaryIndicator'] else ("Involuntary" if "voluntaryIndicator" in worker['workAssignments'][assignments] and not worker['workAssignments'][assignments]['voluntaryIndicator'] else ""))(worker),
                'rehireEligibility': "",
                'taxID': ""                            
        })
                associateInfo.loc[len(associateInfo.index)] = singleAssociate
        stdout.write("\r" + str(count) + "/" + str(len(workers_data)))
        stdout.flush()
    return associateInfo

def executePollFilemaker():
    startTime = time.time()
    cert_file = 'C:/Users/JWeddington/Desktop/cer-key/j.greenhill_st-claire.org.cer'
    key_file = 'C:/Users/JWeddington/Desktop/cer-key/sch_auth.key'
    
    access_token = getAccessToken(cert_file, key_file)
    workers_data = getWorkers(access_token, cert_file, key_file)
    associateInfo = extractAssociates(workers_data)
    #associateInfo = associateInfo.astype(str)
    associateInfo = associateInfo.replace({np.nan: None})
    
    SQLserver = r'server'
    SQLdatabase = 'db'
    SQLusername = 'user'
    SQLpassword = 'pass'
    
    SQLcxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SQLserver+';DATABASE='+SQLdatabase+';TrustServerCertificate=yes;ENCRYPT=yes;UID='+SQLusername+';PWD='+SQLpassword+';APP=Python - Employee Profiles')
    cursor = SQLcxn.cursor()
    
    insertString = "INSERT INTO dbo.SCH_Employee_Profiles_ADP (fileNumber, SCHUsername, positionID, legalLastName, legalFirstName, legalMiddleName, raceCode, genderForInsuranceCoverage, rehireDate, hireDate, birthDate, positionStatus, personalContactHomePhone, personalContactMobilePhone, benefitsElegibilityCode, benefitsElegibilityDescription, primaryAddressLineOne, primaryAddressCity, primaryAddressZipCode, primaryAddressStateOrTerritory, positionStartDate, jobTitleCode, jobTitleDescription, seniorityDate, adjustedServiceDate, FLSADescription, FLSACode, ethnicity, workerCategoryCode, workerCategoryDescription, hoursPeriod, scheduledHours, FTE, assignedShiftDescription, homeDepartmentCode, homeDepartmentDescription, payGradeCode, payGradeDescription, rateTwo, regularPayRateAmount, annualSalary, WFMLogonProfile, WFMPayRule, WFMBadge, WFMAccrualProfile, WFMHHEmployeeType, sixtyDayReviewDueDate, annualReviewDueDate, creditedServiceDate, associateID, rehireReasonDescription, managementPosition, jobFunctionDescription, jobClassDescription, NAICSWorkersCompDescription, EEOCJobClassification, reportsToLegalFirstName, reportsToLegalLastName, reportsToJobTitleDescription, standardHours, locationDescription, homeCostNumberDescription, workContactEmail, workContactPhone, personalContactEmail, otherLastNamesUsed, preferredFirstName, taxIDSSN, terminationDate, terminationReasonCode, terminationReasonDescription, voluntaryTerminationFlag, rehireEligibility, taxID, lastUpdated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    
    # databaseData = pd.read_sql("SELECT * FROM dbo.SCH_Employee_Profiles_ADP", SQLcxn)
    # databaseData = databaseData.drop(columns =['lastUpdated'])
    # databaseData = databaseData.astype(str)
    # associateInfo = pd.concat([associateInfo, databaseData])
    associateInfo = associateInfo.drop_duplicates()
    downStartTime = time.time()
    try:
        cursor.execute("DROP TABLE dbo.SCH_Employee_Profiles_ADP")
        cursor.execute("""CREATE TABLE [dbo].[SCH_Employee_Profiles_ADP](
        	[fileNumber] [varchar](50) NULL,
            [SCHUsername] [varchar](50) NULL,
            [positionID] [varchar] (50) NULL,
        	[legalFirstName] [varchar](50) NULL,
        	[legalLastName] [varchar](50) NULL,
        	[legalMiddleName] [varchar](50) NULL,
        	[raceCode] [varchar](2) NULL,
        	[genderForInsuranceCoverage] [varchar](15) NULL,
        	[rehireDate] [varchar](50) NULL,
        	[hireDate] [varchar](50) NULL,
        	[birthDate] [varchar](50) NULL,
        	[positionStatus] [varchar](15) NULL,
        	[personalContactHomePhone] [varchar](15) NULL,
        	[personalContactMobilePhone] [varchar](15) NULL,
        	[benefitsElegibilityCode] [varchar](5) NULL,
        	[benefitsElegibilityDescription] [varchar](50) NULL,
        	[primaryAddressLineOne] [varchar](50) NULL,
        	[primaryAddressCity] [varchar](50) NULL,
        	[primaryAddressZipCode] [varchar](10) NULL,
        	[primaryAddressStateOrTerritory] [varchar](20) NULL,
        	[positionStartDate] [varchar](50) NULL,
        	[jobTitleCode] [varchar](10) NULL,
        	[jobTitleDescription] [varchar](50) NULL,
        	[seniorityDate] [varchar](50) NULL,
        	[adjustedServiceDate] [varchar](50) NULL,
        	[FLSADescription] [varchar](50) NULL,
        	[FLSACode] [varchar](50) NULL,
        	[ethnicity] [varchar](50) NULL,
        	[workerCategoryCode] [varchar](50) NULL,
        	[workerCategoryDescription] [varchar](50) NULL,
        	[hoursPeriod] [varchar](50) NULL,
        	[scheduledHours] [varchar](50) NULL,
        	[FTE] [varchar](50) NULL,
        	[assignedShiftDescription] [varchar](50) NULL,
        	[homeDepartmentCode] [varchar](50) NULL,
        	[homeDepartmentDescription] [varchar](50) NULL,
        	[payGradeCode] [varchar](50) NULL,
        	[payGradeDescription] [varchar](50) NULL,
        	[rateTwo] [decimal](15, 4) NULL,
        	[regularPayRateAmount] [decimal](15, 4) NULL,
        	[annualSalary] [decimal](15, 4) NULL,
        	[WFMLogonProfile] [varchar](50) NULL,
        	[WFMPayRule] [varchar](50) NULL,
        	[WFMBadge] [varchar](50) NULL,
        	[WFMAccrualProfile] [varchar](50) NULL,
        	[WFMHHEmployeeType] [varchar](50) NULL,
            [sixtyDayReviewDueDate] [datetime] NULL,
        	[annualReviewDueDate] [datetime] NULL,
        	[creditedServiceDate] [varchar](50) NULL,
        	[associateID] [varchar](50) NULL,
        	[rehireReasonDescription] [varchar](50) NULL,
        	[managementPosition] [varchar](50) NULL,
        	[jobFunctionDescription] [varchar](50) NULL,
        	[jobClassDescription] [varchar](100) NULL,
        	[NAICSWorkersCompDescription] [varchar](100) NULL,
        	[EEOCJobClassification] [varchar](50) NULL,
        	[reportsToLegalFirstName] [varchar](50) NULL,
        	[reportsToLegalLastName] [varchar](50) NULL,
        	[reportsToJobTitleDescription] [varchar](50) NULL,
        	[standardHours] [varchar](50) NULL,
        	[locationDescription] [varchar](50) NULL,
        	[homeCostNumberDescription] [varchar](50) NULL,
        	[workContactEmail] [varchar](60) NULL,
        	[workContactPhone] [varchar](15) NULL,
        	[personalContactEmail] [varchar](100) NULL,
        	[otherLastNamesUsed] [varchar](100) NULL,
        	[preferredFirstName] [varchar](50) NULL,
        	[taxIDSSN] [varchar](15) NULL,
        	[terminationDate] [varchar](50) NULL,
        	[terminationReasonCode] [varchar](10) NULL,
        	[terminationReasonDescription] [varchar](50) NULL,
        	[voluntaryTerminationFlag] [varchar](20) NULL,
        	[rehireEligibility] [varchar](50) NULL,
        	[taxID] [varchar](50) NULL,
            [lastUpdated] [varchar](100) NULL
        ) ON [PRIMARY]""")
    
        now = datetime.now()
        
        count = 0
        stdout.write("\n\n")
        for index in associateInfo.values:
            cursor.execute(insertString, (index[0], index[1], index[2], index[3], index[4], index[5], index[6], index[7], index[8], index[9], index[10], index[11], index[12], index[13], index[14], index[15], index[16], index[17], index[18], index[19], index[20], index[21], index[22], index[23], index[24], index[25], index[26], index[27], index[28], index[29], index[30], index[31], index[32], index[33], index[34], index[35], index[36], index[37], index[38], index[39], index[40], index[41], index[42], index[43], index[44], index[45], index[46], index[47], index[48], index[49], index[50], index[51], index[52], index[53], index[54], index[55], index[56], index[57], index[58], index[59], index[60], index[61], index[62], index[63], index[64], index[65], index[66], index[67], index[68], index[69], index[70], index[71], index[72], index[73], now.strftime("%Y/%m/%d %H:%M:%S")))
            stdout.write("\r" + str(count))
            stdout.flush()
            count+=1
    except Exception as e:
        print(f"error inserting data for employee profiles: {e}")
        SQLcxn.close()
        exit(0)
    finally:
        cursor.commit()
        SQLcxn.close()
        
    
    endTime = time.time()
    
    print("\nExecution time: ", time.strftime("%H:%M:%S", time.gmtime(endTime - startTime)))
    print("Table Down Time: ", time.strftime("%H:%M:%S", time.gmtime(endTime - downStartTime)))
    count = 0
    time.sleep(60)
    
def executePollLicensureData():
    cert_file = 'C:/Users/JWeddington/Desktop/cer-key/j.greenhill_st-claire.org.cer'
    key_file = 'C:/Users/JWeddington/Desktop/cer-key/sch_auth.key'
    
    licenseAccessToken = getAccessToken(cert_file, key_file)
    workers_data = getWorkers(licenseAccessToken, cert_file, key_file)
    AOIDS = getAOIDs(workers_data)
    
    licenseData = getLicenses(AOIDS, workers_data, licenseAccessToken, cert_file, key_file)
    
    licensesCertsExtract = pd.DataFrame(columns = ['fileNumber', 'categoryCode', 'categoryDescription', 'comments', 'effectiveDate', 'employeeLicenseDetailID', 
                                                   'expirationDate', 'issuedBy', 'licenseOrCertificationCode', 'licenseOrCertificationDescription', 
                                                   'licenseOrCertificationID', 'renewalRequirement', 'lastUpdated'])
    
    extractLicenses(licenseData, licensesCertsExtract)
    
    SQLserver = r'server'
    SQLdatabase = 'db'
    SQLusername = 'user'
    SQLpassword = 'pass'
    
    SQLcxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SQLserver+';DATABASE='+SQLdatabase+';TrustServerCertificate=yes;ENCRYPT=yes;UID='+SQLusername+';PWD='+SQLpassword+';APP=Python - Employee Certifications')
    cursor = SQLcxn.cursor()
    
    insertStringLicenses = "INSERT INTO dbo.SCH_Employee_Licenses_ADP (fileNumber, categoryCode, categoryDescription, comments, effectiveDate, employeeLicenseDetailID, expirationDate, issuedBy, licenseOrCertificationCode, licenseOrCertificationDescription, licenseOrCertificationID, renewalRequirement, lastUpdated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    
    try:
        cursor.execute("DROP TABLE [dbo].[SCH_Employee_Licenses_ADP]")
        cursor.execute("""CREATE TABLE [dbo].[SCH_Employee_Licenses_ADP](
        	[fileNumber] [varchar](50) NULL,
        	[categoryCode] [varchar](50) NULL,
        	[categoryDescription] [varchar](50) NULL,
        	[comments] [varchar](300) NULL,
        	[effectiveDate] [varchar](50) NULL,
        	[employeeLicenseDetailID] [varchar](50) NULL,
        	[expirationDate] [varchar](50) NULL,
        	[issuedBy] [varchar](100) NULL,
        	[licenseOrCertificationCode] [varchar](50) NULL,
        	[licenseOrCertificationDescription] [varchar](100) NULL,
        	[licenseOrCertificationID] [varchar](50) NULL,
        	[renewalRequirement] [varchar](50) NULL,
            [lastUpdated] [varchar](50) NULL
        ) ON [PRIMARY]""")
        
        print()
        count = 0
        for index in licensesCertsExtract.values:
            stdout.write("\r" + str(count))
            cursor.execute(insertStringLicenses, tuple(index))
            count += 1
    except:
        print("error inserting data for licenses")
        SQLcxn.close()
        exit(0)
    finally:
        cursor.commit()
        SQLcxn.close()
        time.sleep(30)

def runThread(jobFxn):
    jobThread = threading.Thread(target=jobFxn)
    jobThread.start()

executePollFilemaker()
time.sleep(10)
executePollLicensureData()
