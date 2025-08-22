# -*- coding: utf-8 -*-
#Incomplete as the project was abandoned
"""
Created on Fri Jun 13 10:09:32 2025

@author: JW19712
"""

import pandas as pd
import os
import glob
import shutil

folderPath = r'\\st-claire.org\users\storage\Information Services\Informatics\Share\Analytics\PainTrack Drop Folder'

excelFile = glob.glob(os.path.join(folderPath, '*.xls'))
if len(excelFile) != 1:
    raise ValueError("Folder must contain only one .xls file.")
    
excelFile = excelFile[0]
csvFile = os.path.splitext(excelFile)[0] + '.csv'

dataFrame = pd.read_excel(excelFile)
dataFrame['Goals'] = dataFrame['Goals'].apply(lambda x: x[:255] if isinstance(x, str) else x)

dataFrame.to_csv(csvFile, index = False)

shutil.move(excelFile, os.path.join(folderPath + r'\Past Data Loads', os.path.basename(excelFile)))