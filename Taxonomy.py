# -*- coding: utf-8 -*-
"""
Created on Thu Aug 14 12:13:37 2025

@author: JW19712
"""

import pandas as pd

taxonomy = pd.read_csv('nucc_taxonomy_251.csv')
bigFileColumns = pd.read_csv('npidata_pfile_20050523-20250810.csv', nrows=0).columns.tolist()

defDictSpec = dict(zip(taxonomy['Code'], taxonomy['Specialization']))
defDictDesc = dict(zip(taxonomy['Code'], taxonomy['Definition']))
defDictClass = dict(zip(taxonomy['Code'], taxonomy['Classification']))

chunkSize = 100000
outFile = 'DefinedTaxonomyCodes2.csv'

recordsWritten = 0
firstChunk = True
for chunk in pd.read_csv('npidata_pfile_20050523-20250810.csv', chunksize = chunkSize):
    chunk['Healthcare Provider Taxonomy Code_1 English'] = chunk['Healthcare Provider Taxonomy Code_1'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_1 English Class'] = chunk['Healthcare Provider Taxonomy Code_1'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_1 English Definition'] = chunk['Healthcare Provider Taxonomy Code_1'].map(defDictDesc)
    
    chunk['Healthcare Provider Taxonomy Code_2 English'] = chunk['Healthcare Provider Taxonomy Code_2'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_2 English Class'] = chunk['Healthcare Provider Taxonomy Code_2'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_2 English Definition'] = chunk['Healthcare Provider Taxonomy Code_2'].map(defDictDesc)
    
    chunk['Healthcare Provider Taxonomy Code_3 English'] = chunk['Healthcare Provider Taxonomy Code_3'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_3 English Class'] = chunk['Healthcare Provider Taxonomy Code_3'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_3 English Definition'] = chunk['Healthcare Provider Taxonomy Code_3'].map(defDictDesc)
    
    chunk['Healthcare Provider Taxonomy Code_4 English'] = chunk['Healthcare Provider Taxonomy Code_4'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_4 English Class'] = chunk['Healthcare Provider Taxonomy Code_4'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_4 English Definition'] = chunk['Healthcare Provider Taxonomy Code_4'].map(defDictDesc)
    
    chunk['Healthcare Provider Taxonomy Code_5 English'] = chunk['Healthcare Provider Taxonomy Code_5'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_5 English Class'] = chunk['Healthcare Provider Taxonomy Code_5'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_5 English Definition'] = chunk['Healthcare Provider Taxonomy Code_5'].map(defDictDesc)
    
    chunk['Healthcare Provider Taxonomy Code_6 English'] = chunk['Healthcare Provider Taxonomy Code_6'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_6 English Class'] = chunk['Healthcare Provider Taxonomy Code_6'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_6 English Definition'] = chunk['Healthcare Provider Taxonomy Code_6'].map(defDictDesc)
    
    chunk['Healthcare Provider Taxonomy Code_7 English'] = chunk['Healthcare Provider Taxonomy Code_7'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_7 English Class'] = chunk['Healthcare Provider Taxonomy Code_7'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_7 English Definition'] = chunk['Healthcare Provider Taxonomy Code_7'].map(defDictDesc)
    
    chunk['Healthcare Provider Taxonomy Code_8 English'] = chunk['Healthcare Provider Taxonomy Code_8'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_8 English Class'] = chunk['Healthcare Provider Taxonomy Code_8'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_8 English Definition'] = chunk['Healthcare Provider Taxonomy Code_8'].map(defDictDesc)
    
    chunk['Healthcare Provider Taxonomy Code_9 English'] = chunk['Healthcare Provider Taxonomy Code_9'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_9 English Class'] = chunk['Healthcare Provider Taxonomy Code_9'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_9 English Definition'] = chunk['Healthcare Provider Taxonomy Code_9'].map(defDictDesc)
    
    chunk['Healthcare Provider Taxonomy Code_10 English'] = chunk['Healthcare Provider Taxonomy Code_10'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_10 English Class'] = chunk['Healthcare Provider Taxonomy Code_10'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_10 English Definition'] = chunk['Healthcare Provider Taxonomy Code_10'].map(defDictDesc)
    
    chunk['Healthcare Provider Taxonomy Code_11 English'] = chunk['Healthcare Provider Taxonomy Code_11'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_11 English Class'] = chunk['Healthcare Provider Taxonomy Code_11'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_11 English Definition'] = chunk['Healthcare Provider Taxonomy Code_11'].map(defDictDesc) 
    
    chunk['Healthcare Provider Taxonomy Code_12 English'] = chunk['Healthcare Provider Taxonomy Code_12'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_12 English Class'] = chunk['Healthcare Provider Taxonomy Code_12'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_12 English Definition'] = chunk['Healthcare Provider Taxonomy Code_12'].map(defDictDesc)
    
    chunk['Healthcare Provider Taxonomy Code_13 English'] = chunk['Healthcare Provider Taxonomy Code_13'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_13 English Class'] = chunk['Healthcare Provider Taxonomy Code_13'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_13 English Definition'] = chunk['Healthcare Provider Taxonomy Code_13'].map(defDictDesc)
    
    chunk['Healthcare Provider Taxonomy Code_14 English'] = chunk['Healthcare Provider Taxonomy Code_14'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_14 English Class'] = chunk['Healthcare Provider Taxonomy Code_14'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_14 English Definition'] = chunk['Healthcare Provider Taxonomy Code_14'].map(defDictDesc)
    
    chunk['Healthcare Provider Taxonomy Code_15 English'] = chunk['Healthcare Provider Taxonomy Code_15'].map(defDictSpec)
    chunk['Healthcare Provider Taxonomy Code_15 English Class'] = chunk['Healthcare Provider Taxonomy Code_15'].map(defDictClass)
    chunk['Healthcare Provider Taxonomy Code_15 English Definition'] = chunk['Healthcare Provider Taxonomy Code_15'].map(defDictDesc)
    
    chunk.to_csv(outFile, mode='a', index=False, header=firstChunk)
    
    print(f'Records written: {recordsWritten}')
    recordsWritten += chunkSize
    firstChunk=False