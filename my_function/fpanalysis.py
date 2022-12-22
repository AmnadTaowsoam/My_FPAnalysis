import os
import datetime
import pandas as pd
import numpy as np
np.set_printoptions(suppress=True)

import warnings
warnings.filterwarnings("ignore")

from my_db.db import FPDB

db = FPDB()

class Processing():
    def __init__(self) -> None:
        pass
    
    def columns_rename(self,input_data):
        try:
            data = input_data.copy()
            data = data.rename(columns={
                            'Inspection Lot':'inslots',
                            'Sample no':'sample',
                            'Ref. Sample No.':'refsample',
                            'Old Code':'oldcode',
                            'Material Code':'material',
                            'Material Description':'desc',
                            'Truck no.':'truckno',
                            'Pallet No.':'pelletno',
                            'Batch No.':'Batch',
                            'Formula':'formula',
                            'Date':'date',
                            'MIN CP':'MINCP',
                            'DIFF CP':'DIFFCP',
                            'DIFF MIN':'DIFFMIN',
                            'Bulk density':'Bulk_density',
                            '% cook' :'p_cook',
                            'L*' : 'L_star',
                            'a*' : 'a_star',
                            'b*' : 'b_star',
                            'ถัง' : 'bins',
                            'Load Time':'loadtime',
                            'Plant':'plant',
                            'Validation Code':'ud'
                            })
            data = data.drop(columns={
                                    'desc',
                                    'Valuation Date',
                                    'CONCATENATE'
                            })
            data = data.fillna(0)
            return data
        except:
            print('Columns rename error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        
    def columns_cleansing(self, input_data):
        try:
            data = input_data.copy()
            data = input_data.replace("'", "")
            return data 
        except:
            print('Columns cleansing error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        
    def columns_transform(self,input_data):
        try:
            data = input_data.copy()
            data[['fp_nut1','fp_nut2','fp_nut3','fp_nut4','fp_nut5','fp_nut6','fp_nut7','fp_nut8','fp_nut9','fp_nut10']] = 0
            
            return data
        except:
            print('Columns transform data type error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def columns_detype(self, input_data):
        try:
            data = input_data.copy()
            ## columns for string
            col_str = ['inslots', 'sample', 'refsample', 'oldcode', 'material', 'truckno','pelletno', 'Batch', 'formula',\
                        'bins','loadtime','plant', 'Remark', 'ud']
            data[col_str] = data[col_str].astype(pd.StringDtype())
            ## columns for number
            col_num = ['EXCPTCP','MINCP','DIFFCP','DIFFMIN','MOIS','ASH','PROTEIN','FAT','FIBER','P','Ca','INSOL','NaCl','Na','K',\
                        'Fines','Durability','T_FAT','Bulk_density','Aw','Starch','p_cook','L_star','a_star','b_star','Hardness',\
                        'ADF','ADL','NDF','fp_nut1','fp_nut2','fp_nut3','fp_nut4','fp_nut5','fp_nut6','fp_nut7','fp_nut8','fp_nut9','fp_nut10']
            data[col_num] = data[col_num].astype(float)
            
            col_list = ['inslots','sample','refsample', 'oldcode','material','truckno','pelletno','Batch','formula','date','EXCPTCP','MINCP','DIFFCP',\
                        'DIFFMIN','MOIS','ASH','PROTEIN','FAT','FIBER','P','Ca','INSOL','NaCl','Na','K','Fines','Durability','T_FAT','Bulk_density','Aw',\
                        'Starch','p_cook','L_star','a_star','b_star','Hardness','ADF','ADL','NDF','fp_nut1','fp_nut2','fp_nut3','fp_nut4','fp_nut5',\
                        'fp_nut6','fp_nut7','fp_nut8','fp_nut9','fp_nut10','bins', 'loadtime','plant','Remark','ud']
            data = data[col_list]
            return data
        except:
            print('columns_detype error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def fpa_transform(self, input_data):
        try:
            columns_rename = self.columns_rename(input_data)
            columns_cleansing = self.columns_cleansing(columns_rename)
            columns_transform = self.columns_transform(columns_cleansing)
            data = self.columns_detype(columns_transform)
            return data
        except:
            print('fp_processing error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def update_fpanalysis(self,data_input):
        try:
            fpa = data_input.copy()
            for i in range(len(fpa)):
                fpas = fpa.values.tolist()
                fpas = fpas[i][0],fpas[i][1],fpas[i][2],fpas[i][3],fpas[i][4],fpas[i][5],fpas[i][6],fpas[i][7],fpas[i][8],fpas[i][9],fpas[i][10],\
                    fpas[i][11],fpas[i][12],fpas[i][13],fpas[i][14],fpas[i][15],fpas[i][16],fpas[i][17],fpas[i][18],fpas[i][19],fpas[i][20],\
                    fpas[i][21],fpas[i][22],fpas[i][23],fpas[i][24],fpas[i][25],fpas[i][26],fpas[i][27],fpas[i][28],fpas[i][29],fpas[i][30],\
                    fpas[i][31],fpas[i][32],fpas[i][33],fpas[i][34],fpas[i][35],fpas[i][36],fpas[i][37],fpas[i][38],fpas[i][39],fpas[i][40],\
                    fpas[i][41],fpas[i][42],fpas[i][43],fpas[i][44],fpas[i][45],fpas[i][46],fpas[i][47],fpas[i][48],fpas[i][49],fpas[i][50],\
                    fpas[i][51],fpas[i][52],fpas[i][53]
                db.insert_fpanalysis_tbl(fpas)
        except:
            print('Update fpanalysis error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def fpa_processing(self):
        try:
            # Set the directory you want to start from
            rootDir = './documents/rmanalysis/'
            for dirName, subdirList, fileList in os.walk(rootDir):
                print('Found directory: %s' % dirName)
                for fname in fileList:
                    # Skip files that are not Excel
                    if not fname.endswith('.xlsx'):
                        continue
                    print('\t%s' % fname)
                    # Read the Excel file into a dataframe
                    rmanalysis_df = pd.read_excel(os.path.join(dirName, fname))
                    # Do something with the dataframe here
                    
                    rmanalysis = self.fpa_transform(rmanalysis_df)
                    self.update_fpanalysis(rmanalysis)
                    print('\t%s' % fname,':Update Successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('filter_processing error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')