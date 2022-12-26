import os,shutil
import datetime
import pandas as pd
import numpy as np
np.set_printoptions(suppress=True)

import warnings
warnings.filterwarnings("ignore")

from my_db.dbb import FPDBB

dbb = FPDBB()

class Prepare():
    def __init__(self) -> None:
        pass
    
    def columns_rename(self,input_data):
        try:
            data = input_data.copy()
            data = data.rename(columns={
                            'Inspection Lot':'c_inslots',
                            'Sample no':'c_sample',
                            'Ref. Sample No.':'c_refsample',
                            'Old Code':'c_oldcode',
                            'Material Code':'c_material',
                            'Material Description':'c_desc',
                            'Truck no.':'c_truckno',
                            'Pallet No.':'c_pelletno',
                            'Batch No.':'c_Batch',
                            'Formula':'c_formula',
                            'Date':'d_date',
                            'EXCPTCP':'n_EXCPTCP',
                            'MIN CP':'n_MINCP',
                            'DIFF CP':'n_DIFFCP',
                            'DIFF MIN':'n_DIFFMIN',
                            'MOIS':'n_MOIS',
                            'ASH':'n_ASH',
                            'PROTEIN':'n_PROTEIN',
                            'FAT':'n_FAT',
                            'FIBER':'n_FIBER',
                            'P':'n_P',
                            'Ca':'n_Ca',
                            'INSOL':'n_INSOL',
                            'NaCl':'n_NaCl',
                            'Na':'n_Na',
                            'K':'n_K',
                            'Fines':'n_Fines',
                            'Durability':'n_Durability',
                            'T_FAT':'n_T_FAT',
                            'Bulk density':'n_Bulk_density',
                            'Aw':'n_Aw',
                            'Starch':'n_Starch',
                            '% cook' :'n_p_cook',
                            'L*' : 'n_L_star',
                            'a*' : 'n_a_star',
                            'b*' : 'n_b_star',
                            'Hardness':'n_Hardness',
                            'ADF':'n_ADF',
                            'ADL':'n_ADL',
                            'NDF':'n_NDF',
                            'ถัง' : 'c_bins',
                            'Load Time':'c_loadtime',
                            'Plant':'c_plant',
                            'Remark':'c_Remark',
                            'Validation Code':'c_ud',
                            'Valuation Date':'c_Valuation Date',
                            'CONCATENATE':'c_CONCATENATE'
                            })
            data = data.drop(columns={
                                    'c_desc',
                                    'c_Valuation Date',
                                    'c_CONCATENATE'
                            })
            return data
        except:
            print('Columns rename error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        
    def columns_cleansing(self, input_data):
        try:
            data = input_data.copy()
            data = input_data.replace("'", "")
            data = input_data.replace('"', "")
            return data 
        except:
            print('Columns cleansing error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        
    def columns_transform(self,input_data):
        try:
            data = input_data.copy()
            data[['n_fp_nut1','n_fp_nut2','n_fp_nut3','n_fp_nut4','n_fp_nut5','n_fp_nut6','n_fp_nut7','n_fp_nut8','n_fp_nut9','n_fp_nut10']] = 0
            data = data.fillna(0)
            
            return data
        except:
            print('Columns transform data type error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def columns_detype(self, input_data):
        try:
            data = input_data.copy()
            ## columns for string
            col_str = ['c_inslots', 'c_sample', 'c_refsample', 'c_oldcode', 'c_material', 'c_truckno',\
                        'c_pelletno', 'c_Batch', 'c_formula','c_bins','c_loadtime','c_plant', 'c_Remark', 'c_ud']
            data[col_str] = data[col_str].astype(pd.StringDtype())
            ## columns for number
            col_num = ['n_EXCPTCP','n_MINCP','n_DIFFCP','n_DIFFMIN','n_MOIS','n_ASH','n_PROTEIN','n_FAT','n_FIBER',\
                        'n_P','n_Ca','n_INSOL','n_NaCl','n_Na','n_K','n_Fines','n_Durability','n_T_FAT','n_Bulk_density',\
                        'n_Aw','n_Starch','n_p_cook','n_L_star','n_a_star','n_b_star','n_Hardness','n_ADF',\
                        'n_ADL','n_NDF','n_fp_nut1','n_fp_nut2','n_fp_nut3','n_fp_nut4','n_fp_nut5',\
                        'n_fp_nut6','n_fp_nut7','n_fp_nut8','n_fp_nut9','n_fp_nut10']
            data[col_num] = data[col_num].astype(float)
            
            col_list = ['c_inslots','c_sample','c_refsample', 'c_oldcode','c_material','c_truckno','c_pelletno','c_Batch',\
                        'c_formula','d_date','n_EXCPTCP','n_MINCP','n_DIFFCP','n_DIFFMIN','n_MOIS','n_ASH',\
                        'n_PROTEIN','n_FAT','n_FIBER','n_P','n_Ca','n_INSOL','n_NaCl','n_Na','n_K','n_Fines',\
                        'n_Durability','n_T_FAT','n_Bulk_density','n_Aw','n_Starch','n_p_cook','n_L_star',\
                        'n_a_star','n_b_star','n_Hardness','n_ADF','n_ADL','n_NDF','n_fp_nut1','n_fp_nut2',\
                        'n_fp_nut3','n_fp_nut4','n_fp_nut5','n_fp_nut6','n_fp_nut7','n_fp_nut8','n_fp_nut9',\
                        'n_fp_nut10','c_bins', 'c_loadtime','c_plant','c_Remark','c_ud']
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
            
    def fpa_check_file_data(self):
        try:
            # Set the directory you want to start from
            rootDir = './documents/fpanalysis/'
            for dirName, subdirList, fileList in os.walk(rootDir):
                print('Found directory: %s' % dirName)
                for fname in fileList:
                    # Skip files that are not Excel
                    if not fname.endswith('.xlsx'):
                        continue
                    print('\t%s' % fname)
                    # Read the Excel file into a dataframe
                    fpanalysis_df = pd.read_excel(os.path.join(dirName, fname))
                    # Do something with the dataframe here
                    fpanalysis = self.fpa_transform(fpanalysis_df)
                    fpanalysis.to_excel("./documents/fpanalysis_pending/"+'C_' + fname )
                    os.remove("./documents/fpanalysis/"+fname)
                    # print('\t%s' % fname,':Check file fpanalysis Successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('Check file fpanalysis error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')



            
######### Upload data to buffer ###########################
    def columns_detype_buffer(self,input_data):
        try:
            data = input_data.copy()
            col_str = ['c_inslots', 'c_sample', 'c_refsample', 'c_oldcode', 'c_material', 'c_truckno',\
                'c_pelletno', 'c_Batch', 'c_formula','c_bins','c_loadtime','c_plant', 'c_Remark', 'c_ud']
            col_num = ['n_EXCPTCP','n_MINCP','n_DIFFCP','n_DIFFMIN','n_MOIS','n_ASH','n_PROTEIN','n_FAT','n_FIBER',\
                        'n_P','n_Ca','n_INSOL','n_NaCl','n_Na','n_K','n_Fines','n_Durability','n_T_FAT','n_Bulk_density',\
                        'n_Aw','n_Starch','n_p_cook','n_L_star','n_a_star','n_b_star','n_Hardness','n_ADF',\
                        'n_ADL','n_NDF','n_fp_nut1','n_fp_nut2','n_fp_nut3','n_fp_nut4','n_fp_nut5',\
                        'n_fp_nut6','n_fp_nut7','n_fp_nut8','n_fp_nut9','n_fp_nut10']
            col_list = ['c_inslots','c_sample','c_refsample', 'c_oldcode','c_material','c_truckno','c_pelletno','c_Batch',\
                        'c_formula','d_date','n_EXCPTCP','n_MINCP','n_DIFFCP','n_DIFFMIN','n_MOIS','n_ASH',\
                        'n_PROTEIN','n_FAT','n_FIBER','n_P','n_Ca','n_INSOL','n_NaCl','n_Na','n_K','n_Fines',\
                        'n_Durability','n_T_FAT','n_Bulk_density','n_Aw','n_Starch','n_p_cook','n_L_star',\
                        'n_a_star','n_b_star','n_Hardness','n_ADF','n_ADL','n_NDF','n_fp_nut1','n_fp_nut2',\
                        'n_fp_nut3','n_fp_nut4','n_fp_nut5','n_fp_nut6','n_fp_nut7','n_fp_nut8','n_fp_nut9',\
                        'n_fp_nut10','c_bins', 'c_loadtime','c_plant','c_Remark','c_ud']
            data[col_str] = data[col_str].astype(pd.StringDtype())
            data[col_num] = data[col_num].astype(float)
            data = data[col_list]
            data['n_DIFFCP'] = data['n_DIFFCP'].round(2)
            data['n_DIFFMIN'] = data['n_DIFFMIN'].round(2)
            return data
        except:
            print('data_prepare error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    
    def update_fpbuffer(self,input_data):
        try:
            fpa = input_data.copy()
            for i in range(len(fpa)):
                fpas = fpa.values.tolist()
                fpas = fpas[i][0],fpas[i][1],fpas[i][2],fpas[i][3],fpas[i][4],fpas[i][5],fpas[i][6],fpas[i][7],fpas[i][8],fpas[i][9],fpas[i][10],\
                    fpas[i][11],fpas[i][12],fpas[i][13],fpas[i][14],fpas[i][15],fpas[i][16],fpas[i][17],fpas[i][18],fpas[i][19],fpas[i][20],\
                    fpas[i][21],fpas[i][22],fpas[i][23],fpas[i][24],fpas[i][25],fpas[i][26],fpas[i][27],fpas[i][28],fpas[i][29],fpas[i][30],\
                    fpas[i][31],fpas[i][32],fpas[i][33],fpas[i][34],fpas[i][35],fpas[i][36],fpas[i][37],fpas[i][38],fpas[i][39],fpas[i][40],\
                    fpas[i][41],fpas[i][42],fpas[i][43],fpas[i][44],fpas[i][45],fpas[i][46],fpas[i][47],fpas[i][48],fpas[i][49],fpas[i][50],\
                    fpas[i][51],fpas[i][52],fpas[i][53]
                dbb.insert_fpbuffer_tbl(fpas)
        except:
            print('Update fpanalysis error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    
    def fpa_upload_data_buffer(self):
        try:
            # Set the directory you want to start from
            rootDir = './documents/fpanalysis_pending/'
            for dirName, subdirList, fileList in os.walk(rootDir):
                print('Found directory: %s' % dirName)
                for fname in fileList:
                    # Skip files that are not Excel
                    if not fname.endswith('.xlsx'):
                        continue
                    print('\t%s' % fname)
                    # Read the Excel file into a dataframe
                    fpanalysis_df = pd.read_excel(os.path.join(dirName, fname))
                    # Do something with the dataframe here
                    fpanalysis = self.columns_detype_buffer(fpanalysis_df)
                    self.update_fpbuffer(fpanalysis)
                    shutil.move("./documents/fpanalysis_pending/"+fname, "./documents/fpanalysis_complete/" + fname)
                    # print('\t%s' % fname,':upload fpanalysis to buffer Successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('upload fpanalysis to buffer error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')