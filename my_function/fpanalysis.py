import os
import datetime
import pandas as pd
import numpy as np
np.set_printoptions(suppress=True)

import warnings
warnings.filterwarnings("ignore")

from my_db.db import FPDB
from my_db.dbb import FPDBB

db = FPDB()
dbb = FPDBB()

class Processing():
    def __init__(self) -> None:
        pass
    
    def get_data_bufferDB(self):
        data = dbb.get_fpbuffer_tbl()
        data = data.drop(columns={'ID'})
        data = data.drop_duplicates()
        return data
    
    def get_data_fpDB(self):
        data = db.get_fpanalysis_tbl()
        data = data.drop(columns={'ID'})
        data = data.drop_duplicates()
        return data
    
    def check_exit_DB(self):
        data_a = self.get_data_fpDB()
        data_b = self.get_data_bufferDB()
        if data_b.equals(data_a):
            return pd.DataFrame()
        else:
            return data_b
    
    def columns_detype(self):
        try:
            data = self.check_exit_DB()
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
            
    def update_fpanalysis(self):
        try:
            fpa = self.columns_detype()
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
