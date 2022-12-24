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
        return data
    
    def check_exit_DB(self):
        data_a = self.get_data_fpDB()
        data_a = self.get_data_bufferDB()
        data = pd.concat([data_a,data_a]).drop_duplicates(keep=False)
        return data
            
    def update_fpanalysis(self):
        try:
            fpa = self.check_exit_DB()
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
