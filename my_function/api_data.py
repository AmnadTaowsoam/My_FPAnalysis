import os
import datetime
from my_db.apidb import APIFPDB
db = APIFPDB()

class FP_API():
    def __init__(self) -> None:
        pass
    def get_data_db(self,plant,feedN):
        data = db.get_fpanalysis_tbl(plant,feedN)
        col_num = ['n_EXCPTCP','n_MINCP','n_DIFFCP','n_DIFFMIN','n_MOIS','n_ASH','n_PROTEIN','n_FAT','n_FIBER',\
                    'n_P','n_Ca','n_INSOL','n_NaCl','n_Na','n_K','n_Fines','n_Durability','n_T_FAT','n_Bulk_density',\
                    'n_Aw','n_Starch','n_p_cook','n_L_star','n_a_star','n_b_star','n_Hardness','n_ADF',\
                    'n_ADL','n_NDF','n_fp_nut1','n_fp_nut2','n_fp_nut3','n_fp_nut4','n_fp_nut5',\
                    'n_fp_nut6','n_fp_nut7','n_fp_nut8','n_fp_nut9','n_fp_nut10']
        data[col_num] = data[col_num].astype(float)
        return data
    
    def col_rename(self,plant,feedN):
        data = self.get_data_db(plant,feedN)
        data = data.rename(columns= {
                                    'c_inslots':'inslots', 
                                    'c_sample':'sample', 
                                    'c_refsample':'refsample', 
                                    'c_oldcode':'feedN', 
                                    'c_material':'material', 
                                    'c_truckno':'truckno',
                                    'c_pelletno':'pelletno', 
                                    'c_Batch':'Batch', 
                                    'c_formula':'formula', 
                                    'd_date':'dates', 
                                    'n_MOIS':'MOISTURE', 
                                    'n_ASH':'ASH', 
                                    'n_PROTEIN':'PROTEIN', 
                                    'n_FAT':'FAT', 
                                    'n_FIBER':'FIBER', 
                                    'n_P':'P', 
                                    'n_Ca':'Ca', 
                                    'n_INSOL':'INSOL',
                                    'n_NaCl':'NaCl', 
                                    'n_Na':'Na', 
                                    'n_K':'K', 
                                    'n_Fines':'Fines', 
                                    'n_Durability':'Durability', 
                                    'n_T_FAT':'T_FAT', 
                                    'n_Bulk_density':'Bulk_density', 
                                    'n_Aw':'Aw',
                                    'n_Starch':'starch', 
                                    'n_p_cook':'p_cook', 
                                    'n_L_star':'L', 
                                    'n_a_star':'a*', 
                                    'n_b_star':'b*', 
                                    'n_Hardness':'Hardness', 
                                    'n_ADF':'ADF',
                                    'n_ADL':'ADL', 
                                    'n_NDF':'NDF',
                                    'c_bins':'bins', 
                                    'c_loadtime':'loadtime', 
                                    'c_plant':'plant', 
                                    'c_Remark':'remark', 
                                    'c_ud':'ud'})
        return data
    
    def processing(self,param,plant,feedN):
        data = self.col_rename(plant,feedN)
        col_list = ['feedN','material','Batch','dates','plant']
        num_param = len(param)
        for i in range(num_param):
            params = param[i]
            col_list.append(params)
            # print(count_list)
        data = data[col_list]
        data = data.loc[(data != 0).all(axis=1), :]
        data = data.sort_values(by='dates',ascending=False)
        return data
    
    def creat_data(self,param,plant,feedN):
        data = self.processing(param,plant,feedN)
        # Set the directory you want to start from
        rootDir = './APIs_data/'
        filename = f"FPAnalysis_{plant}_{feedN}_{datetime.datetime.now().strftime('%Y%m%d')}"
        data.to_excel(rootDir + filename + '.xlsx')
        return data