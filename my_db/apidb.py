import sys
import pyodbc
import configparser
import pandas as pd
import datetime

class APIFPDB():
    parser = configparser.ConfigParser()
    parser.read('FPAnalysis.ini')
    SQL_driver = parser['myserv']['SQL_driver']
    Server = parser['myserv']['Server']
    Database = parser['myserv']['Database']
    uid = parser['myserv']['uid']
    pwd = parser['myserv']['pwd']

    def __init__(self):
        con_string = f'Driver={self.SQL_driver};Server={self.Server};Database={self.Database};UID={self.uid};pwd={self.pwd}'
        # con_string = f'Driver={self.SQL_driver};Server={self.Server};Database={self.Database};Trusted_Connection=yes;'
        try:
            self.conn = pyodbc.connect(con_string)
        except Exception as e:
            print(e)
            print('Task is terminate')
            sys.exit()
        else:
            self.cursor = self.conn.cursor()
            print('Conection successfully','(',datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),')')
            
    ########################################################################      
    def get_fpanalysis_tbl(self,plant,feedN):
        get_fpanalysis_sql = """SELECT * from fpanalysis where (c_plant=? AND c_oldcode=?)
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(get_fpanalysis_sql,plant,feedN)
            data = self.cursor.fetchall()
            data = pd.DataFrame((tuple(t) for t in data))
            data = data.rename(columns={
                0:'ID',
                1:'c_inslots',
                2:'c_sample',
                3:'c_refsample',
                4:'c_oldcode',
                5:'c_material',
                6:'c_truckno',
                7:'c_pelletno',
                8:'c_Batch',
                9:'c_formula',
                10:'d_date',
                11:'n_EXCPTCP',
                12:'n_MINCP',
                13:'n_DIFFCP',
                14:'n_DIFFMIN',
                15:'n_MOIS',
                16:'n_ASH',
                17:'n_PROTEIN',
                18:'n_FAT',
                19:'n_FIBER',
                20:'n_P',
                21:'n_Ca',
                22:'n_INSOL',
                23:'n_NaCl',
                24:'n_Na',
                25:'n_K',
                26:'n_Fines',
                27:'n_Durability',
                28:'n_T_FAT',
                29:'n_Bulk_density',
                30:'n_Aw',
                31:'n_Starch',
                32:'n_p_cook',
                33:'n_L_star',
                34:'n_a_star',
                35:'n_b_star',
                36:'n_Hardness',
                37:'n_ADF',
                38:'n_ADL',
                39:'n_NDF',
                40:'n_fp_nut1',
                41:'n_fp_nut2',
                42:'n_fp_nut3',
                43:'n_fp_nut4',
                44:'n_fp_nut5',
                45:'n_fp_nut6',
                46:'n_fp_nut7',
                47:'n_fp_nut8',
                48:'n_fp_nut9',
                49:'n_fp_nut10',
                50:'c_bins',
                51:'c_loadtime',
                52:'c_plant',
                53:'c_Remark',
                54:'c_ud'
                })
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('get_fpanalysis_tbl error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        else:
            self.cursor.commit()
            self.cursor.close()
            return  data