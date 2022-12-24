import sys
import pyodbc
import configparser
import pandas as pd
import datetime

class FPDBB():
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
    def create_fpbuffer_tbl(self):
        create_fpbuffer_sql = """CREATE TABLE fpbuffer (
            ID int NOT NULL IDENTITY(1,1),
            c_inslots varchar(15), 
            c_sample varchar(15), 
            c_refsample varchar(15), 
            c_oldcode varchar(20), 
            c_material varchar(15), 
            c_truckno varchar(20),
            c_pelletno varchar(15), 
            c_Batch varchar(15), 
            c_formula varchar(15), 
            d_date datetime, 
            n_EXCPTCP decimal(10,2), 
            n_MINCP decimal(10,2), 
            n_DIFFCP decimal(10,2),
            n_DIFFMIN decimal(10,2), 
            n_MOIS decimal(10,2), 
            n_ASH decimal(10,2), 
            n_PROTEIN decimal(10,2), 
            n_FAT decimal(10,2), 
            n_FIBER decimal(10,2), 
            n_P decimal(10,2), 
            n_Ca decimal(10,2), 
            n_INSOL decimal(10,2),
            n_NaCl decimal(10,2), 
            n_Na decimal(10,2), 
            n_K decimal(10,2), 
            n_Fines decimal(10,2), 
            n_Durability decimal(10,2), 
            n_T_FAT decimal(10,2), 
            n_Bulk_density decimal(10,2), 
            n_Aw decimal(10,2),
            n_Starch decimal(10,2), 
            n_p_cook decimal(10,2), 
            n_L_star decimal(10,2), 
            n_a_star decimal(10,2), 
            n_b_star decimal(10,2), 
            n_Hardness decimal(10,2), 
            n_ADF decimal(10,2),
            n_ADL decimal(10,2), 
            n_NDF decimal(10,2),
            n_fp_nut1 decimal(10,2),
            n_fp_nut2 decimal(10,2),
            n_fp_nut3 decimal(10,2),
            n_fp_nut4 decimal(10,2),
            n_fp_nut5 decimal(10,2),
            n_fp_nut6 decimal(10,2),
            n_fp_nut7 decimal(10,2),
            n_fp_nut8 decimal(10,2),
            n_fp_nut9 decimal(10,2),
            n_fp_nut10 decimal(10,2),
            c_bins varchar(10), 
            c_loadtime varchar(15), 
            c_plant varchar(5), 
            c_Remark varchar(100), 
            c_ud varchar(5)
            )
            """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(create_fpbuffer_sql)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('create_fpbuffer_sql error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        else:
            print('create_fpbuffer_sql successful','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            self.cursor.commit()
            self.cursor.close()
            
    def insert_fpbuffer_tbl(self,data):
        insert_fpbuffer_sql = """INSERT INTO fpbuffer (
            c_inslots, 
            c_sample, 
            c_refsample, 
            c_oldcode, 
            c_material, 
            c_truckno,
            c_pelletno, 
            c_Batch, 
            c_formula, 
            d_date, 
            n_EXCPTCP, 
            n_MINCP, 
            n_DIFFCP,
            n_DIFFMIN, 
            n_MOIS, 
            n_ASH, 
            n_PROTEIN, 
            n_FAT, 
            n_FIBER, 
            n_P, 
            n_Ca, 
            n_INSOL,
            n_NaCl, 
            n_Na, 
            n_K, 
            n_Fines, 
            n_Durability, 
            n_T_FAT, 
            n_Bulk_density, 
            n_Aw,
            n_Starch, 
            n_p_cook, 
            n_L_star, 
            n_a_star, 
            n_b_star, 
            n_Hardness, 
            n_ADF,
            n_ADL, 
            n_NDF,
            n_fp_nut1,
            n_fp_nut2,
            n_fp_nut3,
            n_fp_nut4,
            n_fp_nut5,
            n_fp_nut6,
            n_fp_nut7,
            n_fp_nut8,
            n_fp_nut9,
            n_fp_nut10, 
            c_bins, 
            c_loadtime, 
            c_plant, 
            c_Remark, 
            c_ud
            ) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(insert_fpbuffer_sql, data)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('insert_fpbuffer_tbl error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        else:
            self.cursor.commit()
            self.cursor.close()
            
    def get_fpbuffer_tbl(self):
        get_fpbuffer_sql = """SELECT * from fpbuffer
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(get_fpbuffer_sql)
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
            print('get_fpbuffer_tbl error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        else:
            self.cursor.commit()
            self.cursor.close()
            return  data
        
    def truncate_fpbuffer_tbl(self):
        truncate_fpbuffer_sql = """TRUNCATE TABLE fpbuffer"""
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(truncate_fpbuffer_sql)
        except Exception as ex:
            print(ex)
            print('truncate_fpbuffer error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        else:
            self.cursor.commit()
            self.cursor.close()