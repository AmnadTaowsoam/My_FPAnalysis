import sys
import pyodbc
import configparser
import pandas as pd
import datetime

class FPDB():
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
    def create_fpanalysis_tbl(self):
        create_fpanalysis_sql = """CREATE TABLE fpanalysis (
            ID int NOT NULL IDENTITY(1,1),
            inslots varchar(15), 
            sample varchar(15), 
            refsample varchar(15), 
            oldcode varchar(20), 
            material varchar(15), 
            truckno varchar(20),
            pelletno varchar(15), 
            Batch varchar(15), 
            formula varchar(15), 
            date datetime, 
            EXCPTCP decimal(10,4), 
            MINCP decimal(10,4), 
            DIFFCP decimal(10,4),
            DIFFMIN decimal(10,4), 
            MOIS decimal(10,4), 
            ASH decimal(10,4), 
            PROTEIN decimal(10,4), 
            FAT decimal(10,4), 
            FIBER decimal(10,4), 
            P decimal(10,4), 
            Ca decimal(10,4), 
            INSOL decimal(10,4),
            NaCl decimal(10,4), 
            Na decimal(10,4), 
            K decimal(10,4), 
            Fines decimal(10,4), 
            Durability decimal(10,4), 
            T_FAT decimal(10,4), 
            Bulk_density decimal(10,4), 
            Aw decimal(10,4),
            Starch decimal(10,4), 
            p_cook decimal(10,4), 
            L_star decimal(10,4), 
            a_star decimal(10,4), 
            b_star decimal(10,4), 
            Hardness decimal(10,4), 
            ADF decimal(10,4),
            ADL decimal(10,4), 
            NDF decimal(10,4),
            fp_nut1 decimal(10,4),
            fp_nut2 decimal(10,4),
            fp_nut3 decimal(10,4),
            fp_nut4 decimal(10,4),
            fp_nut5 decimal(10,4),
            fp_nut6 decimal(10,4),
            fp_nut7 decimal(10,4),
            fp_nut8 decimal(10,4),
            fp_nut9 decimal(10,4),
            fp_nut10 decimal(10,4),
            bins varchar(10), 
            loadtime varchar(15), 
            plant varchar(5), 
            Remark varchar(100), 
            ud varchar(5)
            )
            """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(create_fpanalysis_sql)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('create_fpanalysis_sql error')
        else:
            print('create_fpanalysis_sql successful')
            self.cursor.commit()
            self.cursor.close()
            
    def insert_fpanalysis_tbl(self,data):
        insert_fpanalysis_sql = """INSERT INTO fpanalysis (
            inslots, 
            sample, 
            refsample, 
            oldcode, 
            material, 
            truckno,
            pelletno, 
            Batch, 
            formula, 
            date, 
            EXCPTCP, 
            MINCP, 
            DIFFCP,
            DIFFMIN, 
            MOIS, 
            ASH, 
            PROTEIN, 
            FAT, 
            FIBER, 
            P, 
            Ca, 
            INSOL,
            NaCl, 
            Na, 
            K, 
            Fines, 
            Durability, 
            T_FAT, 
            Bulk_density, 
            Aw,
            Starch, 
            p_cook, 
            L_star, 
            a_star, 
            b_star, 
            Hardness, 
            ADF,
            ADL, 
            NDF,
            fp_nut1,
            fp_nut2,
            fp_nut3,
            fp_nut4,
            fp_nut5,
            fp_nut6,
            fp_nut7,
            fp_nut8,
            fp_nut9,
            fp_nut10, 
            bins, 
            loadtime, 
            plant, 
            Remark, 
            ud
            ) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(insert_fpanalysis_sql, data)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('insert_fpanalysis_tbl error')
        else:
            self.cursor.commit()
            self.cursor.close()
            
    def get_fpanalysis_tbl(self):
        get_fpanalysis_sql = """SELECT * from fpanalysis
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(get_fpanalysis_sql)
            data = self.cursor.fetchall()
            data = pd.DataFrame((tuple(t) for t in data))
            data = data.rename(columns={
                0:'ID',
                1:'inslots',
                2:'sample',
                3:'refsample',
                4:'oldcode',
                5:'material',
                6:'truckno',
                7:'pelletno',
                8:'Batch',
                9:'formula',
                10:'date',
                11:'EXCPTCP',
                12:'MINCP',
                13:'DIFFCP',
                14:'DIFFMIN',
                15:'MOIS',
                16:'ASH',
                17:'PROTEIN',
                18:'FAT',
                19:'FIBER',
                20:'P',
                21:'Ca',
                22:'INSOL',
                23:'NaCl',
                24:'Na',
                25:'K',
                26:'Fines',
                27:'Durability',
                28:'T_FAT',
                29:'Bulk_density',
                30:'Aw',
                31:'Starch',
                32:'p_cook',
                33:'L_star',
                34:'a_star',
                35:'b_star',
                36:'Hardness',
                37:'ADF',
                38:'ADL',
                39:'NDF',
                40:'fp_nut1',
                41:'fp_nut2',
                42:'fp_nut3',
                43:'fp_nut4',
                44:'fp_nut5',
                45:'fp_nut6',
                46:'fp_nut7',
                47:'fp_nut8',
                48:'fp_nut9',
                49:'fp_nut10',
                50:'bins',
                51:'loadtime',
                52:'plant',
                53:'Remark',
                54:'ud'
                })
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('get_fpanalysis_tbl error')
        else:
            self.cursor.commit()
            self.cursor.close()
            return  data
        
    def truncate_fpanalysis_tbl(self):
        truncate_fpanalysis_sql = """TRUNCATE TABLE fpanalysis"""
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(truncate_fpanalysis_sql)
        except Exception as ex:
            print(ex)
            print('truncate_fpanalysis error')
        else:
            self.cursor.commit()
            self.cursor.close()