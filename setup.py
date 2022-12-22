from my_db.db import FPDB

db = FPDB()

if __name__=="__main__":
        try:
            db.create_fpanalysis_tbl()
        except:
            print('create_copellet_tbl error')