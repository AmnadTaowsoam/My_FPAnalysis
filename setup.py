import datetime
from my_db.db import FPDB
from my_db.dbb import FPDBB

db = FPDB()
dbb = FPDBB()

if __name__=="__main__":
        try:
            db.create_fpanalysis_tbl()
            dbb.create_fpbuffer_tbl()
            print('create_master_tbl successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('create_master_tbl error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')