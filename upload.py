import datetime
from my_function.fpanalysis import Processing
from my_db.dbb import FPDBB

dbb = FPDBB()
fppc = Processing()

if __name__=="__main__":
        try:
            fppc.update_fpanalysis()
            print('Upload rmanalysis to database successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('Upload rmanalysis to database error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
        try:
            dbb.truncate_fpbuffer_tbl()
            print('truncate_rmbuffer_tbl database successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('truncate_rmbuffer_tbl database error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')