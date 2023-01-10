import os,shutil
import datetime
from my_function.fpprepare import Prepare,RAW_DATA_PRE

fpap = Prepare()
fprd = RAW_DATA_PRE()

if __name__=="__main__":
    try:
        fprd.fpa_create_file_data()
        print('Check file fp analysis successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    except:
        print('Check file fp analysis error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    try:
        fpap.fpa_check_file_data()
        print('Check file fp analysis successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    except:
        print('Check file fp analysis error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    # try:
    #     fpap.fpa_upload_data_buffer()
    #     print('fpa_upload_data_buffer successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    # except:
    #     print('fpa_upload_data_buffer error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')