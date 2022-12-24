import datetime
from my_function.fpprepare import Prepare

fpap = Prepare()

if __name__=="__main__":
        try:
            fpap.fpa_check_file_data()
            print('Check file fp analysis successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('Check file fp analysis error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
        try:
            fpap.fpa_upload_data_buffer()
            print('fpa_upload_data_buffer successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('fpa_upload_data_buffer error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')