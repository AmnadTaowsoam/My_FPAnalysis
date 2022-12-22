import datetime
from my_function.fpanalysis import Processing

fpa = Processing()

if __name__=="__main__":
        try:
            fpa.rma_processing()
            print('Update all fp analysis successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('Update fp analysis error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')