import yaml
from dask.distributed import Client
import dask.dataframe as dd
import os
from datetime import datetime
import pandas as pd
import seaborn as sns
from collections import defaultdict
from matplotlib import pyplot

service_host = os.environ['DASK_SCHEDULER_SERVICE_HOST']
service_port = os.environ['DASK_SCHEDULER_SERVICE_PORT']

config = yaml.load(open('./config.yml','r'), Loader=yaml.FullLoader)

    start = datetime.now()
    
    os.system('! wget https://dsp-workflow.s3.us-west-2.amazonaws.com/heart_failure_clinical_records_dataset.csv')
    for i in range(30):
        os.system(f'cp ./heart_failure_clinical_records_dataset.csv ./heart_failure_clinical_records_dataset_{i+1}.csv')
        
    df = dd.read_csv('./heart_failure_clinical_records_dataset*.csv')
    
    print(df.groupby('age').platelets.mean().compute())
    
    end = datetime.now()
    print(f'test passed {end-start}')

    client.shutdown()
    

def IssueDateLinePlot():
    with performance_report(filename=f"./dask-report_{str(datetime.now()).replace(' ','__')}.html"):    
        dateparse = lambda x: datetime.strptime(x, '%m/%d/%Y')

        # reading all parking violations issued
        pvi_df = dd.read_csv(f'{config['Dataset']['path']}/*.csv',usecols=[ 'Issue Date'],parse_dates=['Issue Date'],date_parser=dateparse)

#         data = dd.read_csv(path,usecols=[ 'Issue Date'],parse_dates=['Issue Date'],date_parser=dateparse)

        months = defaultdict(int)

        for date in pvi_df['Issue Date']:
            months[date.month] += 1

        months_ordered = {'Jan':0,'Feb':0,'Mar':0,
                          'Apr':0,'May':0,'Jun':0,
                          'July':0,'Aug':0,'Sept':0,
                          'Oct':0,'Nov':0,'Dec':0}

        for i,key in enumerate(months_ordered.keys()):
            months_ordered[key] = months[i+1]

        months_df = pd.DataFrame(months_ordered.items(),columns=['Month','Issue Date'])

        _, ax = pyplot.subplots(figsize=(12,9))
        sns.set(style="darkgrid")

        # Plot the isse dates for different months
        sns_lineplot = sns.lineplot(x="Month",y="Issue Date",ax=ax,data=self.months_df,sort=False)
        sns_line.savefig("output.png")



if __name__ == '__main__':
    client = Client()
    
    
    


    client.shutdown()