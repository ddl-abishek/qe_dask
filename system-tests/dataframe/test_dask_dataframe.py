from dask.distributed import Client
import dask.array as da
import dask.dataframe as dd
import os
from datetime import datetime

service_host = os.environ['DASK_SCHEDULER_SERVICE_HOST']
service_port = os.environ['DASK_SCHEDULER_SERVICE_PORT']

if __name__ == '__main__':
    client = Client(f'{service_host}:{service_port}')
    
    start = datetime.now()
    
    os.system('! wget https://dsp-workflow.s3.us-west-2.amazonaws.com/heart_failure_clinical_records_dataset.csv')
    for i in range(30):
        os.system(f'cp ./heart_failure_clinical_records_dataset.csv ./heart_failure_clinical_records_dataset_{i+1}.csv')
        
    df = dd.read_csv('./heart_failure_clinical_records_dataset*.csv')
    
    print(df.groupby('age').platelets.mean().compute())
    
    end = datetime.now()
    print(f'test passed {end-start}')

    client.shutdown()
    
    
    
