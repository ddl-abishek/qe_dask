import dask.array as da
import dask.dataframe as dd
import os
from datetime import datetime
from dask.distributed import Client, performance_report

def test_dataframe(dataset_dir):
    start = datetime.now()
    df = dd.read_csv(f'{dataset_dir}/heart_failure_clinical_records_dataset*.csv')
    mean = df.groupby('age').platelets.mean().compute()
    end = datetime.now()
    
    return (f'test passed {end-start}',mean)
    

if __name__ == '__main__':
    service_port = os.environ['DASK_SCHEDULER_SERVICE_PORT']
    service_host = os.environ['DASK_SCHEDULER_SERVICE_HOST']
    
    client = Client(address=f'{service_host}:{service_port}')
    client.wait_for_workers(n_workers=4)
    client.restart()

    dataset_dir = f'/mnt/data/{os.environ['DOMINO_PROJECT_NAME']}'
    
    os.system(f'wget https://dsp-workflow.s3.us-west-2.amazonaws.com/heart_failure_clinical_records_dataset.csv -P {dataset_dir}')
    
    for i in range(60):
        os.system(f'cp {dataset_dir}/heart_failure_clinical_records_dataset.csv {dataset_dir}/heart_failure_clinical_records_dataset_{i+1}.csv')
        
    filename=f"/mnt/artifacts/results/dask-report_test_dask_dataframe_{str(datetime.now())}.html".replace(' ','')
    
    with performance_report(filename=filename):
        dask_map = client.map(test_dataframe, [dataset_dir for _ in range(10)])
        print(client.gather(dask_map))

    os.system(f"cp {filename} /mnt/code")
    client.restart()
    client.close()
        
