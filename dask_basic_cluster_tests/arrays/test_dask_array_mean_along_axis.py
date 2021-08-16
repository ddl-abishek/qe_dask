from dask.distributed import Client
import dask.array as da
from dask.distributed import performance_report
from datetime import datetime
import os

def test_array(arg):
    start = datetime.now()
    x = da.random.normal(10, 0.1, size=(2000, 2000), chunks=(100, 100)) # 400 million element array ; Cut into 1000x1000 sized chunks
    y = x.mean(axis=0)[::100]
    y.compute()
    end = datetime.now()
    return (f'test passed {end-start}',y)

    
    
if __name__ == '__main__':
    service_port = os.environ['DASK_SCHEDULER_SERVICE_PORT']
    service_host = os.environ['DASK_SCHEDULER_SERVICE_HOST']
    
    client = Client(address=f'{service_host}:{service_port}')
    client.wait_for_workers(n_workers=4)
    client.restart()
    
    filename=f"/mnt/artifacts/results/dask-report_test_dask_array_mean_along_axis_{str(datetime.now())}.html".replace(' ','')
    
    with performance_report(filename=filename):
        dask_map = client.map(test_array, range(10))
        print(client.gather(dask_map))

    os.system(f"cp {filename} /mnt/code")
    client.restart()
    client.close()