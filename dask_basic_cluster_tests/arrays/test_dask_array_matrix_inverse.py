from dask.distributed import Client
import dask.array as da
from dask.distributed import performance_report
from datetime import datetime
import os

def test_array(arg):
    start = datetime.now()    
    #creating a 200x200 array whose values are normally distributed with zero mean and unit variance
    a = da.random.normal(0,1,size=(200, 200),chunks=(100,100))

    #computing the inverse of the array
    inv_a = da.linalg.inv(a).compute()

    end = datetime.now()

    return (f'test passed {end-start}',inv_a)

if __name__ == '__main__':
    service_port = os.environ['DASK_SCHEDULER_SERVICE_PORT']
    service_host = os.environ['DASK_SCHEDULER_SERVICE_HOST']
    
    client = Client(address=f'{service_host}:{service_port}')
    client.wait_for_workers()
    client.restart()
    
    filename=f"/mnt/artifacts/results/dask-report_test_dask_array_matrix_inverse_{str(datetime.now())}.html".replace(' ','')
    
    with performance_report(filename=filename):
        dask_submit = client.submit(test_array, 1)
        print(dask_submit.result())

    os.system(f"cp {filename} /mnt/code")
    client.restart()
    client.close()