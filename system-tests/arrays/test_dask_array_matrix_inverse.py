from dask.distributed import Client
import dask.array as da
from datetime import datetime
import os

if __name__ == '__main__':
    service_port = os.environ['DASK_SCHEDULER_SERVICE_PORT']
    service_host = os.environ['DASK_SCHEDULER_SERVICE_HOST']
    client = Client(f'{service_host}:{service_port}')
    
    start = datetime.now()    
    #creating a 2000x2000 array whose values are normally distributed with zero mean and unit variance
    a = da.random.normal(0,1,size=(200, 200),chunks=(100,100))
    #computing the inverse of the array
    inv_a = da.linalg.inv(a).compute()
    print(inv_a)
    end = datetime.now()
    print(f'test passed {end-start}')

    client.shutdown()
        
    