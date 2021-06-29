from dask.distributed import Client
import dask.array as da
from datetime import datetime

if __name__ == '__main__':
    client = Client()
    
    start = datetime.now()
    
    #creating a 2000x2000 array whose values are normally distributed with zero mean and unit variance
    a = da.random.normal(0,1,size=(200, 200),chunks=(10,10))

    #computing the inverse of the array
    inv_a = da.linalg.inv(a)
    
    try:
        print(inv_a.compute())
        end = datetime.now()
        print(f'test passed {end-start}')
    
    except:
        print(f'test failed {end-start}')
        
    