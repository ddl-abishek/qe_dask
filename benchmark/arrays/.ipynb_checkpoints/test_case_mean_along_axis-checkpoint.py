from dask.distributed import Client
import dask.array as da
from datetime import datetime

if __name__ == '__main__':
    client = Client()
    
    start = datetime.now()
    x = da.random.normal(10, 0.1, size=(20000, 20000), chunks=(1000, 1000)) # 400 million element array ; Cut into 1000x1000 sized chunks
    y = x.mean(axis=0)[::100]
    
    try:
        y.compute()
        end = datetime.now()
        print(f'test passed {end-start}')
        
    except:
        print(f'test failed {end-start}')
    client.shutdown()

