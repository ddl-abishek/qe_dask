from dask.distributed import Client
import dask.array as da
import dask.bag as db
from sklearn.decomposition import TruncatedSVD
from datetime import datetime
import os

def svd_matrix(mat):
    return TruncatedSVD(n_components=5, n_iter=7, random_state=42).fit(mat)

if __name__ == '__main__':
    start = datetime.now()
    service_port = os.environ['DASK_SCHEDULER_SERVICE_PORT']
    service_host = os.environ['DASK_SCHEDULER_SERVICE_HOST']
    client = Client(f'{service_host}:{service_port}')
    
    matrices = db.from_sequence([da.random.normal(0,1,size=(200, 200),chunks=(10,10)) for _ in range(10)], npartitions=5)
    print(matrices.map(svd_matrix).compute())
    end = datetime.now()
    print(f'test passed {end-start}')

    client.shutdown()