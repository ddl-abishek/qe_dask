from dask.distributed import Client, performance_report
import dask.array as da
import dask.bag as db
from sklearn.decomposition import TruncatedSVD
from datetime import datetime
import os

def svd_matrix(mat):
    return TruncatedSVD(n_components=5, n_iter=7, random_state=42).fit(mat)

def test_bag(arg):
    start = datetime.now()    
    matrices = db.from_sequence([da.random.normal(0,1,size=(200, 200),chunks=(10,10)) for _ in range(10)], npartitions=5)
    matrices.map(svd_matrix).compute()
    end = datetime.now()
    return f'test passed {end-start}'

if __name__ == '__main__':
    service_port = os.environ['DASK_SCHEDULER_SERVICE_PORT']
    service_host = os.environ['DASK_SCHEDULER_SERVICE_HOST']

    client = Client(f'{service_host}:{service_port}')
    client.wait_for_workers()
    client.restart()
    filename = f"/mnt/artifacts/results/dask-report_test_dask_bag_{str(datetime.now())}.html"
    with performance_report(filename=filename):
        dask_submit = client.submit(test_bag, 1)
        print(dask_submit.result())

    os.system(f"cp {filename} /mnt/code")
    client.close()
