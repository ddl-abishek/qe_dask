from matplotlib import pyplot as plt
import pickle
import seaborn as sns
sns.set()
from dask_ml.preprocessing import StandardScaler
from dask_ml.decomposition import PCA
import dask.dataframe as dd
from collections import defaultdict
from itertools import combinations
import yaml
import os

config = yaml.load(open("config.yml", "r"))

def get_kmeans_pca(h5_year):
    # change the saving of .h5 file to .csv for dask to read it directly 
    clean_data_15 = dd.from_pandas(pd.read_hdf(eval(config['h5'][h5_year])),chunksize=3) 
    clean_data = clean_data.dropna() # dropping null values
    
    # converting all column datatypes to int from float to save memory
    convert_dict = {'Vehicle Expiration Date': int, 'Violation Precinct': int, 'Issuer Precinct' : int, 'Vehicle Year' : int}
    clean_data = clean_data.astype(convert_dict)
    
    # preprocessing the dataset
    scaler = StandardScaler()
    clean_data = scaler.fit_transform(clean_data)
    
    # applying principal component analysis 
    pca = PCA(n_components = config['PCA']['n_components'])
    pca.fit(clean_data)
    # calculating the resulting components scores for the elements in our data set
    scores_pca = pca.transform(clean_data)
    
    # clustering via k means
    kmeans_pca = KMeans(n_clusters = config['KMeans']['n_clusters'], 
                        init = config['KMeans']['init'], 
                        random_state = config['KMeans']['random_state'])
    kmeans_pca.fit(scores_pca)
    
    clean_data = pd.read_hdf(config['h5'][args.h5_year])
    clean_data = clean_data.dropna() # dropping null values
    
    kmeans_pca = pd.concat([clean_data.reset_index(drop=True),pd.DataFrame(scores_pca)],axis=1)
    kmeans_pca.columns.values[-3:] = ['Component 1','Component 2','Component 3'] # renaming columns
    
    # the last column we add contains the pca k-means clutering labels
    kmeans_pca['Segment K-means PCA'] = kmeans_pca.labels_
    kmeans_pca['Segment'] = kmeans_pca['Segment K-means PCA'].map({0:'first',1:'second',2:'third'})
    kmeans_pca = kmeans_pca.drop(columns='Segment K-means PCA')
    
    return kmeans_pca

def plot(kmeans_pca, h5_year):    
    x_axis = kmeans_pca['Component 2']
    y_axis = kmeans_pca['Component 1']
    plt.figure(figsize=(12,9))
    sns.scatterplot(x_axis,y_axis,hue=kmeans_pca['Segment'],palette=['r','g','b'])
    plt.title('Clusters by PCA Components')
    
    if not(os.path.isdir(config['artifacts']['path'])):
        os.makedirs(config['artifacts']['path'])

    plt.savefig(f'{config['artifacts']['path']}/scatter_{str(datetime.now()).replace(' ','_')}_{h5_year}.png')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--h5_year', type=str ,help="year of .h5 ; possible values - 2013-14, 2015, 2016, 2017", required=True)
    
    args = parser.parse_args()
    
    assert args.h5_year in ['2013-14', '2015', '2016', '2017']
    
    kmeans_pca = get_kmeans_pca(args.h5_year)
    plot(kmeans_pca, args.h5_year)
