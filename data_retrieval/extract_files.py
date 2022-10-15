import os
import tarfile

dir_path = 'C:\Users\HP\Desktop\CAYOTY\CODE\Python, SQL, Spark\Apache Spark ETL\apache-spark-etl-pipeline\apache-spark-etl-pipeline\data'

file_list = os.listdir(dir_path)
data_dest = dir_path + 'extracted'

for archive in file_list:
    file_name = archive.replace('.tar.gz', '') + '.csv'
    with open(archive, 'r:gz') as dest:
        tarfile.open(archive).extract('prices.csv', path=data_dest + '/' + file_name)

