import datetime
import pandas as pd
import os

class DataReader:
    def read_file(self, file_name):
        os.chdir('E:/Projects/Soil_crop Analysis/')
        file_path = 'inputs/' + file_name
        df = pd.read_csv(file_path, low_memory=False)
        return df