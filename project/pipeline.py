import urllib.error
import sqlite3
import time
import numpy as np
import pandas as pd

# The datasets were downloaded from Kaggle,
# but because there was no way to link them directly without using the API,
# they were moved to Google Drive
drive_file1 = "https://drive.google.com/file/d/13R4eUmCy5EUpYrWP1lr4OoOFcl1fjIhA/view?usp=sharing"
file_id1 = drive_file1.split('/')[-2]
url1 = 'https://drive.google.com/uc?id=' + file_id1

drive_file2 = "https://drive.google.com/file/d/1dg4jLrPAhOKt5JAZbvufh8nqHkErUK3S/view?usp=sharing"
file_id2 = drive_file2.split('/')[-2]
url2 = 'https://drive.google.com/uc?id=' + file_id2

# Load data
def load_data(url: str, loads: int = 0) -> pd.DataFrame:
    file = None
    for _ in range(loads+1):
        try:
            file = pd.read_csv(url, header = 0, on_bad_lines = 'skip')
            break
        except (urllib.error.HTTPError, urllib.error.URLError):
            time.sleep(5)
    return file

# Dataset 1 - Amazon Rainforest
def dataset1() -> pd.DataFrame:
    file = load_data(url = url1, loads = 5)
    
    if file is None:
        raise FileNotFoundError("Could not load Data Source 1")
    
    file = file.iloc[:, :10]
    file.columns = ["Occurence Year", "Acre State", "Amazonas State", "Amapa State", \
        "Maranhao State", "Mato Grosso State", "Para State", "Rondonia State", "Roraima State", \
            "Tocantins State"]
    
    file.replace("", np.NaN, inplace=True)
    file.dropna(how="all", inplace=True)
    
    return file

# Dataset 2 - Temperature Change
def dataset2() -> pd.DataFrame:
    file = load_data(url = url2, loads = 5)
    
    if file is None:
        raise FileNotFoundError("Could not load Data Source 2")
    
    file = file.iloc[:, [3, 7, 9, 11]]
    file.columns = ["Country Name", "Months", "Year", "Value"]
    
    file.replace("", np.NaN, inplace=True)
    file.dropna(how="all", inplace=True)
    
    mask = file['Country Name'] != 'Brazil'
    file = file[~mask]
    
    return file

def main():
    amazondf = dataset1()
    tempdf = dataset2()
    
    conn = sqlite3.connect('./data/data.sqlite')
    
    amazondf.to_sql("amazon_forests", conn, if_exists = "replace", index = False)
    print("Amazon Rainforest Data has been successfully added to SQLite")
    
    tempdf.to_sql("temp_change", conn, if_exists = "replace", index = False)
    print("Temperature Change Data has been successfully added to SQLite")
    
    conn.commit()

if __name__ == "__main__":
    main()
