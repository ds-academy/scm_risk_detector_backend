import pandas as pd
import glob
import os
from typing import Optional
from typing import List


class Aggregation:

    def __init__(
            self,
            db_connector: str,
            root_path: str,
            save_path: str,
            freq: str = "1D",
    ):

        self.db_connector = db_connector
        self.root_path = root_path
        self.freq = freq  # 1D, 1W, 1M, -> 1D
        self.save_path = save_path

        self.file_list = glob.glob(
            os.path.join(self.root_path, "/*.csv"))

        self.df_list = Optional[List] = None

    def run(self):
        for file in self.file_list:
            df = pd.read_csv(file, index_col=0)
            if not pd.api.types.is_datetime64_dtype(df.dtypes):
                df.index = pd.to_datetime(df.index)
            df = df.resample(self.freq).mean().ffill()
            self.df_list.append(df)

    def write_to_db(self):
        for df in self.df_list:
            try:
                db = self.db_connector
                db.conn()
                db.insert_table(df)
            except Exception as e:
                print(e)
            finally:
                db.close()

    def write_to_pc(self):
        if len(self.file_list) == len(self.df_list):
            for df, file_path in zip(self.df_list, self.file_list):
                file_name = os.path.basename(file_path)
                save_path = os.path.join(
                    self.save_path, file_name
                )
                df.to_csv(save_path)
        else:
            raise Exception("불러온 파일과 파일 리스트가 다릅니다")




