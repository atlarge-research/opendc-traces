"""
    EnergyData is used to get information about the energy mixture of the grid
"""

import pandas as pd
import numpy as np

from entsoe import EntsoePandasClient
from variables import CO2_worst, CO2_best

import os

class EnergyData:

    def add_carbon_stats(self):
        self.carbon_stats = {}
        
        for column_name in self.df.columns:
            self.carbon_stats[column_name] = {"worst": CO2_worst[column_name], "best": CO2_best[column_name]}

        worst_carbon = {x: self.carbon_stats[x]["worst"] for x in self.carbon_stats}

        self.column_names = list(dict(sorted(worst_carbon.items(), key=lambda item: item[1])).keys())


    def get_carbon_usage(self, serie):
        total_carbon = 0

        for column_name in self.column_names:
            if column_name == "total_energy":
                continue

            val = serie[column_name]

            total_carbon += val * CO2_best[column_name]

        return total_carbon
    
    

    def get_energy_mix(self, timestamp):
        index = np.searchsorted(self.df.index, timestamp, side="right") - 1

        return self.df.iloc[index]["carbon_intensity"]

    @property
    def min_date(self):
        return self.df.index[0]

    @property
    def max_date(self):
        return self.df.index[-1]

    def __len__(self):
        return len(self.df.index)

    def __init__(self, start: pd.Timestamp, end: pd.Timestamp, 
                 country_code: str):

        self.start_date = start
        self.end_date = end
        self.country_code = country_code

        if os.path.exists(f"pickles/{self.start_date}_{self.end_date}_{self.country_code}.pkl"):
                    self.df = pd.read_pickle(f"pickles/{self.start_date}_{self.end_date}_{self.country_code}.pkl")
        else:
            self.client = EntsoePandasClient(api_key="b637f458-cdb7-4771-92bf-cfd49f3646d9")
            self.df = self.client.query_generation(self.country_code, start=self.start_date, end=self.end_date, psr_type=None)
            self.df.to_pickle(f"pickles/{self.start_date}_{self.end_date}_{self.country_code}.pkl")
            
        columns_to_delete = [c for c in self.df.columns if c[1] == 'Actual Consumption']
        self.df = self.df.drop(columns_to_delete, axis = 1)
        
        self.df.columns = [c[0] for c in self.df.columns]
        self.df = self.df.fillna(0)

        self.add_carbon_stats()

        self.df["total_energy"] = self.df.sum(axis=1)   

        self.df["carbon_usage"] = self.df.apply(self.get_carbon_usage, axis=1)
        self.df["carbon_intensity"] = self.df["carbon_usage"] / self.df["total_energy"]

        self.df.to_csv(f"results/all/{start.strftime("%Y-%m-%d")}_{end.strftime("%Y-%m-%d")}_{country_code}.csv")
