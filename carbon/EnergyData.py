"""
    EnergyData is used to get information about the energy mixture of the grid
"""

import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from datetime import datetime, timedelta
from entsoe import EntsoePandasClient
from variables import CO2_worst, CO2_best, EnergyTypeCat, graph_colors

import os

class EnergyData:

    def add_carbon_stats(self):
        self.carbon_stats = {}
        
        for column_name in self.df.columns:
            self.carbon_stats[column_name] = {"worst": CO2_worst[column_name], "best": CO2_best[column_name]}

        worst_carbon = {x: self.carbon_stats[x]["worst"] for x in self.carbon_stats}

        self.column_names = list(dict(sorted(worst_carbon.items(), key=lambda item: item[1])).keys())

    # def get_CO2(self, serie):
    #     total_CO2_worst = 0
    #     total_CO2_best = 0


    #     for c in self.df.columns:
    #         if c == "total_energy":
    #             continue

    #         val = serie[c]

    #         total_CO2_worst += val * CO2_worst[c]
    #         total_CO2_best += val * CO2_best[c]

    #     return total_CO2_worst, total_CO2_best

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


        self.label_fontsize=15
        self.legend_fontsize=17
        self.tick_size = 15

        matplotlib.rc('xtick', labelsize=self.tick_size) 
        matplotlib.rc('ytick', labelsize=self.tick_size) 




    def plot_energy_mix(self, figsize=(10,5), dpi=200, label_fontsize=15, legend_fontsize=17, tick_size = 14, save=False): 
        df_filterd = self.df[(self.df.index >= start_date) & (self.df.index <= end_date)]
        
        matplotlib.rc('xtick', labelsize=tick_size) 
        matplotlib.rc('ytick', labelsize=tick_size) 

        fig, ax = plt.subplots(figsize=figsize, dpi=dpi)

        X = df_filterd.index 

        Y = np.array([df_filterd[c].to_numpy() for c in self.column_names])

        polys = ax.stackplot(X, *Y, baseline="zero")

        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.xaxis.set_minor_locator(mdates.HourLocator())

        ax.xaxis.set_major_formatter(
            mdates.DateFormatter("%d/%m"))

        legendProxies = []
        for poly in polys:
            legendProxies.append(plt.Rectangle((0, 0), 1, 1, fc=poly.get_facecolor()[0]))



        labels = self.column_names
        legend_labels = [f"{c} (100)" for c in labels]
        
        plt.legend(legendProxies, legend_labels, ncol=3, 
                loc="upper center", bbox_to_anchor=(0.5, -.25), fontsize=legend_fontsize)

        plt.xlabel("Time", fontsize=label_fontsize)
        plt.ylabel("Available energy\n(kWh)", fontsize=label_fontsize)

        if save:
            plt.savefig("figures/energy_mix.pdf", format="pdf", bbox_inches="tight")

        plt.tight_layout()

        plt.show()

        

    def plot_energy_mix_simple(self, start_date, end_date, figsize=(10,5), dpi=200, label_fontsize=15, legend_fontsize=17, tick_size = 14, save=False): 
        df_filterd = self.df[(self.df.index >= start_date) & (self.df.index <= end_date)]
        
        matplotlib.rc('xtick', labelsize=tick_size) 
        matplotlib.rc('ytick', labelsize=tick_size) 

        fig, ax = plt.subplots(figsize=figsize, dpi=dpi)

        X = df_filterd.index 

        green_energy_types = [column for column in self.column_names if EnergyTypeCat[column]["green"]]
        non_green_energy_types = [column for column in self.column_names if not EnergyTypeCat[column]["green"]]


        Y = np.array([ df_filterd[green_energy_types].sum(axis=1).to_numpy(), 
                       df_filterd[non_green_energy_types].sum(axis=1).to_numpy()])

        polys = ax.stackplot(X, *Y, baseline="zero", colors=[graph_colors[1], graph_colors[-1]])

        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.xaxis.set_minor_locator(mdates.HourLocator())

        ax.xaxis.set_major_formatter(
            mdates.DateFormatter("%d/%m"))

        legendProxies = []
        for poly in polys:
            legendProxies.append(plt.Rectangle((0, 0), 1, 1, fc=poly.get_facecolor()[0]))

        
        plt.legend(legendProxies, ["green", "non-green"], ncol=2, 
                loc="upper center", bbox_to_anchor=(0.5, -.25), fontsize=legend_fontsize)

        plt.xlabel("Time", fontsize=self.label_fontsize)
        plt.ylabel("Available energy\n(kWh)", fontsize=self.label_fontsize)

        if save:
            plt.savefig("energy_mix_simple.pdf", format="pdf", bbox_inches="tight")

        plt.tight_layout()

        plt.show()

    def plot_carbon_intensity(self, figsize=(10,5), dpi=200, label_fontsize=15, legend_fontsize=17, tick_size = 14, save=False):
        df_filterd = self.df[(self.df.index >= start_date) & (self.df.index <= end_date)]

        matplotlib.rc('xtick', labelsize=tick_size) 
        matplotlib.rc('ytick', labelsize=tick_size) 

        fig, ax = plt.subplots(figsize=figsize, dpi=dpi)

        ax.plot(df_filterd.carbon_intensity)
        plt.xlabel("Time", fontsize=label_fontsize)
        plt.ylabel("Carbon Intensity\n(gCO2/kWh)", fontsize=label_fontsize)

        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.xaxis.set_minor_locator(mdates.HourLocator())

        ax.xaxis.set_major_formatter(
            mdates.DateFormatter("%d/%m"))

        plt.ylim([0,None])

        if save:
            plt.savefig("carbon_intensity.pdf", format="pdf", bbox_inches="tight")
        plt.tight_layout()
        plt.show()

    def filter_df(self, start_date: pd.Timestamp = None, end_date: pd.Timestamp = None):
        if not start_date:
            start_date = self.start_date
        if not end_date:
            end_date = self.end_date

        return self.df[(self.df.index >= start_date) & (self.df.index <= end_date)]

    def plot_combined(self, start_date: pd.Timestamp = None, end_date: pd.Timestamp = None,    
        figsize=(10,3), dpi=200, label_fontsize=15, legend_fontsize=17, tick_size = 14, save=False):
        
        df_filterd = self.filter_df(start_date, end_date)

        matplotlib.rc('xtick', labelsize=tick_size) 
        matplotlib.rc('ytick', labelsize=tick_size)

        fig, (ax1, ax2) = plt.subplots(nrows=2, sharex=True, figsize=figsize, dpi=dpi)   

        X = df_filterd.index 

        green_energy_types = [column for column in self.column_names if EnergyTypeCat[column]["green"]]
        non_green_energy_types = [column for column in self.column_names if not EnergyTypeCat[column]["green"]]


        Y = np.array([ df_filterd[green_energy_types].sum(axis=1).to_numpy(), 
                       df_filterd[non_green_energy_types].sum(axis=1).to_numpy()])

        polys = ax1.stackplot(X, *Y, baseline="zero", colors=[graph_colors[2], graph_colors[-1]])

        ax1.set_ylabel("Available Energy\n(kWh)", fontsize=label_fontsize)

        ax1.get_yaxis().set_label_coords(-0.15,0.6)

        legendProxies = []
        for poly in polys:
            legendProxies.append(plt.Rectangle((0, 0), 1, 1, fc=poly.get_facecolor()[0]))

        
        ax1.legend(legendProxies, ["green", "grey"], ncol=2, title="Energy Type", title_fontsize=legend_fontsize,
                loc="lower center", bbox_to_anchor=(0.5, 1), fontsize=legend_fontsize)


        ax2.plot(df_filterd.carbon_intensity, color=graph_colors[0])
        ax2.set_xlabel("Time [d]", fontsize=label_fontsize)
        ax2.set_ylabel("Carbon Intensity\n(gCO2/kWh)", fontsize=label_fontsize)

        ax2.set_ylim([0,None])
        ax2.get_yaxis().set_label_coords(-0.15,0.4)
        
        ax2.xaxis.set_major_locator(mdates.DayLocator(interval=7))
        ax2.xaxis.set_minor_locator(mdates.DayLocator())

        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))


        if save:
            plt.savefig("figures/energy_combined.pdf", format="pdf", bbox_inches="tight")

        plt.tight_layout()

        plt.show()