import pandas as pd
import numpy as np
import json
from pandas_profiling import ProfileReport


class Profile:
    def __init__(self, data):
        self.data = data

    def create_profile(self, title, timestamp, batch_id, file_name):
        profile = ProfileReport(self.data, title= title, explorative=True)
        json_data = profile.to_json()
        #print(json_data)
        #json_data = json_data.replace("NaN","")
        json_acceptable_string = json_data.replace('"', "\"")
        d = json.loads(json_acceptable_string)
        with open('data.txt', 'w') as outfile:
            json.dump(d, outfile)
            
        # get warnings
        dic = {}
        df_warn = pd.DataFrame()
        for i in d['messages']:
            if i.split('column')[1] not in dic.keys():
                dic[i.split('column')[1]] = []
            dic[i.split('column')[1]].append(i)
        df_warn['col'] = dic.keys()
        df_warn['warnings'] = dic.values()
        df_warn['batch_id'] = batch_id
        df_warn['source_name'] = file_name
        
        # process correlation data
        dfs = []
        for algo in d['correlations'].keys():
            df_corr = pd.DataFrame(d['correlations'][algo])
            df_corr.set_index(df_corr.columns, inplace=True,
                              append=True, drop=False)
            df_corr.set_index(df_corr.columns, inplace=True,
                              append=True, drop=False)
            df_corr['Corr_algo'] = algo

            dfs.append(df_corr)
        final_corr = pd.concat(dfs)
        final_corr.set_index(final_corr['Corr_algo'], inplace=True,
                             append=True, drop=False)
        del final_corr['Corr_algo']
        final_corr_data = pd.DataFrame(final_corr.stack()).reset_index()
        del final_corr_data['level_0']
        del final_corr_data['level_1']
        final_corr_data.columns = ['X', 'Algo', 'y', 'Value']
        final_corr_data['timestamp'] = timestamp
        final_corr_data['batch_id'] = batch_id
        final_corr_data['source_name'] = file_name
        # Porcess table level info
        d['table']['types'] = str(d['table']['types'])
        table_level_profile = pd.DataFrame(d['table'], index=[0])
        table_level_profile['timestamp'] = timestamp
        table_level_profile['batch_d'] = batch_id
        table_level_profile['source_name'] = file_name

        #process column level info
        attribute_profile = pd.DataFrame(d['variables']).transpose()
        del attribute_profile['histogram_frequencies']
        del attribute_profile['first_rows']
        del attribute_profile['length']
        del attribute_profile['histogram_length']
        del attribute_profile['value_counts_without_nan']

        columns = []
        for col in attribute_profile.columns:
            if '%' in col:
                col = col.replace('%', 'percent')
            columns.append(col)
        attribute_profile.columns = columns
        attribute_profile['timestamp'] = timestamp
        attribute_profile['batch_id'] = batch_id
        attribute_profile['source_name'] = file_name
        new_val = []
        for v in attribute_profile['chi_squared']:
            if isinstance(v,dict):
                for key, val in v.items():
                    if np.isnan(val):
                        v[key] = 0.0
                    else:
                        v[key] = val
                new_val.append(v)
            else:
                new_val.append(v)
        attribute_profile['chi_squared'] = new_val

        return final_corr_data, table_level_profile, attribute_profile,df_warn


