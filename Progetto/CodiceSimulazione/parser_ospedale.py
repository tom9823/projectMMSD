#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

year = ['2011', '2012', '2013']
file_name = 'specialtyCapacitySchedules'
path = "Dati/"

file_list = []
for y in year:
    data = pd.read_excel(r'../' + path + y + '/' + file_name + '.xlsx')
    df = pd.DataFrame(data)
    tmp_df = df.sort_values(by=['codici_ospedale'])
    cols = list(tmp_df.columns)
    a, b = cols.index('codici_ospedale'), cols.index('codici_specialita')
    cols[b], cols[a] = cols[a], cols[b]
    new_df = tmp_df[cols]
    year_list = [y] * new_df.shape[0]
    new_df['year'] = year_list
    file_list.append(new_df)


df = pd.concat(file_list, ignore_index=True)
df.index.name = 'Index'
df.to_csv(r'../'+ 'Dati_Elaborati' + '/' + file_name + '.csv')
