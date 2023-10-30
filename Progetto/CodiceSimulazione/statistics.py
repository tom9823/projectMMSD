#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 16 15:49:20 2021

@author: stevi
"""


import pandas as pd
import calendar

def save_queue_statistics(q):
    with open('../Consegna/Simulazione/Risultati/queue_statistics.txt', 'a') as f:
        print(f'Totale pazienti {q[0]}, totale giorni attesa {q[1]} (/1661908={q[7]}),'
              f' media giorni attesa {q[2]}, totale day_max {q[3]},'
              f' totale cap_max {q[4]}, totale all_max {q[5]},'
              f' settimana {q[6]}\n', file=f)


def convert_queue(file):
    read_file = pd.read_csv('../Consegna/Simulazione/Risultati/'+file + '.txt', header=None, skiprows=1)
    read_file.index.name = 'Index'
    read_file.columns = ['Paziente', 'data_ricovero', 'data_effettivo_ricovero',
                         'ospedale', 'specialita', 'motivazione', 'coda',
                         'giorni_attesa']

    for index, row in read_file.iterrows():
        row.iloc[0] = str(row.iloc[0]).split(" ")[1]
        row.iloc[1] = str(row.iloc[1]).split(" ")[3]
        row.iloc[2] = str(row.iloc[2]).split(" ")[4]
        row.iloc[3] = str(row.iloc[3]).split(" ")[2]
        row.iloc[4] = str(row.iloc[4]).split(" ")[2]
        row.iloc[5] = str(row.iloc[5]).split(" ")[2]
        row.iloc[6] = str(row.iloc[6]).split(" ")[3]
        row.iloc[7] = str(row.iloc[7]).split(" ")[3]

    read_file.to_csv(file + '.csv')

    return read_file

def count_day(file):
    file['giorni_attesa'] = pd.to_numeric(file['giorni_attesa'])
    return file['giorni_attesa'].sum()


def count_patients(file):
    index = file.index
    return len(index)


def counting_motivation(file):
    d = 0
    c = 0
    a = 0
    for p in file.iterrows():
        if p[1]['motivazione'] == 'cap_max':
            c += 1
        else:
            if p[1]['motivazione'] == 'day_max':
                d += 1
            else:
                a += 1
    return d, c, a


def count_week(file):
    w = [0,0,0,0,0,0,0]
    
    day_column = file['data_ricovero']
    for d in day_column:
        year, month, day = (int(i) for i in d.split("-"))
        day = int(calendar.weekday(year, month, day))
        w[day] += 1

    return w

def queue_info(name):
    num = [0]
    tot_patients = 1661908
    for n in num:
        print("queue: "+str(n))
        try:
            file = pd.read_csv('../Consegna/Simulazione/Risultati/queue_info_' + name + '.csv')
        except:
            file = convert_queue('queue_info_' + name)

        tot_queued_patients = count_patients(file)
        days_of_waiting = count_day(file)
        normalized_days = days_of_waiting / tot_patients
        week_days = count_week(file)
        days_mean = days_of_waiting / tot_queued_patients
        tot_day_max, tot_cap_max, tot_all_max = counting_motivation(file)
        save_queue_statistics([tot_queued_patients, days_of_waiting, days_mean,
                               tot_day_max, tot_cap_max, tot_all_max, week_days, normalized_days])


def convert_anticipated_queue(file):
    read_file = pd.read_csv('../Consegna/Simulazione/Risultati/'+file + '.txt', header=None, skiprows=1)

    read_file.index.name = 'Index'
    read_file.columns = ['Paziente', 'data_ricovero', 'data_effettivo_ricovero',
                         'ospedale', 'specialita', 'giorni_attesa']

    for index, row in read_file.iterrows():
        row.iloc[0] = str(row.iloc[0]).split(" ")[1]
        row.iloc[1] = str(row.iloc[1]).split(" ")[3]
        row.iloc[2] = str(row.iloc[2]).split(" ")[4]
        row.iloc[3] = str(row.iloc[3]).split(" ")[2]
        row.iloc[4] = str(row.iloc[4]).split(" ")[2]
        row.iloc[5] = str(row.iloc[5]).split(" ")[3]

    read_file.to_csv(file + '.csv')

    return read_file


def save_anticipated_queue(q):
    with open('../Consegna/Simulazione/Risultati/anticipated_queue_statistics.txt', 'a') as f:
        print(f"Totale pazienti {q[0]}, totale giorni anticipati {q[1]},"
              f" media dei giorni risparmiati {q[2]}\n", file=f)


def anticipated_queue(name):
    num = [0]
    for n in num:
        print("anticipated: "+str(n))
        try:
            file = pd.read_csv('../Consegna/Simulazione/Risultati/anticipated_queue_info_' + name + '.csv')
        except:
            file = convert_anticipated_queue('anticipated_queue_info_' + name)

        tot_patients = count_patients(file)
        anticipated_days = count_day(file)
        days_mean = anticipated_days / tot_patients
        save_anticipated_queue([tot_patients, anticipated_days, days_mean])


if __name__=='__main__':
    name = 'SOMMA'
    queue_info(name)
    anticipated_queue(name)










