#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  9 13:59:01 2021

@author: stevi
"""
from datetime import date


def create_day_log(patient_day_list, hosp_list, spurious_days, name):
    with open('../Simulazione/Risultati/log_day_' + name +'.txt', 'w') as log:
        i1 = len(patient_day_list)
        i2 = len(hosp_list)
        print(f"Il numero totale di giorni è {i1} e il numero di specialità è"
              f" {i2}, giorni spuri {spurious_days}\n", file=log)


def create_queue_info(name):
    with open('../Simulazione/Risultati/queue_info_' + name + '.txt', 'w') as q:
        print(f"Numero coda indica quanto ha sforato il paziente", file=q)


def create_anticipated_queue(name):
    with open('../Simulazione/Risultati/anticipated_queue_info_' + name + '.txt', 'w') as q:
        print(f"Lista dei pazienti ricoverati in anticipo", file=q)


def save_day_info(hosp_list, day, weekday, name):
    with open('../Simulazione/Risultati/log_day_' + name + '.txt', 'a') as log:
        for h in hosp_list:
            i1 = len(h.rest_queue)
            i2 = h.capacity[7]
            i3 = len(h.waiting_queue)
            i4 = h.capacity[weekday]
            print(f"Giorno {day}, ospedale {h.id_hosp}, specialità {h.id_spec},"
                  f" ricoveri {h.counter_day_cap}/{i4}, letti occupati"
                  f" {i1}/{i2}, coda attesa {i3}",
                  file=log)
        print(f"\n", file=log)


def save_queue_info(queue_info, day, name):
    with open('../Simulazione/Risultati/queue_info_' + name + '.txt', 'a') as info:
        for p in queue_info:
            if p[0].patient_day_recovery != p[0].patient_true_day_recovery:
                i1 = p[0].id_patient
                i2 = p[0].patient_day_recovery
                i3 = p[0].patient_true_day_recovery
                i4 = p[0].patient_id_hosp
                i5 = p[0].patient_id_spec
                i6 = p[0].queue_motivation
                i7 = p[0].counter_queue
                if i6 == 'cap_max':
                    i8 = p[1].capacity[7]
                else:
                    i8 = p[1].capacity[day]

                d0 = date(int(str(i2.split("-")[0])), int(str(i2.split("-")[1])),
                          int(str(i2.split("-")[2])))
                d1 = date(int(str(i3.split("-")[0])), int(str(i3.split("-")[1])),
                          int(str(i3.split("-")[2])))
                delta = d1 - d0
                i9 = delta.days
                print(f"Paziente {i1}, data ricovero {i2}, data effettivo ricovero "
                      f"{i3}, ospedale {i4}, specialità {i5}, motivazione {i6},"
                      f" numero coda {i7}/{i8}, giorni attesa {i9}", file=info)


def anticipated_patient(queue_anticipated_days, name):
    with open('../Simulazione/Risultati/anticipated_queue_info_' + name + '.txt', 'a') as info:
        for p in queue_anticipated_days:
            i1 = p.id_patient
            i2 = p.patient_day_recovery
            i3 = p.patient_true_day_recovery
            i4 = p.patient_id_hosp
            i5 = p.patient_id_spec
            d0 = date(int(str(i2.split("-")[0])), int(str(i2.split("-")[1])),
                      int(str(i2.split("-")[2])))
            d1 = date(int(str(i3.split("-")[0])), int(str(i3.split("-")[1])),
                      int(str(i3.split("-")[2])))
            delta = d0 - d1
            i6 = delta.days
            print(f"Paziente {i1}, data ricovero {i2}, data effettivo ricovero "
                  f"{i3}, ospedale {i4}, specialità {i5}, giorni anticipati {i6}",
                  file=info)


def save_lista_comuni_mancanti(lista_comuni_mancanti, name):
    with open('../Simulazione/Risultati/lista_comuni_mancanti_' + name + '.txt', 'w') as l:
        for c in lista_comuni_mancanti:
            print(f"Comune: {c[0]}, ospedale: {c[1]}", file=l)
