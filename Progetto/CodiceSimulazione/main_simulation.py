#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 12:57:59 2021

@author: stevi
"""
import datetime
import time

import pandas as pd

import save_info
import parser_data
import utility_functions as uf
import objects_classes as oc
import remove_resources as rr
import reassing_hospital as rh


def start_simulation(hospitalization_dataframe, hosp_dict, resources_to_remove, policy_resources, solver, time_limit,
                     name):
    """
    Inizio simulazione.

    Come output della simulazione ci saranno 3 file .txt che raccoglieranno
    tutte le info necessarie per l'analisi della simulazione.

    Parameters
    ----------
    hospitalization_dataframe : Dataframe
        Dataframe con info sui pazienti da simulare.
    hosp_dict : Dizionario
        Dizionario creato dai file csv contenente le info degli ospedali.

    Returns
    -------
    None.

    """
    print(
        f"Lunghezza risorse da rimuovere: osp {len(resources_to_remove[0])}; spec {len(resources_to_remove[1])}; data {resources_to_remove[2]}")
    start = time.time()

    # Creo una lista di dataframe, ogni dataframe contiene tutti i pazienti
    # ricoverati lo stesso giorno
    gb = hospitalization_dataframe.groupby('data_ricovero')
    hospitalization_day_list = [gb.get_group(x) for x in gb.groups]

    # Mi salvo il primo anno che incontro
    first_date = hospitalization_day_list[0]['data_ricovero'].iloc[0].strftime("%Y-%m-%d")
    previous_year_simulation = pd.to_datetime(first_date).year

    # Creo la lista degli ospedali
    hosp_list = uf.create_hospital_list(hosp_dict, previous_year_simulation)

    print("Totale giorni: " + str(len(hospitalization_day_list)))

    # Variabili Simulazione.
    # giorni di attesa prima che il sistema arrivi ad una condizione di
    # equilibrio (con 0 o len(hospitalization_day_list) non attivo la parte di anticipare i pazienti)
    spurious_days = 0
    # quantità di giorni da controllare per anticipare pazienti. Utilizzato se attivo spurious_days
    forward_days = 0
    # percentuale di riduzione della capacità giornaliera
    daily_percentage_reduction_capacity = 0
    # soglia dalla quale applicare la riduzione percentuale daily_percentage_reduction_capacity
    capacity_threshold = 13
    # flag di blocco del riassegnamento greedy. Mettere False per usare l'ottimizzatore
    is_optimizer_off = False
    # quale modello utilizzare per l'ottimizzatore.
    optimizer_model_type = oc.OptimizerModelType.NORM_2
    #  Flag per rimuovere le risorse solo una volta. Mettere a False se l'ottimizzatore è attivo
    flg_alt_remove = False

    # Creo i file di log
    save_info.create_day_log(hospitalization_day_list, hosp_list, spurious_days, name)

    save_info.create_queue_info(name)

    save_info.create_anticipated_queue(name)

    # Conto quanti pazienti ci sono
    hospitalization_count = uf.count_total_patient(hospitalization_day_list)
    print("Totale pazienti: " + str(hospitalization_count))

    # Contatore dei giorni, non modificare
    simulation_day_index = 0

    # lista comuni che dovrebbero avere un ospedale ma che non lo hanno nel file distanzeComuniOspedali.csv
    lista_comuni_mancanti = []

    # Inizio della simulazione
    start_time_simulation = time.time()
    print("INIZIO SIMULAZIONE", start_time_simulation)
    for hospitalization_day_dataframe in hospitalization_day_list:
        queue_info = []
        list_anticipated_patients = []
        print("Giorno: " + str(simulation_day_index))

        current_date_simulation = hospitalization_day_dataframe['data_ricovero'].iloc[0].strftime("%Y-%m-%d")
        current_year_simulation = pd.to_datetime(current_date_simulation).year
        print(f"current_date_simulation: {current_date_simulation}")

        # Ottengo il numero del giorno della settimana
        day_of_the_week_number = uf.number_of_the_day(current_date_simulation)

        # Se è cambiato l'anno riaggiorno tutte le capacità e ricopio le vecchie code
        if current_year_simulation != previous_year_simulation:
            print("Nuovo anno!")
            new_hosp_list = uf.create_hospital_list(hosp_dict, current_year_simulation)
            hosp_list = uf.update_hospital_capacity(hosp_list, new_hosp_list)
            print(f"Prima nuovo anno: {len(hosp_list)}")
            hosp_list = rr.remove_resources(hosp_list, resources_to_remove)
            print(f"Dopo nuovo anno: {len(hosp_list)}")
            previous_year_simulation = current_year_simulation
            # Riassegno i pazienti che non hanno l'ospedale
            remaining_days_to_sunday = 7 - day_of_the_week_number
            upper_threshold_simulation_day_index = simulation_day_index + remaining_days_to_sunday  # remaining_days_to_sunday sta per i giorni mancanti alla prossima domenica
            if upper_threshold_simulation_day_index > len(hospitalization_day_list):
                upper_threshold_simulation_day_index = len(hospitalization_day_list)
            print(f'upper_threshold_simulation_day_index: {upper_threshold_simulation_day_index}')
            new_anticipated_days, time_optimization = rh.optimization_reassing(simulation_day_index,
                                                                               upper_threshold_simulation_day_index,
                                                                               hospitalization_day_list,
                                                                               resources_to_remove[0],
                                                                               resources_to_remove[1], hosp_list,
                                                                               policy_resources[0],
                                                                               policy_resources[1], policy_resources[2],
                                                                               policy_resources[3], solver, time_limit,
                                                                               optimizer_model_type)
            print(f'Ottimizzazione durata: {time_optimization} secondi')
            hospitalization_day_list[simulation_day_index:upper_threshold_simulation_day_index] = new_anticipated_days

        # Controllo se c'è da rimuovere delle risorse. La rimozione avviene o se si è superata la data passata
        # dal file o se l'ottimizzatore è attivo. Questo perché l'ottimizzatore parte dalla prima domenica 
        # possibile

        if (current_date_simulation == str(resources_to_remove[2][0])) or (not is_optimizer_off):
            if not flg_alt_remove:
                print(f"Lunghezza ospedali PRIMA {len(hosp_list)}")
                hosp_list = rr.remove_resources(hosp_list, resources_to_remove)
                print(f"Lunghezza ospedali DOPO {len(hosp_list)}")
                flg_alt_remove = True
                # Riassegno i pazienti che non hanno l'ospedale
                remaining_days_to_sunday = 7 - day_of_the_week_number
                upper_threshold_simulation_day_index = simulation_day_index + remaining_days_to_sunday  # remaining_days_to_sunday sta per i giorni mancanti alla prossima domenica
                if upper_threshold_simulation_day_index > len(hospitalization_day_list):
                    upper_threshold_simulation_day_index = len(hospitalization_day_list)
                new_anticipated_days, time_optimization = rh.optimization_reassing(simulation_day_index,
                                                                                   upper_threshold_simulation_day_index,
                                                                                   hospitalization_day_list,
                                                                                   resources_to_remove[0],
                                                                                   resources_to_remove[1], hosp_list,
                                                                                   policy_resources[0],
                                                                                   policy_resources[1],
                                                                                   policy_resources[2],
                                                                                   policy_resources[3], solver,
                                                                                   time_limit, optimizer_model_type)
                print(f"Ottimizzazione durata: {time_optimization} secondi")
                hospitalization_day_list[
                simulation_day_index:upper_threshold_simulation_day_index] = new_anticipated_days
        # Decremento i giorni di degenza dei pazienti nelle varie specialità h, poi levo i pazienti a 0 giorni di
        # degenza.
        for h in hosp_list:
            for p in h.rest_queue:
                p.rest_time -= 1
            h.rest_queue = [p for p in h.rest_queue if p.rest_time > 0]

        # Controllo se posso ricoverare i pazienti nella lista di attesa
        for h in hosp_list:
            for p in h.waiting_queue:
                # controllo se non sforo il numero di letti massimo della specialità
                if len(h.rest_queue) < int(h.capacity[7]):
                    # controllo se non sforo la capacità giornaliera della specialità
                    if h.counter_current_day_patients_recovered < int(h.capacity[day_of_the_week_number]):
                        # ricovero il paziente
                        h.counter_current_day_patients_recovered += 1
                        h.rest_queue.append(p)
                        # rimuovo il paziente dalla coda di attesa della specialità
                        h.waiting_queue = ([n_p for n_p in h.waiting_queue
                                            if int(n_p.id_patient) != int(p.id_patient)]
                        )
                        p.patient_true_day_recovery = current_date_simulation
                        queue_info.append([p, h])
        # Controllo se posso ricoverare i nuovi pazienti
        for index, hospitalization_record in hospitalization_day_dataframe.iterrows():
            # Leggo e salvo le info del paziente
            hospitalization_record_id_hosp = hospitalization_record.loc['codice_struttura_erogante']
            hospitalization_record_id_spec = hospitalization_record.loc['COD_BRANCA']
            hospitalization_record_id_ricovero = index

            # Controllo se l'ospedale o la specialità è stata eliminata, nel caso li aggiorno con la policy
            threshold_date_to_close_speciality = datetime.datetime.strptime(str(resources_to_remove[2][0]),
                                                                            "%Y-%m-%d").date()
            # Rimuovo le specialità se si è superata la data e se non si usa l'ottimizzazione settimanale(is_optimizer_off == True)
            if is_optimizer_off and datetime.datetime.strptime(current_date_simulation,
                                                               "%Y-%m-%d").date() >= threshold_date_to_close_speciality:
                # rimuovo sulla base della regola statica
                hospitalization_record_id_hosp = rr.removed_id_check(resources_to_remove,
                                                                     hospitalization_record_id_hosp,
                                                                     hospitalization_record_id_spec,
                                                                     hosp_list, lista_comuni_mancanti, policy_resources,
                                                                     hospitalization_record_id_ricovero)

            # Recupero l'oggetto ospedale del paziente
            hospitalization_record_hospital_object = uf.get_hospitalization_hospital(hosp_list,
                                                                                     hospitalization_record_id_hosp,
                                                                                     hospitalization_record_id_spec)

            hospitalization_record_rest_time = int(hospitalization_record.loc['gg_degenza'])

            hospitalization_record_recovery_date = hospitalization_record['data_ricovero'].strftime("%Y-%m-%d")

            # Creo l'oggetto paziente con tutte le info
            current_hospitalization_patient_object = oc.Patient(index, hospitalization_record_rest_time,
                                                                hospitalization_record_recovery_date,
                                                                hospitalization_record_id_hosp,
                                                                hospitalization_record_id_spec)

            # ottengo le due capacità
            hosp_max_beds_capacity = int(hospitalization_record_hospital_object.capacity[7])
            hosp_day_capacity = int(hospitalization_record_hospital_object.capacity[day_of_the_week_number])
            # ci sono casi in cui la capacità giornaliera è 0, non li considero
            if hosp_day_capacity != 0:
                # diminuisco di una percentuale arrotondando per difetto con
                # vincolo sulla capacità
                if hosp_day_capacity > capacity_threshold:
                    hosp_day_capacity = int(hosp_day_capacity -
                                            (hosp_day_capacity * daily_percentage_reduction_capacity)
                                            )
                # se la capacità diventa nulla la metto a 1
                if hosp_day_capacity == 0:
                    hosp_day_capacity = 1

            # Inizio controllo vincoli
            # Controllo se si è sforata la capacità massima di posti letto
            if len(hospitalization_record_hospital_object.rest_queue) >= hosp_max_beds_capacity:
                # controllo se anche il giorno è pieno, vengono
                # comunque inseriti nella coda della capacità massima ma il
                # paziente ha una motivazione diversa
                # DA RIVEDERE CODICE UGUALE
                if hospitalization_record_hospital_object.counter_current_day_patients_recovered >= hosp_day_capacity:
                    current_hospitalization_patient_object.queue_motivation = 'all_full'
                    hospitalization_record_hospital_object.counter_max_queue = (hospitalization_record_hospital_object.
                                                                                counter_max_queue + 1)
                    current_hospitalization_patient_object.counter_queue = (hospitalization_record_hospital_object.
                                                                            counter_max_queue)
                    hospitalization_record_hospital_object.waiting_queue.append(current_hospitalization_patient_object)
                else:
                    current_hospitalization_patient_object.queue_motivation = 'hospital_speciality_beds_full'
                    hospitalization_record_hospital_object.counter_max_queue = (hospitalization_record_hospital_object.
                                                                                counter_max_queue + 1)
                    current_hospitalization_patient_object.counter_queue = (hospitalization_record_hospital_object.
                                                                            counter_max_queue)
                    hospitalization_record_hospital_object.waiting_queue.append(current_hospitalization_patient_object)
            else:
                # Controllo se si è sforata la capacità giornaliera massima
                if hospitalization_record_hospital_object.counter_current_day_patients_recovered >= hosp_day_capacity:
                    current_hospitalization_patient_object.queue_motivation = 'hospital_speciality_capacity_day_full'
                    hospitalization_record_hospital_object.counter_day_queue = (hospitalization_record_hospital_object.
                                                                                counter_day_queue + 1)
                    current_hospitalization_patient_object.counter_queue = (hospitalization_record_hospital_object.
                                                                            counter_day_queue)
                    hospitalization_record_hospital_object.waiting_queue.append(current_hospitalization_patient_object)
                else:
                    hospitalization_record_hospital_object.counter_current_day_patients_recovered = (
                                hospitalization_record_hospital_object.
                                counter_current_day_patients_recovered + 1)
                    hospitalization_record_hospital_object.rest_queue.append(current_hospitalization_patient_object)

        # Controllo se posso anticipare l'ingresso di pazienti nei prossimi
        # remaining_days_to_sunday giorni se ho superato 'spurious_days' giorni
        # ATTENZIONE! Fare attenzione se si usa insieme all'ottimizzatore settimanale
        if simulation_day_index > spurious_days:
            upper_threshold_simulation_day_index = simulation_day_index + 1 + forward_days
            if upper_threshold_simulation_day_index > len(hospitalization_day_list):
                upper_threshold_simulation_day_index = len(hospitalization_day_list)
            # Prendo una finestra di dataframe
            anticipated_days = hospitalization_day_list[simulation_day_index + 1: upper_threshold_simulation_day_index]
            tmp_ant_day = []
            # cicla dall'indomani fino al giorno forward days
            for next_day in anticipated_days:
                patient_removed = []
                for index, hospitalization_record in next_day.iterrows():
                    hospitalization_record_id_hosp = hospitalization_record.loc['codice_struttura_erogante']
                    hospitalization_record_id_spec = hospitalization_record.loc['COD_BRANCA']
                    hospitalization_record_hospital_object = uf.get_hospitalization_hospital(hosp_list,
                                                                                             hospitalization_record_id_hosp,
                                                                                             hospitalization_record_id_spec)
                    if hospitalization_record_hospital_object != 'None':
                        if len(hospitalization_record_hospital_object.rest_queue) < hosp_max_beds_capacity:
                            hosp_day_capacity = int(hospitalization_record_hospital_object.
                                                    capacity[day_of_the_week_number])
                            if hospitalization_record_hospital_object.counter_current_day_patients_recovered < hosp_day_capacity:
                                hospitalization_record_hospital_object.counter_current_day_patients_recovered = (
                                            hospitalization_record_hospital_object.
                                            counter_current_day_patients_recovered + 1)

                                hospitalization_record_rest_time = int(hospitalization_record.loc['gg_degenza'])
                                hospitalization_record_recovery_date = (str(hospitalization_record['data_ricovero']).
                                split(" ")[0])
                                current_hospitalization_patient_object = oc.Patient(index,
                                                                                    hospitalization_record_rest_time,
                                                                                    hospitalization_record_recovery_date,
                                                                                    hospitalization_record_id_hosp,
                                                                                    hospitalization_record_id_spec)
                                current_hospitalization_patient_object.patient_true_day_recovery = str(
                                    current_date_simulation)
                                hospitalization_record_hospital_object.rest_queue.append(
                                    current_hospitalization_patient_object)
                                list_anticipated_patients.append(current_hospitalization_patient_object)
                                patient_removed.append(index)

                for p in patient_removed:
                    next_day = next_day.drop(p)
                tmp_ant_day.append(next_day)
            # Sostituisco i dataframe in hospitalization_day_list con quelli modificati
            # in anticipated_days
            hospitalization_day_list[simulation_day_index + 1:upper_threshold_simulation_day_index] = tmp_ant_day

        # Ripianifico la destinazione dei pazienti della prossima settimana utilizzando 
        # l'ottimizzatore. Lo eseguo la domenica di ogni settimana.
        if (day_of_the_week_number == 6) and (not is_optimizer_off):
            print("E' domenica")
            d1 = simulation_day_index + 1
            upper_threshold_simulation_day_index = d1 + 8  # 8 indica l'intera settimana. !IMPORTANTE non prendere meno di una settimana
            if upper_threshold_simulation_day_index > len(hospitalization_day_list):
                upper_threshold_simulation_day_index = len(hospitalization_day_list)
            new_anticipated_days, time_optimization = rh.optimization_reassing(d1, upper_threshold_simulation_day_index,
                                                                               hospitalization_day_list,
                                                                               resources_to_remove[0],
                                                                               resources_to_remove[1], hosp_list,
                                                                               policy_resources[0],
                                                                               policy_resources[1], policy_resources[2],
                                                                               policy_resources[3], solver, time_limit,
                                                                               optimizer_model_type)
            print(f"Ottimizzazione durata: {time_optimization} secondi")
            hospitalization_day_list[d1:upper_threshold_simulation_day_index] = new_anticipated_days

        # Salvo le info dei pazienti ricoverati in anticipo
        save_info.anticipated_patient(list_anticipated_patients, name)

        # Salvo le info della giornata degli ospedali
        save_info.save_day_info(hosp_list, simulation_day_index, day_of_the_week_number, name)

        # Salvo le info della situazione code
        save_info.save_queue_info(queue_info, day_of_the_week_number, name)

        # Resetto i counter di tutti gli ospedali
        for h in hosp_list:
            h.counter_current_day_patients_recovered = 0
            h.counter_day_queue = 0
            h.counter_max_queue = 0

        # Nuovo giorno
        simulation_day_index += 1

    # salvo la lista lista_comuni_mancanti
    save_info.save_lista_comuni_mancanti(lista_comuni_mancanti, name)

    end = time.time()
    print(f"Durata di esecuzione: {(end - start) / 60} minuti; {(end - start)} secondi.")


if __name__ == '__main__':
    # Nome del file in cui salvare le statistiche e logs
    name = 'SOMMA'
    # Caricamento dei dati di partenza
    resources, patients = parser_data.load_data()
    patients = patients.set_index(
        'n_record')  # le colonne dei giorni indicano il numero di pazienti ricoverabili nel giorno stesso mentre l'ultima colonna K_MAX indica la capacità giornaliera per poterli curare4++++
    hosp_dict = parser_data.load_hosp_dict(resources)
    # leggo file in cui sono definiti le risorse(ospedale con relativa specialità) da chiudere e date chiusura
    file = "../Parametri/remove_info.txt"
    hosp_id_list, hosp_spec_list, date = rr.read_input(file)
    # dizionario che mappa comune con ospedale
    dict_mapping_hospital_com, dict_distances_com = parser_data.load_policy_data()
    # stesso dizionario di dict_mapping ma con chiave valore invertito (inverso di quello sopra)
    dict_mapping_com_hospital = {v: k for k, v in dict_mapping_hospital_com.items()}
    # Solver del modello di ottimizzazione
    solver = "glpk"
    # Tempo in secondi a disposizione del solver
    time_limit = 10
    # dizionario per la residenza del paziente
    dict_map_residenza = parser_data.load_residenze()

    start_simulation(patients, hosp_dict, [hosp_id_list, hosp_spec_list, date], [dict_mapping_hospital_com,
                                                                                 dict_distances_com,
                                                                                 dict_mapping_com_hospital,
                                                                                 dict_map_residenza], solver,
                     time_limit, name)
