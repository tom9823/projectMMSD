import time

import pandas as pd
import optimization_model_sum as oms
import utility_functions as uf
from pyomo.opt import *

from Progetto.CodiceSimulazione.objects_classes import OptimizerModelType


def __find_distance(id_pat, id_hosp, map_com_hosp, dict_distances, dict_map_residenza):
    # Dato un id_paziente e id_ospedale restituisce la distanza dal comune del paziente a 
    # quell'ospedale
    id_com_pat = dict_map_residenza.get(int(id_pat), None)
    # Se non trovo il comune del paziente assumo che sia quello dell'ospedale quindi la distanza è 0
    if id_com_pat == None:
        # id_com_pat = ['cuneo', 4078]
        return 0
    id_com_hosp = map_com_hosp.get(int(id_hosp), None)
    if id_com_hosp == None:
        # id_com_hosp = '4078'
        return 0
    tmp = dict_distances.get(str(id_com_pat[1]), None)
    if tmp == None:
        dis = 1
    else:
        dis = tmp.get(int(id_com_hosp), None)
    return int(dis)


def calc_pat_to_reassign(hospitalization_day_dataframe_list, hosp_id_list, hosp_spec_id_list):
    # Creo la lista dei pazienti che devono essere riassegnati
    patient_to_reassign_dict = {}
    for next_day in hospitalization_day_dataframe_list:
        for index, hospitalization in next_day.iterrows():
            patient_id_hosp = hospitalization.loc['codice_struttura_erogante']
            patient_id_spec = hospitalization.loc['COD_BRANCA']

            if (int(patient_id_hosp) in hosp_id_list) or (int(patient_id_spec) in hosp_spec_id_list):
                patient_to_reassign_dict[index] = hospitalization

        # Concatena tutti i DataFrame presenti nella lista senza ignorare l'indice
    return pd.DataFrame.from_dict(patient_to_reassign_dict, orient='index')


def rest_days(patient_to_reassing):
    l = {}
    for index, p in patient_to_reassing.iterrows():
        l.update({index: p['gg_degenza']})
    return l


def list_pat(patient_to_reassing):
    list_pat = []
    P = {}
    for index, p in patient_to_reassing.iterrows():
        list_pat.append(index)
    P.update({None: list_pat})
    return P


def __hosp_spec_list(hosp_lists, spec):
    H = {}
    tmp_hosp = []
    for h in hosp_lists:
        if h.id_spec == spec:
            tmp_hosp.append(h.id_hosp)
    H.update({None: tmp_hosp})
    return H, tmp_hosp


def old_distance(patient_to_reassing, dict_mapping, dict_distances, dict_map_residenza):
    # id_pat:distanza
    m = {}
    for index, p in patient_to_reassing.iterrows():
        p_id_hosp = p.loc['codice_struttura_erogante']
        p_id = index
        dis = __find_distance(p_id, p_id_hosp, dict_mapping, dict_distances, dict_map_residenza)
        m.update({p_id: dis})
        # print(f'm finale: {m}')
    return m


def all_distance(patient_to_reassing, spec_hosp_list, hosp_list, spec, dict_mapping, dict_distances,
                 dict_map_residenza):
    # (id_pat,id_hosp):distanza
    d = {}
    for index, p in patient_to_reassing.iterrows():
        p_id = index
        for sh in spec_hosp_list:
            for h in hosp_list:
                if sh == h.id_hosp:
                    if h.id_spec == spec:
                        dis = __find_distance(p_id, h.id_hosp, dict_mapping, dict_distances, dict_map_residenza)
                        d.update({(p_id, h.id_hosp): dis})
    return d


def calculate_gamma(l, H, hosp_list, spec, alfa):
    # gamma: (1+alfa)*f*L
    g = {}
    L = sum(l.values())
    tmp_h = H[None]
    f_tot = 0  # capacità per tutti gli ospedali
    for h in tmp_h:
        for i in hosp_list:
            if h == i.id_hosp:
                if spec == i.id_spec:
                    f_tot += i.capacity[7]
    for h in tmp_h:
        f1 = 0
        for i in hosp_list:
            if h == i.id_hosp:
                if spec == i.id_spec:
                    f1 = i.capacity[7]  # prendo la capacità massima
        f = f1 / f_tot
        """f è il rapporto tra capacità massima del singolo ospedale per specialità e capacità massima di tutti gli ospedali per specialità"""
        tmp_gamma = (1 + alfa) * f * L
        g.update({h: tmp_gamma})
    return g


def create_data(l, p, H, m, d, gamma):
    # Creo il dizionario con tutti i dati per il modello
    data = {}
    tmp_d = {}
    tmp_d.update({'P': p})
    tmp_d.update({'l': l})
    tmp_d.update({'H': H})
    tmp_d.update({'m': m})
    tmp_d.update({'d': d})
    tmp_d.update({'gamma': gamma})
    data.update({None: tmp_d})
    return data


def optimization_reassing(simulation_day_index, upper_threshold_simulation_day_index, hospitalization_day_list,
                          closing_hosp_id_list, closing_spec_list, hosp_list, dict_mapping,
                          dict_distances, dict_map_residenza, map_com_hosp, solver, time_limit, optimizer_model_type):
    start_time = time.time()
    new_list_hosp = []

    # Prendo una finestra di dataframe
    hospitalization_day_to_reassign_dataframe_list = hospitalization_day_list[
                                           simulation_day_index:upper_threshold_simulation_day_index]
    # Lista dei ricoveri in cui i pazienti sono da riassegnare
    hospitalization_to_reassign_dataframe_compat = calc_pat_to_reassign(hospitalization_day_to_reassign_dataframe_list,closing_hosp_id_list, closing_spec_list)
    print(f'SIMULATION DAY INDEX: {simulation_day_index}. Num pazienti: {sum(len(df) for df in hospitalization_to_reassign_dataframe_compat)}')

    if not hospitalization_to_reassign_dataframe_compat.empty:

        # Li divido per specialità per poter utilizzare l'ottimizzatore su gruppi omogenei
        hospitalization_by_spec_dataframe_list = [hospitalization_to_reassign_dataframe_compat[hospitalization_to_reassign_dataframe_compat['COD_BRANCA'] == valore].copy() for valore in hospitalization_to_reassign_dataframe_compat['COD_BRANCA'].unique()]

        # Per ogni specialità calcolo un modello
        print(
            f'Gruppi di specialità: {len(hospitalization_by_spec_dataframe_list)}. Totale pazienti: {uf.count_total_patient(hospitalization_by_spec_dataframe_list)}.')
        spec_counter = 1
        spec_count = len(hospitalization_by_spec_dataframe_list)
        for hospitalization_by_spec_dataframe in hospitalization_by_spec_dataframe_list:
            current_spec_id = hospitalization_by_spec_dataframe['COD_BRANCA'].iloc[0]
            # print(current_spec_id)
            print(
                f'Giro {spec_counter}/{spec_count}. Quantità pazienti nella specialità {current_spec_id}: {len(hospitalization_by_spec_dataframe)}')
            # Creo il dizionario id_ricovero:giorni_degenza
            l = rest_days(hospitalization_by_spec_dataframe)
            # print(f'Lista L: {l}')
            # Creo dizionario None:lista id_ricovero
            p = list_pat(hospitalization_by_spec_dataframe)
            # print(f'Lista P: {p}')
            # Creo dizionario None:lista id_ospedale (divisi per specialità)
            H, tmp_hosp_list = __hosp_spec_list(hosp_list, current_spec_id)
            # print(f'Lista H: {H}')
            # Creo dizionario id_ricovero:distanza_vecchio_ospedale
            m = old_distance(hospitalization_by_spec_dataframe, dict_mapping, dict_distances, dict_map_residenza)
            # print(f'Lista m: {m}')
            # Creo il dizionario (id_ricovero,id_hosp):dis per ogni paziente di quella specialità per ogni
            # ospedale con quella specialità
            d = all_distance(hospitalization_by_spec_dataframe, tmp_hosp_list, hosp_list, current_spec_id, map_com_hosp,
                             dict_distances, dict_map_residenza)
            alfa = 0.5
            gamma = calculate_gamma(l, H, hosp_list, current_spec_id, alfa)
            data = create_data(l, p, H, m, d, gamma)
            # Calcolo il modello

            results, model = oms.create_model(data, solver, time_limit, optimizer_model_type)

            list_of_alfa = []
            list_of_alfa.append([alfa, hospitalization_by_spec_dataframe])
            while results.solver.termination_condition == TerminationCondition.infeasible:
                alfa += 0.5
                gamma = calculate_gamma(l, H, hosp_list, current_spec_id, alfa)
                data = create_data(l, p, H, m, d, gamma)

                results, model = oms.create_model(data, solver, time_limit, optimizer_model_type)

                list_of_alfa.append([alfa, hospitalization_by_spec_dataframe])

            # Aggiorno new_list_hosp con le tuple (id_paz,id_ospedale)
            for tupla_x in model.x:
                if model.x[(tupla_x[0], tupla_x[1])].value == 1:
                    new_list_hosp.append(tupla_x)

            spec_counter += 1

        # sostituisco i vecchi pazienti con i pazienti con dati aggiornati
        # print(f'Inizio sostituzione. Lunghezza t: {len(new_list_hosp)}. Lunghezza d: {len(hospitalization_day_to_reassign_dataframe_list)}.Lunghezza row: {len(hospitalization_day_to_reassign_dataframe_list[0])}.Lunghezza row: {len(hospitalization_day_to_reassign_dataframe_list[1])}')
        for t in new_list_hosp:
            for hospitalization_day_to_reassign_dataframe in hospitalization_day_to_reassign_dataframe_list:
                if t[0] in hospitalization_day_to_reassign_dataframe.index:
                    hospitalization_day_to_reassign_dataframe.at[t[0], 'codice_struttura_erogante'] = '0' + str(t[1])

    return hospitalization_day_to_reassign_dataframe_list, time.time() - start_time
