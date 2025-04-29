import logging
import time

import pandas as pd
import optimization_model_sum as oms
from pyomo.opt import *

from Progetto.CodiceSimulazione.main_simulation import DEFAULT_DISTANCE


def __find_distance(id_com_hospitalization, id_hosp, map_hospital_comune, dict_distances):
    if id_com_hospitalization is None:
        return DEFAULT_DISTANCE
    id_com_hosp = map_hospital_comune.get(id_hosp)
    if id_com_hosp is None:
        return DEFAULT_DISTANCE
    if id_com_hosp == id_com_hospitalization:
        return 0
    dis = dict_distances.get(str(id_com_hospitalization), {}).get(id_com_hosp, DEFAULT_DISTANCE)
    return int(dis)

def calc_pat_to_reassign(hosp_day_df_list, hosp_id_list, hosp_spec_id_list):
    """
    Restituisce i ricoveri da riassegnare perché l’ospedale
    o la specialità del ricovero è in lista di chiusura.

    Parameters
    ----------
    hosp_day_df_list : list[pd.DataFrame]
        Lista di DataFrame giornalieri.
    hosp_id_list : list[int]
        ID ospedali da chiudere.
    hosp_spec_id_list : list[int]
        ID specialità da chiudere.

    Returns
    -------
    pd.DataFrame
        DataFrame filtrato dei soli pazienti da riassegnare,
        con l’indice originale (id_ricovero).
    """
    if not hosp_day_df_list:            # lista vuota → ritorna DF vuoto
        return pd.DataFrame()

    # 1) Concatena tutti i giorni in un unico DataFrame
    df_all = pd.concat(hosp_day_df_list, axis=0)

    # 2) Trasforma le liste in set per lookup O(1)
    hosp_id_set = set(map(int, hosp_id_list))
    spec_id_set = set(map(int, hosp_spec_id_list))

    # 3) Applica la maschera booleana in modo vettoriale
    mask = (
        df_all["codice_struttura_erogante"].astype(int).isin(hosp_id_set) |
        df_all["cod_branca_ammissione"].astype(int).isin(spec_id_set)
    )

    # 4) Filtra (senza fare sort) e restituisce copia
    return df_all.loc[mask].copy()


def rest_days(patient_to_reassign):
    return {int(index): row['giorni_degenza'] for index, row in patient_to_reassign.iterrows()}


def list_pat(patient_to_reassign):
    return {None: [int(index) for index in patient_to_reassign.index]}


def build_hospital_set_for_specialty(hosp_lists, spec):
    tmp_hosp = [h.id_hosp for h in hosp_lists if h.id_spec == spec]
    return {None: tmp_hosp}, tmp_hosp


def all_distance(hospitalization_to_reassign, hospital_specialty_closed_list_string, hosp_list, specialty, map_hospital_comune, dict_distances):
        ret = dict()
        for hospitalization in hospitalization_to_reassign.itertuples():
            id_ricovero = hospitalization.Index
            dict_ricovero = dict()
            for hosp_spec in [h_s for h_s in hosp_list if (h_s.id_hosp not in hospital_specialty_closed_list_string and h_s.id_spec == specialty)]:
                dict_ricovero[hosp_spec] = __find_distance(hospitalization.id_comune_paziente, hospitalization.codice_struttura_erogante, map_hospital_comune, dict_distances)
            ret[id_ricovero] = dict_ricovero


def calculate_gamma(l, H, hosp_list, spec, alfa):
    # gamma: (1+alfa)*f*L
    g = {}
    L = sum(l.values())
    tmp_h = H[None]
    f_tot = 0  # capacità per tutti gli ospedali
    for h in tmp_h:
        for i in hosp_list:
            if h == int(i.id_hosp):
                if spec == i.id_spec:
                    f_tot += i.capacity[7]
    if f_tot == 0:
        return {h: 0 for h in tmp_h}  # Puoi restituire 0 o un altro valore

    # Calcola gamma per ogni ospedale
    for h in tmp_h:
        f1 = 0
        for i in hosp_list:
            if h == i.id_hosp and spec == i.id_spec:
                f1 = i.capacity[7]  # prendo la capacità massima
        #f è il rapporto tra capacità massima del singolo ospedale per specialità e capacità massima di tutti gli ospedali per specialità
        f = f1 / f_tot  # Ora non si può più dividere per zero
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


def optimization_reassing(simulation_day_index, upper_threshold_simulation_day_index,
                           hospitalization_day_list_dataframe,
                           closing_hosp_id_list, closing_spec_list,
                           hosp_spec_list_object,
                           dict_mapping_hospital_com, dict_distances_between_com,
                           solver, time_limit, optimizer_model_type):
    logging.info("Inizio ottimizzazione pazienti da riassegnare.")
    start_time_total = time.time()

    new_list_hosp = []
    d = dict()
    m = dict()

    hospitalization_day_to_reassign_dataframe_list = hospitalization_day_list_dataframe[
        simulation_day_index:upper_threshold_simulation_day_index]

    hospitalization_to_reassign_dataframe_compat = calc_pat_to_reassign(
        hospitalization_day_to_reassign_dataframe_list, closing_hosp_id_list, closing_spec_list)

    total_patients = len(hospitalization_to_reassign_dataframe_compat)
    logging.info(f"Giorni selezionati: {len(hospitalization_day_to_reassign_dataframe_list)}, Ricoveri da riassegnare: {total_patients}")

    if not hospitalization_to_reassign_dataframe_compat.empty:

        hospitalization_by_spec_dataframe_list = [
            hospitalization_to_reassign_dataframe_compat[
                hospitalization_to_reassign_dataframe_compat['cod_branca_ammissione'] == valore
            ].copy() for valore in hospitalization_to_reassign_dataframe_compat['cod_branca_ammissione'].unique()
        ]

        logging.info(f"Identificati {len(hospitalization_by_spec_dataframe_list)} gruppi di specialità.")

        for spec_counter, hospitalization_by_spec_dataframe in enumerate(hospitalization_by_spec_dataframe_list, start=1):
            current_spec_id = hospitalization_by_spec_dataframe['cod_branca_ammissione'].iloc[0]
            logging.info(f"[Specialità {current_spec_id}] Ottimizzazione avviata con {len(hospitalization_by_spec_dataframe)} pazienti da riassegnare.")

            l = rest_days(hospitalization_by_spec_dataframe)
            p = list_pat(hospitalization_by_spec_dataframe)
            H, hospital_specialty_closed_list_string = build_hospital_set_for_specialty(hosp_spec_list_object, current_spec_id)

            m = dict(zip(hospitalization_by_spec_dataframe.index.astype(int),
                         hospitalization_by_spec_dataframe['distanza_vecchio_ospedale']))

            d = all_distance(hospitalization_by_spec_dataframe,
                             hospital_specialty_closed_list_string,
                             hosp_spec_list_object,
                             current_spec_id,
                             dict_mapping_hospital_com,
                             dict_distances_between_com)

            alfa = 0.5
            gamma = calculate_gamma(l, H, hosp_spec_list_object, current_spec_id, alfa)
            data = create_data(l, p, H, m, d, gamma)

            logging.info(f"Parametri del modello per specialità {current_spec_id}:\n"
                         f"- l (giorni degenza per paziente): {len(l)} elementi\n"
                         f"- p (insieme pazienti): {len(p[None])} elementi\n"
                         f"- H (insieme ospedali per specialità): {len(H[None])} elementi\n"
                         f"- m (distanze vecchi ospedali): {len(m)} elementi\n"
                         f"- d (distanze possibili ospedali): {len(d)} elementi\n"
                         f"- gamma (capacità ponderata ospedali): {len(gamma)} elementi\n")

            logging.info(f"Costruzione modello per specialità {current_spec_id} completata. Inizio risoluzione...")

            results, model = oms.create_model(data, solver, time_limit, optimizer_model_type)

            infeasible_attempts = 0
            while results.solver.termination_condition == TerminationCondition.infeasible:
                alfa += 0.5
                infeasible_attempts += 1
                logging.warning(f"Problema infeasible per specialità {current_spec_id}. Incremento parametro α={alfa}.")
                gamma = calculate_gamma(l, H, hosp_spec_list_object, current_spec_id, alfa)
                data = create_data(l, p, H, m, d, gamma)
                results, model = oms.create_model(data, solver, time_limit, optimizer_model_type)

            logging.info(f"Modello specialità {current_spec_id} risolto con successo. Tentativi infeasibili: {infeasible_attempts}.")

            for tupla_x in model.x:
                if model.x[(tupla_x[0], tupla_x[1])].value == 1:
                    new_list_hosp.append(tupla_x)

        for t in new_list_hosp:
            for hospitalization_day_to_reassign_dataframe in hospitalization_day_to_reassign_dataframe_list:
                if t[0] in hospitalization_day_to_reassign_dataframe.index:
                    dist_old_hospital = m.get(t[0], 0)
                    dist_new_hospital = d.get((t[0], '0' + str(t[1])), DEFAULT_DISTANCE)

                    hospitalization_day_to_reassign_dataframe.at[t[0], 'codice_struttura_erogante_nuova'] = '0' + str(t[1])
                    hospitalization_day_to_reassign_dataframe.at[t[0], 'distanza_vecchio_ospedale'] = dist_old_hospital
                    hospitalization_day_to_reassign_dataframe.at[t[0], 'distanza_nuovo_ospedale'] = dist_new_hospital
                    hospitalization_day_to_reassign_dataframe.at[t[0], 'discomfort'] = max(0, dist_new_hospital - dist_old_hospital)

    total_time = time.time() - start_time_total
    logging.info(f"Ottimizzazione completata. Tempo totale impiegato: {total_time:.2f} secondi.")
    return hospitalization_day_to_reassign_dataframe_list, total_time