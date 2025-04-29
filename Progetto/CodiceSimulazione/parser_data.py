"""
Created on Tue Dec 22 20:04:20 2020.

@author: stevi
"""
import os

import joblib
import pandas as pd
import parser_distanze


def cleanNullTerms(d):
    """Leva i valori nulli da un dizionario, anche innestati."""
    clean = {}
    for k, v in d.items():
        if isinstance(v, dict):
            nested = cleanNullTerms(v)
            if len(nested.keys()) > 0:
                clean[k] = nested
        elif v is not None:
            clean[k] = v
    return clean


def unpack_spec(spec_dict):
    """Ottengo una lista da un dizionario."""
    spec_list = []
    for s in spec_dict:
        spec_list.append([s, int(spec_dict[s])])
    return spec_list


def initialize_hospitals(risorse):
    """
    Restituisce un dizionario:
      { '01000300': { 2011: { '09': [...], '34': [...], … }, 2012: {…}, … },
        '01000802': { … }, … }
    (i prefissi 'res_' e 'spec_' sono automaticamente rimossi)
    """
    # estrai serie ospedali/anni
    hosp_series = risorse['codice_struttura_erogante'].drop_duplicates()
    year_series = risorse['year'].drop_duplicates()

    final_dict = {}
    for raw_hosp_code in hosp_series:
        # 1) rimuovi eventuale prefisso "res_"
        if raw_hosp_code.startswith("res_"):
            hosp_code = raw_hosp_code[len("res_"):]
        else:
            hosp_code = raw_hosp_code

        per_year = {}
        for y in year_series:
            spec_map = {}
            # per ogni riga che corrisponde a (hosp, year)
            subset = risorse[
                (risorse['codice_struttura_erogante'] == raw_hosp_code) &
                (risorse['year'] == y)
            ]
            for _, row in subset.iterrows():
                raw_spec = row['codici_specialita']
                # 2) rimuovi eventuale prefisso "spec_"
                spec_key = raw_spec[len("spec_"):] if raw_spec.startswith("spec_") else raw_spec

                spec_map[spec_key] = [
                    row['MONDAY'], row['TUESDAY'], row['WEDNESDAY'],
                    row['THURSDAY'], row['FRIDAY'], row['SATURDAY'],
                    row['SUNDAY'], row['capacita_max']
                ]

            per_year[y] = spec_map

        final_dict[hosp_code] = per_year

    return final_dict

def __risorse_idspec_parser(s):
    tmp_id_spec = str(s).split("_")[1]
    if tmp_id_spec.startswith("0"):
        id_spec = tmp_id_spec.split("0")[1]
    else:
        id_spec = tmp_id_spec
    return str(id_spec)

def __risorse_codice_ospedale_parser(code):
    return str(code).split("_")[1]

def __hospitalizations_codice_struttura_erogante_parser(code):
    h, t = str(code).split("-")
    return h + t

def load_data():
    """
    Carica i dati ottimizzando la velocità e l'utilizzo della memoria.

    Returns
    -------
    risorse : DataFrame
        Capacità delle specialità ospedaliere.
    ricoveri : DataFrame
        Informazioni dettagliate sui pazienti ricoverati.
    """

    risorse_file = '../DatiElaborati/risorse_simulazione'
    ricoveri_file = '../DatiElaborati/ricoveri_simulazione'

    if os.path.exists(risorse_file) and os.path.exists(ricoveri_file):
        risorse = joblib.load(risorse_file)
        ricoveri = joblib.load(ricoveri_file)
    else:
        risorse, ricoveri = _create_dataframes(risorse_file, ricoveri_file)

    return risorse, ricoveri


def _create_dataframes(risorse_file, ricoveri_file):
    sdo_list = [
        "../RawData/03-01-2011-01-01-2012/sdo.csv",
        "../RawData/02-01-2012-30-12-2012/sdo.csv",
        "../RawData/31-12-2012-29-12-2013/sdo.csv"
    ]

    anagrafica_list = [
        "../RawData/03-01-2011-01-01-2012/anagrafica.csv",
        "../RawData/02-01-2012-30-12-2012/anagrafica.csv",
        "../RawData/31-12-2012-29-12-2013/anagrafica.csv"
    ]

    sdo_usecols = [0, 1, 2, 3, 6, 10, 15, 25, 26, 27, 35, 37]
    sdo_names = [
        'anno', 'mese', 'giorno', 'id_ricovero', 'data_dimissione', 'data_ricovero',
        'giorni_degenza', 'asl_territoriale', 'asl_erogante', 'codice_struttura_erogante',
        'cod_branca_ammissione', 'cod_branca_dimissione'
    ]

    dtype_sdo = {
        'anno': 'int64', 'mese': 'int64', 'giorno': 'int64',
        'id_ricovero': 'string', 'data_dimissione': 'string',
        'data_ricovero': 'string', 'giorni_degenza': 'int64',
        'asl_territoriale': 'string', 'asl_erogante': 'string',
        'codice_struttura_erogante': 'string', 'cod_branca_ammissione': 'string',
        'cod_branca_dimissione': 'string'
    }

    hospitalizations_list = []

    for sdo_path, anagrafica_path in zip(sdo_list, anagrafica_list):
        sdo_df = pd.read_csv(
            sdo_path,
            usecols=sdo_usecols,
            names=sdo_names,
            dtype=dtype_sdo,
            header=0
        )

        sdo_df['match_cod_branca'] = (sdo_df['cod_branca_ammissione'] == sdo_df['cod_branca_dimissione'])

        anagrafica_df = pd.read_csv(
            anagrafica_path,
            usecols=[0, 3],
            names=['id_ricovero', 'nome_comune_residenza'],
            dtype={'id_ricovero': 'string', 'nome_comune_residenza': 'string'}
        )

        merged_df = pd.merge(
            sdo_df, anagrafica_df[['id_ricovero', 'nome_comune_residenza']],
            on='id_ricovero', how='left'
        )

        hospitalizations_list.append(merged_df)

    hospitalizations = pd.concat(hospitalizations_list, ignore_index=True)

    hospitalizations['codice_struttura_erogante'] = hospitalizations['codice_struttura_erogante'].str.strip()
    hospitalizations['codice_struttura_erogante'] = hospitalizations['codice_struttura_erogante'].str.replace('-', '')
    hospitalizations['data_ricovero'] = pd.to_datetime(hospitalizations['data_ricovero'], errors='coerce')
    hospitalizations.set_index('id_ricovero', inplace=True)

    risorse = pd.read_csv(
        "../RawData/specialtyCapacitySchedules.csv",
        usecols=['codice_struttura_erogante', 'codici_specialita',
                 'MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY',
                 'FRIDAY', 'SATURDAY', 'SUNDAY', 'capacita_max', 'year'],
        dtype={'codice_struttura_erogante': 'string', 'codici_specialita': 'string',
               'MONDAY': 'int64', 'TUESDAY': 'int64', 'WEDNESDAY': 'int64',
               'THURSDAY': 'int64', 'FRIDAY': 'int64', 'SATURDAY': 'int64',
               'SUNDAY': 'int64', 'capacita_max': 'int64', 'year': 'int64'}
    )

    risorse['codice_struttura_erogante'] = risorse['codice_struttura_erogante'].str.strip()
    risorse['codici_specialita'] = risorse['codici_specialita'].str.strip()

    joblib.dump(risorse, risorse_file)
    joblib.dump(hospitalizations, ricoveri_file)

    return risorse, hospitalizations



def load_hosp_dict(risorse):
    """
    Prova a caricare il dizionario degli ospedali, se non esiste lo crea.

    Parameters
    ----------
    risorse : Pandas Dataframe
        File caricato in precedenza attraverso load_data().

    Returns
    -------
    hosp_dict : Python Dictionary
        Dizionario degli ospedali con anno, specialità e capienze.

    """
    try:
        hosp_dict = joblib.load('../DatiElaborati/hosp_dict_resources')
    except Exception:
        hosp_dict = initialize_hospitals(risorse)

    return hosp_dict


def load_policy_data():
    """
    Carica il dizionario del mapping tra ospedale e comune ed il dizionario delle distanze tra comuni.
    Se non esistono li crea.
    """
    try:
        dict_mapping = joblib.load('../DatiElaborati/mappingOspCom')
    except Exception:
        dict_mapping = parser_distanze.dict_mapping()

    try:
        dict_distances = joblib.load('../DatiElaborati/distanzeComuniOspedali')
    except Exception:
        dict_distances = parser_distanze.dict_comuni_hosp()

    return dict_mapping, dict_distances


def load_residenze(hospitalizations):
    # Carica l'elenco ufficiale dei comuni italiani
    comuni_ita = pd.read_csv('../RawData/Elenco-comuni-italiani.csv', usecols=[4, 5])
    comuni_ita.columns = ['id_comune', 'nome_comune_residenza']

    comuni_ita['id_comune'] = comuni_ita['id_comune'].astype(str)
    # Pulizia: tolgo spazi bianchi e metto minuscolo per confrontare
    comuni_ita['nome_comune_residenza'] = comuni_ita['nome_comune_residenza'].str.replace(r"\\s+", "", regex=True).str.lower()
    hospitalizations['nome_comune_residenza'] = hospitalizations['nome_comune_residenza'].str.replace(r"\\s+", "", regex=True).str.lower()

    # Creo dizionario di mapping
    dict_comuni = comuni_ita.set_index('nome_comune_residenza')['id_comune'].to_dict()

    # Applico il mapping
    hospitalizations['id_comune_paziente'] = hospitalizations['nome_comune_residenza'].map(dict_comuni)

    missing = hospitalizations['id_comune_paziente'].isnull().sum()
    if missing > 0:
        print(f"Attenzione: {missing} ricoveri con comune non trovato. Verrà usato id_comune_struttura_erogante come valore per id_comune_paziente.")

    # Dove id_comune_paziente è mancante, metto id_comune_struttura_erogante
    hospitalizations['id_comune_paziente'] = hospitalizations['id_comune_paziente'].fillna(hospitalizations['id_comune_struttura_erogante'])

    return hospitalizations
