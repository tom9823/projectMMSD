"""
Created on Tue Dec 22 20:04:20 2020.

@author: stevi
"""
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
    Creo un dizionario di dizionari di ospedali.

    Chiave principale "id ospedale", seconda chiave "anno" con valore un
    dizionario con: chiave codice specialità, valore una lista delle capacità
    [capacità giornaliera (7 valori), capacità massima]).

    Il dizionario è:{'id ospedale':'anno':{'codice specialità':
    ['MONDAY','TUESDAY','WEDNESDAY','THURSDAY','FRIDAY','SATURDAY','SUNDAY',
     'capacita_max']}}
    """
    # Ottengo la lista dei codici ospedali senza duplicati
    hosp_series = risorse['codici_ospedale'].drop_duplicates()
    # Ottengo la lista degli anni senza duplicati
    year_series = risorse['year'].drop_duplicates()
    filename = '../DatiElaborati/hosp_series'
    joblib.dump(hosp_series, filename)
    print('Creazione file', filename)
    filename = '../DatiElaborati/year_series'
    joblib.dump(year_series, filename)
    print('Creazione file', filename)
    final_dict = {}
    print('Inizio creazione dizionario ospedali')
    for index, val in hosp_series.items():
        tmp_dict = {}
        for y in year_series:
            y_dict = {}
            for index, row in risorse.iterrows():
                if val == row['codici_ospedale']:
                    if y == row['year']:
                        y_dict[row['codici_specialita']] = [row['MONDAY'],
                                                            row['TUESDAY'],
                                                            row['WEDNESDAY'],
                                                            row['THURSDAY'],
                                                            row['FRIDAY'],
                                                            row['SATURDAY'],
                                                            row['SUNDAY'],
                                                            row['capacita_max']]
            tmp_dict[y] = y_dict
        final_dict[val] = tmp_dict

    print(f'Lunghezza indice: {index}')
    print('Fine creazione dizionario ospedali')
    filename = '../DatiElaborati/hosp_dict_resources'
    joblib.dump(final_dict, filename)
    print('Creato file', filename)
    return final_dict


def __risorse_idspec_parser(s):
    tmp_id_spec = str(s).split("_")[1]
    if tmp_id_spec.startswith("0"):
        id_spec = tmp_id_spec.split("0")[1]
    else:
        id_spec = tmp_id_spec
    return int(id_spec)

def __risorse_codice_ospedale_parser(code):
    return str(code).split("_")[1]

def __hospitalizations_codice_struttura_erogante_parser(code):
    h, t = str(code).split("-")
    return h + t

def __load():
    '''
    Crea i dataframe ricoveri e risorse, salvandoli in determinati file
    Returns
    -------
    '''
    file_sdo_2011 = "../RawData/03-01-2011-01-01-2012/sdo.csv"
    file_anagrafica_2011 = "../RawData/03-01-2011-01-01-2012/anagrafica.csv"
    file_sdo_2012 = "../RawData/02-01-2012-30-12-2012/sdo.csv"
    file_anagrafica_2012 = "../RawData/02-01-2012-30-12-2012/anagrafica.csv"
    file_sdo_2013 = "../RawData/31-12-2012-29-12-2013/sdo.csv"
    file_anagrafica_2013 = "../RawData/31-12-2012-29-12-2013/anagrafica.csv"
    sdo_list = [file_sdo_2011, file_sdo_2012, file_sdo_2013]
    anagrafica_list = [file_anagrafica_2011, file_anagrafica_2012, file_anagrafica_2013]
    hospitalizations = None
    for i in range(len(sdo_list)):
        sdo_path = sdo_list[i]
        anagrafica_path = anagrafica_list[i]
        sdo_dataframe = pd.read_csv(sdo_path, usecols=[0, 1, 2, 3, 6, 10, 15, 25, 26, 27, 35, 37], header=0)
        sdo_dataframe.columns = [
            'anno',
            'mese',
            'giorno',
            'id_ricovero',
            'data_dimissione',
            'data_ricovero',
            'giorni_degenza',
            'asl_territoriale',
            'asl_erogante',
            'codice_struttura_erogante',
            'cod_branca_ammissione',
            'cod_branca_dimissione'
        ]
        sdo_dataframe['match_cod_branca'] = sdo_dataframe['cod_branca_ammissione'] == sdo_dataframe['cod_branca_dimissione']
        anagrafica_dataframe = pd.read_csv(anagrafica_path)
        sdo_dataframe.set_index(sdo_dataframe.columns[3], inplace=True)
        anagrafica_dataframe.set_index(anagrafica_dataframe.columns[0], inplace=True)
        hospitalizations = pd.concat([hospitalizations, sdo_dataframe.merge(
            anagrafica_dataframe,
            left_index=True,
            right_index=True
        )], axis=0)

    hospitalizations["data_ricovero"] = pd.to_datetime(hospitalizations["data_ricovero"])
    hospitalizations.sort_values('data_ricovero', inplace=True)

    hospitalizations['codice_struttura_erogante'].apply(__hospitalizations_codice_struttura_erogante_parser, inplace=True)

    risorse = pd.read_csv("../RawData/specialtyCapacitySchedules.csv",
                          usecols=['codice_struttura_erogante', 'codici_specialita',
                                   'MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY',
                                   'FRIDAY', 'SATURDAY', 'SUNDAY',
                                   'capacita_max', 'year'])
    risorse['codice_struttura_erogante'].apply(__risorse_codice_ospedale_parser,  inplace=True)
    risorse['codici_specialita'].apply(__risorse_idspec_parser,  inplace=True)

    filename = '../DatiElaborati/risorse_simulazione'
    joblib.dump(risorse, filename)
    filename = '../DatiElaborati/ricoveri_simulazione'
    joblib.dump(hospitalizations, filename)
    return risorse, hospitalizations


def load_data():
    """
    Controlla se i dati 'risorse_simulazione' e 'ricoveri_simulazione' esistono.

    Se non esistono li crea parsificandoli rendendo i codici ospedali e codici
    specialità tutti uguali.

    Returns
    -------
    risorse : Pandas Dataframe
        Dataframe del file 'specialtyCapacitySchedules.csv', contiene le
        capacità delle specialità.
    ricoveri : Pandas Dataframe
        Dataframe del file 'ricoveri.csv', contiene tutte le informazioni dei
        pazienti.

    """
    risorse, ricoveri = __load()

    return risorse, ricoveri


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
        path = '../DatiOriginali/mapping_hosp_comuni.csv'
        name = 'mappingOspCom'
        dict_mapping = parser_distanze.dict_mapping()

    try:
        dict_distances = joblib.load('../DatiElaborati/distanzeComuniOspedali')
    except Exception:
        dict_distances = parser_distanze.dict_comuni_hosp()

    return dict_mapping, dict_distances


def load_residenze():
    """
    Carica il dizionario di: id del record del paziente, nome comune di residenza del paziente ed
    id del comune di residenza
    """
    try:
        dict_res = joblib.load('../Dati_Elaborati/map_pat_idComRes')
    except:
        dict_res = parser_distanze.dict_map_id_ricovero_nome_id_comune()
    return dict_res


if __name__ == '__main__':
    # risorse, ricoveri = load_data()
    # load_hosp_dict(risorse)
    load_policy_data()
