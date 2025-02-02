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
    hosp_series = risorse['codice_struttura_erogante'].drop_duplicates()
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
                if val == row['codice_struttura_erogante']:
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
    #codice struttura erogante punta ad anno il quale punta a codici_specialita il quale punta alle diverse info
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
    return str(id_spec)

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
        import pandas as pd

        sdo_dataframe_columns = [
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

        sdo_dataframe_dtype_columns = {
            'anno': 'int64',  # Anno: intero
            'mese': 'int64',  # Mese: intero
            'giorno': 'int64',  # Giorno: intero
            'id_ricovero': 'string',  # ID Ricovero: stringa
            'data_dimissione': 'string',
            'data_ricovero': 'string',
            'giorni_degenza': 'int64',  # Giorni di Degenza: intero
            'asl_territoriale': 'string',  # ASL Territoriale: stringa
            'asl_erogante': 'string',  # ASL Erogante: stringa
            'codice_struttura_erogante': 'string',  # Codice Struttura Erogante: stringa
            'cod_branca_ammissione': 'string',  # Codice Branca Ammissione: stringa
            'cod_branca_dimissione': 'string'  # Codice Branca Dimissione: stringa
        }

        sdo_dataframe = pd.read_csv(
            sdo_path,
            usecols=[0, 1, 2, 3, 6, 10, 15, 25, 26, 27, 35, 37],
            header=0,
            names=sdo_dataframe_columns,
            dtype=sdo_dataframe_dtype_columns,
        )

        sdo_dataframe['match_cod_branca'] = sdo_dataframe['cod_branca_ammissione'] == sdo_dataframe['cod_branca_dimissione']
        dtype_anagrafica_columns= {
            'codice_struttura_erogante': 'string',
            'nome_comune_residenza': 'string'
        }
        anagrafica_dataframe = pd.read_csv(anagrafica_path, usecols=[0, 3], names=['codice_struttura_erogante','nome_comune_residenza'], dtype=dtype_anagrafica_columns )
        sdo_dataframe.set_index(sdo_dataframe.columns[3], inplace=True)
        anagrafica_dataframe.set_index(anagrafica_dataframe.columns[0], inplace=True)
        hospitalizations = pd.concat([hospitalizations, sdo_dataframe.merge(
            anagrafica_dataframe,
            left_index=True,
            right_index=True
        )], axis=0)

    hospitalizations['codice_struttura_erogante'] = hospitalizations['codice_struttura_erogante'].apply(__hospitalizations_codice_struttura_erogante_parser)
    hospitalizations["data_ricovero"] = pd.to_datetime(hospitalizations["data_ricovero"])
    hospitalizations.sort_values('data_ricovero', inplace=True)

    risorse_dtype = {
        'codice_struttura_erogante': 'string',  # Stringa
        'codici_specialita': 'string',  # Stringa
        'MONDAY': 'int64',  # Intero
        'TUESDAY': 'int64',  # Intero
        'WEDNESDAY': 'int64',  # Intero
        'THURSDAY': 'int64',  # Intero
        'FRIDAY': 'int64',  # Intero
        'SATURDAY': 'int64',  # Intero
        'SUNDAY': 'int64',  # Intero
        'capacita_max': 'int64',  # Intero
        'year': 'int64'  # Intero
    }

    # Lettura del CSV con dtypes specificati
    risorse = pd.read_csv(
        "../RawData/specialtyCapacitySchedules.csv",
        usecols=['codice_struttura_erogante', 'codici_specialita',
                 'MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY',
                 'FRIDAY', 'SATURDAY', 'SUNDAY',
                 'capacita_max', 'year'],
        dtype=risorse_dtype
    )
    risorse['codice_struttura_erogante'] = risorse['codice_struttura_erogante'].apply(__risorse_codice_ospedale_parser)
    risorse['codici_specialita'] = risorse['codici_specialita'].apply(__risorse_idspec_parser)

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
    try:
        risorse = joblib.load('../DatiElaborati/risorse_simulazione')
        ricoveri = joblib.load('../DatiElaborati/ricoveri_simulazione')
    except:
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
        dict_mapping = parser_distanze.dict_mapping()

    try:
        dict_distances = joblib.load('../DatiElaborati/distanzeComuniOspedali')
    except Exception:
        dict_distances = parser_distanze.dict_comuni_hosp()

    return dict_mapping, dict_distances


def load_residenze(dataframe_hospitalizations):
    final_dict = dict()
    """
    Carica il dizionario con chiave id del record del paziente valore (nome comune di residenza del paziente, id del comune di residenza del paziente)
    """
    # Creo il dizionario di mapping tra id del record del paziente e l'id del comune di residenza.
    # Avrò {id_r:id_residenza}, avrò bisogno del file di mapping tra nome del comune e suo id
    dataframe_hospitalizations['nome_comune_residenza'] = dataframe_hospitalizations['nome_comune_residenza'].str.lower()
    dataframe_hospitalizations['nome_comune_residenza'] = dataframe_hospitalizations['nome_comune_residenza'].str.replace(r"\s+", "", regex=True)
    comuni_ita = pd.read_csv('../RawData/Elenco-comuni-italiani.csv', usecols=[4, 5])
    comuni_ita.columns = ['id_comune_paziente', 'nome_comune_residenza']
    comuni_ita['nome_comune_residenza'] = comuni_ita['nome_comune_residenza'].str.lower()
    comuni_ita.dropna(subset=['nome_comune_residenza'])
    comuni_ita['nome_comune_residenza'] = comuni_ita['nome_comune_residenza'].str.replace(r"\s+", "", regex=True)
    mapping_dict = comuni_ita.set_index("nome_comune_residenza")["id_comune_paziente"].astype(str).to_dict()
    dataframe_hospitalizations["id_comune_paziente"] = dataframe_hospitalizations["nome_comune_residenza"].map(mapping_dict).astype(str)
    final_dict = dict([(i, [a, b]) for i, a, b in zip(dataframe_hospitalizations.index, dataframe_hospitalizations.nome_comune_residenza, dataframe_hospitalizations.id_comune_paziente)])
    joblib.dump(final_dict, '../DatiElaborati/mapping_idHospitalizations_idComuneResidenzaNomeComuneResidenza')
    return final_dict


if __name__ == '__main__':
    # risorse, ricoveri = load_data()
    # load_hosp_dict(risorse)
    load_policy_data()
