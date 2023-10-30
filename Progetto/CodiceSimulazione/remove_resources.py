import datetime
import pandas as pd
import utility_functions as uf

def read_input(file_txt):
    """
    Legge da file i codici ospedale, le coppie codici ospedale-specialità e data da rimuovere dalla
    simulazione.
    Importante, nel file ci devono essere sempre 3 righe, se non ne serve una lasciarla vuota.
    Args:
        file_txt ([file txt]): [prima riga codici ospedali separati da spazio; seconda riga coppie codici
                                ospedale,specialità e ogni coppia separata da spazio; terza riga data in formato
                                aaaa/mm/gg]

    Returns:
        [list]: [lista dei codici ospedali]
        [list di list]: [lista delle liste dei codici ospedali,codici specialità]
        [list]: [data da quando levare le risorse]
    """
    with open(file_txt) as f:
        content = f.readlines()
    # rimuove il \n alla fine di ogni riga
    content = [x.strip() for x in content]
    hosp_id_list = []
    hosp_spec_list = []
    date = []
    if content[0] != '':
        hosp_id_list = [int(x) for x in content[0].split(" ")]
    if content[1] != '':
        hosp_spec_list = [[int(x[0]),int(x[1])] for x in (x.split(",") for x in content[1].split(" "))]
    if content[2] != '':
        date = [datetime.datetime.strptime(content[2], '%Y-%m-%d').date()]

    return hosp_id_list, hosp_spec_list, date

def remove_resources(hosp_list, resources_to_remove):
    """Ho la lista degli ospedali e devo rimuovere quelli nel file

    Args:
        hosp_list (dizionario): dizionario degli ospedali

    Returns:
        dizionario: dizionario degli ospedali con gli ospedali rimossi
    """
    remove_hosp_id_list = resources_to_remove[0]
    remove_hosp_spec_list = resources_to_remove[1]
    if remove_hosp_id_list != '':
        hosp_list = [h for h in hosp_list if int(h.id_hosp) not in remove_hosp_id_list]
    if remove_hosp_spec_list != '':
        hosp_list = [h for h in hosp_list if [int(h.id_hosp), int(h.id_spec)] not in remove_hosp_spec_list]

    return hosp_list

#----------------------------------------------------------------------------------------------------#

def __nearest_hospital(patient_id_hosp, patient_id_spec, remove_hosp_id_list, hosp_list, lista_comuni_mancanti,
                       policy_resources):
    df_distance_hosp = policy_resources[1]
    map_hosp_com = policy_resources[0]

    # Lista degli ospedali con la stessa specialità del paziente
    tmp_hosp_list = uf.same_hospital(patient_id_hosp, patient_id_spec, hosp_list)
    if tmp_hosp_list == []:
        raise Exception('Non ci sono ospedali con la stessa specialità di quella passata')
    # Cerco l'ospedale più vicino
    new_patient_id_hosp = uf.search_nearest_comune(df_distance_hosp, map_hosp_com, patient_id_hosp,
                                                   tmp_hosp_list, hosp_list, lista_comuni_mancanti)
    return new_patient_id_hosp


def __nearest_hospital_comune_patient(patient_id_hosp, patient_id_spec, policy_resources, patient_record_id,
                                      hosp_list, lista_comuni_mancanti):

    map_hosp_com = policy_resources[0]
    df_distance_hosp = policy_resources[1]
    map_dict = policy_resources[3]

    try:
        id_comune_patient = map_dict[patient_record_id][1]
    except:
        id_comune_patient = map_hosp_com[int(patient_id_hosp)]

    tmp_hosp_list = uf.same_hospital(patient_id_hosp, patient_id_spec, hosp_list)
    if tmp_hosp_list == []:
        raise Exception('Non ci sono ospedali con la stessa specialità di quella passata')

    new_patient_id_hosp = uf.search_nearest_comune_residenza(df_distance_hosp, map_hosp_com, patient_id_hosp,
                                                             tmp_hosp_list, hosp_list, lista_comuni_mancanti,
                                                             id_comune_patient)
    return new_patient_id_hosp


def __apply_policy(patient_id_hosp, patient_id_spec, remove_hosp_id_list, hosp_list, lista_comuni_mancanti,
                   policy_resources, patient_record_id):
    """Funzione switch tra le future policy di scelta del nuovo ospedale"""
    new_patient_id_hosp = __nearest_hospital(patient_id_hosp, patient_id_spec, remove_hosp_id_list, hosp_list,
                                             lista_comuni_mancanti, policy_resources)
    #new_patient_id_hosp = __nearest_hospital_comune_patient(patient_id_hosp, patient_id_spec, policy_resources,
    #                                                         patient_record_id, hosp_list, lista_comuni_mancanti)
    return new_patient_id_hosp


def removed_id_check(resources_to_remove, patient_id_hosp, patient_id_spec, hosp_list, lista_comuni_mancanti, 
                     policy_resources, patient_record_id):
    remove_hosp_id_list = resources_to_remove[0]
    remove_hosp_spec_list = resources_to_remove[1]

    if (int(patient_id_hosp) in remove_hosp_id_list) or ([int(patient_id_hosp), int(patient_id_spec)] in remove_hosp_spec_list):
        patient_id_hosp = __apply_policy(patient_id_hosp, patient_id_spec, remove_hosp_id_list, hosp_list,
                                         lista_comuni_mancanti, policy_resources, patient_record_id)

    return patient_id_hosp


if __name__ == '__main__':
    file = "../Parametri/remove_info.txt"
    #c = read_input(file)
    id_hosp = '01007901'
    id_spec = 8
    __nearest_hospital(id_hosp, id_spec)