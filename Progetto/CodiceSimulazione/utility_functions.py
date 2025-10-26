"""
Serie di funzioni con scopi diversi
"""
import calendar
import copy
import objects_classes as oc


def get_hospitalization_hospital(hosp_object_list, hospitalization_record_id_hosp, hospitalization_record_id_spec):
    """
    Trovo l'ospedale del paziente.

    Parameters
    ----------
    hosp_object_list : List di Oggetti Hospital
        Lista degli ospedali.
    hospitalization_record_id_hosp : String
        Id dell'ospedale nel quale il paziente è andato.
    hospitalization_record_id_spec : String
        Id della specialità nella quale il paziente è andato.

    Returns
    -------
    h : Oggetto Hospital
        Ospedale della lista nel quale il paziente è andato.

    """
    try:
        target_hospital = None
        for h in hosp_object_list:
            if int(h.id_hosp) == int(hospitalization_record_id_hosp):
                if int(h.id_spec) == int(hospitalization_record_id_spec):
                    target_hospital = h
        if target_hospital is None:
            for h in hosp_object_list:
                if int(h.id_spec) == int(hospitalization_record_id_spec):
                    target_hospital = h
    except TypeError:
        print("Invalid integer value",hospitalization_record_id_spec, h.id_spec)
    if target_hospital is None:
        print("!|^!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    return target_hospital


def number_of_the_day(date_pat):
    """
    Trova il numero della settimana.

    Ricava il numero del giorno della settimana in base alla data di ricovero
    del paziente (lunedì=0, martedì=1, ecc...).

    Parameters
    ----------
    patient : Riga del dataframe
        Informazioni del paziente presenti nel dataframe.

    Returns
    -------
    Int
        Il numero del giorno della settimana in cui è stato accettato il
        paziente.

    """
    year, month, day = (int(i) for i in date_pat.split("-"))
    day = calendar.weekday(year, month, day)
    return int(day)


def create_hospital_specialty_list_from_year(hosp_dict, year):
    """
    Creo una nuova lista di oggetti ospedale.

    Creo una lista di oggetti ospedale partendo dal dizionario, la creo in
    base all'anno passato di riferimento.

    Parameters
    ----------
    hosp_dict : Dizionario python
        Dizionario creato dai file csv contenente le info degli ospedali.
    year : Int
        Anno per cui mi interessa creare una lista di ospedali.

    Returns
    -------
    new_hosp_list : List di Oggetti Hospital
        Lista dei nuovi oggetti ospedali con le info del dizionario.

    """
    new_hosp_list = []
    for h in hosp_dict:
        for s in hosp_dict[h][year]:
            new_hosp_list.append(oc.HospitalSpeciality(h, s, hosp_dict[h][year][s]))

    return new_hosp_list


def update_hospital_capacity(old_hosp_list, new_hosp_list):
    """
    Aggiorno le code dei nuovi ospedali.

    Aggiorno le code di attesa della nuova lista di ospedali con le code della
    vecchia lista di ospedali.

    Parameters
    ----------
    old_hosp_list : List di Oggetti Hospital
        Lista degli ospedali con le info dell'anno precedente.
    new_hosp_list : List di Oggetti Hospital
        Nuova lista di ospedali con le info dell'anno nuovo.

    Returns
    -------
    new_hosp_list : List di Oggetti Hospital
        Nuova lista di ospedali con le info dell'anno nuovo e con le code di
        attesa copiate dall'anno precedente.

    """
    for old_h in old_hosp_list:
        for new_h in new_hosp_list:
            if int(old_h.id_hosp) == int(new_h.id_hosp):
                if int(old_h.id_spec) == int(new_h.id_spec):
                    new_h.waiting_queue = copy.deepcopy(old_h.waiting_queue)
                    new_h.rest_queue = copy.deepcopy(old_h.rest_queue)

    return new_hosp_list


def count_total_patient(patient_day_list):
    """
    Conta il numero di pazienti.

    Parameters
    ----------
    patient_day_list : List di Dataframe
        Lista di dataframe dei pazienti divisi per data.

    Returns
    -------
    tot : Int
        Numero totale di pazienti da analizzare.

    """
    tot = 0
    for df in patient_day_list:
        index = df.index
        tot = tot + len(index)

    return tot


def __parse_cell(c):
    cell_list = []
    if c != None:
        ids = c.split('-')
        for i in ids:
            if i != '':
                cell_list.append(i.split('_')[1])
    return cell_list


def search_comune_ospedale(df_comuneSpecialita, id_hosp):
    """Cerco e restituisco il codice comune dell'ospedale passato"""
    for row in df_comuneSpecialita.iterrows():
        for c in row[1][2:]:
            if c != None:
                tmp_list = __parse_cell(str(c))
                for i in tmp_list:
                    if i == id_hosp:
                        return row[1][1]
    
    return 0


def same_hospital(patient_id_hosp, patient_id_spec, hosp_list):
    #Creo la lista degli ospedali con la stessa specialità di quello passato
    new_hosp_list = []
    for h in hosp_list:
        if int(h.id_hosp) != int(patient_id_hosp):
            if int(h.id_spec) == int(patient_id_spec):
                new_hosp_list.append(int(h.id_hosp))

    return new_hosp_list


def __convert_to_comuni(map_hosp_com, patient_id_hosp, tmp_hosp_list, hosp_list):
    # Funzione per il dizionario
    try:
        patient_comune = map_hosp_com[int(patient_id_hosp)]
    except:
        e = 'Comune paziente sbagliato: id osp paziente '+str(patient_id_hosp)
        raise Exception(e)
    comuni_list = []
    for h in tmp_hosp_list:
        try:

            comuni_list.append(int(map_hosp_com[h]))
        
        except:
            l = []
            for i in hosp_list:
                l.append(i.id_hosp)
            print(h)
            print(type(h))
            e = 'Comune sbagliato: '+str(patient_comune) + ', ' + str(h)+', '+ str(len(l))
            raise Exception(e)
        
    return patient_comune, comuni_list


def search_nearest_comune(df_distance_hosp, map_hosp_com, patient_id_hosp, tmp_hosp_list, hosp_list, lista_comuni_mancanti):
    # Funzione per il dizionario
    # Trasformo i codici ospedale nei corrispettivi codici comune
    patient_comune, comuni_list = __convert_to_comuni(map_hosp_com, patient_id_hosp, tmp_hosp_list, hosp_list)

    # Cerco il comune con la distanza minore
    d = df_distance_hosp[patient_comune]
    tmp_d = { your_key: d[your_key] for your_key in comuni_list }
    try:
        minDis = min(tmp_d, key=d.get)
    except:
        minDis = comuni_list[0]
        lista_comuni_mancanti.append([patient_id_hosp, patient_comune])
    # Riconverto il comune con il suo codice ospedale (non posso farlo in altra maniera perchè bisogna tenere
    # una relazione unica tra ospedale e comune, capita che un comune ha più di un ospedale)
    tmp_list = zip(tmp_hosp_list,comuni_list)
    new_patient_hosp=''
    for c in tmp_list:
        if c[1] == minDis:   
            new_patient_hosp = c[0]
    if not (str(new_patient_hosp).startswith('0')):
        new_patient_hosp = str(new_patient_hosp)
    return new_patient_hosp


def search_nearest_comune_residenza(df_distance_hosp, map_hosp_com, patient_id_hosp, tmp_hosp_list, hosp_list,
                                    lista_comuni_mancanti, id_comune_patient):
    # Funzione per il dizionario
    # Trasformo i codici ospedale nei corrispettivi codici comune
    hosp_patient_comune, comuni_list = __convert_to_comuni(map_hosp_com, patient_id_hosp, tmp_hosp_list, hosp_list)
    patient_comune = id_comune_patient

    # Cerco il comune con la distanza minore
    try:
        d = df_distance_hosp[str(patient_comune)]
    except:
        d = df_distance_hosp[hosp_patient_comune]
    
    tmp_d = { your_key: d[your_key] for your_key in comuni_list }
    try:
        minDis = min(tmp_d, key=d.get)
    except:
        minDis = comuni_list[0]
        lista_comuni_mancanti.append([patient_id_hosp, patient_comune])
    # Riconverto il comune con il suo codice ospedale (non posso farlo in altra maniera perchè bisogna tenere
    # una relazione unica tra ospedale e comune, capita che un comune ha più di un ospedale)
    tmp_list = zip(tmp_hosp_list,comuni_list)
    new_patient_hosp=''
    for c in tmp_list:
        if c[1] == minDis:   
            new_patient_hosp = c[0]
    if not (str(new_patient_hosp).startswith('0')):
        new_patient_hosp = str(new_patient_hosp)
    return new_patient_hosp


def random_hospital(hosp_list, patient_id_hosp, patient_id_spec):
    for h in hosp_list:
        if int(h.id_spec) == int(patient_id_spec):
            return h