import os
import re

import pandas as pd
import joblib

def originDistanceMetri_parser(file_name, path): 
    data = pd.read_excel(r'' + path + '/general/'+ file_name + '.xlsx')
    df = pd.DataFrame(data)
    df = df.rename(columns=lambda s: s.split('_')[1])

    # Salvo il file di distanze tra tutti i comuni e tra i comuni con ospedali
    df.index.name = 'Index'
    df.to_csv(r'' + path + '/general/distanzeComuniOspedali' + '.csv')

    # Salvo il file delle distanze tra comuni che hanno ospedali.

    # Riordino i codici delle colonne
    columns_name = df.columns.tolist()[1:]
    columns_name = [int(x) for x in columns_name]
    columns_name.sort()
    columns_name = [str(x) for x in columns_name]

    # Levo le righe(i comuni) che non sono presenti nelle colonne
    new_df = df[df.comune.isin(columns_name)]

    # Ordino le righe del dataframe per codice comune
    new_df = new_df.sort_values('comune')
    # Sostituisco l'index con la lista ordinata dei comuni
    comuni_list = new_df['comune']
    new_df.index = comuni_list
    new_df.index.name = 'Index'
    # Levo la colonna comune dal dataframe
    new_df = new_df.drop(['comune'], axis=1)
    # Ordino le colonne per codice ospedale crescente
    new_df = new_df[columns_name]
    # Metto ad int i nomi delle colonne
    list_col = new_df.columns
    list_col = [int(x) for x in list_col]
    new_df.columns = list_col

    # Quello che ottengo è una matrice quadrata 69x69 con diagonale principale 0 che
    # rappresenta le distanze tra comuni che hanno un ospedale
    ppath = "Dati_Elaborati/"
    spath = os.path.abspath(ppath)
    new_df.to_csv(r''+ spath + '/distanzeOspedali' + '.csv')

def comuneSpecialita_parser(file_name, path):
    data = pd.read_excel(r'' + path + '/general/' + file_name + '.xlsx')
    df = pd.DataFrame(data)
    ppath = "Dati_Elaborati/"
    spath = os.path.abspath(ppath)
    df.to_csv(r'' + spath + '/comuneSpecialita' + '.csv')

def __parseCel(cel):
    num = [int(n) for n in cel.split('_') if n != '']
    tot = sum(num)
    return tot

""" COMMENTO CORREZIONE: Questi array dovrebbero essere generati con un file esterno. Non ha senso averli direttamente nel codice"""
def comuneOspedale_parser(file_name, path, year_name):
    years = ['2011', '2012', '2013']
    c_names = ['codici_comune','res_01000300', 'res_01000401', 'res_01000402', 'res_01000403', 
               'res_01000404', 'res_01000501', 'res_01000502', 'res_01000503', 'res_01000601', 
               'res_01000602', 'res_01000700', 'res_01000801', 'res_01000802', 'res_01000901', 
               'res_01000902', 'res_01001000', 'res_01001100', 'res_01001200', 'res_01001300', 
               'res_01001700', 'res_01001901', 'res_01001903', 'res_01002301', 'res_01002302', 
               'res_01002303', 'res_01002304', 'res_01002601', 'res_01002602', 'res_01002603', 
               'res_01002604', 'res_01002605', 'res_01002606', 'res_01002701', 'res_01002702', 
               'res_01003001', 'res_01003002', 'res_01003003', 'res_01007901', 'res_01007902', 
               'res_01007903', 'res_01007904', 'res_01007905', 'res_01008500', 'res_01012001', 
               'res_01012002', 'res_01012401', 'res_01012402', 'res_01012601', 'res_01012602', 
               'res_01061000', 'res_01061100', 'res_01061200', 'res_01061300', 'res_01061400', 
               'res_01061500', 'res_01061600', 'res_01061700', 'res_01061800', 'res_01062000', 
               'res_01062100', 'res_01062200', 'res_01062300', 'res_01062600', 'res_01062700', 
               'res_01062800', 'res_01062900', 'res_01063000', 'res_01063100', 'res_01063200', 
               'res_01063300', 'res_01063400', 'res_01063500', 'res_01063900', 'res_01064000', 
               'res_01064100', 'res_01064200', 'res_01064300', 'res_01064400', 'res_01064600', 
               'res_01064700', 'res_01064800', 'res_01064900', 'res_01065100', 'res_01065300', 
               'res_01065400', 'res_01065500', 'res_01065600', 'res_01065700', 'res_01066100', 
               'res_01088200', 'res_01089000', 'res_01089100', 'res_01089200', 'res_01089300', 
               'res_01089700', 'res_01089800', 'res_01090101', 'res_01090102', 'res_01090104', 
               'res_01090201', 'res_01090203', 'res_01090301', 'res_01090302', 'res_01090400', 
               'res_01090501', 'res_01090502', 'res_01090600', 'res_01090701', 'res_01090702', 
               'res_01090703', 'res_01090800', 'res_01090901', 'res_01090904', 'res_01090907', 
               'res_01092000', 'res_01092100']
    for y in years:
        data = pd.read_excel(r'' + path + '/' +y + '/' + file_name + '.xlsx')
        df = pd.DataFrame(data)
        for index, row in df.iterrows():
            col_list = df.columns
            for c in col_list[1:]:
                df.loc[index, c] = __parseCel(df.loc[index, c])

        df = df.reindex(columns = c_names)
        df.to_csv(r'' + path + '/general/' + year_name + y + '.csv')
        print(f"Fine anno {y}")


""" COMMENTO CORREZIONE: questi array vanno gestiti con file esterno! """
def totalComuneOspedale(year_name, path):
    # unisco tutti i dati dei csv creati da comuneOspedale_parser
    df1 = pd.read_csv(r'' + path + '/general/' + year_name + '2011' + '.csv')
    df2 = pd.read_csv(r'' + path + '/general/' + year_name + '2012' + '.csv')
    df3 = pd.read_csv(r'' + path + '/general/' + year_name + '2013' + '.csv')
    c_names = ['res_01000300', 'res_01000401', 'res_01000402', 'res_01000403', 'res_01000404', 
               'res_01000501', 'res_01000502', 'res_01000503', 'res_01000601', 'res_01000602', 
               'res_01000700', 'res_01000801', 'res_01000802', 'res_01000901', 'res_01000902', 
               'res_01001000', 'res_01001100', 'res_01001200', 'res_01001300', 'res_01001700', 
               'res_01001901', 'res_01001903', 'res_01002301', 'res_01002302', 'res_01002303', 
               'res_01002304', 'res_01002601', 'res_01002602', 'res_01002603', 'res_01002604', 
               'res_01002605', 'res_01002606', 'res_01002701', 'res_01002702', 'res_01003001', 
               'res_01003002', 'res_01003003', 'res_01007901', 'res_01007902', 'res_01007903', 
               'res_01007904', 'res_01007905', 'res_01008500', 'res_01012001', 'res_01012002', 
               'res_01012401', 'res_01012402', 'res_01012601', 'res_01012602', 'res_01061000', 
               'res_01061100', 'res_01061200', 'res_01061300', 'res_01061400', 'res_01061500', 
               'res_01061600', 'res_01061700', 'res_01061800', 'res_01062000', 'res_01062100', 
               'res_01062200', 'res_01062300', 'res_01062600', 'res_01062700', 'res_01062800', 
               'res_01062900', 'res_01063000', 'res_01063100', 'res_01063200', 'res_01063300', 
               'res_01063400', 'res_01063500', 'res_01063900', 'res_01064000', 'res_01064100', 
               'res_01064200', 'res_01064300', 'res_01064400', 'res_01064600', 'res_01064700', 
               'res_01064800', 'res_01064900', 'res_01065100', 'res_01065300', 'res_01065400', 
               'res_01065500', 'res_01065600', 'res_01065700', 'res_01066100', 'res_01088200', 
               'res_01089000', 'res_01089100', 'res_01089200', 'res_01089300', 'res_01089700', 
               'res_01089800', 'res_01090101', 'res_01090102', 'res_01090104', 'res_01090201', 
               'res_01090203', 'res_01090301', 'res_01090302', 'res_01090400', 'res_01090501', 
               'res_01090502', 'res_01090600', 'res_01090701', 'res_01090702', 'res_01090703', 
               'res_01090800', 'res_01090901', 'res_01090904', 'res_01090907', 'res_01092000', 
               'res_01092100']
    
    df = pd.concat([df1, df2, df3]).groupby('codici_comune')[c_names].sum().reset_index()
    ppath = "Dati_Elaborati/"
    spath = os.path.abspath(ppath)
    df.to_csv(r'' + spath + '/comuneOspedale_Tot' + '.csv')

def dict_comuni_hosp():
    # Creo il dizionario della tabella del file distanzeComuniOspedali.csv. (quello con tutto)
    # il dizionario sarà {id_c:{id_ch : dis}}. id_c è l'id del comune (riga), id_ch è l'id del 
    # comune dell'ospedale (colonna), e dis è la distanza in metri
    df = pd.read_csv('../RawData/distanzeComuniOspedali.csv')
    final_dict = {}
    col_names = df.columns.values[2:]
    for index, row in df.iterrows():
        h_dict = {}
        for c in col_names:
            h_dict[str(c)] = float(row.loc[c])
        final_dict[(str(int(row.loc['comune'])))] = h_dict

    joblib.dump(final_dict, '../DatiElaborati/distanzeComuniOspedali')
    return final_dict

def dict_comunihosp(path, name):
    # Creo il dizionario della tabella del file distanzeComuniOspedali.csv.
    # il dizionario sarà {id_c:{id_ch : dis}}. id_c è l'id del comune (riga), id_ch è l'id del 
    # comune dell'ospedale (colonna), e dis è la distanza in metri
    df = pd.read_csv(path)
    final_dict = {}
    col_names = df.columns.values[1:]
    for index, row in df.iterrows():
        h_dict = {}
        for c in col_names:
            h_dict[str(c)] = float(row.loc[c])
            
        final_dict[str(int(row.loc['Index']))] = h_dict
    ppath = "Dati_Elaborati/"
    spath = os.path.abspath(ppath)
    joblib.dump(final_dict, spath + '/' + name)

def dict_mapping():
    # Creo il dizionario del mapping del file mapping_hosp_comuni.csv
    # il dizionario sarà {id_h:id_c} con id_c l'id del comune e id_h l'id
    # dell'ospedale di quel comune
    path = '../RawData/mapping_hosp_comuni.csv'
    df = pd.read_csv(
        path,
        usecols=['codice_struttura_erogante', 'id_comune_struttura_erogante'],
        dtype={
            'codice_struttura_erogante': 'string',
            'id_comune_struttura_erogante': 'string'
        }
    )
    final_dict = {}
    for index, row in df.iterrows():        
        final_dict[str(row.iloc[0])] = str(row.iloc[1])
    joblib.dump(final_dict, '../DatiElaborati/mappingOspCom')
    return final_dict



if __name__ == '__main__':
    ppath = "Dati/"
    path = os.path.abspath(ppath)
    
    # File di distanze tra comuni
    file_name = 'originDistancesMetri'
    #originDistanceMetri_parser(file_name, path)

    # File dei comuni con ospedali
    file_name = 'comuneSpecialitaMatrix'
    #comuneSpecialita_parser(file_name, path)

    # File quantità pazienti del comune con vari ospedali
    file_name = 'originToHospitalDistribution'
    year_name = 'comuneOspedale_'
    #comuneOspedale_parser(file_name, path, year_name)
    #totalComuneOspedale(year_name, path)

    # Creazione del dizionario del file distanzeComuniOspedali.csv e 
    path = 'Dati/general/distanzeComuniOspedali.csv'
    name = 'distanzeComuniOspedali'
    #dict_comuni_hosp(path, name)
    
    # creazione del dizionario del file distanzeOspedali.csv
    path = 'Dati/general/distanzeComuniOspedali.csv'
    name = 'distanzeOspedali'
    #dict_comunihosp(path, name)

    # Creazione del dizionario di mapping tra id del record del paziente e id del comune di residenza.
    # I file sono i anagrafica.csv in extraction
    path = 'Dati/general/'
    #dict_recod_residenza(path)

