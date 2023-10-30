#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd

if __name__ == '__main__':
    path = '../Consegna/Dati/'
    ricoveri1 = pd.read_csv(path+"2011/sdo1.csv", usecols=['anno', 'n_record', 'data_ricovero', 
                                                           'gg_degenza', 'data_prenotazione', 
                                                           'codice_struttura_erogante', 
                                                           'disciplina_uo_ammissione'])
    ricoveri2 = pd.read_csv(path+"2012/sdo2.csv", usecols=['anno', 'n_record', 'data_ricovero', 
                                                           'gg_degenza', 'data_prenotazione', 
                                                           'codice_struttura_erogante', 
                                                           'disciplina_uo_ammissione'])
    ricoveri3 = pd.read_csv(path+"2013/sdo3.csv", usecols=['anno', 'n_record', 'data_ricovero', 
                                                           'gg_degenza', 'data_prenotazione', 
                                                           'codice_struttura_erogante', 
                                                           'disciplina_uo_ammissione'])  
    read_file = pd.read_excel (r'../Dati/general/branca.xls')
    read_file.to_csv (r'../Dati_Elaborati/branca.csv', index = None, header=True)
    branca = pd.read_csv("Dati_Elaborati/branca.csv")
    ricoveri_concat = pd.concat([ricoveri1, ricoveri2, ricoveri3], axis=0, join='outer', 
                                ignore_index=False)
    ricoveri_concat['data_prenotazione'] = ricoveri_concat['data_prenotazione'].fillna(
                                                                                ricoveri1['data_ricovero'])
    joined = ricoveri_concat.merge(branca, how='left', left_on='disciplina_uo_ammissione', 
                                   right_on='DES_BRANCA')
    joined.index.name='Index'
    joined.to_csv('../Dati_Elaborati/ricoveri.csv')


