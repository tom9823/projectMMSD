from enum import IntEnum


class OptimizerModelType(IntEnum):
    NORM_1 = 1
    NORM_2 = 2
    NORM_INF = 0


"""
Insieme di tutte le classi utili per la simulazione
"""


class HospitalSpeciality():
    """Classe per la creazione dell'oggetto Ospedale."""

    def __init__(self, id_hosp, id_spec, capacity):
        self.id_hosp = id_hosp
        self.id_spec = id_spec
        self.capacity = capacity  # lista lunga 8 valori (i primi 7 elementi sono i pazienti ricoverabili nei primi 7 rispettivi giorni della settimana, l'ttavo leemento è il numero massimo di posti letto dell'ospedale)
        self.waiting_queue = []  # lista di pazienti in attesa per quella specialità s dell'ospedale h
        self.counter_current_day_patients_recovered = 0
        self.rest_queue = []  # lista pzienti in degenza
        self.counter_day_queue = 0
        self.counter_max_queue = 0


class Patient():
    """Classe per la creazione dell'oggetto Paziente."""

    def __init__(self, id_patient, rest_time, patient_day_recovery,
                 patient_id_hosp, patient_id_spec):
        self.id_patient = id_patient
        self.patient_id_hosp = patient_id_hosp
        self.patient_id_spec = patient_id_spec
        self.rest_time = rest_time
        self.patient_day_recovery = patient_day_recovery
        self.patient_true_day_recovery = ''
        self.queue_motivation = ''
        self.counter_queue = 0
