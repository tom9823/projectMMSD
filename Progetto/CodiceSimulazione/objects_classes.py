"""
Insieme di tutte le classi utili per la simulazione
"""
class Hospital():
    """Classe per la creazione dell'oggetto Ospedale."""

    def __init__(self, id_hosp, id_spec, capacity):
        self.id_hosp = id_hosp
        self.id_spec = id_spec
        self.capacity = capacity  # lista lunga 8 valori (primi 7 settimana)
        self.waiting_queue = []
        self.counter_day_cap = 0
        self.rest_queue = []
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