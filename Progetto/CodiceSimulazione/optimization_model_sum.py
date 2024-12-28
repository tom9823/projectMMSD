from gettext import install
from math import sqrt

import numpy as np
from pyomo.opt import *
import pyomo.environ as pyo

from Progetto.CodiceSimulazione.objects_classes import OptimizerModelType

"""
Legenda del modello.
Struttura dati da usare nell'ottimizzatore. Bisogna suddividere i pazienti in base alla loro 
specialità => dovrò eseguire l'ottimizzatore n volte, dove n è il numero di specialità univoche dei 
pazienti (patient_to_reassing.groupby('id_specialita')).
data = {None: {
                'P': {None: [20,10,30]},
                'H': {None: [100, 300, 200]},
                'l': {20: 1, 10:1, 30:1},
                'gamma': {100: 1, 300:1, 200:1},
                'm': {20: 20, 10: 10, 30: 30},
                'd': {(20,100): 30, (20, 300): 31, (20, 200): 32,
                    (10,100): 20, (10, 300): 11, (10, 200): 50,
                    (30,100): 50, (30, 300): 32, (30, 200): 70,
                    },
                }
        }
P = dizionario con lista id pazienti da riallocare (patient_to_reassing)
H = dizionario con lista id ospedali della capacità scelta
l = giorni di degenza per ogni paziente
gamma = (1+alfa)*f*L con: alfa=0.2; f=posti letto / totale posti letto (per ciascuna specialità); 
        L= somma delle degenze di tutti i pazienti
m = distanza del vecchio ospedale
d = distanze con i nuovi ospedali
"""


# Variabile obbiettivo
def obj_expression_norm_1(m):
    return pyo.summation(m.delta)


def obj_expression_norm_2(m):
    return pyo.sqrt(sum(m.delta[p] ** 2 for p in m.P))


def obj_expression_norm_inf(model):
    return max(model.delta)


# Vincoli
def patient_assigned_to_only_one_hospital(model, p):
    return sum(model.x[p, h] for h in model.H) == 1


def patients_redistribution(model, h):
    return sum(model.l[p] * model.x[p, h] for p in model.P) <= model.gamma[h]


def discomfort_calculation(model, p, h):
    return model.delta[p] >= (model.d[p, h] - model.m[p]) * model.x[p, h]


def dont_exceed_maximum_discomfort(model, p):
    return max(model.delta) >= model.delta[p]


def create_model(data, solver, time_limit, optimizer_model_type):
    model = pyo.AbstractModel()
    opt = solvers.SolverFactory(solver)
    if solver == 'cplex':
        opt.options['timelimit'] = time_limit
    elif solver == 'glpk':
        opt.options['tmlim'] = time_limit
    elif solver == 'gurobi':
        opt.options['TimeLimit'] = time_limit

    # Parametri
    model.P = pyo.Set(within=pyo.NonNegativeIntegers)
    model.H = pyo.Set(within=pyo.NonNegativeIntegers)
    model.l = pyo.Param(model.P)
    model.gamma = pyo.Param(model.H)
    model.m = pyo.Param(model.P)
    model.d = pyo.Param(model.P, model.H)

    # Variabili
    model.x = pyo.Var(model.P, model.H, domain=pyo.Binary)
    model.delta = pyo.Var(model.P, domain=pyo.NonNegativeReals)
    model.q = pyo.Var(model.P, domain=pyo.NonNegativeReals)

    #Funzione obiettivo
    if optimizer_model_type == OptimizerModelType.NORM_1:
        model.OBJ = pyo.Objective(rule=obj_expression_norm_1)
    elif optimizer_model_type == OptimizerModelType.NORM_2:
        model.OBJ = pyo.Objective(rule=obj_expression_norm_2)
    elif optimizer_model_type == OptimizerModelType.NORM_INF:
        model.OBJ = pyo.Objective(rule=obj_expression_norm_inf)

    # Vincoli
    model.PatientInOnlyOneHospital = pyo.Constraint(model.P, rule=patient_assigned_to_only_one_hospital)
    model.PatientsRedistribution = pyo.Constraint(model.H, rule=patients_redistribution)
    model.DiscomfortCalculation = pyo.Constraint(model.P, model.H, rule=discomfort_calculation)
    if optimizer_model_type == OptimizerModelType.NORM_INF:
        model.DiscomfortCalculation = pyo.Constraint(model.P, rule=dont_exceed_maximum_discomfort)
    model.OBJ = pyo.Objective(rule=obj_expression_norm_inf)
    model_instance = model.create_instance(data)

    # model_instance.pprint()
    # Solving del modello
    results = opt.solve(model_instance)

    if (results.solver.status == SolverStatus.ok) and (
            results.solver.termination_condition == TerminationCondition.optimal):
        return results, model_instance
    elif results.solver.termination_condition == TerminationCondition.infeasible:
        return results, model_instance
    else:
        print(f'Solver Status: {results.solver.status}')
    return results, model_instance
