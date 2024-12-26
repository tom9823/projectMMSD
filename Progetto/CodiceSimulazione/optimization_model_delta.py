from pyomo.environ import *
from pyomo.opt import *
import pyomo.environ as pyo

# Variabile obbiettivo
def obj_expression(m):
  return m.big_delta

""" Relazionns pag. 9/18 """
# Constraint
def patient_in_only_one_hospital(m, p):
  return sum(m.x[p,h] for h in m.H) == 1

def patients_redistribution(m, h):
  return sum(m.l[p] * m.x[p,h] for p in m.P) <= m.gamma[h]

def discomfort_calculation(m, p, h):
  return m.delta[p] >= (m.d[p,h]-m.m[p])*m.x[p,h]

def big_delta_greater_than_delta(m, p):
  return m.big_delta >= m.delta[p]

def create_model(data, solver, time_limit):
    model = pyo.AbstractModel()
    opt = solvers.SolverFactory(solver)
    if solver == 'cplex': opt.options['timelimit'] = time_limit
    elif solver == 'glpk': opt.options['tmlim'] = time_limit
    elif solver == 'gurobi': opt.options['TimeLimit'] = time_limit
    elif solver == 'scip': opt.options['TimeLimit'] = time_limit
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
    model.big_delta = pyo.Var(domain=pyo.NonNegativeReals)

    model.OBJ = pyo.Objective(rule=obj_expression)
    model.PatientInOnlyOneHospital = pyo.Constraint(model.P, rule=patient_in_only_one_hospital)
    model.PatientsRedistribution = pyo.Constraint(model.H, rule=patients_redistribution)
    model.DiscomfortCalculation = pyo.Constraint(model.P, model.H, rule=discomfort_calculation)
    model.bigDeltaGreaterThanDelta = pyo.Constraint(model.P, rule=big_delta_greater_than_delta)
    
    model_instance = model.create_instance(data)
    result = opt.solve(model_instance)
    return result, model_instance