# CP-SAT - xijt

import _input_config
input = _input_config.config() # input[0] = inputfile; result[1] = time_limit in hours
inputfile = input[0] # inputfile
time_limit_hour = input[1] # Time limit in hours

import os.path # Get script path
curr_dir = os.path.dirname(os.path.abspath(__file__)) # Get script path

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#>SETUP
####################################################################################################
####################################################################################################
# 1 - SETUP

import psutil # To get number of cores

# PANDAS, NUMPY
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile

import xlsxwriter
import numpy as np

# Garbage collector
import gc

# Date/time for naming export files
import time

# OR-TOOLS
from ortools.linear_solver import pywraplp
from ortools.sat.python import cp_model

# CPLEX
import sys

import docplex.mp
from docplex.mp.model import Model

import docplex.mp.sdetails 
from docplex.mp.environment import Environment

env = Environment()
# env.print_information()

# PLOTLY
from plotly.offline import plot
import plotly.figure_factory as ff
import os # Used to save PLOTLY as image

####################################################################################################
####################################################################################################
# 2 - IMPORT INPUT
print("Imported file: ", inputfile)

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#>PREPARE INPUT
####################################################################################################
####################################################################################################
# 1 - Transfer input file to Pandas DF
input_machines = pd.read_excel(curr_dir + inputfile, sheet_name='MACHINES', usecols = "A:C")
input_tasks = pd.read_excel(curr_dir + inputfile, sheet_name='JOBS')

####################################################################################################
####################################################################################################
# 2 - Replace PD and SD by 0, NA by 1 in input_tasks
machines = {'PD': 0,'SD': 0, np.NaN:1} # Define dictionary 
input_tasks['DEV TYPE'] = input_tasks['DEV TYPE'].map(machines)

####################################################################################################
####################################################################################################
# 3 - Replace hex IDs by integer IDs
# Adds ID_integer (row index) to input_tasks
# "Try" allow cells to be run many times.
try:
    input_tasks.insert(0, 'IDNUM', range(0, len(input_tasks)))
except ValueError: # Error handling in case of floating value
    pass # do nothing

# Create dictionary: ID hex to ID int
id_dic = dict(zip(input_tasks['ID'], input_tasks['IDNUM'])) # Dictionary ID hex --> ID integer

####################################################################################################
####################################################################################################
# 4 - Split "Predecessor IDs" in different coluns and add them to input_tasks
split_pred = input_tasks['Predecessor IDs'].str.split(';', n=-1, expand=True) # ";" is the separator in input file
tasks_concat = pd.concat([input_tasks,split_pred], axis=1)

for col in tasks_concat.columns[6:]:
    tasks_concat[col] = tasks_concat[col].map(id_dic) # Maps ID int to previous ID hex 
    tasks_concat.rename(columns={col:'Pred_'+ str(col)}, inplace=True) # Rename columns
    
del tasks_concat['ID']
del tasks_concat['Predecessor IDs'] 

####################################################################################################
####################################################################################################
# 5 - Create Profit and Precedessors DF
# Profit
tasks_profit = tasks_concat.copy()
for col in tasks_profit.columns[1:]:
    if col == "PROFIT":
        continue
    del tasks_profit[col]
tasks_profit = pd.DataFrame(tasks_profit['PROFIT']).astype(int) # Change to int values for CP-SAT
tasks_profit_matrix = pd.concat([tasks_profit]*len(input_machines.index), ignore_index=True, axis=1)

# Predecessors
tasks_pred = tasks_concat.copy()
for col in tasks_pred.columns[0:4]:  
    if col == "DEV TYPE":
        continue
    del tasks_pred[col]

# Convert to int
tasks_pred.fillna(-1,inplace=True) # Replace NAN by -1
tasks_pred = tasks_pred.applymap(str) # Convert object to str
tasks_pred = tasks_pred.apply(pd.to_numeric, errors='raise', downcast='integer').fillna(-1) # Convert str to integer

####################################################################################################
####################################################################################################
# 6 - Split predecessors in Hard and Soft predecessors
physical_access = tasks_concat[tasks_concat['DEV TYPE'] == 0]["IDNUM"] # If DEV TYPE == 0, task may be a Hard Predecessor

# Hard Predecessors
tasks_pred_hard = tasks_pred.applymap(lambda x: x if x in physical_access else -1) # Lambda to consider hard constraints
tasks_pred_hard.insert(0, 'IDNUM', range(0, len(input_tasks))) # Insert IDNUM again
del tasks_pred_hard["DEV TYPE"] # Delete DEV TYPE
tasks_pred_hard.fillna(-1,inplace=True) # Replace NAN by -1
tasks_pred_hard = tasks_pred_hard.applymap(str) # Convert object to str
tasks_pred_hard = tasks_pred_hard.apply(pd.to_numeric, errors='raise', downcast='integer').fillna(-1)  # Convert str to integer

# Soft Predecessors
tasks_pred_soft = tasks_pred.applymap(lambda x: x if x not in physical_access else -1) # Lambda to disconsider hard constraints
tasks_pred_soft.insert(0, 'IDNUM', range(0, len(input_tasks))) # Insert IDNUM again
del tasks_pred_soft["DEV TYPE"] # Delete DEV TYPE
tasks_pred_soft.fillna(-1,inplace=True) # Replace NAN by -1
tasks_pred_soft = tasks_pred_soft.applymap(str) # Convert object to str
tasks_pred_soft = tasks_pred_soft.apply(pd.to_numeric, errors='raise', downcast='integer').fillna(-1)  # Convert str to integer

####################################################################################################
####################################################################################################
# 07 - Tasks successors
# Create temporary tasks_succ DF
tasks_succ = tasks_concat.copy()
tasks_succ.drop(tasks_succ.columns[[1, 2, 3]], axis=1, inplace = True) # Delete DEV TYPE, Driving quantity, and PROFIT columns

# Stack predecessors in a list
pred_list = tasks_succ[tasks_succ.columns[1]] # Create list with 1st column of predecessors
for col in tasks_succ.columns[2:]:
    pred_list = pred_list.append(tasks_succ[col], ignore_index=True) # Stack other columns of predecessors in a list

# Stack IDNUM in a list
index_col = len(tasks_succ.columns)-1 # Number of predecessors columns to repeat IDNUM in a list
succ_id = tasks_succ['IDNUM'] # Create list of IDNUM
for col in range(index_col-1): # -1 since 1st was already created
    succ_id = succ_id.append(tasks_succ['IDNUM'], ignore_index=True) # Stack other columns of predecessors in a list

# Combine predecessors list and IDNUM repeated list in a DF
tasks_succ = pd.concat([succ_id,pred_list], axis=1)
tasks_succ.columns = ['SUCC', 'IDNUM'] # Rename the two columns

# Group lists in multiple columns
tasks_succ = tasks_succ.groupby(['IDNUM'])['SUCC'].apply(lambda x: ';'.join(x.astype(str))).reset_index() # Group by predecessos (IDNUM) and concatenate
tasks_succ = pd.merge(tasks_succ,tasks_concat[['IDNUM']], how='outer') # Complete tasks_succ with IDNUM that do not have successors
tasks_succ = tasks_succ.sort_values(by=['IDNUM']) # Sort by IDNUM
tasks_succ = tasks_succ.reset_index(drop=True) # Reset index
tasks_succ_split = tasks_succ['SUCC'].str.split(';', n=-1, expand=True) # Split sucessors in columns
tasks_succ = pd.concat([tasks_succ,tasks_succ_split], axis=1) # Add split sucessors to DF
for col in tasks_succ.columns[2:]: # Rename sucessors columns 
    tasks_succ.rename(columns={col:'Succ_'+ str(col)}, inplace=True)
del tasks_succ['SUCC'] # Delete column with grouped sucessors

# Convert to int
tasks_succ.fillna(-1,inplace=True) # Replace NAN by -1
tasks_succ = tasks_succ.applymap(str) # Convert object to str
tasks_succ = tasks_succ.apply(pd.to_numeric, errors='raise', downcast='integer').fillna(-1) # Convert str to integer

####################################################################################################
####################################################################################################
# 8 - Create pij duration matrix
pij_duration = tasks_concat[["IDNUM","DEV TYPE","Driving quantity"]].copy()
for i in range(len(input_machines)): # Loop through each machine and create a column for each
    pij_duration['Mach_'+str(i)] = -(-pij_duration['Driving quantity']//input_machines.iloc[i, 2]*(pij_duration['DEV TYPE'] == input_machines.iloc[i, 0])) # If machine does not process task type, duration will be 0. Double slash // for rounding
pij_duration.drop(pij_duration.columns[[0, 1, 2]], axis=1, inplace = True) # Delete IDNUM, DEV TYPE, and Driving quantity columns
pij_duration = pij_duration.astype(int) # Change to int to work in CP-SAT

####################################################################################################
####################################################################################################
# 9 - l-time parameter (upper-bound for time index) - Calculation with greedy allocation
# Create a DF to store cumulative duration for tasks that have predecessors
tasks_cum_duration = tasks_concat.copy()
tasks_cum_duration.drop(tasks_cum_duration.columns[1:], axis=1, inplace = True) # Keep only IDNUM column
tasks_cum_duration['Cumulative duration'] = 0 # Create column to store cumulative duration for tasks that have predecessors

# Initialize group of avaiable tasks (tasks without predecessors)
tasks_available = tasks_pred.copy() # This is used for the whole loop, being updated with -1 as tasks are allocated
tasks_available.drop(tasks_available.columns[0], axis=1, inplace = True) # Keep only 'Pred_' columns
max_num_pred = len(tasks_available.columns) # Maximum number of predecessors that can happen
tasks_available_list = tasks_pred.copy() # This will be a list
tasks_available_list['Count_empty'] = tasks_available.isin([-1]).sum(axis=1) # Column to count empty predecessors
tasks_available_list = tasks_available_list.loc[tasks_available_list['Count_empty'] == max_num_pred] # Filter tasks that are available (do not have predecessors)
tasks_available_list = tasks_available_list.index.values.astype(int).tolist() # list with available tasks to be allocated

# Initialize machines
machine_time = input_machines.copy()
machine_time['Available at time...'] = 0 # Create column to store time of availability of each machine

# Create a DF to store allocation
processed_tasks_allocation = pd.DataFrame(columns=['IDNUM','Machine', 'Task start (xi)', 'Duration'])

# Allocate each and update available tasks
i=0 # Used for processed_tasks_allocation and for printing loop counter
while len(tasks_available_list) != 0: # Allocate loop while available tasks exists
# Allocate first task of tasks_available_list
    process_task = tasks_available_list[0] # Select task to be processed (first)
    process_task_mach_type = tasks_concat.iloc[process_task,1] # Get machine TYPE

    process_task_allocated_mach = machine_time.loc[machine_time['TYPE'] == process_task_mach_type] # Select machine to allocate --> filter by TYPE
    process_task_allocated_mach_min_time = process_task_allocated_mach['Available at time...'].min() # Select machine to allocate --> get the one with minimum available time
#     process_task_allocated_mach = process_task_allocated_mach.loc[process_task_allocated_mach['Available at time...'] == process_task_allocated_mach_min_time].index[0] # If more than one under criteria, get smallest ID --> If this is online, comment next 4 lines
    process_task_allocated_mach = process_task_allocated_mach.loc[process_task_allocated_mach['Available at time...'] == process_task_allocated_mach_min_time] # Get the machines with minimum available time
    process_task_allocated_mach_max_rate = process_task_allocated_mach['RATE'].max() # Maximum rate of available machines
    process_task_allocated_mach = process_task_allocated_mach.loc[process_task_allocated_mach['RATE'] == process_task_allocated_mach_max_rate].index[0] # From the one above, get the highest rate machines. If more than one available get the minimum ID
    
    process_task_dr_quantity = tasks_concat.iloc[process_task,2] # Get driving quantity
    process_task_duration = process_task_dr_quantity / input_machines.iloc[process_task_allocated_mach,2] # Get duration based on machine rate
    if not isinstance(process_task_duration, int): # Check if process_task_duration is integer --> It has to be for MIP solvers
        process_task_duration = int(process_task_duration) + 1 # Round up if not integer
    process_task_cum_duration = process_task_duration + tasks_cum_duration.iloc[process_task,1] # Update cumulative duration of task predecessors with duration of task
        
    machine_time.iloc[process_task_allocated_mach,3] += process_task_duration # Update machine time with processed task duration
    machine_time.iloc[process_task_allocated_mach,3] = max(machine_time.iloc[process_task_allocated_mach,3], process_task_cum_duration) # Update machine time with maximum of (updated machine time, cumulative duration)

# Remove processed task from tasks_available_list
    tasks_available = tasks_available.drop(process_task) # Drop the row of the processed task
    tasks_available_list.remove(process_task) # Remove processed task from available list
    processed_tasks_allocation.loc[i] = process_task # Updated list of processed tasks --> IDNUM. LOC to include row
    processed_tasks_allocation.iloc[i,1] = process_task_allocated_mach # Updated list of processed tasks --> Allocated machine
    processed_tasks_allocation.iloc[i,2] = machine_time.iloc[process_task_allocated_mach,3] - process_task_duration # Updated list of processed tasks --> Task start (xi)
    processed_tasks_allocation.iloc[i,3] = process_task_duration # Updated list of processed tasks --> Duration

# Update processed task time (end) to 'Cumulative duration' of its sucessors
    process_task_succ = tasks_succ.loc[tasks_succ['IDNUM'] == process_task,tasks_succ.columns[1:]].values # Get successors of processed task
    process_task_succ = process_task_succ[process_task_succ!=-1].tolist() # Ignore -1 , since it represents NAN    
    if len(process_task_succ)> 0:
        for succ in process_task_succ:
            tasks_cum_duration.iloc[succ,1] = machine_time.iloc[process_task_allocated_mach,3] # Get value of finish of precessed task
# Replace processed task by -1 in tasks_available 'Pred_' coluns
    tasks_available = tasks_available.replace(process_task, -1)
    
# Check available tasks tasks (without predecessors)
    tmp_tasks_available_list = tasks_available.copy() # This one is temporary
    tmp_tasks_available_list['Count_empty'] = tasks_available.isin([-1]).sum(axis=1) # Column to count empty predecessors
    tmp_tasks_available_list = tmp_tasks_available_list.loc[tmp_tasks_available_list['Count_empty'] == max_num_pred] # Filter tasks that are available (do not have predecessors)
    tmp_tasks_available_list = tmp_tasks_available_list.index.values.astype(int).tolist() # list with available tasks to be allocated

    for task in tmp_tasks_available_list: # Update tasks_available_list with new available tasks but keeping the order
        if task not in tasks_available_list:
            tasks_available_list.append(task)

# Print allocation for each loop
    i+=1
#     print('----- Loop:',i)
#     print('Processed task:',process_task,'  - Machine:',process_task_allocated_mach,'  - Duration:',process_task_duration,'  - Available tasks:',tasks_available_list)
#     print('\n',machine_time,'\n')
#     print('\n',tasks_cum_duration,'\n')

processed_tasks_allocation = processed_tasks_allocation.sort_values(by=['IDNUM']) # Sort by IDNUM
l = machine_time['Available at time...'].max() # Upper-bound for time index
if not isinstance(l, int): # Check if L is integer --> It has to be for MIP solvers
    l = int(l) + 1 # Round up if not integer

####################################################################################################
####################################################################################################
# 10 - Create Qi values
# Hard
Qi = tasks_pred_hard.copy() # DF that copies all tasks_pred_hard DF
Qi['-1 type'] = tasks_pred_hard.eq(-1).sum(axis=1) # Aux column with number of ignored predecessors (-1) values
Qi['Qi'] = len(tasks_pred_hard.columns) - Qi['-1 type'] - 1 # Number of hard predecessors = number of columns - ignored -1 predecessors - 1 (IDNUM)
Qi_hard = Qi[['IDNUM','Qi']]

# Soft
Qi = tasks_pred_soft.copy() # DF that copies all tasks_pred_soft DF
Qi['-1 type'] = tasks_pred_soft.eq(-1).sum(axis=1) # Aux column with number of ignored predecessors (-1) values
Qi['Qi'] = len(tasks_pred_soft.columns) - Qi['-1 type'] - 1 # Number of hard predecessors = number of columns - ignored -1 predecessors - 1 (IDNUM)
Qi_soft = Qi[['IDNUM','Qi']]

# Hard and Soft
Qi = pd.DataFrame({'IDNUM':Qi_hard['IDNUM'], 'Qi':(Qi_hard['Qi'] + Qi_soft['Qi'])})

####################################################################################################
####################################################################################################
# 11 - Allowed machines for each task
allowed_machines = pd.merge(tasks_concat, input_machines, left_on=['DEV TYPE'], right_on=['TYPE'])
allowed_machines = allowed_machines.groupby('IDNUM').aggregate(lambda x: x.unique().tolist())
allowed_machines = allowed_machines[['MACHINE']]
allowed_machines

####################################################################################################
####################################################################################################
# 12 - Get discount rate, foundation & reserves timing costs and period costs
input_parameters = pd.read_excel(curr_dir + inputfile, sheet_name='PARAMETERS', usecols = "A:B")
day_disc_rate = input_parameters.iloc[1,1] # discount rate per day
costs_found_res_timing = input_parameters.iloc[2,1] # US$ per whole project
costs_period = input_parameters.iloc[3,1] # US$/d

####################################################################################################
####################################################################################################
#13 - Tasks profit (i) vs j vs t
tasks_profit_xit = pd.DataFrame(np.tile(tasks_profit.to_numpy(),int(l))) # DF with profit repeated l columns
discount_factors_xit = np.fromfunction(lambda i, j: 1/(1+day_disc_rate)**j, (len(input_tasks), int(l))) # Discount rates multiplying factors for each
discount_factors_xit = pd.DataFrame(discount_factors_xit) # DF with previous line data
tasks_profit_xijt = (tasks_profit_xit*discount_factors_xit).astype(int) # Multiply profit and discount factors
tasks_profit_xijt['0'] = tasks_profit_xijt.values.tolist() # Group columns into lists into one cell
tasks_profit_xijt.drop(tasks_profit_xijt.columns.difference(['0']), 1, inplace=True) # Drop all columns but the list one
tasks_profit_xijt = pd.DataFrame(np.tile(tasks_profit_xijt.to_numpy(),len(input_machines))) # Repeat the list column (with discounted profit) j-machine times
tasks_profit_xijt = tasks_profit_xijt.values.tolist() # Get list from df

####################################################################################################
####################################################################################################
# 14 - Inputs avaiable
# input_tasks
# input_machines
# allowed_machines
# tasks_concat
# tasks_profit
# tasks_profit_matrix
# tasks_profit_xijt
# tasks_pred
# tasks_pred_hard
# tasks_pred_soft
# tasks_succ
# pij_duration
# processed_tasks_allocation # For L calculation
# l
# Qi_hard
# Qi_soft
# Qi
# day_disc_rate
# costs_found_res_timing # Does not affect objective function
# costs_period

####################################################################################################
####################################################################################################
# 15 - Garbage collector --> Clear RAM
# %whos # Get objects created
# %whos DataFrame # DataFrames created
# %whos list # Lists created
del [[process_task_succ,
    tasks_available_list,
    tmp_tasks_available_list,
    discount_factors_xit,
    input_parameters,
    machine_time,
    split_pred,
    tasks_available,
    tasks_cum_duration,
    tasks_profit_xit,
    tasks_succ_split]] # Del DFs and lists not in use

gc_extra_call = gc.collect() # Garbage collector extra call to save RAM

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#>CP-SAT - xijt OPTIMIZING NPV
####################################################################################################
####################################################################################################
# 1 - Inputs / Parameters

print("L (upper bound) value considered for optimization: %i \n" % l)
l_input = l

maximum_gap_allowed = 0 # Initialize and reset value
# maximum_gap_allowed = 200  # Maximum GAP allowed

time_limit = 0 # Initialize and reset value
time_limit = 3600000*time_limit_hour # Time limit in miliseconds

try:
    num_threads = psutil.cpu_count() # Use multi threading / cores
except:
    num_threads = 1
   
####################################################################################################
####################################################################################################
# 2 - MIP Model
# Model
model = cp_model.CpModel()

# Variables
cmax = model.NewIntVar(0, int(l), 'cmax') # Makespan used to subtract period costs
xijt = [[[model.NewBoolVar('xij_%i_%i_%i' % (i, j, t)) for t in range(int(l))] for j in range(len(input_machines))] for i in range(len(input_tasks))]
wi = [model.NewBoolVar('wi_%i' % (i)) for i in range(len(input_tasks))]

# Count number of variables
num_var1 = 1 # Cmax
num_var2 = len(xijt[0][0])*len(xijt[0])*len(xijt) # xijt
num_var3 = len(wi) # wi
num_var_total = num_var1 + num_var2 + num_var3 # Total number of variables

# Objective function --> Maximize NPV
model.Maximize(np.multiply(xijt,tasks_profit_xijt).sum() - int(costs_period)*cmax)

# Constraints
num_const1 = 0
num_const2 = 0
num_const3 = 0
num_const4 = 0
num_const5 = 0
num_const6 = 0
num_const7 = 0
num_const_total = 0

print("CONST 1") # wi + sum(xijt) = 1 --> wi = 1 => tasks not processed. Otherwise: wi = 0
for i in range(len(input_tasks)):
    num_const1 += 1
    model.Add(wi[i] + np.sum(np.array(xijt[i])) == 1)
print(num_const1, "constraints \n")

print("CONST 2") # sum(xijs) <= 1 --> For each machine, in each time t only one task can occupy the machine
for t in range(int(l)):
    for j in range(len(input_machines)):
        num_const2 += 1
        sum_xijs = []
        for i in range(len(input_tasks)):
                for s in range(max(int(t-pij_duration.iloc[i,j]+1),0),t+1): # s is the time window that includes processing time
                    sum_xijs.append(xijt[i][j][s]) # append adds 1 element, extend adds a list of elements
        model.Add(sum(sum_xijs) <= 1)
print(num_const2, "constraints \n")

print("CONST 3") # Hard precedencies --> Qi_hard*sum(xijt) <= sum(xkjt) --> i only happens if ALL k hard predecessors happens  All hard pred as requirement for xijt = 1. Otherwise, xijt = 0
for i in range(len(input_tasks)):
    xijt_forall_k = []
    for a in range(len(tasks_pred_hard.columns)-1): # a is a contraint index to loop all predecessors DF --> total num of pred
        k = tasks_pred_hard.iloc[i,a+1] # k is the predecessor IDNUM of i; a+1 because 1st column is IDNUM
        if k!=-1:
            xijt_forall_k.extend(xijt[k])
    if Qi_hard.iloc[i,1] > 0: # Contraint exists only if i has predecessors
        num_const3 += 1
        model.Add(Qi_hard.iloc[i,1]*np.sum(xijt[i]) <= sum(np.array(xijt_forall_k).flatten())) # Flatten used to change 2D list to 1D list
print(num_const3, "constraints \n")

print("CONST 4") # Hard precedencies --> sum(t*xijt) + l*wi >= (s + pkj)*xkjs for each hard pred --> i start has to be > hard pred start or i does not happen
for i in range(len(input_tasks)):
    for a in range(len(tasks_pred_hard.columns)-1): # a is a contraint index to loop all predecessors DF --> total num of pred
        k = tasks_pred_hard.iloc[i,a+1] # k is the predecessor IDNUM of i; a+1 because 1st column is IDNUM
        if k!=-1:
            num_const4 += 1
            t_multi_xijt_forall_tj = []
            s_multi_xkjs_forall_sj = []
            for j in range(len(input_machines)):
                for t in range(int(l)):
                    t_multi_xijt_forall_tj.append(t*xijt[i][j][t])
                for s in range(int(l)):
                    s_multi_xkjs_forall_sj.append((s+pij_duration.iloc[k,j])*xijt[k][j][s])
            model.Add(sum(np.array(t_multi_xijt_forall_tj).flatten()) + l*wi[i] >= sum(np.array(s_multi_xkjs_forall_sj).flatten()))
print(num_const4, "constraints \n")

print("CONST 5") # Soft precedencies --> sum(t*xijt) + l*wi >= (s + pkj)*xkjs for each soft pred --> i start has to be > soft pred start or i does not happen
for i in range(len(input_tasks)):
    for a in range(len(tasks_pred_soft.columns)-1): # a is a contraint index to loop all predecessors DF --> total num of pred
        k = tasks_pred_soft.iloc[i,a+1] # k is the predecessor IDNUM of i; a+1 because 1st column is IDNUM
        if k!=-1:
            num_const5 += 1
            t_multi_xijt_forall_tj = []
            s_multi_xkjs_forall_sj = []
            for j in range(len(input_machines)):
                for t in range(int(l)):
                    t_multi_xijt_forall_tj.append(t*xijt[i][j][t])
                for s in range(int(l)):
                    s_multi_xkjs_forall_sj.append((s+pij_duration.iloc[k,j])*xijt[k][j][s])
            model.Add(sum(np.array(t_multi_xijt_forall_tj).flatten()) + l*wi[i] >= sum(np.array(s_multi_xkjs_forall_sj).flatten()))
print(num_const5, "constraints \n")

print("CONST 6") # Exclusion of impossible machines --> if machine not allowed sum(xijt) = 0
for i in range(len(input_tasks)):
    num_const6 += 1
    for j in range(len(input_machines)):
        if j not in allowed_machines.iloc[i,0]:
            model.Add(sum(np.array(xijt[i][j]).flatten()) == 0)
print(num_const6, "constraints \n")

print("CONST 7")  # Define makespan -->  Cmax >= sum(t*xijt) + sum(pij*sum(xij))
for i in range(len(input_tasks)):
    num_const7 += 1
    t_multi_xijt_forall_tj = []
    xijt_multi_pij_duration = []
    for j in range(len(input_machines)):
        for t in range(int(l)):
            t_multi_xijt_forall_tj.append(t*xijt[i][j][t]) # Start time
            xijt_multi_pij_duration.append(xijt[i][j][t]*pij_duration.iloc[i,j]) # Duration
    model.Add(cmax >= sum(np.array(t_multi_xijt_forall_tj).flatten()) + sum(np.array(xijt_multi_pij_duration).flatten()))
print(num_const7, "constraints \n")

# Solve
print(">>>>>>>>>> SOLVING <<<<<<<<<<")
solver = cp_model.CpSolver() # Define solver

if time_limit > 0:
    solver.parameters.max_time_in_seconds = time_limit/1000 # Set time limit

# if maximum_gap_allowed > 0: # Not available on CP-SAT
#     parameters.RELATIVE_MIP_GAP = maximum_gap_allowed # Set maximum GAP # Not available on CP-SAT

solver.parameters.num_search_workers = num_threads # Use multi threading / cores
status = solver.Solve(model) # Solve
status = solver.StatusName(status) # Get status
num_const_total = num_const1 + num_const2 + num_const3 + num_const4 + num_const5 + num_const6 + num_const7 # Total number of constraints

####################################################################################################
####################################################################################################
# 3 - Generate DF with results (sheet1 = stats, sheet2 = allocation) and print results
# Create output matrices
mip_stats_output = [] # Stats
for i in range(1):
    dim1 = []
    for col_num in range(15): # 15 stats to be reported
         dim1.append('')
    mip_stats_output.append(dim1)

mip_allocation_output=[] # Allocation
for i in range(len(input_tasks)):
    dim1 = []
    for col_num in range(6): # 'IDNUM', 'MACHINE', 'START xij', 'DURATION pij', 'Finish' , 'PROFIT'
        dim1.append('')
    mip_allocation_output.append(dim1)

# Populate 2D matrices with solution values
# Populate allocation
row_num = 0 # Row index
for i in range(len(input_tasks)):
    mip_allocation_output[row_num][0] = i # IDNUM of task
    for j in range(len(input_machines)):
        for t in range(int(l)):
            if solver.Value(xijt[i][j][t]) == 1: # Only add to DF results, skip blank values
                mip_allocation_output[row_num][1] = t # Start
                mip_allocation_output[row_num][2] = t + pij_duration.iloc[i,j] # Finish
                mip_allocation_output[row_num][3] = j # Machine
                mip_allocation_output[row_num][4] = pij_duration.iloc[i,j] # Duration (pij)
                mip_allocation_output[row_num][5] = tasks_profit_matrix.iloc[i,j] # Profit
    row_num += 1 # Add an extra row at end of 2D output matrix

# Create output DF for Allocation
mip_allocation_output = np.array(mip_allocation_output) # Move to np array to export directly to pandas DF
mip_allocation_output_df = pd.DataFrame(mip_allocation_output)
mip_allocation_output_df.columns = ['Task', 'Start', 'Finish', 'Machine', 'Duration (pij)', 'Profit'] # Rename columns

# Calculate NPV and GAP (needs mip_allocation_output ready)
npv = 0
if status in ("OPTIMAL", "FEASIBLE"):
    for i in range(len(input_tasks)):
        for j in range(len(input_machines)):
            for t in range(int(l)):
                if solver.Value(xijt[i][j][t]) == 1:
                    npv += tasks_profit.iloc[i,0]/((1+day_disc_rate)**t)
    result_makespan = pd.to_numeric(mip_allocation_output_df['Finish']).max() # Calculate makespan based on mip_output_df
    npv += - result_makespan*costs_period # Period costs subtracting NPV
    
    result_gap = abs(solver.BestObjectiveBound() - solver.ObjectiveValue())/solver.ObjectiveValue() # GAP = (BEST BOUND - OBJECTIVE)/ OBJECTIVE
    
# Populate stats
mip_stats_output[0][0] = inputfile # Input xlsx file used
mip_stats_output[0][1] = len(input_tasks) # Number of tasks
mip_stats_output[0][2] = len(input_machines) # Number of machines
mip_stats_output[0][3] = l_input # l value used as input
mip_stats_output[0][4] = "CP-SAT" # Solver: CBC; CP-SAT; CPLEX
mip_stats_output[0][5] = "xijt" # Model: xi_makespan; xijt; xi_profit
mip_stats_output[0][6] = num_var_total
mip_stats_output[0][7] = num_const_total
mip_stats_output[0][8] = time_limit/1000/3600 # Time limit in hours
mip_stats_output[0][9] = solver.WallTime()/3600 # Wall time in hours
mip_stats_output[0][10] = solver.ObjectiveValue() # Optimized value
mip_stats_output[0][11] = solver.BestObjectiveBound() # Best bound
mip_stats_output[0][12] = result_gap # GAP
mip_stats_output[0][13] = npv # NPV
mip_stats_output[0][14] = result_makespan # Makespan

# Create output DF for Stats
mip_stats_output = np.array(mip_stats_output) # Move to np array to export directly to pandas DF
mip_stats_output_df = pd.DataFrame(mip_stats_output)
mip_stats_output_df.columns = ['Input file', 'Input tasks', 'Input machines', 'Input L (days)', 'Solver', 'Algorithm', 'Variables number', 'Constraints number', 'Time limit (hour)', 'Walltime (hour)', 'Optimized value', 'Best bound', 'GAP', 'NPV ($)', 'Makespan (days)'] # Rename columns

# Convert allocation output DF to int (so as to have number in Excel)
mip_allocation_output_df_xlsx = mip_allocation_output_df.copy()
mip_allocation_output_df_xlsx.fillna(-1,inplace=True) # Replace NAN by -1
mip_allocation_output_df_xlsx = mip_allocation_output_df_xlsx.applymap(str) # Convert object to str
mip_allocation_output_df_xlsx = mip_allocation_output_df_xlsx.apply(pd.to_numeric, errors='raise', downcast='integer').fillna(-1) # Convert str to integer

# Create output DFs as sheets
sh1 = pd.DataFrame(mip_stats_output_df)# Sheet 1 - Stats
sh2 = pd.DataFrame(mip_allocation_output_df_xlsx) # Sheet 2 - Allocation

# Output filename
outfile = curr_dir + "\\" + "Results" + "\\" + str(mip_stats_output[0][4]) + str('-') + str(mip_stats_output[0][5]) + str('-') + str(len(input_tasks)) + str('_tasks-') + str(len(input_machines)) + str('_mach-') + time.strftime("%Y_%m_%d-%H%M%S") # Solver + Algorithm + # tasks + # machines + Datetime

# Export output in Excel file
writer = pd.ExcelWriter(outfile + str('.xlsx'), engine='xlsxwriter') # Create a Pandas Excel writer using XlsxWriter as the engine
sh1_name = 'STATS' # Sheet names
sh2_name = 'ALLOCATION' # Sheet names

sh1.to_excel(writer, sheet_name=sh1_name) # Convert the dataframe to an XlsxWriter Excel object
sh2.to_excel(writer, sheet_name=sh2_name) # Convert the dataframe to an XlsxWriter Excel object

# Close the Pandas Excel writer and output the Excel file
writer.save()
print("\nResults saved as: " + outfile + str('.xlsx'))

####################################################################################################
####################################################################################################
# 4 - Plotly export (image)
mip_allocation_output_df_xlsx['Machine'] = mip_allocation_output_df_xlsx['Machine'].astype(str) # Change machine to string to have proper legend
fig = ff.create_gantt(mip_allocation_output_df_xlsx, index_col='Machine',show_colorbar=True, group_tasks=True) # Create Plotly gantt graph

# Get x axis with linear format from 0 to L*110%
fig['layout']['xaxis']['tickformat'] = '%L'
fig['layout']['xaxis']['tickvals'] = np.arange(0,l*1.1)
fig['layout']['xaxis']['ticktext'] = list(range(len(fig['layout']['xaxis']['tickvals'])))
fig.layout.xaxis.type = 'linear'
fig.layout.legend.traceorder = 'normal'

fig.write_image(outfile + str('.png'))# Save as image
print("\nImage saved as: " + outfile + str('.png'))

####################################################################################################
####################################################################################################
# 5 - Export linear model as txt
model = str(model.ModelStats()) + "\n\n" + str(solver.ResponseProto()) # CP-SAT Model stats
f = open(outfile + str('.txt'), 'w')
f.writelines(model)
f.close()
print("\nMIP model saved as: " + outfile + str('.txt'))

####################################################################################################
####################################################################################################
# 6 - Print results
# Solver results
print("\n>>>>>>>>>> RESULTS <<<<<<<<<<\n")
if status == "OPTIMAL":  # Optimal solution found
    print("Optimized value: {:,.0f}".format(solver.ObjectiveValue()).replace(',', ' '))
else:  # Optimal solution not found
    if status == "FEASIBLE":
        print("Feasible (sub-optimal solution) found: {:,.0f}".format(solver.ObjectiveValue()).replace(',', ' '))
    else:
        print("No feasible solution could be found.")
if status in ("OPTIMAL", "FEASIBLE"):
# Report solution NPV
    print("\nNPV:  {:,.0f} \n".format(npv).replace(',', ' '))
# Report other stats
    print("Best bound: {:,.0f}".format(solver.BestObjectiveBound()).replace(',', ' '))
    print("\nMakespan: %i" % result_makespan)
    print("\nTotal number of variables: %i" % num_var_total)
    print("Total number of constraints: %i" % num_const_total)
    print("Total number of branch-and-bound branches: %i \n" % solver.NumBranches())
    if time_limit > 0:
        print("Time limit: %.2f seconds" % (time_limit/1000))
    print("Runtime: %.2f seconds \n" % solver.WallTime())
#     if maximum_gap_allowed > 0: # Not available on CP-SAT
#         print("Gap limit: %.2f" % maximum_gap_allowed) # Not available on CP-SAT
    print("Gap: %.2f" % result_gap)

print(mip_allocation_output_df) # Prompt print

####################################################################################################
####################################################################################################
# 7 - Update L value
l = int(result_makespan)