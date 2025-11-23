import datetime
import collections
import time
import database_handler
import visualize_schedule
from ortools.sat.python import cp_model

# --- CONFIGURATION ---
SHIFT_START_HOUR = 8
SHIFT_START_MIN = 0
SHIFT_END_HOUR = 16
SHIFT_END_MIN = 30

# Solver Settings
SOLVER_TIME_LIMIT_SECONDS = 60.0 

# Strategy Settings
# True = Pull tasks to start ASAP (Remove gaps)
# False = Allow tasks to float/delay (Just-In-Time)
ENABLE_GRAVITY_STRATEGY = True 

SHIFT_DURATION_MINUTES = (SHIFT_END_HOUR * 60 + SHIFT_END_MIN) - (SHIFT_START_HOUR * 60 + SHIFT_START_MIN)

def convert_days_to_working_minutes(days_float):
    if days_float is None: return 0
    return int(float(days_float) * 1440)

def get_working_minutes_from_date(start_simulation_date, target_date):
    if not target_date: return 999999
    delta = target_date - start_simulation_date
    total_days = delta.days
    return total_days * SHIFT_DURATION_MINUTES

def working_minutes_to_real_time(start_datetime, worked_minutes):
    full_shifts = worked_minutes // SHIFT_DURATION_MINUTES
    remainder_mins = worked_minutes % SHIFT_DURATION_MINUTES
    current_date = start_datetime + datetime.timedelta(days=full_shifts)
    current_date = current_date.replace(hour=SHIFT_START_HOUR, minute=SHIFT_START_MIN, second=0)
    final_date = current_date + datetime.timedelta(minutes=remainder_mins)
    return final_date

def solve_schedule():
    start_time_perf = time.time()
    print("\n" + "="*80)
    print("🏭 PRODUCTION SCHEDULER - Final Optimized Version")
    print(f"   > Gravity Strategy: {'ENABLED (ASAP)' if ENABLE_GRAVITY_STRATEGY else 'DISABLED (JIT)'}")
    print("="*80)

    # 1. LOAD DATA
    try:
        raw_orders, bom, resources, groups, mappings = database_handler.get_data()
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    if not raw_orders:
        print("❌ Aborting: No orders returned.")
        return

    # 2. PRE-PROCESSING
    print("   > Pre-processing data...")
    res_id_to_name = {r['ResourcesId']: r['Name'] for r in resources}
    group_to_resources = collections.defaultdict(list)
    for m in mappings:
        group_to_resources[m['ResourceGroupsId']].append(m['ResourcesId'])

    ops_by_orderno = collections.defaultdict(list)
    for order in raw_orders:
        ops_by_orderno[order['OrderNo']].append(order)

    part_producers = collections.defaultdict(list)
    order_last_ops = {} 

    for order_no, ops in ops_by_orderno.items():
        sorted_ops = sorted(ops, key=lambda x: x.get('OpNo', 0))
        if sorted_ops:
            last_op = sorted_ops[-1]
            order_last_ops[order_no] = last_op['OrdersId']
            for row in bom:
                if row['OrderNo'] == order_no and row['OrderPartNo']:
                    part_name = row['OrderPartNo']
                    if order_no not in part_producers[part_name]:
                        part_producers[part_name].append(order_no)

    sim_start_time = datetime.datetime.now().replace(hour=SHIFT_START_HOUR, minute=SHIFT_START_MIN, second=0, microsecond=0)
    if datetime.datetime.now().hour >= SHIFT_END_HOUR:
        sim_start_time += datetime.timedelta(days=1)

    # 3. BUILD MODEL
    print("   > Building Mathematical Model...")
    model = cp_model.CpModel()
    task_vars = {}
    horizon = 60 * SHIFT_DURATION_MINUTES 

    lateness_vars = []
    resource_usage_vars = collections.defaultdict(list)

    for order in raw_orders:
        oid = order['OrdersId']
        if oid is None: continue

        setup_mins = convert_days_to_working_minutes(order.get('TotalSetupTime', 0))
        process_mins = convert_days_to_working_minutes(order.get('TotalProcessTime', 0))
        duration = setup_mins + process_mins
        if duration < 1: duration = 1

        start_var = model.NewIntVar(0, horizon, f'start_{oid}')
        end_var = model.NewIntVar(0, horizon, f'end_{oid}')
        interval_var = model.NewIntervalVar(start_var, duration, end_var, f'interval_{oid}')

        task_vars[oid] = {
            'start': start_var,
            'end': end_var,
            'interval': interval_var,
            'data': order,
            'resource_selections': []
        }

        if order.get('EarliestStartDate'):
            earliest_mins = get_working_minutes_from_date(sim_start_time, order['EarliestStartDate'])
            if earliest_mins > 0:
                model.Add(start_var >= earliest_mins)

        req_group = order['ResourceGroup']
        eligible_resources = group_to_resources.get(req_group, [])

        if eligible_resources:
            literals = []
            for res_id in eligible_resources:
                is_selected = model.NewBoolVar(f'sel_{oid}_{res_id}')
                literals.append(is_selected)
                opt_interval = model.NewOptionalIntervalVar(start_var, duration, end_var, is_selected, f'opt_{oid}_{res_id}')
                task_vars[oid]['resource_selections'].append({'res_id': res_id, 'is_selected': is_selected, 'interval': opt_interval})
                
                usage_var = model.NewIntVar(0, duration, f'usage_{oid}_{res_id}')
                model.Add(usage_var == duration).OnlyEnforceIf(is_selected)
                model.Add(usage_var == 0).OnlyEnforceIf(is_selected.Not())
                resource_usage_vars[res_id].append(usage_var)

            model.Add(sum(literals) == 1)

        if order.get('DueDate'):
            due_date_mins = get_working_minutes_from_date(sim_start_time, order['DueDate'])
            late_var = model.NewIntVar(0, horizon, f'late_{oid}')
            model.Add(late_var >= end_var - due_date_mins)
            lateness_vars.append(late_var)

    if not task_vars: return

    # 4. SEQUENCE
    for order_no, ops in ops_by_orderno.items():
        sorted_ops = sorted(ops, key=lambda x: x.get('OpNo', 0))
        for i in range(len(sorted_ops) - 1):
            curr = sorted_ops[i]['OrdersId']
            next_op = sorted_ops[i+1]['OrdersId']
            if curr in task_vars and next_op in task_vars:
                model.Add(task_vars[next_op]['start'] >= task_vars[curr]['end'])

    # 5. BOM
    for row in bom:
        parent_ord = row['OrderNo']
        part = row.get('RequiredPartNo')
        producers = part_producers.get(part, [])
        if not producers: continue
        parent_ops = ops_by_orderno.get(parent_ord, [])
        if not parent_ops: continue
        first_parent_op_id = sorted(parent_ops, key=lambda x: x.get('OpNo', 0))[0]['OrdersId']
        if first_parent_op_id in task_vars:
            for prod in producers:
                last_prod_id = order_last_ops.get(prod)
                if last_prod_id and last_prod_id in task_vars:
                    model.Add(task_vars[first_parent_op_id]['start'] >= task_vars[last_prod_id]['end'])

    # 6. NO OVERLAP
    res_intervals = collections.defaultdict(list)
    for oid, info in task_vars.items():
        for sel in info['resource_selections']:
            res_intervals[sel['res_id']].append(sel['interval'])
    for intervals in res_intervals.values():
        if len(intervals) > 1:
            model.AddNoOverlap(intervals)

    # 7. OBJECTIVE FUNCTION
    print("   > Setting Objectives...")
    
    # Objective Components
    total_lateness = model.NewIntVar(0, horizon * len(raw_orders), 'total_lateness')
    if lateness_vars: model.Add(total_lateness == sum(lateness_vars))

    all_ends = [t['end'] for t in task_vars.values()]
    makespan = model.NewIntVar(0, horizon, 'makespan')
    model.AddMaxEquality(makespan, all_ends)

    total_load_vars = []
    for res_id, usages in resource_usage_vars.items():
        load = model.NewIntVar(0, horizon, f'load_{res_id}')
        model.Add(load == sum(usages))
        total_load_vars.append(load)

    max_load = model.NewIntVar(0, horizon, 'max_load')
    load_range = model.NewIntVar(0, horizon, 'load_range')
    if total_load_vars:
        model.AddMaxEquality(max_load, total_load_vars)
        if len(total_load_vars) > 1:
            min_load = model.NewIntVar(0, horizon, 'min_load')
            model.AddMinEquality(min_load, total_load_vars)
            model.Add(load_range == max_load - min_load)

    # --- BUILD OBJECTIVE LIST ---
    objective_terms = [
        (total_lateness * 10000),   # 1. Meet Due Dates
        (makespan * 100),           # 2. Finish Fast
        (load_range * 50),          # 3. Balance Load
        (max_load * 1)              # 4. Reduce Peak Load
    ]

    # Optional Gravity
    if ENABLE_GRAVITY_STRATEGY:
        total_start_time = model.NewIntVar(0, horizon * len(raw_orders), 'total_start')
        model.Add(total_start_time == sum(t['start'] for t in task_vars.values()))
        objective_terms.append(total_start_time * 1)
        print("   > 🧲 Gravity Enabled: Pulling all tasks to start ASAP.")
    else:
        print("   > ☁️  Gravity Disabled: Tasks may float (Just-In-Time).")

    model.Minimize(sum(objective_terms))

    # 8. SOLVE
    print(f"   > 🚀 STARTING SOLVER (Max {SOLVER_TIME_LIMIT_SECONDS}s)...")
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = SOLVER_TIME_LIMIT_SECONDS
    solver.parameters.log_search_progress = False 
    solver.parameters.num_search_workers = 8
    
    status = solver.Solve(model)
    end_time_perf = time.time()

    # 9. OUTPUT SUMMARY
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        print("\n" + "="*50)
        print("✅  SCHEDULE COMPLETE")
        print("="*50)
        print(f"Status          : {solver.StatusName(status)}")
        print(f"Computation Time: {end_time_perf - start_time_perf:.2f}s")
        print(f"Total Lateness  : {solver.Value(total_lateness)} min")
        print(f"Project Length  : {solver.Value(makespan)} min")
        
        print("-" * 50)
        print(f"{'RESOURCE':<30} | {'LOAD (min)':<12} | {'UTIL %':<10}")
        print("-" * 50)
        
        output_list = []
        for oid, info in task_vars.items():
            start_val = solver.Value(info['start'])
            end_val = solver.Value(info['end'])
            
            res_name = "Unassigned"
            for sel in info['resource_selections']:
                if solver.Value(sel['is_selected']):
                    res_name = res_id_to_name.get(sel['res_id'], str(sel['res_id']))
                    break
            
            real_start = working_minutes_to_real_time(sim_start_time, start_val)
            real_end = working_minutes_to_real_time(sim_start_time, end_val)
            
            is_late = "NO"
            if info['data'].get('DueDate') and real_end > info['data']['DueDate']:
                is_late = "YES"

            output_list.append({
                'OrderNo': info['data']['OrderNo'],
                'OpNo': info['data'].get('OpNo', 0),
                'OpName': info['data']['OperationName'],
                'ResourceName': res_name,
                'StartTime': real_start.strftime('%Y-%m-%d %H:%M'),
                'EndTime': real_end.strftime('%Y-%m-%d %H:%M'),
                'IsLate': is_late,
                'Color': info['data']['OrderNo']
            })

        # Print resource stats
        makespan_val = solver.Value(makespan)
        for res_id, usages in resource_usage_vars.items():
            total_load = sum(solver.Value(u) for u in usages)
            res_name = res_id_to_name.get(res_id, str(res_id))
            pct = (total_load / makespan_val * 100) if makespan_val > 0 else 0
            print(f"{res_name:<30} | {total_load:<12} | {pct:.1f}%")

        print("\n   > Generating chart...")
        visualize_schedule.create_gantt_chart(output_list)
    else:
        print("❌ No solution found within the time limit.")

if __name__ == "__main__":
    solve_schedule()