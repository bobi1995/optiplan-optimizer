import datetime
import collections
import time
import database_handler
import results_writer      # <--- New import for saving to DB
import visualize_schedule
from ortools.sat.python import cp_model

# --- CONFIGURATION ---
SHIFT_START_HOUR = 8
SHIFT_START_MIN = 0
SHIFT_END_HOUR = 16
SHIFT_END_MIN = 30

# Solver Settings
SOLVER_TIME_LIMIT_SECONDS = 600.0  # Set to 600.0 for production runs

# Strategy Settings
ENABLE_GRAVITY_STRATEGY = True    # True = Pull tasks left (Compact), False = Allow float

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
    print(f"   > Gravity Strategy: {'ENABLED' if ENABLE_GRAVITY_STRATEGY else 'DISABLED'}")
    print("="*80)

    # 1. LOAD DATA
    try:
        # database_handler handles fetching raw data
        raw_orders, bom, resources, groups, mappings, order_attrs, attributes, attr_params, changeover_groups, changeover_times, changeover_data = database_handler.get_data()
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

    # 2a. BUILD ATTRIBUTE LOOKUP STRUCTURES
    print("   > Building changeover logic...")
    attr_param_map = {ap['AttributeParamId']: ap for ap in attr_params}
    attribute_map = {a['AttributeId']: a for a in attributes}
    
    # Map: OrderId -> List of AttributeParamIds
    order_to_attr_params = collections.defaultdict(list)
    for oa in order_attrs:
        order_id = oa['OrderId']
        if oa['AttributeParamId']:
            order_to_attr_params[order_id].append(oa['AttributeParamId'])
    
    # Map: ResourceId -> ChangeoverGroupId
    res_to_changeover_group = {}
    res_accumulative = {}
    for r in resources:
        if r.get('ChangeoverGroupId'):
            res_to_changeover_group[r['ResourcesId']] = r['ChangeoverGroupId']
        res_accumulative[r['ResourcesId']] = r.get('Accumulative', False)
    
    # Build Changeover Matrix: (changeover_group_id, attribute_id, from_param_id, to_param_id) -> setup_time
    changeover_matrix = {}
    for cd in changeover_data:
        key = (cd['ChangeoverGroupId'], cd['AttributeId'], cd['FromAttrParamId'], cd['ToAttrParamId'])
        # Convert to float and ensure it's a number
        setup_time = cd.get('SetupTime', 0)
        changeover_matrix[key] = float(setup_time) if setup_time is not None else 0.0
    
    # Build Changeover Times: (changeover_group_id, attribute_id) -> setup_time (when no matrix)
    changeover_standard = {}
    for ct in changeover_times:
        if ct.get('ChangeoverTime') is not None:
            key = (ct['ChangeoverGroupId'], ct['AttributeId'])
            changeover_standard[key] = float(ct['ChangeoverTime'])
    
    # Function to calculate changeover time between two orders on a resource
    def get_changeover_time(from_order_id, to_order_id, resource_id):
        """
        Calculate the changeover time when transitioning from one order to another on a resource.
        Returns time in minutes.
        """
        if resource_id not in res_to_changeover_group:
            return 0  # No changeover rules for this resource
        
        changeover_group_id = res_to_changeover_group[resource_id]
        from_params = order_to_attr_params.get(from_order_id, [])
        to_params = order_to_attr_params.get(to_order_id, [])
        
        if not from_params or not to_params:
            return 0  # No attributes defined
        
        times = []
        
        # Check all combinations of attributes
        for to_param_id in to_params:
            to_param = attr_param_map.get(to_param_id)
            if not to_param:
                continue
            
            attr_id = to_param['AttributeId']
            
            for from_param_id in from_params:
                from_param = attr_param_map.get(from_param_id)
                if not from_param or from_param['AttributeId'] != attr_id:
                    continue  # Must be same attribute type
                
                # Same value = 0 changeover
                if from_param_id == to_param_id:
                    times.append(0)
                    continue
                
                # Check matrix first
                matrix_key = (changeover_group_id, attr_id, from_param_id, to_param_id)
                if matrix_key in changeover_matrix:
                    setup_time = changeover_matrix[matrix_key]
                    if setup_time is not None:
                        times.append(float(setup_time))
                else:
                    # Check standard time
                    standard_key = (changeover_group_id, attr_id)
                    if standard_key in changeover_standard:
                        times.append(float(changeover_standard[standard_key]))
        
        if not times:
            return 0
        
        # Apply accumulative logic
        is_accumulative = res_accumulative.get(resource_id, False)
        if is_accumulative:
            return max(times)  # Take the biggest time
        else:
            return sum(times)  # Sum all times

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
    
    # Track setup time variables for each task
    setup_time_vars = {}

    for order in raw_orders:
        oid = order['OrdersId']
        if oid is None: continue

        setup_mins = convert_days_to_working_minutes(order.get('TotalSetupTime', 0))
        process_mins = convert_days_to_working_minutes(order.get('TotalProcessTime', 0))
        duration = setup_mins + process_mins
        if duration < 1: duration = 1

        start_var = model.NewIntVar(0, horizon, f'start_{oid}')
        end_var = model.NewIntVar(0, horizon, f'end_{oid}')
        
        # Create a variable for the actual setup time (will be determined by sequence)
        actual_setup_var = model.NewIntVar(0, horizon, f'setup_{oid}')
        setup_time_vars[oid] = actual_setup_var
        
        # Duration now includes variable setup time + fixed process time
        duration_var = model.NewIntVar(0, horizon, f'duration_{oid}')
        model.Add(duration_var == actual_setup_var + process_mins)
        
        interval_var = model.NewIntervalVar(start_var, duration_var, end_var, f'interval_{oid}')

        task_vars[oid] = {
            'start': start_var,
            'end': end_var,
            'interval': interval_var,
            'data': order,
            'resource_selections': [],
            'process_time': process_mins,
            'duration_var': duration_var
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
                opt_interval = model.NewOptionalIntervalVar(start_var, duration_var, end_var, is_selected, f'opt_{oid}_{res_id}')
                task_vars[oid]['resource_selections'].append({'res_id': res_id, 'is_selected': is_selected, 'interval': opt_interval})
                
                usage_var = model.NewIntVar(0, horizon, f'usage_{oid}_{res_id}')
                model.Add(usage_var == duration_var).OnlyEnforceIf(is_selected)
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
    res_tasks = collections.defaultdict(list)  # Track which tasks can run on each resource
    
    for oid, info in task_vars.items():
        for sel in info['resource_selections']:
            res_intervals[sel['res_id']].append(sel['interval'])
            res_tasks[sel['res_id']].append((oid, sel['is_selected']))
    
    for intervals in res_intervals.values():
        if len(intervals) > 1:
            model.AddNoOverlap(intervals)

    # 6a. SEQUENCE-DEPENDENT SETUP TIMES
    print("   > Adding changeover constraints...")
    for res_id, task_list in res_tasks.items():
        if len(task_list) < 2:
            continue  # No sequencing needed for single task
        
        # For each pair of tasks that could run on this resource
        for i, (task_i, sel_i) in enumerate(task_list):
            for j, (task_j, sel_j) in enumerate(task_list):
                if i == j:
                    continue
                
                # Create a literal: "task_i runs before task_j on this resource"
                precedence_lit = model.NewBoolVar(f'prec_{task_i}_before_{task_j}_on_{res_id}')
                
                # If both tasks are on this resource AND i comes before j
                both_on_resource = model.NewBoolVar(f'both_{task_i}_{task_j}_on_{res_id}')
                model.AddBoolAnd([sel_i, sel_j]).OnlyEnforceIf(both_on_resource)
                model.AddBoolOr([sel_i.Not(), sel_j.Not()]).OnlyEnforceIf(both_on_resource.Not())
                
                # If task_i ends before task_j starts, then precedence_lit = True
                model.Add(task_vars[task_j]['start'] >= task_vars[task_i]['end']).OnlyEnforceIf([both_on_resource, precedence_lit])
                
                # Calculate changeover time for this transition
                changeover_mins = get_changeover_time(task_i, task_j, res_id)
                
                # If this precedence is active, the setup time of task_j includes the changeover
                # We'll use a more sophisticated approach: track if task_j is first on resource
                
                # For simplicity, we'll add the constraint:
                # If task_i immediately precedes task_j on this resource, add changeover to task_j's setup
                if changeover_mins > 0:
                    # This is a simplified version - in production you'd want to track immediate predecessors
                    # For now, if both are on resource and i ends before j starts, add changeover buffer
                    # Convert to integer for CP-SAT
                    changeover_mins_int = int(round(changeover_mins))
                    model.Add(task_vars[task_j]['start'] >= task_vars[task_i]['end'] + changeover_mins_int).OnlyEnforceIf([both_on_resource, precedence_lit])
    
    # For now, initialize all setup times to 0 (will be refined with better sequencing logic)
    for oid in setup_time_vars:
        model.Add(setup_time_vars[oid] == 0)

    # 7. OBJECTIVE FUNCTION
    print("   > Setting Objectives...")
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

    # --- WEIGHTED OBJECTIVE ---
    objective_terms = [
        (total_lateness * 10000),   # 1. Meet Due Dates (Critical)
        (makespan * 100),           # 2. Finish Project Fast
        (load_range * 50),          # 3. Balance Loads
        (max_load * 1)              # 4. Reduce Peak Load
    ]

    if ENABLE_GRAVITY_STRATEGY:
        total_start_time = model.NewIntVar(0, horizon * len(raw_orders), 'total_start')
        model.Add(total_start_time == sum(t['start'] for t in task_vars.values()))
        objective_terms.append(total_start_time * 1) # 5. Gravity (Pull Left)

    model.Minimize(sum(objective_terms))

    # 8. SOLVE
    print(f"   > 🚀 STARTING SOLVER (Max {SOLVER_TIME_LIMIT_SECONDS}s)...")
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = SOLVER_TIME_LIMIT_SECONDS
    solver.parameters.log_search_progress = False 
    solver.parameters.num_search_workers = 8
    
    status = solver.Solve(model)
    end_time_perf = time.time()

    # 9. OUTPUT & SAVE
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        print("\n" + "="*50)
        print("✅  SCHEDULE COMPLETE")
        print("="*50)
        print(f"Status          : {solver.StatusName(status)}")
        print(f"Computation Time: {end_time_perf - start_time_perf:.2f}s")
        print(f"Total Lateness  : {solver.Value(total_lateness)} min")
        print(f"Project Length  : {solver.Value(makespan)} min")
        
        output_list = []
        db_save_list = []
        order_start_end = collections.defaultdict(lambda: {'start': float('inf'), 'end': 0})

        # 9a. First pass: Get raw solver values to calculate order totals
        for oid, info in task_vars.items():
            start_val = solver.Value(info['start'])
            end_val = solver.Value(info['end'])
            order_no = info['data']['OrderNo']
            
            order_start_end[order_no]['start'] = min(order_start_end[order_no]['start'], start_val)
            order_start_end[order_no]['end'] = max(order_start_end[order_no]['end'], end_val)

        # 9a.1 Calculate actual changeover times based on solved sequence
        print("   > Calculating sequence-dependent changeovers...")
        task_setup_times = {}  # oid -> actual setup time in minutes
        
        # Build resource schedules: resource_id -> list of (start_time, end_time, task_id)
        resource_schedules = collections.defaultdict(list)
        for oid, info in task_vars.items():
            start_val = solver.Value(info['start'])
            end_val = solver.Value(info['end'])
            for sel in info['resource_selections']:
                if solver.Value(sel['is_selected']):
                    resource_schedules[sel['res_id']].append((start_val, end_val, oid))
                    break
        
        # Sort each resource's schedule by start time
        for res_id in resource_schedules:
            resource_schedules[res_id].sort(key=lambda x: x[0])
        
        # Calculate changeover times
        for res_id, schedule in resource_schedules.items():
            for i in range(len(schedule)):
                if i == 0:
                    # First task on resource has no changeover
                    task_setup_times[schedule[i][2]] = 0
                else:
                    # Calculate changeover from previous task
                    prev_task_id = schedule[i-1][2]
                    curr_task_id = schedule[i][2]
                    changeover = get_changeover_time(prev_task_id, curr_task_id, res_id)
                    task_setup_times[curr_task_id] = changeover

        # 9b. Second pass: Build Data Lists
        for oid, info in task_vars.items():
            start_val = solver.Value(info['start'])
            end_val = solver.Value(info['end'])
            
            res_name = "Unassigned"
            res_id_assigned = None
            for sel in info['resource_selections']:
                if solver.Value(sel['is_selected']):
                    res_id_assigned = sel['res_id']
                    res_name = res_id_to_name.get(sel['res_id'], str(sel['res_id']))
                    break
            
            real_start = working_minutes_to_real_time(sim_start_time, start_val)
            real_end = working_minutes_to_real_time(sim_start_time, end_val)
            
            is_late = "NO"
            if info['data'].get('DueDate') and real_end > info['data']['DueDate']:
                is_late = "YES"

            # Order Totals for DB
            order_no = info['data']['OrderNo']
            order_start_dt = working_minutes_to_real_time(sim_start_time, order_start_end[order_no]['start'])
            order_end_dt = working_minutes_to_real_time(sim_start_time, order_start_end[order_no]['end'])
            
            # Get actual changeover time for this task
            actual_changeover_mins = task_setup_times.get(oid, 0)
            actual_changeover_days = actual_changeover_mins / 1440.0  # Convert to days

            # For Visualization - Add changeover block if exists
            if actual_changeover_mins > 0:
                changeover_start = real_start
                changeover_end = working_minutes_to_real_time(sim_start_time, start_val + actual_changeover_mins)
                
                output_list.append({
                    'OrderNo': f'CHANGEOVER',
                    'OpNo': 0,
                    'OpName': 'CHANGEOVER',
                    'ResourceName': res_name,
                    'StartTime': changeover_start.strftime('%Y-%m-%d %H:%M'),
                    'EndTime': changeover_end.strftime('%Y-%m-%d %H:%M'),
                    'IsLate': 'NO',
                    'Color': 'CHANGEOVER',
                    'ChangeoverMins': int(actual_changeover_mins)
                })
                
                # Add the actual operation block (starting after changeover)
                operation_start = changeover_end
                output_list.append({
                    'OrderNo': info['data']['OrderNo'],
                    'OpNo': info['data'].get('OpNo', 0),
                    'OpName': info['data']['OperationName'],
                    'ResourceName': res_name,
                    'StartTime': operation_start.strftime('%Y-%m-%d %H:%M'),
                    'EndTime': real_end.strftime('%Y-%m-%d %H:%M'),
                    'IsLate': is_late,
                    'Color': info['data']['OrderNo'],
                    'ChangeoverMins': 0
                })
            else:
                # No changeover, add task normally
                output_list.append({
                    'OrderNo': info['data']['OrderNo'],
                    'OpNo': info['data'].get('OpNo', 0),
                    'OpName': info['data']['OperationName'],
                    'ResourceName': res_name,
                    'StartTime': real_start.strftime('%Y-%m-%d %H:%M'),
                    'EndTime': real_end.strftime('%Y-%m-%d %H:%M'),
                    'IsLate': is_late,
                    'Color': info['data']['OrderNo'],
                    'ChangeoverMins': 0
                })

            # For Database
            db_save_list.append({
                'id': info['data']['OrdersId'],
                'orno': info['data']['OrderNo'],
                'opno': info['data']['OpNo'],
                'start_time': real_start,
                'end_time': real_end,
                'duration': info['data']['TotalProcessTime'],
                'op_name': info['data']['OperationName'],
                'remaining_quan': info['data']['Quantity'],
                'setup_time': actual_changeover_days,  # Use calculated changeover time
                'resource_id': res_id_assigned,
                'resource_group_id': info['data']['ResourceGroup'],
                'belongs_to_order': info['data']['BelongsToOrderNo'],
                'due_date': info['data']['DueDate'],
                'order_start': order_start_dt,
                'order_end': order_end_dt,
                'part_no': info['data']['PartNo'], 
                'product': info['data']['Product']
            })

        # 9c. Save to Database
        results_writer.save_schedule(db_save_list)

        # 9d. Print Stats
        print("\n" + "-" * 50)
        print(f"{'RESOURCE':<30} | {'LOAD (min)':<12} | {'UTIL %':<10}")
        print("-" * 50)
        makespan_val = solver.Value(makespan)
        for res_id, usages in resource_usage_vars.items():
            total_load = sum(solver.Value(u) for u in usages)
            res_name = res_id_to_name.get(res_id, str(res_id))
            pct = (total_load / makespan_val * 100) if makespan_val > 0 else 0
            print(f"{res_name:<30} | {total_load:<12} | {pct:.1f}%")

        # 9e. Visualize
        print("\n   > Generating visual chart...")
        visualize_schedule.create_gantt_chart(output_list)
    else:
        print("❌ No solution found within the time limit.")

if __name__ == "__main__":
    solve_schedule()