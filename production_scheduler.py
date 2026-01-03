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
    print("🏭 PRODUCTION SCHEDULER - Unified Orders Optimizer")
    print(f"   > Strategy: {'Pull Left (Gravity)' if ENABLE_GRAVITY_STRATEGY else 'Allow Float'}")
    print(f"   > Data Source: Unified Orders table (Single Source of Truth)")
    print("="*80)

    # 1. LOAD DATA
    try:
        # database_handler fetches from unified Orders table
        raw_orders, bom, resources, groups, mappings, order_attrs, attributes, attr_params, changeover_groups, changeover_times, changeover_data, schedules, shifts, breaks, break_shift_rel = database_handler.get_data()
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    if not raw_orders:
        print("❌ Aborting: No active orders found in database.")
        return
    
    print(f"   > Loaded {len(raw_orders)} active orders for scheduling")

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

    # 2b. BUILD SCHEDULE/SHIFT CALENDAR SYSTEM
    print("   > Building resource calendars...")
    print("   > Validating schedules and shifts...")
    
    # Map shift_id -> shift details
    shift_map = {s['ShiftId']: s for s in shifts}
    
    # Map schedule_id -> schedule details
    schedule_map = {sch['ScheduleId']: sch for sch in schedules}
    
    # Map shift_id -> list of break details
    shift_breaks = collections.defaultdict(list)
    break_map = {b['BreakId']: b for b in breaks}
    for rel in break_shift_rel:
        if rel['BreakId'] in break_map:
            shift_breaks[rel['ShiftId']].append(break_map[rel['BreakId']])
    
    # Map resource_id -> schedule_id (note: using ScheduleId from updated query)
    res_to_schedule = {r['ResourcesId']: r.get('ScheduleId') for r in resources}
    
    # Validate and display resource schedules
    print("\n" + "="*80)
    print("📅 RESOURCE SCHEDULE OVERVIEW")
    print("="*80)
    weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    for r in resources[:10]:  # Show first 10 resources
        res_id = r['ResourcesId']
        res_name = r['Name']
        schedule_id = r.get('ScheduleId')
        
        if not schedule_id or schedule_id not in schedule_map:
            print(f"{res_name:<25} | No schedule assigned (no work)")
            continue
        
        schedule = schedule_map[schedule_id]
        schedule_name = schedule.get('Name', 'Unknown')
        working_days = []
        
        for day_name in weekday_names:
            shift_id = schedule.get(day_name)
            if shift_id is not None and shift_id in shift_map:
                shift_info = shift_map[shift_id]
                working_days.append(f"{day_name[:3]}({shift_info.get('Name', 'N/A')})")
        
        if working_days:
            print(f"{res_name:<25} | Schedule: {schedule_name:<15} | Works: {', '.join(working_days)}")
        else:
            print(f"{res_name:<25} | Schedule: {schedule_name:<15} | No working days defined")
    print("="*80 + "\n")
    
    def time_to_minutes(time_obj):
        """Convert time object to minutes since midnight."""
        if time_obj is None:
            return 0
        if isinstance(time_obj, str):
            # Parse "HH:MM" or "HH:MM:SS"
            parts = time_obj.split(':')
            return int(parts[0]) * 60 + int(parts[1])
        # Assume it's a datetime.time object
        return time_obj.hour * 60 + time_obj.minute
    
    def get_resource_working_minutes_for_date(resource_id, date):
        """
        Calculate total working minutes for a resource on a specific date.
        Returns 0 if resource doesn't work that day.
        """
        schedule_id = res_to_schedule.get(resource_id)
        if not schedule_id or schedule_id not in schedule_map:
            # No schedule defined, use default (but check for NULL schedule - no work)
            if schedule_id is None:
                # Explicitly no schedule assigned - assume no work
                return 0
            return SHIFT_DURATION_MINUTES
        
        schedule = schedule_map[schedule_id]
        
        # Get weekday (0=Monday, 6=Sunday)
        weekday = date.weekday()
        weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        shift_id = schedule.get(weekday_names[weekday])
        
        if shift_id is None:
            # Resource doesn't work this day (NULL in schedule table)
            return 0
        
        if shift_id not in shift_map:
            # Shift not found, use default
            return SHIFT_DURATION_MINUTES
        
        shift = shift_map[shift_id]
        shift_start = time_to_minutes(shift['StartTime'])
        shift_end = time_to_minutes(shift['EndTime'])
        
        # Calculate gross shift duration
        gross_minutes = shift_end - shift_start
        if gross_minutes < 0:
            gross_minutes += 1440  # Handle overnight shifts
        
        # Subtract breaks
        total_break_minutes = 0
        for brk in shift_breaks.get(shift_id, []):
            break_start = time_to_minutes(brk['StartTime'])
            break_end = time_to_minutes(brk['EndTime'])
            break_duration = break_end - break_start
            if break_duration < 0:
                break_duration += 1440
            total_break_minutes += break_duration
        
        net_minutes = gross_minutes - total_break_minutes
        return max(0, net_minutes)
    
    def get_shift_start_time_for_date(resource_id, date):
        """Get the shift start time (minutes from midnight) for a resource on a date."""
        schedule_id = res_to_schedule.get(resource_id)
        if not schedule_id or schedule_id not in schedule_map:
            return SHIFT_START_HOUR * 60 + SHIFT_START_MIN
        
        schedule = schedule_map[schedule_id]
        weekday = date.weekday()
        weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        shift_id = schedule.get(weekday_names[weekday])
        
        if shift_id is None or shift_id not in shift_map:
            return SHIFT_START_HOUR * 60 + SHIFT_START_MIN
        
        shift = shift_map[shift_id]
        return time_to_minutes(shift['StartTime'])
    
    def working_minutes_to_real_time_for_resource(resource_id, start_datetime, worked_minutes):
        """
        Convert working minutes to real datetime for a specific resource,
        respecting their schedule (working days and shift hours).
        This function ensures tasks only span working days.
        """
        if worked_minutes == 0:
            # Return shift start time for the start date
            # First, find the next working day from start_datetime if it's non-working
            current_date = start_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
            while get_resource_working_minutes_for_date(resource_id, current_date) == 0:
                current_date += datetime.timedelta(days=1)
            shift_start_mins = get_shift_start_time_for_date(resource_id, current_date)
            return current_date.replace(hour=shift_start_mins // 60, minute=shift_start_mins % 60, second=0)
        
        # Start from the beginning of the start date
        current_date = start_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        remaining_minutes = worked_minutes
        
        # Skip to first working day if starting on non-working day
        while get_resource_working_minutes_for_date(resource_id, current_date) == 0:
            current_date += datetime.timedelta(days=1)
        
        # Consume working days until we've accounted for all minutes
        while remaining_minutes > 0:
            day_minutes = get_resource_working_minutes_for_date(resource_id, current_date)
            
            if day_minutes == 0:
                # Skip non-working day
                current_date += datetime.timedelta(days=1)
                continue
            
            if remaining_minutes <= day_minutes:
                # Final day - calculate exact time within this shift
                shift_start_mins = get_shift_start_time_for_date(resource_id, current_date)
                final_time = current_date.replace(
                    hour=shift_start_mins // 60, 
                    minute=shift_start_mins % 60, 
                    second=0
                ) + datetime.timedelta(minutes=remaining_minutes)
                return final_time
            else:
                # Consume entire day and move to next
                remaining_minutes -= day_minutes
                current_date += datetime.timedelta(days=1)
        
        # Fallback - should not reach here
        shift_start_mins = get_shift_start_time_for_date(resource_id, current_date)
        return current_date.replace(hour=shift_start_mins // 60, minute=shift_start_mins % 60, second=0)

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
    
    # Ensure simulation starts on a working day (check against first resource)
    if resources:
        first_res_id = resources[0]['ResourcesId']
        test_date = sim_start_time
        attempts = 0
        while get_resource_working_minutes_for_date(first_res_id, test_date) == 0 and attempts < 7:
            print(f"   > Skipping non-working day: {test_date.strftime('%A, %Y-%m-%d')}")
            test_date += datetime.timedelta(days=1)
            attempts += 1
        if attempts > 0:
            sim_start_time = test_date
    
    print(f"   > Simulation Start: {sim_start_time.strftime('%A, %Y-%m-%d %H:%M')}")

    # 3. BUILD MODEL
    print("   > Building Mathematical Model...")
    model = cp_model.CpModel()
    task_vars = {}
    
    # Calculate a more realistic horizon based on actual working days
    # Use 60 calendar days, but calculate actual working minutes for resources
    planning_days = 90
    max_working_minutes = 0
    for res_id in [r['ResourcesId'] for r in resources]:
        total_working_mins = 0
        current_date = sim_start_time
        for day in range(planning_days):
            check_date = current_date + datetime.timedelta(days=day)
            total_working_mins += get_resource_working_minutes_for_date(res_id, check_date)
        max_working_minutes = max(max_working_minutes, total_working_mins)
    
    # Use the maximum available working time across all resources as horizon
    horizon = max(max_working_minutes, 60 * SHIFT_DURATION_MINUTES)  # Fallback to old value if calculation fails
    print(f"   > Planning Horizon: {horizon} working minutes ({horizon // SHIFT_DURATION_MINUTES} equivalent shifts)") 

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

    # 6a. SEQUENCE-DEPENDENT SETUP TIMES WITH ATTRIBUTE GROUPING
    print("   > Adding changeover constraints and grouping optimization...")
    
    # Track changeover cost variables for optimization
    changeover_cost_vars = []
    
    for res_id, task_list in res_tasks.items():
        if len(task_list) < 2:
            continue  # No sequencing needed for single task
        
        # For each pair of tasks that could run on this resource
        for i, (task_i, sel_i) in enumerate(task_list):
            for j, (task_j, sel_j) in enumerate(task_list):
                if i >= j:
                    continue  # Only need one direction and avoid self
                
                # Create ordering variables: is task_i before task_j?
                i_before_j = model.NewBoolVar(f'order_{task_i}_before_{task_j}_res{res_id}')
                
                # If both tasks are on this resource, one must come before the other
                both_on_resource = model.NewBoolVar(f'both_{task_i}_{task_j}_on_{res_id}')
                model.AddBoolAnd([sel_i, sel_j]).OnlyEnforceIf(both_on_resource)
                model.AddBoolOr([sel_i.Not(), sel_j.Not()]).OnlyEnforceIf(both_on_resource.Not())
                
                # Define ordering: if i_before_j, then end_i <= start_j
                # Calculate changeover time for both directions
                changeover_i_to_j = get_changeover_time(task_i, task_j, res_id)
                changeover_j_to_i = get_changeover_time(task_j, task_i, res_id)
                
                # If i comes before j on this resource
                if changeover_i_to_j > 0:
                    changeover_mins_int = int(round(changeover_i_to_j))
                    model.Add(task_vars[task_j]['start'] >= task_vars[task_i]['end'] + changeover_mins_int).OnlyEnforceIf([both_on_resource, i_before_j])
                    
                    # Add to changeover cost (penalize attribute changes)
                    changeover_cost_var = model.NewIntVar(0, changeover_mins_int, f'cost_{task_i}_to_{task_j}_res{res_id}')
                    model.Add(changeover_cost_var == changeover_mins_int).OnlyEnforceIf([both_on_resource, i_before_j])
                    model.Add(changeover_cost_var == 0).OnlyEnforceIf([both_on_resource.Not()])
                    model.Add(changeover_cost_var == 0).OnlyEnforceIf([i_before_j.Not()])
                    changeover_cost_vars.append(changeover_cost_var)
                else:
                    model.Add(task_vars[task_j]['start'] >= task_vars[task_i]['end']).OnlyEnforceIf([both_on_resource, i_before_j])
                
                # If j comes before i on this resource
                if changeover_j_to_i > 0:
                    changeover_mins_int = int(round(changeover_j_to_i))
                    model.Add(task_vars[task_i]['start'] >= task_vars[task_j]['end'] + changeover_mins_int).OnlyEnforceIf([both_on_resource, i_before_j.Not()])
                    
                    # Add to changeover cost
                    changeover_cost_var = model.NewIntVar(0, changeover_mins_int, f'cost_{task_j}_to_{task_i}_res{res_id}')
                    model.Add(changeover_cost_var == changeover_mins_int).OnlyEnforceIf([both_on_resource, i_before_j.Not()])
                    model.Add(changeover_cost_var == 0).OnlyEnforceIf([both_on_resource.Not()])
                    model.Add(changeover_cost_var == 0).OnlyEnforceIf([i_before_j])
                    changeover_cost_vars.append(changeover_cost_var)
                else:
                    model.Add(task_vars[task_i]['start'] >= task_vars[task_j]['end']).OnlyEnforceIf([both_on_resource, i_before_j.Not()])
    
    # Calculate total changeover cost
    total_changeover_cost = model.NewIntVar(0, horizon * len(raw_orders), 'total_changeover_cost')
    if changeover_cost_vars:
        model.Add(total_changeover_cost == sum(changeover_cost_vars))
        print(f"   > Tracking {len(changeover_cost_vars)} potential changeover transitions")
    
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
    # PRIORITY ORDER:
    # 1. Meet Due Dates (Critical - Must satisfy)
    # 2. Minimize Changeovers (NEW - Group same attributes = Huge savings!)
    # 3. Finish Fast (Makespan)
    # 4. Balance Resources
    # 5. Pull tasks left (Gravity)
    objective_terms = [
        (total_lateness * 10000),      # 1. Meet Due Dates (Critical)
        (total_changeover_cost * 500), # 2. Minimize Changeovers (HIGH PRIORITY - groups same colors/attributes!)
        (makespan * 100),              # 3. Finish Project Fast
        (load_range * 50),             # 4. Balance Loads
        (max_load * 1)                 # 5. Reduce Peak Load
    ]

    if ENABLE_GRAVITY_STRATEGY:
        total_start_time = model.NewIntVar(0, horizon * len(raw_orders), 'total_start')
        model.Add(total_start_time == sum(t['start'] for t in task_vars.values()))
        objective_terms.append(total_start_time * 1) # 6. Gravity (Pull Left)

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
        print(f"Total Changeovers: {solver.Value(total_changeover_cost)} min (minimized by grouping same attributes)")
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
            
            # Use resource-aware time conversion if resource is assigned
            if res_id_assigned:
                real_start = working_minutes_to_real_time_for_resource(res_id_assigned, sim_start_time, start_val)
                real_end = working_minutes_to_real_time_for_resource(res_id_assigned, sim_start_time, end_val)
            else:
                # Fallback to default conversion
                real_start = working_minutes_to_real_time(sim_start_time, start_val)
                real_end = working_minutes_to_real_time(sim_start_time, end_val)
            
            is_late = "NO"
            if info['data'].get('DueDate') and real_end > info['data']['DueDate']:
                is_late = "YES"

            # Order Totals for DB (use resource-aware or fallback)
            order_no = info['data']['OrderNo']
            # For order totals, use the first task's resource if available
            order_start_dt = working_minutes_to_real_time(sim_start_time, order_start_end[order_no]['start'])
            order_end_dt = working_minutes_to_real_time(sim_start_time, order_start_end[order_no]['end'])
            
            # Get actual changeover time for this task
            actual_changeover_mins = task_setup_times.get(oid, 0)
            actual_changeover_days = actual_changeover_mins / 1440.0  # Convert to days

            # For Visualization - Add changeover block if exists
            if actual_changeover_mins > 0:
                changeover_start = real_start
                if res_id_assigned:
                    changeover_end = working_minutes_to_real_time_for_resource(res_id_assigned, sim_start_time, start_val + actual_changeover_mins)
                else:
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

        # 9c.1 VALIDATE: Check if tasks are scheduled on non-working days
        print("\n" + "="*80)
        print("✅ SCHEDULE VALIDATION: Checking for non-working day violations")
        print("="*80)
        violations = []
        weekday_names_short = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        
        for entry in db_save_list:
            if entry['resource_id']:
                start_date = entry['start_time']
                end_date = entry['end_time']
                res_id = entry['resource_id']
                
                # Check each day the task spans
                current_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_check = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
                
                while current_date <= end_check:
                    working_mins = get_resource_working_minutes_for_date(res_id, current_date)
                    if working_mins == 0:
                        # Task scheduled on non-working day
                        res_name = res_id_to_name.get(res_id, str(res_id))
                        violations.append({
                            'order': entry['orno'],
                            'op': entry['opno'],
                            'resource': res_name,
                            'date': current_date,
                            'day': weekday_names_short[current_date.weekday()]
                        })
                    current_date += datetime.timedelta(days=1)
        
        if violations:
            print(f"⚠️  WARNING: Found {len(violations)} task(s) scheduled on non-working days:")
            for i, v in enumerate(violations[:10]):  # Show first 10
                print(f"   {i+1}. Order {v['order']}-Op{v['op']} on {v['resource']} - {v['day']} {v['date'].strftime('%Y-%m-%d')}")
            if len(violations) > 10:
                print(f"   ... and {len(violations) - 10} more violations")
            
            # Show detailed debug for first violation
            if violations:
                first_viol = violations[0]
                print(f"\n   DEBUG - First Violation Details:")
                matching_entry = next((e for e in db_save_list if e['orno'] == first_viol['order'] and e['opno'] == first_viol['op']), None)
                if matching_entry:
                    print(f"   Task Start: {matching_entry['start_time'].strftime('%A %Y-%m-%d %H:%M')}")
                    print(f"   Task End:   {matching_entry['end_time'].strftime('%A %Y-%m-%d %H:%M')}")
                    print(f"   Resource:   {first_viol['resource']} (ID: {matching_entry['resource_id']})")
                    res_sched_id = res_to_schedule.get(matching_entry['resource_id'])
                    if res_sched_id and res_sched_id in schedule_map:
                        sched = schedule_map[res_sched_id]
                        print(f"   Schedule:   {sched.get('Name', 'Unknown')}")
                        print(f"   Working Days: Mon={sched.get('Monday')}, Tue={sched.get('Tuesday')}, Wed={sched.get('Wednesday')}, Thu={sched.get('Thursday')}, Fri={sched.get('Friday')}, Sat={sched.get('Saturday')}, Sun={sched.get('Sunday')}")
        else:
            print("✅ All tasks scheduled on valid working days!")
        print("="*80 + "\n")

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