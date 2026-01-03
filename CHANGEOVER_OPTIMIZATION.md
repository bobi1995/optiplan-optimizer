# Changeover Optimization - Attribute Grouping

## Problem Statement

**Before**: The scheduler was placing orders without considering attribute grouping, resulting in inefficient sequences like:

```
Yellow → Red → Yellow → Red
  ↓       ↓       ↓
  0min   30min   30min   30min
Total changeover: 90 minutes
```

**After**: Orders with the same attributes (color, width, etc.) are grouped together:

```
Yellow → Yellow → Red → Red
  ↓        ↓       ↓       ↓
  0min    0min    30min   0min
Total changeover: 30 minutes
✅ 66% reduction in changeover time!
```

## Solution

### 1. Enhanced Changeover Tracking

The optimizer now:

- Tracks every potential changeover transition between tasks on each resource
- Calculates the exact changeover cost for each transition
- Creates decision variables for the order of tasks

### 2. High-Priority Objective

Changeover cost is now the **2nd highest priority** in the objective function:

```python
PRIORITY ORDER:
1. Meet Due Dates (10,000x weight) - Must satisfy customer requirements
2. Minimize Changeovers (500x weight) - NEW! Groups same attributes
3. Finish Fast (100x weight) - Reduce makespan
4. Balance Resources (50x weight) - Even workload distribution
5. Pull Left (1x weight) - Compact schedule
```

### 3. Bidirectional Changeover Logic

The system now considers:

- **Forward transitions**: Yellow → Red (30 min setup)
- **Reverse transitions**: Red → Yellow (30 min setup)
- **Same attribute**: Yellow → Yellow (0 min setup)

The optimizer chooses the sequence that minimizes total changeover time.

## How It Works

### Changeover Matrix Example

```
From/To    Yellow    Red    Blue
Yellow       0       30     45
Red         30        0     25
Blue        45       25      0
```

### Optimization Logic

For each resource, the optimizer:

1. Identifies all tasks that can run on that resource
2. Creates ordering variables for each pair of tasks
3. Calculates changeover cost for both possible orderings
4. Adds changeover time as spacing between tasks
5. Minimizes total changeover cost in the objective

### Example Scenario

**Orders on Resource "Painting Station":**

- Order A: Yellow (2 hours)
- Order B: Red (3 hours)
- Order C: Yellow (1 hour)
- Order D: Red (2 hours)

**Bad Sequence (not optimized):**

```
A(Yellow) → B(Red) → C(Yellow) → D(Red)
   2h    30min   3h   30min   1h   30min   2h
Total time: 8h + 90min = 9.5 hours
```

**Optimized Sequence (grouped):**

```
A(Yellow) → C(Yellow) → B(Red) → D(Red)
   2h     0min    1h   30min  3h   0min   2h
Total time: 8h + 30min = 8.5 hours
✅ 1 hour saved!
```

## Configuration

### Changeover Data

Changeovers are defined in the database:

1. **Changeover_groups** - Resource groups with changeover rules
2. **Changeover_times** - Default changeover time per attribute
3. **Changeover_data** - Specific transition matrix (From → To)

### Accumulative Logic

- **Accumulative = False**: Sum all attribute changeover times
- **Accumulative = True**: Take the maximum attribute changeover time

## Benefits

### 1. Reduced Setup Time

- Fewer attribute changes = Less downtime
- More production time = Higher throughput

### 2. Material Waste Reduction

- In painting/coating: Less paint wasted during color changes
- In cutting: Less material wasted during width changes

### 3. Improved Quality

- Fewer transitions = Less contamination risk
- Consistent runs = Better quality control

### 4. Energy Savings

- Fewer temperature/pressure adjustments
- More stable production conditions

## Output

The scheduler now shows total changeover time:

```
✅  SCHEDULE COMPLETE
==================================================
Status          : OPTIMAL
Computation Time: 45.32s
Total Lateness  : 0 min
Total Changeovers: 180 min (minimized by grouping same attributes)
Project Length  : 14,400 min
==================================================
```

## Visual Example

### Before Optimization

```
Resource: Painting Line
├─ Order 101 (Yellow) ──────── 08:00-10:00
├─ [30min changeover]
├─ Order 205 (Red) ────────── 10:30-13:30
├─ [30min changeover]
├─ Order 102 (Yellow) ──────── 14:00-15:00
├─ [30min changeover]
└─ Order 206 (Red) ────────── 15:30-17:30

Total changeover: 90 minutes
```

### After Optimization

```
Resource: Painting Line
├─ Order 101 (Yellow) ──────── 08:00-10:00
├─ [0min changeover]
├─ Order 102 (Yellow) ──────── 10:00-11:00
├─ [30min changeover]
├─ Order 205 (Red) ────────── 11:30-14:30
├─ [0min changeover]
└─ Order 206 (Red) ────────── 14:30-16:30

Total changeover: 30 minutes ✅ 60 min saved!
```

## Advanced Features

### Multi-Attribute Support

The system handles multiple attributes simultaneously:

- **Color**: Yellow, Red, Blue
- **Width**: 50mm, 75mm, 100mm
- **Thickness**: 2mm, 3mm, 5mm

Example: Order with (Yellow, 50mm, 2mm) → Order with (Yellow, 50mm, 2mm) = 0 changeover

### Changeover Matrix Priority

1. **Exact match** in Changeover_data (from_param → to_param)
2. **Attribute default** in Changeover_times
3. **No changeover** (0 minutes)

### Resource-Specific Rules

Each resource can have its own:

- Changeover group assignment
- Accumulative vs. additive logic
- Specific transition matrices

## Testing & Verification

### Check Attribute Grouping

After running the scheduler, verify grouping:

```sql
SELECT
    resource_id,
    oa.attributeId,
    ap.attribute_value,
    COUNT(*) as consecutive_count,
    MIN(o.start_time) as group_start,
    MAX(o.end_time) as group_end
FROM Orders o
INNER JOIN Orders_attr oa ON o.id = oa.orderId
INNER JOIN Attributes_parameters ap ON oa.attributeParamId = ap.id
WHERE o.isScheduled = 1
  AND o.resource_id IS NOT NULL
GROUP BY resource_id, oa.attributeId, ap.attribute_value
HAVING COUNT(*) > 1
ORDER BY resource_id, group_start;
```

### Expected Results

- Orders with same color should be sequential
- Minimal transitions between different attribute values
- Changeover time concentrated at attribute boundaries

## Troubleshooting

### Issue: Still seeing scattered attributes

**Solution**: Increase changeover cost weight in objective function

```python
(total_changeover_cost * 500)  # Try 1000 or higher
```

### Issue: Due dates being missed

**Solution**: Reduce changeover weight (due dates have priority)

```python
(total_changeover_cost * 250)  # Lower than current 500
```

### Issue: Long solve times

**Solution**:

- Reduce planning horizon
- Increase solver time limit
- Use fewer attributes for changeover

## Summary

The enhanced changeover optimization ensures that:

- ✅ Orders with identical attributes run consecutively
- ✅ Changeover time is minimized across the entire schedule
- ✅ Setup waste (time, materials, energy) is reduced
- ✅ Production efficiency is maximized
- ✅ Due dates are still respected (highest priority)

This results in **significant cost savings** and **improved production efficiency** by intelligently grouping orders with similar characteristics.
