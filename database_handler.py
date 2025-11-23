import pymssql
from typing import List, Dict, Any, Tuple

# --- CONFIGURATION ---
SERVER = '192.168.1.187'
PORT = 51309
DATABASE = 'PreactorDemo'
USER = 'sa'
PASSWORD = 'evrista_pass359'

def get_data() -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]]:
    """
    Fetches all required tables for the scheduler.
    Returns: (orders, bom, resources, groups, group_mappings)
    """
    conn = None
    try:
        print(f"Connecting to {DATABASE} on {SERVER}...")
        conn = pymssql.connect(server=SERVER, port=PORT, user=USER, password=PASSWORD, database=DATABASE)
        cursor = conn.cursor(as_dict=True)

        # 1. FETCH RAW ORDERS
        # Added: OpNo, OrderStart, OrderEnd, CriticalRatio, BelongsToOrderNo
        print("Fetching Orders_Raw...")
        cursor.execute("""
            SELECT 
                OrdersId, BelongsToOrderNo, OrderNo, DueDate, 
                ResourceGroup, OpTimePerItem, TotalSetupTime, 
                TotalProcessTime, Quantity, OperationName, OpNo,
                EarliestStartDate, DemandDate
            FROM [PreactorDemo].[dbo].[Orders_Raw]
        """)
        orders = list(cursor)

        # 2. FETCH BOM
        # FIXED: Added 'OrderPartNo' which was causing the KeyError
        # Added: OpNo, OperationName to match your provided query
        print("Fetching BillOfMaterials...")
        cursor.execute("""
            SELECT 
                BillOfMaterialsId, BelongsToBOM, OrderNo, OrderPartNo, 
                OpNo, RequiredPartNo, RequiredQuantity, OperationName
            FROM [PreactorDemo].[UserData].[BillOfMaterials]
        """)
        bom = list(cursor)

        # 3. FETCH RESOURCE GROUPS
        print("Fetching ResourceGroups...")
        cursor.execute("SELECT ResourceGroupsId, Name FROM [PreactorDemo].[UserData].[ResourceGroups]")
        groups = list(cursor)

        # 4. FETCH RESOURCES
        print("Fetching Resources...")
        cursor.execute("SELECT ResourcesId, Name FROM [PreactorDemo].[UserData].[Resources]")
        resources = list(cursor)

        # 5. FETCH MAPPINGS
        print("Fetching ResourceGroupsResources...")
        cursor.execute("SELECT ResourceGroupsId, Resources as ResourcesId FROM [PreactorDemo].[UserData].[ResourceGroupsResources]")
        mappings = list(cursor)

        print(f"Data load complete. {len(orders)} ops, {len(resources)} resources loaded.")
        return orders, bom, resources, groups, mappings

    except pymssql.Error as ex:
        print(f"--- DATABASE ERROR --- : {ex}")
        return [], [], [], [], []
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    # Test run
    o, b, r, g, m = get_data()
    if o: print(f"First Order Keys: {list(o[0].keys())}")
    if b: print(f"First BOM Keys: {list(b[0].keys())}")