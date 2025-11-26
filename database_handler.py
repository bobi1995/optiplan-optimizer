import pymssql
from typing import List, Dict, Any, Tuple

# --- CONFIGURATION ---
SERVER = '192.168.1.187'
PORT = 51309
DATABASE = 'PreactorDemo' # Primary connection (cross-db queries work from here)
USER = 'sa'
PASSWORD = 'evrista_pass359'

def get_data() -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]]:
    """
    Fetches Orders/BOM from [PreactorDemo] and Resources from [BycicleDemo].
    """
    conn = None
    try:
        print(f"   > Connecting to SQL Server on {SERVER}...")
        conn = pymssql.connect(server=SERVER, port=PORT, user=USER, password=PASSWORD, database=DATABASE)
        cursor = conn.cursor(as_dict=True)

        # 1. FETCH RAW ORDERS (Still from PreactorDemo as per previous context)
        print("   > Fetching Orders_Raw (Source)...")
        cursor.execute("""
            SELECT 
                OrdersId, BelongsToOrderNo, OrderNo, DueDate, 
                ResourceGroup, OpTimePerItem, TotalSetupTime, 
                TotalProcessTime, Quantity, OperationName, OpNo,
                EarliestStartDate, DemandDate,
                PartNo, Product  
            FROM [PreactorDemo].[dbo].[Orders_Raw]
        """)
        orders = list(cursor)

        # 2. FETCH BOM (Still from PreactorDemo)
        print("   > Fetching BillOfMaterials (Source)...")
        cursor.execute("""
            SELECT 
                BillOfMaterialsId, BelongsToBOM, OrderNo, OrderPartNo, 
                OpNo, RequiredPartNo, RequiredQuantity, OperationName
            FROM [PreactorDemo].[UserData].[BillOfMaterials]
        """)
        bom = list(cursor)

        # --- NEW: FETCH RESOURCES FROM [BycicleDemo] ---

        # 3. FETCH RESOURCE GROUPS
        # Mapping: id -> ResourceGroupsId, name -> Name
        print("   > Fetching Resource_groups (BicycleDemo)...")
        cursor.execute("""
            SELECT 
                id AS ResourceGroupsId, 
                name AS Name 
            FROM [BicycleDemo].[dbo].[Resource_groups]
        """)
        groups = list(cursor)

        # 4. FETCH RESOURCES
        # Mapping: id -> ResourcesId, name -> Name
        print("   > Fetching Resources (BicycleDemo)...")
        cursor.execute("""
            SELECT 
                id AS ResourcesId, 
                name AS Name 
            FROM [BicycleDemo].[dbo].[Resources]
        """)
        resources = list(cursor)

        # 5. FETCH MAPPINGS (RELATIONSHIPS)
        # Mapping: resource_group_id -> ResourceGroupsId, resource_id -> ResourcesId
        print("   > Fetching REL_Resource_group (BicycleDemo)...")
        cursor.execute("""
            SELECT 
                resource_group_id AS ResourceGroupsId, 
                resource_id AS ResourcesId 
            FROM [BicycleDemo].[dbo].[REL_Resource_group]
        """)
        mappings = list(cursor)

        return orders, bom, resources, groups, mappings

    except pymssql.Error as ex:
        print(f"--- DATABASE ERROR --- : {ex}")
        raise ex 
    finally:
        if conn: conn.close()