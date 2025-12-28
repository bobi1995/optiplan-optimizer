import pymssql
from typing import List, Dict, Any, Tuple

# --- CONFIGURATION ---
SERVER = '192.168.1.187'
PORT = 51309
DATABASE = 'BicycleDemo' # Primary database connection
USER = 'sa'
PASSWORD = 'evrista_pass359'

def get_data() -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]]:
    """
    Fetches Orders/BOM/Resources from databases, plus Changeover configuration.
    Returns: orders, bom, resources, groups, mappings, order_attrs, attributes, attr_params, changeover_groups, changeover_times, changeover_data
    """
    conn = None
    try:
        print(f"   > Connecting to SQL Server on {SERVER}...")
        conn = pymssql.connect(server=SERVER, port=PORT, user=USER, password=PASSWORD, database=DATABASE)
        cursor = conn.cursor(as_dict=True)

        # 1. FETCH RAW ORDERS (Now from BicycleDemo)
        print("   > Fetching Orders_Raw (Source)...")
        cursor.execute("""
            SELECT 
                OrdersId, BelongsToOrderNo, OrderNo, DueDate, 
                ResourceGroup, OpTimePerItem, TotalSetupTime, 
                TotalProcessTime, Quantity, OperationName, OpNo,
                EarliestStartDate, DemandDate,
                PartNo, Product  
            FROM [BicycleDemo].[dbo].[Orders_Raw]
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

        # 3. FETCH RESOURCE GROUPS
        print("   > Fetching Resource_groups (BicycleDemo)...")
        cursor.execute("""
            SELECT 
                id AS ResourceGroupsId, 
                name AS Name 
            FROM [BicycleDemo].[dbo].[Resource_groups]
        """)
        groups = list(cursor)

        # 4. FETCH RESOURCES (with changeover_group_id and accumulative)
        print("   > Fetching Resources (BicycleDemo)...")
        cursor.execute("""
            SELECT 
                id AS ResourcesId, 
                name AS Name,
                changeover_group_id AS ChangeoverGroupId,
                accumulative AS Accumulative
            FROM [BicycleDemo].[dbo].[Resources]
        """)
        resources = list(cursor)

        # 5. FETCH MAPPINGS (RELATIONSHIPS)
        print("   > Fetching REL_Resource_group (BicycleDemo)...")
        cursor.execute("""
            SELECT 
                resource_group_id AS ResourceGroupsId, 
                resource_id AS ResourcesId 
            FROM [BicycleDemo].[dbo].[REL_Resource_group]
        """)
        mappings = list(cursor)

        # 6. FETCH ORDER ATTRIBUTES
        print("   > Fetching Orders_attr (BicycleDemo)...")
        cursor.execute("""
            SELECT 
                id AS OrderAttrId,
                orderId AS OrderId,
                attributeId AS AttributeId,
                attributeParamId AS AttributeParamId,
                value AS Value
            FROM [BicycleDemo].[dbo].[Orders_attr]
        """)
        order_attrs = list(cursor)

        # 7. FETCH ATTRIBUTES
        print("   > Fetching Attributes (BicycleDemo)...")
        cursor.execute("""
            SELECT 
                id AS AttributeId,
                name AS Name,
                isParam AS IsParam
            FROM [BicycleDemo].[dbo].[Attributes]
        """)
        attributes = list(cursor)

        # 8. FETCH ATTRIBUTE PARAMETERS
        print("   > Fetching Attributes_parameters (BicycleDemo)...")
        cursor.execute("""
            SELECT 
                id AS AttributeParamId,
                attribute_value AS AttributeValue,
                attribute_note AS AttributeNote,
                attribute_id AS AttributeId
            FROM [BicycleDemo].[dbo].[Attributes_parameters]
        """)
        attr_params = list(cursor)

        # 9. FETCH CHANGEOVER GROUPS
        print("   > Fetching Changeover_groups (BicycleDemo)...")
        cursor.execute("""
            SELECT 
                id AS ChangeoverGroupId,
                name AS Name
            FROM [BicycleDemo].[dbo].[Changeover_groups]
        """)
        changeover_groups = list(cursor)

        # 10. FETCH CHANGEOVER TIMES
        print("   > Fetching Changeover_times (BicycleDemo)...")
        cursor.execute("""
            SELECT 
                id AS ChangeoverTimeId,
                changeover_time AS ChangeoverTime,
                changeover_group_id AS ChangeoverGroupId,
                attribute_id AS AttributeId
            FROM [BicycleDemo].[dbo].[Changeover_times]
        """)
        changeover_times = list(cursor)

        # 11. FETCH CHANGEOVER DATA (Matrix)
        print("   > Fetching Changeover_data (BicycleDemo)...")
        cursor.execute("""
            SELECT 
                id AS ChangeoverDataId,
                setup_time AS SetupTime,
                changeover_group_id AS ChangeoverGroupId,
                attribute_id AS AttributeId,
                from_attr_param_id AS FromAttrParamId,
                to_attr_param_id AS ToAttrParamId
            FROM [BicycleDemo].[dbo].[Changeover_data]
        """)
        changeover_data = list(cursor)

        return orders, bom, resources, groups, mappings, order_attrs, attributes, attr_params, changeover_groups, changeover_times, changeover_data

    except pymssql.Error as ex:
        print(f"--- DATABASE ERROR --- : {ex}")
        raise ex 
    finally:
        if conn: conn.close()