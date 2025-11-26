import pymssql
from typing import List, Dict, Any
import database_handler 

def save_schedule(schedule_data: List[Dict[str, Any]]):
    """
    Truncates [BicycleDemo] orders and inserts new schedule with IDENTITY_INSERT ON.
    """
    conn = None
    try:
        print(f"\n   > 💾 Saving {len(schedule_data)} rows to [BicycleDemo]...")
        
        conn = pymssql.connect(
            server=database_handler.SERVER,
            port=database_handler.PORT,
            user=database_handler.USER,
            password=database_handler.PASSWORD,
            database=database_handler.DATABASE
        )
        cursor = conn.cursor()

        # 1. Clear Old Data
        cursor.execute("TRUNCATE TABLE [BicycleDemo].[dbo].[Orders]")
        
        # 2. ENABLE EXPLICIT ID INSERTION (Critical for preserving IDs)
        cursor.execute("SET IDENTITY_INSERT [BicycleDemo].[dbo].[Orders] ON")

        # 3. Insert Data
        sql = """
            INSERT INTO [BicycleDemo].[dbo].[Orders] (
                [id], [orno], [opno], 
                [start_time], [end_time], 
                [project], [duration], [task_index], 
                [part_no], [product], [op_name], 
                [remaining_quan], [setup_time], 
                [resource_id], [resource_group_id], 
                [belongs_to_order], [due_date], 
                [order_start], [order_end]
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        data_to_insert = []
        for row in schedule_data:
            data_to_insert.append((
                row['id'],              
                row['orno'],            
                row['opno'],            
                row['start_time'],      
                row['end_time'],        
                None, # project 
                row['duration'],        
                None, # task_index 
                row['part_no'],        
                row['product'],
                row['op_name'],         
                row['remaining_quan'],  
                row['setup_time'],      
                row['resource_id'],     
                row['resource_group_id'], 
                row['belongs_to_order'], 
                row['due_date'],        
                row['order_start'],     
                row['order_end']        
            ))

        cursor.executemany(sql, data_to_insert)
        
        # 4. DISABLE EXPLICIT ID INSERTION (Cleanup)
        cursor.execute("SET IDENTITY_INSERT [BicycleDemo].[dbo].[Orders] OFF")
        
        conn.commit()
        print("   ✅ Data saved successfully to SQL Server.")

    except pymssql.Error as ex:
        print(f"   ❌ DATABASE SAVE ERROR: {ex}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()