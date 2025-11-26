print(">>> Script is starting...")

import pymssql
import datetime
import database_handler 

def test_write():
    print("--- TESTING DATABASE WRITE (With Identity Insert) ---")
    print(f"Target: {database_handler.SERVER} -> [BycicleDemo].[dbo].[Orders]")
    
    conn = None
    try:
        conn = pymssql.connect(
            server=database_handler.SERVER,
            port=database_handler.PORT,
            user=database_handler.USER,
            password=database_handler.PASSWORD,
            database=database_handler.DATABASE
        )
        cursor = conn.cursor()

        # 1. TRUNCATE
        print("1. Attempting TRUNCATE...")
        cursor.execute("TRUNCATE TABLE [BycicleDemo].[dbo].[Orders]")
        print("   ✅ Truncate successful.")

        # 2. ALLOW EXPLICIT ID INSERTION
        print("2. Enabling IDENTITY_INSERT...")
        cursor.execute("SET IDENTITY_INSERT [BycicleDemo].[dbo].[Orders] ON")

        # 3. INSERT
        print("3. Attempting INSERT...")
        sql = """
            INSERT INTO [BycicleDemo].[dbo].[Orders] (
                [id], [orno], [opno], 
                [start_time], [end_time], 
                [project], [duration], [task_index], 
                [part_no], [product], [op_name], 
                [remaining_quan], [setup_time], 
                [resource_id], [resource_group_id], 
                [belongs_to_order], [due_date], 
                [order_start], [order_end]
            ) VALUES (
                %s, %s, %s, 
                %s, %s, 
                %s, %s, %s, 
                %s, %s, %s, 
                %s, %s, 
                %s, %s, 
                %s, %s, 
                %s, %s
            )
        """
        
        now = datetime.datetime.now()
        dummy_data = (
            99999, 'TEST-001', 10, 
            now, now, None, 60, None, 'PART-X', 'PROD-Y', 
            'Test Op', 100, 0, 1, 1, -1, now, now, now
        )

        cursor.execute(sql, dummy_data)
        
        # 4. DISABLE EXPLICIT ID INSERTION (Cleanup)
        cursor.execute("SET IDENTITY_INSERT [BycicleDemo].[dbo].[Orders] OFF")
        
        conn.commit() 
        print("   ✅ Insert & Commit successful.")
        print("\n👉 PLEASE CHECK SQL MANAGEMENT STUDIO NOW.")

    except pymssql.Error as ex:
        print(f"\n❌ DATABASE ERROR: {ex}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    test_write()