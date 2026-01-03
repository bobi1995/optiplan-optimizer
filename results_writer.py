import pymssql
from typing import List, Dict, Any
import database_handler 
import datetime

def save_schedule(schedule_data: List[Dict[str, Any]]):
    """
    Updates existing Orders records with calculated schedule (start_time, end_time, resource_id, etc.).
    Now uses UPDATE strategy instead of TRUNCATE/INSERT.
    """
    conn = None
    try:
        print(f"\n   > 💾 Updating {len(schedule_data)} orders in database...")
        
        conn = pymssql.connect(
            server=database_handler.SERVER,
            port=database_handler.PORT,
            user=database_handler.USER,
            password=database_handler.PASSWORD,
            database=database_handler.DATABASE
        )
        cursor = conn.cursor()

        # Mark start of scheduling operation
        scheduled_at = datetime.datetime.now()

        # Update each order with its calculated schedule
        sql = """
            UPDATE [BicycleDemo].[dbo].[Orders]
            SET 
                [start_time] = %s,
                [end_time] = %s,
                [resource_id] = %s,
                [order_start] = %s,
                [order_end] = %s,
                [setup_time] = %s,
                [isScheduled] = 1,
                [scheduled_at] = %s
            WHERE [id] = %s
        """

        updates = []
        for row in schedule_data:
            updates.append((
                row['start_time'],      
                row['end_time'],        
                row['resource_id'],     
                row['order_start'],     
                row['order_end'],       
                row['setup_time'],      
                scheduled_at,
                row['id']               # WHERE id = ?
            ))

        # Execute batch update
        cursor.executemany(sql, updates)
        
        # Mark any orders that weren't scheduled (if they exist in DB but not in schedule_data)
        scheduled_ids = [row['id'] for row in schedule_data]
        if scheduled_ids:
            placeholders = ','.join(['%s'] * len(scheduled_ids))
            cursor.execute(f"""
                UPDATE [BicycleDemo].[dbo].[Orders]
                SET [isScheduled] = 0
                WHERE [id] NOT IN ({placeholders}) AND [isActive] = 1
            """, scheduled_ids)
        
        conn.commit()
        print(f"   ✅ Successfully updated {len(schedule_data)} orders in database.")
        print(f"   📅 Scheduled at: {scheduled_at.strftime('%Y-%m-%d %H:%M:%S')}")

    except pymssql.Error as ex:
        print(f"   ❌ DATABASE SAVE ERROR: {ex}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()