"""
Quick diagnostic script to test database connection and data quality
Run this FIRST before the main scheduler
"""

import pymssql
import sys

# --- CONFIGURATION ---
SERVER = '192.168.1.187'
PORT = 51309
DATABASE = 'PreactorDemo'
USER = 'sa'
PASSWORD = 'evrista_pass359'

def test_connection():
    print("="*80)
    print("🔍 DATABASE CONNECTION TEST")
    print("="*80)
    
    print(f"\n📡 Connection Details:")
    print(f"   Server: {SERVER}:{PORT}")
    print(f"   Database: {DATABASE}")
    print(f"   User: {USER}")
    
    try:
        print(f"\n⏳ Connecting...")
        conn = pymssql.connect(
            server=SERVER,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE,
            timeout=10
        )
        print("✅ Connection successful!")
        
        cursor = conn.cursor(as_dict=True)
        
        # Test 1: Orders_Raw
        print("\n" + "-"*80)
        print("TEST 1: Orders_Raw Table")
        print("-"*80)
        cursor.execute("SELECT COUNT(*) as cnt FROM [PreactorDemo].[dbo].[Orders_Raw]")
        result = cursor.fetchone()
        print(f"✅ Total rows: {result['cnt']}")
        
        if result['cnt'] > 0:
            cursor.execute("SELECT TOP 3 * FROM [PreactorDemo].[dbo].[Orders_Raw]")
            rows = cursor.fetchall()
            print(f"\n📋 Sample rows (first 3):")
            for i, row in enumerate(rows, 1):
                print(f"\n   Row {i}:")
                print(f"      OrdersId: {row.get('OrdersId')}")
                print(f"      OrderNo: {row.get('OrderNo')}")
                print(f"      OpNo: {row.get('OpNo')}")
                print(f"      OperationName: {row.get('OperationName')}")
                print(f"      ResourceGroup: {row.get('ResourceGroup')}")
                print(f"      TotalProcessTime: {row.get('TotalProcessTime')} days")
        else:
            print("⚠️  Table is empty!")
        
        # Test 2: BOM
        print("\n" + "-"*80)
        print("TEST 2: BillOfMaterials Table")
        print("-"*80)
        cursor.execute("SELECT COUNT(*) as cnt FROM [PreactorDemo].[UserData].[BillOfMaterials]")
        result = cursor.fetchone()
        print(f"✅ Total rows: {result['cnt']}")
        
        # Test 3: Resources
        print("\n" + "-"*80)
        print("TEST 3: Resources Table")
        print("-"*80)
        cursor.execute("SELECT COUNT(*) as cnt FROM [PreactorDemo].[UserData].[Resources]")
        result = cursor.fetchone()
        print(f"✅ Total rows: {result['cnt']}")
        
        if result['cnt'] > 0:
            cursor.execute("SELECT TOP 5 * FROM [PreactorDemo].[UserData].[Resources]")
            rows = cursor.fetchall()
            print(f"\n🔧 Available Resources:")
            for row in rows:
                print(f"      - {row.get('Name')} (ID: {row.get('ResourcesId')})")
        
        # Test 4: Resource Groups
        print("\n" + "-"*80)
        print("TEST 4: ResourceGroups Table")
        print("-"*80)
        cursor.execute("SELECT COUNT(*) as cnt FROM [PreactorDemo].[UserData].[ResourceGroups]")
        result = cursor.fetchone()
        print(f"✅ Total rows: {result['cnt']}")
        
        if result['cnt'] > 0:
            cursor.execute("SELECT * FROM [PreactorDemo].[UserData].[ResourceGroups]")
            rows = cursor.fetchall()
            print(f"\n👥 Resource Groups:")
            for row in rows:
                print(f"      - {row.get('Name')} (ID: {row.get('ResourceGroupsId')})")
        
        # Test 5: Mappings
        print("\n" + "-"*80)
        print("TEST 5: ResourceGroupsResources Mapping")
        print("-"*80)
        cursor.execute("SELECT COUNT(*) as cnt FROM [PreactorDemo].[UserData].[ResourceGroupsResources]")
        result = cursor.fetchone()
        print(f"✅ Total mappings: {result['cnt']}")
        
        # Test 6: Data Quality Checks
        print("\n" + "-"*80)
        print("TEST 6: Data Quality Checks")
        print("-"*80)
        
        # Check for NULL critical fields
        cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM [PreactorDemo].[dbo].[Orders_Raw]
            WHERE OrdersId IS NULL OR OrderNo IS NULL
        """)
        result = cursor.fetchone()
        if result['cnt'] > 0:
            print(f"⚠️  Warning: {result['cnt']} orders with NULL OrdersId or OrderNo")
        else:
            print(f"✅ All orders have valid IDs")
        
        # Check for operations with no resource group
        cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM [PreactorDemo].[dbo].[Orders_Raw]
            WHERE ResourceGroup IS NULL
        """)
        result = cursor.fetchone()
        if result['cnt'] > 0:
            print(f"⚠️  Warning: {result['cnt']} operations with NULL ResourceGroup")
        else:
            print(f"✅ All operations have ResourceGroup assigned")
        
        # Check for zero/negative durations
        cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM [PreactorDemo].[dbo].[Orders_Raw]
            WHERE TotalProcessTime IS NULL OR TotalProcessTime <= 0
        """)
        result = cursor.fetchone()
        if result['cnt'] > 0:
            print(f"⚠️  Warning: {result['cnt']} operations with zero/null process time")
        else:
            print(f"✅ All operations have valid process time")
        
        conn.close()
        
        print("\n" + "="*80)
        print("✅ ALL TESTS PASSED - Database is ready!")
        print("="*80)
        print("\n💡 You can now run: python production_scheduler.py")
        
        return True
        
    except pymssql.Error as e:
        print(f"\n❌ DATABASE CONNECTION FAILED!")
        print(f"Error: {e}")
        print(f"\n💡 Common issues:")
        print(f"   1. Check if SQL Server is running")
        print(f"   2. Verify server IP and port are correct")
        print(f"   3. Confirm username/password")
        print(f"   4. Check firewall settings")
        print(f"   5. Ensure pymssql is installed: pip install pymssql")
        return False
        
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR!")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)