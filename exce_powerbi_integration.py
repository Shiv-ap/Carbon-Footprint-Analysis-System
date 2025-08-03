# excel_powerbi_integration.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import json

class ExcelPowerBIIntegration:
    """
    Excel and Power BI integration for Carbon Footprint Dashboard
    """
    
    def __init__(self, db_path='carbon_footprint_tracker.db'):
        self.db_path = db_path
    
    def export_data_to_excel(self, filename='carbon_data_export.xlsx'):
        """Export database data to Excel for Power BI"""
        conn = sqlite3.connect(self.db_path)
        
        # Get all data with proper joins
        query = """
        SELECT 
            h.consumption_date as Date,
            h.user_id as User_ID,
            u.name as User_Name,
            h.electricity_usage as Electricity_kWh,
            h.gas_usage as Gas_kWh,
            h.water_usage as Water_L,
            h.heating_usage as Heating_kWh,
            COALESCE(t.transport_mode, 'None') as Transport_Mode,
            COALESCE(t.distance, 0) as Distance_km,
            h.carbon_emissions as Home_CO2,
            COALESCE(t.carbon_emissions, 0) as Transport_CO2,
            h.carbon_emissions + COALESCE(t.carbon_emissions, 0) as Total_CO2
        FROM home_energy h
        LEFT JOIN users u ON h.user_id = u.user_id
        LEFT JOIN transportation t ON h.user_id = t.user_id AND h.consumption_date = t.activity_date
        ORDER BY h.consumption_date DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Create Excel file with multiple sheets
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Main data sheet
            df.to_excel(writer, sheet_name='Carbon_Data', index=False)
            
            # Summary by user
            user_summary = df.groupby(['User_ID', 'User_Name']).agg({
                'Electricity_kWh': 'sum',
                'Gas_kWh': 'sum',
                'Water_L': 'sum',
                'Distance_km': 'sum',
                'Total_CO2': 'sum'
            }).reset_index()
            user_summary.to_excel(writer, sheet_name='User_Summary', index=False)
            
            # Monthly trends
            df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M')
            monthly_trends = df.groupby('Month')['Total_CO2'].sum().reset_index()
            monthly_trends['Month'] = monthly_trends['Month'].astype(str)
            monthly_trends.to_excel(writer, sheet_name='Monthly_Trends', index=False)
        
        print(f"Data exported to Excel: {filename}")
        return filename
    
    def create_excel_data_template(self, filename='carbon_footprint_template.xlsx'):
        """Create Excel template for data entry"""
        
        # Create sample data structure
        template_data = {
            'Dashboard': pd.DataFrame({
                'Metric': ['Total CO2 Emissions (kg)', 'Electricity Usage (kWh)', 'Transportation (km)', 'Water Usage (L)'],
                'Current_Month': [0, 0, 0, 0],
                'Previous_Month': [0, 0, 0, 0],
                'Target': [150, 300, 500, 8000],
                'Status': ['', '', '', '']
            }),
            'Data_Entry': pd.DataFrame({
                'Date': [datetime.now().date() - timedelta(days=i) for i in range(10)],
                'Electricity_kWh': np.random.uniform(10, 40, 10),
                'Gas_kWh': np.random.uniform(5, 25, 10),
                'Water_L': np.random.uniform(150, 400, 10),
                'Transport_Mode': ['Car'] * 10,
                'Distance_km': np.random.uniform(10, 50, 10),
                'Notes': [''] * 10
            }),
            'Carbon_Factors': pd.DataFrame({
                'Category': ['Electricity', 'Natural Gas', 'Water', 'Car Petrol', 'Public Transport'],
                'Unit': ['kWh', 'kWh', 'L', 'km', 'km'],
                'CO2_Factor_kg': [0.233, 0.184, 0.0003, 0.171, 0.041]
            })
        }
        
        # Write to Excel
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for sheet_name, data in template_data.items():
                data.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"Excel template created: {filename}")
        return filename

def demo_excel_powerbi():
    """Demonstrate Excel and Power BI integration"""
    integration = ExcelPowerBIIntegration()
    
    print("=== Excel and Power BI Integration Demo ===")
    
    # Create Excel template
    integration.create_excel_data_template()
    
    # Export data to Excel
    excel_file = integration.export_data_to_excel()
    
    print("\nFiles created for Excel and Power BI:")
    print("- carbon_footprint_template.xlsx")
    print("- carbon_data_export.xlsx") 
    
    return integration

if __name__ == "__main__":
    demo_excel_powerbi()
