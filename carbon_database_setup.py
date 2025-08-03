# carbon_database_setup.py
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def create_carbon_database():
    """Create complete carbon footprint tracking database"""
    
    # Create the database and tables
    conn = sqlite3.connect('carbon_footprint_tracker.db')
    cursor = conn.cursor()

    # Create table for user profiles
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        household_size INTEGER,
        location TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Create table for activity categories
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS activity_categories (
        category_id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_name TEXT NOT NULL,
        unit TEXT NOT NULL,
        carbon_factor REAL NOT NULL
    )
    ''')

    # Create table for daily activities logging
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_activities (
        activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        category_id INTEGER,
        activity_date DATE NOT NULL,
        quantity REAL NOT NULL,
        carbon_emissions REAL NOT NULL,
        logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id),
        FOREIGN KEY (category_id) REFERENCES activity_categories (category_id)
    )
    ''')

    # Create table for IoT device data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS iot_device_data (
        device_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        device_type TEXT NOT NULL,
        device_name TEXT NOT NULL,
        energy_consumption REAL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')

    # Create table for transportation data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transportation (
        transport_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        transport_mode TEXT NOT NULL,
        distance REAL NOT NULL,
        fuel_type TEXT,
        carbon_emissions REAL NOT NULL,
        activity_date DATE NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')

    # Create table for home energy consumption
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS home_energy (
        energy_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        electricity_usage REAL,
        gas_usage REAL,
        water_usage REAL,
        heating_usage REAL,
        carbon_emissions REAL NOT NULL,
        consumption_date DATE NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')

    # Insert sample data for activity categories
    activity_categories = [
        ('Electricity', 'kWh', 0.233),  # kg CO2 per kWh
        ('Natural Gas', 'kWh', 0.184),  # kg CO2 per kWh
        ('Water Usage', 'L', 0.0003),   # kg CO2 per liter
        ('Car Petrol', 'km', 0.171),    # kg CO2 per km
        ('Car Diesel', 'km', 0.164),    # kg CO2 per km
        ('Public Transport', 'km', 0.041),  # kg CO2 per km
        ('Flight Domestic', 'km', 0.255),   # kg CO2 per km
        ('Flight International', 'km', 0.195),  # kg CO2 per km
        ('Food Meat', 'kg', 7.26),      # kg CO2 per kg
        ('Food Vegetables', 'kg', 0.43), # kg CO2 per kg
        ('Waste Recycling', 'kg', -0.1), # negative carbon impact
        ('Waste Landfill', 'kg', 0.8)   # kg CO2 per kg
    ]

    cursor.executemany('''
    INSERT INTO activity_categories (category_name, unit, carbon_factor) 
    VALUES (?, ?, ?)
    ''', activity_categories)

    # Insert sample user data
    sample_users = [
        ('John Doe', 'john.doe@email.com', 4, 'London, UK'),
        ('Jane Smith', 'jane.smith@email.com', 2, 'Manchester, UK'),
        ('Alice Brown', 'alice.brown@email.com', 1, 'Birmingham, UK')
    ]

    cursor.executemany('''
    INSERT INTO users (name, email, household_size, location) 
    VALUES (?, ?, ?, ?)
    ''', sample_users)

    # Generate sample time series data for the past 365 days
    start_date = datetime.now() - timedelta(days=365)
    activities_data = []
    transport_data = []
    energy_data = []

    user_ids = [1, 2, 3]

    for user_id in user_ids:
        for day in range(365):
            current_date = start_date + timedelta(days=day)
            
            # Generate daily activities (random patterns)
            for category_id in [1, 2, 3]:  # Electricity, Gas, Water
                if category_id == 1:  # Electricity
                    quantity = random.uniform(15, 45)  # kWh per day
                elif category_id == 2:  # Natural Gas
                    quantity = random.uniform(10, 30)  # kWh per day
                else:  # Water
                    quantity = random.uniform(150, 400)  # L per day
                
                # Get carbon factor
                cursor.execute('SELECT carbon_factor FROM activity_categories WHERE category_id = ?', (category_id,))
                carbon_factor = cursor.fetchone()[0]
                
                carbon_emissions = quantity * carbon_factor
                
                activities_data.append((
                    user_id, category_id, current_date.date(), 
                    quantity, carbon_emissions
                ))
            
            # Generate transportation data (some days)
            if random.random() < 0.7:  # 70% chance of transport activity
                transport_mode = random.choice(['Car Petrol', 'Car Diesel', 'Public Transport'])
                distance = random.uniform(5, 50)  # km
                
                if transport_mode == 'Car Petrol':
                    carbon_factor = 0.171
                elif transport_mode == 'Car Diesel':
                    carbon_factor = 0.164
                else:
                    carbon_factor = 0.041
                
                carbon_emissions = distance * carbon_factor
                
                transport_data.append((
                    user_id, transport_mode, distance, 
                    transport_mode.split()[1] if 'Car' in transport_mode else 'Electric',
                    carbon_emissions, current_date.date()
                ))
            
            # Generate home energy data
            electricity = random.uniform(15, 45)
            gas = random.uniform(10, 30)
            water = random.uniform(150, 400)
            heating = random.uniform(5, 25)
            
            total_carbon = (electricity * 0.233) + (gas * 0.184) + (water * 0.0003) + (heating * 0.2)
            
            energy_data.append((
                user_id, electricity, gas, water, heating, 
                total_carbon, current_date.date()
            ))

    # Insert sample data
    cursor.executemany('''
    INSERT INTO daily_activities (user_id, category_id, activity_date, quantity, carbon_emissions)
    VALUES (?, ?, ?, ?, ?)
    ''', activities_data)

    cursor.executemany('''
    INSERT INTO transportation (user_id, transport_mode, distance, fuel_type, carbon_emissions, activity_date)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', transport_data)

    cursor.executemany('''
    INSERT INTO home_energy (user_id, electricity_usage, gas_usage, water_usage, heating_usage, carbon_emissions, consumption_date)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', energy_data)

    conn.commit()
    conn.close()
    
    print("✅ Database created successfully!")
    print(f"✅ Generated {len(activities_data)} daily activity records")
    print(f"✅ Generated {len(transport_data)} transportation records")
    print(f"✅ Generated {len(energy_data)} home energy records")

if __name__ == "__main__":
    create_carbon_database()
