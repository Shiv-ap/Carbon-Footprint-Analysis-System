# iot_carbon_integration.py
import json
import requests
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import asyncio
from typing import Dict, List, Optional

class IoTCarbonTracker:
    """
    IoT Integration for Carbon Footprint Tracking
    Supports Home Assistant, smart meters, and various IoT devices
    """
    
    def __init__(self, db_path='carbon_footprint_tracker.db'):
        self.db_path = db_path
        self.home_assistant_config = {
            'url': 'http://homeassistant.local:8123',
            'token': 'YOUR_HOME_ASSISTANT_TOKEN'
        }
        self.carbon_factors = {
            'electricity': 0.233,  # kg CO2 per kWh
            'gas': 0.184,         # kg CO2 per kWh
            'water': 0.0003       # kg CO2 per liter
        }
    
    def configure_home_assistant(self, url: str, token: str):
        """Configure Home Assistant connection"""
        self.home_assistant_config = {'url': url, 'token': token}
    
    def simulate_iot_data(self, user_id: int) -> Dict:
        """Simulate IoT device data for testing"""
        import random
        
        # Simulate various smart home devices
        devices = {
            'smart_thermostat': {
                'temperature': random.uniform(18, 24),
                'heating_power': random.uniform(0, 3),  # kW
                'energy_consumed': random.uniform(0.5, 2.5)  # kWh
            },
            'smart_meter_electricity': {
                'current_power': random.uniform(0.5, 4.0),  # kW
                'daily_consumption': random.uniform(15, 45)  # kWh
            },
            'smart_water_meter': {
                'flow_rate': random.uniform(0, 15),  # L/min
                'daily_usage': random.uniform(150, 400)  # L
            },
            'ev_charger': {
                'charging_power': random.uniform(0, 7.4),  # kW
                'energy_delivered': random.uniform(0, 50)  # kWh
            },
            'solar_panels': {
                'current_generation': random.uniform(0, 5),  # kW
                'daily_generation': random.uniform(10, 35)  # kWh
            }
        }
        
        return devices
    
    def calculate_device_carbon_footprint(self, device_data: Dict) -> Dict:
        """Calculate carbon footprint for each IoT device"""
        carbon_data = {}
        
        for device_name, readings in device_data.items():
            device_carbon = 0
            
            if 'energy_consumed' in readings:
                device_carbon += readings['energy_consumed'] * self.carbon_factors['electricity']
            elif 'daily_consumption' in readings:
                device_carbon += readings['daily_consumption'] * self.carbon_factors['electricity']
            elif 'daily_usage' in readings and 'water' in device_name:
                device_carbon += readings['daily_usage'] * self.carbon_factors['water']
            
            # Account for solar generation (negative carbon impact)
            if 'daily_generation' in readings and 'solar' in device_name:
                device_carbon -= readings['daily_generation'] * self.carbon_factors['electricity']
            
            carbon_data[device_name] = {
                'carbon_emissions': device_carbon,
                'readings': readings,
                'timestamp': datetime.now()
            }
        
        return carbon_data
    
    def store_iot_data(self, user_id: int, device_data: Dict):
        """Store IoT device data in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for device_name, data in device_data.items():
            cursor.execute(
                "INSERT INTO iot_device_data (user_id, device_type, device_name, energy_consumption, timestamp) VALUES (?, ?, ?, ?, ?)",
                (
                    user_id,
                    device_name.split('_')[1] if '_' in device_name else device_name,
                    device_name,
                    data.get('readings', {}).get('energy_consumed', 0),
                    data['timestamp']
                )
            )
        
        conn.commit()
        conn.close()
    
    def get_real_time_carbon_data(self, user_id: int) -> Dict:
        """Get real-time carbon footprint data from IoT devices"""
        # Simulate real-time data fetch
        iot_data = self.simulate_iot_data(user_id)
        carbon_data = self.calculate_device_carbon_footprint(iot_data)
        
        # Store data
        self.store_iot_data(user_id, carbon_data)
        
        # Calculate totals
        total_carbon = sum(data['carbon_emissions'] for data in carbon_data.values())
        
        return {
            'user_id': user_id,
            'timestamp': datetime.now().isoformat(),
            'total_carbon_emissions': total_carbon,
            'device_breakdown': carbon_data,
            'summary': {
                'high_carbon_devices': [
                    device for device, data in carbon_data.items() 
                    if data['carbon_emissions'] > 1.0
                ],
                'carbon_saving_devices': [
                    device for device, data in carbon_data.items() 
                    if data['carbon_emissions'] < 0
                ]
            }
        }
    
    def create_home_assistant_sensors(self) -> Dict:
        """Generate Home Assistant sensor configurations for carbon tracking"""
        sensors = {
            'template': {
                'sensor': [
                    {
                        'name': 'Daily Carbon Footprint',
                        'unique_id': 'daily_carbon_footprint',
                        'unit_of_measurement': 'kg CO2',
                        'state_class': 'total_increasing',
                        'device_class': 'carbon_dioxide',
                        'state': '{{ (states("sensor.daily_energy_kwh") | float * 0.233) | round(2) }}'
                    },
                    {
                        'name': 'Hourly Carbon Rate',
                        'unique_id': 'hourly_carbon_rate', 
                        'unit_of_measurement': 'kg CO2/h',
                        'state': '{{ (states("sensor.current_power_kw") | float * 0.233) | round(3) }}'
                    },
                    {
                        'name': 'Transport Carbon Today',
                        'unique_id': 'transport_carbon_today',
                        'unit_of_measurement': 'kg CO2',
                        'state': '{{ (states("input_number.daily_km_driven") | float * 0.171) | round(2) }}'
                    }
                ]
            },
            'automation': [
                {
                    'alias': 'Log Daily Carbon Footprint',
                    'trigger': {
                        'platform': 'time',
                        'at': '23:59:00'
                    },
                    'action': {
                        'service': 'notify.persistent_notification',
                        'data': {
                            'message': 'Daily carbon footprint: {{ states("sensor.daily_carbon_footprint") }} kg CO2'
                        }
                    }
                }
            ]
        }
        
        return sensors
    
    def integrate_with_google_fit(self, api_key: str) -> Dict:
        """Simulate Google Fit API integration for activity tracking"""
        # This would normally use the Google Fit API
        # For demo purposes, we'll simulate the data structure
        
        fitness_data = {
            'steps': 8500,
            'cycling_minutes': 30,
            'walking_minutes': 45,
            'driving_minutes': 60
        }
        
        # Calculate carbon impact of activities
        # Walking/cycling reduces carbon footprint (alternative to driving)
        active_minutes = fitness_data['cycling_minutes'] + fitness_data['walking_minutes']
        carbon_saved = (active_minutes / 60) * 5 * 0.171  # Assuming 5km/h average, petrol car factor
        
        # Driving increases carbon footprint
        driving_distance = (fitness_data['driving_minutes'] / 60) * 30  # Assuming 30km/h average
        carbon_emitted = driving_distance * 0.171  # Petrol car factor
        
        return {
            'fitness_data': fitness_data,
            'carbon_saved': carbon_saved,
            'carbon_emitted': carbon_emitted,
            'net_transport_carbon': carbon_emitted - carbon_saved
        }

def demo_iot_integration():
    """Demonstrate IoT integration capabilities"""
    tracker = IoTCarbonTracker()
    
    print("=== IoT Carbon Footprint Tracker Demo ===")
    
    # Simulate real-time data collection
    user_id = 1
    real_time_data = tracker.get_real_time_carbon_data(user_id)
    
    print(f"Real-time carbon data for User {user_id}:")
    print(f"Total emissions: {real_time_data['total_carbon_emissions']:.3f} kg CO2")
    print(f"High carbon devices: {real_time_data['summary']['high_carbon_devices']}")
    print(f"Carbon saving devices: {real_time_data['summary']['carbon_saving_devices']}")
    
    # Generate Home Assistant configuration
    ha_config = tracker.create_home_assistant_sensors()
    print(f"Home Assistant sensors configured: {len(ha_config['template']['sensor'])}")
    
    # Simulate Google Fit integration
    fit_data = tracker.integrate_with_google_fit('demo_api_key')
    print(f"Google Fit integration:")
    print(f"Carbon saved from active transport: {fit_data['carbon_saved']:.3f} kg CO2")
    print(f"Carbon from driving: {fit_data['carbon_emitted']:.3f} kg CO2")
    
    return tracker

if __name__ == "__main__":
    demo_iot_integration()
