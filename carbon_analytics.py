# carbon_analytics.py
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import json

class CarbonFootprintAnalytics:
    def __init__(self, db_path='carbon_footprint_tracker.db'):
        self.db_path = db_path
    
    def get_user_carbon_timeline(self, user_id, days=30):
        """Get time series carbon footprint data for a user"""
        conn = sqlite3.connect(self.db_path)
        
        query = f"""
        SELECT consumption_date, SUM(carbon_emissions) as daily_carbon
        FROM home_energy 
        WHERE user_id = ? 
        AND consumption_date >= date('now', '-{days} days')
        GROUP BY consumption_date
        ORDER BY consumption_date
        """
        
        df = pd.read_sql_query(query, conn, params=(user_id,))
        conn.close()
        
        return df
    
    def analyze_carbon_patterns(self, user_id):
        """Analyze carbon emission patterns for a user"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            h.consumption_date,
            h.electricity_usage,
            h.gas_usage,
            h.water_usage,
            h.heating_usage,
            h.carbon_emissions as home_emissions,
            COALESCE(t.transport_emissions, 0) as transport_emissions
        FROM home_energy h
        LEFT JOIN (
            SELECT activity_date, user_id, SUM(carbon_emissions) as transport_emissions
            FROM transportation
            GROUP BY activity_date, user_id
        ) t ON h.consumption_date = t.activity_date AND h.user_id = t.user_id
        WHERE h.user_id = ?
        ORDER BY h.consumption_date
        """
        
        df = pd.read_sql_query(query, conn, params=(user_id,))
        conn.close()
        
        # Calculate total daily emissions
        df['total_emissions'] = df['home_emissions'] + df['transport_emissions']
        
        # Add time-based features
        df['consumption_date'] = pd.to_datetime(df['consumption_date'])
        df['day_of_week'] = df['consumption_date'].dt.dayofweek
        df['month'] = df['consumption_date'].dt.month
        df['week_of_year'] = df['consumption_date'].dt.isocalendar().week
        
        return df
    
    def get_eco_heavy_activities(self, user_id, top_n=5):
        """Identify the most carbon-intensive activities for a user"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            ac.category_name,
            AVG(da.quantity) as avg_quantity,
            AVG(da.carbon_emissions) as avg_carbon,
            SUM(da.carbon_emissions) as total_carbon,
            COUNT(*) as frequency
        FROM daily_activities da
        JOIN activity_categories ac ON da.category_id = ac.category_id
        WHERE da.user_id = ?
        GROUP BY ac.category_name
        ORDER BY total_carbon DESC
        LIMIT ?
        """
        
        df = pd.read_sql_query(query, conn, params=(user_id, top_n))
        conn.close()
        
        return df
    
    def generate_optimization_suggestions(self, user_id):
        """Generate personalized eco-friendly optimization suggestions"""
        patterns_df = self.analyze_carbon_patterns(user_id)
        eco_heavy_df = self.get_eco_heavy_activities(user_id)
        
        suggestions = []
        
        # Analyze electricity usage
        avg_electricity = patterns_df['electricity_usage'].mean()
        if avg_electricity > 30:  # High electricity usage
            suggestions.append({
                'category': 'Electricity',
                'current_avg': round(avg_electricity, 2),
                'suggestion': 'Consider switching to LED bulbs and unplugging devices when not in use',
                'potential_reduction': '15-20%'
            })
        
        # Analyze transportation patterns
        avg_transport = patterns_df['transport_emissions'].mean()
        if avg_transport > 2:  # High transport emissions
            suggestions.append({
                'category': 'Transportation',
                'current_avg': round(avg_transport, 2),
                'suggestion': 'Try using public transport or cycling for short trips',
                'potential_reduction': '30-40%'
            })
        
        # Analyze seasonal patterns
        monthly_avg = patterns_df.groupby('month')['total_emissions'].mean()
        peak_months = monthly_avg.nlargest(3).index.tolist()
        
        suggestions.append({
            'category': 'Seasonal Optimization',
            'peak_months': peak_months,
            'suggestion': 'Focus on energy efficiency during high-consumption months',
            'potential_reduction': '10-15%'
        })
        
        return suggestions
    
    def calculate_carbon_trends(self, user_id, period_days=30):
        """Calculate carbon footprint trends and changes"""
        conn = sqlite3.connect(self.db_path)
        
        query = f"""
        SELECT 
            consumption_date,
            carbon_emissions + COALESCE(transport_emissions, 0) as total_daily_carbon
        FROM home_energy h
        LEFT JOIN (
            SELECT activity_date, user_id, SUM(carbon_emissions) as transport_emissions
            FROM transportation
            GROUP BY activity_date, user_id
        ) t ON h.consumption_date = t.activity_date AND h.user_id = t.user_id
        WHERE h.user_id = ?
        AND h.consumption_date >= date('now', '-{period_days * 2} days')
        ORDER BY h.consumption_date
        """
        
        df = pd.read_sql_query(query, conn, params=(user_id,))
        conn.close()
        
        if len(df) < period_days:
            return None
        
        df['consumption_date'] = pd.to_datetime(df['consumption_date'])
        
        # Split into recent and previous periods
        midpoint = len(df) // 2
        recent_period = df.iloc[midpoint:]['total_daily_carbon'].mean()
        previous_period = df.iloc[:midpoint]['total_daily_carbon'].mean()
        
        # Calculate trend
        change_percent = ((recent_period - previous_period) / previous_period) * 100
        
        return {
            'recent_avg': round(recent_period, 2),
            'previous_avg': round(previous_period, 2),
            'change_percent': round(change_percent, 2),
            'trend': 'increasing' if change_percent > 0 else 'decreasing'
        }

# Demo function
def demo_analytics():
    analytics = CarbonFootprintAnalytics()
    
    user_id = 1
    print(f"Analyzing carbon footprint for User {user_id}")
    
    # Get timeline data
    timeline = analytics.get_user_carbon_timeline(user_id, days=30)
    print(f"Timeline data (last 30 days): {len(timeline)} records")
    
    # Get eco-heavy activities
    eco_heavy = analytics.get_eco_heavy_activities(user_id)
    print(f"Top carbon-intensive activities:")
    if not eco_heavy.empty:
        print(eco_heavy[['category_name', 'total_carbon']].to_string(index=False))
    
    # Generate suggestions
    suggestions = analytics.generate_optimization_suggestions(user_id)
    print(f"Optimization suggestions: {len(suggestions)} recommendations")
    
    # Calculate trends
    trends = analytics.calculate_carbon_trends(user_id)
    if trends:
        print(f"Carbon footprint trend: {trends['trend']} by {abs(trends['change_percent']):.1f}%")
    
    return analytics

if __name__ == "__main__":
    demo_analytics()
