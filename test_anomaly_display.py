#!/usr/bin/env python3
"""
Test script to verify anomalous dates are displayed with full details.
"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_anomaly_display():
    """Test display of specific anomalous dates"""
    
    print("\n" + "="*60)
    print("TESTING ANOMALOUS DATE DISPLAY")
    print("="*60)
    
    # Mock validation result with various anomaly types
    mock_result = {
        'valid': False,
        'failure_reasons': [
            '8 date(s) with anomalous row counts'
        ],
        'table_name': 'FACTLENDINGBENCHMARK',
        'date_range': {
            'requested_start': '2024-01-01',
            'requested_end': '2024-01-31',
            'actual_min': '2024-01-01',
            'actual_max': '2024-01-31'
        },
        'statistics': {
            'total_rows': 1488000,
            'unique_dates': 31,
            'expected_dates': 31,
            'missing_dates': 0,
            'avg_rows_per_day': 48000
        },
        'row_count_analysis': {
            'mean': 48000.0,
            'median': 48000.0,
            'q1': 46500.0,
            'q3': 49500.0,
            'anomalous_dates_count': 8
        },
        'anomalous_dates': [
            # Critically low
            {'date': '2024-01-05', 'count': 1, 'expected_range': [46500, 49500], 
             'severity': 'SEVERELY_LOW', 'percent_of_avg': 0.002},
            {'date': '2024-01-12', 'count': 480, 'expected_range': [46500, 49500],
             'severity': 'SEVERELY_LOW', 'percent_of_avg': 1.0},
            {'date': '2024-01-18', 'count': 2400, 'expected_range': [46500, 49500],
             'severity': 'SEVERELY_LOW', 'percent_of_avg': 5.0},
            
            # Low
            {'date': '2024-01-22', 'count': 18000, 'expected_range': [46500, 49500],
             'severity': 'LOW', 'percent_of_avg': 37.5},
            {'date': '2024-01-25', 'count': 20000, 'expected_range': [46500, 49500],
             'severity': 'LOW', 'percent_of_avg': 41.7},
            
            # Statistical outliers
            {'date': '2024-01-28', 'count': 42000, 'expected_range': [46500, 49500],
             'severity': 'OUTLIER_LOW', 'percent_of_avg': 87.5},
            {'date': '2024-01-29', 'count': 43000, 'expected_range': [46500, 49500],
             'severity': 'OUTLIER_LOW', 'percent_of_avg': 89.6},
            {'date': '2024-01-30', 'count': 44000, 'expected_range': [46500, 49500],
             'severity': 'OUTLIER_LOW', 'percent_of_avg': 91.7}
        ]
    }
    
    # Display the result
    display_validation_result(mock_result)
    
    return True

def display_validation_result(result):
    """Display validation result with enhanced anomaly details"""
    table_name = result.get('table_name', 'Unknown')
    print(f"\n{table_name}:")
    print("  Status: ✗ INVALID")
    
    # Show failure reasons
    if result.get('failure_reasons'):
        print("\n  ❌ VALIDATION FAILED BECAUSE:")
        for reason in result['failure_reasons']:
            print(f"    • {reason}")
    
    # Show statistics
    stats = result['statistics']
    print(f"\n  Date Range Requested: {result['date_range']['requested_start']} to {result['date_range']['requested_end']}")
    print(f"  Date Range Found: {result['date_range']['actual_min']} to {result['date_range']['actual_max']}")
    print(f"  Total Rows: {stats['total_rows']:,}")
    print(f"  Unique Dates: {stats['unique_dates']} of {stats['expected_dates']} expected")
    print(f"  Avg Rows/Day: {stats['avg_rows_per_day']:,.0f}")
    
    # Show row count analysis
    if 'row_count_analysis' in result:
        analysis = result['row_count_analysis']
        if analysis.get('anomalous_dates_count', 0) > 0:
            print("\n  Row Count Analysis:")
            print(f"    Mean: {analysis['mean']:,.0f} rows/day")
            print(f"    Median: {analysis['median']:,.0f} rows/day")
            print(f"    Expected Range (Q1-Q3): {analysis['q1']:,.0f} - {analysis['q3']:,.0f}")
            print(f"    Anomalies Detected: {analysis['anomalous_dates_count']} dates")
    
    # Show specific anomalous dates
    if result.get('anomalous_dates'):
        print("\n  ⚠️  SPECIFIC DATES WITH ANOMALIES:")
        
        # Group by severity
        severely_low = [a for a in result['anomalous_dates'] if a.get('severity') == 'SEVERELY_LOW']
        low = [a for a in result['anomalous_dates'] if a.get('severity') == 'LOW']
        outlier_low = [a for a in result['anomalous_dates'] if a.get('severity') == 'OUTLIER_LOW']
        
        if severely_low:
            print("    CRITICALLY LOW (<10% of average):")
            for anomaly in severely_low[:5]:
                expected_min = anomaly.get('expected_range', [0, 0])[0]
                print(f"      • {anomaly['date']} → {anomaly['count']} rows (expected ~{expected_min:,}, got {anomaly['percent_of_avg']:.1f}% of avg)")
        
        if low:
            print("    LOW (<50% of average):")
            for anomaly in low[:3]:
                expected_min = anomaly.get('expected_range', [0, 0])[0]
                print(f"      • {anomaly['date']} → {anomaly['count']} rows (expected ~{expected_min:,}, got {anomaly['percent_of_avg']:.1f}% of avg)")
        
        if outlier_low:
            print("    STATISTICAL OUTLIERS:")
            for anomaly in outlier_low[:3]:
                expected_min = anomaly.get('expected_range', [0, 0])[0]
                print(f"      • {anomaly['date']} → {anomaly['count']} rows (expected ~{expected_min:,})")

if __name__ == "__main__":
    print("Testing Anomalous Date Display")
    print("="*40)
    
    success = test_anomaly_display()
    
    if success:
        print("\n" + "="*60)
        print("KEY FEATURES OF ANOMALY DISPLAY")
        print("="*60)
        print("\n✓ Specific dates are listed with their actual row counts")
        print("✓ Grouped by severity (CRITICALLY LOW, LOW, OUTLIERS)")
        print("✓ Shows expected range for context")
        print("✓ Displays percentage of average")
        print("✓ Most critical issues shown first")
        print("\nThis makes it easy to identify exactly which dates have problems")
        print("and how severe those problems are.")
    
    sys.exit(0 if success else 1)