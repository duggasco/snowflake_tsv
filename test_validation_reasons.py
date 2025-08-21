#!/usr/bin/env python3
"""
Test script for improved validation failure explanations.
Shows clear reasons why validation failed even when date ranges match.
"""

import json
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_validation_failure_reasons():
    """Test different validation failure scenarios"""
    
    print("\n" + "="*60)
    print("TESTING VALIDATION FAILURE EXPLANATIONS")
    print("="*60)
    
    # Scenario 1: Anomalous dates only (date range matches)
    print("\n" + "-"*40)
    print("Scenario 1: Date range matches but has anomalies")
    print("-"*40)
    
    mock_result1 = {
        'valid': False,
        'failure_reasons': [
            '3 date(s) with critically low row counts (<10% of average)'
        ],
        'table_name': 'TEST_TABLE_1',
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
            'anomalous_dates_count': 3
        },
        'anomalous_dates': [
            {'date': '2024-01-05', 'count': 12, 'severity': 'SEVERELY_LOW', 'percent_of_avg': 0.025},
            {'date': '2024-01-15', 'count': 100, 'severity': 'SEVERELY_LOW', 'percent_of_avg': 0.2},
            {'date': '2024-01-22', 'count': 500, 'severity': 'SEVERELY_LOW', 'percent_of_avg': 1.0}
        ]
    }
    
    display_validation_result(mock_result1)
    
    # Scenario 2: Missing dates (gaps in sequence)
    print("\n" + "-"*40)
    print("Scenario 2: Missing dates with gaps")
    print("-"*40)
    
    mock_result2 = {
        'valid': False,
        'failure_reasons': [
            'Missing 3 dates (found 28 of 31 expected)',
            'Found 2 gap(s) in date sequence'
        ],
        'table_name': 'TEST_TABLE_2',
        'date_range': {
            'requested_start': '2024-01-01',
            'requested_end': '2024-01-31',
            'actual_min': '2024-01-01',
            'actual_max': '2024-01-31'
        },
        'statistics': {
            'total_rows': 1344000,
            'unique_dates': 28,
            'expected_dates': 31,
            'missing_dates': 3,
            'avg_rows_per_day': 48000
        },
        'gaps': [
            {'from': '2024-01-10', 'to': '2024-01-12', 'missing_days': 1},
            {'from': '2024-01-20', 'to': '2024-01-22', 'missing_days': 1}
        ]
    }
    
    display_validation_result(mock_result2)
    
    # Scenario 3: Multiple issues
    print("\n" + "-"*40)
    print("Scenario 3: Multiple validation issues")
    print("-"*40)
    
    mock_result3 = {
        'valid': False,
        'failure_reasons': [
            'Missing 2 dates (found 29 of 31 expected)',
            'Found 1 gap(s) in date sequence',
            '5 date(s) with anomalous row counts'
        ],
        'table_name': 'TEST_TABLE_3',
        'date_range': {
            'requested_start': '2024-01-01',
            'requested_end': '2024-01-31',
            'actual_min': '2024-01-01',
            'actual_max': '2024-01-31'
        },
        'statistics': {
            'total_rows': 1400000,
            'unique_dates': 29,
            'expected_dates': 31,
            'missing_dates': 2,
            'avg_rows_per_day': 48276
        },
        'row_count_analysis': {
            'anomalous_dates_count': 5
        }
    }
    
    display_validation_result(mock_result3)
    
    # Scenario 4: Valid result for comparison
    print("\n" + "-"*40)
    print("Scenario 4: Valid result (for comparison)")
    print("-"*40)
    
    mock_result4 = {
        'valid': True,
        'failure_reasons': [],
        'table_name': 'TEST_TABLE_4',
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
        }
    }
    
    display_validation_result(mock_result4)
    
    return True

def display_validation_result(result):
    """Display validation result with new format"""
    table_name = result.get('table_name', 'Unknown')
    print(f"\n{table_name}:")
    
    if result.get('valid'):
        print("  Status: ✓ VALID")
        if 'statistics' in result:
            stats = result['statistics']
            print(f"  Date Range: {result['date_range']['actual_min']} to {result['date_range']['actual_max']}")
            print(f"  Total Rows: {stats['total_rows']:,}")
            print(f"  Unique Dates: {stats['unique_dates']}")
            print(f"  Expected Dates: {stats['expected_dates']}")
            print(f"  Avg Rows/Day: {stats['avg_rows_per_day']:,.0f}")
    else:
        print("  Status: ✗ INVALID")
        
        # Show clear failure reasons first
        if result.get('failure_reasons'):
            print("\n  ❌ VALIDATION FAILED BECAUSE:")
            for reason in result['failure_reasons']:
                print(f"    • {reason}")
        
        if 'statistics' in result:
            stats = result['statistics']
            print(f"\n  Date Range Requested: {result['date_range']['requested_start']} to {result['date_range']['requested_end']}")
            print(f"  Date Range Found: {result['date_range']['actual_min']} to {result['date_range']['actual_max']}")
            print(f"  Total Rows: {stats['total_rows']:,}")
            print(f"  Unique Dates: {stats['unique_dates']} of {stats['expected_dates']} expected")
            if stats.get('missing_dates', 0) > 0:
                print(f"  Missing Dates: {stats['missing_dates']} completely absent")
            print(f"  Avg Rows/Day: {stats['avg_rows_per_day']:,.0f}")
            
            # Show anomalous dates if any
            if result.get('anomalous_dates'):
                print("\n  ⚠️  Anomalous Dates (sample):")
                for i, anomaly in enumerate(result['anomalous_dates'][:3], 1):
                    print(f"    {i}) {anomaly['date']} - {anomaly['count']} rows ({anomaly['percent_of_avg']:.1f}% of avg) - {anomaly['severity']}")
            
            # Show gaps if any
            if result.get('gaps'):
                print("\n  Date Gaps Found:")
                for i, gap in enumerate(result['gaps'][:3], 1):
                    print(f"    {i}) {gap['from']} to {gap['to']} ({gap['missing_days']} days missing)")

if __name__ == "__main__":
    print("Testing Improved Validation Failure Explanations")
    print("="*50)
    
    success = test_validation_failure_reasons()
    
    if success:
        print("\n" + "="*60)
        print("KEY IMPROVEMENTS")
        print("="*60)
        print("\n✓ Clear failure reasons shown first")
        print("✓ Distinguishes between 'Requested' and 'Found' date ranges")
        print("✓ Specific counts for missing dates vs anomalies")
        print("✓ Explains WHY validation failed even when ranges match")
        print("✓ More informative for troubleshooting data issues")
        
        print("\nCommon failure reasons:")
        print("• Date(s) with critically low row counts (<10% of average)")
        print("• Missing X dates (found Y of Z expected)")
        print("• Found X gap(s) in date sequence")
        print("• Date(s) with anomalous row counts")
    
    sys.exit(0 if success else 1)