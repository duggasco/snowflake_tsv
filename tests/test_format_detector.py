"""
Tests for format detection module
"""

import pytest
import tempfile
import gzip
from pathlib import Path
from snowflake_etl.utils.format_detector import FormatDetector


class TestFormatDetector:
    """Test suite for FormatDetector class"""
    
    def test_detect_csv_by_extension(self, tmp_path):
        """Test CSV detection by file extension"""
        # Create a CSV file
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("name,age,city\nJohn,30,NYC\nJane,25,LA")
        
        result = FormatDetector.detect_format(str(csv_file))
        
        assert result['format'] == 'CSV'
        assert result['delimiter'] == ','
        assert result['has_header'] == True
        assert result['confidence'] >= 0.9
        assert result['method'] == 'extension'
    
    def test_detect_tsv_by_extension(self, tmp_path):
        """Test TSV detection by file extension"""
        # Create a TSV file
        tsv_file = tmp_path / "test.tsv"
        tsv_file.write_text("name\tage\tcity\nJohn\t30\tNYC\nJane\t25\tLA")
        
        result = FormatDetector.detect_format(str(tsv_file))
        
        assert result['format'] == 'TSV'
        assert result['delimiter'] == '\t'
        assert result['has_header'] == True
        assert result['confidence'] >= 0.9
        assert result['method'] == 'extension'
    
    def test_detect_csv_by_content(self, tmp_path):
        """Test CSV detection by content analysis for .txt file"""
        # Create a CSV-formatted .txt file
        txt_file = tmp_path / "data.txt"
        txt_file.write_text("id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300")
        
        result = FormatDetector.detect_format(str(txt_file))
        
        assert result['format'] == 'CSV'
        assert result['delimiter'] == ','
        assert result['method'] == 'content_analysis'
    
    def test_detect_pipe_delimiter(self, tmp_path):
        """Test detection of pipe-delimited file"""
        # Create a pipe-delimited file
        txt_file = tmp_path / "data.txt"
        txt_file.write_text("id|name|value\n1|Alice|100\n2|Bob|200\n3|Charlie|300")
        
        result = FormatDetector.detect_format(str(txt_file))
        
        assert result['delimiter'] == '|'
        assert result['format'] == 'CSV'  # Non-tab delimiters are classified as CSV
        assert result['method'] == 'content_analysis'
    
    def test_detect_semicolon_delimiter(self, tmp_path):
        """Test detection of semicolon-delimited file"""
        # Create a semicolon-delimited file
        txt_file = tmp_path / "data.txt"
        txt_file.write_text("id;name;value\n1;Alice;100\n2;Bob;200\n3;Charlie;300")
        
        result = FormatDetector.detect_format(str(txt_file))
        
        assert result['delimiter'] == ';'
        assert result['format'] == 'CSV'
        assert result['method'] == 'content_analysis'
    
    def test_compressed_csv_detection(self, tmp_path):
        """Test detection of compressed CSV file"""
        # Create a compressed CSV file
        csv_content = b"name,age,city\nJohn,30,NYC\nJane,25,LA"
        gz_file = tmp_path / "test.csv.gz"
        
        with gzip.open(gz_file, 'wb') as f:
            f.write(csv_content)
        
        result = FormatDetector.detect_format(str(gz_file))
        
        assert result['format'] == 'CSV'
        assert result['delimiter'] == ','
        assert result['confidence'] >= 0.9
    
    def test_validate_delimiter_correct(self, tmp_path):
        """Test delimiter validation with correct delimiter"""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b,c\n1,2,3\n4,5,6")
        
        assert FormatDetector.validate_delimiter(str(csv_file), ',') == True
        assert FormatDetector.validate_delimiter(str(csv_file), '\t') == False
    
    def test_validate_delimiter_inconsistent(self, tmp_path):
        """Test delimiter validation with inconsistent delimiters"""
        bad_file = tmp_path / "bad.csv"
        bad_file.write_text("a,b,c\n1,2\n4,5,6,7,8")  # Inconsistent columns
        
        # Should still return True as delimiter exists, but confidence would be lower
        assert FormatDetector.validate_delimiter(str(bad_file), ',') == True
    
    def test_detect_header_with_text(self, tmp_path):
        """Test header detection with text headers"""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Name,Age,Salary\nJohn,30,50000\nJane,25,45000")
        
        has_header = FormatDetector._detect_header(csv_file, ',')
        assert has_header == True
    
    def test_detect_header_without_header(self, tmp_path):
        """Test header detection with numeric data only"""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("1,2,3\n4,5,6\n7,8,9")
        
        has_header = FormatDetector._detect_header(csv_file, ',')
        assert has_header == False
    
    def test_get_format_from_extension(self):
        """Test format detection from extension only"""
        assert FormatDetector.get_format_from_extension("data.csv") == 'CSV'
        assert FormatDetector.get_format_from_extension("data.tsv") == 'TSV'
        assert FormatDetector.get_format_from_extension("data.csv.gz") == 'CSV'
        assert FormatDetector.get_format_from_extension("data.tsv.gz") == 'TSV'
        assert FormatDetector.get_format_from_extension("data.txt") == None
        assert FormatDetector.get_format_from_extension("data.dat") == None
    
    def test_empty_file_handling(self, tmp_path):
        """Test handling of empty files"""
        empty_file = tmp_path / "empty.csv"
        empty_file.write_text("")
        
        result = FormatDetector.detect_format(str(empty_file))
        
        # Should fallback to extension-based detection
        assert result['format'] == 'CSV'
        assert result['delimiter'] == ','
        assert result['confidence'] >= 0.9
        assert result['method'] == 'extension'
    
    def test_mixed_delimiters(self, tmp_path):
        """Test file with multiple delimiters (should pick most consistent)"""
        mixed_file = tmp_path / "mixed.txt"
        # More commas than tabs, and more consistent
        mixed_file.write_text("a,b,c\td\n1,2,3\te\n4,5,6\tf")
        
        result = FormatDetector.detect_format(str(mixed_file))
        
        # Should detect comma as primary delimiter
        assert result['delimiter'] == ','
        assert result['format'] == 'CSV'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])