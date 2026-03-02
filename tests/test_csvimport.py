import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
import pytest
from csvimport import parse_format, remove_duplicates

# Sample test for parse_format

def test_parse_format_comma():
    result = parse_format("A,B,C")
    assert result == ["A", "B", "C"]

def test_parse_format_yaml():
    result = parse_format("[A, B, C]")
    assert result == ["A", "B", "C"]

# Sample test for remove_duplicates

def test_remove_duplicates_basic():
    rows = [
        {"A": "1", "B": "x"},
        {"A": "2", "B": "y"},
        {"A": "1", "B": "x"},
    ]
    existing = [{"A": "1", "B": "x"}]
    key_columns = ["A", "B"]
    class DummyLogger:
        def debug(self, msg): pass
        def info(self, msg): pass
    deduped = remove_duplicates(rows, existing, key_columns, DummyLogger())
    assert deduped == [{"A": "2", "B": "y"}]

def test_merge_multiple_input_files(tmp_path):
    import subprocess, os, sys
    # Create two input CSVs
    file1 = tmp_path / "input1.csv"
    file2 = tmp_path / "input2.csv"
    file1.write_text("col1,col2\nA,1\nB,2\n")
    file2.write_text("col1,col2\nC,3\nD,4\n")
    output = tmp_path / "output.csv"
    # Ensure logs/ directory exists
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir(exist_ok=True)
    # Create minimal config file
    config_file = tmp_path / "test_config.conf"
    config_file.write_text("""
organizations:
  testorg:
    input_format: [col1, col2]
    output_format: [col1, col2]
    key_fields: [col1, col2]
    sheet_name: test_sheet
""")
    # Use absolute path to csvimport.py
    csvimport_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "csvimport.py"))
    cmd = [
        sys.executable, csvimport_path,
        "--input-files", f"{file1},{file2}",
        "--output", str(output),
        "--input-format", "col1,col2",
        "--output-format", "col1,col2",
        "--log-file", str(logs_dir / "csvimport.log"),
        "--config", str(config_file),
        "--org", "testorg"
    ]
    result = subprocess.run(cmd, cwd=tmp_path, capture_output=True)
    assert result.returncode == 0, result.stderr.decode()
    out_text = output.read_text()
    assert "A,1" in out_text
    assert "B,2" in out_text
    assert "C,3" in out_text
    assert "D,4" in out_text

def test_field_mismatch_exits_with_friendly_error(tmp_path):
    import subprocess, os, sys
    # CSV has an extra field not in input_format
    file1 = tmp_path / "input_extra.csv"
    file1.write_text("col1,col2,extra_field\nA,1,unexpected\n")
    output = tmp_path / "output.csv"
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir(exist_ok=True)
    config_file = tmp_path / "test_config.conf"
    config_file.write_text("""
organizations:
  testorg:
    input_format: [col1, col2]
    output_format: [col1, col2]
    key_fields: [col1, col2]
    sheet_name: test_sheet
""")
    csvimport_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "csvimport.py"))
    cmd = [
        sys.executable, csvimport_path,
        "--input-files", str(file1),
        "--output", str(output),
        "--log-file", str(logs_dir / "csvimport.log"),
        "--config", str(config_file),
        "--org", "testorg"
    ]
    result = subprocess.run(cmd, cwd=tmp_path, capture_output=True)
    assert result.returncode == 2
    stderr = result.stderr.decode()
    assert "extra_field" in stderr
    assert "input_format" in stderr or "output_format" in stderr
