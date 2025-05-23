import os
import csv
import glob

def read_file_with_encoding(file_path):
    """Try to read file with different encodings."""
    encodings = ['utf-8', 'latin-1', 'cp1252']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not read {file_path} with any of the attempted encodings")

def convert_codelist_file(file_path):
    """Convert a codelist file to the correct format."""
    # Read the file content with appropriate encoding
    content = read_file_with_encoding(file_path)
    
    # Parse CSV content
    rows = list(csv.reader(content.splitlines()))
    
    # Filter out empty rows and clean up
    cleaned_rows = []
    for row in rows:
        if row and any(cell.strip() for cell in row):
            # Replace 'description' with 'term' in header
            if row[0] == 'code' and row[1] == 'description':
                row[1] = 'term'
            cleaned_rows.append(row)
    
    # Write back to file with UTF-8 encoding
    with open(file_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(cleaned_rows)

def main():
    # Get all CSV files in the codelists/local directory
    codelist_files = glob.glob('codelists/local/*.csv')
    
    # Convert each file
    for file_path in codelist_files:
        print(f"Converting {file_path}...")
        try:
            convert_codelist_file(file_path)
            print(f"Done converting {file_path}")
        except Exception as e:
            print(f"Error converting {file_path}: {str(e)}")

if __name__ == "__main__":
    main() 