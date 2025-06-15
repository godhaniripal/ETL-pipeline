#!/usr/bin/env python3
"""
Convert presentation.md to PDF
"""

import markdown
import pdfkit
import os
from pathlib import Path

def convert_md_to_pdf(md_file_path, output_pdf_path):
    """Convert Markdown file to PDF"""
    
    # Read the markdown file
    with open(md_file_path, 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    # Convert markdown to HTML
    html = markdown.markdown(md_content, extensions=['tables', 'fenced_code'])
    
    # Add some CSS styling for better PDF appearance
    css_style = """
    <style>
    body {
        font-family: 'Segoe UI', Arial, sans-serif;
        line-height: 1.6;
        color: #333;
        max-width: 800px;
        margin: 0 auto;
        padding: 20px;
    }
    h1 {
        color: #2c3e50;
        border-bottom: 3px solid #3498db;
        padding-bottom: 10px;
    }
    h2 {
        color: #34495e;
        border-bottom: 2px solid #ecf0f1;
        padding-bottom: 5px;
        margin-top: 30px;
    }
    h3 {
        color: #2980b9;
        margin-top: 25px;
    }
    h4 {
        color: #27ae60;
        margin-top: 20px;
    }
    ul, ol {
        padding-left: 20px;
    }
    li {
        margin-bottom: 5px;
    }
    strong {
        color: #e74c3c;
    }
    em {
        font-style: italic;
        color: #8e44ad;
    }
    code {
        background-color: #f8f9fa;
        padding: 2px 4px;
        border-radius: 3px;
        font-family: 'Consolas', 'Monaco', monospace;
    }
    pre {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 5px;
        border-left: 4px solid #3498db;
        overflow-x: auto;
    }
    .emoji {
        font-size: 1.2em;
    }
    hr {
        border: none;
        height: 2px;
        background: linear-gradient(90deg, #3498db, #2ecc71);
        margin: 30px 0;
    }
    </style>
    """
    
    # Combine CSS and HTML
    full_html = f"<!DOCTYPE html><html><head><meta charset='UTF-8'>{css_style}</head><body>{html}</body></html>"
    
    # PDF options for better formatting
    options = {
        'page-size': 'A4',
        'margin-top': '0.75in',
        'margin-right': '0.75in',
        'margin-bottom': '0.75in',
        'margin-left': '0.75in',
        'encoding': "UTF-8",
        'no-outline': None,
        'enable-local-file-access': None
    }
    
    try:
        # Convert HTML to PDF
        pdfkit.from_string(full_html, output_pdf_path, options=options)
        print(f"✅ Successfully converted to PDF: {output_pdf_path}")
        return True
    except Exception as e:
        print(f"❌ Error converting to PDF: {str(e)}")
        return False

def main():
    # File paths
    current_dir = Path(__file__).parent
    md_file = current_dir / "presentation.md"
    pdf_file = current_dir / "presentation.pdf"
    
    # Check if markdown file exists
    if not md_file.exists():
        print(f"❌ Markdown file not found: {md_file}")
        return
    
    print(f"📄 Converting {md_file} to PDF...")
    
    # Convert to PDF
    success = convert_md_to_pdf(str(md_file), str(pdf_file))
    
    if success:
        print(f"🎉 Conversion complete! PDF saved as: {pdf_file}")
        print(f"📊 File size: {pdf_file.stat().st_size / 1024:.1f} KB")
    else:
        print("💥 Conversion failed!")

if __name__ == "__main__":
    main()
