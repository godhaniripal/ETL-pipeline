#!/usr/bin/env python3
"""
Convert presentation.md to PDF using WeasyPrint
"""

import markdown
from weasyprint import HTML, CSS
from pathlib import Path

def convert_md_to_pdf_weasy(md_file_path, output_pdf_path):
    """Convert Markdown file to PDF using WeasyPrint"""
    
    # Read the markdown file
    with open(md_file_path, 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    # Convert markdown to HTML
    html = markdown.markdown(md_content, extensions=['tables', 'fenced_code'])
    
    # Add some CSS styling for better PDF appearance
    css_style = """
    @page {
        size: A4;
        margin: 2cm;
    }
    
    body {
        font-family: 'Segoe UI', Arial, sans-serif;
        line-height: 1.6;
        color: #333;
        font-size: 11pt;
    }
    
    h1 {
        color: #2c3e50;
        border-bottom: 3px solid #3498db;
        padding-bottom: 10px;
        page-break-after: avoid;
        font-size: 24pt;
        margin-top: 0;
    }
    
    h2 {
        color: #34495e;
        border-bottom: 2px solid #ecf0f1;
        padding-bottom: 5px;
        margin-top: 25px;
        page-break-after: avoid;
        font-size: 18pt;
    }
    
    h3 {
        color: #2980b9;
        margin-top: 20px;
        page-break-after: avoid;
        font-size: 14pt;
    }
    
    h4 {
        color: #27ae60;
        margin-top: 15px;
        page-break-after: avoid;
        font-size: 12pt;
    }
    
    ul, ol {
        padding-left: 20px;
        page-break-inside: avoid;
    }
    
    li {
        margin-bottom: 5px;
    }
    
    strong {
        color: #e74c3c;
        font-weight: bold;
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
        font-size: 10pt;
    }
    
    pre {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 5px;
        border-left: 4px solid #3498db;
        overflow-x: auto;
        font-size: 10pt;
        page-break-inside: avoid;
    }
    
    hr {
        border: none;
        height: 2px;
        background: linear-gradient(90deg, #3498db, #2ecc71);
        margin: 20px 0;
    }
    
    .page-break {
        page-break-before: always;
    }
    
    /* Prevent widows and orphans */
    p, li {
        orphans: 2;
        widows: 2;
    }
    
    /* Table styling */
    table {
        border-collapse: collapse;
        width: 100%;
        margin: 15px 0;
    }
    
    th, td {
        border: 1px solid #ddd;
        padding: 8px;
        text-align: left;
    }
    
    th {
        background-color: #f8f9fa;
        font-weight: bold;
    }
    """
    
    # Combine HTML with proper structure
    full_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset='UTF-8'>
        <title>COVID-19 ETL Pipeline Presentation</title>
    </head>
    <body>
        {html}
    </body>
    </html>
    """
    
    try:
        # Create CSS object
        css = CSS(string=css_style)
        
        # Convert HTML to PDF
        HTML(string=full_html).write_pdf(output_pdf_path, stylesheets=[css])
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
    
    print(f"📄 Converting {md_file} to PDF using WeasyPrint...")
    
    # Convert to PDF
    success = convert_md_to_pdf_weasy(str(md_file), str(pdf_file))
    
    if success:
        print(f"🎉 Conversion complete! PDF saved as: {pdf_file}")
        if pdf_file.exists():
            print(f"📊 File size: {pdf_file.stat().st_size / 1024:.1f} KB")
    else:
        print("💥 Conversion failed!")

if __name__ == "__main__":
    main()
