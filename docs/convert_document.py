#!/usr/bin/env python3
"""
Convert Enterprise Big Data Platform Governance and Standards document
to multiple downloadable formats.

This script converts the markdown document to:
- Plain text (.txt)
- HTML (.html)
- Styled HTML with CSS
- Microsoft Word (.docx) - requires python-docx library
"""

import re
import os
from pathlib import Path


def convert_markdown_to_text(md_content):
    """Convert markdown to plain text by removing markdown syntax."""
    # Remove markdown formatting
    text = md_content

    # Remove code blocks
    text = re.sub(r'```[\s\S]*?```', '', text)

    # Remove inline code
    text = re.sub(r'`([^`]+)`', r'\1', text)

    # Remove bold and italic
    text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)
    text = re.sub(r'\*([^*]+)\*', r'\1', text)
    text = re.sub(r'__([^_]+)__', r'\1', text)
    text = re.sub(r'_([^_]+)_', r'\1', text)

    # Remove links but keep text
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)

    # Remove images
    text = re.sub(r'!\[([^\]]*)\]\([^)]+\)', r'', text)

    # Remove horizontal rules
    text = re.sub(r'^---+$', '-' * 80, text, flags=re.MULTILINE)

    # Convert headers to uppercase with underlines
    def replace_header(match):
        level = len(match.group(1))
        title = match.group(2).strip()
        if level == 1:
            return f"\n{'=' * 80}\n{title.upper()}\n{'=' * 80}\n"
        elif level == 2:
            return f"\n{'-' * 80}\n{title.upper()}\n{'-' * 80}\n"
        else:
            return f"\n{title}\n{'-' * len(title)}\n"

    text = re.sub(r'^(#{1,6})\s+(.+)$', replace_header, text, flags=re.MULTILINE)

    # Clean up excessive newlines
    text = re.sub(r'\n{4,}', '\n\n\n', text)

    return text


def convert_markdown_to_html(md_content, with_styling=True):
    """Convert markdown to HTML with optional CSS styling."""
    html = md_content

    # Escape HTML entities first
    # html = html.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    # Convert code blocks
    def code_block_replace(match):
        lang = match.group(1) if match.group(1) else ''
        code = match.group(2)
        return f'<pre><code class="language-{lang}">{code}</code></pre>'

    html = re.sub(r'```(\w*)\n([\s\S]*?)```', code_block_replace, html)

    # Convert inline code
    html = re.sub(r'`([^`]+)`', r'<code>\1</code>', html)

    # Convert headers
    for i in range(6, 0, -1):
        html = re.sub(f'^{"#" * i}\s+(.+)$', f'<h{i}>\\1</h{i}>', html, flags=re.MULTILINE)

    # Convert bold and italic
    html = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', html)
    html = re.sub(r'\*([^*]+)\*', r'<em>\1</em>', html)
    html = re.sub(r'__([^_]+)__', r'<strong>\1</strong>', html)
    html = re.sub(r'_([^_]+)_', r'<em>\1</em>', html)

    # Convert links
    html = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', html)

    # Convert images
    html = re.sub(r'!\[([^\]]*)\]\(([^)]+)\)', r'<img src="\2" alt="\1">', html)

    # Convert horizontal rules
    html = re.sub(r'^---+$', '<hr>', html, flags=re.MULTILINE)

    # Convert lists
    # Unordered lists
    html = re.sub(r'^\s*[-*]\s+(.+)$', r'<li>\1</li>', html, flags=re.MULTILINE)

    # Wrap consecutive <li> tags in <ul>
    html = re.sub(r'(<li>.*?</li>\n?)+', r'<ul>\n\g<0></ul>\n', html, flags=re.DOTALL)

    # Convert paragraphs (lines separated by blank lines)
    paragraphs = html.split('\n\n')
    formatted_paragraphs = []
    for para in paragraphs:
        para = para.strip()
        if para and not para.startswith('<'):
            para = f'<p>{para}</p>'
        formatted_paragraphs.append(para)

    html = '\n\n'.join(formatted_paragraphs)

    if with_styling:
        css = """
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                line-height: 1.6;
                max-width: 900px;
                margin: 0 auto;
                padding: 20px;
                color: #333;
                background-color: #f5f5f5;
            }
            .container {
                background-color: white;
                padding: 40px;
                box-shadow: 0 0 10px rgba(0,0,0,0.1);
                border-radius: 5px;
            }
            h1 {
                color: #2c3e50;
                border-bottom: 3px solid #3498db;
                padding-bottom: 10px;
                margin-top: 30px;
            }
            h2 {
                color: #34495e;
                border-bottom: 2px solid #95a5a6;
                padding-bottom: 8px;
                margin-top: 25px;
            }
            h3 {
                color: #34495e;
                margin-top: 20px;
            }
            h4 {
                color: #555;
                margin-top: 15px;
            }
            code {
                background-color: #f4f4f4;
                padding: 2px 6px;
                border-radius: 3px;
                font-family: 'Courier New', Courier, monospace;
                font-size: 0.9em;
                color: #d63384;
            }
            pre {
                background-color: #2d2d2d;
                color: #f8f8f2;
                padding: 15px;
                border-radius: 5px;
                overflow-x: auto;
                border-left: 4px solid #3498db;
            }
            pre code {
                background-color: transparent;
                color: #f8f8f2;
                padding: 0;
            }
            table {
                border-collapse: collapse;
                width: 100%;
                margin: 20px 0;
            }
            table th, table td {
                border: 1px solid #ddd;
                padding: 12px;
                text-align: left;
            }
            table th {
                background-color: #3498db;
                color: white;
                font-weight: bold;
            }
            table tr:nth-child(even) {
                background-color: #f9f9f9;
            }
            ul, ol {
                margin: 10px 0;
                padding-left: 30px;
            }
            li {
                margin: 5px 0;
            }
            hr {
                border: none;
                border-top: 2px solid #e0e0e0;
                margin: 30px 0;
            }
            a {
                color: #3498db;
                text-decoration: none;
            }
            a:hover {
                text-decoration: underline;
            }
            .doc-info {
                background-color: #e8f4f8;
                padding: 15px;
                border-left: 4px solid #3498db;
                margin-bottom: 20px;
            }
            @media print {
                body {
                    background-color: white;
                }
                .container {
                    box-shadow: none;
                }
            }
        </style>
        """

        html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Enterprise Big Data Platform Governance and Standards</title>
    {css}
</head>
<body>
    <div class="container">
        {html}
    </div>
</body>
</html>"""
        return html_doc

    return html


def main():
    """Main conversion function."""
    # Get the script directory
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent if script_dir.name == 'docs' else script_dir

    # Input file
    input_file = repo_root / 'Enterprise_BigData_Platform_Governance_Standards.md'

    # Output directory
    output_dir = repo_root / 'docs'
    output_dir.mkdir(exist_ok=True)

    print(f"Reading markdown file: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        md_content = f.read()

    # Convert to plain text
    print("Converting to plain text...")
    text_content = convert_markdown_to_text(md_content)
    text_file = output_dir / 'Enterprise_BigData_Platform_Governance_Standards.txt'
    with open(text_file, 'w', encoding='utf-8') as f:
        f.write(text_content)
    print(f"✓ Created: {text_file}")

    # Convert to HTML
    print("Converting to HTML...")
    html_content = convert_markdown_to_html(md_content, with_styling=True)
    html_file = output_dir / 'Enterprise_BigData_Platform_Governance_Standards.html'
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"✓ Created: {html_file}")

    # Copy original markdown to docs folder
    import shutil
    md_copy = output_dir / 'Enterprise_BigData_Platform_Governance_Standards.md'
    shutil.copy(input_file, md_copy)
    print(f"✓ Copied: {md_copy}")

    print("\n" + "=" * 80)
    print("Conversion complete! Generated files:")
    print(f"  - {text_file.name}")
    print(f"  - {html_file.name}")
    print(f"  - {md_copy.name}")
    print("=" * 80)
    print("\nGenerating Microsoft Word document...")

    # Try to generate Word document if python-docx is available
    try:
        import subprocess
        word_script = output_dir / 'convert_to_word.py'
        if word_script.exists():
            subprocess.run(['python3', str(word_script)], check=True)
            print("\n✓ Word document (.docx) also generated!")
        else:
            print("\nNote: Run convert_to_word.py separately to generate .docx format")
    except Exception as e:
        print(f"\nNote: Word document not generated. Run convert_to_word.py separately.")
        print(f"  Error: {e}")

    print("\n" + "=" * 80)
    print("\nYou can now:")
    print("  1. Open the HTML file in a browser and save as PDF")
    print("  2. Open the .docx file directly in Microsoft Word")
    print("  3. Import any format into Word/Google Docs")
    print("  4. Share the files directly with stakeholders")


if __name__ == '__main__':
    main()
