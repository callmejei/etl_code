#!/usr/bin/env python3
"""
Convert Enterprise Big Data Platform Governance and Standards markdown document
to Microsoft Word (.docx) format with professional formatting.
"""

import re
from pathlib import Path
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.style import WD_STYLE_TYPE


def parse_markdown_to_docx(md_content, doc):
    """Parse markdown content and convert to Word document with formatting."""

    lines = md_content.split('\n')
    i = 0
    in_code_block = False
    code_lines = []
    in_list = False
    list_level = 0
    in_table = False
    table_data = []

    while i < len(lines):
        line = lines[i]

        # Handle code blocks
        if line.strip().startswith('```'):
            if not in_code_block:
                in_code_block = True
                code_lines = []
            else:
                in_code_block = False
                # Add code block to document
                if code_lines:
                    code_text = '\n'.join(code_lines)
                    para = doc.add_paragraph(code_text)
                    para.style = 'Code'
                code_lines = []
            i += 1
            continue

        if in_code_block:
            code_lines.append(line)
            i += 1
            continue

        # Skip horizontal rules
        if re.match(r'^-{3,}$', line.strip()):
            i += 1
            continue

        # Handle headers
        header_match = re.match(r'^(#{1,6})\s+(.+)$', line)
        if header_match:
            level = len(header_match.group(1))
            title = header_match.group(2).strip()

            # Remove markdown links from headers
            title = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', title)

            if level == 1:
                heading = doc.add_heading(title, level=0)
                heading.runs[0].font.size = Pt(24)
                heading.runs[0].font.color.rgb = RGBColor(44, 62, 80)
            elif level == 2:
                heading = doc.add_heading(title, level=1)
                heading.runs[0].font.size = Pt(18)
                heading.runs[0].font.color.rgb = RGBColor(52, 73, 94)
            elif level == 3:
                heading = doc.add_heading(title, level=2)
                heading.runs[0].font.size = Pt(14)
                heading.runs[0].font.color.rgb = RGBColor(52, 73, 94)
            else:
                heading = doc.add_heading(title, level=3)
                heading.runs[0].font.size = Pt(12)
            i += 1
            continue

        # Handle tables
        if '|' in line and line.strip().startswith('|'):
            if not in_table:
                in_table = True
                table_data = []

            # Parse table row
            cells = [cell.strip() for cell in line.split('|')[1:-1]]

            # Skip separator rows
            if all(re.match(r'^[-:]+$', cell) for cell in cells):
                i += 1
                continue

            table_data.append(cells)
            i += 1

            # Check if next line is still part of table
            if i < len(lines) and ('|' not in lines[i] or not lines[i].strip().startswith('|')):
                # Create table
                if table_data and len(table_data) > 0:
                    num_cols = len(table_data[0])
                    table = doc.add_table(rows=len(table_data), cols=num_cols)
                    table.style = 'Light Grid Accent 1'

                    for row_idx, row_data in enumerate(table_data):
                        for col_idx, cell_data in enumerate(row_data):
                            if col_idx < num_cols:
                                cell = table.rows[row_idx].cells[col_idx]
                                # Clean markdown formatting
                                cell_text = re.sub(r'\*\*([^*]+)\*\*', r'\1', cell_data)
                                cell_text = re.sub(r'\*([^*]+)\*', r'\1', cell_text)
                                cell_text = re.sub(r'`([^`]+)`', r'\1', cell_text)
                                cell.text = cell_text

                                # Make header row bold
                                if row_idx == 0:
                                    for paragraph in cell.paragraphs:
                                        for run in paragraph.runs:
                                            run.font.bold = True

                in_table = False
                table_data = []
            continue

        # Handle lists
        list_match = re.match(r'^(\s*)([-*]|\d+\.)\s+(.+)$', line)
        if list_match:
            indent = len(list_match.group(1))
            content = list_match.group(3)

            # Clean markdown formatting
            content = format_text_content(content)

            para = doc.add_paragraph(content, style='List Bullet')
            i += 1
            continue

        # Handle regular paragraphs
        if line.strip():
            # Clean markdown formatting
            content = format_text_content(line)

            # Add paragraph
            para = doc.add_paragraph()
            add_formatted_text(para, content)
            i += 1
            continue

        # Empty line
        i += 1


def format_text_content(text):
    """Clean basic markdown formatting from text."""
    # Remove links but keep text
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    return text


def add_formatted_text(paragraph, text):
    """Add text with markdown formatting (bold, italic, code) to paragraph."""
    # Split text into segments with formatting
    segments = []
    current_pos = 0

    # Find all bold, italic, and code patterns
    patterns = [
        (r'\*\*([^*]+)\*\*', 'bold'),
        (r'\*([^*]+)\*', 'italic'),
        (r'`([^`]+)`', 'code'),
    ]

    # Simple approach: handle bold and code
    text = re.sub(r'\*\*([^*]+)\*\*', r'<<<BOLD>>>\1<<<ENDBOLD>>>', text)
    text = re.sub(r'`([^`]+)`', r'<<<CODE>>>\1<<<ENDCODE>>>', text)

    # Now split and format
    parts = re.split(r'(<<<BOLD>>>|<<<ENDBOLD>>>|<<<CODE>>>|<<<ENDCODE>>>)', text)

    current_format = None
    for part in parts:
        if part == '<<<BOLD>>>':
            current_format = 'bold'
        elif part == '<<<ENDBOLD>>>':
            current_format = None
        elif part == '<<<CODE>>>':
            current_format = 'code'
        elif part == '<<<ENDCODE>>>':
            current_format = None
        elif part:
            run = paragraph.add_run(part)
            if current_format == 'bold':
                run.font.bold = True
            elif current_format == 'code':
                run.font.name = 'Courier New'
                run.font.size = Pt(10)
                run.font.color.rgb = RGBColor(214, 51, 132)


def create_word_document(md_file, output_file):
    """Create a professionally formatted Word document from markdown."""

    print(f"Reading markdown file: {md_file}")
    with open(md_file, 'r', encoding='utf-8') as f:
        md_content = f.read()

    # Create new Word document
    doc = Document()

    # Set document margins
    sections = doc.sections
    for section in sections:
        section.top_margin = Inches(1)
        section.bottom_margin = Inches(1)
        section.left_margin = Inches(1)
        section.right_margin = Inches(1)

    # Add custom styles
    styles = doc.styles

    # Code style
    try:
        code_style = styles.add_style('Code', WD_STYLE_TYPE.PARAGRAPH)
        code_style.font.name = 'Courier New'
        code_style.font.size = Pt(9)
        code_style.paragraph_format.left_indent = Inches(0.5)
        code_style.paragraph_format.space_before = Pt(6)
        code_style.paragraph_format.space_after = Pt(6)
    except:
        # Style already exists
        pass

    # Parse and convert markdown to Word
    print("Converting markdown to Word format...")
    parse_markdown_to_docx(md_content, doc)

    # Save document
    print(f"Saving Word document: {output_file}")
    doc.save(output_file)
    print(f"✓ Word document created successfully!")


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

    # Output file
    output_file = output_dir / 'Enterprise_BigData_Platform_Governance_Standards.docx'

    # Create Word document
    create_word_document(input_file, output_file)

    print("\n" + "=" * 80)
    print("Conversion complete!")
    print(f"Word document: {output_file.name}")
    print("=" * 80)
    print("\nYou can now:")
    print("  1. Open the .docx file in Microsoft Word")
    print("  2. Edit and customize the formatting as needed")
    print("  3. Save as PDF from Word (File → Save As → PDF)")
    print("  4. Share directly with stakeholders")


if __name__ == '__main__':
    main()
