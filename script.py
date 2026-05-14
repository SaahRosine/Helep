import pypdf
reader = pypdf.PdfReader('Lecture Notes Software Architecture Orchestration with K8s Mini Project Specifications.pdf')
with open('pdf_output.txt', 'w', encoding='utf-8') as f:
    for page in reader.pages:
        f.write(page.extract_text() + '\n')
