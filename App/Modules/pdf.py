import re
import pdfplumber as pl
from pathlib import Path

class Pdf:
    
    ## Immutable vars
    def __init__(self, file: Path):
        self.file = file
    
    ## Reads a pdf and returns its content (text only).
    def read_text(self):
        content: str = ''    # Content is blank at start
        
        ## Initialize pdfplumber
        with pl.open(self.file) as file:
            
            ## Applies for every page in pdf
            for page in file.pages:
                
                text = page.extract_text()    # Extracts the content of the page
                content = content + text      # Stores page text with other pages
            
        return content
    
    def filter_lines(self, content: str, filter: str):
        text = []    # Content is blank at start
        for line in content.split('\n'):
            if re.compile(fr'{filter}').search(line):
                text.append(line)
    
        
        return text