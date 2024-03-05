import re


def clean_text(text):
    text = re.sub(r"[“”]", '"', text)  # Replace smart double quotes
    text = re.sub(r"[‘’]", "'", text)  # Replace smart single quotes
    text = re.sub(r"[—–]", "-", text)
    text = re.sub(r"[^\x00-\x7F]+", "", text)
    
    return text
