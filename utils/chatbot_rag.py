import os
import PyPDF2
import streamlit as st

KNOWLEDGE_BASE_DIR = "data/knowledge_base"

def init_knowledge_base():
    """Crea el directorio de base de conocimiento si no existe."""
    if not os.path.exists(KNOWLEDGE_BASE_DIR):
        os.makedirs(KNOWLEDGE_BASE_DIR)

def extract_text_from_pdf(file_path):
    """Extrae el texto de un archivo PDF dado."""
    text = ""
    try:
        with open(file_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return text

@st.cache_data(ttl=600)  # Cachear por 10 minutos para no leer los PDFs en cada recarga de la página
def get_knowledge_base_context():
    """Lee todos los PDFs en la base de conocimiento y retorna un string gigante con el contenido."""
    init_knowledge_base()
    context = ""
    for filename in os.listdir(KNOWLEDGE_BASE_DIR):
        if filename.lower().endswith(".pdf"):
            file_path = os.path.join(KNOWLEDGE_BASE_DIR, filename)
            doc_text = extract_text_from_pdf(file_path)
            if doc_text:
                context += f"\n--- INICIO DEL DOCUMENTO: {filename} ---\n"
                context += doc_text
                context += f"\n--- FIN DEL DOCUMENTO: {filename} ---\n"
    return context
