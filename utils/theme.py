def get_theme_css(theme_preference: str) -> str:
    if theme_preference == "Claro":
        return """
    <style>
    /* VARIABLES MODO CLARO */
    :root {
        --primary-blue: #2563EB;
        --glow-blue: rgba(37, 99, 235, 0.2);
        --glass-bg: rgba(255, 255, 255, 0.85);
        --glass-border: rgba(0, 0, 0, 0.1);
    }
    
    /* Fondo Claro Moderno */
    .stApp {
        background: radial-gradient(circle at 15% 50%, rgba(59, 130, 246, 0.08), transparent 40%),
                    radial-gradient(circle at 85% 30%, rgba(99, 102, 241, 0.08), transparent 40%),
                    radial-gradient(circle at 50% 80%, rgba(236, 72, 153, 0.05), transparent 40%),
                    linear-gradient(135deg, #F8FAFC 0%, #F1F5F9 100%) !important;
        background-attachment: fixed !important;
        color: #0F172A !important;
    }
    
    /* Header Claro */
    header[data-testid="stHeader"] {
        background: rgba(248, 250, 252, 0.6) !important;
        border-bottom: 1px solid var(--glass-border) !important;
    }

    /* Sobrescribir textos de Streamlit a oscuros */
    .stMarkdown p, .stMarkdown strong, .stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown li, label, .stMarkdown span {
        color: #1E293B !important;
    }
    
    /* Paneles / Métricas en Claro */
    div[data-testid="stMetric"], div[data-testid="stExpander"], div[data-testid="stDataFrame"] > div, div[data-testid="stTable"] > div, div[data-testid="stForm"] {
        background: rgba(255, 255, 255, 0.7) !important;
        border: 1px solid var(--glass-border) !important;
        box-shadow: 0 8px 32px rgba(0,0,0,0.05) !important;
        color: #0F172A !important;
    }
    
    /* Botones Premium Claro */
    button[kind="primary"], button[kind="primaryFormSubmit"] {
        background: linear-gradient(90deg, #3B82F6, #6366F1) !important;
        color: white !important;
        border: none !important;
        box-shadow: 0 4px 15px rgba(59, 130, 246, 0.25) !important;
    }
    button[kind="primary"]:hover {
        background: linear-gradient(90deg, #2563EB, #4F46E5) !important;
        box-shadow: 0 6px 20px rgba(59, 130, 246, 0.4) !important;
    }
    
    /* Inputs Claro */
    div[data-baseweb="input"], div[data-baseweb="select"] > div {
        background: #FFFFFF !important;
        border: 1px solid #CBD5E1 !important;
        color: #0F172A !important;
    }
    
    /* Texto en Inputs */
    input, textarea, div[data-baseweb="select"] * {
        color: #0F172A !important;
    }
    
    /* Modales Claro */
    div[data-testid="stDialog"] > div {
        background: rgba(255, 255, 255, 0.95) !important;
        border: 1px solid rgba(0,0,0,0.1) !important;
        box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.15) !important;
    }
    </style>
    """
    else:
        return "" # Devuelve vacío porque app.py ya tiene inyectado por defecto el CSS del Modo Oscuro
