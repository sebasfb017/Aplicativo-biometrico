import requests
import streamlit as st
from streamlit_lottie import st_lottie

@st.cache_data
def load_lottieurl(url: str):
    """Descarga la animación Lottie desde la URL dada. Se cachea para optimizar rendimiento."""
    r = requests.get(url)
    if r.status_code != 200:
        return None
    return r.json()

def render_lottie_loading(key="loading", height=200):
    """Renderiza una animación de carga (reloj de arena, engranajes, etc.)"""
    # Lottie URL de un loader minimalista azul/morado
    lottie_url = "https://assets9.lottiefiles.com/packages/lf20_b88nh30c.json"
    animation = load_lottieurl(lottie_url)
    if animation:
        st_lottie(animation, height=height, key=key, loop=True)

def render_lottie_success(key="success", height=150):
    """Renderiza una animación de éxito (Checkmark verde)"""
    # Lottie URL de un check moderno
    lottie_url = "https://assets1.lottiefiles.com/packages/lf20_lk80fpsm.json"
    animation = load_lottieurl(lottie_url)
    if animation:
        st_lottie(animation, height=height, key=key, loop=False)

def render_lottie_sync(key="sync", height=250):
    """Renderiza una animación de sincronización (nube, refresh, biométrico)"""
    # Lottie URL de sincronización en la nube o refresh
    lottie_url = "https://assets1.lottiefiles.com/packages/lf20_vnikrcia.json"
    animation = load_lottieurl(lottie_url)
    if animation:
        st_lottie(animation, height=height, key=key, loop=True)
