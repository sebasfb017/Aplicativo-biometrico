---
name: streamlit-ui-biometrico
description: Guía de diseño visual, componentes y patrones UI en Streamlit para el aplicativo biométrico de Dolormed.
---

# Estándar de Interfaz y Diseño Visual (UI/UX)

## 1. Paleta de Colores y Tokens de Diseño
- **Primario / Acción**: Azul `#3B82F6` (hover `#2563EB`), Gradiente `#3B82F6` a `#6366F1`.
- **Estados Operativos**:
  - Aprobado / Éxito: `#10B981` (borde `#34D399`)
  - Rechazado / Error: `#EF4444` (borde `#F87171`)
  - Alerta / Pendiente: `#F59E0B` (borde `#FBBF24`)
  - En espera / Revisión: `#8B5CF6` (borde `#A78BFA`)
- **Glassmorphism**: Fondos `rgba(15, 23, 42, 0.65)` o `rgba(255, 255, 255, 0.05)`, borde `1px solid rgba(255, 255, 255, 0.08)`, `backdrop-filter: blur(16px)`.

## 2. Componentes Visuales Reutilizables
- **KPI Cards (`views/dashboard_view.py`)**:
  - Contenedores con borde inferior acentuado en color (`border-bottom: 3px solid {color}`).
  - Métricas con texto grande, subtítulo de estado e iconos temáticos.
- **Línea de Tiempo / Audit Trail (`components/timeline_component.py`)**:
  - Usar `render_approval_timeline(req_id)` en todo modal o vista de solicitud.
- **Animaciones Lottie (`utils/animations.py`)**:
  - Carga: `render_lottie_loading(key="...")`
  - Éxito: `render_lottie_success(key="...")`
  - Sincronización biométrica: `render_lottie_sync(key="...")`

## 3. Estructura Estándar de una Vista (`views/`)
```python
def page_modulo(user):
    # 1. Header institucional
    st.title("📋 Título del Módulo")
    st.write("Subtítulo descriptivo de la acción.")
    
    # 2. Filtros colapsables
    with st.expander("🔍 Buscar y Filtrar", expanded=False):
        c1, c2 = st.columns(2)
        ...
    
    # 3. Listados en tarjetas con columnas asimétricas
    for _, r in df.iterrows():
        with st.container(border=True):
            col_info, col_actions = st.columns([3, 1])
            with col_info:
                st.markdown(f"**{r['full_name']}** - *{r['reason_type']}*")
                st.write(f"Fechas: {r['leave_date_start']} al {r['leave_date_end']}")
            with col_actions:
                st.button("👍 Aprobar", type="primary", key=f"btn_{r['id']}", on_click=callback, args=(r,))
```

## 4. Reglas Estrictas de Experiencia de Usuario
- **Modales Diálogo**: Siempre usar `@st.dialog("Título", width="large")`.
- **Feedback Inmediato**: Usar `st.toast("Mensaje", icon="✅")` en mutaciones exitosas.
- **Evitar Bloqueos UI**: Acciones de base de datos o envíos masivos deben delegarse a funciones con `on_click` para evitar recargas en blanco o fallos de renderizado.
- **Compatibilidad de Tema**: Respetar modo Claro/Oscuro mediante variables CSS dinámicas de `utils/theme.py`.
