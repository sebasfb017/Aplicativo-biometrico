import calendar
from datetime import date, datetime, time, timedelta

import pandas as pd
import streamlit as st

from database_conn.queries import (
    get_cached_employees,
    get_orphan_punches_for_month,
    get_pending_leaves_for_month,
)
from services.analytics import (
    compute_month_lateness,
    fetch_attendance_between,
    get_late_punch_ids,
    to_excel_bytes,
)
from utils.auth import require_role


def page_lateness_report():
    require_role("admin", "nomina")
    st.title("📑 Gestión y Reportes de Nómina - Dolormed")
    st.write(
        "Herramienta integral para auditoría pre-cierre y consolidación oficial de incidencias de asistencia."
    )

    today = date.today()
    default_year = today.year
    default_month = today.month

    if today.day <= 5:
        default_month -= 1
        if default_month == 0:
            default_month = 12
            default_year -= 1

    col_y, col_m = st.columns(2)
    with col_y:
        year = st.number_input(
            "Año Fiscal", min_value=2020, max_value=2100, value=default_year, step=1
        )
    with col_m:
        month = st.number_input(
            "Mes de Nómina", min_value=1, max_value=12, value=default_month, step=1
        )

    tab_auditoria, tab_reporte = st.tabs(
        [
            "🛡️ Auditoría y Validación Pre-Cierre",
            "📊 Reporte Consolidado de Incidencias",
        ]
    )

    # ==========================================
    # TAB 1: AUDITORÍA PRE-CIERRE DE NÓMINA
    # ==========================================
    with tab_auditoria:
        st.subheader("🛡️ Validación Preventiva de Cierre")
        st.write(
            "Ejecuta un diagnóstico completo para asegurar que no existan permisos represados, "
            "marcaciones huérfanas o inconsistencias antes de generar los reportes de descuento."
        )

        first_day = date(int(year), int(month), 1)
        last_day = date(
            int(year), int(month), calendar.monthrange(int(year), int(month))[1]
        )

        with st.spinner("Analizando consistencia de marcaciones y novedades..."):
            pending_df = get_pending_leaves_for_month(int(year), int(month))
            orphan_df = get_orphan_punches_for_month(int(year), int(month))
            late_map, just_map = get_late_punch_ids(
                first_day, last_day, return_justified=True
            )

        # Identificar marcaciones con permiso pero con tardanza excedida
        exceeded_punches = []
        for pid, lateness in late_map.items():
            if pid in just_map and just_map[pid].get("exceeded"):
                j = just_map[pid]
                exceeded_punches.append(
                    {
                        "ID Marcación": pid,
                        "Tipo Permiso": j.get("reason"),
                        "Horario Permiso": j.get("time"),
                        "Minutos Excedidos": lateness,
                        "Motivo Permiso": j.get("desc"),
                    }
                )
        exceeded_df = pd.DataFrame(exceeded_punches)

        # Semáforo de Estado General
        num_pending = len(pending_df)
        num_orphans = len(orphan_df)
        num_exceeded = len(exceeded_df)

        k1, k2, k3 = st.columns(3)
        with k1:
            st.metric(
                "Permisos Pendientes",
                num_pending,
                "Requiere aprobación" if num_pending > 0 else "Al día",
                delta_color="inverse" if num_pending > 0 else "normal",
            )
        with k2:
            st.metric(
                "Marcaciones sin Turno",
                num_orphans,
                "Turno no asignado" if num_orphans > 0 else "Todo asignado",
                delta_color="inverse" if num_orphans > 0 else "normal",
            )
        with k3:
            st.metric(
                "Tardanzas que Excedieron Permiso",
                num_exceeded,
                "Llegadas posteriores al permiso" if num_exceeded > 0 else "0 excesos",
                delta_color="off",
            )

        st.markdown("---")

        if num_pending == 0 and num_orphans == 0:
            st.success(
                "🟢 **ESTADO PRE-CIERRE ÓPTIMO**: No se detectaron permisos pendientes ni marcaciones "
                "huérfanas para este mes. La base de datos es 100% consistente para emisión de nómina."
            )
        else:
            st.warning(
                "🟡 **ESTADO PRE-CIERRE: ATENCIÓN REQUERIDA**: Se detectaron inconsistencias operativas "
                "que podrían provocar descuentos erróneos o penalizaciones injustas. Revise el detalle a continuación."
            )

        # 1. Alerta de Permisos Pendientes
        if num_pending > 0:
            st.markdown("#### 🚨 1. Solicitudes de Permiso en Trámite (Sin Aprobar)")
            st.error(
                f"Existen **{num_pending}** solicitudes de permiso que abarcan este mes y aún no están aprobadas. "
                "Si la nómina se liquida sin resolverlas, los colaboradores afectados serán penalizados como llegadas tarde o faltas injustificadas."
            )
            pending_display = pending_df.copy()
            cols_to_show = [
                c
                for c in [
                    "id",
                    "user_id",
                    "full_name",
                    "department",
                    "reason_type",
                    "status",
                    "leave_date_start",
                    "leave_date_end",
                    "start_time",
                    "end_time",
                    "reason_description",
                ]
                if c in pending_display.columns
            ]
            pending_display = pending_display[cols_to_show]
            rename_map = {
                "id": "ID",
                "user_id": "Cédula",
                "full_name": "Empleado",
                "department": "Departamento",
                "reason_type": "Tipo Novedad",
                "status": "Estado Actual",
                "leave_date_start": "Fecha Inicio",
                "leave_date_end": "Fecha Fin",
                "start_time": "Hora Inicio",
                "end_time": "Hora Fin",
                "reason_description": "Motivo",
            }
            pending_display = pending_display.rename(columns=rename_map)
            st.dataframe(pending_display, use_container_width=True, hide_index=True)

        # 2. Marcaciones sin turno asignado
        if num_orphans > 0:
            st.markdown("#### ⚠️ 2. Colaboradores con Marcaciones sin Turno Asignado")
            st.warning(
                f"Se detectaron **{num_orphans}** colaboradores que registraron asistencia en el biométrico "
                "pero no cuentan con turno oficial asignado en la matriz. Sin turno, sus marcaciones no pueden "
                "ser evaluadas con precisión."
            )
            orphan_display = orphan_df.copy()
            orphan_rename = {
                "user_id": "Cédula",
                "full_name": "Empleado",
                "department": "Departamento",
                "total_marcaciones": "Total Marcaciones",
                "primera_marcacion": "Primera Marcación",
                "ultima_marcacion": "Última Marcación",
            }
            orphan_display = orphan_display.rename(columns=orphan_rename)
            st.dataframe(orphan_display, use_container_width=True, hide_index=True)

        # 3. Tardanzas con permiso excedido
        if num_exceeded > 0:
            st.markdown("#### ⏱️ 3. Marcaciones que Excedieron el Tiempo del Permiso")
            st.info(
                "Estas marcaciones cuentan con un permiso registrado y aprobado, pero el colaborador llegó después "
                "de la hora límite autorizada. El sistema solo descuenta los minutos exactos de retraso posterior al permiso."
            )
            st.dataframe(exceeded_df, use_container_width=True, hide_index=True)

    # ==========================================
    # TAB 2: REPORTE CONSOLIDADO DE INCIDENCIAS
    # ==========================================
    with tab_reporte:
        st.subheader("📊 Reporte Consolidado de Incidencias y Retardos")
        st.write(
            "Genera el archivo avalado para procesos disciplinarios y descuentos de nómina."
        )

        search_dni = st.text_input(
            "Buscar DNI", value="", placeholder="Ej: 100646459", key="search_dni_report"
        )

        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("⚙️ Generar Analítica de Tardanzas", type="primary"):
            with st.spinner(
                "Procesando histórico de marcaciones cruzado con horarios..."
            ):
                summary_df, detail_df = compute_month_lateness(int(year), int(month))

            if summary_df.empty:
                st.success(
                    "🎉 ¡Excelente! No se registran tardanzas (o faltan asignaciones previas)."
                )
                return

            # Top 3 de personas con más faltas
            st.markdown("### 🏆 Top 3: Mayores Incidencias")
            top3 = summary_df.head(3)
            cols = st.columns(3)
            for i, (idx, row) in enumerate(top3.iterrows()):
                if i < 3:
                    cols[i].metric(
                        f"ID {row['user_id']}",
                        f"{row['minutos_tarde_total']} mins",
                        f"{row['dias_tarde']} días infractores",
                        delta_color="inverse",
                    )

            st.markdown("---")
            st.markdown("### 📊 Tabla General Resumida")

            # Unir nombre en el summary temporalmente para mostrarlo bonito
            nombres = get_cached_employees()
            summary_view = pd.merge(summary_df, nombres, on="user_id", how="left")

            if search_dni:
                summary_view = summary_view[
                    summary_view["user_id"]
                    .astype(str)
                    .str.contains(search_dni.strip())
                ]

            summary_view = summary_view[
                [
                    "user_id",
                    "full_name",
                    "department",
                    "dias_tarde",
                    "minutos_tarde_total",
                    "fechas_tarde",
                ]
            ]
            summary_view.columns = [
                "DNI Empleado",
                "Nombre",
                "Departamento",
                "Días con Retraso",
                "Minutos Totales Adeudados",
                "Fechas de Retraso",
            ]

            st.dataframe(summary_view, use_container_width=True, hide_index=True)

            first_day = date(int(year), int(month), 1)
            last_day = date(
                int(year), int(month), calendar.monthrange(int(year), int(month))[1]
            )

            raw_df = fetch_attendance_between(
                datetime.combine(first_day, time(0, 0)),
                datetime.combine(last_day + timedelta(days=1), time(0, 0)),
            )

            excel_bytes = to_excel_bytes(summary_df, detail_df, raw_df)
            st.markdown("<br>", unsafe_allow_html=True)
            st.download_button(
                "📥 Descargar Archivo Oficial (Excel Completo)",
                data=excel_bytes,
                file_name=f"Reporte_Tardanzas_Dolormed_{year}_{month:02d}.xlsx",
                mime="application/octet-stream",
                use_container_width=True,
            )

            csv_bytes = summary_df.to_csv(index=False, sep=";").encode("utf-8-sig")
            st.download_button(
                "📥 Descargar Resumen (CSV)",
                data=csv_bytes,
                file_name=f"Resumen_Mensual_Dolormed_{year}_{month:02d}.csv",
                mime="text/csv",
                use_container_width=True,
            )
