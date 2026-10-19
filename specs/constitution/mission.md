# Constitución del Sistema — Misión y Alcance

## 1. Propósito del Sistema
El aplicativo biométrico y de nómina de **Dolormed** es una plataforma integral orientada a:
- Centralizar y automatizar el registro de marcaciones de asistencia física en terminales ZKTeco distribuidas en sedes clínicas.
- Proveer un portal de autogestión para los colaboradores donde puedan radicar solicitudes de permisos, licencias, vacaciones e incapacidades con trazabilidad documental (formato F-TH-012).
- Gestionar flujos jerárquicos de aprobación multi-nivel (Coordinador de Equipo, Jefe de Área, Control Interno y Recursos Humanos/Nómina).
- Calcular automáticamente horas trabajadas, llegadas tarde, inasistencias y cruces de turnos para alimentar la prenómina corporativa.

---

## 2. Módulos Actuales del Sistema

| Módulo | Archivo Principal | Propósito y Funcionalidad |
| :--- | :--- | :--- |
| **Dashboard** | `views/dashboard_view.py` | Panel ejecutivo con KPIs del día: asistencias en tiempo real, empleados ausentes, llegadas tarde, gráficos interactivos de puntualidad y alertas operativas. |
| **Sincronizar Relojes** | `views/sync_zkteco_view.py` | Centro de comando de hardware biométrico. Conexión TCP/IP con terminales ZKTeco, descarga masiva de marcaciones (`attendance_raw`), sincronización horaria, corrección de IDs huérfanos y descarga de festivos nacionales. |
| **Visualizar Data** | `views/attendance_view.py` | Consulta tabular y analítica de marcaciones consolidadas por rango de fechas, sedes, áreas o empleados, con cálculo de retrasos y horas extras. |
| **Mi Expediente (360)** | `views/employee_360_view.py` | Vista 360° del colaborador: expediente digital, histórico de marcaciones, horarios asignados, balance acumulado de vacaciones y novedades radicadas. |
| **Portal del Empleado** | `views/employee_portal_view.py` | Autogestión del colaborador: radicación de novedades (permisos personales, laborales, licencias remuneradas/no remuneradas, incapacidades médicas con soportes adjuntos, cambios de turno), seguimiento de radicado F-TH-012 y generación de comprobantes PDF. |
| **Gestión de Permisos** | `views/exceptions_view.py` | Bandeja multi-rol de autorizaciones: revisión y decisión (Aprobar/Rechazar) por parte de coordinadores y jefes, panel de Control Interno, módulo de RRHH/Nómina, calendario consolidado de ausencias y auditoría de decisiones. |
| **Turnos y Horarios** | `views/schedules_view.py` | Parametrización de turnos laborales (horas de entrada, salida y holgura), asignación individual o masiva mediante plantillas Excel/CSV y vista de cuadrantes. |
| **Reporte de Nómina** | `views/month_report_view.py` | Consolidación mensual de horas de retraso, ausencias justificadas e injustificadas para el cierre quincenal/mensual de nómina. |
| **Gestión de Empleados** | `views/employees_view.py` | Directorio maestro de colaboradores: creación, actualización de datos contractuales, sub-áreas, sincronización de usuarios hacia terminales biométricas. |
| **Administración Usuarios** | `views/users_admin_view.py` | Gestión de cuentas de acceso al sistema, asignación de roles, áreas y departamentos gestionados, reinicio de credenciales y desbloqueo de usuarios. |
| **Autenticación** | `views/auth_view.py` | Inicio de sesión seguro con control de intentos fallidos, bloqueo temporal, recuperación de contraseña mediante PIN de 6 dígitos por correo o WhatsApp. |

---

## 3. Roles de Usuario y Jerarquía de Permisos

El sistema implementa un modelo de control de acceso basado en roles (RBAC) con reglas de negocio específicas:

1. **`admin` (Administrador del Sistema)**:
   - Control total de la plataforma, configuración global de dispositivos ZKTeco, gestión de base de datos, administración de usuarios y anulación/reversión de decisiones.
2. **`nomina` (Gestión de Nómina y Talento Humano)**:
   - Acceso al cierre de asistencia, reportes de llegadas tarde, parametrización de turnos, alta/edición de colaboradores y última instancia de aprobación de novedades en estado `PENDING_RRHH`.
   - **Regla de Equivalencia Dinámica**: Empleados registrados con rol base `empleado` pero adscritos a las sub-áreas `'Nomina'` o `'Talento humano'` son promovidos en tiempo de ejecución con permisos equivalentes a `nomina`.
3. **`coordinador` (Líder de Equipo / Departamento)**:
   - Primer nivel de aprobación para empleados que pertenezcan a su departamento gestionado (`managed_department`).
   - Revisa solicitudes en estado `PENDING_COORD`. Sus propias solicitudes no aparecen en su bandeja de aprobación.
4. **`jefe_area` (Jefe de Área Corporativa)**:
   - Segundo nivel de autorización para colaboradores dentro de su área (`managed_area`), procesando solicitudes en estado `PENDING_JEFE`.
   - Incluye filtros especiales para áreas como *Control Interno*, *Administrativo* y *Asistencial*.
5. **`empleado` (Colaborador General)**:
   - Acceso restringido exclusivamente a su Portal de Autogestión, consulta de su expediente personal (360), histórico de solicitudes y descarga de comprobantes F-TH-012.
