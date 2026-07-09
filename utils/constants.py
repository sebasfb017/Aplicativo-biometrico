# utils/constants.py

ROLES = ("admin", "nomina")

AREA_MAPPING = {
    "Administrativo": ["Calidad", "SST", "Dirección Administrativa", "Sistemas", "Servicios Generales", "Gerencia", "administrativa", "siau", "Contratación, marketing y publicidad", "Marketing, publicidad y comunicaciones", "Ejecutivo comercial", "Cirugía", "Jurídica", "Auditor Médico", "Orientador", "Seguridad", "Archivo", "Control Interno", "Mantenimiento"],
    "Financiera": ["Facturación", "Glosas", "Cartera", "Nomina", "Contabilidad", "Dirección Financiera", "Compras", "Talento humano"],
    "Asistencial": ["Enfermería", "Farmacia", "Admisiones", "Rehabilitación"],
    "Medico": ["Medico"],
    "Rayos X": ["Tecnólogo Rayos X"]
}
# Touch para recargar cache

ZARZAL_EMPLOYEES = [
    "100389343", "100628953", "100643956", "100748910",
    "111421083", "111644217", "38797265", "66681600", "16732215"
]