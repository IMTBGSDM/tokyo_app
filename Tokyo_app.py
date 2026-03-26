import streamlit as st
import pandas as pd
from datetime import datetime
import re
import gspread
from google.oauth2.service_account import Credentials

# --- CONFIGURACIÓN Y CONSTANTES ---
st.set_page_config(page_title="Tokyo Garage - Gestión", layout="wide")

# Google Sheets Configuración
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1anidyNrk2dsmEo-5-Q9dj0bC7XnTxeUGGcZzEVsVBAY/edit"

# Regla de Oro #1: Estructura de datos intacta
# Regla de Oro #2: No eliminar campos creados (A menos que el usuario lo exija explícitamente)
SHEETS_CONFIG = {
    "1_Maestro": ["Código.", "Categoría", "Descripción del Trabajo", "Costo Fijo"],
    "08_Clientes": ["ID Cliente", "Fecha", "Nombre Cliente", "Teléfono / WhatsApp", "Correo Electrónico", "Dirección", "Tipo (Frecuente/Nuevo)"],
    "09_Carros por Cliente": ["ID Vehículo", "Placa", "Marca", "Modelo", "Año", "Color", "ID Cliente", "Notas Técnicas (Detalles)", "Nombre Cliente", "Kilometraje"],
    "00_Catalogos": ["Area", "Especialidades", "Proveedores"], 
    "2_Ordenes de Trabajo": [
        "ID Orden", "Fecha Creacion", "Fecha Cierre Tecnico", "Fecha Cierra Admin", 
        "ID Cliente", "Nombre Cliente", "Placa", "Kilometraje", "Estado Tecnico", 
        "Estado Admin", "Tipo Ingreso", "Forma de Pago", "Total Mano de Obra", 
        "Total Repuestos", "Costo Total OT", "Subtotal Venta OT", "ISV (15%)", 
        "Gran Total Cobrado", "Utilidad Neta OT"
    ],
    "10_Detalles de Ordenes": [
        "Fecha Creación", "ID Orden", "ID Servicio", "Tipo Item", "Descripcion", 
        "Mecanico Asignado", "Proveedor", "Cantidad", "Costo Unitario", 
        "Subtotal Costo", "Subtotal Venta", "Ganancia Bruta", "Comentario"
    ], 
    "11_Cotizaciones": ["ID Cotizacion", "ID Cliente", "Nombre Cliente", "ID Vehiculo", "Fecha Cotizacion", "Precio", "Impuesto", "Total"],
    "3_Nomina": ["ID OT", "Servicio Realizado", "Técnico Asignado", "Fecha Terminado", "Subtotal Servicio", "Pago a Empleado", "Margen Bruto"],
    "7_Empleados": ["ID Empleado", "Nombre Completo", "Identidad", "Telefono", "Especialidad Principal", "Área Asignada (Control Interno)", "Tipo de Contratación", "Estado (Activo/Baja)"],
    "4_Kardex CI": ["ID Producto", "Nombre", "Categoría", "Stock Inicial", "Entradas (Compras)", "Salidas (Uso)", "Stock Actual", "Costo Unitario"]
}

# --- ESTILOS CSS ---
st.markdown("""
    <style>
    .main .block-container { padding-top: 1.5rem; }
    h3 { margin-bottom: 0.5rem !important; padding-bottom: 0rem !important; }
    </style>
    """, unsafe_allow_html=True)

# --- CONEXIÓN A GOOGLE SHEETS ---

@st.cache_resource
def get_gspread_client():
    cred_dict = dict(st.secrets["gcp_service_account"])
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = Credentials.from_service_account_info(cred_dict, scopes=scopes)
    return gspread.authorize(credentials)

try:
    gc = get_gspread_client()
    sh = gc.open_by_url(SPREADSHEET_URL)
except Exception as e:
    st.error("Error de conexión con Google Sheets. Verifica tus secretos y permisos.")
    st.stop()

# --- FUNCIONES DE BASE DE DATOS OPTIMIZADAS ---

def inicializar_sheets():
    existentes = [ws.title for ws in sh.worksheets()]
    for sheet, columns in SHEETS_CONFIG.items():
        if sheet not in existentes:
            nuevo_ws = sh.add_worksheet(title=sheet, rows="100", cols=str(len(columns)))
            nuevo_ws.update([columns])
        else:
            if sheet == "08_Clientes":
                ws = sh.worksheet(sheet)
                headers = ws.row_values(1)
                if "Nombre Completo" in headers:
                    idx = headers.index("Nombre Completo")
                    headers[idx] = "Nombre Cliente"
                    ws.update(f"A1:G1", [headers])

def cargar_toda_la_base():
    if 'db' not in st.session_state:
        st.session_state.db = {}
    for sheet in SHEETS_CONFIG.keys():
        try:
            worksheet = sh.worksheet(sheet)
            data = worksheet.get_all_records()
            if not data:
                df = pd.DataFrame(columns=SHEETS_CONFIG[sheet])
            else:
                df = pd.DataFrame(data)
            
            if sheet == "08_Clientes" and "Nombre Completo" in df.columns:
                df = df.rename(columns={"Nombre Completo": "Nombre Cliente"})
                
            st.session_state.db[sheet] = df
        except Exception:
            st.session_state.db[sheet] = pd.DataFrame(columns=SHEETS_CONFIG[sheet])

def leer_datos(sheet_name):
    if 'db' in st.session_state and sheet_name in st.session_state.db:
        return st.session_state.db[sheet_name].copy()
    return pd.DataFrame(columns=SHEETS_CONFIG.get(sheet_name, []))

def safe_money(val):
    try:
        return round(float(val) * 100.0) / 100.0
    except (ValueError, TypeError):
        return 0.0

def guardar_registro(sheet_name, id_col_name, id_valor, registro_lista):
    try:
        ws = sh.worksheet(sheet_name)
        headers = ws.row_values(1)
        
        buscar_col_name = "Nombre Completo" if (sheet_name == "08_Clientes" and "Nombre Completo" in headers and id_col_name == "Nombre Cliente") else id_col_name
            
        col_idx = headers.index(buscar_col_name) + 1
        col_vals = ws.col_values(col_idx)
        
        registro_formateado = []
        for val in registro_lista:
            if isinstance(val, pd.Timestamp) or isinstance(val, datetime):
                registro_formateado.append(val.strftime('%Y-%m-%d'))
            elif pd.isna(val) or val is None:
                registro_formateado.append("")
            elif isinstance(val, float):
                registro_formateado.append(f"{val:.2f}")
            else:
                registro_formateado.append(str(val))
                
        if id_valor in col_vals:
            row_idx = col_vals.index(id_valor) + 1
            from gspread.utils import rowcol_to_a1
            rango_update = f"A{row_idx}:{rowcol_to_a1(row_idx, len(registro_formateado))}"
            ws.update(range_name=rango_update, values=[registro_formateado])
        else:
            ws.append_row(registro_formateado)
            
        df_actual = st.session_state.db[sheet_name]
        nuevo_df = pd.DataFrame([registro_lista], columns=SHEETS_CONFIG[sheet_name])
        
        if id_valor in df_actual[id_col_name].values:
            df_actual = df_actual[df_actual[id_col_name] != id_valor]
            
        st.session_state.db[sheet_name] = pd.concat([df_actual, nuevo_df], ignore_index=True)
        
    except Exception as e:
        st.error(f"Error al guardar registro en {sheet_name}: {e}")

def eliminar_registro(sheet_name, id_col_name, id_valor):
    try:
        ws = sh.worksheet(sheet_name)
        headers = ws.row_values(1)
        col_idx = headers.index(id_col_name) + 1
        col_vals = ws.col_values(col_idx)
        
        if id_valor in col_vals:
            row_idx = col_vals.index(id_valor) + 1
            ws.delete_rows(row_idx)
            
        df_actual = st.session_state.db[sheet_name]
        st.session_state.db[sheet_name] = df_actual[df_actual[id_col_name] != id_valor]
        return True
    except Exception as e:
        st.error(f"Error al deshacer registro: {e}")
        return False

def generar_id_ot():
    df = leer_datos("2_Ordenes de Trabajo")
    anio_short = datetime.now().strftime("%y")
    prefijo = f"OT-{anio_short}-"
    if df.empty: return f"{prefijo}0001"
    mask = df["ID Orden"].astype(str).str.contains(f"OT-{anio_short}-")
    df_anio = df[mask]
    if df_anio.empty: return f"{prefijo}0001"
    try:
        ultimos_ids = df_anio["ID Orden"].str.split('-').str[-1].astype(int)
        nuevo_num = ultimos_ids.max() + 1
    except: nuevo_num = 1
    return f"{prefijo}{nuevo_num:04d}"

def generar_id(prefijo, sheet_name, digitos=2):
    df = leer_datos(sheet_name)
    if df.empty: return f"{prefijo}-{1:0{digitos}d}"
    ids_existentes = df.iloc[:, 0].astype(str)
    numeros = []
    for val in ids_existentes:
        partes = val.split('-')
        if len(partes) > 1 and partes[1].isdigit():
            numeros.append(int(partes[1]))
    max_num = max(numeros) if numeros else 0
    return f"{prefijo}-{max_num + 1:0{digitos}d}"

def generar_id_servicio_global(modalidad):
    df = leer_datos("10_Detalles de Ordenes")
    letra = "E" if modalidad == "Estándar" else "C"
    if df.empty: return f"SER{letra}-00001"
    
    max_num = 0
    for val in df["ID Servicio"].dropna().astype(str):
        partes = val.split('-')
        if len(partes) > 1 and partes[-1].isdigit():
            num = int(partes[-1])
            if num > max_num:
                max_num = num
    return f"SER{letra}-{max_num + 1:05d}"

def limpiar_telefono(valor):
    if pd.isna(valor) or str(valor).lower() == 'nan': return ""
    return str(valor).replace('.0', '').strip()

# --- INICIALIZACIÓN DE ESTADOS ---
if 'db_cargada' not in st.session_state:
    with st.spinner("Conectando y descargando base de datos segura desde la nube..."):
        inicializar_sheets()
        cargar_toda_la_base()
        st.session_state.db_cargada = True

if 'menu_opcion' not in st.session_state:
    st.session_state.menu_opcion = "Generar Orden de Trabajo"

if 'cliente_vehiculo_data' not in st.session_state:
    st.session_state.cliente_vehiculo_data = {
        'ID Cliente': '', 'Nombre Cliente': '', 'Teléfono': '', 'Fecha': datetime.now(),
        'Correo': '', 'Dirección': '', 'Tipo': 'Nuevo', 'ID Vehículo': '', 'Placa': '',
        'Marca': '', 'Modelo': '', 'Año': 2024, 'Color': '', 'Kilometraje': 0, 'Notas': '',
        'Estado Vehículo': 'Nuevo'
    }

if 'ot_form_data' not in st.session_state:
    st.session_state.ot_form_data = {
        'ID Orden': '', 'Fecha Creacion': datetime.now(), 'ID Cliente': '', 'Nombre Cliente': '',
        'Placa': '', 'Kilometraje': 0
    }

if 'ot_generada_exitosa' not in st.session_state:
    st.session_state.ot_generada_exitosa = False
    st.session_state.id_ot_generada = ""

if 'servicio_agregado_exitoso' not in st.session_state:
    st.session_state.servicio_agregado_exitoso = False
    st.session_state.id_ultimo_servicio = ""

if 'servicio_form_data' not in st.session_state:
    st.session_state.servicio_form_data = {
        'Fecha Creación': datetime.now(), 'ID Orden': '', 'ID Servicio': '', 
        'Tipo Item': 'Mano de Obra', 'Descripcion': '',
        'Mecanico Asignado': '', 'Proveedor': '', 
        'Cantidad': 1, 'Costo Unitario': 0.0, 'Subtotal Costo': 0.0, 'Comentario': ''
    }

# --- NAVEGACIÓN LATERAL ---
menu_items = [
    "Master", "Catálogos", "Clientes y Vehículos", "Generar Orden de Trabajo", 
    "Servicios", "Detalles de Ordenes de Trabajo", "Cerrar Orden de Trabajo", 
    "Cotizaciones", "Nómina", "Empleados", "Kardex", "Finanzas"
]

with st.sidebar:
    st.title("🚗 TOKYO GARAGE")
    st.divider()
    
    idx = menu_items.index(st.session_state.menu_opcion) if st.session_state.menu_opcion in menu_items else 3
    
    def menu_callback():
        st.session_state.menu_opcion = st.session_state._menu_radio_widget
        
    st.radio("Navegación", menu_items, index=idx, key="_menu_radio_widget", on_change=menu_callback)
    menu_opcion = st.session_state.menu_opcion
    
    st.divider()
    if st.button("↻ Sincronizar / Forzar Descarga"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

# --- MÓDULOS ---

if menu_opcion == "Master":
    st.header("Servicios Maestros")
    st.dataframe(leer_datos("1_Maestro"), use_container_width=True, hide_index=True)

elif menu_opcion == "Catálogos":
    st.header("Catálogos Generales")
    st.dataframe(leer_datos("00_Catalogos"), use_container_width=True, hide_index=True)

elif menu_opcion == "Clientes y Vehículos":
    col_form, col_space, col_table = st.columns([2, 0.1, 3.2])
    df_clientes_base = leer_datos("08_Clientes")
    df_vehiculos_base = leer_datos("09_Carros por Cliente")

    with col_form:
        with st.container(height=800, border=False):
            st.subheader("Datos del Cliente")
            c_row_header_1, c_row_header_2 = st.columns(2)
            
            tipo_cli_opciones = ["Frecuente", "Nuevo", "Flota"]
            curr_tipo = st.session_state.cliente_vehiculo_data.get('Tipo', 'Nuevo')
            tipo_cli = c_row_header_1.selectbox(":red[*] Tipo", tipo_cli_opciones, index=tipo_cli_opciones.index(curr_tipo) if curr_tipo in tipo_cli_opciones else 1)
            
            def_tel = limpiar_telefono(st.session_state.cliente_vehiculo_data.get('Teléfono', ''))
            def_fecha = st.session_state.cliente_vehiculo_data.get('Fecha', datetime.now())
            
            if tipo_cli == "Frecuente":
                noms_existentes = [""] + sorted(df_clientes_base["Nombre Cliente"].dropna().unique().tolist())
                curr_nom = st.session_state.cliente_vehiculo_data.get('Nombre Cliente', '')
                nom_cli = st.selectbox(":red[*] Nombre Cliente", options=noms_existentes, index=noms_existentes.index(curr_nom) if curr_nom in noms_existentes else 0)
                if nom_cli:
                    match_c = df_clientes_base[df_clientes_base["Nombre Cliente"] == nom_cli]
                    if not match_c.empty:
                        id_cli_display = match_c.iloc[0]["ID Cliente"]
                    else:
                        id_cli_display = generar_id("CLI", "08_Clientes", 4)
                else:
                    id_cli_display = generar_id("CLI", "08_Clientes", 4)
            else:
                nom_cli = st.text_input(":red[*] Nombre Cliente", value=str(st.session_state.cliente_vehiculo_data.get('Nombre Cliente', '')))
                id_cli_display = generar_id("CLI", "08_Clientes", 4)
            
            c_row_header_2.text_input(":red[*] Código de Cliente", value=id_cli_display, disabled=True)
            
            c_row1_1, c_row1_2 = st.columns(2)
            tel_cli = c_row1_1.text_input(":red[*] Teléfono (8+ dígitos)", value=def_tel)
            fecha_reg = c_row1_2.date_input(":red[*] Fecha de Registro", value=def_fecha, format="DD/MM/YYYY")
            email_cli = st.text_input("e-mail", value=str(st.session_state.cliente_vehiculo_data.get('Correo', '')))
            dir_cli = st.text_input("Dirección", value=str(st.session_state.cliente_vehiculo_data.get('Dirección', '')))
            
            st.divider()
            st.subheader("Datos del Vehículo")
            v_header_1, v_header_2 = st.columns(2)
            
            tipo_veh = v_header_1.radio("Estado del Vehículo", ["Registrado", "Nuevo"], horizontal=True) if tipo_cli == "Frecuente" else "Nuevo"
            
            v_row1_1, v_row1_2 = st.columns(2)
            
            if tipo_veh == "Registrado" and nom_cli:
                df_veh_filtrado = df_vehiculos_base[df_vehiculos_base["ID Cliente"] == id_cli_display]
                placas_existentes = [""] + df_veh_filtrado["Placa"].dropna().unique().tolist()
                placa_raw = v_row1_1.selectbox(":red[*] Placa", options=placas_existentes)
                if placa_raw:
                    match_v = df_veh_filtrado[df_veh_filtrado["Placa"] == placa_raw]
                    id_veh_display = match_v.iloc[0]["ID Vehículo"] if not match_v.empty else generar_id("VEH", "09_Carros por Cliente", 5)
                else:
                    id_veh_display = generar_id("VEH", "09_Carros por Cliente", 5)
            else:
                placa_raw = v_row1_1.text_input(":red[*] Placa").upper()
                id_veh_display = generar_id("VEH", "09_Carros por Cliente", 5)
            
            v_header_2.text_input(":red[*] ID Vehículo", value=id_veh_display, disabled=True)
            
            km_val = v_row1_2.number_input("Kilometraje Inicial", step=1000)
            v_row2_1, v_row2_2 = st.columns(2)
            marca_val = v_row2_1.text_input(":red[*] Marca")
            modelo_val = v_row2_2.text_input(":red[*] Modelo")
            
            v_row3_1, v_row3_2 = st.columns(2)
            anio_val = v_row3_1.number_input(":red[*] Año", min_value=1950, max_value=2030, value=2024)
            color_val = v_row3_2.text_input(":red[*] Color")
            notas_val = st.text_area("Notas Técnicas", height=100)
            
            campos_obligatorios = [id_cli_display, fecha_reg, nom_cli, tel_cli, tipo_cli, id_veh_display, placa_raw, marca_val, modelo_val, anio_val, color_val]
            btn_disabled = any(not str(campo).strip() for campo in campos_obligatorios)
            
            st.write("") 
            if st.button("Guardar Datos", type="primary", use_container_width=True, disabled=btn_disabled):
                registro_c = [id_cli_display, fecha_reg.strftime("%Y-%m-%d"), nom_cli, tel_cli, email_cli, dir_cli, tipo_cli]
                guardar_registro("08_Clientes", "ID Cliente", id_cli_display, registro_c)
                
                registro_v = [id_veh_display, placa_raw, marca_val, modelo_val, anio_val, color_val, id_cli_display, notas_val, nom_cli, km_val]
                guardar_registro("09_Carros por Cliente", "ID Vehículo", id_veh_display, registro_v)
                st.success("Guardado exitosamente.")
                st.rerun()

    with col_table:
        st.write("### Clientes")
        st.dataframe(df_clientes_base, use_container_width=True, hide_index=True)
        st.divider()
        st.write("### Vehículos")
        st.dataframe(df_vehiculos_base, use_container_width=True, hide_index=True)

# --- SECCIÓN: GENERAR ORDEN DE TRABAJO ---
elif menu_opcion == "Generar Orden de Trabajo":
    col_form, col_space, col_table = st.columns([2.2, 0.1, 3.2])
    df_clientes = leer_datos("08_Clientes")
    df_vehiculos = leer_datos("09_Carros por Cliente")
    df_ots = leer_datos("2_Ordenes de Trabajo")

    with col_form:
        with st.container(height=600, border=False):
            st.subheader("Generar Orden de Trabajo")
            
            ot_lock = st.session_state.ot_generada_exitosa
            
            ot_row1_1, ot_row1_2 = st.columns(2)
            f_crea = ot_row1_1.date_input(":red[*] Fecha Creacion", value=st.session_state.ot_form_data['Fecha Creacion'], format="DD/MM/YYYY", disabled=ot_lock)
            
            id_ot_val = st.session_state.id_ot_generada if ot_lock else generar_id_ot()
            ot_row1_2.text_input(":red[*] ID Orden", value=id_ot_val, disabled=True)
            
            st.divider()
            
            col_cli_1, col_cli_2 = st.columns(2)
            
            noms_cli_list = [""] + sorted(df_clientes["Nombre Cliente"].dropna().unique().tolist())
            curr_nom = st.session_state.ot_form_data.get('Nombre Cliente', '')
            
            nom_cli_ot = col_cli_1.selectbox(":red[*] Nombre Cliente", options=noms_cli_list, 
                                            index=noms_cli_list.index(curr_nom) if curr_nom in noms_cli_list else 0,
                                            disabled=ot_lock)
            
            id_cli_calc = ""
            if nom_cli_ot:
                match_c = df_clientes[df_clientes["Nombre Cliente"] == nom_cli_ot]
                if not match_c.empty: id_cli_calc = match_c.iloc[0]["ID Cliente"]
            
            id_cli_ot = col_cli_2.text_input(":red[*] ID Cliente", value=id_cli_calc if not ot_lock else st.session_state.ot_form_data.get('ID Cliente', ''), disabled=True)
            
            col_placa, col_km = st.columns(2)
            placas_list = [""]
            if id_cli_calc:
                df_veh_cli = df_vehiculos[df_vehiculos["ID Cliente"] == id_cli_calc]
                placas_list += df_veh_cli["Placa"].dropna().tolist()
            
            curr_placa = st.session_state.ot_form_data.get('Placa', '')
            placa_ot_sel = col_placa.selectbox(":red[*] Placa del Vehículo", options=placas_list,
                                                index=placas_list.index(curr_placa) if curr_placa in placas_list else 0,
                                                disabled=ot_lock or not id_cli_calc)
            
            km_calc = 0
            if placa_ot_sel:
                match_v = df_vehiculos[(df_vehiculos["ID Cliente"] == id_cli_calc) & (df_vehiculos["Placa"] == placa_ot_sel)]
                if not match_v.empty: km_calc = match_v.iloc[0]["Kilometraje"]
            
            km_ot_val = col_km.number_input("Kilometraje Actual", value=int(km_calc) if not ot_lock else int(st.session_state.ot_form_data.get('Kilometraje', 0)), disabled=ot_lock or not placa_ot_sel)

            st.write("")
            
            if not ot_lock:
                campos_obligatorios_ot = [f_crea, id_ot_val, nom_cli_ot, placa_ot_sel]
                btn_ot_disabled = any(not str(campo).strip() for campo in campos_obligatorios_ot)
                
                if st.button("Crear Orden de Trabajo", type="primary", use_container_width=True, disabled=btn_ot_disabled):
                    
                    nueva_ot = [
                        id_ot_val, f_crea.strftime("%Y-%m-%d"), "", "",
                        id_cli_calc, nom_cli_ot, placa_ot_sel, km_ot_val, 
                        "", "", "Con Factura", "Efectivo", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                    ]
                    
                    guardar_registro("2_Ordenes de Trabajo", "ID Orden", id_ot_val, nueva_ot)
                    
                    st.session_state.ot_form_data.update({
                        'ID Cliente': id_cli_calc, 'Nombre Cliente': nom_cli_ot, 
                        'Placa': placa_ot_sel, 'Kilometraje': km_ot_val
                    })
                    st.session_state.ot_generada_exitosa = True
                    st.session_state.id_ot_generada = id_ot_val
                    st.rerun()
            else:
                st.success(f"¡Orden {st.session_state.id_ot_generada} creada con éxito!")
                
                c_btn1, c_btn2, c_btn3 = st.columns(3)
                
                with c_btn1:
                    if st.button("Generar Nueva", use_container_width=True):
                        st.session_state.ot_generada_exitosa = False
                        st.session_state.id_ot_generada = ""
                        st.session_state.ot_form_data = {'ID Orden': '', 'Fecha Creacion': datetime.now(), 'ID Cliente': '', 'Nombre Cliente': '', 'Placa': '', 'Kilometraje': 0}
                        st.rerun()
                        
                with c_btn2:
                    if st.button("Continuar con Servicios", type="primary", use_container_width=True):
                        st.session_state.servicio_form_data['ID Orden'] = st.session_state.id_ot_generada
                        st.session_state.menu_opcion = "Servicios" 
                        st.session_state.ot_generada_exitosa = False 
                        st.session_state.id_ot_generada = ""
                        st.rerun()
                        
                with c_btn3:
                    if st.button("Deshacer Orden", type="secondary", use_container_width=True):
                        eliminar_registro("2_Ordenes de Trabajo", "ID Orden", st.session_state.id_ot_generada)
                        st.session_state.ot_generada_exitosa = False
                        st.session_state.id_ot_generada = ""
                        st.rerun()

    with col_table:
        with st.container(height=600, border=False):
            st.write("### Ordenes de Trabajo")
            st.dataframe(df_ots, use_container_width=True, hide_index=True)

# --- SECCIÓN: SERVICIOS ---
elif menu_opcion == "Servicios":
    col_form, col_space, col_table = st.columns([2.2, 0.1, 3.2])
    df_detalles = leer_datos("10_Detalles de Ordenes")
    df_empleados = leer_datos("7_Empleados")
    df_ots = leer_datos("2_Ordenes de Trabajo")
    df_maestro = leer_datos("1_Maestro")
    df_catalogos = leer_datos("00_Catalogos")

    with col_form:
        with st.container(height=800, border=False):
            st.subheader("Agregar Servicios a Orden")
            
            serv_lock = st.session_state.get('servicio_agregado_exitoso', False)
            
            # FILA 1: Fecha Creación e ID Orden
            r1c1, r1c2 = st.columns(2)
            fecha_creacion_serv = r1c1.date_input(":red[*] Fecha Creación", value=datetime.now(), format="DD/MM/YYYY", disabled=serv_lock)
            
            lista_ots = [""] + df_ots["ID Orden"].dropna().tolist()
            curr_ot_serv = st.session_state.servicio_form_data.get('ID Orden', '')
            
            def sync_ot_serv():
                st.session_state.servicio_form_data['ID Orden'] = st.session_state.sel_ot_serv_key
                
            sel_ot_serv = r1c2.selectbox(":red[*] Seleccionar Orden de Trabajo (ID)", options=lista_ots, 
                                       index=lista_ots.index(curr_ot_serv) if curr_ot_serv in lista_ots else 0,
                                       key="sel_ot_serv_key", on_change=sync_ot_serv, disabled=serv_lock)
            
            disabled_all = not bool(sel_ot_serv) or serv_lock
            
            # FILA 2: Modalidad y ID Servicio
            s_col1, s_col2 = st.columns(2)
            modalidad_serv = s_col1.radio("Modalidad de Servicio", ["Estándar", "Por Cotización"], horizontal=True, disabled=disabled_all)
            
            id_serv_auto = st.session_state.id_ultimo_servicio if serv_lock else (generar_id_servicio_global(modalidad_serv) if sel_ot_serv else "")
            s_col2.text_input(":red[*] ID Servicio", value=id_serv_auto, disabled=True)
            
            st.write("")
            
            # FILA 3: Tipo Item y Categoría
            c_cat1, c_cat2 = st.columns(2)
            tipo_item = c_cat1.selectbox(":red[*] Tipo Item", ["Mano de Obra", "Repuestos"], disabled=disabled_all)
            
            categorias_list = [""] + df_maestro["Categoría"].dropna().unique().tolist() if "Categoría" in df_maestro.columns else [""]
            categoria_serv = c_cat2.selectbox(":red[*] Categoría", categorias_list, disabled=disabled_all)
            
            # FILA 4: Descripción
            if modalidad_serv == "Por Cotización":
                desc_serv = st.text_input(":red[*] Descripción del Trabajo", disabled=disabled_all)
            else:
                opciones_desc = [""]
                if categoria_serv:
                    opciones_desc += df_maestro[df_maestro["Categoría"] == categoria_serv]["Descripción del Trabajo"].dropna().unique().tolist()
                desc_serv = st.selectbox(":red[*] Descripción del Trabajo", opciones_desc, disabled=disabled_all)
            
            # FILA 5: Mecánico / Proveedor (Misma ubicación) y Cantidad
            s_col_dyn, s_col_cant = st.columns(2)
            
            if tipo_item == "Mano de Obra":
                lista_mecanicos = [""] + df_empleados["Nombre Completo"].dropna().tolist() if not df_empleados.empty else [""]
                mec_asignado = s_col_dyn.selectbox(":red[*] Mecanico Asignado", options=lista_mecanicos, disabled=disabled_all)
                proveedor = ""
            else: # Repuestos
                lista_prov = [""] + df_catalogos["Proveedores"].dropna().unique().tolist() if "Proveedores" in df_catalogos.columns else [""]
                proveedor = s_col_dyn.selectbox(":red[*] Proveedor", options=lista_prov, disabled=disabled_all)
                mec_asignado = ""
                
            cantidad_serv = s_col_cant.number_input("Cantidad", min_value=1, step=1, disabled=disabled_all)
            
            # FILA 6: Costos
            s_col9, s_col10 = st.columns(2)
            
            if tipo_item == "Mano de Obra":
                costo_fijo_calc = 0.0
                if desc_serv:
                    match_m = df_maestro[df_maestro["Descripción del Trabajo"] == desc_serv]
                    if not match_m.empty and "Costo Fijo" in match_m.columns:
                        costo_fijo_calc = safe_money(match_m.iloc[0]["Costo Fijo"])
                costo_uni_input = s_col9.number_input("Costo Unitario (L)", value=float(costo_fijo_calc), disabled=True)
            else:
                costo_uni_input = s_col9.number_input("Costo Unitario (L)", format="%.2f", step=0.01, disabled=disabled_all)
            
            subtotal_costo_calc = safe_money(cantidad_serv * costo_uni_input)
            s_col10.number_input("Subtotal Costo (L)", value=subtotal_costo_calc, disabled=True)
            
            # FILA 7: Comentarios
            comentario_serv = st.text_area("Comentario", height=68, disabled=disabled_all)
            
            # --- LÓGICA DE BOTONES PARA SERVICIOS ---
            if not serv_lock:
                # Validar botones según el tipo de item para hacer obligatorios los campos dinámicos
                btn_serv_disabled = disabled_all or not (sel_ot_serv and desc_serv and categoria_serv)
                if tipo_item == "Mano de Obra" and not mec_asignado:
                    btn_serv_disabled = True
                elif tipo_item == "Repuestos" and not proveedor:
                    btn_serv_disabled = True
                
                if st.button("Agregar Servicio a Orden", type="primary", use_container_width=True, disabled=btn_serv_disabled):
                    
                    costo_uni = safe_money(costo_uni_input)
                    subtotal_venta_calc = 0.0 
                    ganancia_bruta_calc = "" 
                    
                    nuevo_detalle = [
                        fecha_creacion_serv.strftime("%Y-%m-%d"), sel_ot_serv, id_serv_auto, 
                        tipo_item, desc_serv, mec_asignado, proveedor, cantidad_serv, 
                        costo_uni, subtotal_costo_calc, subtotal_venta_calc, ganancia_bruta_calc, comentario_serv
                    ]
                    
                    guardar_registro("10_Detalles de Ordenes", "ID Servicio", id_serv_auto, nuevo_detalle)
                    
                    st.session_state.servicio_agregado_exitoso = True
                    st.session_state.id_ultimo_servicio = id_serv_auto
                    st.rerun()
            else:
                st.success(f"¡Servicio {st.session_state.id_ultimo_servicio} guardado con éxito!")
                
                c_btn1, c_btn2, c_btn3 = st.columns(3)
                
                with c_btn1:
                    if st.button("Agregar Nuevo", use_container_width=True):
                        st.session_state.servicio_agregado_exitoso = False
                        st.session_state.id_ultimo_servicio = ""
                        st.rerun()
                        
                with c_btn2:
                    if st.button("Continuar con Cierre", type="primary", use_container_width=True):
                        match_ot = df_ots[df_ots["ID Orden"] == sel_ot_serv]
                        if not match_ot.empty:
                            data = match_ot.iloc[0]
                            st.session_state.ot_form_data = {
                                'ID Orden': str(data['ID Orden']),
                                'Fecha Creacion': pd.to_datetime(data['Fecha Creacion']),
                                'Fecha Cierre Tecnico': pd.to_datetime(data['Fecha Cierre Tecnico']) if not pd.isna(data['Fecha Cierre Tecnico']) and data['Fecha Cierre Tecnico'] != "" else None,
                                'Fecha Cierra Admin': pd.to_datetime(data['Fecha Cierra Admin']) if not pd.isna(data['Fecha Cierra Admin']) and data['Fecha Cierra Admin'] != "" else None,
                                'ID Cliente': data['ID Cliente'], 'Nombre Cliente': data['Nombre Cliente'],
                                'Placa': data['Placa'] if not pd.isna(data['Placa']) else "",
                                'Kilometraje': data['Kilometraje'] if not pd.isna(data['Kilometraje']) else 0,
                                'Estado Tecnico': str(data['Estado Tecnico']) if not pd.isna(data['Estado Tecnico']) else "",
                                'Estado Admin': str(data['Estado Admin']) if not pd.isna(data['Estado Admin']) else "",
                                'Tipo Ingreso': data['Tipo Ingreso'], 'Forma de Pago': data['Forma de Pago'],
                                'Total Mano de Obra': float(data['Total Mano de Obra']), 'Total Repuestos': float(data['Total Repuestos'])
                            }
                        st.session_state.menu_opcion = "Cerrar Orden de Trabajo"
                        st.session_state.servicio_agregado_exitoso = False
                        st.session_state.id_ultimo_servicio = ""
                        st.rerun()
                        
                with c_btn3:
                    if st.button("Deshacer Servicio", type="secondary", use_container_width=True):
                        eliminar_registro("10_Detalles de Ordenes", "ID Servicio", st.session_state.id_ultimo_servicio)
                        st.session_state.servicio_agregado_exitoso = False
                        st.session_state.id_ultimo_servicio = ""
                        st.rerun()

    with col_table:
        with st.container(height=800, border=False):
            st.write("### Ordenes de Trabajo")
            # Mostrar tabla de órdenes de trabajo en lugar de detalles
            st.dataframe(df_ots, use_container_width=True, hide_index=True, height=250)
            
            st.divider()
            
            if sel_ot_serv:
                st.write(f"### 🛒 Resumen de Orden Actual: `{sel_ot_serv}`")
                df_cart = df_detalles[df_detalles["ID Orden"] == sel_ot_serv]
                
                if not df_cart.empty:
                    c_met1, c_met2, c_met3 = st.columns(3)
                    items_count = len(df_cart)
                    mo_total = df_cart[df_cart["Tipo Item"] == "Mano de Obra"]["Subtotal Costo"].sum()
                    rep_total = df_cart[df_cart["Tipo Item"] == "Repuestos"]["Subtotal Costo"].sum()
                    
                    c_met1.metric("Cantidad de Items", f"{items_count}")
                    c_met2.metric("Total Costo (M.O.)", f"L. {safe_money(mo_total):.2f}")
                    c_met3.metric("Total Costo (Rep.)", f"L. {safe_money(rep_total):.2f}")
                    
                    # Carrito mostrará las columnas relevantes para lectura rápida
                    columnas_carrito = ["ID Servicio", "Tipo Item", "Descripcion", "Cantidad", "Subtotal Costo"]
                    st.dataframe(df_cart[columnas_carrito], use_container_width=True, hide_index=True, height=250)
                else:
                    st.info("Aún no se han agregado servicios a esta Orden de Trabajo.")
            else:
                st.info("Selecciona una Orden de Trabajo para ver el carrito de servicios.")

# --- SECCIÓN NUEVA: DETALLES DE ORDENES DE TRABAJO ---
elif menu_opcion == "Detalles de Ordenes de Trabajo":
    st.header("Detalles de Ordenes de Trabajo (Histórico de Servicios)")
    df_detalles = leer_datos("10_Detalles de Ordenes")
    st.dataframe(df_detalles, use_container_width=True, hide_index=True)

# --- SECCIÓN: CERRAR ORDEN DE TRABAJO ---
elif menu_opcion == "Cerrar Orden de Trabajo":
    col_form, col_space, col_table = st.columns([2.2, 0.1, 3.2])
    df_ots = leer_datos("2_Ordenes de Trabajo")
    df_detalles = leer_datos("10_Detalles de Ordenes")

    with col_form:
        if st.button("⬅ Regresar a Servicios"):
            st.session_state.menu_opcion = "Servicios"
            st.rerun()
            
        with st.container(height=750, border=False):
            st.subheader("Cerrar Orden de Trabajo")
            
            ot_row1_1, ot_row1_2 = st.columns(2)
            f_crea_val = st.session_state.ot_form_data.get('Fecha Creacion', '')
            if isinstance(f_crea_val, datetime): f_crea_val = f_crea_val.strftime("%d/%m/%Y")
            ot_row1_1.text_input("Fecha Creacion", value=str(f_crea_val), disabled=True)
            id_ot_val = st.session_state.ot_form_data.get('ID Orden', '')
            ot_row1_2.text_input("ID Orden", value=id_ot_val, disabled=True)
            
            col_cli_1, col_cli_2 = st.columns(2)
            col_cli_1.text_input("Nombre Cliente", value=st.session_state.ot_form_data.get('Nombre Cliente', ''), disabled=True)
            col_cli_2.text_input("ID Cliente", value=st.session_state.ot_form_data.get('ID Cliente', ''), disabled=True)

            col_placa, col_km = st.columns(2)
            col_placa.text_input("Placa del Vehículo", value=st.session_state.ot_form_data.get('Placa', ''), disabled=True)
            col_km.number_input("Kilometraje Actual", value=int(st.session_state.ot_form_data.get('Kilometraje', 0)), disabled=True)

            st.divider()
            st.write("##### Estado y Cierre Administrativo")
            
            ot_row_est1, ot_row_est2 = st.columns(2)
            est_tec = ot_row_est1.text_input("Estado Tecnico", value=st.session_state.ot_form_data.get('Estado Tecnico', ''))
            est_adm = ot_row_est2.text_input("Estado Admin", value=st.session_state.ot_form_data.get('Estado Admin', ''))
            
            ot_row_f1, ot_row_f2 = st.columns(2)
            f_tec = ot_row_f1.date_input("Fecha Cierre Tecnico", value=st.session_state.ot_form_data.get('Fecha Cierre Tecnico') or datetime.now(), format="DD/MM/YYYY")
            f_adm = ot_row_f2.date_input("Fecha Cierra Admin", value=st.session_state.ot_form_data.get('Fecha Cierra Admin') or datetime.now(), format="DD/MM/YYYY")

            ot_row_fin1, ot_row_fin2 = st.columns(2)
            t_ingreso = ot_row_fin1.selectbox("Tipo Ingreso", ["Con Factura", "Sin Factura"], index=["Con Factura", "Sin Factura"].index(st.session_state.ot_form_data.get('Tipo Ingreso', 'Con Factura')))
            f_pago = ot_row_fin2.selectbox("Forma de Pago", ["Efectivo", "Tarjeta", "Por definir"], index=["Efectivo", "Tarjeta", "Por definir"].index(st.session_state.ot_form_data.get('Forma de Pago', 'Efectivo')))
            
            ot_row_val1, ot_row_val2 = st.columns(2)
            
            df_det_actual = df_detalles[df_detalles["ID Orden"] == id_ot_val]
            calc_mo = safe_money(df_det_actual[df_det_actual["Tipo Item"] == "Mano de Obra"]["Costo Unitario"].sum()) if not df_det_actual.empty else 0.0
            calc_rep = safe_money(df_det_actual[df_det_actual["Tipo Item"] == "Repuestos"]["Subtotal Venta"].sum()) if not df_det_actual.empty else 0.0
            
            m_obra = ot_row_val1.number_input("Total Mano de Obra (L)", value=calc_mo, step=100.0, disabled=True)
            repuestos = ot_row_val2.number_input("Total Repuestos (L)", value=calc_rep, step=100.0, disabled=True)
            
            btn_cerrar_disabled = not id_ot_val 
            
            if st.button("Actualizar Orden (Cierre)", type="primary", use_container_width=True, disabled=btn_cerrar_disabled):
                
                m_obra_safe = safe_money(m_obra)
                repuestos_safe = safe_money(repuestos)
                sub_venta = safe_money(m_obra_safe + repuestos_safe)
                costo_base = safe_money(m_obra_safe * 0.80)
                
                isv_raw = (sub_venta - safe_money(repuestos_safe * 0.15)) * 0.15 if t_ingreso == "Con Factura" else 0.0
                isv = safe_money(isv_raw)
                
                total_cobro = safe_money(sub_venta + isv)
                utilidad = safe_money(sub_venta - costo_base)
                
                f_crea_save = st.session_state.ot_form_data['Fecha Creacion']
                if isinstance(f_crea_save, datetime): f_crea_save = f_crea_save.strftime("%Y-%m-%d")
                
                ot_cerrada = [
                    id_ot_val, f_crea_save, 
                    f_tec.strftime("%Y-%m-%d") if f_tec else "", f_adm.strftime("%Y-%m-%d") if f_adm else "",
                    st.session_state.ot_form_data['ID Cliente'], st.session_state.ot_form_data['Nombre Cliente'], 
                    st.session_state.ot_form_data['Placa'], st.session_state.ot_form_data['Kilometraje'], 
                    est_tec, est_adm, t_ingreso, f_pago,
                    m_obra_safe, repuestos_safe, costo_base, sub_venta, isv, total_cobro, utilidad
                ]
                
                guardar_registro("2_Ordenes de Trabajo", "ID Orden", id_ot_val, ot_cerrada)
                
                st.success("Cierre de Orden y datos financieros guardados correctamente.")
                if 'last_selected_ot_cerrar_idx' in st.session_state:
                    del st.session_state['last_selected_ot_cerrar_idx']
                st.rerun()

    with col_table:
        with st.container(height=800, border=False):
            st.write("### Ordenes de Trabajo")
            sel_ot = st.dataframe(df_ots, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row")
            
            if sel_ot and len(sel_ot.selection.rows) > 0:
                idx = sel_ot.selection.rows[0]
                if idx < len(df_ots):
                    if st.session_state.get('last_selected_ot_cerrar_idx') != idx:
                        data = df_ots.iloc[idx]
                        st.session_state.ot_form_data = {
                            'ID Orden': str(data['ID Orden']),
                            'Fecha Creacion': pd.to_datetime(data['Fecha Creacion']),
                            'Fecha Cierre Tecnico': pd.to_datetime(data['Fecha Cierre Tecnico']) if not pd.isna(data['Fecha Cierre Tecnico']) and data['Fecha Cierre Tecnico'] != "" else None,
                            'Fecha Cierra Admin': pd.to_datetime(data['Fecha Cierra Admin']) if not pd.isna(data['Fecha Cierra Admin']) and data['Fecha Cierra Admin'] != "" else None,
                            'ID Cliente': data['ID Cliente'], 'Nombre Cliente': data['Nombre Cliente'],
                            'Placa': data['Placa'] if not pd.isna(data['Placa']) else "",
                            'Kilometraje': data['Kilometraje'] if not pd.isna(data['Kilometraje']) else 0,
                            'Estado Tecnico': str(data['Estado Tecnico']) if not pd.isna(data['Estado Tecnico']) else "",
                            'Estado Admin': str(data['Estado Admin']) if not pd.isna(data['Estado Admin']) else "",
                            'Tipo Ingreso': data['Tipo Ingreso'], 'Forma de Pago': data['Forma de Pago'],
                            'Total Mano de Obra': float(data['Total Mano de Obra']), 'Total Repuestos': float(data['Total Repuestos'])
                        }
                        st.session_state.last_selected_ot_cerrar_idx = idx
                        st.rerun()
            else:
                if 'last_selected_ot_cerrar_idx' in st.session_state:
                    del st.session_state['last_selected_ot_cerrar_idx']

# --- OTRAS SECCIONES SIMPLES ---
elif menu_opcion == "Cotizaciones":
    st.header("Cotizaciones")
    st.dataframe(leer_datos("11_Cotizaciones"), use_container_width=True, hide_index=True)

elif menu_opcion == "Nómina":
    st.header("Control de Nómina")
    st.dataframe(leer_datos("3_Nomina"), use_container_width=True, hide_index=True)

elif menu_opcion == "Empleados":
    st.header("Base de Empleados")
    st.dataframe(leer_datos("7_Empleados"), use_container_width=True, hide_index=True)

elif menu_opcion == "Kardex":
    st.header("Inventario Kardex")
    st.dataframe(leer_datos("4_Kardex CI"), use_container_width=True, hide_index=True)

elif menu_opcion == "Finanzas":
    st.header("Resumen Financiero")
    df_fin = leer_datos("2_Ordenes de Trabajo")
    if not df_fin.empty:
        st.dataframe(df_fin[["ID Orden", "Nombre Cliente", "Gran Total Cobrado", "Utilidad Neta OT"]], use_container_width=True, hide_index=True)
