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
# Regla de Oro #2: No eliminar campos creados
SHEETS_CONFIG = {
    "1_Maestro": ["Código.", "Categoría", "Descripción del Trabajo", "Tipo", "Costo Fijo"],
    "08_Clientes": ["ID Cliente", "Fecha", "Nombre Cliente", "Teléfono / WhatsApp", "Correo Electrónico", "Dirección", "Tipo (Frecuente/Nuevo)"],
    "09_Carros por Cliente": ["ID Vehículo", "Placa", "Marca", "Modelo", "Año", "Color", "ID Cliente", "Notas Técnicas (Detalles)", "Nombre Cliente", "Kilometraje"],
    "2_Ordenes de Trabajo": [
        "ID Orden", "Fecha Creacion", "Fecha Cierre Tecnico", "Fecha Cierra Admin", 
        "ID Cliente", "Nombre Cliente", "Placa", "Kilometraje", "Estado Tecnico", 
        "Estado Admin", "Tipo Ingreso", "Forma de Pago", "Total Mano de Obra", 
        "Total Repuestos", "Costo Total OT", "Subtotal Venta OT", "ISV (15%)", 
        "Gran Total Cobrado", "Utilidad Neta OT"
    ],
    "10_Detalles de Ordenes": [
        "ID Orden", "ID Servicio", "Tipo Item", "Descripcion", "Mecanico Asignado", 
        "Proveedor", "Cobra al Cliente", "Estado Pago Costo", "Fecha pago Costo", 
        "Cantidad", "Costo Unitario", "Subtotal Costo", "Precio Venta Unitario", 
        "Subtotal Venta", "Ganancia Bruta", "Comentario"
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
    st.error("Error de conexión con Google Sheets. Verifica tus secretos (secrets.toml) y permisos.")
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
    """Elimina errores de punto flotante forzando redondeo exacto de 2 decimales"""
    try:
        # Multiplica por 100, redondea al entero más cercano, divide entre 100
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
                # Forzar formato exacto de 2 decimales para Google Sheets
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

def generar_id_servicio(id_orden):
    df_detalles = leer_datos("10_Detalles de Ordenes")
    if id_orden and id_orden.count('-') >= 2:
        partes = id_orden.split('-')
        yy_xxxx = f"{partes[1]}-{partes[2]}"
    else:
        yy_xxxx = "00-0000"
        
    prefijo = f"SER-{yy_xxxx}"
    
    if df_detalles.empty: return f"{prefijo}-01"
    df_actual = df_detalles[df_detalles["ID Orden"] == id_orden]
    if df_actual.empty: return f"{prefijo}-01"
    try:
        ultimos_nums = df_actual["ID Servicio"].str.split('-').str[-1].astype(int)
        nuevo_num = ultimos_nums.max() + 1
    except:
        nuevo_num = 1
    return f"{prefijo}-{nuevo_num:02d}"

def limpiar_telefono(valor):
    if pd.isna(valor) or str(valor).lower() == 'nan': return ""
    return str(valor).replace('.0', '').strip()

# --- INICIALIZACIÓN DE ESTADOS ---
if 'db_cargada' not in st.session_state:
    with st.spinner("Conectando y descargando base de datos segura desde la nube..."):
        inicializar_sheets()
        cargar_toda_la_base()
        st.session_state.db_cargada = True

if 'cliente_vehiculo_data' not in st.session_state:
    st.session_state.cliente_vehiculo_data = {
        'ID Cliente': '', 'Nombre Cliente': '', 'Teléfono': '', 'Fecha': datetime.now(),
        'Correo': '', 'Dirección': '', 'Tipo': 'Nuevo', 'ID Vehículo': '', 'Placa': '',
        'Marca': '', 'Modelo': '', 'Año': 2024, 'Color': '', 'Kilometraje': 0, 'Notas': '',
        'Estado Vehículo': 'Nuevo'
    }

if 'ot_form_data' not in st.session_state:
    st.session_state.ot_form_data = {
        'ID Orden': '', 'Fecha Creacion': datetime.now(), 'Fecha Cierre Tecnico': None,
        'Fecha Cierra Admin': None, 'ID Cliente': '', 'Nombre Cliente': '',
        'Placa': '', 'Kilometraje': 0, 'Estado Tecnico': '', 'Estado Admin': '', 
        'Tipo Ingreso': 'Con Factura', 'Forma de Pago': 'Efectivo', 
        'Total Mano de Obra': 0.0, 'Total Repuestos': 0.0, 'is_edit': False
    }

if 'servicio_form_data' not in st.session_state:
    st.session_state.servicio_form_data = {
        'ID Servicio': '', 'Tipo Item': 'Mano de Obra', 'Descripcion': '',
        'Mecanico Asignado': '', 'Proveedor': '', 'Cobra al Cliente': '',
        'Estado Pago Costo': 'Pendiente', 'Fecha pago Costo': datetime.now(),
        'Cantidad': 1, 'Costo Unitario': 0.0, 'Subtotal Costo': 0.0,
        'Precio Venta Unitario': 0.0, 'Comentario': ''
    }

with st.sidebar:
    st.title("🚗 TOKYO GARAGE")
    st.divider()
    menu_opcion = st.radio("Navegación", ["Master", "Clientes y Vehículos", "Ordenes de Trabajo", "Cotizaciones", "Nómina", "Empleados", "Kardex", "Finanzas"], index=2)

# --- MÓDULOS ---

if menu_opcion == "Master":
    st.header("Servicios Maestros")
    st.dataframe(leer_datos("1_Maestro"), use_container_width=True, hide_index=True)

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
            
            st.session_state.cliente_vehiculo_data['Tipo'] = tipo_cli
            
            def_tel = limpiar_telefono(st.session_state.cliente_vehiculo_data.get('Teléfono', ''))
            def_fecha = st.session_state.cliente_vehiculo_data.get('Fecha', datetime.now())
            def_email = str(st.session_state.cliente_vehiculo_data.get('Correo', ''))
            def_dir = str(st.session_state.cliente_vehiculo_data.get('Dirección', ''))
            
            if tipo_cli == "Frecuente":
                noms_existentes = [""] + sorted(df_clientes_base["Nombre Cliente"].dropna().unique().tolist())
                curr_nom = st.session_state.cliente_vehiculo_data.get('Nombre Cliente', '')
                nom_cli = st.selectbox(":red[*] Nombre Cliente", options=noms_existentes, index=noms_existentes.index(curr_nom) if curr_nom in noms_existentes else 0)
                
                if nom_cli:
                    match_c = df_clientes_base[df_clientes_base["Nombre Cliente"] == nom_cli]
                    if not match_c.empty:
                        id_cli_display = match_c.iloc[0]["ID Cliente"]
                        def_tel = limpiar_telefono(match_c.iloc[0]["Teléfono / WhatsApp"])
                        def_email = str(match_c.iloc[0]["Correo Electrónico"])
                        def_dir = str(match_c.iloc[0]["Dirección"])
                        try: def_fecha = pd.to_datetime(match_c.iloc[0]["Fecha"]).date()
                        except: pass
                    else:
                        id_cli_display = st.session_state.cliente_vehiculo_data['ID Cliente'] or generar_id("CLI", "08_Clientes", 4)
                else:
                    id_cli_display = st.session_state.cliente_vehiculo_data['ID Cliente'] or generar_id("CLI", "08_Clientes", 4)
            else:
                nom_cli = st.text_input(":red[*] Nombre Cliente", value=str(st.session_state.cliente_vehiculo_data.get('Nombre Cliente', '')))
                id_cli_display = st.session_state.cliente_vehiculo_data['ID Cliente'] if (st.session_state.cliente_vehiculo_data['ID Cliente'] and curr_tipo != 'Frecuente') else generar_id("CLI", "08_Clientes", 4)
            
            c_row_header_2.text_input(":red[*] Código de Cliente", value=id_cli_display, disabled=True)
            
            c_row1_1, c_row1_2 = st.columns(2)
            tel_cli = c_row1_1.text_input(":red[*] Teléfono (8+ dígitos)", value=def_tel)
            fecha_reg = c_row1_2.date_input(":red[*] Fecha de Registro", value=def_fecha, format="DD/MM/YYYY")
            email_cli = st.text_input("e-mail", value=def_email)
            dir_cli = st.text_input("Dirección", value=def_dir)
            
            st.divider()
            st.subheader("Datos del Vehículo")
            
            v_header_1, v_header_2 = st.columns(2)
            curr_estado_veh = st.session_state.cliente_vehiculo_data.get('Estado Vehículo', 'Nuevo')
            
            if tipo_cli == "Frecuente":
                tipo_veh = v_header_1.radio("Estado del Vehículo", ["Registrado", "Nuevo"], 
                                          index=0 if curr_estado_veh == "Registrado" else 1, horizontal=True)
            else:
                tipo_veh = "Nuevo"
                v_header_1.write("")
            
            st.session_state.cliente_vehiculo_data['Estado Vehículo'] = tipo_veh
                
            def_km = int(st.session_state.cliente_vehiculo_data.get('Kilometraje', 0))
            def_marca = str(st.session_state.cliente_vehiculo_data.get('Marca', ''))
            def_modelo = str(st.session_state.cliente_vehiculo_data.get('Modelo', ''))
            def_anio = int(st.session_state.cliente_vehiculo_data.get('Año', 2024))
            def_color = str(st.session_state.cliente_vehiculo_data.get('Color', ''))
            def_notas = str(st.session_state.cliente_vehiculo_data.get('Notas', ''))
            
            v_row1_1, v_row1_2 = st.columns(2)
            
            if tipo_veh == "Registrado" and nom_cli:
                df_veh_filtrado = df_vehiculos_base[df_vehiculos_base["ID Cliente"] == id_cli_display]
                placas_existentes = [""] + df_veh_filtrado["Placa"].dropna().unique().tolist()
                curr_placa = st.session_state.cliente_vehiculo_data.get('Placa', '')
                placa_raw = v_row1_1.selectbox(":red[*] Placa", options=placas_existentes, index=placas_existentes.index(curr_placa) if curr_placa in placas_existentes else 0)
                
                if placa_raw:
                    match_v = df_veh_filtrado[df_veh_filtrado["Placa"] == placa_raw]
                    if not match_v.empty:
                        id_veh_display = match_v.iloc[0]["ID Vehículo"]
                        try: def_km = int(match_v.iloc[0].get("Kilometraje", 0) or 0)
                        except: pass
                        def_marca = str(match_v.iloc[0].get("Marca", ""))
                        def_modelo = str(match_v.iloc[0].get("Modelo", ""))
                        try: def_anio = int(match_v.iloc[0].get("Año", 2024) or 2024)
                        except: pass
                        def_color = str(match_v.iloc[0].get("Color", ""))
                        def_notas = str(match_v.iloc[0].get("Notas Técnicas (Detalles)", ""))
                    else:
                        id_veh_display = st.session_state.cliente_vehiculo_data['ID Vehículo'] or generar_id("VEH", "09_Carros por Cliente", 5)
                else:
                    id_veh_display = st.session_state.cliente_vehiculo_data['ID Vehículo'] or generar_id("VEH", "09_Carros por Cliente", 5)
            else:
                placa_raw = v_row1_1.text_input(":red[*] Placa", value=str(st.session_state.cliente_vehiculo_data.get('Placa', ''))).upper()
                id_veh_display = st.session_state.cliente_vehiculo_data['ID Vehículo'] if (st.session_state.cliente_vehiculo_data['ID Vehículo'] and tipo_veh == 'Nuevo') else generar_id("VEH", "09_Carros por Cliente", 5)
            
            v_header_2.text_input(":red[*] ID Vehículo", value=id_veh_display, disabled=True)
            
            km_val = v_row1_2.number_input("Kilometraje Inicial", value=def_km, step=1000)
            v_row2_1, v_row2_2 = st.columns(2)
            marca_val = v_row2_1.text_input(":red[*] Marca", value=def_marca)
            modelo_val = v_row2_2.text_input(":red[*] Modelo", value=def_modelo)
            
            v_row3_1, v_row3_2 = st.columns(2)
            anio_val = v_row3_1.number_input(":red[*] Año", min_value=1950, max_value=2030, value=def_anio)
            color_val = v_row3_2.text_input(":red[*] Color", value=def_color)
            
            notas_val = st.text_area("Notas Técnicas", value=def_notas, height=100)
            
            campos_obligatorios = [id_cli_display, fecha_reg, nom_cli, tel_cli, tipo_cli, id_veh_display, placa_raw, marca_val, modelo_val, anio_val, color_val]
            btn_disabled = any(not str(campo).strip() for campo in campos_obligatorios)
            
            if tipo_cli in ["Nuevo", "Flota"]:
                lbl_boton = "Ingresar Cliente y Vehículo"
            elif tipo_cli == "Frecuente" and tipo_veh == "Nuevo":
                lbl_boton = "Ingresar Vehículo y Actualizar Cliente"
            else: 
                lbl_boton = "Actualizar Cliente y Vehículo"
            
            st.write("") 
            if st.button(lbl_boton, type="primary", use_container_width=True, disabled=btn_disabled):
                registro_c = [id_cli_display, fecha_reg.strftime("%Y-%m-%d"), nom_cli, tel_cli, email_cli, dir_cli, tipo_cli]
                guardar_registro("08_Clientes", "ID Cliente", id_cli_display, registro_c)
                
                registro_v = [id_veh_display, placa_raw, marca_val, modelo_val, anio_val, color_val, id_cli_display, notas_val, nom_cli, km_val]
                guardar_registro("09_Carros por Cliente", "ID Vehículo", id_veh_display, registro_v)
                
                st.session_state.cliente_vehiculo_data.update({
                    'ID Cliente': id_cli_display, 'Nombre Cliente': nom_cli, 'Tipo': tipo_cli,
                    'ID Vehículo': id_veh_display, 'Placa': placa_raw, 'Estado Vehículo': tipo_veh
                })
                st.success(f"Acción '{lbl_boton}' ejecutada exitosamente.")
                
                if 'last_selected_vehiculo_idx' in st.session_state:
                    del st.session_state['last_selected_vehiculo_idx']
                st.rerun()

    with col_table:
        st.write("### Clientes")
        st.dataframe(df_clientes_base, use_container_width=True, hide_index=True)
        
        st.divider()
        st.write("### Vehículos por Cliente")
        df_view = df_vehiculos_base.copy()
        selected_row = st.dataframe(df_view, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row")
        
        if selected_row and len(selected_row.selection.rows) > 0:
            idx = selected_row.selection.rows[0]
            if idx < len(df_view):
                if st.session_state.get('last_selected_vehiculo_idx') != idx:
                    data_veh = df_view.iloc[idx]
                    data_cli = df_clientes_base[df_clientes_base['ID Cliente'] == data_veh['ID Cliente']]
                    if not data_cli.empty:
                        c = data_cli.iloc[0]
                        st.session_state.cliente_vehiculo_data = {
                            'ID Cliente': c['ID Cliente'], 'Nombre Cliente': c['Nombre Cliente'], 
                            'Teléfono': c['Teléfono / WhatsApp'], 'Fecha': pd.to_datetime(c['Fecha']),
                            'Correo': c['Correo Electrónico'], 'Dirección': c['Dirección'], 
                            'Tipo': 'Frecuente', 
                            'ID Vehículo': data_veh['ID Vehículo'], 'Placa': data_veh['Placa'],
                            'Marca': data_veh['Marca'], 'Modelo': data_veh['Modelo'], 'Año': data_veh['Año'], 
                            'Color': data_veh['Color'], 'Kilometraje': data_veh['Kilometraje'], 
                            'Notas': data_veh['Notas Técnicas (Detalles)'],
                            'Estado Vehículo': 'Registrado' 
                        }
                        st.session_state.last_selected_vehiculo_idx = idx
                        st.rerun()
        else:
            if 'last_selected_vehiculo_idx' in st.session_state:
                del st.session_state['last_selected_vehiculo_idx']

elif menu_opcion == "Ordenes de Trabajo":
    col_form, col_space, col_table = st.columns([2.2, 0.1, 3.2])
    df_clientes = leer_datos("08_Clientes")
    df_ots = leer_datos("2_Ordenes de Trabajo")
    df_vehiculos = leer_datos("09_Carros por Cliente")
    df_detalles = leer_datos("10_Detalles de Ordenes")
    df_empleados = leer_datos("7_Empleados")

    with col_form:
        with st.container(height=850, border=False):
            st.subheader("Gestión de Orden de Trabajo")
            
            ot_row1_1, ot_row1_2 = st.columns(2)
            f_crea = ot_row1_1.date_input(":red[*] Fecha Creacion", value=st.session_state.ot_form_data['Fecha Creacion'], disabled=st.session_state.ot_form_data['is_edit'], format="DD/MM/YYYY")
            id_ot_val = st.session_state.ot_form_data['ID Orden'] if st.session_state.ot_form_data['ID Orden'] else generar_id_ot()
            ot_row1_2.text_input(":red[*] ID Orden", value=id_ot_val, disabled=True)
            
            noms_cli_list = [""] + df_clientes["Nombre Cliente"].dropna().unique().tolist()
            curr_nom = st.session_state.ot_form_data['Nombre Cliente'] or ""
            curr_id = st.session_state.ot_form_data['ID Cliente'] or ""
            
            col_cli_1, col_cli_2 = st.columns(2)
            
            def sync_nombre_a_id():
                if st.session_state.sel_nom_cli_ot:
                    match = df_clientes[df_clientes["Nombre Cliente"] == st.session_state.sel_nom_cli_ot]
                    if not match.empty:
                        new_id = match.iloc[0]["ID Cliente"]
                        st.session_state.ot_form_data['ID Cliente'] = new_id
                        st.session_state.ot_form_data['Nombre Cliente'] = st.session_state.sel_nom_cli_ot
                        st.session_state.ot_form_data['Placa'] = ""
                        st.session_state.ot_form_data['Kilometraje'] = 0
                else:
                    st.session_state.ot_form_data['ID Cliente'] = ""
                    st.session_state.ot_form_data['Nombre Cliente'] = ""
                    st.session_state.ot_form_data['Placa'] = ""
                    st.session_state.ot_form_data['Kilometraje'] = 0

            nom_cli_ot = col_cli_1.selectbox(":red[*] Nombre Cliente", options=noms_cli_list, 
                                           index=noms_cli_list.index(curr_nom) if curr_nom in noms_cli_list else 0,
                                           key="sel_nom_cli_ot", on_change=sync_nombre_a_id, disabled=st.session_state.ot_form_data['is_edit'])
            
            col_cli_2.text_input(":red[*] ID Cliente", value=st.session_state.ot_form_data['ID Cliente'], disabled=True)

            col_placa, col_km = st.columns(2)
            
            if st.session_state.ot_form_data['ID Cliente']:
                df_veh_cli = df_vehiculos[df_vehiculos["ID Cliente"] == st.session_state.ot_form_data['ID Cliente']]
                if "Placa" in df_veh_cli.columns:
                    placas_list = [""] + df_veh_cli["Placa"].dropna().tolist()
                else:
                    placas_list = [""]
                placa_disabled = False
            else:
                placas_list = [""]
                placa_disabled = True

            curr_placa = st.session_state.ot_form_data['Placa'] or ""
            
            def sync_placa_a_km():
                if st.session_state.sel_placa_ot_act:
                    match_v = df_vehiculos[(df_vehiculos["ID Cliente"] == st.session_state.ot_form_data['ID Cliente']) & 
                                          (df_vehiculos["Placa"] == st.session_state.sel_placa_ot_act)]
                    if not match_v.empty:
                        km = match_v.iloc[0]["Kilometraje"]
                        st.session_state.ot_form_data['Placa'] = st.session_state.sel_placa_ot_act
                        st.session_state.ot_form_data['Kilometraje'] = km
                else:
                    st.session_state.ot_form_data['Placa'] = ""
                    st.session_state.ot_form_data['Kilometraje'] = 0

            placa_ot_sel = col_placa.selectbox("Placa del Vehículo", options=placas_list,
                                              index=placas_list.index(curr_placa) if curr_placa in placas_list else 0,
                                              key="sel_placa_ot_act", on_change=sync_placa_a_km, 
                                              disabled=placa_disabled)
            
            km_disabled = True if not st.session_state.ot_form_data['Placa'] else False
            km_ot_val = col_km.number_input("Kilometraje Actual", value=int(st.session_state.ot_form_data['Kilometraje']), 
                                          disabled=km_disabled)

            ot_row_est1, ot_row_est2 = st.columns(2)
            est_tec = ot_row_est1.text_input("Estado Tecnico", value=st.session_state.ot_form_data['Estado Tecnico'])
            est_adm = ot_row_est2.text_input("Estado Admin", value=st.session_state.ot_form_data['Estado Admin'])
            
            ot_row_f1, ot_row_f2 = st.columns(2)
            f_tec = ot_row_f1.date_input("Fecha Cierre Tecnico", value=st.session_state.ot_form_data['Fecha Cierre Tecnico'], format="DD/MM/YYYY")
            f_adm = ot_row_f2.date_input("Fecha Cierra Admin", value=st.session_state.ot_form_data['Fecha Cierra Admin'], format="DD/MM/YYYY")

            ot_row_fin1, ot_row_fin2 = st.columns(2)
            t_ingreso = ot_row_fin1.selectbox("Tipo Ingreso", ["Con Factura", "Sin Factura"], index=["Con Factura", "Sin Factura"].index(st.session_state.ot_form_data['Tipo Ingreso']))
            f_pago = ot_row_fin2.selectbox("Forma de Pago", ["Efectivo", "Tarjeta", "Por definir"], index=["Efectivo", "Tarjeta", "Por definir"].index(st.session_state.ot_form_data['Forma de Pago']))
            
            ot_row_val1, ot_row_val2 = st.columns(2)
            
            df_det_actual = df_detalles[df_detalles["ID Orden"] == id_ot_val]
            
            # Aplicamos roundings de dinero seguros incluso en la lectura agregada
            calc_mo = safe_money(df_det_actual[df_det_actual["Tipo Item"] == "Mano de Obra"]["Costo Unitario"].sum()) if not df_det_actual.empty else 0.0
            calc_rep = safe_money(df_det_actual[df_det_actual["Tipo Item"] == "Repuestos"]["Precio Venta Unitario"].sum()) if not df_det_actual.empty else 0.0
            
            m_obra = ot_row_val1.number_input("Total Mano de Obra (L)", value=calc_mo, step=100.0, disabled=True)
            repuestos = ot_row_val2.number_input("Total Repuestos (L)", value=calc_rep, step=100.0, disabled=True)
            
            # BLOQUEO DE BOTON OT
            campos_obligatorios_ot = [f_crea, id_ot_val, nom_cli_ot, st.session_state.ot_form_data['ID Cliente']]
            btn_ot_disabled = any(not str(campo).strip() for campo in campos_obligatorios_ot)
            
            btn_ot_label = "Actualizar Orden" if st.session_state.ot_form_data['is_edit'] else "Crear Orden de Trabajo"
            if st.button(btn_ot_label, type="primary", use_container_width=True, disabled=btn_ot_disabled):
                # Aplicamos el filtro para precisión de punto flotante puro a todas las operaciones
                m_obra_safe = safe_money(m_obra)
                repuestos_safe = safe_money(repuestos)
                sub_venta = safe_money(m_obra_safe + repuestos_safe)
                costo_base = safe_money(m_obra_safe * 0.80)
                
                isv_raw = (sub_venta - safe_money(repuestos_safe * 0.15)) * 0.15 if t_ingreso == "Con Factura" else 0.0
                isv = safe_money(isv_raw)
                
                total_cobro = safe_money(sub_venta + isv)
                utilidad = safe_money(sub_venta - costo_base)
                
                nueva_ot = [
                    id_ot_val, f_crea.strftime("%Y-%m-%d"), 
                    f_tec.strftime("%Y-%m-%d") if f_tec else "", f_adm.strftime("%Y-%m-%d") if f_adm else "",
                    st.session_state.ot_form_data['ID Cliente'], st.session_state.ot_form_data['Nombre Cliente'], 
                    placa_ot_sel, km_ot_val, est_tec, est_adm, t_ingreso, f_pago,
                    m_obra_safe, repuestos_safe, costo_base, sub_venta, isv, total_cobro, utilidad
                ]
                
                guardar_registro("2_Ordenes de Trabajo", "ID Orden", id_ot_val, nueva_ot)
                
                st.success("Orden Guardada Correctamente.")
                if 'last_selected_ot_idx' in st.session_state:
                    del st.session_state['last_selected_ot_idx']
                st.rerun()

            st.divider()
            st.subheader("Servicios")
            
            s_col1, s_col2 = st.columns(2)
            curr_tipo_item = st.session_state.servicio_form_data['Tipo Item']
            tipo_item = s_col1.selectbox(":red[*] Tipo Item", ["Mano de Obra", "Repuestos"], index=0 if curr_tipo_item == "Mano de Obra" else 1)
            
            id_serv_auto = st.session_state.servicio_form_data['ID Servicio'] if st.session_state.servicio_form_data['ID Servicio'] else generar_id_servicio(id_ot_val)
            s_col2.text_input(":red[*] ID Servicio", value=id_serv_auto, disabled=True)
            
            desc_serv = st.text_input(":red[*] Descripcion", value=st.session_state.servicio_form_data['Descripcion'])
            
            s_col3, s_col4 = st.columns(2)
            lista_mecanicos = [""] + df_empleados["Nombre Completo"].dropna().tolist() if not df_empleados.empty else [""]
            curr_mec = st.session_state.servicio_form_data['Mecanico Asignado']
            mec_asignado = s_col3.selectbox(":red[*] Mecanico Asignado", options=lista_mecanicos, index=lista_mecanicos.index(curr_mec) if curr_mec in lista_mecanicos else 0)
            
            proveedor = s_col4.text_input("Proveedor", value=st.session_state.servicio_form_data['Proveedor'])
            
            s_col5, s_col6 = st.columns(2)
            cobra_cliente = s_col5.text_input("Cobra al Cliente", value=st.session_state.servicio_form_data['Cobra al Cliente'])
            
            curr_est_pago = st.session_state.servicio_form_data['Estado Pago Costo']
            opciones_est_pago = ["Pendiente", "Pagado", "N/A"]
            est_pago_costo = s_col6.selectbox("Estado Pago Costo", opciones_est_pago, index=opciones_est_pago.index(curr_est_pago) if curr_est_pago in opciones_est_pago else 0)
            
            s_col7, s_col8 = st.columns(2)
            fecha_pago_costo = s_col7.date_input("Fecha pago Costo", value=st.session_state.servicio_form_data['Fecha pago Costo'], format="DD/MM/YYYY")
            cantidad_serv = s_col8.number_input("Cantidad", min_value=1, step=1, value=int(st.session_state.servicio_form_data['Cantidad']))
            
            s_col9, s_col10, s_col11 = st.columns(3)
            costo_uni_input = s_col9.number_input("Costo Unitario (L)", format="%.2f", step=0.01, value=float(st.session_state.servicio_form_data['Costo Unitario']))
            subtotal_costo_input = s_col10.number_input("Subtotal Costo (L)", format="%.2f", step=0.01, value=float(st.session_state.servicio_form_data['Subtotal Costo']))
            precio_venta_uni_input = s_col11.number_input("Precio Venta Unitario (L)", format="%.2f", step=0.01, value=float(st.session_state.servicio_form_data['Precio Venta Unitario']))
            
            comentario_serv = st.text_area("Comentario", height=68, value=st.session_state.servicio_form_data['Comentario'])
            
            # BLOQUEO DE BOTON SERVICIO Y LOGICA MUTANTE
            btn_serv_disabled = not (tipo_item and id_serv_auto and desc_serv and mec_asignado)
            
            # Comparamos matemáticamente usando dataframes si el servicio ya existe
            existe_servicio = not df_detalles[(df_detalles["ID Orden"] == id_ot_val) & (df_detalles["ID Servicio"] == id_serv_auto)].empty
            btn_serv_label = "Actualizar Servicio en Orden" if existe_servicio else "Grabar Servicio en Orden"
            
            if st.button(btn_serv_label, type="primary", use_container_width=True, disabled=btn_serv_disabled):
                # Redondeo exacto a 2 cifras eliminando rastro de punto flotante
                costo_uni = safe_money(costo_uni_input)
                subtotal_costo = safe_money(subtotal_costo_input)
                precio_venta_uni = safe_money(precio_venta_uni_input)
                
                subtotal_venta_calc = safe_money(costo_uni) 
                ganancia_bruta_calc = safe_money(costo_uni + subtotal_costo - precio_venta_uni)
                
                nuevo_detalle = [
                    id_ot_val, id_serv_auto, tipo_item, desc_serv, mec_asignado,
                    proveedor, cobra_cliente, est_pago_costo, fecha_pago_costo.strftime("%Y-%m-%d"),
                    cantidad_serv, costo_uni, subtotal_costo, precio_venta_uni,
                    subtotal_venta_calc, ganancia_bruta_calc, comentario_serv
                ]
                
                guardar_registro("10_Detalles de Ordenes", "ID Servicio", id_serv_auto, nuevo_detalle)
                
                st.success(f"Servicio {id_serv_auto} guardado/actualizado en la orden.")
                if 'last_selected_det_idx' in st.session_state:
                    del st.session_state['last_selected_det_idx']
                
                # Resetear el form del servicio para forzar un nuevo input limpio
                st.session_state.servicio_form_data = {
                    'ID Servicio': '', 'Tipo Item': 'Mano de Obra', 'Descripcion': '',
                    'Mecanico Asignado': '', 'Proveedor': '', 'Cobra al Cliente': '',
                    'Estado Pago Costo': 'Pendiente', 'Fecha pago Costo': datetime.now(),
                    'Cantidad': 1, 'Costo Unitario': 0.0, 'Subtotal Costo': 0.0,
                    'Precio Venta Unitario': 0.0, 'Comentario': ''
                }
                st.rerun()

    with col_table:
        with st.container(height=800, border=False):
            st.write("### Ordenes de Trabajo")
            sel_ot = st.dataframe(df_ots, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row")
            
            if sel_ot and len(sel_ot.selection.rows) > 0:
                idx = sel_ot.selection.rows[0]
                if idx < len(df_ots):
                    if st.session_state.get('last_selected_ot_idx') != idx:
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
                            'Total Mano de Obra': float(data['Total Mano de Obra']), 'Total Repuestos': float(data['Total Repuestos']),
                            'is_edit': True
                        }
                        
                        # Limpiar el formulario de Servicios al cambiar de OT
                        st.session_state.servicio_form_data = {
                            'ID Servicio': '', 'Tipo Item': 'Mano de Obra', 'Descripcion': '',
                            'Mecanico Asignado': '', 'Proveedor': '', 'Cobra al Cliente': '',
                            'Estado Pago Costo': 'Pendiente', 'Fecha pago Costo': datetime.now(),
                            'Cantidad': 1, 'Costo Unitario': 0.0, 'Subtotal Costo': 0.0,
                            'Precio Venta Unitario': 0.0, 'Comentario': ''
                        }
                        if 'last_selected_det_idx' in st.session_state:
                            del st.session_state['last_selected_det_idx']
                            
                        st.session_state.last_selected_ot_idx = idx
                        st.rerun()
            else:
                if 'last_selected_ot_idx' in st.session_state:
                    del st.session_state['last_selected_ot_idx']

            st.divider()
            st.write("### Detalle de Ordenes de Trabajo")
            
            # Tabla interactiva para copiar datos a los campos de Servicios
            sel_det = st.dataframe(df_detalles, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row")
            
            if sel_det and len(sel_det.selection.rows) > 0:
                idx = sel_det.selection.rows[0]
                if idx < len(df_detalles):
                    if st.session_state.get('last_selected_det_idx') != idx:
                        data = df_detalles.iloc[idx]
                        st.session_state.servicio_form_data = {
                            'ID Servicio': data['ID Servicio'],
                            'Tipo Item': data['Tipo Item'],
                            'Descripcion': data['Descripcion'],
                            'Mecanico Asignado': data['Mecanico Asignado'],
                            'Proveedor': data['Proveedor'],
                            'Cobra al Cliente': data['Cobra al Cliente'],
                            'Estado Pago Costo': data['Estado Pago Costo'],
                            'Fecha pago Costo': pd.to_datetime(data['Fecha pago Costo']) if data['Fecha pago Costo'] else datetime.now(),
                            'Cantidad': int(data['Cantidad']) if not pd.isna(data['Cantidad']) else 1,
                            'Costo Unitario': float(data['Costo Unitario']) if not pd.isna(data['Costo Unitario']) else 0.0,
                            'Subtotal Costo': float(data['Subtotal Costo']) if not pd.isna(data['Subtotal Costo']) else 0.0,
                            'Precio Venta Unitario': float(data['Precio Venta Unitario']) if not pd.isna(data['Precio Venta Unitario']) else 0.0,
                            'Comentario': data['Comentario']
                        }
                        st.session_state.last_selected_det_idx = idx
                        st.rerun()
            else:
                if 'last_selected_det_idx' in st.session_state:
                    del st.session_state['last_selected_det_idx']

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

# --- BOTÓN DE RESETEO Y SINCRONIZACIÓN ---
st.sidebar.divider()
if st.sidebar.button("↻ Sincronizar / Forzar Descarga"):
    st.session_state.clear()
    st.rerun()

if st.sidebar.button("Resetear Formularios"):
    for key in ['cliente_vehiculo_data', 'ot_form_data', 'servicio_form_data', 'last_selected_vehiculo_idx', 'last_selected_ot_idx', 'last_selected_det_idx']:
        if key in st.session_state: del st.session_state[key]
    st.rerun()
