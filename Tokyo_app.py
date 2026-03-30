import streamlit as st
import pandas as pd
from datetime import datetime
import re
import requests
import gspread
from google.oauth2.service_account import Credentials

# --- CONFIGURACIÓN Y CONSTANTES ---
st.set_page_config(page_title="Tokyo Garage - Gestión", layout="wide")

# Google Sheets Configuración
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1anidyNrk2dsmEo-5-Q9dj0bC7XnTxeUGGcZzEVsVBAY/edit"

# Regla de Oro #1: Estructura de datos intacta
SHEETS_CONFIG = {
    "1_Maestro": ["Código", "Categoría", "Descripción del Trabajo", "Tipo", "Costo Fijo"],
    "08_Clientes": ["ID Cliente", "Fecha", "Nombre Cliente", "Teléfono / WhatsApp", "Correo Electrónico", "Dirección", "Tipo (Frecuente/Nuevo)"],
    "09_Carros por Cliente": ["ID Vehículo", "Placa", "Marca", "Modelo", "Año", "Color", "ID Cliente", "Notas Técnicas (Detalles)", "Nombre Cliente", "Kilometraje"],
    "00_Catalogos": ["Area", "", "Especialidades", "", "Proveedores"], 
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
        "Subtotal Costo", "Ganancia Bruta", "Comentario"
    ], 
    "11_Cotizaciones": ["ID Cotizacion", "ID Cliente", "Nombre Cliente", "ID Vehiculo", "Fecha Cotizacion", "Precio", "Impuesto", "Total"],
    "3_Nomina": ["ID OT", "Servicio Realizado", "Técnico Asignado", "Fecha Terminado", "Subtotal Servicio", "Pago a Empleado", "Margen Bruto"],
    "7_Empleados": ["ID Empleado", "Nombre Completo", "Identidad", "Telefono", "Especialidad Principal", "Área Asignada (Control Interno)", "Tipo de Contratación", "Estado (Activo/Baja)"],
    "4_Kardex CI": ["ID Producto", "Nombre", "Categoría", "Stock Inicial", "Entradas (Compras)", "Salidas (Uso)", "Stock Actual", "Costo Unitario"],
    "BD_Veh": ["Marca", "Modelo"]
}

MARCAS_COMUNES = [
    "", "Toyota", "Nissan", "Honda", "Ford", "Chevrolet", "Hyundai", "Kia", "Mazda", 
    "Mitsubishi", "Volkswagen", "Suzuki", "Isuzu", "BMW", "Mercedes-Benz", "Audi", 
    "Jeep", "Lexus", "Subaru", "Volvo", "Peugeot", "Otra"
]

# --- ESTILOS CSS PERSONALIZADOS (Ajuste de proporciones y márgenes) ---
st.markdown("""
    <style>
    /* 1. Reducir márgenes laterales, aprovechar ancho y eliminar margen inferior */
    .main .block-container { 
        padding-top: 1.5rem !important; 
        padding-bottom: 0.5rem !important; 
        padding-left: 2.5rem !important; 
        padding-right: 2.5rem !important;
        max-width: 98% !important;
    }
    
    /* 2. Dar espacio a la barra de scroll en contenedores de formulario para que no tape campos */
    div[data-testid="stScrollableContainer"] > div {
        padding-right: 15px !important;
    }
    
    /* 3. Ocultar el pie de página predeterminado de Streamlit que roba espacio abajo */
    footer { display: none !important; }

    /* Ajuste de espaciado en subtítulos */
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

def cargar_toda_la_base():
    if 'db' not in st.session_state:
        st.session_state.db = {}
    for sheet in SHEETS_CONFIG.keys():
        try:
            worksheet = sh.worksheet(sheet)
            data = worksheet.get_all_values()
            if not data:
                df = pd.DataFrame(columns=SHEETS_CONFIG[sheet])
            else:
                headers = data[0]
                unique_headers = [h if h else f"Unnamed: {i}" for i, h in enumerate(headers)]
                df = pd.DataFrame(data[1:], columns=unique_headers)
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

def guardar_registro(sheet_name, id_col_name, id_valor, registro_dict):
    try:
        ws = sh.worksheet(sheet_name)
        headers = ws.row_values(1)
        
        registro_formateado = []
        for col in headers:
            val = registro_dict.get(col, "")
            if isinstance(val, (pd.Timestamp, datetime)):
                registro_formateado.append(val.strftime('%Y-%m-%d'))
            elif pd.isna(val) or val is None:
                registro_formateado.append("")
            elif isinstance(val, float):
                registro_formateado.append(f"{val:.2f}")
            else:
                registro_formateado.append(str(val))
                
        col_idx = headers.index(id_col_name) + 1 if id_col_name in headers else 1
        col_vals = ws.col_values(col_idx)
        
        if id_valor in col_vals:
            row_idx = col_vals.index(id_valor) + 1
            from gspread.utils import rowcol_to_a1
            rango_update = f"A{row_idx}:{rowcol_to_a1(row_idx, len(registro_formateado))}"
            ws.update(range_name=rango_update, values=[registro_formateado])
        else:
            ws.append_row(registro_formateado)
            
        df_actual = st.session_state.db[sheet_name]
        dict_para_df = {col: val for col, val in zip(headers, registro_formateado)}
            
        if id_valor in df_actual[id_col_name].values:
            idx = df_actual.index[df_actual[id_col_name] == id_valor].tolist()[0]
            for col_name in headers:
                if col_name in df_actual.columns:
                    df_actual.at[idx, col_name] = dict_para_df[col_name]
            st.session_state.db[sheet_name] = df_actual
        else:
            nuevo_df = pd.DataFrame([dict_para_df])
            st.session_state.db[sheet_name] = pd.concat([df_actual, nuevo_df], ignore_index=True)
            
    except Exception as e:
        st.error(f"Error al guardar registro en {sheet_name}: {e}")

def eliminar_registro(sheet_name, id_col_name, id_valor):
    try:
        ws = sh.worksheet(sheet_name)
        headers = ws.row_values(1)
        col_idx = headers.index(id_col_name) + 1 if id_col_name in headers else 1
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

def actualizar_catalogo(col_name, old_val, new_val, action="update"):
    try:
        ws = sh.worksheet("00_Catalogos")
        headers = ws.row_values(1)
        if col_name not in headers:
            return False
            
        col_idx = headers.index(col_name) + 1
        col_vals = ws.col_values(col_idx)
        data_vals = col_vals[1:] if len(col_vals) > 1 else []

        if action == "add":
            if new_val and new_val not in data_vals:
                data_vals.append(new_val)
        elif action == "delete":
            if old_val in data_vals:
                data_vals.remove(old_val)
        elif action == "update":
            if old_val in data_vals:
                idx = data_vals.index(old_val)
                data_vals[idx] = new_val
            elif new_val:
                data_vals.append(new_val)

        data_vals = [v for v in data_vals if str(v).strip() != ""]

        max_rows = max(len(col_vals), len(data_vals) + 1)
        update_matrix = [[headers[col_idx-1]]] + [[v] for v in data_vals]
        while len(update_matrix) < max_rows:
            update_matrix.append([""])

        from gspread.utils import rowcol_to_a1
        range_start = rowcol_to_a1(1, col_idx)
        range_end = rowcol_to_a1(max_rows, col_idx)
        ws.update(range_name=f"{range_start}:{range_end}", values=update_matrix)

        data = ws.get_all_values()
        unique_headers = [h if h else f"Unnamed: {i}" for i, h in enumerate(data[0])]
        st.session_state.db["00_Catalogos"] = pd.DataFrame(data[1:], columns=unique_headers)
        return True
    except Exception as e:
        st.error(f"Error actualizando catálogo: {e}")
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

@st.cache_data(ttl=86400)
def obtener_modelos_api(marca):
    if not marca or len(marca) < 2: return []
    try:
        url = f"https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMake/{marca}?format=json"
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            data = response.json()
            modelos = [item['Model_Name'] for item in data.get('Results', [])]
            return sorted(list(set(modelos)))
    except:
        return []
    return []

# --- INICIALIZACIÓN DE ESTADOS ---
if 'db_cargada' not in st.session_state:
    with st.spinner("Conectando y descargando base de datos segura desde la nube..."):
        inicializar_sheets()
        cargar_toda_la_base()
        st.session_state.db_cargada = True

if 'active_tab' not in st.session_state:
    st.session_state.active_tab = "Generar Orden de Trabajo"

if 'master_form_data' not in st.session_state:
    st.session_state.master_form_data = {
        'Código': '', 'Categoría': '', 'Descripción del Trabajo': '', 'Tipo': 'Estándar', 'Costo Fijo': 0.0
    }
if 'master_agregado_exitoso' not in st.session_state:
    st.session_state.master_agregado_exitoso = False
    st.session_state.id_ultimo_master = ""

if 'ot_seleccionada_servicios' not in st.session_state:
    st.session_state.ot_seleccionada_servicios = ""

# --- NAVEGACIÓN LATERAL ---
menu_items = [
    "Master", "Catálogos", "Clientes y Vehículos", "Generar Orden de Trabajo", 
    "Servicios", "Cerrar Orden de Trabajo", "Detalles de Ordenes de Trabajo", 
    "Cotizaciones", "Nómina", "Empleados", "Inventario", "Finanzas"
]

with st.sidebar:
    st.title("🚗 TOKYO GARAGE")
    st.divider()
    
    current_index = menu_items.index(st.session_state.active_tab) if st.session_state.active_tab in menu_items else 0
    selected_tab = st.radio("Navegación", menu_items, index=current_index)
    
    if selected_tab != st.session_state.active_tab:
        st.session_state.active_tab = selected_tab
        st.rerun()
        
    menu_opcion = st.session_state.active_tab 
    
    st.divider()
    if st.button("↻ Sincronizar / Forzar Descarga"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

# --- MÓDULOS ---

# --- SECCIÓN: MASTER ---
if menu_opcion == "Master":
    # Proporción homogeneizada [2, 0.05, 3]
    col_form, col_space, col_table = st.columns([2, 0.05, 3])
    df_maestro = leer_datos("1_Maestro")

    with col_form:
        with st.container(height=650, border=False):
            st.subheader("Gestión de Servicios Maestros")
            
            master_lock = st.session_state.master_agregado_exitoso
            
            m_r1_1, m_r1_2 = st.columns(2)
            codigo_val = m_r1_1.text_input(":red[*] Código", value=st.session_state.master_form_data.get('Código', ''), disabled=master_lock)
            categoria_val = m_r1_2.text_input(":red[*] Categoría", value=st.session_state.master_form_data.get('Categoría', ''), disabled=master_lock)
            
            desc_val = st.text_area(":red[*] Descripción del Trabajo", value=st.session_state.master_form_data.get('Descripción del Trabajo', ''), disabled=master_lock)
            
            c_m1, c_m2 = st.columns(2)
            opciones_tipo = ["Estándar", "Por Cotización"]
            curr_tipo = st.session_state.master_form_data.get('Tipo', 'Estándar')
            tipo_val = c_m1.selectbox(":red[*] Modalidad de Servicio (Tipo)", opciones_tipo, index=opciones_tipo.index(curr_tipo) if curr_tipo in opciones_tipo else 0, disabled=master_lock)
            
            costo_disabled = master_lock or (tipo_val == "Por Cotización")
            curr_costo = float(st.session_state.master_form_data.get('Costo Fijo', 0.0))
            costo_val = c_m2.number_input("Costo Fijo (L)", value=curr_costo if tipo_val == "Estándar" else 0.0, disabled=costo_disabled, step=1.0)
            
            st.write("")
            
            if not master_lock:
                btn_disabled = not (codigo_val and categoria_val and desc_val and tipo_val)
                
                if st.button("Crear / Actualizar Servicio", type="primary", use_container_width=True, disabled=btn_disabled):
                    dict_master = {
                        "Código": codigo_val,
                        "Categoría": categoria_val,
                        "Descripción del Trabajo": desc_val,
                        "Tipo": tipo_val,
                        "Costo Fijo": safe_money(costo_val) if tipo_val == "Estándar" else 0.0
                    }
                    guardar_registro("1_Maestro", "Código", codigo_val, dict_master)
                    
                    st.session_state.master_agregado_exitoso = True
                    st.session_state.id_ultimo_master = codigo_val
                    st.rerun()
            else:
                st.success(f"Servicio {st.session_state.id_ultimo_master} guardado exitosamente.")
                c_b1, c_b2 = st.columns(2)
                with c_b1:
                    if st.button("Limpiar / Nuevo", use_container_width=True):
                        st.session_state.master_agregado_exitoso = False
                        st.session_state.id_ultimo_master = ""
                        st.session_state.master_form_data = {'Código': '', 'Categoría': '', 'Descripción del Trabajo': '', 'Tipo': 'Estándar', 'Costo Fijo': 0.0}
                        st.rerun()
                with c_b2:
                    if st.button("Deshacer Servicio", type="secondary", use_container_width=True):
                        eliminar_registro("1_Maestro", "Código", st.session_state.id_ultimo_master)
                        st.session_state.master_agregado_exitoso = False
                        st.session_state.id_ultimo_master = ""
                        st.rerun()

    with col_table:
        with st.container(height=650, border=False):
            st.write("### Catálogo de Servicios Maestros")
            st.caption("Selecciona una fila para cargar sus datos y editarla.")
            sel_m = st.dataframe(df_maestro, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row")
            
            if sel_m and len(sel_m.selection.rows) > 0:
                idx = sel_m.selection.rows[0]
                if idx < len(df_maestro):
                    if st.session_state.get('last_master_idx') != idx:
                        data = df_maestro.iloc[idx]
                        st.session_state.master_form_data = {
                            'Código': str(data.get('Código', '')),
                            'Categoría': str(data.get('Categoría', '')),
                            'Descripción del Trabajo': str(data.get('Descripción del Trabajo', '')),
                            'Tipo': str(data.get('Tipo', 'Estándar')),
                            'Costo Fijo': float(data.get('Costo Fijo', 0.0)) if not pd.isna(data.get('Costo Fijo')) else 0.0
                        }
                        st.session_state.last_master_idx = idx
                        st.session_state.master_agregado_exitoso = False
                        st.rerun()
            else:
                if 'last_master_idx' in st.session_state:
                    del st.session_state['last_master_idx']

# --- SECCIÓN: CATÁLOGOS ---
elif menu_opcion == "Catálogos":
    st.header("Catálogos Generales")
    df_catalogos = leer_datos("00_Catalogos")
    
    c1, c2, c3 = st.columns(3)
    
    catalogos_config = [
        ("Area", c1, "Área"),
        ("Especialidades", c2, "Especialidades"),
        ("Proveedores", c3, "Proveedores")
    ]
    
    for col_name, st_col, title in catalogos_config:
        with st_col:
            with st.container(height=500, border=True):
                st.write(f"### {title}")
                
                if col_name in df_catalogos.columns:
                    df_sub = df_catalogos[[col_name]].copy()
                    df_sub = df_sub[df_sub[col_name].astype(str).str.strip() != ""]
                else:
                    df_sub = pd.DataFrame(columns=[col_name])
                    
                sel_cat = st.dataframe(df_sub, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"tbl_{col_name}")
                
                selected_val = ""
                if sel_cat and len(sel_cat.selection.rows) > 0:
                    idx = sel_cat.selection.rows[0]
                    selected_val = str(df_sub.iloc[idx][col_name])
                    
                input_val = st.text_input("Valor a gestionar", value=selected_val, key=f"in_{col_name}")
                
                btn_col1, btn_col2 = st.columns(2)
                
                if btn_col1.button("Crear/Actualizar", key=f"btn_save_{col_name}", type="primary", use_container_width=True):
                    if input_val.strip():
                        if selected_val:
                            actualizar_catalogo(col_name, selected_val, input_val.strip(), "update")
                        else:
                            actualizar_catalogo(col_name, "", input_val.strip(), "add")
                        st.rerun()
                        
                if btn_col2.button("Eliminar", key=f"btn_del_{col_name}", type="secondary", use_container_width=True):
                    if selected_val:
                        actualizar_catalogo(col_name, selected_val, "", "delete")
                        st.rerun()

# --- SECCIÓN: CLIENTES Y VEHÍCULOS ---
elif menu_opcion == "Clientes y Vehículos":
    # Proporción homogeneizada [2, 0.05, 3]
    col_form, col_space, col_table = st.columns([2, 0.05, 3])
    df_clientes_base = leer_datos("08_Clientes")
    df_vehiculos_base = leer_datos("09_Carros por Cliente")

    with col_form:
        with st.container(height=800, border=False):
            st.subheader("Datos del Cliente")
            c_row_header_1, c_row_header_2 = st.columns(2)
            
            tipo_cli_opciones = ["Frecuente", "Nuevo", "Flota"]
            curr_tipo = st.session_state.get('cliente_vehiculo_data', {}).get('Tipo', 'Nuevo')
            tipo_cli = c_row_header_1.selectbox(":red[*] Tipo", tipo_cli_opciones, index=tipo_cli_opciones.index(curr_tipo) if curr_tipo in tipo_cli_opciones else 1)
            
            def_tel = limpiar_telefono(st.session_state.get('cliente_vehiculo_data', {}).get('Teléfono', ''))
            def_fecha = st.session_state.get('cliente_vehiculo_data', {}).get('Fecha', datetime.now())
            
            if tipo_cli == "Frecuente":
                noms_existentes = [""] + sorted(df_clientes_base["Nombre Cliente"].dropna().unique().tolist())
                curr_nom = st.session_state.get('cliente_vehiculo_data', {}).get('Nombre Cliente', '')
                nom_cli = st.selectbox(":red[*] Nombre Cliente", options=noms_existentes, index=noms_existentes.index(curr_nom) if curr_nom in noms_existentes else 0)
                if nom_cli:
                    match_c = df_clientes_base[df_clientes_base["Nombre Cliente"] == nom_cli]
                    id_cli_display = match_c.iloc[0]["ID Cliente"] if not match_c.empty else generar_id("CLI", "08_Clientes", 4)
                else:
                    id_cli_display = generar_id("CLI", "08_Clientes", 4)
            else:
                nom_cli = st.text_input(":red[*] Nombre Cliente", value=str(st.session_state.get('cliente_vehiculo_data', {}).get('Nombre Cliente', '')))
                id_cli_display = generar_id("CLI", "08_Clientes", 4)
            
            c_row_header_2.text_input(":red[*] Código de Cliente", value=id_cli_display, disabled=True)
            
            c_row1_1, c_row1_2 = st.columns(2)
            tel_cli = c_row1_1.text_input(":red[*] Teléfono (8+ dígitos)", value=def_tel)
            fecha_reg = c_row1_2.date_input(":red[*] Fecha de Registro", value=def_fecha, format="DD/MM/YYYY")
            email_cli = st.text_input("e-mail", value=str(st.session_state.get('cliente_vehiculo_data', {}).get('Correo', '')))
            dir_cli = st.text_input("Dirección", value=str(st.session_state.get('cliente_vehiculo_data', {}).get('Dirección', '')))
            
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
            
            marca_sel = v_row2_1.selectbox(":red[*] Marca", MARCAS_COMUNES)
            if marca_sel == "Otra":
                marca_val = v_row2_1.text_input("Ingresar Marca Manualmente").title()
            else:
                marca_val = marca_sel
            
            modelos_api = obtener_modelos_api(marca_val) if marca_val else []
            
            if modelos_api:
                modelo_val = v_row2_2.selectbox(":red[*] Modelo", [""] + modelos_api)
                if not modelo_val:
                    modelo_custom = st.text_input("Ingresar modelo manualmente si no aparece arriba")
                    if modelo_custom: modelo_val = modelo_custom
            else:
                modelo_val = v_row2_2.text_input(":red[*] Modelo")
            
            v_row3_1, v_row3_2 = st.columns(2)
            anio_val = v_row3_1.number_input(":red[*] Año", value=datetime.now().year, step=1)
            color_val = v_row3_2.text_input(":red[*] Color")
            notas_val = st.text_area("Notas Técnicas", height=100)
            
            campos_obligatorios = [id_cli_display, fecha_reg, nom_cli, tel_cli, tipo_cli, id_veh_display, placa_raw, marca_val, modelo_val, anio_val, color_val]
            btn_disabled = any(not str(campo).strip() for campo in campos_obligatorios)
            
            st.write("") 
            if st.button("Guardar Datos", type="primary", use_container_width=True, disabled=btn_disabled):
                dict_cliente = {
                    "ID Cliente": id_cli_display, "Fecha": fecha_reg, "Nombre Cliente": nom_cli,
                    "Teléfono / WhatsApp": tel_cli, "Correo Electrónico": email_cli,
                    "Dirección": dir_cli, "Tipo (Frecuente/Nuevo)": tipo_cli
                }
                guardar_registro("08_Clientes", "ID Cliente", id_cli_display, dict_cliente)
                
                dict_vehiculo = {
                    "ID Vehículo": id_veh_display, "Placa": placa_raw, "Marca": marca_val,
                    "Modelo": modelo_val, "Año": anio_val, "Color": color_val,
                    "ID Cliente": id_cli_display, "Notas Técnicas (Detalles)": notas_val,
                    "Nombre Cliente": nom_cli, "Kilometraje": km_val
                }
                guardar_registro("09_Carros por Cliente", "ID Vehículo", id_veh_display, dict_vehiculo)
                
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
    # Proporción homogeneizada [2, 0.05, 3]
    col_form, col_space, col_table = st.columns([2, 0.05, 3])
    df_clientes = leer_datos("08_Clientes")
    df_vehiculos = leer_datos("09_Carros por Cliente")
    df_ots = leer_datos("2_Ordenes de Trabajo")
    
    df_ots_abiertas = df_ots[(df_ots["Estado Tecnico"].fillna("") != "Cerrado") | (df_ots["Estado Admin"].fillna("") != "Cerrado")]

    with col_form:
        with st.container(height=600, border=False):
            st.subheader("Generar Orden de Trabajo")
            
            ot_lock = st.session_state.get('ot_generada_exitosa', False)
            
            ot_row1_1, ot_row1_2 = st.columns(2)
            f_crea = ot_row1_1.date_input(":red[*] Fecha Creacion", value=st.session_state.get('ot_form_data', {}).get('Fecha Creacion', datetime.now()), format="DD/MM/YYYY", disabled=ot_lock)
            
            id_ot_val = st.session_state.id_ot_generada if ot_lock else generar_id_ot()
            ot_row1_2.text_input(":red[*] ID Orden", value=id_ot_val, disabled=True)
            
            st.divider()
            
            col_cli_1, col_cli_2 = st.columns(2)
            
            noms_cli_list = [""] + sorted(df_clientes["Nombre Cliente"].dropna().unique().tolist())
            curr_nom = st.session_state.get('ot_form_data', {}).get('Nombre Cliente', '')
            
            nom_cli_ot = col_cli_1.selectbox(":red[*] Nombre Cliente", options=noms_cli_list, 
                                            index=noms_cli_list.index(curr_nom) if curr_nom in noms_cli_list else 0,
                                            disabled=ot_lock)
            
            id_cli_calc = ""
            if nom_cli_ot:
                match_c = df_clientes[df_clientes["Nombre Cliente"] == nom_cli_ot]
                if not match_c.empty: id_cli_calc = match_c.iloc[0]["ID Cliente"]
            
            id_cli_ot = col_cli_2.text_input(":red[*] ID Cliente", value=id_cli_calc if not ot_lock else st.session_state.get('ot_form_data', {}).get('ID Cliente', ''), disabled=True)
            
            col_placa, col_km = st.columns(2)
            placas_list = [""]
            if id_cli_calc:
                df_veh_cli = df_vehiculos[df_vehiculos["ID Cliente"] == id_cli_calc]
                placas_list += df_veh_cli["Placa"].dropna().tolist()
            
            curr_placa = st.session_state.get('ot_form_data', {}).get('Placa', '')
            placa_ot_sel = col_placa.selectbox(":red[*] Placa del Vehículo", options=placas_list,
                                                index=placas_list.index(curr_placa) if curr_placa in placas_list else 0,
                                                disabled=ot_lock or not id_cli_calc)
            
            km_calc = 0
            if placa_ot_sel:
                match_v = df_vehiculos[(df_vehiculos["ID Cliente"] == id_cli_calc) & (df_vehiculos["Placa"] == placa_ot_sel)]
                if not match_v.empty: km_calc = match_v.iloc[0]["Kilometraje"]
            
            km_ot_val = col_km.number_input("Kilometraje Actual", value=int(km_calc) if not ot_lock else int(st.session_state.get('ot_form_data', {}).get('Kilometraje', 0)), disabled=ot_lock or not placa_ot_sel)

            st.write("")
            
            if not ot_lock:
                campos_obligatorios_ot = [f_crea, id_ot_val, nom_cli_ot, placa_ot_sel]
                btn_ot_disabled = any(not str(campo).strip() for campo in campos_obligatorios_ot)
                
                if st.button("Crear Orden de Trabajo", type="primary", use_container_width=True, disabled=btn_ot_disabled):
                    dict_ot = {
                        "ID Orden": id_ot_val, "Fecha Creacion": f_crea, "Fecha Cierre Tecnico": "",
                        "Fecha Cierra Admin": "", "ID Cliente": id_cli_calc, "Nombre Cliente": nom_cli_ot,
                        "Placa": placa_ot_sel, "Kilometraje": km_ot_val, "Estado Tecnico": "Abierto",
                        "Estado Admin": "Abierto", "Tipo Ingreso": "Con Factura", "Forma de Pago": "Por definir",
                        "Total Mano de Obra": 0.0, "Total Repuestos": 0.0, "Costo Total OT": 0.0,
                        "Subtotal Venta OT": 0.0, "ISV (15%)": 0.0, "Gran Total Cobrado": 0.0, "Utilidad Neta OT": 0.0
                    }
                    guardar_registro("2_Ordenes de Trabajo", "ID Orden", id_ot_val, dict_ot)
                    
                    st.session_state.ot_form_data = {
                        'ID Orden': id_ot_val, 'Fecha Creacion': f_crea, 'ID Cliente': id_cli_calc, 
                        'Nombre Cliente': nom_cli_ot, 'Placa': placa_ot_sel, 'Kilometraje': km_ot_val,
                        'Estado Tecnico': 'Abierto', 'Estado Admin': 'Abierto'
                    }
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
                        st.session_state.ot_form_data = {}
                        st.rerun()
                with c_btn2:
                    if st.button("Continuar con Servicios", type="primary", use_container_width=True):
                        st.session_state.ot_seleccionada_servicios = st.session_state.id_ot_generada
                        st.session_state.active_tab = "Servicios"
                        st.session_state.ot_generada_exitosa = False 
                        st.rerun()
                with c_btn3:
                    if st.button("Deshacer Orden", type="secondary", use_container_width=True):
                        eliminar_registro("2_Ordenes de Trabajo", "ID Orden", st.session_state.id_ot_generada)
                        st.session_state.ot_generada_exitosa = False
                        st.session_state.id_ot_generada = ""
                        st.rerun()

    with col_table:
        with st.container(height=600, border=False):
            st.write("### Ordenes de Trabajo (Abiertas)")
            st.dataframe(df_ots_abiertas, use_container_width=True, hide_index=True)

# --- SECCIÓN: SERVICIOS ---
elif menu_opcion == "Servicios":
    # Proporción homogeneizada [2, 0.05, 3]
    col_form, col_space, col_table = st.columns([2, 0.05, 3])
    df_detalles = leer_datos("10_Detalles de Ordenes")
    df_empleados = leer_datos("7_Empleados")
    df_ots = leer_datos("2_Ordenes de Trabajo")
    df_maestro = leer_datos("1_Maestro")
    df_catalogos = leer_datos("00_Catalogos")

    df_ots_abiertas = df_ots[(df_ots["Estado Tecnico"].fillna("") != "Cerrado") | (df_ots["Estado Admin"].fillna("") != "Cerrado")]

    with col_form:
        with st.container(height=800, border=False):
            st.subheader("Agregar Servicios a Orden")
            
            serv_lock = st.session_state.get('servicio_agregado_exitoso', False)
            
            r1c1, r1c2 = st.columns(2)
            fecha_creacion_serv = r1c1.date_input(":red[*] Fecha Creación", value=datetime.now(), format="DD/MM/YYYY", disabled=serv_lock)
            
            sel_ot_serv = st.session_state.get('ot_seleccionada_servicios', '')
            r1c2.text_input(":red[*] Seleccionar Orden de Trabajo (Clic en tabla)", value=sel_ot_serv, disabled=True)
            
            disabled_all = not bool(sel_ot_serv) or serv_lock
            
            s_col1, s_col2 = st.columns(2)
            modalidad_serv = s_col1.radio("Modalidad de Servicio", ["Estándar", "Por Cotización"], horizontal=True, disabled=disabled_all)
            
            id_serv_auto = st.session_state.id_ultimo_servicio if serv_lock else (generar_id_servicio_global(modalidad_serv) if sel_ot_serv else "")
            s_col2.text_input(":red[*] ID Servicio", value=id_serv_auto, disabled=True)
            
            st.write("")
            
            c_cat1, c_cat2 = st.columns(2)
            tipo_item = c_cat1.selectbox(":red[*] Tipo Item", ["Mano de Obra", "Repuestos"], disabled=disabled_all)
            
            df_m_filtrado = df_maestro[df_maestro["Tipo"] == modalidad_serv] if "Tipo" in df_maestro.columns else df_maestro
            categorias_list = [""] + df_m_filtrado["Categoría"].dropna().unique().tolist() if "Categoría" in df_m_filtrado.columns else [""]
            categoria_serv = c_cat2.selectbox(":red[*] Categoría", categorias_list, disabled=disabled_all)
            
            if modalidad_serv == "Por Cotización":
                desc_serv = st.text_input(":red[*] Descripción del Trabajo", disabled=disabled_all)
            else:
                opciones_desc = [""]
                if categoria_serv:
                    opciones_desc += df_m_filtrado[df_m_filtrado["Categoría"] == categoria_serv]["Descripción del Trabajo"].dropna().unique().tolist()
                desc_serv = st.selectbox(":red[*] Descripción del Trabajo", opciones_desc, disabled=disabled_all)
            
            s_col_dyn, s_col_cant = st.columns(2)
            
            if tipo_item == "Mano de Obra":
                lista_mecanicos = [""] + df_empleados["Nombre Completo"].dropna().tolist() if not df_empleados.empty else [""]
                mec_asignado = s_col_dyn.selectbox(":red[*] Mecanico Asignado", options=lista_mecanicos, disabled=disabled_all)
                proveedor = ""
            else: 
                prov_limpios = df_catalogos["Proveedores"][df_catalogos["Proveedores"].astype(str).str.strip() != ""] if "Proveedores" in df_catalogos.columns else pd.Series()
                lista_prov = [""] + prov_limpios.dropna().unique().tolist()
                proveedor = s_col_dyn.selectbox(":red[*] Proveedor", options=lista_prov, disabled=disabled_all)
                mec_asignado = ""
                
            cantidad_serv = s_col_cant.number_input("Cantidad", min_value=1, step=1, disabled=disabled_all)
            
            s_col9, s_col10 = st.columns(2)
            
            if tipo_item == "Mano de Obra":
                costo_fijo_calc = 0.0
                if desc_serv and modalidad_serv == "Estándar":
                    match_m = df_m_filtrado[df_m_filtrado["Descripción del Trabajo"] == desc_serv]
                    if not match_m.empty and "Costo Fijo" in match_m.columns:
                        costo_fijo_calc = safe_money(match_m.iloc[0]["Costo Fijo"])
                        
                is_costo_disabled = disabled_all or (modalidad_serv != "Por Cotización")
                costo_uni_input = s_col9.number_input("Costo Unitario (L)", value=float(costo_fijo_calc), disabled=is_costo_disabled)
            else:
                costo_uni_input = s_col9.number_input("Costo Unitario (L)", format="%.2f", step=0.01, disabled=disabled_all)
            
            subtotal_costo_calc = safe_money(cantidad_serv * costo_uni_input)
            s_col10.number_input("Subtotal Costo (L)", value=subtotal_costo_calc, disabled=True)
            
            comentario_serv = st.text_area("Comentario", height=68, disabled=disabled_all)
            
            if not serv_lock:
                btn_serv_disabled = disabled_all or not (sel_ot_serv and desc_serv and categoria_serv)
                if tipo_item == "Mano de Obra" and not mec_asignado:
                    btn_serv_disabled = True
                elif tipo_item == "Repuestos" and not proveedor:
                    btn_serv_disabled = True
                
                if st.button("Agregar Servicio a Orden", type="primary", use_container_width=True, disabled=btn_serv_disabled):
                    
                    dict_detalle = {
                        "Fecha Creación": fecha_creacion_serv, "ID Orden": sel_ot_serv, "ID Servicio": id_serv_auto,
                        "Tipo Item": tipo_item, "Descripcion": desc_serv, "Mecanico Asignado": mec_asignado,
                        "Proveedor": proveedor, "Cantidad": cantidad_serv, "Costo Unitario": safe_money(costo_uni_input),
                        "Subtotal Costo": subtotal_costo_calc, "Ganancia Bruta": "", "Comentario": comentario_serv
                    }
                    guardar_registro("10_Detalles de Ordenes", "ID Servicio", id_serv_auto, dict_detalle)
                    
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
                                'ID Cliente': data['ID Cliente'], 'Nombre Cliente': data['Nombre Cliente'],
                                'Placa': data['Placa'] if not pd.isna(data['Placa']) else "",
                                'Kilometraje': data['Kilometraje'] if not pd.isna(data['Kilometraje']) else 0,
                                'Estado Tecnico': str(data['Estado Tecnico']) if not pd.isna(data['Estado Tecnico']) else "Abierto",
                                'Estado Admin': str(data['Estado Admin']) if not pd.isna(data['Estado Admin']) else "Abierto",
                                'Tipo Ingreso': data['Tipo Ingreso'], 'Forma de Pago': data['Forma de Pago'],
                            }
                        st.session_state.active_tab = "Cerrar Orden de Trabajo"
                        st.session_state.servicio_agregado_exitoso = False
                        st.rerun()
                with c_btn3:
                    if st.button("Deshacer Servicio", type="secondary", use_container_width=True):
                        eliminar_registro("10_Detalles de Ordenes", "ID Servicio", st.session_state.id_ultimo_servicio)
                        st.session_state.servicio_agregado_exitoso = False
                        st.session_state.id_ultimo_servicio = ""
                        st.rerun()

    with col_table:
        with st.container(height=800, border=False):
            st.write("### Ordenes de Trabajo (Abiertas)")
            
            sel_ot_df = st.dataframe(df_ots_abiertas, use_container_width=True, hide_index=True, height=250, on_select="rerun", selection_mode="single-row")
            
            if sel_ot_df and len(sel_ot_df.selection.rows) > 0:
                idx = sel_ot_df.selection.rows[0]
                if idx < len(df_ots_abiertas):
                    selected_id_orden = df_ots_abiertas.iloc[idx]["ID Orden"]
                    if st.session_state.get('ot_seleccionada_servicios') != selected_id_orden:
                        st.session_state.ot_seleccionada_servicios = selected_id_orden
                        st.rerun()
            
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
                    
                    columnas_carrito = ["ID Servicio", "Tipo Item", "Descripcion", "Cantidad", "Subtotal Costo"]
                    
                    cols_finales = [c for c in columnas_carrito if c in df_cart.columns]
                    st.dataframe(df_cart[cols_finales], use_container_width=True, hide_index=True, height=250)
                else:
                    st.info("Aún no se han agregado servicios a esta Orden de Trabajo.")
            else:
                st.info("Selecciona una Orden de Trabajo para ver el carrito de servicios.")

# --- SECCIÓN: CERRAR ORDEN DE TRABAJO ---
elif menu_opcion == "Cerrar Orden de Trabajo":
    # Proporción homogeneizada [2, 0.05, 3]
    col_form, col_space, col_table = st.columns([2, 0.05, 3])
    df_ots = leer_datos("2_Ordenes de Trabajo")
    df_detalles = leer_datos("10_Detalles de Ordenes")
    
    df_ots_abiertas = df_ots[(df_ots["Estado Tecnico"].fillna("") != "Cerrado") | (df_ots["Estado Admin"].fillna("") != "Cerrado")]

    with col_form:
        id_ot_val = st.session_state.get('ot_form_data', {}).get('ID Orden', '')
        btn_cerrar_disabled = not id_ot_val 
            
        btn_col1, btn_col2 = st.columns(2)
        if btn_col1.button("⬅ Regresar a Servicios", use_container_width=True):
            st.session_state.active_tab = "Servicios"
            st.rerun()
            
        with st.container(height=750, border=False):
            st.subheader("Cerrar Orden de Trabajo")
            
            ot_row1_1, ot_row1_2 = st.columns(2)
            f_crea_val = st.session_state.get('ot_form_data', {}).get('Fecha Creacion', '')
            if isinstance(f_crea_val, datetime) or isinstance(f_crea_val, pd.Timestamp): 
                f_crea_val = f_crea_val.strftime("%d/%m/%Y")
            ot_row1_1.text_input("Fecha Creacion", value=str(f_crea_val), disabled=True)
            ot_row1_2.text_input("ID Orden", value=id_ot_val, disabled=True)
            
            col_cli_1, col_cli_2 = st.columns(2)
            col_cli_1.text_input("Nombre Cliente", value=st.session_state.get('ot_form_data', {}).get('Nombre Cliente', ''), disabled=True)
            col_cli_2.text_input("ID Cliente", value=st.session_state.get('ot_form_data', {}).get('ID Cliente', ''), disabled=True)

            col_placa, col_km = st.columns(2)
            col_placa.text_input("Placa del Vehículo", value=st.session_state.get('ot_form_data', {}).get('Placa', ''), disabled=True)
            col_km.number_input("Kilometraje Actual", value=int(st.session_state.get('ot_form_data', {}).get('Kilometraje', 0)), disabled=True)

            st.divider()
            st.write("##### Estado y Cierre Administrativo")
            
            ot_row_chk1, ot_row_chk2 = st.columns(2)
            is_tec_cerrado = st.session_state.get('ot_form_data', {}).get('Estado Tecnico', 'Abierto') == 'Cerrado'
            is_adm_cerrado = st.session_state.get('ot_form_data', {}).get('Estado Admin', 'Abierto') == 'Cerrado'
            
            cierre_tecnico = ot_row_chk1.checkbox("Cierre Técnico", value=is_tec_cerrado, disabled=not id_ot_val or is_tec_cerrado)
            cierre_admin = ot_row_chk2.checkbox("Cierre Administrativo", value=is_adm_cerrado, disabled=not id_ot_val or is_adm_cerrado)

            ot_row_fin1, ot_row_fin2 = st.columns(2)
            t_ingreso = ot_row_fin1.selectbox("Tipo Ingreso", ["Con Factura", "Sin Factura"], index=["Con Factura", "Sin Factura"].index(st.session_state.get('ot_form_data', {}).get('Tipo Ingreso', 'Con Factura')))
            
            opciones_pago = ["Por definir", "Efectivo", "Tarjeta", "Transferencia"]
            idx_pago = opciones_pago.index(st.session_state.get('ot_form_data', {}).get('Forma de Pago', 'Por definir')) if st.session_state.get('ot_form_data', {}).get('Forma de Pago') in opciones_pago else 0
            f_pago = ot_row_fin2.selectbox("Forma de Pago", opciones_pago, index=idx_pago)
            
            ot_row_val1, ot_row_val2 = st.columns(2)
            
            df_det_actual = df_detalles[df_detalles["ID Orden"] == id_ot_val]
            calc_mo = safe_money(df_det_actual[df_det_actual["Tipo Item"] == "Mano de Obra"]["Costo Unitario"].sum()) if not df_det_actual.empty else 0.0
            calc_rep = safe_money(df_det_actual[df_det_actual["Tipo Item"] == "Repuestos"]["Costo Unitario"].sum()) if not df_det_actual.empty else 0.0
            
            m_obra = ot_row_val1.number_input("Total Mano de Obra (L)", value=calc_mo, disabled=True)
            repuestos = ot_row_val2.number_input("Total Repuestos (L)", value=calc_rep, disabled=True)
            
        with btn_col2:
            if st.button("Actualizar Orden", type="primary", use_container_width=True, disabled=btn_cerrar_disabled):
                m_obra_safe = safe_money(m_obra)
                repuestos_safe = safe_money(repuestos)
                sub_venta = safe_money(m_obra_safe + repuestos_safe)
                costo_base = safe_money(m_obra_safe * 0.80)
                
                isv_raw = (sub_venta - safe_money(repuestos_safe * 0.15)) * 0.15 if t_ingreso == "Con Factura" else 0.0
                isv = safe_money(isv_raw)
                
                total_cobro = safe_money(sub_venta + isv)
                utilidad = safe_money(sub_venta - costo_base)
                
                f_crea_save = st.session_state.ot_form_data['Fecha Creacion']
                if isinstance(f_crea_save, (datetime, pd.Timestamp)): f_crea_save = f_crea_save.strftime("%Y-%m-%d")
                
                est_tec_save = "Cerrado" if cierre_tecnico else st.session_state.ot_form_data.get('Estado Tecnico', 'Abierto')
                est_adm_save = "Cerrado" if cierre_admin else st.session_state.ot_form_data.get('Estado Admin', 'Abierto')
                
                match_ot = df_ots[df_ots["ID Orden"] == id_ot_val]
                old_f_tec = match_ot.iloc[0]["Fecha Cierre Tecnico"] if not match_ot.empty else ""
                old_f_adm = match_ot.iloc[0]["Fecha Cierra Admin"] if not match_ot.empty else ""
                
                f_tec_save = datetime.now().strftime("%Y-%m-%d") if (cierre_tecnico and not is_tec_cerrado) else old_f_tec
                f_adm_save = datetime.now().strftime("%Y-%m-%d") if (cierre_admin and not is_adm_cerrado) else old_f_adm
                
                if pd.isna(f_tec_save): f_tec_save = ""
                if pd.isna(f_adm_save): f_adm_save = ""
                
                dict_ot_cerrada = {
                    "ID Orden": id_ot_val, "Fecha Creacion": f_crea_save, 
                    "Fecha Cierre Tecnico": f_tec_save, "Fecha Cierra Admin": f_adm_save,
                    "ID Cliente": st.session_state.ot_form_data['ID Cliente'], 
                    "Nombre Cliente": st.session_state.ot_form_data['Nombre Cliente'], 
                    "Placa": st.session_state.ot_form_data['Placa'], 
                    "Kilometraje": st.session_state.ot_form_data['Kilometraje'], 
                    "Estado Tecnico": est_tec_save, "Estado Admin": est_adm_save, 
                    "Tipo Ingreso": t_ingreso, "Forma de Pago": f_pago,
                    "Total Mano de Obra": m_obra_safe, "Total Repuestos": repuestos_safe, 
                    "Costo Total OT": costo_base, "Subtotal Venta OT": sub_venta, 
                    "ISV (15%)": isv, "Gran Total Cobrado": total_cobro, "Utilidad Neta OT": utilidad
                }
                guardar_registro("2_Ordenes de Trabajo", "ID Orden", id_ot_val, dict_ot_cerrada)
                
                st.success("Orden actualizada correctamente.")
                if 'last_selected_ot_cerrar_idx' in st.session_state: del st.session_state['last_selected_ot_cerrar_idx']
                if est_tec_save == "Cerrado" and est_adm_save == "Cerrado":
                     st.session_state.ot_form_data = {}
                st.rerun()

    with col_table:
        with st.container(height=800, border=False):
            st.write("### Ordenes de Trabajo (Abiertas)")
            sel_ot = st.dataframe(df_ots_abiertas, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row")
            
            if sel_ot and len(sel_ot.selection.rows) > 0:
                idx = sel_ot.selection.rows[0]
                if idx < len(df_ots_abiertas):
                    if st.session_state.get('last_selected_ot_cerrar_idx') != idx:
                        data = df_ots_abiertas.iloc[idx]
                        st.session_state.ot_form_data = {
                            'ID Orden': str(data['ID Orden']), 'Fecha Creacion': pd.to_datetime(data['Fecha Creacion']),
                            'ID Cliente': data['ID Cliente'], 'Nombre Cliente': data['Nombre Cliente'],
                            'Placa': data['Placa'] if not pd.isna(data['Placa']) else "",
                            'Kilometraje': data['Kilometraje'] if not pd.isna(data['Kilometraje']) else 0,
                            'Estado Tecnico': str(data['Estado Tecnico']) if not pd.isna(data['Estado Tecnico']) else "Abierto",
                            'Estado Admin': str(data['Estado Admin']) if not pd.isna(data['Estado Admin']) else "Abierto",
                            'Tipo Ingreso': data['Tipo Ingreso'], 'Forma de Pago': data['Forma de Pago'],
                        }
                        st.session_state.last_selected_ot_cerrar_idx = idx
                        st.rerun()
            else:
                if 'last_selected_ot_cerrar_idx' in st.session_state: del st.session_state['last_selected_ot_cerrar_idx']

# --- SECCIÓN: DETALLES DE ORDENES DE TRABAJO ---
elif menu_opcion == "Detalles de Ordenes de Trabajo":
    st.header("Detalles de Ordenes de Trabajo (Histórico de Servicios)")
    df_detalles = leer_datos("10_Detalles de Ordenes")
    st.dataframe(df_detalles, use_container_width=True, hide_index=True)

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

elif menu_opcion == "Inventario":
    st.header("Inventario Kardex")
    st.dataframe(leer_datos("4_Kardex CI"), use_container_width=True, hide_index=True)

elif menu_opcion == "Finanzas":
    st.header("Resumen Financiero")
    df_fin = leer_datos("2_Ordenes de Trabajo")
    if not df_fin.empty:
        st.dataframe(df_fin[["ID Orden", "Nombre Cliente", "Gran Total Cobrado", "Utilidad Neta OT"]], use_container_width=True, hide_index=True)
