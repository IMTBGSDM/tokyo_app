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
    # Cargar credenciales desde st.secrets
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

# --- FUNCIONES DE BASE DE DATOS ---

def inicializar_sheets():
    existentes = [ws.title for ws in sh.worksheets()]
    for sheet, columns in SHEETS_CONFIG.items():
        if sheet not in existentes:
            # Crear la hoja y agregar las columnas si no existe
            nuevo_ws = sh.add_worksheet(title=sheet, rows="100", cols=str(len(columns)))
            nuevo_ws.update([columns])
        else:
            # Lógica de compatibilidad si la hoja ya existe (Ej: Renombrar columna Nombre Completo a Nombre Cliente)
            if sheet == "08_Clientes":
                ws = sh.worksheet(sheet)
                headers = ws.row_values(1)
                if "Nombre Completo" in headers:
                    idx = headers.index("Nombre Completo")
                    headers[idx] = "Nombre Cliente"
                    ws.update(f"A1:G1", [headers])

def leer_datos(sheet_name):
    try:
        worksheet = sh.worksheet(sheet_name)
        data = worksheet.get_all_records()
        if not data:
            return pd.DataFrame(columns=SHEETS_CONFIG.get(sheet_name, []))
        df = pd.DataFrame(data)
        
        # Mapeo por retrocompatibilidad de tus datos en Excel
        if sheet_name == "08_Clientes" and "Nombre Completo" in df.columns:
            df = df.rename(columns={"Nombre Completo": "Nombre Cliente"})
            
        return df
    except Exception as e:
        return pd.DataFrame(columns=SHEETS_CONFIG.get(sheet_name, []))

def guardar_datos(df, sheet_name):
    try:
        worksheet = sh.worksheet(sheet_name)
        worksheet.clear()
        
        df_clean = df.copy()
        
        # Formatear fechas a strings para que GSheets no arroje error de serialización JSON
        for col in df_clean.columns:
            if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d')
                
        # GSheets no soporta valores NaN (Nulos de pandas), reemplazamos por string vacío
        df_clean = df_clean.fillna("")
        
        # Preparar la lista de listas para insertar
        datos_a_guardar = [df_clean.columns.values.tolist()] + df_clean.values.tolist()
        
        # Subir todos los datos en una sola llamada a la API
        worksheet.update(datos_a_guardar)
    except Exception as e:
        st.error(f"Error al guardar datos en {sheet_name}: {e}")

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
    
    # Extraer el YY-XXXX del ID de Orden (ej. OT-26-0004 -> 26-0004)
    if id_orden and id_orden.count('-') >= 2:
        partes = id_orden.split('-')
        yy_xxxx = f"{partes[1]}-{partes[2]}"
    else:
        yy_xxxx = "00-0000"
        
    prefijo = f"SER-{yy_xxxx}"
    
    if df_detalles.empty:
        return f"{prefijo}-01"
    
    df_actual = df_detalles[df_detalles["ID Orden"] == id_orden]
    if df_actual.empty:
        return f"{prefijo}-01"
    
    try:
        ultimos_nums = df_actual["ID Servicio"].str.split('-').str[-1].astype(int)
        nuevo_num = ultimos_nums.max() + 1
    except:
        nuevo_num = 1
        
    return f"{prefijo}-{nuevo_num:02d}"

def limpiar_telefono(valor):
    if pd.isna(valor) or str(valor).lower() == 'nan': return ""
    return str(valor).replace('.0', '').strip()

# --- INICIALIZACIÓN ---
inicializar_sheets()

if 'cliente_vehiculo_data' not in st.session_state:
    st.session_state.cliente_vehiculo_data = {
        'ID Cliente': '', 'Nombre Cliente': '', 'Teléfono': '', 'Fecha': datetime.now(),
        'Correo': '', 'Dirección': '', 'Tipo': 'Nuevo', 'ID Vehículo': '', 'Placa': '',
        'Marca': '', 'Modelo': '', 'Año': 2024, 'Color': '', 'Kilometraje': 0, 'Notas': ''
    }

if 'ot_form_data' not in st.session_state:
    st.session_state.ot_form_data = {
        'ID Orden': '', 'Fecha Creacion': datetime.now(), 'Fecha Cierre Tecnico': None,
        'Fecha Cierra Admin': None, 'ID Cliente': '', 'Nombre Cliente': '',
        'Placa': '', 'Kilometraje': 0, 'Estado Tecnico': '', 'Estado Admin': '', 
        'Tipo Ingreso': 'Con Factura', 'Forma de Pago': 'Efectivo', 
        'Total Mano de Obra': 0.0, 'Total Repuestos': 0.0, 'is_edit': False
    }

with st.sidebar:
    st.title("🚗 TOKYO GARAGE")
    st.divider()
    menu_opcion = st.radio("Navegación", ["Master", "Clientes y Vehículos", "Ordenes de Trabajo", "Cotizaciones", "Nómina", "Empleados", "Kardex", "Finanzas", "Clientes"], index=2)

# --- MÓDULOS ---

if menu_opcion == "Master":
    st.header("Servicios Maestros")
    st.dataframe(leer_datos("1_Maestro"), use_container_width=True, hide_index=True)

elif menu_opcion == "Clientes y Vehículos":
    col_form, col_space, col_table = st.columns([2, 0.1, 3.2])
    df_clientes_base = leer_datos("08_Clientes")
    df_vehiculos_base = leer_datos("09_Carros por Cliente")

    with col_form:
        with st.container(height=750, border=False):
            st.subheader("Datos del Cliente")
            nom_cli = st.text_input(":red[*] Nombre Cliente", value=str(st.session_state.cliente_vehiculo_data['Nombre Cliente']))
            c_row_header_1, c_row_header_2 = st.columns(2)
            tipo_cli = c_row_header_1.selectbox(":red[*] Tipo", ["Frecuente", "Nuevo", "Flota"], index=["Frecuente", "Nuevo", "Flota"].index(st.session_state.cliente_vehiculo_data['Tipo']))
            id_cli_display = st.session_state.cliente_vehiculo_data['ID Cliente'] or generar_id("CLI", "08_Clientes", 4)
            c_row_header_2.text_input(":red[*] Código de Cliente", value=id_cli_display, disabled=True)
            c_row1_1, c_row1_2 = st.columns(2)
            tel_cli = c_row1_1.text_input(":red[*] Teléfono (8+ dígitos)", value=limpiar_telefono(st.session_state.cliente_vehiculo_data['Teléfono']))
            fecha_reg = c_row1_2.date_input(":red[*] Fecha de Registro", value=st.session_state.cliente_vehiculo_data['Fecha'], format="DD/MM/YYYY")
            email_cli = st.text_input("e-mail", value=str(st.session_state.cliente_vehiculo_data['Correo']))
            dir_cli = st.text_input("Dirección", value=str(st.session_state.cliente_vehiculo_data['Dirección']))
            
            st.divider()
            st.subheader("Datos del Vehículo")
            id_veh_display = st.session_state.cliente_vehiculo_data['ID Vehículo'] or generar_id("VEH", "09_Carros por Cliente", 5)
            st.text_input(":red[*] ID Vehículo", value=id_veh_display, disabled=True)
            v_row1_1, v_row1_2 = st.columns(2)
            placa_raw = v_row1_1.text_input(":red[*] Placa", value=str(st.session_state.cliente_vehiculo_data['Placa'])).upper()
            km_val = v_row1_2.number_input("Kilometraje Inicial", value=int(st.session_state.cliente_vehiculo_data['Kilometraje']), step=1000)
            v_row2_1, v_row2_2 = st.columns(2)
            marca_val = v_row2_1.text_input(":red[*] Marca", value=str(st.session_state.cliente_vehiculo_data['Marca']))
            modelo_val = v_row2_2.text_input(":red[*] Modelo", value=str(st.session_state.cliente_vehiculo_data['Modelo']))
            v_row3_1, v_row3_2 = st.columns(2)
            anio_val = v_row3_1.number_input(":red[*] Año", min_value=1950, max_value=2030, value=int(st.session_state.cliente_vehiculo_data['Año']))
            color_val = v_row3_2.text_input(":red[*] Color", value=str(st.session_state.cliente_vehiculo_data['Color']))
            notas_val = st.text_area("Notas Técnicas", value=str(st.session_state.cliente_vehiculo_data['Notas']))
            
            if st.button("Guardar", type="primary", use_container_width=True):
                new_df_c = df_clientes_base[df_clientes_base['ID Cliente'] != id_cli_display]
                reg_c = pd.DataFrame([[id_cli_display, fecha_reg.strftime("%Y-%m-%d"), nom_cli, tel_cli, email_cli, dir_cli, tipo_cli]], columns=SHEETS_CONFIG["08_Clientes"])
                guardar_datos(pd.concat([new_df_c, reg_c]), "08_Clientes")
                
                df_v_actual = leer_datos("09_Carros por Cliente")
                new_df_v = df_v_actual[df_v_actual['ID Vehículo'] != id_veh_display]
                reg_v = pd.DataFrame([[id_veh_display, placa_raw, marca_val, modelo_val, anio_val, color_val, id_cli_display, notas_val, nom_cli, km_val]], columns=SHEETS_CONFIG["09_Carros por Cliente"])
                guardar_datos(pd.concat([new_df_v, reg_v]), "09_Carros por Cliente")
                
                st.success("Registro actualizado exitosamente.")
                st.rerun()

    with col_table:
        st.write("### Vehículos por Cliente")
        df_view = df_vehiculos_base.copy()
        selected_row = st.dataframe(df_view, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row")
        if selected_row and len(selected_row.selection.rows) > 0:
            idx = selected_row.selection.rows[0]
            data_veh = df_view.iloc[idx]
            data_cli = df_clientes_base[df_clientes_base['ID Cliente'] == data_veh['ID Cliente']]
            if not data_cli.empty:
                c = data_cli.iloc[0]
                st.session_state.cliente_vehiculo_data = {
                    'ID Cliente': c['ID Cliente'], 'Nombre Cliente': c['Nombre Cliente'], 
                    'Teléfono': c['Teléfono / WhatsApp'], 'Fecha': pd.to_datetime(c['Fecha']),
                    'Correo': c['Correo Electrónico'], 'Dirección': c['Dirección'], 'Tipo': c['Tipo (Frecuente/Nuevo)'],
                    'ID Vehículo': data_veh['ID Vehículo'], 'Placa': data_veh['Placa'],
                    'Marca': data_veh['Marca'], 'Modelo': data_veh['Modelo'], 'Año': data_veh['Año'], 
                    'Color': data_veh['Color'], 'Kilometraje': data_veh['Kilometraje'], 'Notas': data_veh['Notas Técnicas (Detalles)']
                }
                st.rerun()

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
            
            # --- SECCIÓN 1: Identificación ---
            ot_row1_1, ot_row1_2 = st.columns(2)
            f_crea = ot_row1_1.date_input(":red[*] Fecha Creacion", value=st.session_state.ot_form_data['Fecha Creacion'], disabled=st.session_state.ot_form_data['is_edit'], format="DD/MM/YYYY")
            id_ot_val = st.session_state.ot_form_data['ID Orden'] if st.session_state.ot_form_data['ID Orden'] else generar_id_ot()
            ot_row1_2.text_input(":red[*] ID Orden", value=id_ot_val, disabled=True)
            
            # --- SECCIÓN 2: Cliente (Intercambiado y Mejorado) ---
            noms_cli_list = [""] + df_clientes["Nombre Cliente"].dropna().unique().tolist()
            curr_nom = st.session_state.ot_form_data['Nombre Cliente'] or ""
            curr_id = st.session_state.ot_form_data['ID Cliente'] or ""
            
            col_cli_1, col_cli_2 = st.columns(2)
            
            def sync_nombre_a_id():
                if st.session_state.sel_nom_cli_ot:
                    # Buscar el ID basado en el nombre en la hoja 08_Clientes
                    match = df_clientes[df_clientes["Nombre Cliente"] == st.session_state.sel_nom_cli_ot]
                    if not match.empty:
                        new_id = match.iloc[0]["ID Cliente"]
                        st.session_state.ot_form_data['ID Cliente'] = new_id
                        st.session_state.ot_form_data['Nombre Cliente'] = st.session_state.sel_nom_cli_ot
                        # Limpiar placa y KM al cambiar de cliente
                        st.session_state.ot_form_data['Placa'] = ""
                        st.session_state.ot_form_data['Kilometraje'] = 0
                else:
                    st.session_state.ot_form_data['ID Cliente'] = ""
                    st.session_state.ot_form_data['Nombre Cliente'] = ""
                    st.session_state.ot_form_data['Placa'] = ""
                    st.session_state.ot_form_data['Kilometraje'] = 0

            # Nombre es el selector (ahora primero)
            nom_cli_ot = col_cli_1.selectbox(":red[*] Nombre Cliente", options=noms_cli_list, 
                                           index=noms_cli_list.index(curr_nom) if curr_nom in noms_cli_list else 0,
                                           key="sel_nom_cli_ot", on_change=sync_nombre_a_id, disabled=st.session_state.ot_form_data['is_edit'])
            
            # ID es bloqueado (ahora segundo)
            col_cli_2.text_input(":red[*] ID Cliente", value=st.session_state.ot_form_data['ID Cliente'], disabled=True)

            # --- SECCIÓN 3: Vehículo (Placa y KM dinámicos) ---
            col_placa, col_km = st.columns(2)
            
            # Filtrar placas del cliente seleccionado desde 09_Carros por Cliente
            if st.session_state.ot_form_data['ID Cliente']:
                df_veh_cli = df_vehiculos[df_vehiculos["ID Cliente"] == st.session_state.ot_form_data['ID Cliente']]
                # Asegurar que existan las columnas necesarias para evitar KeyError
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
                    # Buscar KM en la tabla de vehículos
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
            
            # KM bloqueado hasta que haya placa
            km_disabled = True if not st.session_state.ot_form_data['Placa'] else False
            km_ot_val = col_km.number_input("Kilometraje Actual", value=int(st.session_state.ot_form_data['Kilometraje']), 
                                          disabled=km_disabled)

            # --- SECCIÓN 4: Estados y Fechas Cierre ---
            ot_row_est1, ot_row_est2 = st.columns(2)
            est_tec = ot_row_est1.text_input("Estado Tecnico", value=st.session_state.ot_form_data['Estado Tecnico'])
            est_adm = ot_row_est2.text_input("Estado Admin", value=st.session_state.ot_form_data['Estado Admin'])
            
            ot_row_f1, ot_row_f2 = st.columns(2)
            f_tec = ot_row_f1.date_input("Fecha Cierre Tecnico", value=st.session_state.ot_form_data['Fecha Cierre Tecnico'], format="DD/MM/YYYY")
            f_adm = ot_row_f2.date_input("Fecha Cierra Admin", value=st.session_state.ot_form_data['Fecha Cierra Admin'], format="DD/MM/YYYY")

            # --- SECCIÓN 5: Financiero ---
            ot_row_fin1, ot_row_fin2 = st.columns(2)
            t_ingreso = ot_row_fin1.selectbox("Tipo Ingreso", ["Con Factura", "Sin Factura"], index=["Con Factura", "Sin Factura"].index(st.session_state.ot_form_data['Tipo Ingreso']))
            f_pago = ot_row_fin2.selectbox("Forma de Pago", ["Efectivo", "Tarjeta", "Por definir"], index=["Efectivo", "Tarjeta", "Por definir"].index(st.session_state.ot_form_data['Forma de Pago']))
            
            ot_row_val1, ot_row_val2 = st.columns(2)
            
            # Cálculo automático de sumas desde 10_Detalles de Ordenes
            df_det_actual = df_detalles[df_detalles["ID Orden"] == id_ot_val]
            calc_mo = float(df_det_actual[df_det_actual["Tipo Item"] == "Mano de Obra"]["Costo Unitario"].sum()) if not df_det_actual.empty else 0.0
            calc_rep = float(df_det_actual[df_det_actual["Tipo Item"] == "Repuestos"]["Precio Venta Unitario"].sum()) if not df_det_actual.empty else 0.0
            
            m_obra = ot_row_val1.number_input("Mano de Obra (L)", value=calc_mo, step=100.0, disabled=True)
            repuestos = ot_row_val2.number_input("Repuestos (L)", value=calc_rep, step=100.0, disabled=True)
            
            # --- ACCIÓN PRINCIPAL ---
            btn_ot_label = "Actualizar Orden" if st.session_state.ot_form_data['is_edit'] else "Crear Orden de Trabajo"
            if st.button(btn_ot_label, type="primary", use_container_width=True):
                sub_venta = m_obra + repuestos
                costo_base = m_obra * 0.80
                isv = (sub_venta - (repuestos * 0.15)) * 0.15 if t_ingreso == "Con Factura" else 0.0
                total_cobro = sub_venta + isv
                utilidad = sub_venta - costo_base
                
                nueva_ot = [
                    id_ot_val, f_crea.strftime("%Y-%m-%d"), 
                    f_tec.strftime("%Y-%m-%d") if f_tec else "", f_adm.strftime("%Y-%m-%d") if f_adm else "",
                    st.session_state.ot_form_data['ID Cliente'], st.session_state.ot_form_data['Nombre Cliente'], 
                    placa_ot_sel, km_ot_val, est_tec, est_adm, t_ingreso, f_pago,
                    m_obra, repuestos, costo_base, sub_venta, isv, total_cobro, utilidad
                ]
                
                df_ots_new = df_ots[df_ots["ID Orden"] != id_ot_val]
                reg_ot = pd.DataFrame([nueva_ot], columns=SHEETS_CONFIG["2_Ordenes de Trabajo"])
                guardar_datos(pd.concat([df_ots_new, reg_ot]), "2_Ordenes de Trabajo")
                
                st.success("Orden Guardada Correctamente.")
                st.rerun()

            st.divider()
            st.subheader("Servicios")
            
            id_serv_auto = generar_id_servicio(id_ot_val)
            
            s_col1, s_col2 = st.columns(2)
            tipo_item = s_col1.selectbox("Tipo Item", ["Mano de Obra", "Repuestos"])
            s_col2.text_input("ID Servicio", value=id_serv_auto, disabled=True)
            
            desc_serv = st.text_input("Descripcion")
            
            s_col3, s_col4 = st.columns(2)
            lista_mecanicos = [""] + df_empleados["Nombre Completo"].dropna().tolist() if not df_empleados.empty else [""]
            mec_asignado = s_col3.selectbox("Mecanico Asignado", options=lista_mecanicos)
            proveedor = s_col4.text_input("Proveedor")
            
            s_col5, s_col6 = st.columns(2)
            cobra_cliente = s_col5.text_input("Cobra al Cliente")
            est_pago_costo = s_col6.selectbox("Estado Pago Costo", ["Pendiente", "Pagado", "N/A"])
            
            s_col7, s_col8 = st.columns(2)
            fecha_pago_costo = s_col7.date_input("Fecha pago Costo", value=datetime.now(), format="DD/MM/YYYY")
            cantidad_serv = s_col8.number_input("Cantidad", min_value=1, step=1, value=1)
            
            s_col9, s_col10, s_col11 = st.columns(3)
            costo_uni = s_col9.number_input("Costo Unitario (L)", format="%.2f", step=0.01)
            subtotal_costo = s_col10.number_input("Subtotal Costo (L)", format="%.2f", step=0.01)
            precio_venta_uni = s_col11.number_input("Precio Venta Unitario (L)", format="%.2f", step=0.01)
            
            comentario_serv = st.text_area("Comentario", height=68)
            
            if st.button("Grabar Servicio en Orden", type="primary", use_container_width=True):
                subtotal_venta_calc = costo_uni 
                ganancia_bruta_calc = costo_uni + subtotal_costo - precio_venta_uni
                
                nuevo_detalle = [
                    id_ot_val, id_serv_auto, tipo_item, desc_serv, mec_asignado,
                    proveedor, cobra_cliente, est_pago_costo, fecha_pago_costo.strftime("%Y-%m-%d"),
                    cantidad_serv, costo_uni, subtotal_costo, precio_venta_uni,
                    subtotal_venta_calc, ganancia_bruta_calc, comentario_serv
                ]
                
                df_detalles_actual = leer_datos("10_Detalles de Ordenes")
                df_detalles_new = pd.concat([df_detalles_actual, pd.DataFrame([nuevo_detalle], columns=SHEETS_CONFIG["10_Detalles de Ordenes"])])
                guardar_datos(df_detalles_new, "10_Detalles de Ordenes")
                st.success(f"Servicio {id_serv_auto} agregado a la orden.")
                st.rerun()

    with col_table:
        with st.container(height=800, border=False):
            st.write("### Ordenes de Trabajo")
            sel_ot = st.dataframe(df_ots, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row")
            
            if sel_ot and len(sel_ot.selection.rows) > 0:
                data = df_ots.iloc[sel_ot.selection.rows[0]]
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
                st.rerun()

            st.divider()
            st.write("### Detalle de Ordenes de Trabajo")
            st.dataframe(df_detalles, use_container_width=True, hide_index=True)

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

elif menu_opcion == "Clientes":
    st.header("Base Maestra de Clientes")
    st.dataframe(leer_datos("08_Clientes"), use_container_width=True, hide_index=True)

# --- BOTÓN DE RESETEO ---
if st.sidebar.button("Resetear Formularios"):
    for key in ['cliente_vehiculo_data', 'ot_form_data']:
        if key in st.session_state: del st.session_state[key]
    st.rerun()