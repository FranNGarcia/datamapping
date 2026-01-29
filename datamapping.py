import streamlit as st
import pandas as pd
import json
import os
from pathlib import Path

# Configuración de página
st.set_page_config(
    layout="wide"
)

# Título principal
st.title("Data Mapping")
st.markdown("---")

st.header("Mapeo de Datos CSV a Esquemas")
st.write("Sube un archivo CSV y mapea sus columnas a un esquema predefinido.")

# ===== SECCIÓN 1: SELECCIÓN DE ESQUEMA =====
st.subheader("1️⃣ Selecciona el Esquema de Destino")

# Buscar archivos JSON en el directorio actual
current_dir = Path(__file__).parent
json_files = list(current_dir.glob("*.json"))

if not json_files:
    st.error("No se encontraron archivos de esquema (.json) en el directorio.")
    st.stop()

# Selector de esquema
schema_names = ["--Selecciones esquema--"] + [f.name for f in json_files]
selected_schema = st.selectbox(
    "Selecciona un esquema:",
    schema_names,
    help="Elige el esquema al que deseas mapear tus datos"
)

# Cargar el esquema seleccionado
if selected_schema != "--Selecciones esquema--":
    schema_path = current_dir / selected_schema
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)
    
    # Mostrar información del esquema
    st.success(f"✅ Esquema cargado: **{schema.get('contract_name', 'Sin nombre')}**")
    
    # Extraer campos del esquema
    fields = schema.get('fields', [])
    
    # Crear tabla con información de los campos
    if fields:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.write(f"**Total de campos:** {len(fields)}")
            required_fields = [f for f in fields if f.get('required', False)]
            st.write(f"**Campos requeridos:** {len(required_fields)}")
        
        with col2:
            if st.checkbox("Ver detalles del esquema", value=False):
                st.json(schema)
        
        # Tabla de campos
        st.write("**Campos del esquema:**")
        field_data = []
        for field in fields:
            field_data.append({
                "Campo": field.get('target_field', ''),
                "Tipo": field.get('type', ''),
                "Requerido": "✅" if field.get('required', False) else "❌",
                "Descripción": field.get('description', '')
            })
        
        df_schema = pd.DataFrame(field_data)
        st.dataframe(df_schema, use_container_width=True, hide_index=True)
    else:
        st.warning("El esquema no contiene campos definidos.")

st.markdown("---")

# ===== SECCIÓN 2: CARGA DE CSV =====
st.subheader("2️⃣ Sube tu Archivo CSV")

uploaded_file = st.file_uploader(
    "Selecciona un archivo CSV",
    type=['csv'],
    help="Sube el archivo CSV que contiene los datos a mapear"
)

if uploaded_file is not None:
    try:
        # Leer el CSV
        df_source = pd.read_csv(uploaded_file)
        
        st.success(f"✅ Archivo cargado: **{uploaded_file.name}**")
        
        # Información del CSV
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Filas", f"{len(df_source):,}")
        with col2:
            st.metric("Columnas", len(df_source.columns))
        with col3:
            st.metric("Tamaño", f"{uploaded_file.size / 1024:.2f} KB")
        
        # Mostrar columnas disponibles
        with st.expander(f"Ver columnas disponibles ({len(df_source.columns)} columnas)"):
            st.write(", ".join([f"`{col}`" for col in df_source.columns]))
        
        # Preview de datos
        st.write("**Preview de los datos (primeras 5 filas):**")
        st.dataframe(df_source.head(5), use_container_width=True)
        
    except Exception as e:
        st.error(f"Error al cargar el archivo: {str(e)}")
        st.stop()
else:
    st.info("Sube un archivo CSV para continuar")

st.markdown("---")

# ===== SECCIÓN 3: MAPEO DE COLUMNAS =====
if uploaded_file is not None and selected_schema:
    st.subheader("3️⃣ Mapea las Columnas")
    st.write("Asocia cada campo del esquema con una columna de tu CSV")
    
    # Preparar opciones para los selectbox
    csv_columns = ["-- Sin mapear --"] + list(df_source.columns)
    
    # Diccionario para guardar los mapeos
    mappings = {}
    
    # Crear la interfaz de mapeo
    st.write("**Configuración de mapeo:**")
    
    # Separar campos requeridos y opcionales
    required_fields = [f for f in fields if f.get('required', False)]
    optional_fields = [f for f in fields if not f.get('required', False)]
    
    # Encabezados de columnas
    header1, header2, header3 = st.columns([2, 2, 3])
    with header1:
        st.markdown("**Campo Destino**")
    with header2:
        st.markdown("**Columna Origen (CSV)**")
    with header3:
        st.markdown("**Descripción**")
    
    st.markdown("")  # Espacio
    
    # Mapeo de campos requeridos
    if required_fields:
        st.markdown("##### 🔴 Campos Requeridos")
        for field in required_fields:
            col1, col2, col3 = st.columns([2, 2, 3])
            
            with col1:
                st.markdown(f"**`{field['target_field']}`**")
                st.caption(f"Tipo: `{field['type']}`")
            
            with col2:
                # Selectbox para elegir la columna del CSV
                selected_column = st.selectbox(
                    f"Columna origen",
                    csv_columns,
                    key=f"map_{field['target_field']}",
                    label_visibility="collapsed"
                )
                mappings[field['target_field']] = selected_column
            
            with col3:
                st.caption(field.get('description', ''))
                # Mostrar valores permitidos si es enum
                if field.get('type') == 'enum' and field.get('values'):
                    st.caption(f"Valores: {', '.join([f'`{v}`' for v in field['values']])}")
        
    
    # Mapeo de campos opcionales
    if optional_fields:
        st.markdown("##### ⚪ Campos Opcionales")
        for field in optional_fields:
            col1, col2, col3 = st.columns([2, 2, 3])
            
            with col1:
                st.markdown(f"`{field['target_field']}`")
                st.caption(f"Tipo: `{field['type']}`")
            
            with col2:
                # Selectbox para elegir la columna del CSV
                selected_column = st.selectbox(
                    f"Columna origen",
                    csv_columns,
                    key=f"map_{field['target_field']}",
                    label_visibility="collapsed"
                )
                mappings[field['target_field']] = selected_column
            
            with col3:
                st.caption(field.get('description', ''))
                # Mostrar valores permitidos si es enum
                if field.get('type') == 'enum' and field.get('values'):
                    st.caption(f"Valores: {', '.join([f'`{v}`' for v in field['values']])}")
    
    # Guardar mappings en session state
    st.session_state['mappings'] = mappings
    st.session_state['fields'] = fields
    st.session_state['df_source'] = df_source

st.markdown("---")
