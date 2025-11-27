import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import glob
from pathlib import Path
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, date
import boto3
from io import BytesIO

# Cargar variables de entorno
load_dotenv()

# ============================================
# CONFIGURACIÓN AZURE OPENAI
# ============================================
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")
DEPLOYMENT_NAME = os.getenv("DEPLOYMENT_NAME", "gpt-4o-mini")
API_VERSION = os.getenv("API_VERSION", "2024-02-15-preview")

# ============================================
# CONFIGURACIÓN FUENTE DE DATOS
# ============================================
TIPO_FUENTE = os.getenv("TIPO_FUENTE", "s3")
RUTA_DATOS = os.getenv("RUTA_DATOS", r"C:\Users\resendizjg\Downloads\piloto_resultados")

# Configuración AWS S3 (solo si TIPO_FUENTE = "s3")
# OPCIONAL: Si usas rol IAM en AWS (App Runner, EC2, Lambda), deja estas vacías
# REQUERIDO: Si ejecutas localmente, configura AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_PREFIX = os.getenv("S3_PREFIX", "")

# ============================================
# CONFIGURACIÓN STREAMLIT
# ============================================
st.set_page_config(
    page_title="Dashboard Retención", 
    page_icon="📊", 
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS mejorado
st.markdown("""
<style>
    #MainMenu, footer, header {visibility: hidden;}
    
    .stats-box {
        background: #f8f9fa;
        padding: 20px;
        border-radius: 8px;
        margin-top: 15px;
        border-left: 4px solid #667eea;
    }
    
    .stats-row {
        display: flex;
        justify-content: space-between;
        padding: 8px 0;
        border-bottom: 1px solid #e0e0e0;
    }
    
    .stats-row:last-child {
        border-bottom: none;
        font-weight: bold;
        padding-top: 12px;
        margin-top: 8px;
        border-top: 2px solid #667eea;
    }
    
    .stats-label {
        color: #555;
        font-weight: 500;
    }
    
    .stats-value {
        color: #333;
        font-weight: bold;
    }
    
    .stats-positive {
        color: #2ecc71;
    }
    
    .stats-negative {
        color: #e74c3c;
    }
    
    .config-info {
        background: #e3f2fd;
        padding: 10px;
        border-radius: 5px;
        border-left: 4px solid #2196f3;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# NORMALIZACIÓN DE DATOS
# ============================================

def extraer_fecha_nombre_archivo(nombre_archivo):
    """Extrae fecha del nombre del archivo"""
    import re
    
    # Patrón 1: 24Nov, 25Nov, etc.
    patron1 = r'(\d{1,2})(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
    match1 = re.search(patron1, nombre_archivo, re.IGNORECASE)
    
    if match1:
        dia = match1.group(1).zfill(2)
        mes = match1.group(2)
        año = "2024"
        
        meses = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
        }
        mes_num = meses.get(mes.lower(), '01')
        
        try:
            return pd.to_datetime(f"{año}-{mes_num}-{dia}")
        except:
            pass
    
    # Patrón 2: 2024-11-24, 20241124, etc.
    patron2 = r'(\d{4})[-_]?(\d{2})[-_]?(\d{2})'
    match2 = re.search(patron2, nombre_archivo)
    
    if match2:
        try:
            return pd.to_datetime(f"{match2.group(1)}-{match2.group(2)}-{match2.group(3)}")
        except:
            pass
    
    return None

def normalizar_respuestas(df):
    """Normaliza Si/si/SI/NO/no → formato consistente"""
    columnas_normalizar = ['Quiere baja', 'Acepto descuento']
    
    for col in columnas_normalizar:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df_lower = df[col].str.lower()
            df[col] = df_lower.apply(lambda x: 'Si' if x in ['si', 'sí'] else 'No')
    
    return df

# ============================================
# CARGAR DATOS - LOCAL O S3
# ============================================

@st.cache_data(ttl=300)
def cargar_datos_s3(bucket_name, prefix=""):
    """Carga archivos Excel desde S3"""
    try:
        # Intentar usar rol IAM primero (para EC2, App Runner, Lambda, etc.)
        # Si no hay rol, usar credenciales explícitas
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            # Credenciales explícitas (local o sin rol)
            s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION
            )
        else:
            # Usar rol IAM (recomendado para producción en AWS)
            s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            return None
        
        dfs = []
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.xlsx'):
                try:
                    file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
                    file_content = file_obj['Body'].read()
                    df = pd.read_excel(BytesIO(file_content))
                    
                    nombre_archivo = key.split('/')[-1]
                    df['archivo_origen'] = nombre_archivo
                    
                    fecha_archivo = extraer_fecha_nombre_archivo(nombre_archivo)
                    
                    if 'Fecha respuesta' in df.columns:
                        df['Fecha respuesta'] = pd.to_datetime(df['Fecha respuesta'], errors='coerce')
                        if fecha_archivo:
                            df['Fecha respuesta'].fillna(fecha_archivo, inplace=True)
                    elif fecha_archivo:
                        df['Fecha respuesta'] = fecha_archivo
                    
                    dfs.append(df)
                except Exception as e:
                    st.warning(f"Error al leer {key}: {str(e)}")
                    continue
        
        if not dfs:
            return None
        
        df = pd.concat(dfs, ignore_index=True)
        df = normalizar_respuestas(df)
        
        if 'Fecha respuesta' in df.columns:
            df['Fecha respuesta'] = pd.to_datetime(df['Fecha respuesta'], errors='coerce')
        
        return df
    
    except Exception as e:
        st.error(f"Error conectando a S3: {str(e)}")
        return None

@st.cache_data(ttl=300)
def cargar_datos_local(directorio):
    """Carga archivos Excel desde un directorio local"""
    archivos = glob.glob(f"{directorio}/*.xlsx")
    
    if not archivos:
        return None
    
    dfs = []
    for archivo in archivos:
        try:
            df = pd.read_excel(archivo)
            
            nombre_archivo = Path(archivo).name
            df['archivo_origen'] = nombre_archivo
            
            fecha_archivo = extraer_fecha_nombre_archivo(nombre_archivo)
            
            if 'Fecha respuesta' in df.columns:
                df['Fecha respuesta'] = pd.to_datetime(df['Fecha respuesta'], errors='coerce')
                if fecha_archivo:
                    df['Fecha respuesta'].fillna(fecha_archivo, inplace=True)
            elif fecha_archivo:
                df['Fecha respuesta'] = fecha_archivo
            
            dfs.append(df)
        except Exception as e:
            st.warning(f"Error al leer {Path(archivo).name}: {str(e)}")
            continue
    
    if not dfs:
        return None
    
    df = pd.concat(dfs, ignore_index=True)
    df = normalizar_respuestas(df)
    
    if 'Fecha respuesta' in df.columns:
        df['Fecha respuesta'] = pd.to_datetime(df['Fecha respuesta'], errors='coerce')
    
    return df

def cargar_datos():
    """Carga datos desde la fuente configurada (local o S3)"""
    if TIPO_FUENTE.lower() == "s3":
        return cargar_datos_s3(RUTA_DATOS, S3_PREFIX)
    else:
        return cargar_datos_local(RUTA_DATOS)

# ============================================
# CHATBOT CON AZURE OPENAI
# ============================================

def obtener_contexto_datos(df):
    """Genera contexto conciso de los datos"""
    total = len(df)
    bajas = len(df[df['Quiere baja'] == 'Si'])
    descuentos = len(df[df['Acepto descuento'] == 'Si'])
    retenidos = len(df[(df['Quiere baja'] == 'Si') & (df['Acepto descuento'] == 'Si')])
    tasa_ret = (retenidos/bajas*100) if bajas > 0 else 0
    
    return f"Total:{total}|Bajas:{bajas}({bajas/total*100:.1f}%)|Desc:{descuentos}({descuentos/total*100:.1f}%)|Ret:{retenidos}|Tasa:{tasa_ret:.1f}%"

def call_azure_openai(pregunta, contexto):
    """Llama a Azure OpenAI optimizado para rapidez"""
    if not AZURE_OPENAI_ENDPOINT or not AZURE_OPENAI_API_KEY:
        return "⚠️ Configura Azure OpenAI en el archivo .env"
    
    try:
        api_url = f"{AZURE_OPENAI_ENDPOINT}openai/deployments/{DEPLOYMENT_NAME}/chat/completions?api-version={API_VERSION}"
        
        headers = {
            "Content-Type": "application/json",
            "api-key": AZURE_OPENAI_API_KEY
        }
        
        messages = [
            {
                "role": "system",
                "content": "Eres asistente de análisis de retención. Responde en español, máximo 2-3 líneas, directo."
            },
            {
                "role": "user",
                "content": f"Datos: {contexto}\nPregunta: {pregunta}\nRespuesta breve:"
            }
        ]
        
        data = {
            "messages": messages,
            "max_tokens": 150,
            "temperature": 0.3,
            "top_p": 0.95
        }
        
        response = requests.post(api_url, headers=headers, json=data, timeout=10)
        
        if response.status_code == 200:
            return response.json()['choices'][0]['message']['content'].strip()
        else:
            return f"❌ Error {response.status_code}"
    
    except requests.Timeout:
        return "⏱️ Timeout. Intenta de nuevo."
    except Exception as e:
        return f"❌ Error: {str(e)[:100]}"

# ============================================
# INTERFAZ PRINCIPAL
# ============================================

st.title("Dashboard de Retención de Clientes")

# Tabs para Dashboard y Chat
tab1, tab2 = st.tabs(["📊 Dashboard", "💬 Chat IA"])

# ============================================
# TAB 1: DASHBOARD
# ============================================
with tab1:
    st.markdown("---")
    
    # Configuración (colapsada)
    with st.expander("⚙️ Configuración"):
        st.markdown(f"""
        <div class='config-info'>
            <strong>Configuración actual:</strong><br>
            <strong>Fuente de datos:</strong> {TIPO_FUENTE.upper()}<br>
            <strong>Ubicación:</strong> {RUTA_DATOS}<br>
            {f"<strong>Prefijo S3:</strong> {S3_PREFIX}<br>" if TIPO_FUENTE == "s3" and S3_PREFIX else ""}
            {f"<strong>Región AWS:</strong> {AWS_REGION}<br>" if TIPO_FUENTE == "s3" else ""}
        </div>
        """, unsafe_allow_html=True)
        
        if AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY:
            st.success(f"✅ Azure OpenAI configurado: {DEPLOYMENT_NAME}")
        else:
            st.warning("⚠️ Azure OpenAI no configurado. Configura el archivo .env")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Recargar Datos", use_container_width=True):
                st.cache_data.clear()
                st.rerun()
        
        with col2:
            if TIPO_FUENTE.lower() == "local":
                uploaded_file = st.file_uploader("📁 Subir archivo Excel", type=['xlsx'])
                if uploaded_file:
                    Path(RUTA_DATOS).mkdir(parents=True, exist_ok=True)
                    file_path = Path(RUTA_DATOS) / uploaded_file.name
                    with open(file_path, 'wb') as f:
                        f.write(uploaded_file.getbuffer())
                    st.success(f"✅ Archivo guardado: {uploaded_file.name}")
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.info("Modo S3 activo. Los archivos se cargan desde el bucket configurado.")

    # Cargar datos
    df = cargar_datos()

    if df is None:
        if TIPO_FUENTE.lower() == "s3":
            st.error(f"""
            ❌ No se encontraron archivos Excel en S3.
            
            **Configuración actual:**
            - Bucket: `{RUTA_DATOS}`
            - Prefijo: `{S3_PREFIX if S3_PREFIX else '(raíz del bucket)'}`
            - Región: `{AWS_REGION}`
            
            **Verifica:**
            1. Las credenciales AWS en el archivo .env
            2. Que el bucket existe y tiene archivos .xlsx
            3. Los permisos de acceso al bucket
            """)
        else:
            st.info(f"""
            ℹ️ No se encontraron archivos Excel en: `{RUTA_DATOS}`
            
            **Opciones:**
            1. Coloca tus archivos .xlsx en la carpeta `{RUTA_DATOS}`
            2. Usa el botón de subir archivo en Configuración
            """)
        st.stop()

    # ============================================
    # DISTRIBUCIÓN DE RESPUESTAS CON FILTROS
    # ============================================

    st.subheader("Distribución de Respuestas")

    # Filtros de fecha
    col1, col2, col3 = st.columns([2, 2, 2])

    # Fecha FIJA: 24 de noviembre como inicio
    FECHA_INICIO_FIJA = date(2024, 11, 24)

    # Calcular fecha máxima de los datos
    fecha_max_datos = df['Fecha respuesta'].max().date() if not df['Fecha respuesta'].isna().all() else datetime.now().date()

    # Inicializar reset en session_state
    if 'reset_filtros' not in st.session_state:
        st.session_state.reset_filtros = False

    with col1:
        fecha_inicio = st.date_input(
            "Fecha inicio",
            value=FECHA_INICIO_FIJA,
            min_value=FECHA_INICIO_FIJA,
            max_value=fecha_max_datos,
            help="Fecha de inicio del análisis (por defecto: 24 nov)",
            key=f"fecha_inicio_{st.session_state.get('reset_count', 0)}"
        )

    with col2:
        fecha_fin = st.date_input(
            "Fecha fin",
            value=fecha_max_datos,
            min_value=FECHA_INICIO_FIJA,
            max_value=fecha_max_datos,
            help="Selecciona fecha de fin",
            key=f"fecha_fin_{st.session_state.get('reset_count', 0)}"
        )

    with col3:
        if st.button("🔄 Resetear fechas", use_container_width=True):
            # Incrementar contador para forzar recreación de widgets
            st.session_state.reset_count = st.session_state.get('reset_count', 0) + 1
            st.rerun()

    # Filtrar datos por fecha
    df_filtrado = df.copy()
    if fecha_inicio and fecha_fin:
        df_filtrado = df_filtrado[
            (df_filtrado['Fecha respuesta'].dt.date >= fecha_inicio) &
            (df_filtrado['Fecha respuesta'].dt.date <= fecha_fin)
        ]

    # Mensaje informativo
    if len(df_filtrado) < len(df):
        st.info(f"📅 Mostrando {len(df_filtrado)} de {len(df)} registros (Rango: {fecha_inicio} a {fecha_fin})")
    else:
        st.success(f"✅ Mostrando todos los {len(df)} registros")

    # Recalcular totales con datos filtrados
    total_filtrado = len(df_filtrado)
    bajas_filtrado = len(df_filtrado[df_filtrado['Quiere baja'] == 'Si'])
    no_bajas_filtrado = len(df_filtrado[df_filtrado['Quiere baja'] == 'No'])
    descuentos_filtrado = len(df_filtrado[df_filtrado['Acepto descuento'] == 'Si'])
    no_descuentos_filtrado = len(df_filtrado[df_filtrado['Acepto descuento'] == 'No'])

    st.markdown("<br>", unsafe_allow_html=True)

    # Gráficas circulares
    col1, col2 = st.columns(2)

    # Gráfica 1: Quiere Baja
    with col1:
        st.markdown("### Intención de Baja")
        
        counts = df_filtrado['Quiere baja'].value_counts()
        
        fig = px.pie(
            values=counts.values,
            names=counts.index,
            color_discrete_sequence=['#2ecc71', '#e74c3c'],
            hole=0.4
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            textfont_size=16
        )
        
        fig.update_layout(
            showlegend=False,
            height=350,
            margin=dict(t=20, b=20, l=20, r=20)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Estadísticas
        pct_no_bajas = no_bajas_filtrado/total_filtrado*100 if total_filtrado > 0 else 0
        pct_bajas = bajas_filtrado/total_filtrado*100 if total_filtrado > 0 else 0
        
        st.markdown(f"""
        <div class='stats-box'>
            <h4 style='margin:0 0 15px 0; color:#2c3e50;'>Estadísticas de Intención de Baja</h4>
            <div class='stats-row'>
                <span class='stats-label'>Clientes satisfechos (No quieren baja)</span>
                <span class='stats-value stats-positive'>{no_bajas_filtrado:,} ({pct_no_bajas:.1f}%)</span>
            </div>
            <div class='stats-row'>
                <span class='stats-label'>Clientes en riesgo (Quieren baja)</span>
                <span class='stats-value stats-negative'>{bajas_filtrado:,} ({pct_bajas:.1f}%)</span>
            </div>
            <div class='stats-row'>
                <span class='stats-label'>Total de clientes</span>
                <span class='stats-value'>{total_filtrado:,}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Gráfica 2: Acepta Descuento
    with col2:
        st.markdown("### Aceptación de Descuentos")
        
        counts = df_filtrado['Acepto descuento'].value_counts()
        
        fig = px.pie(
            values=counts.values,
            names=counts.index,
            color_discrete_sequence=['#2ecc71', '#e74c3c'],
            hole=0.4
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            textfont_size=16
        )
        
        fig.update_layout(
            showlegend=False,
            height=350,
            margin=dict(t=20, b=20, l=20, r=20)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Estadísticas
        pct_descuentos = descuentos_filtrado/total_filtrado*100 if total_filtrado > 0 else 0
        pct_no_descuentos = no_descuentos_filtrado/total_filtrado*100 if total_filtrado > 0 else 0
        
        st.markdown(f"""
        <div class='stats-box'>
            <h4 style='margin:0 0 15px 0; color:#2c3e50;'>Estadísticas de Descuentos</h4>
            <div class='stats-row'>
                <span class='stats-label'>Aceptan descuento</span>
                <span class='stats-value stats-positive'>{descuentos_filtrado:,} ({pct_descuentos:.1f}%)</span>
            </div>
            <div class='stats-row'>
                <span class='stats-label'>No aceptan descuento</span>
                <span class='stats-value stats-negative'>{no_descuentos_filtrado:,} ({pct_no_descuentos:.1f}%)</span>
            </div>
            <div class='stats-row'>
                <span class='stats-label'>Total de clientes</span>
                <span class='stats-value'>{total_filtrado:,}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Footer
    archivos_info = f"{len(glob.glob(f'{RUTA_DATOS}/*.xlsx'))} archivo(s)" if TIPO_FUENTE == "local" else "S3"
    st.markdown("---")
    st.caption(f"{len(df):,} registros total | Mostrando {len(df_filtrado):,} registros | Fuente: {TIPO_FUENTE.upper()} ({archivos_info})")

# ============================================
# TAB 2: CHAT IA
# ============================================
with tab2:
    st.markdown("### 💬 Asistente IA de Retención")
    st.caption("Pregúntame sobre los datos de retención de clientes")
    st.markdown("---")
    
    # Inicializar historial de chat
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    
    # Mensaje de bienvenida si no hay mensajes
    if len(st.session_state.messages) == 0:
        with st.chat_message("assistant"):
            st.markdown("""
            👋 **¡Hola! Soy tu asistente de análisis de retención.**
            
            **Puedes preguntarme:**
            - ¿Cuál es la tasa de retención?
            - ¿Cuántos clientes están en riesgo?
            - ¿Qué porcentaje acepta descuentos?
            - ¿Cuáles son las tendencias?
            - Y mucho más...
            
            **Escribe tu pregunta abajo** ⬇️
            """)
    
    # Mostrar historial de chat
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Input del usuario
    if prompt := st.chat_input("Escribe tu pregunta aquí..."):
        # Agregar mensaje del usuario
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Obtener respuesta de IA
        with st.chat_message("assistant"):
            with st.spinner("Pensando..."):
                try:
                    df_temp = cargar_datos()
                    if df_temp is not None:
                        contexto = obtener_contexto_datos(df_temp)
                        respuesta = call_azure_openai(prompt, contexto)
                    else:
                        respuesta = "⚠️ No hay datos cargados para analizar."
                except:
                    respuesta = "⚠️ Error al cargar datos."
                
                st.markdown(respuesta)
        
        # Agregar respuesta al historial
        st.session_state.messages.append({"role": "assistant", "content": respuesta})
    
    # Botones de control
    st.markdown("---")
    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("🗑️ Limpiar conversación", use_container_width=True):
            st.session_state.messages = []
            st.rerun()
    with col_btn2:
        if st.button("🔄 Recargar datos", use_container_width=True):
            st.cache_data.clear()
            st.rerun()