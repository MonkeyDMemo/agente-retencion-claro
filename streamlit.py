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
import re

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
TIPO_FUENTE = os.getenv("TIPO_FUENTE", "s3")  # "local" o "s3"
RUTA_DATOS = os.getenv("RUTA_DATOS", r"C:\Users\resendizjg\Downloads\piloto_resultados")

# Configuración AWS S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-west-2")
S3_PREFIX = os.getenv("S3_PREFIX", "")

# ============================================
# CONFIGURACIÓN STREAMLIT
# ============================================
st.set_page_config(
    page_title="Dashboard Retencion", 
    page_icon="", 
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
    
    .duplicados-warning {
        background: #fff3cd;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #ffc107;
        margin: 10px 0;
    }
    
    .duplicados-ok {
        background: #d4edda;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #28a745;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# NORMALIZACIÓN DE DATOS
# ============================================

def extraer_fecha_nombre_archivo(nombre_archivo):
    """Extrae fecha del nombre del archivo (ej: _27Nov.xlsx)"""
    
    # Mapeo de meses en español e inglés
    meses = {
        'ene': 1, 'jan': 1, 'enero': 1,
        'feb': 2, 'febrero': 2,
        'mar': 3, 'marzo': 3,
        'abr': 4, 'apr': 4, 'abril': 4,
        'may': 5, 'mayo': 5,
        'jun': 6, 'junio': 6,
        'jul': 7, 'julio': 7,
        'ago': 8, 'aug': 8, 'agosto': 8,
        'sep': 9, 'sept': 9, 'septiembre': 9,
        'oct': 10, 'octubre': 10,
        'nov': 11, 'noviembre': 11,
        'dic': 12, 'dec': 12, 'diciembre': 12,
    }
    
    # Patrón: _27Nov.xlsx, _03Dic.xlsx, etc.
    patron = r'_?(\d{1,2})([A-Za-z]+)\.xlsx'
    match = re.search(patron, nombre_archivo, re.IGNORECASE)
    
    if match:
        dia = int(match.group(1))
        mes_str = match.group(2).lower()
        mes = meses.get(mes_str)
        
        if mes:
            año = datetime.now().year
            try:
                return pd.Timestamp(year=año, month=mes, day=dia)
            except:
                pass
    
    # Patrón alternativo: 2024-11-24, 20241124
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
            df[col] = df[col].astype(str).str.strip().str.upper()
            df[col] = df[col].apply(lambda x: 'Si' if x in ['SI', 'SÍ', 'YES', '1', 'TRUE'] else 'No')
    
    return df

def homologar_columnas(df):
    """Homologa nombres de columnas a formato estandar (Title Case)"""
    column_mapping = {
        # snake_case -> Title Case
        'customer_id': 'Customer id',
        'quiere_baja': 'Quiere baja',
        'acepto_descuento': 'Acepto descuento',
        'fecha_respuesta': 'Fecha respuesta',
        # Otras variantes
        'Customer Id': 'Customer id',
        'CUSTOMER_ID': 'Customer id',
        'Quiere Baja': 'Quiere baja',
        'QUIERE_BAJA': 'Quiere baja',
        'Acepto Descuento': 'Acepto descuento',
        'ACEPTO_DESCUENTO': 'Acepto descuento',
        'Fecha Respuesta': 'Fecha respuesta',
        'FECHA_RESPUESTA': 'Fecha respuesta',
    }
    
    df = df.rename(columns=column_mapping)
    return df

# ============================================
# DETECCIÓN DE DUPLICADOS
# ============================================

def detectar_duplicados(df):
    """
    Detecta cuentas duplicadas en el DataFrame.
    Retorna un diccionario con información de duplicados.
    """
    if 'Customer id' not in df.columns:
        return {'tiene_duplicados': False, 'total_duplicados': 0, 'detalle': []}
    
    # Encontrar customer_id duplicados
    conteo = df['Customer id'].value_counts()
    duplicados = conteo[conteo > 1]
    
    if len(duplicados) == 0:
        return {'tiene_duplicados': False, 'total_duplicados': 0, 'detalle': []}
    
    # Construir detalle de duplicados
    detalle = []
    for customer_id, count in duplicados.items():
        registros = df[df['Customer id'] == customer_id]
        archivos = registros['archivo_origen'].unique().tolist() if 'archivo_origen' in df.columns else []
        fechas = registros['fecha_corte'].unique().tolist() if 'fecha_corte' in df.columns else []
        
        detalle.append({
            'customer_id': customer_id,
            'ocurrencias': count,
            'archivos': archivos,
            'fechas': [str(f)[:10] for f in fechas if pd.notna(f)]
        })
    
    return {
        'tiene_duplicados': True,
        'total_duplicados': len(duplicados),
        'registros_afectados': int(duplicados.sum()),
        'detalle': detalle
    }

# ============================================
# CARGAR DATOS - LOCAL O S3
# ============================================

@st.cache_data(ttl=300)
def cargar_datos_s3(bucket_name, prefix=""):
    """Carga archivos Excel desde S3"""
    try:
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION
            )
        else:
            s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:
            return None
        
        dfs = []
        for obj in response['Contents']:
            key = obj['Key']
            nombre_archivo = key.split('/')[-1]
            
            # Filtrar archivos temporales
            if nombre_archivo.startswith('~$'):
                continue
                
            if key.endswith('.xlsx'):
                try:
                    file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
                    file_content = file_obj['Body'].read()
                    df = pd.read_excel(BytesIO(file_content))
                    
                    df['archivo_origen'] = nombre_archivo
                    
                    # Homologar columnas primero
                    df = homologar_columnas(df)
                    
                    # Extraer fecha del nombre del archivo como fecha_corte
                    fecha_archivo = extraer_fecha_nombre_archivo(nombre_archivo)
                    if fecha_archivo:
                        df['fecha_corte'] = fecha_archivo.normalize()
                    else:
                        df['fecha_corte'] = pd.NaT
                    
                    # Procesar Fecha respuesta
                    if 'Fecha respuesta' in df.columns:
                        df['Fecha respuesta'] = pd.to_datetime(df['Fecha respuesta'], errors='coerce')
                        if fecha_archivo:
                            df['Fecha respuesta'] = df['Fecha respuesta'].fillna(fecha_archivo)
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
        
        return df
    
    except Exception as e:
        st.error(f"Error conectando a S3: {str(e)}")
        return None

@st.cache_data(ttl=300)
def cargar_datos_local(directorio):
    """Carga archivos Excel desde un directorio local"""
    archivos = glob.glob(f"{directorio}/*.xlsx")
    
    # Filtrar archivos temporales de Excel (empiezan con ~$)
    archivos = [a for a in archivos if not Path(a).name.startswith('~$')]
    
    if not archivos:
        return None
    
    dfs = []
    for archivo in archivos:
        try:
            df = pd.read_excel(archivo)
            
            nombre_archivo = Path(archivo).name
            df['archivo_origen'] = nombre_archivo
            
            # Homologar columnas primero
            df = homologar_columnas(df)
            
            # Extraer fecha del nombre del archivo como fecha_corte
            fecha_archivo = extraer_fecha_nombre_archivo(nombre_archivo)
            if fecha_archivo:
                df['fecha_corte'] = fecha_archivo.normalize()
            else:
                df['fecha_corte'] = pd.NaT
            
            # Procesar Fecha respuesta
            if 'Fecha respuesta' in df.columns:
                df['Fecha respuesta'] = pd.to_datetime(df['Fecha respuesta'], errors='coerce')
                if fecha_archivo:
                    df['Fecha respuesta'] = df['Fecha respuesta'].fillna(fecha_archivo)
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
    
    return df

def cargar_datos():
    """Carga datos desde la fuente configurada"""
    if TIPO_FUENTE.lower() == "s3":
        return cargar_datos_s3(RUTA_DATOS, S3_PREFIX)
    else:
        return cargar_datos_local(RUTA_DATOS)

# ============================================
# GRÁFICAS DE BARRAS APILADAS
# ============================================

def crear_grafica_barras_diaria(df):
    """Crea grafica de barras apiladas por fecha de corte (diaria)"""
    
    # Agrupar por fecha_corte
    df['acepto'] = df['Acepto descuento'] == 'Si'
    
    resumen = df.groupby('fecha_corte').agg(
        total=('Customer id', 'count'),
        aceptaron=('acepto', 'sum')
    ).reset_index()
    
    resumen['no_aceptaron'] = resumen['total'] - resumen['aceptaron']
    resumen['pct_aceptaron'] = (resumen['aceptaron'] / resumen['total'] * 100).round(1)
    resumen['pct_no_aceptaron'] = (resumen['no_aceptaron'] / resumen['total'] * 100).round(1)
    resumen = resumen.sort_values('fecha_corte')
    resumen['fecha_str'] = resumen['fecha_corte'].dt.strftime('%Y-%m-%d')
    
    # Crear gráfica
    fig = go.Figure()
    
    # Barras de aceptaron
    fig.add_trace(go.Bar(
        name='Aceptaron',
        x=resumen['fecha_str'],
        y=resumen['aceptaron'],
        marker_color='#2ecc71',
        text=[f"{int(v)} ({p}%)" for v, p in zip(resumen['aceptaron'], resumen['pct_aceptaron'])],
        textposition='inside',
        textfont=dict(color='white', size=12, family='Arial Black')
    ))
    
    # Barras de no aceptaron
    fig.add_trace(go.Bar(
        name='No aceptaron',
        x=resumen['fecha_str'],
        y=resumen['no_aceptaron'],
        marker_color='#e74c3c',
        text=[f"{int(v)} ({p}%)" for v, p in zip(resumen['no_aceptaron'], resumen['pct_no_aceptaron'])],
        textposition='inside',
        textfont=dict(color='white', size=12, family='Arial Black')
    ))
    
    fig.update_layout(
        barmode='stack',
        title=dict(text='Aceptación por fecha de corte', font=dict(size=18)),
        xaxis_title='Fecha de corte',
        yaxis_title='Total de llamadas al agente',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        height=450,
        margin=dict(t=80, b=50)
    )
    
    return fig, resumen

def crear_grafica_barras_acumulada(df):
    """Crea grafica de barras apiladas acumulada"""
    
    df['acepto'] = df['Acepto descuento'] == 'Si'
    
    resumen = df.groupby('fecha_corte').agg(
        total=('Customer id', 'count'),
        aceptaron=('acepto', 'sum')
    ).reset_index()
    
    resumen['no_aceptaron'] = resumen['total'] - resumen['aceptaron']
    resumen = resumen.sort_values('fecha_corte')
    
    # Calcular acumulados
    resumen['aceptaron_acum'] = resumen['aceptaron'].cumsum()
    resumen['no_aceptaron_acum'] = resumen['no_aceptaron'].cumsum()
    resumen['total_acum'] = resumen['total'].cumsum()
    resumen['pct_aceptaron_acum'] = (resumen['aceptaron_acum'] / resumen['total_acum'] * 100).round(1)
    resumen['pct_no_aceptaron_acum'] = (resumen['no_aceptaron_acum'] / resumen['total_acum'] * 100).round(1)
    resumen['fecha_str'] = resumen['fecha_corte'].dt.strftime('%Y-%m-%d')
    
    # Crear gráfica
    fig = go.Figure()
    
    # Barras acumuladas de aceptaron
    fig.add_trace(go.Bar(
        name='Aceptaron acumulado',
        x=resumen['fecha_str'],
        y=resumen['aceptaron_acum'],
        marker_color='#2ecc71',
        text=[f"{int(v)}<br>({p}%)" for v, p in zip(resumen['aceptaron_acum'], resumen['pct_aceptaron_acum'])],
        textposition='inside',
        textfont=dict(color='white', size=11, family='Arial Black')
    ))
    
    # Barras acumuladas de no aceptaron
    fig.add_trace(go.Bar(
        name='No aceptaron acumulado',
        x=resumen['fecha_str'],
        y=resumen['no_aceptaron_acum'],
        marker_color='#e74c3c',
        text=[f"{int(v)}<br>({p}%)" for v, p in zip(resumen['no_aceptaron_acum'], resumen['pct_no_aceptaron_acum'])],
        textposition='inside',
        textfont=dict(color='white', size=11, family='Arial Black')
    ))
    
    fig.update_layout(
        barmode='stack',
        title=dict(text='Volumen acumulado de aceptación por fecha de corte', font=dict(size=18)),
        xaxis_title='Fecha de corte',
        yaxis_title='Total acumulado de llamadas',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='left', x=0),
        height=450,
        margin=dict(t=80, b=50)
    )
    
    return fig, resumen

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
    """Llama a Azure OpenAI"""
    if not AZURE_OPENAI_ENDPOINT or not AZURE_OPENAI_API_KEY:
        return "Configura Azure OpenAI en el archivo .env"
    
    try:
        api_url = f"{AZURE_OPENAI_ENDPOINT}openai/deployments/{DEPLOYMENT_NAME}/chat/completions?api-version={API_VERSION}"
        
        headers = {
            "Content-Type": "application/json",
            "api-key": AZURE_OPENAI_API_KEY
        }
        
        messages = [
            {"role": "system", "content": "Eres asistente de analisis de retencion. Responde en espanol, maximo 2-3 lineas, directo."},
            {"role": "user", "content": f"Datos: {contexto}\nPregunta: {pregunta}\nRespuesta breve:"}
        ]
        
        data = {"messages": messages, "max_tokens": 150, "temperature": 0.3}
        
        response = requests.post(api_url, headers=headers, json=data, timeout=10)
        
        if response.status_code == 200:
            return response.json()['choices'][0]['message']['content'].strip()
        else:
            return f"Error {response.status_code}"
    
    except requests.Timeout:
        return "Timeout. Intenta de nuevo."
    except Exception as e:
        return f"Error: {str(e)[:100]}"

# ============================================
# INTERFAZ PRINCIPAL
# ============================================

st.title("Dashboard Agente de Retencion de Claro Perú 🔴")

# Tabs
tab1, tab2 = st.tabs(["Dashboard", "Chat IA"])

# ============================================
# TAB 1: DASHBOARD
# ============================================
with tab1:
    
    # Configuracion (colapsada)
    with st.expander("Configuracion"):
        st.markdown(f"""
        <div class='config-info'>
            <strong>Fuente:</strong> {TIPO_FUENTE.upper()} | 
            <strong>Ubicacion:</strong> {RUTA_DATOS}
            {f" | <strong>Prefijo:</strong> {S3_PREFIX}" if TIPO_FUENTE == "s3" and S3_PREFIX else ""}
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("Recargar Datos", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # Cargar datos
    df = cargar_datos()

    if df is None:
        st.error(f"No se encontraron archivos Excel en: {RUTA_DATOS}")
        st.stop()

    # ============================================
    # FILTROS DE FECHA
    # ============================================
    st.subheader("Filtros")
    
    col1, col2, col3 = st.columns([2, 2, 2])

    # Calcular fechas min/max
    fecha_min = df['fecha_corte'].min().date() if not df['fecha_corte'].isna().all() else date(2024, 11, 24)
    fecha_max = datetime.now().date()  # Siempre la fecha de hoy

    if 'reset_count' not in st.session_state:
        st.session_state.reset_count = 0

    with col1:
        fecha_inicio = st.date_input(
            "Fecha inicio",
            value=fecha_min,
            min_value=fecha_min,
            max_value=fecha_max,
            key=f"fecha_inicio_{st.session_state.reset_count}"
        )

    with col2:
        fecha_fin = st.date_input(
            "Fecha fin",
            value=fecha_max,
            min_value=fecha_min,
            max_value=fecha_max,
            key=f"fecha_fin_{st.session_state.reset_count}"
        )

    with col3:
        if st.button("Resetear fechas", use_container_width=True):
            st.session_state.reset_count += 1
            st.rerun()

    # Filtrar por fecha_corte
    df_filtrado = df.copy()
    if fecha_inicio and fecha_fin:
        df_filtrado = df_filtrado[
            (df_filtrado['fecha_corte'].dt.date >= fecha_inicio) &
            (df_filtrado['fecha_corte'].dt.date <= fecha_fin)
        ]

    # Info de registros
    if len(df_filtrado) < len(df):
        st.info(f"Mostrando {len(df_filtrado):,} de {len(df):,} registros ({fecha_inicio} a {fecha_fin})")
    else:
        st.success(f"Mostrando todos los {len(df):,} registros")

    st.markdown("---")

    # ============================================
    # GRAFICAS DE BARRAS APILADAS (PRIMERO)
    # ============================================
    st.subheader("Aceptacion por Fecha de Corte")
    
    if len(df_filtrado) > 0 and not df_filtrado['fecha_corte'].isna().all():
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_diaria, resumen_diario = crear_grafica_barras_diaria(df_filtrado)
            st.plotly_chart(fig_diaria, use_container_width=True)
        
        with col2:
            fig_acum, resumen_acum = crear_grafica_barras_acumulada(df_filtrado)
            st.plotly_chart(fig_acum, use_container_width=True)
        
        # Tabla resumen
        st.markdown("### Resumen por Fecha")
        resumen_tabla = resumen_diario[['fecha_str', 'aceptaron', 'no_aceptaron', 'total', 'pct_aceptaron']].copy()
        resumen_tabla.columns = ['Fecha', 'Aceptaron', 'No Aceptaron', 'Total', '% Aceptacion']
        st.dataframe(resumen_tabla, use_container_width=True, hide_index=True)
        
    else:
        st.warning("No hay datos con fecha de corte valida para graficar")

    st.markdown("---")

    # ============================================
    # ESTADISTICAS GENERALES
    # ============================================
    st.subheader("Estadisticas Generales")
    
    total = len(df_filtrado)
    aceptaron = len(df_filtrado[df_filtrado['Acepto descuento'] == 'Si'])
    no_aceptaron = len(df_filtrado[df_filtrado['Acepto descuento'] == 'No'])
    quieren_baja = len(df_filtrado[df_filtrado['Quiere baja'] == 'Si'])
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Registros", f"{total:,}")
    with col2:
        st.metric("Aceptaron Descuento", f"{aceptaron:,}", f"{aceptaron/total*100:.1f}%" if total > 0 else "0%")
    with col3:
        st.metric("No Aceptaron", f"{no_aceptaron:,}", f"{no_aceptaron/total*100:.1f}%" if total > 0 else "0%")
    with col4:
        st.metric("Quieren Baja", f"{quieren_baja:,}", f"{quieren_baja/total*100:.1f}%" if total > 0 else "0%")

    st.markdown("---")

    # ============================================
    # SECCION DE DUPLICADOS (AL FINAL)
    # ============================================
    st.subheader("Verificacion de Datos")
    
    duplicados_info = detectar_duplicados(df)
    
    if duplicados_info['tiene_duplicados']:
        st.markdown(f"""
        <div class='duplicados-warning'>
            <h4 style='margin:0; color:#856404;'>Se encontraron cuentas duplicadas</h4>
            <p style='margin:10px 0 0 0;'>
                <strong>{duplicados_info['total_duplicados']}</strong> customer_id aparecen mas de una vez 
                ({duplicados_info['registros_afectados']} registros afectados)
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Mostrar detalle de duplicados
        with st.expander("Ver detalle de duplicados"):
            for dup in duplicados_info['detalle'][:20]:
                archivos_str = ", ".join(dup['archivos']) if dup['archivos'] else "N/A"
                fechas_str = ", ".join(dup['fechas']) if dup['fechas'] else "N/A"
                st.markdown(f"""
                - **Customer ID {int(dup['customer_id'])}**: {dup['ocurrencias']} ocurrencias
                  - Archivos: `{archivos_str}`
                  - Fechas: `{fechas_str}`
                """)
            
            if len(duplicados_info['detalle']) > 20:
                st.info(f"... y {len(duplicados_info['detalle']) - 20} mas")
    else:
        st.markdown("""
        <div class='duplicados-ok'>
            <h4 style='margin:0; color:#155724;'>Sin duplicados</h4>
            <p style='margin:5px 0 0 0;'>Todas las cuentas son unicas en el conjunto de datos.</p>
        </div>
        """, unsafe_allow_html=True)

    # Footer
    st.markdown("---")
    st.caption(f"{len(df):,} registros total | Fuente: {TIPO_FUENTE.upper()}")

# ============================================
# TAB 2: CHAT IA
# ============================================
with tab2:
    st.markdown("### Asistente IA de Retencion")
    st.caption("Preguntame sobre los datos de retencion de clientes")
    st.markdown("---")
    
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    
    if len(st.session_state.messages) == 0:
        with st.chat_message("assistant"):
            st.markdown("""
            Hola, soy tu asistente de analisis de retencion.
            
            Puedes preguntarme sobre tasas de retencion, clientes en riesgo, tendencias, etc.
            """)
    
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    if prompt := st.chat_input("Escribe tu pregunta aqui..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("Pensando..."):
                df_temp = cargar_datos()
                if df_temp is not None:
                    contexto = obtener_contexto_datos(df_temp)
                    respuesta = call_azure_openai(prompt, contexto)
                else:
                    respuesta = "No hay datos cargados."
                st.markdown(respuesta)
        
        st.session_state.messages.append({"role": "assistant", "content": respuesta})
    
    st.markdown("---")
    if st.button("Limpiar conversacion", use_container_width=True):
        st.session_state.messages = []
        st.rerun()