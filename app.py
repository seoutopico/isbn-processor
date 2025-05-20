# Procesar el archivo
        result_df, stats, messages = process_excel_with_isbns(df, progress_bar, status_container, status_placeholder)
        
        if result_df is not None:
            # Mostrar estadísticas finales
            st.success(f"Proceso completado. Se procesaron {stats['total']} ISBNs")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("ISBNs del caché", stats["from_cache"])
            with col2:
                st.metric("ISBNs de la API", stats["from_api"])
            with col3:
                st.metric("ISBNs no encontrados", stats["not_found"])
            
            # Mostrar resultado
            st.subheader("Resultado")
            st.dataframe(result_df)
            
            # Guardar el DataFrame en un archivo Excel en memoria
            buffer = io.BytesIO()
            
            # Asegurarnos de que los ISBNs se formateen como texto en Excel
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                # Crear una copia del DataFrame
                export_df = result_df.copy()
                
                # Definir formatos personalizados para columnas específicas
                formats = {
                    export_df.columns[0]: {'format': '@'}  # Formato de texto para la columna ISBN
                }
                
                # Exportar a Excel
                export_df.to_excel(writer, index=False, sheet_name='ISBNs')
                
                # Acceder a la hoja de trabajo
                workbook = writer.book
                worksheet = writer.sheets['ISBNs']
                
                # Aplicar formato de texto a la columna de ISBNs
                for col_idx, col_name in enumerate(export_df.columns):
                    col_letter = chr(65 + col_idx)  # A, B, C, etc.
                    for row_idx in range(2, len(export_df) + 2):  # Excel es 1-indexed y tenemos header
                        cell = f"{col_letter}{row_idx}"
                        if col_name == export_df.columns[0]:  # Si es la columna de ISBNs
                            # Aplicar formato de texto
                            worksheet[cell].number_format = '@'
            
            # Obtener los datos del buffer
            buffer.seek(0)
            
            # Botón de descarga
            st.download_button(
                label="Descargar archivo procesado",
                data=buffer,
                file_name="ISBNs_procesados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )import streamlit as st
import pandas as pd
import json
import requests
import time
import os
import io
import certifi
import urllib3
from isbnlib import is_isbn10, is_isbn13, to_isbn13

# Configuración para evitar problemas de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# Configurar título y descripción de la página
st.set_page_config(page_title="Procesador de ISBNs", page_icon="📚", layout="wide")
st.title("Procesador de ISBNs")

# Crear directorios si no existen
os.makedirs('uploads', exist_ok=True)
os.makedirs('downloads', exist_ok=True)
JSON_FILE = 'isbn_index.json'

# Configurar variables de entorno para SSL
os.environ['REQUESTS_CA_BUNDLE'] = 'certifi'

# Inicializar variables de sesión para depuración
if 'debug_logs' not in st.session_state:
    st.session_state['debug_logs'] = []
if 'enable_debug' not in st.session_state:
    st.session_state['enable_debug'] = False

# Función para buscar ISBN en API
def fetch_isbn_date_from_api(isbn):
    # Convertir cualquier ISBN-10 a ISBN-13 para consistencia
    if is_isbn10(isbn):
        isbn = to_isbn13(isbn)
    
    # Logging para depuración
    st.session_state.setdefault('debug_logs', [])
    debug_log = st.session_state['debug_logs']
    debug_log.append(f"Buscando ISBN: {isbn}")
    
    # Intentamos primero con Google Books API
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    
    try:
        debug_log.append(f"Consultando Google Books API: {url}")
        
        # Configurar los encabezados para evitar problemas de CORS
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json'
        }
        
        # Aumentar el timeout para entornos con conexiones más lentas
        response = requests.get(url, headers=headers, timeout=10, verify=False)
        debug_log.append(f"Respuesta de Google Books - Status: {response.status_code}")
        
        if response.status_code != 200:
            debug_log.append(f"Error en Google Books API: {response.status_code}")
            debug_log.append(f"Contenido de respuesta: {response.text[:500]}")  # Primeros 500 caracteres
        
        data = response.json()
        
        # Si encontramos resultados
        if data.get('totalItems', 0) > 0:
            published_date = data['items'][0]['volumeInfo'].get('publishedDate', 'Desconocido')
            debug_log.append(f"Fecha encontrada en Google Books: {published_date}")
            
            # Formateamos como YYYY o DD-MM-YY según la longitud
            if len(published_date) == 4:  # Solo año
                return published_date, True
            elif len(published_date) >= 10:  # Fecha completa
                date_parts = published_date.split('-')
                if len(date_parts) >= 3:
                    return f"{date_parts[2][:2]}-{date_parts[1]}-{date_parts[0][2:]}", True
            return published_date, True
    
    except Exception as e:
        debug_log.append(f"Error en Google Books: {str(e)}")
    
    # NOTA: Desactivar la conexión a Open Library que está causando timeouts
    # en el entorno de Streamlit Cloud
    
    # Directamente probamos con ISBNdb como alternativa
    try:
        # Utilizar una API alternativa: WorldCat
        url_worldcat = f"https://www.worldcat.org/isbn/{isbn}"
        debug_log.append(f"Consultando WorldCat: {url_worldcat}")
        
        response_wc = requests.get(
            url_worldcat, 
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            }, 
            timeout=10, 
            verify=False
        )
        
        debug_log.append(f"Respuesta de WorldCat - Status: {response_wc.status_code}")
        
        # WorldCat no tiene una API formal, por lo que extraemos la fecha de la página HTML
        if response_wc.status_code == 200:
            html = response_wc.text
            # Buscar patrón común donde aparece la fecha de publicación
            if "Date:" in html or "fecha de publicación" in html or "Publication" in html:
                # Esta es una extracción básica y puede necesitar ajustes
                start_markers = ["Date:", "Fecha:", "Publication date:", "Fecha de publicación:"]
                for marker in start_markers:
                    if marker in html:
                        start_idx = html.find(marker) + len(marker)
                        end_idx = html.find("<", start_idx)
                        if start_idx > len(marker) and end_idx > start_idx:
                            publish_date = html[start_idx:end_idx].strip()
                            # Limpiar y formatear fecha si es posible
                            publish_date = ''.join([c for c in publish_date if c.isdigit() or c in ['-', '/']]).strip()
                            if publish_date:
                                debug_log.append(f"Fecha encontrada en WorldCat: {publish_date}")
                                return publish_date, True
    
    except Exception as e:
        debug_log.append(f"Error en WorldCat: {str(e)}")
    
    # Como tercera opción, usamos Google Search directamente
    try:
        # Consultar Google directamente para encontrar información del libro
        search_query = f"libro isbn {isbn} fecha publicación"
        url_google = f"https://www.google.com/search?q={search_query}"
        debug_log.append(f"Consultando Google Search: {url_google}")
        
        headers_google = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }
        
        response_google = requests.get(url_google, headers=headers_google, timeout=10, verify=False)
        debug_log.append(f"Respuesta de Google Search - Status: {response_google.status_code}")
        
        # Extraer fecha de publicación de los resultados de búsqueda (método simple)
        if response_google.status_code == 200:
            html = response_google.text
            year_patterns = [r"publicado en (\d{4})", r"fecha de publicación[:\s]+(\d{4})", r"(\d{4})\s*edition", r"publicación:\s*(\d{4})"]
            
            for pattern in year_patterns:
                import re
                matches = re.search(pattern, html, re.IGNORECASE)
                if matches:
                    year = matches.group(1)
                    debug_log.append(f"Año encontrado en Google Search: {year}")
                    return year, True
    
    except Exception as e:
        debug_log.append(f"Error en Google Search: {str(e)}")
        
    # Si todas las APIs fallaron, intentamos un método alternativo
    # Usar una fecha provisional según el prefijo del ISBN
    debug_log.append(f"Intentando estimar fecha por prefijo ISBN")
    try:
        # Los primeros dígitos de un ISBN-13 pueden dar pistas sobre la fecha
        # Esta es una estimación muy aproximada y debe ser usada solo como último recurso
        if isbn.startswith('978'):
            prefix = isbn[3:7]  # Extraer grupo y editor
            
            # Algunos prefijos comunes de editoriales españolas y estimación de fechas
            spanish_publishers = {
                '8467': '2010-2023',  # Ejemplo para ISBN que comienzan con 978-84-67
                '8408': '2000-2020',  # Ejemplo para ISBN que comienzan con 978-84-08
                '8432': '1990-2010',  # Ejemplo para ISBN que comienzan con 978-84-32
            }
            
            # Comprobar si coincide con algún prefijo conocido
            for pub_prefix, date_range in spanish_publishers.items():
                if prefix.startswith(pub_prefix):
                    debug_log.append(f"Fecha estimada por prefijo editorial: {date_range}")
                    # Extraer el año más reciente del rango
                    latest_year = date_range.split('-')[1]
                    return latest_year, True
            
            # Si no coincide con ningún prefijo conocido, usar una estimación genérica
            return "Fecha no encontrada (estimado 2000-2023)", True
    
    except Exception as e:
        debug_log.append(f"Error al estimar fecha por prefijo: {str(e)}")
    
    debug_log.append(f"No se encontró fecha para ISBN {isbn} en ninguna API")
    return "No encontrado", False  # Si no se encuentra en ninguna API

def process_excel_with_isbns(df, progress_bar=None, status_container=None, status_placeholder=None):
    # Cargar el índice existente de ISBNs
    isbn_index = {}
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            try:
                isbn_index = json.load(f)
            except json.JSONDecodeError:
                isbn_index = {}
    
    # Verificar que hay al menos una columna
    if df.shape[1] == 0:
        st.error("El archivo Excel no tiene columnas.")
        return None, None, None
    
    # Asegurarnos de que la primera columna sea tratada como texto
    # Preservamos la columna original para mantener el formato
    original_col_name = df.columns[0]
    df[original_col_name] = df[original_col_name].astype(str)
    
    # Extraer los ISBNs de la primera columna
    isbns = df.iloc[:, 0].astype(str).str.strip()
    
    # Crear una nueva columna para las fechas de lanzamiento
    release_dates = []
    new_isbns_added = 0
    
    # Calcular estadísticas iniciales
    total_isbns = len(isbns)
    isbns_in_cache = sum(1 for isbn in isbns if ''.join(c for c in isbn if c.isdigit() or c == 'X' or c == 'x') in isbn_index)
    isbns_to_search = total_isbns - isbns_in_cache
    
    stats = {
        "total": total_isbns, 
        "from_cache": 0, 
        "from_api": 0, 
        "not_found": 0, 
        "pending": isbns_to_search
    }
    
    # Mostrar estadísticas iniciales
    if status_container:
        status_container.text(f"Total de ISBNs a procesar: {stats['total']}")
        status_container.text(f"ISBNs en base de datos: {isbns_in_cache}")
        status_container.text(f"ISBNs pendientes de buscar en API: {isbns_to_search}")
    
    # Lista para almacenar mensajes
    messages = []
    
    # Procesar cada ISBN
    for i, isbn in enumerate(isbns):
        # Actualizar la barra de progreso si se proporciona
        if progress_bar is not None:
            progress_bar.progress((i + 1) / len(isbns))
        
        # Limpiar ISBN de caracteres no numéricos si es necesario
        isbn_clean = ''.join(c for c in isbn if c.isdigit() or c == 'X' or c == 'x')
        
        # Si el ISBN está en el índice, usar la fecha almacenada
        if isbn_clean in isbn_index:
            release_dates.append(isbn_index[isbn_clean])
            stats["from_cache"] += 1
            
            # Añadir mensaje neutral (sin formato de éxito) para ISBNs en caché
            messages.append(f"ISBN {isbn_clean} encontrado en caché: {isbn_index[isbn_clean]}")
        else:
            # Si no está en el índice, buscar en la API
            messages.append(f"🔍 Buscando fecha para ISBN {isbn_clean} en API...")
            
            date, found = fetch_isbn_date_from_api(isbn_clean)
            
            # Almacenar el resultado en el índice
            if found:
                isbn_index[isbn_clean] = date
                new_isbns_added += 1
                stats["from_api"] += 1
                messages.append(f"ISBN {isbn_clean} resultado: {date}")
            else:
                stats["not_found"] += 1
                messages.append(f"ISBN {isbn_clean} no encontrado")
            
            stats["pending"] -= 1
            release_dates.append(date)
            
            # Actualizar estadísticas en tiempo real y mostrar mensajes
            if status_placeholder:
                status_text = (
                    f"Total de ISBNs a procesar: {stats['total']}\n"
                    f"ISBNs en base de datos: {isbns_in_cache}\n"
                    f"ISBNs encontrados en caché: {stats['from_cache']}\n"
                    f"ISBNs encontrados en API: {stats['from_api']}\n"
                    f"ISBNs no encontrados: {stats['not_found']}\n"
                    f"ISBNs pendientes: {stats['pending']}\n\n"
                )
                
                # Mostrar los últimos 10 mensajes
                status_text += "\n".join(messages[-10:])
                status_placeholder.text(status_text)
            
            # Esperar un breve tiempo para no sobrecargar la API
            time.sleep(0.5)
    
    # Añadir la columna de fechas al DataFrame
    df['Fecha de Lanzamiento'] = release_dates
    
    # Actualizar el archivo JSON con los nuevos ISBNs encontrados
    if new_isbns_added > 0:
        with open(JSON_FILE, 'w', encoding='utf-8') as f:
            json.dump(isbn_index, f, indent=2, ensure_ascii=False)
    
    return df, stats, messages

# Función para cargar el índice de ISBNs
def load_isbn_index():
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}

# Función para guardar el índice de ISBNs
def save_isbn_index(isbn_index):
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(isbn_index, f, indent=2, ensure_ascii=False)

# Función para validar ISBN
def validate_isbn(isbn):
    isbn_clean = ''.join(c for c in isbn if c.isdigit() or c == 'X' or c == 'x')
    return is_isbn10(isbn_clean) or is_isbn13(isbn_clean)

# Barra lateral con estadísticas y gestión manual de ISBNs
with st.sidebar:
    st.header("Estadísticas")
    
    # Cargar y mostrar estadísticas del cache de ISBNs
    isbn_index = load_isbn_index()
    isbn_count = len(isbn_index)
    
    if isbn_count > 0:
        st.info(f"Total de ISBNs en la base de datos: {isbn_count}")
    else:
        st.info("No hay base de datos de ISBNs creada todavía.")
    
    # Opción para descargar o limpiar la base de datos
    if isbn_count > 0:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            try:
                isbn_data = f.read()
                st.download_button(
                    label="Descargar base de datos de ISBNs",
                    data=isbn_data,
                    file_name="isbn_index.json",
                    mime="application/json",
                )
                
                if st.button("Limpiar base de datos", type="secondary"):
                    os.remove(JSON_FILE)
                    st.success("Base de datos limpiada correctamente.")
                    st.rerun()
            except:
                st.warning("Error al acceder a la base de datos.")
    
    # Sección para gestión manual de ISBNs
    st.header("Gestión Manual de ISBNs")
    
    # Pestañas para añadir o eliminar ISBNs
    tab1, tab2 = st.tabs(["Añadir ISBN", "Eliminar ISBN"])
    
    with tab1:
        st.subheader("Añadir ISBN a la base de datos")
        st.markdown("Puedes añadir varios ISBNs separados por espacios.")
        isbns_to_add = st.text_area("ISBN(s)", key="add_isbn", placeholder="Introduce uno o varios ISBNs separados por espacios")
        release_date = st.text_input("Fecha de lanzamiento", key="add_date")
        
        if st.button("Añadir a la base de datos", key="btn_add"):
            if isbns_to_add and release_date:
                # Dividir la entrada en múltiples ISBNs
                isbn_list = isbns_to_add.strip().split()
                
                # Variables para seguimiento del proceso
                successful_isbns = []
                invalid_isbns = []
                
                # Procesar cada ISBN
                for isbn in isbn_list:
                    # Limpiar ISBN de caracteres no numéricos
                    isbn_clean = ''.join(c for c in isbn if c.isdigit() or c == 'X' or c == 'x')
                    
                    # Validar ISBN
                    if validate_isbn(isbn_clean):
                        # Convertir ISBN-10 a ISBN-13 para consistencia si es necesario
                        if is_isbn10(isbn_clean):
                            isbn_clean = to_isbn13(isbn_clean)
                        
                        # Actualizar la base de datos
                        isbn_index[isbn_clean] = release_date
                        successful_isbns.append(isbn_clean)
                    else:
                        invalid_isbns.append(isbn)
                
                # Guardar los cambios en la base de datos
                if successful_isbns:
                    save_isbn_index(isbn_index)
                    st.success(f"Se añadieron {len(successful_isbns)} ISBNs correctamente con fecha {release_date}.")
                    
                    # Mostrar los ISBNs añadidos en una lista expandible
                    with st.expander("Ver ISBNs añadidos"):
                        for isbn in successful_isbns:
                            st.code(f"{isbn}: {release_date}")
                
                # Mostrar ISBNs inválidos si hay alguno
                if invalid_isbns:
                    st.error(f"No se pudieron añadir {len(invalid_isbns)} ISBNs inválidos: {', '.join(invalid_isbns)}")
                
                if successful_isbns:
                    st.rerun()
            else:
                st.warning("Por favor, introduce tanto el ISBN como la fecha de lanzamiento.")
    
    with tab2:
        st.subheader("Eliminar ISBN de la base de datos")
        isbns_to_remove = st.text_area("ISBN(s) a eliminar", key="remove_isbn", placeholder="Introduce uno o varios ISBNs separados por espacios")
        
        # Mostrar opción para buscar en la base de datos
        if st.checkbox("Buscar en la base de datos", key="search_db"):
            search_term = st.text_input("Término de búsqueda", key="search_term")
            if search_term:
                results = {k: v for k, v in isbn_index.items() if search_term in k}
                if results:
                    st.write(f"Resultados encontrados ({len(results)}):")
                    for k, v in results.items():
                        st.code(f"{k}: {v}")
                else:
                    st.info("No se encontraron resultados.")
        
        if st.button("Eliminar de la base de datos", key="btn_remove"):
            if isbns_to_remove:
                # Dividir la entrada en múltiples ISBNs
                isbn_list = isbns_to_remove.strip().split()
                
                # Variables para seguimiento del proceso
                removed_isbns = []
                not_found_isbns = []
                
                # Procesar cada ISBN
                for isbn in isbn_list:
                    # Limpiar ISBN de caracteres no numéricos
                    isbn_clean = ''.join(c for c in isbn if c.isdigit() or c == 'X' or c == 'x')
                    
                    # Verificar si el ISBN existe en la base de datos
                    if isbn_clean in isbn_index:
                        # Eliminar el ISBN
                        del isbn_index[isbn_clean]
                        removed_isbns.append(isbn_clean)
                    else:
                        not_found_isbns.append(isbn_clean)
                
                # Guardar los cambios en la base de datos
                if removed_isbns:
                    save_isbn_index(isbn_index)
                    st.success(f"Se eliminaron {len(removed_isbns)} ISBNs correctamente.")
                    
                    # Mostrar los ISBNs eliminados en una lista expandible
                    with st.expander("Ver ISBNs eliminados"):
                        for isbn in removed_isbns:
                            st.code(isbn)
                
                # Mostrar ISBNs no encontrados si hay alguno
                if not_found_isbns:
                    st.warning(f"{len(not_found_isbns)} ISBNs no encontrados en la base de datos: {', '.join(not_found_isbns)}")
                
                if removed_isbns:
                    st.rerun()
            else:
                st.warning("Por favor, introduce el ISBN que deseas eliminar.")
    
    # Opción para activar el modo de depuración
    st.header("Configuración avanzada")
    st.session_state['enable_debug'] = st.checkbox("Activar modo de depuración", value=st.session_state['enable_debug'])
    
    if st.session_state['enable_debug']:
        if st.button("Limpiar logs de depuración"):
            st.session_state['debug_logs'] = []
            st.rerun()

# Instrucciones
with st.expander("📋 Instrucciones de uso", expanded=True):
    st.markdown("""
    1. Sube un archivo Excel (.xls o .xlsx) que contenga ISBNs en la primera columna.
    2. El sistema añadirá una nueva columna con las fechas de lanzamiento de cada ISBN.
    3. El sistema primero comprobará si el ISBN existe en la base de datos local, y si no, buscará la información a través de APIs externas.
    4. Cuando termine el proceso, podrás descargar el archivo Excel procesado.
    5. Puedes añadir o eliminar ISBNs manualmente usando las opciones en la barra lateral:
       - Para añadir: Introduce uno o varios ISBNs separados por espacios y la fecha de lanzamiento
       - Para eliminar: Introduce uno o varios ISBNs separados por espacios
    """)

# Carga de archivo
uploaded_file = st.file_uploader("Selecciona el archivo Excel con ISBNs", type=["xls", "xlsx"])

if uploaded_file is not None:
    try:
        # Cargar archivo
        df = pd.read_excel(uploaded_file)
        
        # Mostrar vista previa
        st.subheader("Vista previa del archivo")
        st.dataframe(df.head(5))
        
        # Obtener resumen preliminar
        if os.path.exists(JSON_FILE):
            with open(JSON_FILE, 'r', encoding='utf-8') as f:
                try:
                    isbn_index = json.load(f)
                    total_isbns = len(df)
                    isbns_in_db = sum(1 for isbn in df.iloc[:, 0].astype(str).str.strip() if ''.join(c for c in isbn if c.isdigit() or c == 'X' or c == 'x') in isbn_index)
                    
                    st.info(f"De los {total_isbns} ISBNs en tu archivo, {isbns_in_db} ya están en la base de datos y {total_isbns - isbns_in_db} deberán buscarse en APIs.")
                except:
                    pass
        
        # Verificar que hay datos en la primera columna
        if df.shape[0] == 0:
            st.error("El archivo no contiene datos")
        else:
            # Procesar archivo cuando el usuario haga clic en el botón
            if st.button("Procesar ISBNs", type="primary"):
                st.subheader("Procesando archivo...")
                
                # Crear barra de progreso
                progress_bar = st.progress(0)
                
                # Crear contenedor para mensajes de estado
                status_container = st.container()
                status_placeholder = st.empty()
                
                # Procesar el archivo
                result_df, stats, messages = process_excel_with_isbns(df, progress_bar, status_container, status_placeholder)
                
                if result_df is not None:
                    # Mostrar estadísticas finales
                    st.success(f"Proceso completado. Se procesaron {stats['total']} ISBNs")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("ISBNs del caché", stats["from_cache"])
                    with col2:
                        st.metric("ISBNs de la API", stats["from_api"])
                    with col3:
                        st.metric("ISBNs no encontrados", stats["not_found"])
                    
                    # Mostrar resultado
                    st.subheader("Resultado")
                    st.dataframe(result_df)
                    
                    # Guardar el DataFrame en un archivo Excel en memoria
                    buffer = io.BytesIO()
                    
                    # Asegurarnos de que los ISBNs se formateen como texto en Excel
                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                        # Crear una copia del DataFrame
                        export_df = result_df.copy()
                        
                        # Definir formatos personalizados para columnas específicas
                        formats = {
                            export_df.columns[0]: {'format': '@'}  # Formato de texto para la columna ISBN
                        }
                        
                        # Exportar a Excel
                        export_df.to_excel(writer, index=False, sheet_name='ISBNs')
                        
                        # Acceder a la hoja de trabajo
                        workbook = writer.book
                        worksheet = writer.sheets['ISBNs']
                        
                        # Aplicar formato de texto a la columna de ISBNs
                        for col_idx, col_name in enumerate(export_df.columns):
                            col_letter = chr(65 + col_idx)  # A, B, C, etc.
                            for row_idx in range(2, len(export_df) + 2):  # Excel es 1-indexed y tenemos header
                                cell = f"{col_letter}{row_idx}"
                                if col_name == export_df.columns[0]:  # Si es la columna de ISBNs
                                    # Aplicar formato de texto
                                    worksheet[cell].number_format = '@'
                    
                    # Obtener los datos del buffer
                    buffer.seek(0)
                    
                    # Botón de descarga
                    st.download_button(
                        label="Descargar archivo procesado",
                        data=buffer,
                        file_name="ISBNs_procesados.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                    
                    # Mostrar el log completo de procesamiento
                    with st.expander("Ver log completo de procesamiento"):
                        for msg in messages:
                            st.text(msg)
    except Exception as e:
        st.error(f"Error al procesar el archivo: {e}")

# Mostrar información adicional al final
st.markdown("---")
st.markdown("### Acerca de")
st.markdown("""
Esta aplicación busca fechas de lanzamiento para ISBNs utilizando varias APIs (Google Books y Open Library).
Los ISBNs encontrados se almacenan en una base de datos local para acelerar futuras búsquedas.
La aplicación también permite gestionar manualmente los ISBNs en la base de datos, pudiendo añadir o eliminar varios ISBNs a la vez.
""")
