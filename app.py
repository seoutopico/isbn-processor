import streamlit as st
import pandas as pd
import json
import requests
import time
import os
import io
import random
from isbnlib import is_isbn10, is_isbn13, to_isbn13

# Configurar título y descripción de la página
st.set_page_config(page_title="Procesador de ISBNs", page_icon="📚", layout="wide")
st.title("Procesador de ISBNs")

# Crear directorios si no existen
os.makedirs('uploads', exist_ok=True)
os.makedirs('downloads', exist_ok=True)
JSON_FILE = 'isbn_index.json'

# Función para buscar ISBN en API con reintentos y backoff exponencial
def fetch_isbn_date_from_api(isbn, max_retries=3, initial_timeout=10):
    # Convertir cualquier ISBN-10 a ISBN-13 para consistencia
    if is_isbn10(isbn):
        isbn = to_isbn13(isbn)
    
    # Función para intentar una solicitud con reintentos y backoff exponencial
    def make_request_with_retry(url, current_retry=0, timeout=initial_timeout):
        if current_retry >= max_retries:
            return None  # Agotamos los reintentos
        
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            # Si el código no es 200, consideramos reintento
            time.sleep(0.5 * (current_retry + 1))
            return make_request_with_retry(url, current_retry + 1, timeout * 1.5)
        except requests.exceptions.Timeout:
            # En caso de timeout, aumentamos el timeout en el siguiente intento
            time.sleep(0.5 * (current_retry + 1))
            return make_request_with_retry(url, current_retry + 1, timeout * 1.5)
        except requests.exceptions.ConnectionError as e:
            # Esperamos un poco antes de reintentar en caso de error de conexión
            time.sleep(1 * (current_retry + 1))
            return make_request_with_retry(url, current_retry + 1, timeout * 1.5)
        except Exception as e:
            if current_retry < max_retries - 1:
                time.sleep(1 * (current_retry + 1))
                return make_request_with_retry(url, current_retry + 1, timeout * 1.5)
            return None
    
    # Intentamos primero con Google Books API
    url_google = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    data_google = make_request_with_retry(url_google)
    
    if data_google and data_google.get('totalItems', 0) > 0:
        published_date = data_google['items'][0]['volumeInfo'].get('publishedDate', 'Desconocido')
        # Formateamos como YYYY o DD-MM-YY según la longitud
        if len(published_date) == 4:  # Solo año
            return published_date, True
        elif len(published_date) >= 10:  # Fecha completa
            date_parts = published_date.split('-')
            if len(date_parts) >= 3:
                return f"{date_parts[2][:2]}-{date_parts[1]}-{date_parts[0][2:]}", True
        return published_date, True
    
    # Si no hay resultados con Google Books o falla, intentamos con Open Library
    # con un timeout mayor, ya que parece más propenso a timeouts
    url_ol = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
    data_ol = make_request_with_retry(url_ol, timeout=15)  # Timeout mayor para Open Library
    
    if data_ol and f"ISBN:{isbn}" in data_ol:
        publish_date = data_ol[f"ISBN:{isbn}"].get("publish_date", "Desconocido")
        # Intentamos formatear la fecha si es posible
        try:
            if len(publish_date) == 4:  # Solo año
                return publish_date, True
            return publish_date, True
        except:
            return publish_date, True
    
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
            
            try:
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
                
                release_dates.append(date)
            except Exception as e:
                # Capturar cualquier error durante el procesamiento
                stats["not_found"] += 1
                error_msg = f"Error al procesar ISBN {isbn_clean}: {str(e)}"
                messages.append(error_msg)
                release_dates.append("Error: No procesado")
            
            stats["pending"] -= 1
            
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
            
            # Guardar regularmente para no perder progreso
            if new_isbns_added > 0 and new_isbns_added % 5 == 0:
                with open(JSON_FILE, 'w', encoding='utf-8') as f:
                    json.dump(isbn_index, f, indent=2, ensure_ascii=False)
            
            # Esperar un breve tiempo para no sobrecargar la API, con jitter aleatorio
            time.sleep(0.5 + random.uniform(0, 0.5))
    
    # Añadir la columna de fechas al DataFrame
    df['Fecha de Lanzamiento'] = release_dates
    
    # Actualizar el archivo JSON con los nuevos ISBNs encontrados
    if new_isbns_added > 0:
        with open(JSON_FILE, 'w', encoding='utf-8') as f:
            json.dump(isbn_index, f, indent=2, ensure_ascii=False)
    
    return df, stats, messages

# Función para procesar por lotes
def process_excel_with_isbns_batch(df, batch_size=10, progress_bar=None, status_container=None, status_placeholder=None):
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
    original_col_name = df.columns[0]
    df[original_col_name] = df[original_col_name].astype(str)
    
    # Extraer los ISBNs de la primera columna
    isbns = df.iloc[:, 0].astype(str).str.strip()
    
    # Crear una nueva columna para las fechas de lanzamiento
    release_dates = ["Pendiente"] * len(isbns)
    
    # Estadísticas iniciales
    total_isbns = len(isbns)
    stats = {
        "total": total_isbns, 
        "from_cache": 0, 
        "from_api": 0, 
        "not_found": 0, 
        "pending": total_isbns,
        "processed": 0
    }
    
    messages = []
    new_isbns_added = 0
    
    # Procesar en lotes
    for batch_start in range(0, len(isbns), batch_size):
        batch_end = min(batch_start + batch_size, len(isbns))
        
        # Procesar cada ISBN en el lote actual
        for i in range(batch_start, batch_end):
            isbn = isbns[i]
            
            # Actualizar progreso
            if progress_bar is not None:
                progress_bar.progress((i + 1) / len(isbns))
            
            # Limpiar ISBN
            isbn_clean = ''.join(c for c in isbn if c.isdigit() or c == 'X' or c == 'x')
            
            # Verificar si está en caché
            if isbn_clean in isbn_index:
                release_dates[i] = isbn_index[isbn_clean]
                stats["from_cache"] += 1
                stats["pending"] -= 1
                stats["processed"] += 1
                messages.append(f"ISBN {isbn_clean} encontrado en caché: {isbn_index[isbn_clean]}")
            else:
                try:
                    messages.append(f"🔍 Buscando fecha para ISBN {isbn_clean} en API...")
                    date, found = fetch_isbn_date_from_api(isbn_clean)
                    
                    if found:
                        isbn_index[isbn_clean] = date
                        new_isbns_added += 1
                        stats["from_api"] += 1
                        messages.append(f"ISBN {isbn_clean} resultado: {date}")
                    else:
                        stats["not_found"] += 1
                        messages.append(f"ISBN {isbn_clean} no encontrado")
                    
                    release_dates[i] = date
                except Exception as e:
                    stats["not_found"] += 1
                    error_msg = f"Error al procesar ISBN {isbn_clean}: {str(e)}"
                    messages.append(error_msg)
                    release_dates[i] = "Error: No procesado"
                
                stats["pending"] -= 1
                stats["processed"] += 1
            
            # Actualizar estadísticas en tiempo real
            if status_placeholder:
                status_text = (
                    f"ISBNs procesados: {stats['processed']}/{stats['total']} "
                    f"(Caché: {stats['from_cache']}, API: {stats['from_api']}, "
                    f"No encontrados: {stats['not_found']}, Pendientes: {stats['pending']})\n\n"
                )
                status_text += "\n".join(messages[-5:])  # Mostrar menos mensajes para no sobrecargar
                status_placeholder.text(status_text)
            
            # Esperar un breve tiempo para no sobrecargar la API, con jitter aleatorio
            if isbn_clean not in isbn_index:
                time.sleep(0.5 + random.uniform(0, 0.5))
        
        # Guardar progreso después de cada lote
        df['Fecha de Lanzamiento'] = release_dates
        
        if new_isbns_added > 0:
            with open(JSON_FILE, 'w', encoding='utf-8') as f:
                json.dump(isbn_index, f, indent=2, ensure_ascii=False)
        
        # Permitir descarga parcial después de cada lote
        if batch_end < len(isbns):
            # Mostrar opción para descargar parcialmente
            st.warning(f"Procesados {batch_end} de {len(isbns)} ISBNs. Puedes descargar lo procesado hasta ahora.")
            
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                export_df = df.copy()
                export_df.to_excel(writer, index=False, sheet_name='ISBNs')
                workbook = writer.book
                worksheet = writer.sheets['ISBNs']
                for row_idx in range(2, len(export_df) + 2):
                    worksheet[f"A{row_idx}"].number_format = '@'
            
            buffer.seek(0)
            st.download_button(
                label="Descargar resultados parciales",
                data=buffer,
                file_name=f"ISBNs_procesados_parcial_{batch_end}_de_{len(isbns)}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    
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
    6. Modo por lotes: Para archivos grandes o en entornos con limitaciones de tiempo (como Streamlit Cloud gratuito), usa el modo por lotes para procesar grupos pequeños de ISBNs y evitar timeouts.
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
            # Añadir opciones para procesamiento por lotes
            st.subheader("Opciones de procesamiento")
            processing_method = st.radio(
                "Selecciona el método de procesamiento",
                ["Procesar todo de una vez", "Procesar por lotes (recomendado para Streamlit Cloud)"]
            )
            
            if processing_method == "Procesar por lotes (recomendado para Streamlit Cloud)":
                batch_size = st.slider("Tamaño de lote", min_value=5, max_value=50, value=10, help="Número de ISBNs a procesar en cada lote")
            
            # Procesar archivo cuando el usuario haga clic en el botón
            if st.button("Procesar ISBNs", type="primary"):
                st.subheader("Procesando archivo...")
                
                # Crear barra de progreso
                progress_bar = st.progress(0)
                
                # Crear contenedor para mensajes de estado
                status_container = st.container()
                status_placeholder = st.empty()
                
                # Procesar el archivo
                if processing_method == "Procesar todo de una vez":
                    result_df, stats, messages = process_excel_with_isbns(df, progress_bar, status_container, status_placeholder)
                else:
                    result_df, stats, messages = process_excel_with_isbns_batch(df, batch_size, progress_bar, status_container, status_placeholder)
                
                if result_df is not None:
                    # Mostrar estadísticas finales
                    st.success(f"Proceso completado. Se procesaron {stats['total']} ISBNs")
                    
                    if processing_method == "Procesar todo de una vez":
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("ISBNs del caché", stats["from_cache"])
                        with col2:
                            st.metric("ISBNs de la API", stats["from_api"])
                        with col3:
                            st.metric("ISBNs no encontrados", stats["not_found"])
                    else:
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("ISBNs del caché", stats["from_cache"])
                        with col2:
                            st.metric("ISBNs de la API", stats["from_api"])
                        with col3:
                            st.metric("ISBNs no encontrados", stats["not_found"])
                        with col4:
                            st.metric("Total procesados", stats["processed"])
                    
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
