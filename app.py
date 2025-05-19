import streamlit as st
import pandas as pd
import json
import requests
import time
import os
import io
from isbnlib import is_isbn10, is_isbn13, to_isbn13

# Configurar título y descripción de la página
st.set_page_config(page_title="Procesador de ISBNs", page_icon="📚", layout="wide")
st.title("Procesador de ISBNs")

# Crear directorios si no existen
os.makedirs('uploads', exist_ok=True)
os.makedirs('downloads', exist_ok=True)
JSON_FILE = 'isbn_index.json'

# Función para buscar ISBN en API
def fetch_isbn_date_from_api(isbn):
    # Convertir cualquier ISBN-10 a ISBN-13 para consistencia
    if is_isbn10(isbn):
        isbn = to_isbn13(isbn)
    
    # Intentamos primero con Google Books API
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        
        # Si encontramos resultados
        if data.get('totalItems', 0) > 0:
            published_date = data['items'][0]['volumeInfo'].get('publishedDate', 'Desconocido')
            # Formateamos como YYYY o DD-MM-YY según la longitud
            if len(published_date) == 4:  # Solo año
                return published_date, True
            elif len(published_date) >= 10:  # Fecha completa
                date_parts = published_date.split('-')
                if len(date_parts) >= 3:
                    return f"{date_parts[2][:2]}-{date_parts[1]}-{date_parts[0][2:]}", True
            return published_date, True
        
        # Si no hay resultados con Google Books, intentamos con Open Library
        url_ol = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
        response_ol = requests.get(url_ol, timeout=5)
        data_ol = response_ol.json()
        
        if f"ISBN:{isbn}" in data_ol:
            publish_date = data_ol[f"ISBN:{isbn}"].get("publish_date", "Desconocido")
            # Intentamos formatear la fecha si es posible
            try:
                if len(publish_date) == 4:  # Solo año
                    return publish_date, True
                return publish_date, True
            except:
                return publish_date, True
                
    except Exception as e:
        st.error(f"Error al buscar ISBN {isbn}: {e}")
    
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

# Barra lateral con estadísticas
with st.sidebar:
    st.header("Estadísticas")
    
    # Cargar y mostrar estadísticas del cache de ISBNs
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            try:
                isbn_index = json.load(f)
                st.info(f"Total de ISBNs en la base de datos: {len(isbn_index)}")
            except:
                st.warning("No se pudo leer el archivo de base de datos.")
    else:
        st.info("No hay base de datos de ISBNs creada todavía.")
    
    # Opción para descargar o limpiar la base de datos
    if os.path.exists(JSON_FILE):
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

# Instrucciones
with st.expander("📋 Instrucciones de uso", expanded=True):
    st.markdown("""
    1. Sube un archivo Excel (.xls o .xlsx) que contenga ISBNs en la primera columna.
    2. El sistema añadirá una nueva columna con las fechas de lanzamiento de cada ISBN.
    3. El sistema primero comprobará si el ISBN existe en la base de datos local, y si no, buscará la información a través de APIs externas.
    4. Cuando termine el proceso, podrás descargar el archivo Excel procesado.
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
""")