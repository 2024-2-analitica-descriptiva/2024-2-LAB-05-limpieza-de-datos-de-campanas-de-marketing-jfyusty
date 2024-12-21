"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

# Importa las bibliotecas necesarias.
import pandas as pd # Importa pandas para la manipulación de datos.
import zipfile # Importa zipfile para manejar archivos comprimidos.
import os # Importa os para interactuar con el sistema de archivos.

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    """
    Función para procesar datos de campañas almacenados en archivos CSV comprimidos.
    Divide los datos en tres archivos CSV: client.csv, campaign.csv y economics.csv,
    aplicando transformaciones específicas a cada conjunto de datos.

    Dependencias:
        - pandas
        - zipfile
        - os
    """

    # Define las rutas de entrada y salida.
    input_path = 'files/input/' # Carpeta que contiene los archivos ZIP.
    output_path = 'files/output/' # Carpeta donde se guardarán los CSV procesados.
    os.makedirs(output_path, exist_ok=True)  # Crea la carpeta de salida si no existe
    
    # Elimina archivos existentes en la carpeta de salida para evitar duplicados.
    for filename in ['client.csv', 'campaign.csv', 'economics.csv']:
        if os.path.exists(output_path + filename): # Verifica si el archivo existe.
            os.remove(output_path + filename) # Elimina el archivo si existe.

    # Inicializa indicadores para controlar si se escriben los encabezados de los CSV.
    header_client = True
    header_campaign = True
    header_economics = True

    # Procesa cada archivo comprimido en la carpeta de entrada.
    for file in os.listdir(input_path): # Lista los archivos en la carpeta de entrada.
        if file.endswith('.zip'): # Procesa solo los archivos con extensión .zip.
            with zipfile.ZipFile(os.path.join(input_path, file), 'r') as z: # Abre el archivo ZIP.
                for csv_file in z.namelist(): # Itera sobre los archivos dentro del ZIP.
                    with z.open(csv_file) as f: # Abre cada archivo CSV dentro del ZIP.
                        df = pd.read_csv(f) # Carga el contenido del archivo en un DataFrame.

                        # Transformaciones para client.csv
                        client_df = df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
                        client_df['job'] = client_df['job'].str.replace('.', '').str.replace('-', '_') # Limpia los valores en la columna 'job'.
                        client_df['education'] = client_df['education'].str.replace('.', '_').replace('unknown', pd.NA) # Limpia valores en 'education'.
                        client_df['credit_default'] = client_df['credit_default'].map({'yes': 1}).fillna(0) # Mapea 'yes' a 1, otros a 0.
                        client_df['mortgage'] = client_df['mortgage'].map({'yes': 1}).fillna(0) # Mapea 'yes' a 1, otros a 0.
                        client_df.to_csv(output_path + 'client.csv', index=False, mode='a', header=header_client) # Guarda el DataFrame en un CSV, Usa mode='a' para agregar datos sin sobrescribir el archivo..
                        header_client = False  # Evita reescribir encabezados.

                        # Transformaciones para campaign.csv
                        campaign_df = df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'day', 'month']].copy()
                        campaign_df['previous_outcome'] = campaign_df['previous_outcome'].map({'success': 1}).fillna(0) # Mapea 'success' a 1, otros a 0.
                        campaign_df['campaign_outcome'] = campaign_df['campaign_outcome'].map({'yes': 1}).fillna(0) # Mapea 'yes' a 1, otros a 0.
                        campaign_df['last_contact_date'] = pd.to_datetime(
                            '2022-' + campaign_df['month'] + '-' + campaign_df['day'].astype(str), 
                            format='%Y-%b-%d'
                        ).dt.strftime('%Y-%m-%d') # Crea una nueva columna 'last_contact_date' combinando 'month' y 'day'.
                        campaign_df.drop(columns=['month', 'day'], inplace=True) # Elimina las columnas 'month' y 'day'.
                        campaign_df.to_csv(output_path + 'campaign.csv', index=False, mode='a', header=header_campaign) # Guarda el DataFrame en un CSV, Usa mode='a' para agregar datos sin sobrescribir el archivo.
                        header_campaign = False  # Evita reescribir encabezados.

                        # Transformaciones para economics.csv
                        economics_df = df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
                        economics_df.to_csv(output_path + 'economics.csv', index=False, mode='a', header=header_economics) # Guarda el DataFrame en un CSV, Usa mode='a' para agregar datos sin sobrescribir el archivo.
                        header_economics = False  # Evita reescribir encabezados.

if __name__ == "__main__":
    clean_campaign_data()
