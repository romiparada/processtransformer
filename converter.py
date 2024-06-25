import pm4py
import pandas as pd
from pm4py.objects.log.importer.xes import importer as xes_importer

# Función para concatenar la información deseada en concept:name
def concatenate_info(row):
    parts = [row['concept:name']]  # Iniciar con el valor existente en concept:name
    # Agregar cada una de las siguientes solo si no están vacías
    for col in ['collab:participant', 'collab:toParticipant', 'collab:fromParticipant']:
        # Agrega información de la columna, o un string vacío si no hay valor.
        parts.append(str(row[col]) if pd.notna(row[col]) else '')
    return '/'.join(parts)

   


def convert_xes_to_csv(xes_path, csv_path):
    # Import the XES file as an event log
    log = xes_importer.apply(xes_path)
    
    # Convert the event log to a DataFrame
    dataframe = pm4py.convert_to_dataframe(log)


    # Aplicar la función a cada fila
    dataframe['concept:name'] = dataframe.apply(concatenate_info, axis=1)

    # Eliminar las columnas innecesarias
    dataframe.drop(columns=['collab:participant', 'collab:toParticipant', 'collab:fromParticipant'], inplace=True)

    # Save the DataFrame to CSV
    dataframe.to_csv(csv_path, index=False)
    
    print(f"Data saved to {csv_path}")

# Specify the file paths
xes_path = 'log.xes'
csv_path = 'output_file.csv'
convert_xes_to_csv(xes_path, csv_path)
