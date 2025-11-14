import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from itertools import product

# Define las opciones para las combinaciones
variables = ["r", "e", "z"]
sectors = ["gdp", "agriculture", "fishing", "mining", "manufacturing", 
           "electricity", "construction", "commerce", "services"]
frequencies = ["monthly", "quarterly", "annual"]

# Ruta al archivo Jupyter Notebook
notebook_path = "gdp_revisions_datasets.ipynb"  # Cambia por la ruta correcta si es necesario

# Función para ejecutar el notebook con una combinación específica
def run_notebook_for_combination(variable, sector, frequency):
    print(f"Running notebook for: Variable={variable}, Sector={sector}, Frequency={frequency}")
    
    # Carga el notebook
    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook = nbformat.read(f, as_version=4)
    
    # Inserta las combinaciones como código en la primera celda
    setup_code = f"""
variable = "{variable}"
sector = "{sector}"
frequency = "{frequency}"
"""
    notebook['cells'].insert(0, nbformat.v4.new_code_cell(setup_code))
    
    # Ejecuta el notebook
    ep = ExecutePreprocessor(timeout=1200, kernel_name='python3')
    try:
        ep.preprocess(notebook, {'metadata': {'path': './'}})
        print(f"Notebook executed for {variable}-{sector}-{frequency}")
    except Exception as e:
        print(f"Error processing {variable}-{sector}-{frequency}: {e}")

# Itera sobre todas las combinaciones y ejecuta el notebook
for variable, sector, frequency in product(variables, sectors, frequencies):
    run_notebook_for_combination(variable, sector, frequency)
