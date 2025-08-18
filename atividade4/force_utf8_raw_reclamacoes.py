import os

# Pasta onde estão os CSVs originais
input_folder = "./data/source/reclamacoes"
# Pasta de saída para os arquivos convertidos
output_folder = "./data/source/reclamacoes_utf8"

os.makedirs(output_folder, exist_ok=True)

for filename in os.listdir(input_folder):
    if filename.lower().endswith(".csv"):
        input_path = os.path.join(input_folder, filename)
        output_path = os.path.join(output_folder, filename.replace(".csv", "_utf8.csv"))
        
        print(f"Processando: {filename} → {os.path.basename(output_path)}")
        
        with open(input_path, "r", encoding="utf-8", errors="replace") as f_in:
            with open(output_path, "w", encoding="utf-8") as f_out:
                for line in f_in:
                    f_out.write(line)

print("Conversão concluída!")
