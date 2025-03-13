from flask import Flask, request, jsonify, render_template
import pandas as pd
from werkzeug.utils import secure_filename
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import calendar

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/')
def index():
    return render_template('upload.html')  # Renderiza a página de upload

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'Nenhum arquivo enviado'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'Nenhum arquivo selecionado'}), 400
    
    filename = secure_filename(file.filename)
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(file_path)
    return render_template('success.html')  # Renderiza a página de sucesso
    
    try:
        # Ler a primeira aba do arquivo Excel
        df_metas = pd.read_excel(file_path, sheet_name=0)
        
        # Extraindo os dados conforme especificado
        mes = df_metas.iloc[0, 3]  # D2
        lojas = [df_metas.iloc[1, 1], df_metas.iloc[1, 5], df_metas.iloc[1, 9], df_metas.iloc[1, 13]]  # B3, F3, J3, N3
        meta_1 = [df_metas.iloc[18, 3], df_metas.iloc[18, 7], df_metas.iloc[18, 11], df_metas.iloc[18, 15]]  # D20, H20, L20, P20
        meta_2 = [df_metas.iloc[22, 3], df_metas.iloc[22, 7], df_metas.iloc[22, 11], df_metas.iloc[22, 15]]  # D24, H24, L24, P24
        meta_3 = [df_metas.iloc[26, 3], df_metas.iloc[26, 7], df_metas.iloc[26, 11], df_metas.iloc[26, 15]]  # D28, H28, L28, P28
        
        data_metas = {
            'MÊS': [mes] * len(lojas),
            'LOJA': lojas,
            'META 1': meta_1,
            'META 2': meta_2,
            'META 3': meta_3
        }
        
        df_final_metas = pd.DataFrame(data_metas).fillna(0)  # Substituindo NaN por zero
        print("METAS:")
        print(df_final_metas)  # Exibir no terminal

        # =================== PROCESSAMENTO DA ABA SPLT ===================

        nome_aba_splt = "SPLT"  # Nome da aba que será processada
        df_splt = pd.read_excel(file_path, sheet_name=nome_aba_splt)

        # Remover a linha 33 (índice 32, pois o índice começa em 0)
        df_splt.drop(index=32, inplace=True, errors='ignore')

        # Remover as colunas indesejadas
        colunas_remover_splt = ['DISCRI', 'NOME', 'NOME.1', 'NOME.2', 'DISTRI', 'NOME.3', 'NOME.4']
        df_splt.drop(columns=[col for col in colunas_remover_splt if col in df_splt.columns], inplace=True, errors='ignore')

        # Criar a coluna DATA com os valores da primeira coluna original (sem concatenação com o mês)
        df_splt.rename(columns={df_splt.columns[0]: "DATA"}, inplace=True)

        # Criar a nova coluna MES com o valor da variável mes
        df_splt["MES"] = mes

        # Criar a coluna LOJA com o nome da aba
        df_splt["LOJA"] = nome_aba_splt

        # Transformar os dados de pagamento no formato desejado
        df_splt_melted = df_splt.melt(id_vars=["DATA", "MES", "LOJA"], var_name="PAGAMENTO", value_name="VALOR")

        # Substituir NaN por zero
        df_splt_melted["VALOR"] = df_splt_melted["VALOR"].fillna(0)

        # Remover linhas onde VALOR é zero
        df_splt_melted = df_splt_melted[df_splt_melted["VALOR"] != 0]

        # Remover linhas onde a coluna DATA contém a palavra "TOTAL"
        df_splt_melted = df_splt_melted[~df_splt_melted["DATA"].astype(str).str.contains("TOTAL", case=False, na=False)]

        # Converter a coluna MES para datetime
        df_splt_melted["MES"] = pd.to_datetime(df_splt_melted["MES"], format="%m/%Y", errors="coerce")

        # Garantir que DATA seja um número válido
        df_splt_melted["DATA"] = pd.to_numeric(df_splt_melted["DATA"], errors="coerce")

        # Substituir o dia pelo valor da coluna DATA, verificando se é um dia válido
        def ajustar_data(row):
            try:
                return row["MES"].replace(day=int(row["DATA"]))
            except ValueError:
                return None  # Retorna None para valores inválidos

        df_splt_melted["MES"] = df_splt_melted.apply(ajustar_data, axis=1)

        # Remover linhas onde MES seja inválido
        df_splt_melted = df_splt_melted.dropna(subset=["MES"])

        print("\nMOVIMENTAÇÃO SPLT:")
        print(df_splt_melted)  # Exibir no terminal

        # =================== PROCESSAMENTO DA ABA TLPS ===================

        nome_aba_tlps = "TLPS"  # Nome da aba que será processada
        df_tlps = pd.read_excel(file_path, sheet_name=nome_aba_tlps)

        # Remover a linha 33 (índice 32, pois o índice começa em 0)
        df_tlps.drop(index=32, inplace=True, errors='ignore')

        # Remover as colunas indesejadas
        colunas_remover_tlps = ['DISCRI', 'NOME', 'NOME.1', 'NOME.2', 'NOME.3']
        df_tlps.drop(columns=[col for col in colunas_remover_tlps if col in df_tlps.columns], inplace=True, errors='ignore')

        # Criar a coluna DATA com os valores da primeira coluna original (sem concatenação com o mês)
        df_tlps.rename(columns={df_tlps.columns[0]: "DATA"}, inplace=True)

        # Criar a nova coluna MES com o valor da variável mes
        df_tlps["MES"] = mes

        # Criar a coluna LOJA com o nome da aba
        df_tlps["LOJA"] = nome_aba_tlps

        # Transformar os dados de pagamento no formato desejado
        df_tlps_melted = df_tlps.melt(id_vars=["DATA", "MES", "LOJA"], var_name="PAGAMENTO", value_name="VALOR")

        # Substituir NaN por zero
        df_tlps_melted["VALOR"] = df_tlps_melted["VALOR"].fillna(0)

        # Remover linhas onde VALOR é zero
        df_tlps_melted = df_tlps_melted[df_tlps_melted["VALOR"] != 0]

        # Remover linhas onde a coluna DATA contém a palavra "TOTAL"
        df_tlps_melted = df_tlps_melted[~df_tlps_melted["DATA"].astype(str).str.contains("TOTAL", case=False, na=False)]

        # Converter a coluna MES para datetime
        df_tlps_melted["MES"] = pd.to_datetime(df_tlps_melted["MES"], format="%m/%Y", errors="coerce")

        # Garantir que DATA seja um número válido
        df_tlps_melted["DATA"] = pd.to_numeric(df_tlps_melted["DATA"], errors="coerce")

        # Substituir o dia pelo valor da coluna DATA, verificando se é um dia válido
        def ajustar_data(row):
            try:
                return row["MES"].replace(day=int(row["DATA"]))
            except ValueError:
                return None  # Retorna None para valores inválidos

        df_tlps_melted["MES"] = df_tlps_melted.apply(ajustar_data, axis=1)

        # Remover linhas onde MES seja inválido
        df_tlps_melted = df_tlps_melted.dropna(subset=["MES"])

        print("\nMOVIMENTAÇÃO TLPS:")
        print(df_tlps_melted)  # Exibir no terminal

        # =================== PROCESSAMENTO DA ABA PATIO ===================

        nome_aba_patio = "PATIO"  # Nome da aba que será processada
        df_patio = pd.read_excel(file_path, sheet_name=nome_aba_patio)

        # Remover a linha 33 (índice 32, pois o índice começa em 0)
        df_patio.drop(index=32, inplace=True, errors='ignore')

        # Remover as colunas indesejadas
        colunas_remover_patio = ['DISCRI', 'NOME', 'NOME.1', 'DISCRIM']
        df_patio.drop(columns=[col for col in colunas_remover_patio if col in df_patio.columns], inplace=True, errors='ignore')

        # Criar a coluna DATA com os valores da primeira coluna original
        df_patio.rename(columns={df_patio.columns[0]: "DATA"}, inplace=True)

        # Criar a nova coluna MES com o valor da variável mes
        df_patio["MES"] = mes

        # Criar a coluna LOJA com o nome da aba
        df_patio["LOJA"] = nome_aba_patio

        # Transformar os dados de pagamento no formato desejado
        df_patio_melted = df_patio.melt(id_vars=["DATA", "MES", "LOJA"], var_name="PAGAMENTO", value_name="VALOR")

        # Substituir NaN por zero
        df_patio_melted["VALOR"] = df_patio_melted["VALOR"].fillna(0)

        # Remover linhas onde VALOR é zero
        df_patio_melted = df_patio_melted[df_patio_melted["VALOR"] != 0]

        # Remover linhas onde a coluna DATA contém a palavra "TOTAL"
        df_patio_melted = df_patio_melted[~df_patio_melted["DATA"].astype(str).str.contains("TOTAL", case=False, na=False)]

        # Converter a coluna MES para datetime
        df_patio_melted["MES"] = pd.to_datetime(df_patio_melted["MES"], format="%m/%Y", errors="coerce")

        # Garantir que DATA seja um número válido
        df_patio_melted["DATA"] = pd.to_numeric(df_patio_melted["DATA"], errors="coerce")

        # Substituir o dia pelo valor da coluna DATA, verificando se é um dia válido
        def ajustar_data(row):
            try:
                return row["MES"].replace(day=int(row["DATA"]))
            except ValueError:
                return None  # Retorna None para valores inválidos

        df_patio_melted["MES"] = df_patio_melted.apply(ajustar_data, axis=1)

        # Remover linhas onde MES seja inválido
        df_patio_melted = df_patio_melted.dropna(subset=["MES"])

        print("\nMOVIMENTAÇÃO PATIO:")
        print(df_patio_melted)  # Exibir no terminal

         # =================== PROCESSAMENTO DA ABA KONI ===================

        nome_aba_koni = "KONI"  # Nome da aba que será processada
        df_koni = pd.read_excel(file_path, sheet_name=nome_aba_koni)

        # Remover a linha 33 (índice 32, pois o índice começa em 0)
        df_koni.drop(index=32, inplace=True, errors='ignore')

        # Remover as colunas D, W, Y, AC, AF
        colunas_remover_koni = ['DISCRI', 'NOME', 'NOME.1', 'NOME.2', 1, 'DISCRI.1']
        df_koni.drop(columns=[col for col in colunas_remover_koni if col in df_koni.columns], inplace=True, errors='ignore')

        # Criar a coluna DATA com os valores da primeira coluna original (sem concatenação com o mês)
        df_koni.rename(columns={df_koni.columns[0]: "DATA"}, inplace=True)

        # Criar a nova coluna MES com o valor da variável mes
        df_koni["MES"] = mes

        # Criar a coluna LOJA com o nome da aba
        df_koni["LOJA"] = nome_aba_koni

        # Transformar os dados de pagamento no formato desejado
        df_koni_melted = df_koni.melt(id_vars=["DATA", "MES", "LOJA"], var_name="PAGAMENTO", value_name="VALOR")

        # Substituir NaN por zero
        df_koni_melted["VALOR"] = df_koni_melted["VALOR"].fillna(0)

        # Remover linhas onde VALOR é zero
        df_koni_melted = df_koni_melted[df_koni_melted["VALOR"] != 0]

        # Remover linhas onde a coluna DATA contém a palavra "TOTAL"
        df_koni_melted = df_koni_melted[~df_koni_melted["DATA"].astype(str).str.contains("TOTAL", case=False, na=False)]

        # Converter a coluna MES para datetime
        df_koni_melted["MES"] = pd.to_datetime(df_koni_melted["MES"], format="%m/%Y", errors="coerce")

        # Garantir que DATA seja um número válido
        df_koni_melted["DATA"] = pd.to_numeric(df_koni_melted["DATA"], errors="coerce")

        # Substituir o dia pelo valor da coluna DATA, verificando se é um dia válido
        def ajustar_data(row):
            try:
                return row["MES"].replace(day=int(row["DATA"]))
            except ValueError:
                return None  # Retorna None para valores inválidos

        df_koni_melted["MES"] = df_koni_melted.apply(ajustar_data, axis=1)

        # Remover linhas onde MES seja inválido
        df_koni_melted = df_koni_melted.dropna(subset=["MES"])

        print("\nMOVIMENTAÇÃO KONI:")
        print(df_koni_melted)  # Exibir no terminal

        # CONCATENANDO OS DATA FRAMES

        df_concatenado = pd.concat([df_splt_melted, df_tlps_melted, df_patio_melted, df_koni_melted], ignore_index=True)

        # =================== GRAVAR NO GOOGLE SHEETS ===================
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(r"config\grupocarvalho-adcf355b43a3.json", scope)
        client = gspread.authorize(creds)

        # Abrindo a planilha pelo nome
        spreadsheet = client.open_by_key("1XAvRtdLuR1L37zgW0UrX8z-Gvd5H2Tbk1q4HwepxwRA")  # Aqui você define a variável corretamente

        # Atualizar a aba METAS com df_final_metas
        worksheet_metas = spreadsheet.worksheet("METAS")
        
        # Ler os dados existentes na aba METAS para verificar se já existe alguma data
        existing_data_metas = worksheet_metas.get_all_values()
        if existing_data_metas:
            df_existing_metas = pd.DataFrame(existing_data_metas[1:], columns=existing_data_metas[0])  # Ignorar cabeçalhos
            # Concatenar os novos dados com os existentes, sem duplicar a data
            df_final_metas = pd.concat([df_existing_metas, df_final_metas], ignore_index=True)
        
        worksheet_metas.clear()  # Limpar a aba antes de atualizar
        set_with_dataframe(worksheet_metas, df_final_metas)

        # Atualizar a aba MOVIMENTAÇÃO com df_concatenado
        worksheet_movimentacao = spreadsheet.worksheet("MOVIMENTAÇÃO")
        
        # Ler os dados existentes na aba MOVIMENTAÇÃO
        existing_data_movimentacao = worksheet_movimentacao.get_all_values()
        if existing_data_movimentacao:
            df_existing_movimentacao = pd.DataFrame(existing_data_movimentacao[1:], columns=existing_data_movimentacao[0])  # Ignorar cabeçalhos
            # Concatenar os novos dados com os existentes, sem duplicar a data
            df_concatenado = pd.concat([df_existing_movimentacao, df_concatenado], ignore_index=True)
        
        worksheet_movimentacao.clear()  # Limpar a aba antes de atualizar
        set_with_dataframe(worksheet_movimentacao, df_concatenado)

        return df_concatenado.to_html() # Exibir os dados na página web

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)