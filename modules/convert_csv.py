import os
import csv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv


def autenticar_google_drive(caminho_credenciais=None):
    """
    Autentica no Google Drive usando credenciais de Service Account
    """
    try:
        load_dotenv()
        if caminho_credenciais is None:
            caminho_credenciais = os.getenv('GOOGLE_CREDENTIALS_PATH')
        
        if not caminho_credenciais:
            print("❌ Caminho das credenciais não fornecido")
            return None
        
        if not os.path.exists(caminho_credenciais):
            print(f"❌ Arquivo de credenciais não encontrado: {caminho_credenciais}")
            return None
        
        SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        
        credentials = Credentials.from_service_account_file(
            caminho_credenciais,
            scopes=SCOPES
        )
        
        service = build('drive', 'v3', credentials=credentials)
        sheets_service = build('sheets', 'v4', credentials=credentials)
        
        print("✅ Autenticação no Google Drive realizada com sucesso")
        return service, sheets_service
        
    except Exception as e:
        print(f"❌ Erro na autenticação do Google Drive: {e}")
        return None, None


def csv_para_google_sheets(service, sheets_service, caminho_csv, nome_planilha, folder_id):
    """
    Converte um CSV para Google Sheets e salva no Drive
    
    Args:
        service: Serviço do Google Drive
        sheets_service: Serviço do Google Sheets
        caminho_csv: Caminho completo do arquivo CSV
        nome_planilha: Nome que a planilha terá no Google Drive
        folder_id: ID da pasta de destino no Drive
    
    Returns:
        spreadsheet_id do arquivo criado ou None em caso de erro
    """
    try:
        print(f"\n   📊 Convertendo CSV para Google Sheets...")
        
        # Lê o CSV
        dados = []
        with open(caminho_csv, 'r', encoding='utf-8') as arquivo:
            leitor = csv.reader(arquivo)
            for linha in leitor:
                dados.append(linha)
        
        if not dados:
            print(f"   ❌ Arquivo CSV está vazio")
            return None
        
        print(f"   📈 Linhas lidas: {len(dados)}")
        
        # Cria a planilha no Google Sheets
        spreadsheet_body = {
            'properties': {
                'title': nome_planilha
            },
            'sheets': [
                {
                    'properties': {
                        'sheetId': 0,
                        'title': 'Dados'
                    }
                }
            ]
        }
        
        spreadsheet = sheets_service.spreadsheets().create(
            body=spreadsheet_body,
            fields='spreadsheetId'
        ).execute()
        
        spreadsheet_id = spreadsheet.get('spreadsheetId')
        print(f"   ✅ Planilha criada: {spreadsheet_id}")
        
        # Insere os dados na planilha
        range_name = 'Dados!A1'
        body = {
            'values': dados
        }
        
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        print(f"   ✅ Dados inseridos na planilha")
        
        # Move a planilha para a pasta especificada
        try:
            # Obtém o arquivo para pegar os pais atuais
            file = service.files().get(
                fileId=spreadsheet_id,
                fields='parents',
                supportsAllDrives=True
            ).execute()
            
            pais_anteriores = ','.join(file.get('parents', []))
            
            # Move para a nova pasta
            file = service.files().update(
                fileId=spreadsheet_id,
                addParents=folder_id,
                removeParents=pais_anteriores,
                fields='id, parents',
                supportsAllDrives=True
            ).execute()
            
            print(f"   ✅ Planilha movida para a pasta de destino")
        except Exception as e:
            print(f"   ⚠️  Não foi possível mover a planilha: {e}")
        
        return spreadsheet_id
        
    except Exception as e:
        print(f"   ❌ Erro ao converter CSV para Sheets: {e}")
        import traceback
        traceback.print_exc()
        return None


def verificar_ou_criar_arquivo(service, nome_arquivo, folder_id):
    """
    Verifica se o arquivo já existe na pasta do Drive
    """
    try:
        query = f"name='{nome_arquivo}' and '{folder_id}' in parents and trashed=false"
        results = service.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        files = results.get('files', [])
        
        if files:
            return files[0]['id']
        return None
        
    except HttpError as error:
        print(f"   ⚠️ Erro ao verificar arquivo: {error}")
        return None


def limpar_nome_arquivo(nome_arquivo):
    """
    Remove a parte de mês/ano do final do nome do arquivo
    """
    import re
    
    nome_sem_ext, extensao = os.path.splitext(nome_arquivo)
    nome_limpo = re.sub(r'_\d{1,2}_\d{2,4}$', '', nome_sem_ext)
    
    return nome_limpo


def salvar_arquivos_no_drive_como_sheets(bases, diretorio, folder_id=None, credenciais_path=None, sobrescrever_existentes=True):
    """
    Converte arquivos CSV para Google Sheets e faz upload
    
    Args:
        bases: Lista com nomes dos arquivos CSV (ex: ['api_consumo_10_2024.csv'])
        diretorio: Diretório local onde os arquivos estão salvos
        folder_id: ID da pasta no Google Drive
        credenciais_path: Caminho do arquivo de credenciais
        sobrescrever_existentes: Se True, sobrescreve planilhas existentes
    
    Returns:
        dict: Dicionário com status de cada arquivo
    """
    
    print(f"\n{'='*60}")
    print(f"📊 Convertendo CSV para Google Sheets")
    print(f"{'='*60}\n")
    
    # Validações iniciais
    if not bases or len(bases) == 0:
        print("❌ Lista de bases vazia")
        return None
    
    if not os.path.exists(diretorio):
        print(f"❌ Diretório não encontrado: {diretorio}")
        return None
    
    # Busca folder_id se não fornecido
    if folder_id is None:
        folder_id = os.getenv('GOOGLE_DRIVE_FOLDER_ID')
    
    if not folder_id:
        print("❌ ID da pasta do Google Drive não fornecido")
        return None
    
    print(f"📁 Diretório local: {diretorio}")
    print(f"📂 Pasta no Drive: {folder_id}")
    print(f"📊 Total de arquivos: {len(bases)}\n")
    
    # Autentica no Google Drive e Sheets
    service, sheets_service = autenticar_google_drive(credenciais_path)
    
    if service is None or sheets_service is None:
        return None
    
    # Processa cada arquivo
    resultados = {
        'sucesso': [],
        'erro': [],
        'nao_encontrado': []
    }
    
    for idx, nome_base in enumerate(bases, 1):
        print(f"📄 [{idx}/{len(bases)}] Processando: {nome_base}")
        
        # Verifica extensão
        if not nome_base.lower().endswith('.csv'):
            print(f"   ⚠️ Arquivo não é CSV, ignorando")
            resultados['erro'].append(nome_base)
            continue
        
        caminho_completo = os.path.join(diretorio, nome_base)
        
        # Verifica se o arquivo existe localmente
        if not os.path.exists(caminho_completo):
            print(f"   ⚠️ Arquivo não encontrado localmente")
            resultados['nao_encontrado'].append(nome_base)
            continue
        
        # Limpa o nome para usar como nome da planilha
        nome_planilha = limpar_nome_arquivo(nome_base).replace('.csv', '')
        
        # Verifica se a planilha já existe
        file_id_existente = verificar_ou_criar_arquivo(service, nome_planilha, folder_id)
        
        if file_id_existente and not sobrescrever_existentes:
            print(f"   ⏭️  Planilha já existe, ignorando")
            resultados['erro'].append(nome_base)
            continue
        elif file_id_existente and sobrescrever_existentes:
            print(f"   🔄 Deletando planilha existente...")
            try:
                service.files().delete(
                    fileId=file_id_existente,
                    supportsAllDrives=True
                ).execute()
                print(f"   ✅ Planilha anterior deletada")
            except Exception as e:
                print(f"   ⚠️ Não foi possível deletar a anterior: {e}")
        
        # Converte CSV para Google Sheets
        spreadsheet_id = csv_para_google_sheets(
            service=service,
            sheets_service=sheets_service,
            caminho_csv=caminho_completo,
            nome_planilha=nome_planilha,
            folder_id=folder_id
        )
        
        if spreadsheet_id:
            resultados['sucesso'].append({
                'arquivo_original': nome_base,
                'planilha_nome': nome_planilha,
                'spreadsheet_id': spreadsheet_id
            })
        else:
            resultados['erro'].append(nome_base)
        
        print()
    
    # Resumo final
    print(f"\n{'='*60}")
    print(f"📊 Resumo da Conversão")
    print(f"{'='*60}")
    print(f"✅ Sucesso: {len(resultados['sucesso'])} arquivo(s)")
    print(f"❌ Erro: {len(resultados['erro'])} arquivo(s)")
    print(f"⚠️  Não encontrado: {len(resultados['nao_encontrado'])} arquivo(s)")
    
    if resultados['sucesso']:
        print(f"\n✅ Planilhas criadas:")
        for item in resultados['sucesso']:
            print(f"   📊 {item['arquivo_original']} → {item['planilha_nome']}")
    
    if resultados['erro']:
        print(f"\n❌ Arquivos com erro:")
        for arquivo in resultados['erro']:
            print(f"   - {arquivo}")
    
    if resultados['nao_encontrado']:
        print(f"\n⚠️ Arquivos não encontrados:")
        for arquivo in resultados['nao_encontrado']:
            print(f"   - {arquivo}")
    
    print(f"{'='*60}\n")
    
    return resultados