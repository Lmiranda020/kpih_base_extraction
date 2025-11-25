import os
import csv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

def limpar_nome_arquivo(nome_arquivo):
    """
    Remove a parte de mês/ano do final do nome do arquivo
    """
    import re
    
    nome_sem_ext, extensao = os.path.splitext(nome_arquivo)
    nome_limpo = re.sub(r'_\d{1,2}_\d{2,4}$', '', nome_sem_ext)
    
    return nome_limpo + extensao

def autenticar_google_drive(caminho_credenciais=None):
    """
    Autentica no Google Drive
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
        
        SCOPES = ['https://www.googleapis.com/auth/drive']
        
        credentials = Credentials.from_service_account_file(
            caminho_credenciais,
            scopes=SCOPES
        )
        
        service = build('drive', 'v3', credentials=credentials)
        
        print("✅ Autenticação no Google Drive realizada com sucesso")
        return service
        
    except Exception as e:
        print(f"❌ Erro na autenticação: {e}")
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


def upload_arquivo_com_nome_customizado(service, caminho_arquivo, nome_arquivo_drive, folder_id, sobrescrever=True):
    """
    Faz upload usando um nome customizado para o arquivo no Drive
    """
    try:
        file_id_existente = verificar_ou_criar_arquivo(service, nome_arquivo_drive, folder_id)
        
        mime_types = {
            '.csv': 'text/csv',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.xls': 'application/vnd.ms-excel',
            '.pdf': 'application/pdf',
            '.txt': 'text/plain',
            '.json': 'application/json'
        }
        
        extensao = os.path.splitext(caminho_arquivo)[1].lower()
        mime_type = mime_types.get(extensao, 'application/octet-stream')
        
        media = MediaFileUpload(
            caminho_arquivo,
            mimetype=mime_type,
            resumable=True
        )
        
        if file_id_existente and sobrescrever:
            file = service.files().update(
                fileId=file_id_existente,
                media_body=media,
                supportsAllDrives=True
            ).execute()
            print(f"   ♻️  Arquivo atualizado no Drive")
        else:
            file_metadata = {
                'name': nome_arquivo_drive,
                'parents': [folder_id]
            }
            
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink',
                supportsAllDrives=True
            ).execute()
            print(f"   ✅ Arquivo CSV enviado: {nome_arquivo_drive}")
        
        return file.get('id')
        
    except HttpError as error:
        print(f"   ❌ Erro no upload: {error}")
        return None
    except Exception as e:
        print(f"   ❌ Erro inesperado: {e}")
        return None


def csv_para_google_sheets(service, file_id_csv, nome_planilha, folder_id):
    """
    Converte um arquivo CSV que já está no Drive para Google Sheets
    usando a API do Drive (copy + convert)
    """
    try:
        print(f"   📊 Convertendo CSV para Google Sheets...")
        
        # Copia o arquivo CSV e converte para Google Sheets
        file_metadata = {
            'name': nome_planilha,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [folder_id]
        }
        
        converted_file = service.files().copy(
            fileId=file_id_csv,
            body=file_metadata,
            supportsAllDrives=True
        ).execute()
        
        spreadsheet_id = converted_file.get('id')
        print(f"   ✅ Google Sheet criado: {nome_planilha}")
        
        return spreadsheet_id
        
    except HttpError as e:
        print(f"   ❌ Erro ao converter: {e}")
        return None
    except Exception as e:
        print(f"   ❌ Erro inesperado: {e}")
        return None


def salvar_arquivos_no_drive(bases, diretorio, folder_id=None, sobrescrever=True, credenciais_path=None, limpar_nomes=True, criar_google_sheets=True):
    """
    Upload para Google Drive com opção de converter para Google Sheets
    
    Args:
        bases: Lista com nomes dos arquivos
        diretorio: Diretório local onde os arquivos estão
        folder_id: ID da pasta no Google Drive
        sobrescrever: Se True, sobrescreve arquivos existentes
        credenciais_path: Caminho do arquivo de credenciais
        limpar_nomes: Se True, remove mês/ano do nome
        criar_google_sheets: Se True, também cria Google Sheets para CSVs
    """
    
    print(f"\n{'='*60}")
    print(f"📤 Iniciando upload para Google Drive")
    print(f"{'='*60}\n")
    
    if not bases or len(bases) == 0:
        print("❌ Lista de bases vazia")
        return None
    
    if not os.path.exists(diretorio):
        print(f"❌ Diretório não encontrado: {diretorio}")
        return None
    
    if folder_id is None:
        folder_id = os.getenv('GOOGLE_DRIVE_FOLDER_ID')
    
    if not folder_id:
        print("❌ ID da pasta do Google Drive não fornecido")
        return None
    
    print(f"📁 Diretório local: {diretorio}")
    print(f"📂 Pasta no Drive: {folder_id}")
    print(f"📊 Total de arquivos: {len(bases)}")
    if limpar_nomes:
        print(f"✨ Limpeza de nomes: ATIVADO")
    if criar_google_sheets:
        print(f"📊 Criar Google Sheets: ATIVADO")
    print()
    
    # Autentica
    service = autenticar_google_drive(credenciais_path)
    
    if service is None:
        return None
    
    resultados = {
        'sucesso': [],
        'erro': [],
        'nao_encontrado': []
    }
    
    for idx, nome_base in enumerate(bases, 1):
        print(f"📄 [{idx}/{len(bases)}] Processando: {nome_base}")
        
        caminho_completo = os.path.join(diretorio, nome_base)
        
        if not os.path.exists(caminho_completo):
            print(f"   ⚠️ Arquivo não encontrado localmente")
            resultados['nao_encontrado'].append(nome_base)
            continue
        
        # Limpa o nome
        nome_no_drive = limpar_nome_arquivo(nome_base) if limpar_nomes else nome_base
        
        if nome_no_drive != nome_base:
            print(f"   🔄 Renomeando: {nome_base} → {nome_no_drive}")
        
        # Upload do CSV
        file_id_csv = upload_arquivo_com_nome_customizado(
            service=service,
            caminho_arquivo=caminho_completo,
            nome_arquivo_drive=nome_no_drive,
            folder_id=folder_id,
            sobrescrever=sobrescrever
        )
        
        file_id_sheets = None
        
        # Cria Google Sheets se ativado e for CSV
        if criar_google_sheets and nome_base.lower().endswith('.csv') and file_id_csv:
            nome_planilha = limpar_nome_arquivo(nome_base).replace('.csv', '')
            
            # Verifica se planilha já existe
            file_id_existente = verificar_ou_criar_arquivo(service, nome_planilha, folder_id)
            
            if file_id_existente and sobrescrever:
                print(f"   🗑️  Deletando planilha anterior...")
                try:
                    service.files().delete(
                        fileId=file_id_existente,
                        supportsAllDrives=True
                    ).execute()
                    print(f"   ✅ Planilha anterior deletada")
                except Exception as e:
                    print(f"   ⚠️ Não foi possível deletar: {e}")
            
            # Converte o CSV que já está no Drive para Google Sheets
            file_id_sheets = csv_para_google_sheets(
                service=service,
                file_id_csv=file_id_csv,
                nome_planilha=nome_planilha,
                folder_id=folder_id
            )
        
        if file_id_csv or file_id_sheets:
            resultados['sucesso'].append({
                'arquivo_original': nome_base,
                'arquivo_drive': nome_no_drive,
                'file_id_csv': file_id_csv,
                'file_id_sheets': file_id_sheets
            })
        else:
            resultados['erro'].append(nome_base)
        
        print()
    
    # Resumo final
    print(f"\n{'='*60}")
    print(f"📊 Resumo do Upload")
    print(f"{'='*60}")
    print(f"✅ Sucesso: {len(resultados['sucesso'])} arquivo(s)")
    print(f"❌ Erro: {len(resultados['erro'])} arquivo(s)")
    print(f"⚠️  Não encontrado: {len(resultados['nao_encontrado'])} arquivo(s)")
    
    if resultados['sucesso']:
        print(f"\n✅ Arquivos processados:")
        for item in resultados['sucesso']:
            print(f"   📁 CSV: {item['arquivo_drive']}")
            if item['file_id_sheets']:
                print(f"   📊 Sheets: {item['arquivo_drive'].replace('.csv', '')}")
    
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