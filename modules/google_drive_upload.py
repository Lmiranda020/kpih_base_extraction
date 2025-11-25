import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from dotenv import load_dotenv


def autenticar_google_drive(caminho_credenciais=None):
    """
    Autentica no Google Drive usando credenciais de Service Account
    
    Args:
        caminho_credenciais: Caminho do arquivo JSON de credenciais.
                           Se None, busca na variável de ambiente GOOGLE_CREDENTIALS_PATH
    
    Returns:
        service: Objeto de serviço do Google Drive
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
        
        # Define os escopos necessários
        SCOPES = ['https://www.googleapis.com/auth/drive.file']
        
        # Cria as credenciais
        credentials = Credentials.from_service_account_file(
            caminho_credenciais,
            scopes=SCOPES
        )
        
        # Constrói o serviço
        service = build('drive', 'v3', credentials=credentials)
        
        print("✅ Autenticação no Google Drive realizada com sucesso")
        return service
        
    except Exception as e:
        print(f"❌ Erro na autenticação do Google Drive: {e}")
        return None


def verificar_ou_criar_arquivo(service, nome_arquivo, folder_id):
    """
    Verifica se o arquivo já existe na pasta do Drive
    
    Args:
        service: Serviço do Google Drive
        nome_arquivo: Nome do arquivo a procurar
        folder_id: ID da pasta no Drive
    
    Returns:
        file_id se existir, None caso contrário
    """
    try:
        query = f"name='{nome_arquivo}' and '{folder_id}' in parents and trashed=false"
        results = service.files().list(
            q=query,
            fields="files(id, name)"
        ).execute()
        
        files = results.get('files', [])
        
        if files:
            return files[0]['id']
        return None
        
    except HttpError as error:
        print(f"   ⚠️ Erro ao verificar arquivo: {error}")
        return None


def upload_arquivo(service, caminho_arquivo, folder_id, sobrescrever=True):
    """
    Faz upload de um arquivo para o Google Drive
    
    Args:
        service: Serviço do Google Drive
        caminho_arquivo: Caminho completo do arquivo local
        folder_id: ID da pasta de destino no Drive
        sobrescrever: Se True, sobrescreve arquivo existente. Se False, cria nova versão
    
    Returns:
        file_id do arquivo criado/atualizado ou None em caso de erro
    """
    try:
        nome_arquivo = os.path.basename(caminho_arquivo)
        
        # Verifica se arquivo já existe
        file_id_existente = verificar_ou_criar_arquivo(service, nome_arquivo, folder_id)
        
        # Configura o tipo MIME baseado na extensão
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
            # Atualiza arquivo existente
            file = service.files().update(
                fileId=file_id_existente,
                media_body=media
            ).execute()
            print(f"   ♻️  Arquivo atualizado: {nome_arquivo}")
        else:
            # Cria novo arquivo
            file_metadata = {
                'name': nome_arquivo,
                'parents': [folder_id]
            }
            
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink'
            ).execute()
            print(f"   📤 Arquivo enviado: {nome_arquivo}")
        
        return file.get('id')
        
    except HttpError as error:
        print(f"   ❌ Erro no upload: {error}")
        return None
    except Exception as e:
        print(f"   ❌ Erro inesperado: {e}")
        return None


def salvar_arquivos_no_drive(bases, diretorio, folder_id=None, sobrescrever=True, credenciais_path=None):
    """
    Percorre uma lista de bases (arquivos) e faz upload para o Google Drive
    
    Args:
        bases: Lista com nomes dos arquivos (ex: ['api_consumo_10_2024.csv', 'api_folha_10_2024.csv'])
        diretorio: Diretório local onde os arquivos estão salvos
        folder_id: ID da pasta no Google Drive (pode ser obtido da URL da pasta)
                   Se None, busca na variável de ambiente GOOGLE_DRIVE_FOLDER_ID
        sobrescrever: Se True, sobrescreve arquivos existentes
        credenciais_path: Caminho do arquivo de credenciais do Google
    
    Returns:
        dict: Dicionário com status de cada arquivo
    """
    
    print(f"\n{'='*60}")
    print(f"📤 Iniciando upload para Google Drive")
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
        print("   Configure a variável GOOGLE_DRIVE_FOLDER_ID no .env")
        print("   Ou passe o folder_id como parâmetro")
        return None
    
    print(f"📁 Diretório local: {diretorio}")
    print(f"📂 Pasta no Drive: {folder_id}")
    print(f"📊 Total de arquivos: {len(bases)}")
    print()
    
    # Autentica no Google Drive
    service = autenticar_google_drive(credenciais_path)
    
    if service is None:
        return None
    
    # Processa cada arquivo
    resultados = {
        'sucesso': [],
        'erro': [],
        'nao_encontrado': []
    }
    
    for idx, nome_base in enumerate(bases, 1):
        print(f"📄 [{idx}/{len(bases)}] Processando: {nome_base}")
        
        caminho_completo = os.path.join(diretorio, nome_base)
        
        # Verifica se o arquivo existe localmente
        if not os.path.exists(caminho_completo):
            print(f"   ⚠️ Arquivo não encontrado localmente")
            resultados['nao_encontrado'].append(nome_base)
            continue
        
        # Faz o upload
        file_id = upload_arquivo(
            service=service,
            caminho_arquivo=caminho_completo,
            folder_id=folder_id,
            sobrescrever=sobrescrever
        )
        
        if file_id:
            resultados['sucesso'].append({
                'arquivo': nome_base,
                'file_id': file_id
            })
        else:
            resultados['erro'].append(nome_base)
    
    # Resumo final
    print(f"\n{'='*60}")
    print(f"📊 Resumo do Upload")
    print(f"{'='*60}")
    print(f"✅ Sucesso: {len(resultados['sucesso'])} arquivo(s)")
    print(f"❌ Erro: {len(resultados['erro'])} arquivo(s)")
    print(f"⚠️  Não encontrado: {len(resultados['nao_encontrado'])} arquivo(s)")
    
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


# ============================================
# EXEMPLO DE USO
# ============================================
if __name__ == "__main__":
    
    # Lista de arquivos para enviar
    bases = [
        'api_consumo_10_2024.csv',
        'api_folha_10_2024.csv',
        'api_contratos_10_2024.csv'
    ]
    
    # Diretório onde os arquivos estão
    diretorio = r"C:\projetos\dados"
    
    # ID da pasta no Google Drive
    # Você pode pegar da URL: https://drive.google.com/drive/folders/ESTE_É_O_ID
    folder_id = "1a2b3c4d5e6f7g8h9i0j"
    
    # Executa o upload
    resultados = salvar_arquivos_no_drive(
        bases=bases,
        diretorio=diretorio,
        folder_id=folder_id,
        sobrescrever=True
    )
    
    # Ou usando variáveis de ambiente (.env):
    # GOOGLE_DRIVE_FOLDER_ID=1a2b3c4d5e6f7g8h9i0j
    # GOOGLE_CREDENTIALS_PATH=C:\credenciais\service-account.json
    
    # resultados = salvar_arquivos_no_drive(
    #     bases=bases,
    #     diretorio=diretorio
    # )