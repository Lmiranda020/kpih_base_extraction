from modules.api_extractor import extrair_dados_api
from config.api_config import APIS_CONFIG

def api_quantidadeCirurgia(diretorio_arquivo_competencia, caminho_to_save, tracker=None):
    """
    Extrai dados de consumo
    
    Args:
        diretorio_arquivo_competencia: Caminho do arquivo de competências
        caminho_to_save: Diretório para salvar os dados
        tracker: ExecutionTracker para registrar execuções (opcional)
    """
    config = APIS_CONFIG["QuantidadeCirurgia"]
    
    return extrair_dados_api(
        diretorio_arquivo_competencia=diretorio_arquivo_competencia,
        caminho_to_save=caminho_to_save,
        nome_api="QuantidadeCirurgia",
        env_var_url=config["env_var"],
        payload_func=config["payload_func"],
        processar_func=config["processar_func"],
        timeout=config["timeout"],
        tracker=tracker
    )