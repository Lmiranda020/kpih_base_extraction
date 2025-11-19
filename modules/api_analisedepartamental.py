from modules.api_extractor import extrair_dados_api
from config.api_config import APIS_CONFIG

def api_analisedepartamental(
    diretorio_arquivo_competencia, 
    caminho_to_save, 
    tracker=None, 
    delay_entre_chamadas=0.2,           # ← AUMENTADO de 0.5 para 2.0
    max_tentativas_403=3,               # ← NOVO
    backoff_inicial=3.0,                # ← NOVO
    agrupar_por_unidade=True,           # ← NOVO
    delay_entre_unidades=5.0            # ← NOVO
):
    """
    Extrai dados de Quantidade de Leito com retry automático
    
    Args:
        diretorio_arquivo_competencia: Caminho do arquivo de competências
        caminho_to_save: Diretório para salvar os dados
        tracker: ExecutionTracker para registrar execuções (opcional)
        delay_entre_chamadas: Delay entre requisições (padrão: 2.0s)
        max_tentativas_403: Tentativas de retry para erro 403 (padrão: 3)
        backoff_inicial: Tempo inicial de espera no retry (padrão: 3.0s)
        agrupar_por_unidade: Agrupa processamento por unidade (padrão: True)
        delay_entre_unidades: Delay ao mudar de unidade (padrão: 5.0s)
    """
    config = APIS_CONFIG["analisedepartamental"]
    
    return extrair_dados_api(
        diretorio_arquivo_competencia=diretorio_arquivo_competencia,
        caminho_to_save=caminho_to_save,
        nome_api="analisedepartamental",
        env_var_url=config["env_var"],
        payload_func=config["payload_func"],
        processar_func=config["processar_func"],
        timeout=config["timeout"],
        tracker=tracker,
        delay_entre_chamadas=delay_entre_chamadas,
        max_tentativas_403=max_tentativas_403,
        backoff_inicial=backoff_inicial,
        agrupar_por_unidade=agrupar_por_unidade,
        delay_entre_unidades=delay_entre_unidades
    )