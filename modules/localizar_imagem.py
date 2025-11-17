import pyautogui
import time


def localizar_imagem(caminho_imagem, confidence=0.8, timeout=10, descricao=""):
    """
    Procura uma imagem na tela
    
    Args:
        caminho_imagem: caminho para a imagem
        confidence: precisão da busca (0.0 a 1.0)
        timeout: tempo máximo de espera em segundos
        descricao: descrição da imagem para logs
    
    Returns:
        A localização (objeto) se encontrou, None caso contrário
    """
    print(f"Procurando por: {descricao if descricao else caminho_imagem}")
    
    tempo_inicial = time.time()
    
    while time.time() - tempo_inicial < timeout:
        try:
            # Procurar a imagem na tela
            localizacao = pyautogui.locateOnScreen(caminho_imagem, confidence=confidence)
            
            if localizacao:
                print(f"✓ Imagem localizada: {descricao if descricao else caminho_imagem}")
                return localizacao  # Retorna o objeto de localização
        
        except Exception as e:
            # Se houver erro (ex: arquivo não encontrado), continua tentando
            pass
        
        time.sleep(0.5)  # Pequena pausa entre verificações
    
    print(f"✗ Imagem não encontrada: {descricao if descricao else caminho_imagem}")
    return None  # Retorna None se não encontrar