import pyautogui
import time

def clicar_imagem(caminho_imagem, confidence=0.8, timeout=10, descricao=""):
    """
    Procura uma imagem na tela e clica nela
    
    Args:
        caminho_imagem: caminho para a imagem do botão
        confidence: precisão da busca (0.0 a 1.0)
        timeout: tempo máximo de espera em segundos
        descricao: descrição do botão para logs
    
    Returns:
        True se encontrou e clicou, False caso contrário
    """
    print(f"Procurando por: {descricao if descricao else caminho_imagem}")
    
    tempo_inicial = time.time()
    
    while time.time() - tempo_inicial < timeout:
        try:
            # Procurar a imagem na tela
            localizacao = pyautogui.locateOnScreen(caminho_imagem, confidence=confidence)
            
            if localizacao:
                # Pegar o centro da imagem
                centro = pyautogui.center(localizacao)
                
                # Clicar no centro
                pyautogui.click(centro)
                print(f"✓ Clicou em: {descricao if descricao else caminho_imagem}")
                return True
                
        except pyautogui.ImageNotFoundException:
            pass
        except Exception as e:
            print(f"Erro ao procurar imagem: {e}")
        
        time.sleep(0.5)  # Aguardar meio segundo antes de tentar novamente
    
    print(f"✗ Não encontrou: {descricao if descricao else caminho_imagem}")
    return False
