import pyautogui
import time
from modules.clicar_imagem import clicar_imagem
from modules.localizar_imagem import localizar_imagem

def conectar_vpn():

    pyautogui.press('win')

    time.sleep(2)

    config_vpn = "Configurações de VPN"

    pyautogui.write(config_vpn, interval=0.1)

    time.sleep(2)

    pyautogui.press('enter')

    time.sleep(2)

    # Verifica se a VPN já está conectada
    vpn_conectada = localizar_imagem("data/VPN_conectada.png", confidence=0.8, timeout=15, descricao="Botão vpn conectada")

    if vpn_conectada:
        print("VPN já está conectada")
        # Fecha a janela de configurações
        pyautogui.keyDown('alt')
        pyautogui.press('f4')
        pyautogui.keyUp('alt')
        return  # Sai da função sem tentar conectar
    
    # Se não está conectada, tenta conectar
    if not clicar_imagem("data/botao_vpn.png", confidence=0.8, timeout=15, descricao="Botão conectar vpn"):
        print("Erro ao conectar VPN")
        return
    
    time.sleep(3)

    # Fecha a janela de configurações
    pyautogui.keyDown('alt')
    pyautogui.press('f4')
    pyautogui.keyUp('alt')