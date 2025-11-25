import pyautogui
import time
import os
import sys
from modules.clicar_imagem import clicar_imagem
from modules.localizar_imagem import localizar_imagem

def get_resource_path(relative_path):
    """Obtém caminho correto tanto em desenvolvimento quanto em executável"""
    try:
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)

def conectar_vpn():
    pyautogui.press('win')
    time.sleep(2)

    config_vpn = "Configurações de VPN"
    pyautogui.write(config_vpn, interval=0.1)
    time.sleep(2)
    pyautogui.press('enter')
    time.sleep(2)

    # USA get_resource_path AQUI
    vpn_conectada = localizar_imagem(
        get_resource_path("data/VPN_conectada.png"), 
        confidence=0.8, 
        timeout=15, 
        descricao="Botão vpn conectada"
    )

    if vpn_conectada:
        print("VPN já está conectada")
        pyautogui.keyDown('alt')
        pyautogui.press('f4')
        pyautogui.keyUp('alt')
        return
    
    # USA get_resource_path AQUI
    if not clicar_imagem(
        get_resource_path("data/botao_vpn.png"), 
        confidence=0.8, 
        timeout=15, 
        descricao="Botão conectar vpn"
    ):
        print("Erro ao conectar VPN")
        return
    
    time.sleep(3)
    pyautogui.keyDown('alt')
    pyautogui.press('f4')
    pyautogui.keyUp('alt')