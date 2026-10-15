"""
Utilitários para ler/gravar na pasta de rede (Y:) tolerando quedas momentâneas
da rede/VPN: em vez de falhar na hora, aguarda e tenta de novo.
"""
import os
import shutil
import tempfile
import time

TENTATIVAS_REDE = 10
ESPERA_REDE_SEGUNDOS = 30


def executar_com_retry(funcao, descricao, tentativas=TENTATIVAS_REDE, espera=ESPERA_REDE_SEGUNDOS):
    """Executa funcao(); em erro de E/S (rede), espera e tenta novamente"""
    for tentativa in range(1, tentativas + 1):
        try:
            return funcao()
        except OSError as e:
            if tentativa == tentativas:
                raise
            print(f"   🌐 Falha de rede em '{descricao}' ({e}) - "
                  f"tentativa {tentativa}/{tentativas}, aguardando {espera}s...")
            time.sleep(espera)


def aguardar_caminho(caminho, tentativas=TENTATIVAS_REDE, espera=ESPERA_REDE_SEGUNDOS):
    """True se o caminho existir (aguardando a rede voltar, se preciso)"""
    for tentativa in range(1, tentativas + 1):
        if os.path.exists(caminho):
            return True
        if tentativa < tentativas:
            print(f"   🌐 Caminho indisponível: {caminho} - "
                  f"tentativa {tentativa}/{tentativas}, aguardando {espera}s...")
            time.sleep(espera)
    return False


def copiar_seguro(origem, destino):
    """Copia para um temporário ao lado do destino e só então substitui o destino"""
    def _copiar():
        temporario = destino + ".tmp"
        shutil.copyfile(origem, temporario)
        os.replace(temporario, destino)
    executar_com_retry(_copiar, f"copiar {os.path.basename(destino)}")


def salvar_dataframe_seguro(df, destino, **kwargs_to_csv):
    """
    Grava o DataFrame primeiro no disco local e depois copia para a rede.
    Assim uma queda de rede não deixa arquivo pela metade no destino.
    CSV por padrão; .xlsx usa to_excel.
    """
    extensao = os.path.splitext(destino)[1].lower()
    fd, local = tempfile.mkstemp(suffix=extensao, prefix="kpih_")
    os.close(fd)
    try:
        if extensao == '.xlsx':
            df.to_excel(local, index=False)
        else:
            df.to_csv(local, **kwargs_to_csv)
        copiar_seguro(local, destino)
    finally:
        try:
            os.remove(local)
        except OSError:
            pass
