import os
from datetime import datetime
from dotenv import load_dotenv

def to_save():
    
    caminho = os.getenv("caminho_fixo")

    data_execucao = datetime.today()
    mes = data_execucao.month
    ano = data_execucao.year

    # Corrige o caso de janeiro (volta pra dezembro do ano anterior)
    if mes == 1:
        mes_anterior = 12
        ano_anterior = ano - 1
    else:
        mes_anterior = mes - 1
        ano_anterior = ano

    # mes_anterior = 2
    # ano_anterior = 2026

    # Garante o mês com dois dígitos (01, 02, ..., 12)
    pasta_mes_e_ano = f"{mes_anterior:02d}_{ano_anterior}"

    # Monta o caminho completo
    caminho_atual = os.path.join(caminho, str(ano_anterior), pasta_mes_e_ano)

    # Cria a pasta automaticamente, se não existir
    os.makedirs(caminho_atual, exist_ok=True)

    print(f"📁 Pasta criada ou já existente: {caminho_atual}")
    return caminho_atual
