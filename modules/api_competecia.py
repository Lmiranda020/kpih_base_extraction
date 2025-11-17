import requests
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

def api_competencia(caminho):
    # Validações iniciais
    if not os.path.exists(caminho):
        print(f"❌ Caminho não existe: {caminho}")
        return None
    
    url_base = os.getenv("url_competencia")
    if not url_base:
        print("❌ Variável de ambiente 'url_competencia' não configurada!")
        return None
    
    if not os.path.exists(r'data\unidades_tokens.xlsx'):
        print("❌ Arquivo 'unidades_tokens.xlsx' não encontrado!")
        return None

    df_unidades = pd.read_excel(r'data\unidades_tokens.xlsx')
    
    if df_unidades.empty:
        print("❌ Arquivo Excel está vazio!")
        return None

    arquivo_unico = []
    total_unidades = len(df_unidades)

    # Loop pelas unidades
    for idx, unidade in df_unidades.iterrows():
        id_unidade = unidade['id']
        token = unidade['token']
        nome_unidade = unidade.get('nome', f'ID {id_unidade}')
        
        print(f"🔄 Processando {idx + 1}/{total_unidades}: {nome_unidade}")
        
        url = f"{url_base}{id_unidade}"
        headers = {"Authorization": f"Bearer {token}"}

        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            competencias = response.json()
            lista_competencias = competencias.get('items', [])

            if lista_competencias:
                df_competencias = pd.DataFrame(lista_competencias)
                df_competencias['unidade_id'] = id_unidade
                df_competencias = df_competencias.merge(
                    df_unidades[['id', 'nome', 'token']],
                    left_on='unidade_id',
                    right_on='id',
                    how='left',
                    suffixes=('', '_info')
                ).drop(columns=['id'])
                
                arquivo_unico.append(df_competencias)
                print(f"✅ {len(lista_competencias)} registros coletados")
            else:
                print(f"⚠️ Nenhum registro encontrado")
                
        except requests.exceptions.Timeout:
            print(f"⏱️ Timeout na requisição")
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na requisição: {e}")
        except Exception as e:
            print(f"⚠️ Erro inesperado: {e}")

    # Consolidação
    if not arquivo_unico:
        print("❌ Nenhuma competência foi coletada.")
        return None
    
    df_consolidado = pd.concat(arquivo_unico, ignore_index=True)
    
    # Filtros e transformações
    df_consolidado = df_consolidado[df_consolidado['ano'].astype(int) >= 2024]
    df_consolidado['competencia'] = (
        df_consolidado['mes'].astype(str).str.zfill(2) + '/' + 
        df_consolidado['ano'].astype(str)
    )
    
    # Salvamento
    nome_arquivo = "competencias_todas_unidades.xlsx"
    caminho_arquivo = os.path.join(caminho, nome_arquivo)
    
    try:
        df_consolidado.to_excel(caminho_arquivo, index=False)
        print(f"📁 ✅ Arquivo salvo: {caminho_arquivo}")
        print(f"📊 Total de {len(df_consolidado)} registros consolidados")
        return caminho_arquivo
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo: {e}")
        return None