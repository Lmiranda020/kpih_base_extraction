import requests
import pandas as pd
from datetime import datetime
import os
import time

def api_exercicioOrcamento(caminho):
    """
    Extrai dados de Exercício Orçamento para todas as unidades
    Esta API retorna dados gerais por unidade (não por competência)
    """
    print(f"\n{'='*60}")
    print(f"🚀 Iniciando extração: Exercício Orçamento")
    print(f"{'='*60}\n")
    
    # Validações iniciais
    if not os.path.exists(caminho):
        print(f"❌ Caminho não existe: {caminho}")
        return None
    
    url_base = os.getenv("url_exercicioOrcamento")
    if not url_base:
        print("❌ Variável de ambiente 'url_exercicioOrcamento' não configurada!")
        return None
    
    arquivo_unidades = r'data\unidades_tokens.xlsx'
    if not os.path.exists(arquivo_unidades):
        print(f"❌ Arquivo não encontrado: {arquivo_unidades}")
        return None

    # Leitura das unidades
    try:
        df_unidades = pd.read_excel(arquivo_unidades)
    except Exception as e:
        print(f"❌ Erro ao ler arquivo de unidades: {e}")
        return None
    
    if df_unidades.empty:
        print("❌ Arquivo de unidades está vazio!")
        return None

    print(f"📊 Total de unidades a processar: {len(df_unidades)}\n")

    dados_coletados = []
    total_unidades = len(df_unidades)
    erros = []

    # Loop pelas unidades
    for idx, unidade in df_unidades.iterrows():
        id_unidade = unidade['id']
        token = unidade['token']
        nome_unidade = unidade.get('nome', f'ID {id_unidade}')
        
        print(f"🔄 [{idx + 1}/{total_unidades}] {nome_unidade}")
        
        url = f"{url_base}{id_unidade}"
        headers = {"Authorization": f"Bearer {token}"}

        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                dados = response.json()
                lista_items = dados.get('items', [])

                if lista_items:
                    df_dados = pd.DataFrame(lista_items)
                    df_dados['unidade_id'] = id_unidade
                    df_dados['unidade_nome'] = nome_unidade
                    
                    dados_coletados.append(df_dados)
                    print(f"   ✅ {len(lista_items)} registros coletados")
                else:
                    print(f"   ⚠️ Resposta sem dados")
                    
            elif response.status_code == 401:
                erro_msg = f"{nome_unidade}: Token inválido (HTTP 401)"
                erros.append(erro_msg)
                print(f"   ❌ Token inválido")
                
            elif response.status_code == 403:
                erro_msg = f"{nome_unidade}: Acesso negado (HTTP 403)"
                erros.append(erro_msg)
                print(f"   ❌ Acesso negado")
                
            elif response.status_code == 404:
                erro_msg = f"{nome_unidade}: Endpoint não encontrado (HTTP 404)"
                erros.append(erro_msg)
                print(f"   ❌ Endpoint não encontrado")
                
            else:
                erro_msg = f"{nome_unidade}: Erro HTTP {response.status_code}"
                erros.append(erro_msg)
                print(f"   ❌ Erro HTTP {response.status_code}")
                
        except requests.exceptions.Timeout:
            erro_msg = f"{nome_unidade}: Timeout (>30s)"
            erros.append(erro_msg)
            print(f"   ⏱️ Timeout na requisição")
            
        except requests.exceptions.RequestException as e:
            erro_msg = f"{nome_unidade}: Erro de requisição - {str(e)[:50]}"
            erros.append(erro_msg)
            print(f"   ❌ Erro na requisição: {e}")
            
        except Exception as e:
            erro_msg = f"{nome_unidade}: Erro inesperado - {str(e)[:50]}"
            erros.append(erro_msg)
            print(f"   ⚠️ Erro inesperado: {e}")
        
        # Delay entre requisições (exceto na última)
        if idx < total_unidades - 1:
            time.sleep(0.5)

    # Consolidação e salvamento
    if not dados_coletados:
        print(f"\n❌ Nenhum dado foi coletado")
        if erros:
            print(f"\n⚠️ Erros encontrados ({len(erros)}):")
            for erro in erros[:5]:
                print(f"   - {erro}")
        return None
    
    try:
        df_consolidado = pd.concat(dados_coletados, ignore_index=True)
        
        # Calcula mês/ano anterior para nomenclatura
        data_execucao = datetime.today()
        mes = data_execucao.month
        ano = data_execucao.year

        if mes == 1:
            mes_anterior = 12
            ano_anterior = ano - 1
        else:
            mes_anterior = mes - 1
            ano_anterior = ano

        mes_e_ano = f"{mes_anterior:02d}_{ano_anterior}"
        
        # Define nome e caminho do arquivo
        nome_arquivo = f"api_exercicioorcamento_{mes_e_ano}.csv"
        caminho_arquivo = os.path.join(caminho, nome_arquivo)
        
        # Salva o arquivo
        df_consolidado.to_csv(caminho_arquivo, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"\n{'='*60}")
        print(f"✅ Exercício Orçamento extraído com sucesso!")
        print(f"📊 Total de registros: {len(df_consolidado)}")
        print(f"🏢 Unidades processadas: {len(dados_coletados)}/{total_unidades}")
        print(f"📍 Arquivo: {caminho_arquivo}")
        
        if erros:
            print(f"⚠️ Houve {len(erros)} erro(s) durante a extração")
        
        print(f"{'='*60}\n")
        
        return caminho_arquivo
        
    except Exception as e:
        print(f"\n❌ Erro ao salvar arquivo: {e}")
        return None