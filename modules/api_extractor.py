import requests
import pandas as pd
from datetime import datetime
import os
import time

def extrair_dados_api(
    diretorio_arquivo_competencia,
    caminho_to_save,
    nome_api,
    env_var_url,
    payload_func,
    processar_func=None,
    timeout=60,
    tracker=None
):
    """
    Função genérica para extrair dados de qualquer API
    
    Args:
        diretorio_arquivo_competencia: Caminho do Excel com competências
        caminho_to_save: Diretório para salvar o resultado
        nome_api: Nome da API (ex: "Consumo", "Folha")
        env_var_url: Nome da variável de ambiente com a URL base
        payload_func: Função que recebe (unidade) e retorna payload
        processar_func: Função que recebe (dados_json, unidade) e retorna DataFrame
        timeout: Timeout da requisição em segundos
        tracker: Instância de ExecutionTracker para registrar execuções
    """
    
    print(f"\n{'='*60}")
    print(f"🚀 Iniciando extração: {nome_api}")
    print(f"{'='*60}\n")
    
    # Validações iniciais
    if not os.path.exists(diretorio_arquivo_competencia):
        print(f"❌ Arquivo não encontrado: {diretorio_arquivo_competencia}")
        return None
    
    if not os.path.exists(caminho_to_save):
        print(f"❌ Diretório de destino não existe: {caminho_to_save}")
        return None
    
    url_base = os.getenv(env_var_url)
    if not url_base:
        print(f"❌ Variável de ambiente '{env_var_url}' não configurada!")
        return None

    # Leitura do arquivo de competências
    try:
        df_consolidado = pd.read_excel(diretorio_arquivo_competencia)
    except Exception as e:
        print(f"❌ Erro ao ler arquivo de competências: {e}")
        return None
    
    # Validação de colunas necessárias
    colunas_necessarias = ['unidade_id', 'token', 'competencia', 'nome', 'situacao']
    colunas_faltantes = [col for col in colunas_necessarias if col not in df_consolidado.columns]
    if colunas_faltantes:
        print(f"❌ Colunas faltantes no arquivo: {colunas_faltantes}")
        return None

    # Filtra apenas competências fechadas
    df_consolidado = df_consolidado[df_consolidado['situacao'] != "ABERTA"]
    
    if df_consolidado.empty:
        print("⚠️ Nenhuma competência fechada encontrada")
        return None
    
    print(f"📊 Total de competências a processar: {len(df_consolidado)}")

    dados_extraidos = []
    total = len(df_consolidado)
    erros = []

    # Loop pelas unidades
    for idx, unidade in df_consolidado.iterrows():
        id_unidade = unidade['unidade_id']
        token = unidade['token']
        nome_unidade = unidade['nome']
        competencia = unidade['competencia']
        
        print(f"🔄 [{idx + 1}/{total}] {nome_unidade} - {competencia}")
        
        inicio_requisicao = time.time()
        
        # Monta payload específico da API
        try:
            payload = payload_func(unidade)
        except Exception as e:
            erro_msg = f"Erro ao montar payload - {e}"
            erros.append(f"{nome_unidade}: {erro_msg}")
            print(f"   ⚠️ {erro_msg}")
            
            if tracker:
                tracker.registrar_execucao(
                    endpoint=nome_api,
                    unidade=nome_unidade,
                    competencia=competencia,
                    status='erro',
                    erro=erro_msg
                )
            continue

        url = f"{url_base}{id_unidade}"
        headers = {"Authorization": f"Bearer {token}"}

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=timeout)
            tempo_execucao = time.time() - inicio_requisicao
            
            if response.status_code == 200:
                try:
                    dados = response.json()
                    
                    # Processa response (customizado ou padrão)
                    if processar_func:
                        df_dados = processar_func(dados, unidade)
                    else:
                        # Processamento padrão
                        if 'items' in dados and dados['items']:
                            df_dados = pd.DataFrame(dados['items'])
                            df_dados['unidade'] = nome_unidade
                            df_dados['competencia'] = competencia
                        else:
                            print(f"   ⚠️ Resposta sem dados")
                            
                            if tracker:
                                tracker.registrar_execucao(
                                    endpoint=nome_api,
                                    unidade=nome_unidade,
                                    competencia=competencia,
                                    status='sem_dados',
                                    tempo_execucao=tempo_execucao
                                )
                            continue
                    
                    if df_dados is not None and not df_dados.empty:
                        dados_extraidos.append(df_dados)
                        qtd_registros = len(df_dados)
                        print(f"   ✅ {qtd_registros} registros coletados")
                        
                        if tracker:
                            tracker.registrar_execucao(
                                endpoint=nome_api,
                                unidade=nome_unidade,
                                competencia=competencia,
                                status='sucesso',
                                registros=qtd_registros,
                                tempo_execucao=tempo_execucao
                            )
                    else:
                        print(f"   ⚠️ Nenhum dado retornado")
                        
                        if tracker:
                            tracker.registrar_execucao(
                                endpoint=nome_api,
                                unidade=nome_unidade,
                                competencia=competencia,
                                status='sem_dados',
                                tempo_execucao=tempo_execucao
                            )
                        
                except (ValueError, KeyError) as e:
                    erro_msg = f"Erro ao processar JSON - {e}"
                    erros.append(f"{nome_unidade}: {erro_msg}")
                    print(f"   ⚠️ {erro_msg}")
                    
                    if tracker:
                        tracker.registrar_execucao(
                            endpoint=nome_api,
                            unidade=nome_unidade,
                            competencia=competencia,
                            status='erro',
                            erro=erro_msg,
                            tempo_execucao=tempo_execucao
                        )
                    
            elif response.status_code == 401:
                erro_msg = "Token inválido"
                erros.append(f"{nome_unidade}: {erro_msg}")
                print(f"   ❌ {erro_msg}")
                
                if tracker:
                    tracker.registrar_execucao(
                        endpoint=nome_api,
                        unidade=nome_unidade,
                        competencia=competencia,
                        status='erro',
                        erro=f"HTTP 401 - {erro_msg}",
                        tempo_execucao=tempo_execucao
                    )
                    
            elif response.status_code == 404:
                erro_msg = "Endpoint não encontrado"
                erros.append(f"{nome_unidade}: {erro_msg}")
                print(f"   ❌ {erro_msg}")
                
                if tracker:
                    tracker.registrar_execucao(
                        endpoint=nome_api,
                        unidade=nome_unidade,
                        competencia=competencia,
                        status='erro',
                        erro=f"HTTP 404 - {erro_msg}",
                        tempo_execucao=tempo_execucao
                    )
            else:
                erro_msg = f"Erro HTTP {response.status_code}"
                erros.append(f"{nome_unidade}: {erro_msg}")
                print(f"   ❌ {erro_msg}")
                
                if tracker:
                    tracker.registrar_execucao(
                        endpoint=nome_api,
                        unidade=nome_unidade,
                        competencia=competencia,
                        status='erro',
                        erro=erro_msg,
                        tempo_execucao=tempo_execucao
                    )

        except requests.exceptions.Timeout:
            tempo_execucao = time.time() - inicio_requisicao
            erro_msg = f"Timeout (>{timeout}s)"
            erros.append(f"{nome_unidade}: {erro_msg}")
            print(f"   ⏱️ {erro_msg}")

            if tracker:
                tracker.registrar_execucao(
                    endpoint=nome_api,
                    unidade=nome_unidade,
                    competencia=competencia,
                    status='timeout',
                    erro=erro_msg,
                    tempo_execucao=tempo_execucao
                )
                
        except requests.exceptions.RequestException as e:
            tempo_execucao = time.time() - inicio_requisicao
            erro_msg = f"Erro na requisição - {str(e)[:100]}"
            erros.append(f"{nome_unidade}: {erro_msg}")
            print(f"   ❌ {erro_msg}")
            
            if tracker:
                tracker.registrar_execucao(
                    endpoint=nome_api,
                    unidade=nome_unidade,
                    competencia=competencia,
                    status='erro',
                    erro=erro_msg,
                    tempo_execucao=tempo_execucao
                )
                
        except Exception as e:
            tempo_execucao = time.time() - inicio_requisicao
            erro_msg = f"Erro inesperado - {str(e)[:100]}"
            erros.append(f"{nome_unidade}: {erro_msg}")
            print(f"   ⚠️ {erro_msg}")
            
            if tracker:
                tracker.registrar_execucao(
                    endpoint=nome_api,
                    unidade=nome_unidade,
                    competencia=competencia,
                    status='erro',
                    erro=erro_msg,
                    tempo_execucao=tempo_execucao
                )

    # Consolidação e salvamento
    if not dados_extraidos:
        print(f"\n❌ Nenhum dado de {nome_api} foi extraído")
        if erros:
            print(f"\n⚠️ Erros encontrados ({len(erros)}):")
            for erro in erros[:5]:
                print(f"   - {erro}")
        return None
    
    try:
        df_final = pd.concat(dados_extraidos, ignore_index=True)
        
        # Calcula mês/ano anterior
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
        nome_arquivo = f"api_{nome_api.lower()}_{mes_e_ano}.csv"
        caminho_arquivo = os.path.join(caminho_to_save, nome_arquivo)

        # Salva o arquivo
        df_final.to_csv(caminho_arquivo, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"\n{'='*60}")
        print(f"✅ {nome_api} extraído com sucesso!")
        print(f"📊 Total de registros: {len(df_final)}")
        print(f"📍 Arquivo: {caminho_arquivo}")
        if erros:
            print(f"⚠️ Houve {len(erros)} erro(s) durante a extração")
        print(f"{'='*60}\n")
        
        return caminho_arquivo
        
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo: {e}")
        return None