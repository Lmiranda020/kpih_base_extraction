import requests
import pandas as pd
from datetime import datetime
import os
import time
import json
import random

# def gerar_curl(url, headers, payload):
#     """Gera comando cURL para debug"""
#     curl = f"curl -X POST '{url}' \\\n"
    
#     for key, value in headers.items():
#         curl += f"  -H '{key}: {value}' \\\n"
    
#     curl += f"  -H 'Content-Type: application/json' \\\n"
#     curl += f"  -d '{json.dumps(payload)}'"
    
#     return curl


def fazer_requisicao_com_retry(
    url, 
    headers, 
    payload, 
    timeout, 
    max_tentativas=4,
    backoff_inicial=2.0,
    nome_unidade="",
    competencia=""
):
    """
    Faz requisição com retry automático em caso de erro 403
    
    Args:
        url: URL da requisição
        headers: Headers da requisição
        payload: Payload JSON
        timeout: Timeout da requisição
        max_tentativas: Número máximo de tentativas
        backoff_inicial: Tempo inicial de espera (será multiplicado a cada tentativa)
        nome_unidade: Nome da unidade (para logs)
        competencia: Competência (para logs)
    
    Returns:
        tuple: (response, tempo_execucao, tentativa_sucesso)
    """
    
    for tentativa in range(1, max_tentativas + 1):
        inicio = time.time()
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=timeout)
            tempo_execucao = time.time() - inicio
            
            # Se não for 403, retorna imediatamente (sucesso ou outro erro)
            if response.status_code != 403:
                if tentativa > 1:
                    print(f"   ✅ Sucesso na tentativa {tentativa}")
                return response, tempo_execucao, tentativa
            
            # Se for 403 e não for a última tentativa, faz retry
            if tentativa < max_tentativas:
                # Backoff exponencial com jitter
                tempo_espera = backoff_inicial * (2 ** (tentativa - 1))
                jitter = random.uniform(0, 0.5)
                tempo_total = tempo_espera + jitter
                
                print(f"   ⚠️ HTTP 403 (tentativa {tentativa}/{max_tentativas})")
                print(f"   ⏳ Aguardando {tempo_total:.1f}s antes de tentar novamente...")
                time.sleep(tempo_total)
            else:
                # Última tentativa falhou
                print(f"   ❌ HTTP 403 após {max_tentativas} tentativas")
                return response, tempo_execucao, tentativa
                
        except requests.exceptions.Timeout:
            tempo_execucao = time.time() - inicio
            if tentativa < max_tentativas:
                print(f"   ⏱️ Timeout (tentativa {tentativa}/{max_tentativas})")
                print(f"   ⏳ Aguardando 3s antes de tentar novamente...")
                time.sleep(3)
            else:
                raise
        
        except requests.exceptions.RequestException:
            # Erros de conexão não devem fazer retry
            raise
    
    # Não deveria chegar aqui, mas por segurança
    return response, tempo_execucao, max_tentativas

def carregar_de_para_unidades():
    """
    Carrega o arquivo de DE-PARA de unidades com tratamento correto de encoding
    
    Returns:
        DataFrame com as informações de unidades ou None se houver erro
    """
    try:
        caminho_base = os.getenv('caminho_de_para_unidades')
        
        if not caminho_base:
            print("⚠️ Variável 'caminho_de_para_unidades' não configurada no .env")
            return None
        
        caminho_arquivo = os.path.join(caminho_base, "Unidades.xlsx")
        
        if not os.path.exists(caminho_arquivo):
            print(f"⚠️ Arquivo de DE-PARA não encontrado: {caminho_arquivo}")
            return None
        
        # Lê o arquivo Excel com engine openpyxl para melhor suporte a encoding
        df_unidades = pd.read_excel(
            caminho_arquivo,
            engine='openpyxl'
        )
        
        # Função para corrigir encoding corrompido
        def corrigir_encoding(texto):
            if not isinstance(texto, str):
                return texto
            
            # Lista de padrões de encoding corrompido
            padroes_corrompidos = ['Ã§', 'Ã£', 'Ã©', 'Ã', 'Ã­', 'Ã³', 'Ãº', 'Ã¡', 'Ã¢', 'Ãª', 'Ã´']
            
            # Se não tem padrões problemáticos, retorna como está (já está correto!)
            if not any(padrao in texto for padrao in padroes_corrompidos):
                return texto
            
            # Tenta diferentes estratégias de correção
            try:
                # Estratégia 1: UTF-8 mal interpretado como Latin-1
                corrigido = texto.encode('latin-1').decode('utf-8')
                # Valida se a correção melhorou (não deve ter Ã depois da correção)
                if not any(padrao in corrigido for padrao in padroes_corrompidos):
                    return corrigido
            except (UnicodeDecodeError, UnicodeEncodeError):
                pass
            
            try:
                # Estratégia 2: CP1252 (Windows) mal interpretado
                corrigido = texto.encode('cp1252').decode('utf-8')
                if not any(padrao in corrigido for padrao in padroes_corrompidos):
                    return corrigido
            except (UnicodeDecodeError, UnicodeEncodeError):
                pass
            
            try:
                # Estratégia 3: ISO-8859-1
                corrigido = texto.encode('iso-8859-1').decode('utf-8')
                if not any(padrao in corrigido for padrao in padroes_corrompidos):
                    return corrigido
            except (UnicodeDecodeError, UnicodeEncodeError):
                pass
            
            # Se chegou aqui, nenhuma estratégia funcionou - retorna original
            print(f"   ⚠️ Não foi possível corrigir: {texto[:60]}...")
            return texto
        
        # Aplica correção em todas as colunas de texto
        for col in df_unidades.select_dtypes(include=['object']).columns:
            df_unidades[col] = df_unidades[col].apply(corrigir_encoding)
        
        print(f"✅ Arquivo DE-PARA carregado: {len(df_unidades)} unidades")
        print(f"   Colunas disponíveis: {', '.join(df_unidades.columns.tolist())}")
        
        # Mostra exemplos de nomes para validar encoding
        if 'unidade' in df_unidades.columns or 'nome' in df_unidades.columns:
            col_exemplo = 'unidade' if 'unidade' in df_unidades.columns else 'nome'
            print(f"   Exemplos de nomes (primeiros 5):")
            for nome in df_unidades[col_exemplo].head(5):
                print(f"      - {nome}")
            
            # Verifica se ainda existem problemas de encoding
            padroes_corrompidos = ['Ã§', 'Ã£', 'Ã©', 'Ã', 'Ã­', 'Ã³', 'Ãº', 'Ã¡', 'Ã¢', 'Ãª', 'Ã´']
            problemas = df_unidades[col_exemplo].apply(
                lambda x: any(padrao in str(x) for padrao in padroes_corrompidos) if pd.notna(x) else False
            ).sum()
            
            if problemas > 0:
                print(f"   ⚠️ {problemas} registro(s) ainda com possíveis problemas de encoding")
            else:
                print(f"   ✅ Nenhum problema de encoding detectado")
        
        return df_unidades
        
    except Exception as e:
        print(f"❌ Erro ao carregar arquivo DE-PARA: {e}")
        import traceback
        print(f"   Detalhes: {traceback.format_exc()}")
        return None


def aplicar_de_para_unidades(df_final, coluna_unidade='unidade'):
    """
    Aplica o DE-PARA de unidades ao DataFrame final com tratamento de encoding
    
    Args:
        df_final: DataFrame com os dados extraídos
        coluna_unidade: Nome da coluna que contém o nome da unidade
    
    Returns:
        DataFrame com as informações de unidades mescladas
    """
    df_unidades = carregar_de_para_unidades()
    
    if df_unidades is None:
        print("⚠️ Continuando sem aplicar DE-PARA de unidades")
        return df_final
    
    # Identifica a coluna de nome da unidade no arquivo DE-PARA
    colunas_possiveis = ['unidade', 'nome', 'nome_unidade', 'Unidade', 'Nome']
    coluna_merge = None
    
    for col in colunas_possiveis:
        if col in df_unidades.columns:
            coluna_merge = col
            break
    
    if coluna_merge is None:
        print(f"⚠️ Não foi possível identificar a coluna de nome da unidade")
        print(f"   Colunas disponíveis: {df_unidades.columns.tolist()}")
        return df_final
    
    try:
        # Normaliza strings em ambos os DataFrames antes do merge
        # Remove espaços extras e padroniza
        df_final[coluna_unidade] = df_final[coluna_unidade].str.strip()
        df_unidades[coluna_merge] = df_unidades[coluna_merge].str.strip()
        
        # Faz o merge (VLOOKUP)
        df_final_com_depara = df_final.merge(
            df_unidades,
            left_on=coluna_unidade,
            right_on=coluna_merge,
            how='left'
        )
        
        # Conta quantas unidades encontraram match
        total_registros = len(df_final)
        registros_com_match = df_final_com_depara[coluna_merge].notna().sum()
        
        print(f"📊 DE-PARA aplicado:")
        print(f"   Total de registros: {total_registros}")
        print(f"   Registros com match: {registros_com_match}")
        
        if registros_com_match < total_registros:
            unidades_sem_match = df_final[~df_final[coluna_unidade].isin(df_unidades[coluna_merge])][coluna_unidade].unique()
            print(f"   ⚠️ {len(unidades_sem_match)} unidade(s) sem match:")
            for unidade in list(unidades_sem_match)[:5]:
                print(f"      - {unidade}")
            if len(unidades_sem_match) > 5:
                print(f"      ... e mais {len(unidades_sem_match) - 5}")
        
        return df_final_com_depara
        
    except Exception as e:
        print(f"❌ Erro ao aplicar DE-PARA: {e}")
        import traceback
        print(f"   Detalhes: {traceback.format_exc()}")
        return df_final

def padronizar_competencia(competencia):
    """
    Converte competência de 'jan/2025' para '01/2025'
    
    Args:
        competencia: String no formato 'mes/ano' ou já no formato 'MM/YYYY'
    
    Returns:
        String no formato 'MM/YYYY'
    """
    if pd.isna(competencia):
        return competencia
    
    competencia_str = str(competencia).strip()
    
    # Dicionário de meses
    meses = {
        'jan': '01', 'fev': '02', 'mar': '03', 'abr': '04',
        'mai': '05', 'jun': '06', 'jul': '07', 'ago': '08',
        'set': '09', 'out': '10', 'nov': '11', 'dez': '12'
    }
    
    # Se já está no formato numérico, retorna
    if '/' in competencia_str:
        partes = competencia_str.split('/')
        if len(partes) == 2 and partes[0].isdigit() and len(partes[0]) == 2:
            return competencia_str
    
    # Converte de texto para numérico
    for mes_texto, mes_numero in meses.items():
        if mes_texto.lower() in competencia_str.lower():
            ano = competencia_str.split('/')[-1].strip()
            return f"{mes_numero}/{ano}"
    
    # Se não encontrou correspondência, retorna original
    return competencia_str

def extrair_dados_api(
    diretorio_arquivo_competencia,
    caminho_to_save,
    nome_api,
    env_var_url,
    payload_func,
    processar_func=None,
    timeout=60,
    tracker=None,
    delay_entre_chamadas=0.5,
    max_tentativas_403=4,
    backoff_inicial=2.0,
    agrupar_por_unidade=True,
    delay_entre_unidades=2.0
):
    """
    Função genérica para extrair dados de qualquer API com retry automático
    
    Args:
        diretorio_arquivo_competencia: Caminho do Excel com competências
        caminho_to_save: Diretório para salvar o resultado
        nome_api: Nome da API (ex: "Consumo", "Folha")
        env_var_url: Nome da variável de ambiente com a URL base
        payload_func: Função que recebe (unidade) e retorna payload
        processar_func: Função que recebe (dados_json, unidade) e retorna DataFrame
        timeout: Timeout da requisição em segundos
        tracker: Instância de ExecutionTracker para registrar execuções
        delay_entre_chamadas: Delay entre cada requisição (em segundos)
        max_tentativas_403: Número máximo de tentativas quando receber 403
        backoff_inicial: Tempo inicial de espera entre tentativas (dobra a cada retry)
        agrupar_por_unidade: Se True, processa todas competências de uma unidade antes de passar para próxima
        delay_entre_unidades: Delay maior ao mudar de unidade (em segundos)
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
    
    # Reordena para agrupar por unidade se solicitado
    if agrupar_por_unidade:
        df_consolidado = df_consolidado.sort_values(['unidade_id', 'competencia']).reset_index(drop=True)
        print(f"📋 Processamento agrupado por unidade")
    
    print(f"📊 Total de competências a processar: {len(df_consolidado)}")
    print(f"🔄 Retry automático: {max_tentativas_403} tentativas para erros 403")
    print(f"⏱️ Delay entre requisições: {delay_entre_chamadas}s")
    if agrupar_por_unidade:
        print(f"⏱️ Delay entre unidades: {delay_entre_unidades}s")
    print()

    dados_extraidos = []
    total = len(df_consolidado)
    erros = []
    erros_403_persistentes = []
    
    # arquivo_curl = os.path.join(caminho_to_save, f"curls_erro_403_{nome_api}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    
    unidade_anterior = None

    # Loop pelas unidades
    for idx, unidade in df_consolidado.iterrows():
        id_unidade = unidade['unidade_id']
        token = unidade['token']
        nome_unidade = unidade['nome']
        competencia = unidade['competencia']
        
        # Detecta mudança de unidade e adiciona delay maior
        if agrupar_por_unidade and unidade_anterior is not None and unidade_anterior != id_unidade:
            print(f"\n🔄 Mudando de unidade (delay de {delay_entre_unidades}s)...\n")
            time.sleep(delay_entre_unidades)
        
        unidade_anterior = id_unidade
        
        print(f"🔄 [{idx + 1}/{total}] {nome_unidade} - {competencia}")
        
        # Monta payload específico da API
        try:
            payload = payload_func(unidade)
            # print(f"\n{'='*60}")
            # print(f"🔍 DEBUG - Detalhes da requisição:")
            # print(f"   Unidade: {nome_unidade} (ID: {id_unidade})")
            # print(f"   Competência: {competencia}")
            # print(f"   URL completa: {url_base}")
            # print(f"   Payload enviado:")
            # print(f"   {json.dumps(payload, indent=6, ensure_ascii=False)}")
            # print(f"{'='*60}\n")
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
        # print(f"   URL completa: {url}")

        headers = {"Authorization": f"Bearer {token}"}

        # print(f"\n{'='*60}")
        # print(f"🔍 DEBUG - Detalhes da requisição:")
        # print(f"   Unidade: {nome_unidade} (ID: {id_unidade})")
        # print(f"   Competência: {competencia}")
        # print(f"   URL completa: {url}")
        # print(f"   Payload enviado:")
        # print(f"   {json.dumps(payload, indent=6, ensure_ascii=False)}")
        # print(f"Headers: {headers}")
        # print(f"{'='*60}\n")

        try:
            # Usa função com retry
            response, tempo_execucao, tentativa = fazer_requisicao_com_retry(
                url=url,
                headers=headers,
                payload=payload,
                timeout=timeout,
                max_tentativas=max_tentativas_403,
                backoff_inicial=backoff_inicial,
                nome_unidade=nome_unidade,
                competencia=competencia
            )
            
            if response.status_code == 200:
                try:
                    dados = response.json()

                    # # ===== ADICIONE ESTE DEBUG AQUI =====
                    # print(f"   🔍 DEBUG - Estrutura da resposta:")
                    # print(f"   Tipo: {type(dados)}")
                    # print(f"   Chaves: {dados.keys() if isinstance(dados, dict) else 'Não é dict'}")
                    # print(f"   Conteúdo (primeiros 500 chars): {str(dados)[:500]}")
                    # # ===================================
                    
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

            elif response.status_code == 500:
                # Erro 500 geralmente indica que:
                # 1. A competência solicitada não está disponível para extração
                # 2. O tipo de relatório não é aplicável para esta unidade
                try:
                    response_data = response.json()
                    erro_detalhes = response_data.get('message', 'Sem detalhes')
                except:
                    erro_detalhes = 'Não foi possível obter detalhes do erro'
                
                erro_msg = f"Competência indisponível ou relatório não aplicável - {erro_detalhes}"
                erros.append(f"{nome_unidade}: {erro_msg}")
                print(f"   ⚠️ {erro_msg}")
                print(f"   💡 Possíveis causas:")
                print(f"      • Competência {competencia} ainda não processada para este relatório")
                print(f"      • Tipo de unidade incompatível (ex: UBS/UPA sem linha de contratação)")
                
                if tracker:
                    tracker.registrar_execucao(
                        endpoint=nome_api,
                        unidade=nome_unidade,
                        competencia=competencia,
                        status='indisponivel',
                        erro=erro_msg,
                        tempo_execucao=tempo_execucao
                    )
            
            elif response.status_code == 403:
                # Salva cURL apenas para 403 que persistiram após todas as tentativas
                erro_msg = f"Erro HTTP 403 (após {tentativa} tentativas)"
                erros.append(f"{nome_unidade}: {erro_msg}")
                erros_403_persistentes.append(f"{nome_unidade} - {competencia}")
                
                # curl_command = gerar_curl(url, headers, payload)
                
                # with open(arquivo_curl, 'a', encoding='utf-8') as f:
                #     f.write(f"\n{'='*80}\n")
                #     f.write(f"API: {nome_api}\n")
                #     f.write(f"Unidade: {nome_unidade}\n")
                #     f.write(f"Competência: {competencia}\n")
                #     f.write(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                #     f.write(f"Status: 403 Forbidden (após {tentativa} tentativas)\n")
                #     # f.write(f"\ncURL:\n{curl_command}\n")
                #     f.write(f"{'='*80}\n")
                
                # print(f"   💾 cURL salvo em: {arquivo_curl}")
                
                if tracker:
                    tracker.registrar_execucao(
                        endpoint=nome_api,
                        unidade=nome_unidade,
                        competencia=competencia,
                        status='erro',
                        erro=erro_msg,
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
            tempo_execucao = timeout
            erro_msg = f"Timeout (>{timeout}s) após {max_tentativas_403} tentativas"
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
                    tempo_execucao=0
                )
                
        except Exception as e:
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
                    tempo_execucao=0
                )
        
        # Delay entre requisições (exceto na última)
        if idx < total - 1:
            print(f"   ⏳ Aguardando {delay_entre_chamadas}s antes da próxima requisição...")
            time.sleep(delay_entre_chamadas)

    # Consolidação e salvamento
    if not dados_extraidos:
        print(f"\n❌ Nenhum dado de {nome_api} foi extraído")
        if erros:
            print(f"\n⚠️ Erros encontrados ({len(erros)}):")
            for erro in erros[:5]:
                print(f"   - {erro}")
        
        if erros_403_persistentes:
            print(f"\n🚨 Erros 403 persistentes ({len(erros_403_persistentes)}):")
            for erro_403 in erros_403_persistentes[:10]:
                print(f"   - {erro_403}")
        
        # if os.path.exists(arquivo_curl):
        #     print(f"\n📋 Arquivo com cURLs de erros 403 gerado:")
        #     print(f"   {arquivo_curl}")
        
        return None
    
    try:
        df_final = pd.concat(dados_extraidos, ignore_index=True)

        print(f"\n{'='*60}")
        print(f"🔄 Aplicando DE-PARA de unidades...")
        print(f"{'='*60}")
        
        df_final = aplicar_de_para_unidades(df_final, coluna_unidade='unidade')
        
        print(f"{'='*60}\n")
        
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

        # converte a coluna de comptencia para deixar no formato 01/2025
        if 'competencia' in df_final.columns:
            print(f"\n🔄 Padronizando formato da competência...")
            df_final['competencia'] = df_final['competencia'].apply(padronizar_competencia)
            print(f"✅ Competências padronizadas para formato MM/YYYY")

        # excluir colunas desnecessárias
        colunas_para_excluir = ['NomeCompletoUnidade', 'competencia']
        apis_que_precisam_manter_competencia = ['benchmarkcomposicaodecustos', 'painelcomparativodecustos']
        for coluna in colunas_para_excluir:
            if coluna in df_final.columns:
                if coluna == 'competencia' and nome_api.lower() in apis_que_precisam_manter_competencia:
                    continue    
                df_final = df_final.drop(columns=[coluna])

        # Define nome e caminho do arquivo
        nome_arquivo = f"api_{nome_api.lower()}_{mes_e_ano}.csv"
        caminho_arquivo = os.path.join(caminho_to_save, nome_arquivo)

        # Normaliza encoding de todas as colunas texto
        #x não é a coluna inteira, mas sim cada valor individual (cada célula) dentro daquela coluna
        print(f"🔄 Normalizando encoding dos dados...")
        for col in df_final.select_dtypes(include=['object']).columns:
            df_final[col] = df_final[col].apply(
                lambda x: x.encode('utf-8', errors='ignore').decode('utf-8') if isinstance(x, str) else x
            )

        # Salva o arquivo
        df_final.to_csv(caminho_arquivo, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"\n{'='*60}")
        print(f"✅ {nome_api} extraído com sucesso!")
        print(f"📊 Total de registros: {len(df_final)}")
        print(f"📍 Arquivo: {caminho_arquivo}")
        if erros:
            print(f"⚠️ Houve {len(erros)} erro(s) durante a extração")
        
        if erros_403_persistentes:
            print(f"🚨 {len(erros_403_persistentes)} erro(s) 403 persistentes (após retries)")
        
        # if os.path.exists(arquivo_curl):
        #     print(f"📋 Arquivo com cURLs de erros 403:")
        #     print(f"   {arquivo_curl}")
        
        print(f"{'='*60}\n")
        
        return caminho_arquivo
        
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo: {e}")
        return None