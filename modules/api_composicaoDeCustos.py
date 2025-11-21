from modules.api_extractor import extrair_dados_api
from config.api_config import APIS_CONFIG
import pandas as pd

def api_composicaoDeCustos(
    diretorio_arquivo_competencia, 
    caminho_to_save, 
    tracker=None, 
    delay_entre_chamadas=0.2,
    max_tentativas_403=3,
    backoff_inicial=3.0,
    agrupar_por_unidade=True,
    delay_entre_unidades=5.0,
    filtrar_tipo_unidade=True  # ← NOVO: opção para filtrar tipos de unidade
):
    """
    Extrai dados de Composição de Custos com retry automático
    
    IMPORTANTE: Este relatório só está disponível para unidades com linha de contratação
    (ex: Hospitais, AME, LUCI). UBS, UPA e outros tipos podem retornar erro 500.
    
    Args:
        diretorio_arquivo_competencia: Caminho do arquivo de competências
        caminho_to_save: Diretório para salvar os dados
        tracker: ExecutionTracker para registrar execuções (opcional)
        delay_entre_chamadas: Delay entre requisições (padrão: 0.2s)
        max_tentativas_403: Tentativas de retry para erro 403 (padrão: 3)
        backoff_inicial: Tempo inicial de espera no retry (padrão: 3.0s)
        agrupar_por_unidade: Agrupa processamento por unidade (padrão: True)
        delay_entre_unidades: Delay ao mudar de unidade (padrão: 5.0s)
        filtrar_tipo_unidade: Filtra apenas unidades aplicáveis (padrão: True)
    """
    
    # Se filtrar_tipo_unidade estiver ativado, validar o arquivo antes
    if filtrar_tipo_unidade:
        try:
            df_temp = pd.read_excel(diretorio_arquivo_competencia)
            
            # Verifica se existe coluna 'nome'
            if 'nome' in df_temp.columns:
                # Palavras-chave que indicam unidades COM composição de custos
                palavras_chave_validas = [
                    'HOSPITAL',
                    'AME',
                    'LUCY',
                    'CER'
                ]
                
                # Cria regex pattern: busca qualquer uma das palavras (case-insensitive)
                # Exemplo: 'HOSPITAL|AME|LUCI'
                pattern = '|'.join(palavras_chave_validas)
                
                total_antes = len(df_temp)
                
                # Filtra linhas onde o nome contém alguma das palavras-chave
                df_temp_filtrado = df_temp[
                    df_temp['nome'].str.contains(pattern, case=False, na=False, regex=True)
                ]
                
                total_depois = len(df_temp_filtrado)
                
                if total_depois < total_antes:
                    print(f"\n⚠️ AVISO: Composição de Custos - Filtro por Nome de Unidade")
                    print(f"   📊 Total de competências no arquivo: {total_antes}")
                    print(f"   ✅ Competências aplicáveis (Hospital/AME/LUCI): {total_depois}")
                    print(f"   ⏭️ Competências ignoradas (UBS/UPA/outros): {total_antes - total_depois}")
                    print(f"   💡 Filtrado por palavras-chave: {', '.join(palavras_chave_validas)}\n")
                
                if total_depois == 0:
                    print(f"❌ ERRO: Nenhuma unidade aplicável encontrada para Composição de Custos")
                    print(f"   Este relatório requer unidades com: {', '.join(palavras_chave_validas)}")
                    return None
                
                # Salva arquivo filtrado temporariamente para usar na extração
                import tempfile
                import os
                
                # Cria arquivo temporário com apenas as unidades válidas
                temp_dir = os.path.dirname(diretorio_arquivo_competencia)
                temp_file = os.path.join(temp_dir, 'temp_composicao_filtrado.xlsx')
                df_temp_filtrado.to_excel(temp_file, index=False)
                
                # Atualiza caminho para usar arquivo filtrado
                diretorio_arquivo_competencia = temp_file
                    
            else:
                print(f"\n❌ ERRO: Coluna 'nome' não encontrada no arquivo")
                print(f"   Não é possível filtrar unidades sem a coluna de nome")
                return None
                
        except Exception as e:
            print(f"⚠️ Não foi possível validar tipos de unidade: {e}")
            print(f"   Continuando com todas as unidades...\n")
    
    config = APIS_CONFIG["composicaoDeCustos"]
    
    print(f"📋 Informações do Relatório:")
    print(f"   • Nome: Composição de Custos")
    print(f"   • Requer: Linha de contratação (Hospital/AME/LUCI)")
    print(f"   • Pode falhar para: UBS, UPA, outros tipos\n")
    
    resultado = extrair_dados_api(
        diretorio_arquivo_competencia=diretorio_arquivo_competencia,
        caminho_to_save=caminho_to_save,
        nome_api="composicaoDeCustos",
        env_var_url=config["env_var"],
        payload_func=config["payload_func"],
        processar_func=config["processar_func"],
        timeout=config["timeout"],
        tracker=tracker,
        delay_entre_chamadas=delay_entre_chamadas,
        max_tentativas_403=max_tentativas_403,
        backoff_inicial=backoff_inicial,
        agrupar_por_unidade=agrupar_por_unidade,
        delay_entre_unidades=delay_entre_unidades
    )
    
    # Remove arquivo temporário se foi criado
    if filtrar_tipo_unidade:
        try:
            temp_file = os.path.join(os.path.dirname(diretorio_arquivo_competencia), 'temp_composicao_filtrado.xlsx')
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass
    
    return resultado