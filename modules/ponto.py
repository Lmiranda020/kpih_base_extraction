"""
Módulo para análise incremental de competências com detecção de reabertura
Analisa 2 meses anteriores para capturar competências que foram reabertas e fechadas
"""
"""
Módulo para análise incremental de competências com detecção de reabertura
Analisa 2 meses anteriores para capturar competências que foram reabertas e fechadas
"""
import os
import pandas as pd
import shutil
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import glob

class AnalisadorIncremental:
    """Gerencia a análise incremental de competências entre meses"""
    
    def __init__(self, caminho_atual):
        """
        Inicializa o analisador
        
        Args:
            caminho_atual: Caminho do diretório do mês vigente
        """
        self.caminho_atual = caminho_atual
        self.caminho_mes_1 = None  # Mês -1
        self.caminho_mes_2 = None  # Mês -2
        self._obter_caminhos_meses_anteriores()
        
    def _obter_caminhos_meses_anteriores(self):
        """
        Identifica os diretórios dos 2 meses anteriores
        """
        load_dotenv()
        caminho_fixo = os.getenv("caminho_fixo")
        
        if not caminho_fixo:
            print("❌ Variável 'caminho_fixo' não encontrada no .env")
            return
        
        # Extrai informações do caminho atual
        partes = self.caminho_atual.split(os.sep)
        pasta_competencia = partes[-1]  # Ex: "11_2024"
        
        try:
            mes_atual, ano_atual = pasta_competencia.split('_')
            mes_atual = int(mes_atual)
            ano_atual = int(ano_atual)
        except ValueError:
            print(f"❌ Formato inválido da pasta: {pasta_competencia}")
            return
        
        # Calcula mês -1
        if mes_atual == 1:
            mes_1 = 12
            ano_1 = ano_atual - 1
        else:
            mes_1 = mes_atual - 1
            ano_1 = ano_atual
        
        # Calcula mês -2
        if mes_1 == 1:
            mes_2 = 12
            ano_2 = ano_1 - 1
        else:
            mes_2 = mes_1 - 1
            ano_2 = ano_1
        
        # Monta caminhos
        pasta_mes_1 = f"{mes_1:02d}_{ano_1}"
        pasta_mes_2 = f"{mes_2:02d}_{ano_2}"
        
        self.caminho_mes_1 = os.path.join(caminho_fixo, str(ano_1), pasta_mes_1)
        self.caminho_mes_2 = os.path.join(caminho_fixo, str(ano_2), pasta_mes_2)
        
        # Verifica existência
        print("\n📂 VERIFICANDO MESES ANTERIORES:")
        
        if os.path.exists(self.caminho_mes_1):
            print(f"✅ Mês -1 encontrado: {pasta_mes_1}")
        else:
            print(f"⚠️ Mês -1 NÃO encontrado: {pasta_mes_1}")
            self.caminho_mes_1 = None
        
        if os.path.exists(self.caminho_mes_2):
            print(f"✅ Mês -2 encontrado: {pasta_mes_2}")
        else:
            print(f"⚠️ Mês -2 NÃO encontrado: {pasta_mes_2}")
            self.caminho_mes_2 = None
    
    def _carregar_competencias_mes(self, caminho_mes, rotulo):
        """
        Carrega arquivo de competências de um mês específico
        
        Args:
            caminho_mes: Caminho do diretório do mês
            rotulo: Rótulo para log (ex: "Mês -1")
            
        Returns:
            DataFrame ou None
        """
        if not caminho_mes:
            return None
        
        arquivo = os.path.join(caminho_mes, "competencias_todas_unidades.xlsx")
        
        if not os.path.exists(arquivo):
            print(f"⚠️ {rotulo}: Arquivo não encontrado")
            return None
        
        try:
            df = pd.read_excel(arquivo)
            print(f"✅ {rotulo}: {len(df)} competências carregadas")
            return df
        except Exception as e:
            print(f"❌ {rotulo}: Erro ao carregar - {e}")
            return None
    
    def filtrar_competencias_nao_processadas(self, arquivo_competencia_atual, 
                                             processar_somente_fechadas=True):
        """
        Filtra competências usando análise de 2 meses anteriores
        
        Lógica:
        1. Carrega competências do mês atual, mês -1 e mês -2
        2. Identifica competências que estavam FECHADAS no mês -2
        3. Remove competências que continuaram FECHADAS em AMBOS os meses anteriores
        4. Mantém competências que foram REABERTAS (fechada → reaberta → fechada)
        
        Args:
            arquivo_competencia_atual: Caminho do arquivo de competências do mês vigente
            processar_somente_fechadas: Se True, processa apenas competências fechadas
            
        Returns:
            str: Caminho do arquivo filtrado ou None
        """
        print("\n" + "="*70)
        print("🔍 ANÁLISE INCREMENTAL COM 2 MESES ANTERIORES")
        print("="*70)
        
        # ====================================================================
        # PASSO 1: Carregar competências do mês atual
        # ====================================================================
        if not os.path.exists(arquivo_competencia_atual):
            print(f"❌ Arquivo não encontrado: {arquivo_competencia_atual}")
            return None
        
        df_atual = pd.read_excel(arquivo_competencia_atual)
        total_inicial = len(df_atual)
        print(f"\n📊 MÊS ATUAL: {total_inicial} competências")
        
        # ====================================================================
        # PASSO 2: Carregar competências dos meses anteriores
        # ====================================================================
        print("\n📂 Carregando histórico...")
        df_mes_1 = self._carregar_competencias_mes(self.caminho_mes_1, "Mês -1")
        df_mes_2 = self._carregar_competencias_mes(self.caminho_mes_2, "Mês -2")
        
        # ====================================================================
        # PASSO 3: Filtrar competências do mês atual (opcional)
        # ====================================================================
        if processar_somente_fechadas:
            df_atual_filtrado = df_atual[
                (df_atual['situacao'] != 'ABERTA') & 
                (df_atual['situacao'] != 'REABERTA')
            ].copy()
            
            print(f"\n🔒 Filtro aplicado: apenas competências FECHADAS")
            print(f"   • Total no mês atual: {total_inicial}")
            print(f"   • Fechadas: {len(df_atual_filtrado)}")
            
            abertas = len(df_atual[df_atual['situacao'] == 'ABERTA'])
            reabertas = len(df_atual[df_atual['situacao'] == 'REABERTA'])
            print(f"   • Abertas (ignoradas): {abertas}")
            print(f"   • Reabertas (ignoradas): {reabertas}")
        else:
            df_atual_filtrado = df_atual.copy()
            print(f"\n🔓 Sem filtro de status - processando TODAS as competências")
        
        if df_atual_filtrado.empty:
            print("\n⚠️ Nenhuma competência para processar no mês atual!")
            return None
        
        # ====================================================================
        # PASSO 4: Se não há histórico, processa tudo
        # ====================================================================
        if df_mes_1 is None and df_mes_2 is None:
            print("\n⚠️ SEM HISTÓRICO - Primeira execução")
            print("   ➡️ Processando TODAS as competências disponíveis")
            
            nome_filtrado = "competencias_todas_unidades_filtrado.xlsx"
            caminho_filtrado = os.path.join(
                os.path.dirname(arquivo_competencia_atual), 
                nome_filtrado
            )
            df_atual_filtrado.to_excel(caminho_filtrado, index=False)
            print(f"\n💾 Arquivo salvo: {caminho_filtrado}")
            return caminho_filtrado
        
        # ====================================================================
        # PASSO 5: Criar chave única para comparação
        # ====================================================================
        print("\n🔑 Criando chaves de identificação...")
        
        df_atual_filtrado['chave'] = (
            df_atual_filtrado['nome'] + '_' + df_atual_filtrado['competencia']
        )
        
        if df_mes_1 is not None:
            df_mes_1['chave'] = df_mes_1['nome'] + '_' + df_mes_1['competencia']
        
        if df_mes_2 is not None:
            df_mes_2['chave'] = df_mes_2['nome'] + '_' + df_mes_2['competencia']
        
        # ====================================================================
        # PASSO 6: Identificar status nos meses anteriores
        # ====================================================================
        print("\n🔍 ANÁLISE DE STATUS:")
        
        # Competências FECHADAS no mês -1
        fechadas_mes_1 = set()
        reabertas_mes_1 = set()
        if df_mes_1 is not None:
            fechadas_mes_1 = set(
                df_mes_1[
                    (df_mes_1['situacao'] != 'ABERTA') & 
                    (df_mes_1['situacao'] != 'REABERTA')
                ]['chave']
            )
            reabertas_mes_1 = set(
                df_mes_1[df_mes_1['situacao'] == 'REABERTA']['chave']
            )
            print(f"   • Mês -1: {len(fechadas_mes_1)} fechadas | {len(reabertas_mes_1)} reabertas")
        
        # Competências FECHADAS e REABERTAS no mês -2
        fechadas_mes_2 = set()
        reabertas_mes_2 = set()
        if df_mes_2 is not None:
            fechadas_mes_2 = set(
                df_mes_2[
                    (df_mes_2['situacao'] != 'ABERTA') & 
                    (df_mes_2['situacao'] != 'REABERTA')
                ]['chave']
            )
            reabertas_mes_2 = set(
                df_mes_2[df_mes_2['situacao'] == 'REABERTA']['chave']
            )
            print(f"   • Mês -2: {len(fechadas_mes_2)} fechadas | {len(reabertas_mes_2)} reabertas")
        
        # ====================================================================
        # PASSO 7: Regra de exclusão
        # ====================================================================
        print("\n🧮 APLICANDO REGRA DE EXCLUSÃO:")
        print("   Remover competências que estavam FECHADAS em AMBOS os meses")
        print("   EXCETO as que foram REABERTAS no mês -1")
        
        # Competências que devem ser EXCLUÍDAS (estavam fechadas em ambos)
        excluir = set()
        
        if df_mes_1 is not None and df_mes_2 is not None:
            # Tem ambos os meses: exclui apenas se estava fechada nos 2
            excluir = fechadas_mes_1 & fechadas_mes_2
            print(f"   • Fechadas em AMBOS os meses: {len(excluir)}")
            
        elif df_mes_1 is not None:
            # Só tem mês -1: exclui se estava fechada
            excluir = fechadas_mes_1
            print(f"   • Fechadas no mês -1: {len(excluir)}")
            
        elif df_mes_2 is not None:
            # Só tem mês -2: exclui se estava fechada
            excluir = fechadas_mes_2
            print(f"   • Fechadas no mês -2: {len(excluir)}")
        
        # ====================================================================
        # PASSO 8: Detectar competências REABERTAS e depois FECHADAS
        # ====================================================================
        competencias_para_reprocessar = set()
        
        if df_mes_1 is not None and df_mes_2 is not None:
            chaves_atuais_fechadas = set(df_atual_filtrado['chave'])
            
            # CENÁRIO 1: Fechada → Reaberta → Fechada
            # Mês -2: FECHADA | Mês -1: REABERTA | Atual: FECHADA
            cenario_1 = fechadas_mes_2 & reabertas_mes_1 & chaves_atuais_fechadas
            
            # CENÁRIO 2: Reaberta → Fechada → Fechada
            # Mês -2: REABERTA | Mês -1: FECHADA | Atual: FECHADA
            cenario_2 = reabertas_mes_2 & fechadas_mes_1 & chaves_atuais_fechadas
            
            # Combina ambos os cenários
            competencias_para_reprocessar = cenario_1 | cenario_2
            
            print(f"\n🔄 COMPETÊNCIAS DETECTADAS PARA REPROCESSAMENTO:")
            print(f"   • Cenário 1 (Fechada → Reaberta → Fechada): {len(cenario_1)}")
            print(f"   • Cenário 2 (Reaberta → Fechada → Fechada): {len(cenario_2)}")
            print(f"   • TOTAL: {len(competencias_para_reprocessar)}")
            
            # Remove as que precisam ser reprocessadas do conjunto de exclusão
            excluir = excluir - competencias_para_reprocessar
            
            if competencias_para_reprocessar:
                print("\n   📋 Exemplos de competências que serão REPROCESSADAS:")
                
                # Mostra exemplos do cenário 1
                if cenario_1:
                    print("\n   🔹 Cenário 1 (Fechada → Reaberta → Fechada):")
                    for i, chave in enumerate(list(cenario_1)[:3], 1):
                        partes = chave.split('_')
                        unidade = '_'.join(partes[:-1])
                        competencia = partes[-1]
                        print(f"      {i}. {unidade} - Competência: {competencia}")
                        
                        status_mes_2 = df_mes_2[df_mes_2['chave'] == chave]['situacao'].values
                        status_mes_2 = status_mes_2[0] if len(status_mes_2) > 0 else 'N/A'
                        
                        status_mes_1 = df_mes_1[df_mes_1['chave'] == chave]['situacao'].values
                        status_mes_1 = status_mes_1[0] if len(status_mes_1) > 0 else 'N/A'
                        
                        status_atual = df_atual_filtrado[df_atual_filtrado['chave'] == chave]['situacao'].values[0]
                        
                        print(f"         Mês -2: {status_mes_2} | Mês -1: {status_mes_1} | Atual: {status_atual}")
                
                # Mostra exemplos do cenário 2
                if cenario_2:
                    print("\n   🔹 Cenário 2 (Reaberta → Fechada → Fechada):")
                    for i, chave in enumerate(list(cenario_2)[:3], 1):
                        partes = chave.split('_')
                        unidade = '_'.join(partes[:-1])
                        competencia = partes[-1]
                        print(f"      {i}. {unidade} - Competência: {competencia}")
                        
                        status_mes_2 = df_mes_2[df_mes_2['chave'] == chave]['situacao'].values
                        status_mes_2 = status_mes_2[0] if len(status_mes_2) > 0 else 'N/A'
                        
                        status_mes_1 = df_mes_1[df_mes_1['chave'] == chave]['situacao'].values
                        status_mes_1 = status_mes_1[0] if len(status_mes_1) > 0 else 'N/A'
                        
                        status_atual = df_atual_filtrado[df_atual_filtrado['chave'] == chave]['situacao'].values[0]
                        
                        print(f"         Mês -2: {status_mes_2} | Mês -1: {status_mes_1} | Atual: {status_atual}")
        
        # ====================================================================
        # PASSO 9: Aplicar filtro
        # ====================================================================
        print(f"\n📊 RESUMO:")
        print(f"   • Total no mês atual (filtrado): {len(df_atual_filtrado)}")
        print(f"   • A excluir (já processadas): {len(excluir)}")
        
        df_final = df_atual_filtrado[
            ~df_atual_filtrado['chave'].isin(excluir)
        ].copy()
        
        df_final = df_final.drop(columns=['chave'])
        
        total_final = len(df_final)
        removidos = len(df_atual_filtrado) - total_final
        
        print(f"   • Removidas: {removidos}")
        print(f"   • ✅ RESTANTES PARA PROCESSAR: {total_final}")
        
        # ====================================================================
        # PASSO 10: Resultado
        # ====================================================================
        if df_final.empty:
            print("\n⚠️ NENHUMA COMPETÊNCIA NOVA PARA PROCESSAR!")
            print("   📋 Ação: Copiar arquivos do mês anterior")
            return None
        
        # Salva arquivo filtrado
        nome_filtrado = "competencias_todas_unidades_filtrado.xlsx"
        caminho_filtrado = os.path.join(
            os.path.dirname(arquivo_competencia_atual), 
            nome_filtrado
        )
        
        df_final.to_excel(caminho_filtrado, index=False)
        print(f"\n✅ Arquivo filtrado salvo: {caminho_filtrado}")
        
        return caminho_filtrado
    
    def copiar_arquivos_mes_anterior(self, nomes_arquivos_apis):
        """
        Copia arquivos das APIs do mês -1 para o mês atual
        """
        print("\n" + "="*60)
        print("📂 COPIANDO ARQUIVOS DO MÊS ANTERIOR")
        print("="*60)
        
        if not self.caminho_mes_1:
            print("❌ Não há mês -1 disponível")
            return {}
        
        resultados = {}
        
        for nome_arquivo in nomes_arquivos_apis:
            arquivo_origem = os.path.join(self.caminho_mes_1, nome_arquivo)
            arquivo_destino = os.path.join(self.caminho_atual, nome_arquivo)
            
            try:
                if not os.path.exists(arquivo_origem):
                    print(f"⚠️ {nome_arquivo} - não encontrado no mês -1")
                    resultados[nome_arquivo] = False
                    continue
                
                shutil.copy2(arquivo_origem, arquivo_destino)
                print(f"✅ {nome_arquivo} - copiado com sucesso")
                resultados[nome_arquivo] = True
                
            except Exception as e:
                print(f"❌ {nome_arquivo} - erro ao copiar: {e}")
                resultados[nome_arquivo] = False
        
        sucessos = sum(1 for v in resultados.values() if v)
        total = len(resultados)
        
        print(f"\n📊 Resumo: {sucessos}/{total} arquivos copiados com sucesso")
        
        return resultados
    
    def consolidar_dados_api(self, nome_arquivo_api):
        """
        Consolida dados de uma API: novos (mês atual) + histórico (mês -1)
        Busca arquivos que começam com o nome base da API (CSV ou XLSX)
        """
        # Extrai nome base e extensão
        nome_base, extensao = os.path.splitext(nome_arquivo_api)
        extensao = extensao.lower()  # .csv ou .xlsx
        
        print(f"\n🔄 Consolidando: {nome_base}")
        
        # ================================================================
        # PASSO 1: Buscar arquivo no mês atual
        # ================================================================
        padrao_novo = os.path.join(self.caminho_atual, f"{nome_base}*{extensao}")
        arquivos_novos = glob.glob(padrao_novo)
        
        if not arquivos_novos:
            print(f"   ⚠️ Nenhum arquivo encontrado para: {nome_base}{extensao}")
            print(f"   📁 Padrão de busca: {padrao_novo}")
            return False
        
        arquivo_novo = arquivos_novos[0]
        print(f"   📄 Arquivo novo: {os.path.basename(arquivo_novo)}")
        
        # ================================================================
        # PASSO 2: Verificar se há mês -1
        # ================================================================
        if not self.caminho_mes_1:
            print("   ✅ Sem consolidação necessária (sem mês -1)")
            return True
        
        # ================================================================
        # PASSO 3: Buscar arquivo no mês -1
        # ================================================================
        padrao_antigo = os.path.join(self.caminho_mes_1, f"{nome_base}*{extensao}")
        arquivos_antigos = glob.glob(padrao_antigo)
        
        if not arquivos_antigos:
            print(f"   ℹ️ Arquivo não encontrado no mês -1 - mantendo apenas dados novos")
            return True
        
        arquivo_antigo = arquivos_antigos[0]
        print(f"   📄 Arquivo mês -1: {os.path.basename(arquivo_antigo)}")
        
        # ================================================================
        # PASSO 4: Carregar dados (CSV ou XLSX)
        # ================================================================
        try:
            if extensao == '.csv':
                df_novo = pd.read_csv(arquivo_novo)
                df_antigo = pd.read_csv(arquivo_antigo)
            else:  # .xlsx
                df_novo = pd.read_excel(arquivo_novo)
                df_antigo = pd.read_excel(arquivo_antigo)
            
            print(f"   📊 Registros novos: {len(df_novo):,}")
            print(f"   📊 Registros antigos: {len(df_antigo):,}")
            
            # ================================================================
            # PASSO 5: Consolidar
            # ================================================================
            df_consolidado = pd.concat([df_antigo, df_novo], ignore_index=True)
            
            tamanho_antes = len(df_consolidado)
            df_consolidado = df_consolidado.drop_duplicates()
            duplicatas = tamanho_antes - len(df_consolidado)
            
            if duplicatas > 0:
                print(f"   🗑️ Duplicatas removidas: {duplicatas}")
            
            print(f"   ✅ Total consolidado: {len(df_consolidado):,}")
            
            # ================================================================
            # PASSO 6: Salvar no formato original
            # ================================================================
            if extensao == '.csv':
                df_consolidado.to_csv(arquivo_novo, index=False)
            else:
                df_consolidado.to_excel(arquivo_novo, index=False)
            
            print(f"   💾 Arquivo consolidado salvo")
            
            return True
            
        except Exception as e:
            print(f"   ❌ Erro ao consolidar: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def consolidar_todas_apis(self, nomes_arquivos_apis):
        """
        Consolida dados de múltiplas APIs
        """
        print("\n" + "="*60)
        print("📦 CONSOLIDANDO DADOS DAS APIs")
        print("="*60)
        
        resultados = {}
        
        for nome_arquivo in nomes_arquivos_apis:
            sucesso = self.consolidar_dados_api(nome_arquivo)
            resultados[nome_arquivo] = sucesso
        
        print("\n" + "="*60)
        print("📋 RESUMO DA CONSOLIDAÇÃO")
        print("="*60)
        
        sucessos = sum(1 for v in resultados.values() if v)
        total = len(resultados)
        
        for arquivo, sucesso in resultados.items():
            status = "✅" if sucesso else "❌"
            print(f"{status} {arquivo}")
        
        print(f"\n📊 Total: {sucessos}/{total} consolidações bem-sucedidas")
        
        return resultados


def processar_incremental(caminho_atual, arquivo_competencia_atual, nomes_arquivos_apis,
                         processar_somente_fechadas=True):
    """
    Função principal para processamento incremental com análise de 2 meses
    
    Args:
        caminho_atual: Diretório do mês vigente
        arquivo_competencia_atual: Caminho do arquivo de competências
        nomes_arquivos_apis: Lista de nomes dos arquivos das APIs
        processar_somente_fechadas: Se True, processa apenas competências fechadas
        
    Returns:
        tuple: (arquivo_competencia_filtrado, resultados, modo_operacao)
    """
    print("\n" + "="*60)
    print("🚀 INICIANDO PROCESSAMENTO INCREMENTAL")
    print("="*60)
    
    analisador = AnalisadorIncremental(caminho_atual)
    
    arquivo_filtrado = analisador.filtrar_competencias_nao_processadas(
        arquivo_competencia_atual,
        processar_somente_fechadas=processar_somente_fechadas
    )
    
    if arquivo_filtrado is None:
        print("\n" + "="*60)
        print("📋 MODO: CÓPIA (sem novas competências)")
        print("="*60)
        
        resultados_copia = analisador.copiar_arquivos_mes_anterior(nomes_arquivos_apis)
        
        print("\n" + "="*60)
        print("✅ PROCESSAMENTO INCREMENTAL CONCLUÍDO (MODO CÓPIA)")
        print("="*60 + "\n")
        
        return None, resultados_copia, 'copiar'
    
    print("\n" + "="*60)
    print("📋 MODO: PROCESSAMENTO (há novas competências)")
    print("="*60)
    print("\n⚠️ Consolidação será executada após extração dos dados novos")
    
    print("\n" + "="*60)
    print("✅ ANÁLISE INCREMENTAL CONCLUÍDA (MODO PROCESSAMENTO)")
    print("="*60 + "\n")
    
    return arquivo_filtrado, {}, 'processar'


def consolidar_apos_extracao(caminho_atual, nomes_arquivos_apis):
    """
    Consolida dados das APIs após a extração
    """
    print("\n" + "="*60)
    print("📦 CONSOLIDANDO DADOS (NOVOS + MÊS ANTERIOR)")
    print("="*60)
    
    analisador = AnalisadorIncremental(caminho_atual)
    resultados = analisador.consolidar_todas_apis(nomes_arquivos_apis)
    
    print("\n" + "="*60)
    print("✅ CONSOLIDAÇÃO CONCLUÍDA")
    print("="*60 + "\n")
    
    return resultados

"""
Módulo para consolidação incremental de dados
Consolida dados novos (mês atual) com histórico (apenas mês -1)
"""
import glob
import os
import pandas as pd
import shutil
from dotenv import load_dotenv


class ConsolidadorDados:
    """Gerencia a consolidação de dados entre mês atual e mês -1"""
    
    def __init__(self, caminho_atual):
        """
        Inicializa o consolidador
        
        Args:
            caminho_atual: Caminho do diretório do mês vigente
        """
        self.caminho_atual = caminho_atual
        self.caminho_mes_1 = None
        self._obter_caminho_mes_anterior()
    
    def _obter_caminho_mes_anterior(self):
        """Identifica o diretório do mês anterior"""
        load_dotenv()
        caminho_fixo = os.getenv("caminho_fixo")
        
        if not caminho_fixo:
            print("❌ Variável 'caminho_fixo' não encontrada no .env")
            return
        
        # Extrai informações do caminho atual
        partes = self.caminho_atual.split(os.sep)
        pasta_competencia = partes[-1]  # Ex: "11_2024"
        
        try:
            mes_atual, ano_atual = pasta_competencia.split('_')
            mes_atual = int(mes_atual)
            ano_atual = int(ano_atual)
        except ValueError:
            print(f"❌ Formato inválido da pasta: {pasta_competencia}")
            return
        
        # Calcula mês -1
        if mes_atual == 1:
            mes_1 = 12
            ano_1 = ano_atual - 1
        else:
            mes_1 = mes_atual - 1
            ano_1 = ano_atual
        
        pasta_mes_1 = f"{mes_1:02d}_{ano_1}"
        self.caminho_mes_1 = os.path.join(caminho_fixo, str(ano_1), pasta_mes_1)
        
        if os.path.exists(self.caminho_mes_1):
            print(f"✅ Mês -1 encontrado: {pasta_mes_1}")
        else:
            print(f"⚠️ Mês -1 NÃO encontrado: {pasta_mes_1}")
            self.caminho_mes_1 = None
    
    def encontrar_arquivo(self, caminho, nome_base):
        """
        Busca arquivo que comece com o nome base
        
        Args:
            caminho: Diretório onde buscar
            nome_base: Nome base do arquivo (sem extensão)
            
        Returns:
            Caminho completo do arquivo ou None
        """
        padrao = os.path.join(caminho, f"{nome_base}*.xlsx")
        arquivos = glob.glob(padrao)
        
        if arquivos:
            return arquivos[0]  # Retorna o primeiro encontrado
        return None
    
    def consolidar_api(self, nome_arquivo_api):
        """
        Consolida dados de uma API: novos (mês atual) + histórico (mês -1)
        
        Args:
            nome_arquivo_api: Nome base do arquivo (ex: 'api_estatistica.xlsx')
            
        Returns:
            bool: True se consolidou com sucesso
        """
        nome_base = nome_arquivo_api.replace('.xlsx', '')
        print(f"\n🔄 Consolidando: {nome_base}")
        
        # ================================================================
        # PASSO 1: Buscar arquivo novo (mês atual)
        # ================================================================
        arquivo_novo = self.encontrar_arquivo(self.caminho_atual, nome_base)
        
        if not arquivo_novo:
            print(f"   ⚠️ Arquivo não encontrado no mês atual")
            return False
        
        print(f"   📄 Novo: {os.path.basename(arquivo_novo)}")
        
        # ================================================================
        # PASSO 2: Verificar se há mês -1
        # ================================================================
        if not self.caminho_mes_1:
            print(f"   ℹ️ Sem mês -1 - mantendo apenas dados novos")
            return True
        
        # ================================================================
        # PASSO 3: Buscar arquivo antigo (mês -1)
        # ================================================================
        arquivo_antigo = self.encontrar_arquivo(self.caminho_mes_1, nome_base)
        
        if not arquivo_antigo:
            print(f"   ℹ️ Arquivo não encontrado no mês -1 - mantendo apenas dados novos")
            return True
        
        print(f"   📄 Antigo: {os.path.basename(arquivo_antigo)}")
        
        # ================================================================
        # PASSO 4: Carregar e consolidar
        # ================================================================
        try:
            df_novo = pd.read_excel(arquivo_novo)
            df_antigo = pd.read_excel(arquivo_antigo)
            
            print(f"   📊 Registros novos: {len(df_novo):,}")
            print(f"   📊 Registros antigos: {len(df_antigo):,}")
            
            # Concatena: antigo primeiro, depois novo
            df_consolidado = pd.concat([df_antigo, df_novo], ignore_index=True)
            
            # Remove duplicatas
            tamanho_antes = len(df_consolidado)
            df_consolidado = df_consolidado.drop_duplicates()
            duplicatas = tamanho_antes - len(df_consolidado)
            
            if duplicatas > 0:
                print(f"   🗑️ Duplicatas removidas: {duplicatas}")
            
            print(f"   ✅ Total consolidado: {len(df_consolidado):,}")
            
            # Salva consolidado no arquivo novo
            df_consolidado.to_excel(arquivo_novo, index=False)
            print(f"   💾 Arquivo salvo: {os.path.basename(arquivo_novo)}")
            
            return True
            
        except Exception as e:
            print(f"   ❌ Erro ao consolidar: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def consolidar_multiplas_apis(self, nomes_arquivos_apis):
        """
        Consolida dados de múltiplas APIs
        
        Args:
            nomes_arquivos_apis: Lista de nomes base dos arquivos
            
        Returns:
            dict: Resultados da consolidação
        """
        print("\n" + "="*70)
        print("📦 CONSOLIDANDO DADOS (NOVOS + MÊS ANTERIOR)")
        print("="*70)
        
        resultados = {}
        
        for nome_arquivo in nomes_arquivos_apis:
            sucesso = self.consolidar_api(nome_arquivo)
            resultados[nome_arquivo] = sucesso
        
        # Resumo
        print("\n" + "="*70)
        print("📋 RESUMO DA CONSOLIDAÇÃO")
        print("="*70)
        
        sucessos = sum(1 for v in resultados.values() if v)
        total = len(resultados)
        
        for arquivo, sucesso in resultados.items():
            status = "✅" if sucesso else "❌"
            nome_base = arquivo.replace('.xlsx', '')
            print(f"{status} {nome_base}")
        
        print(f"\n📊 Total: {sucessos}/{total} consolidações bem-sucedidas")
        print("="*70 + "\n")
        
        return resultados
    
    def copiar_arquivo_mes_anterior(self, nome_arquivo_api):
        """
        Copia arquivo do mês -1 para o mês atual (modo cópia)
        
        Args:
            nome_arquivo_api: Nome base do arquivo
            
        Returns:
            bool: True se copiou com sucesso
        """
        if not self.caminho_mes_1:
            return False
        
        nome_base = nome_arquivo_api.replace('.xlsx', '')
        
        # Busca no mês -1
        arquivo_origem = self.encontrar_arquivo(self.caminho_mes_1, nome_base)
        
        if not arquivo_origem:
            print(f"⚠️ {nome_base} - não encontrado no mês -1")
            return False
        
        # Destino mantém o mesmo nome
        nome_arquivo_real = os.path.basename(arquivo_origem)
        arquivo_destino = os.path.join(self.caminho_atual, nome_arquivo_real)
        
        try:
            shutil.copy2(arquivo_origem, arquivo_destino)
            print(f"✅ {nome_arquivo_real} - copiado")
            return True
        except Exception as e:
            print(f"❌ {nome_arquivo_real} - erro: {e}")
            return False
    
    def copiar_multiplos_arquivos(self, nomes_arquivos_apis):
        """
        Copia múltiplos arquivos do mês -1
        
        Args:
            nomes_arquivos_apis: Lista de nomes base
            
        Returns:
            dict: Resultados das cópias
        """
        print("\n" + "="*60)
        print("📂 COPIANDO ARQUIVOS DO MÊS ANTERIOR")
        print("="*60)
        
        if not self.caminho_mes_1:
            print("❌ Não há mês -1 disponível")
            return {}
        
        resultados = {}
        
        for nome_arquivo in nomes_arquivos_apis:
            sucesso = self.copiar_arquivo_mes_anterior(nome_arquivo)
            resultados[nome_arquivo] = sucesso
        
        sucessos = sum(1 for v in resultados.values() if v)
        total = len(resultados)
        
        print(f"\n📊 Resumo: {sucessos}/{total} arquivos copiados")
        
        return resultados