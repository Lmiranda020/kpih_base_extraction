"""
Módulo para análise incremental de competências com consolidação robusta
Versão corrigida - Dezembro 2024
"""
import os
import pandas as pd
import shutil
import glob
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv


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
        
        Args:
            arquivo_competencia_atual: Caminho do arquivo de competências do mês vigente
            processar_somente_fechadas: Se True, processa apenas competências fechadas
            
        Returns:
            str: Caminho do arquivo filtrado ou None
        """
        print("\n" + "="*70)
        print("🔍 ANÁLISE INCREMENTAL COM 2 MESES ANTERIORES")
        print("="*70)
        
        # Carregar competências do mês atual
        if not os.path.exists(arquivo_competencia_atual):
            print(f"❌ Arquivo não encontrado: {arquivo_competencia_atual}")
            return None
        
        df_atual = pd.read_excel(arquivo_competencia_atual)
        total_inicial = len(df_atual)
        print(f"\n📊 MÊS ATUAL: {total_inicial} competências")
        
        # Carregar competências dos meses anteriores
        print("\n📂 Carregando histórico...")
        df_mes_1 = self._carregar_competencias_mes(self.caminho_mes_1, "Mês -1")
        df_mes_2 = self._carregar_competencias_mes(self.caminho_mes_2, "Mês -2")
        
        # Filtrar competências do mês atual
        if processar_somente_fechadas:
            df_atual_filtrado = df_atual[
                (df_atual['situacao'] != 'ABERTA') & 
                (df_atual['situacao'] != 'REABERTA')
            ].copy()
            
            print(f"\n🔒 Filtro aplicado: apenas competências FECHADAS")
            print(f"   • Total no mês atual: {total_inicial}")
            print(f"   • Fechadas: {len(df_atual_filtrado)}")
        else:
            df_atual_filtrado = df_atual.copy()
            print(f"\n🔓 Sem filtro de status - processando TODAS as competências")
        
        if df_atual_filtrado.empty:
            print("\n⚠️ Nenhuma competência para processar no mês atual!")
            return None
        
        # Se não há histórico, processa tudo
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
        
        # Criar chave única para comparação
        print("\n🔑 Criando chaves de identificação...")
        
        df_atual_filtrado['chave'] = (
            df_atual_filtrado['nome'] + '_' + df_atual_filtrado['competencia']
        )
        
        if df_mes_1 is not None:
            df_mes_1['chave'] = df_mes_1['nome'] + '_' + df_mes_1['competencia']
        
        if df_mes_2 is not None:
            df_mes_2['chave'] = df_mes_2['nome'] + '_' + df_mes_2['competencia']
        
        # Identificar status nos meses anteriores
        print("\n🔍 ANÁLISE DE STATUS:")
        
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
        
        # Regra de exclusão
        print("\n🧮 APLICANDO REGRA DE EXCLUSÃO:")
        
        excluir = set()
        
        if df_mes_1 is not None and df_mes_2 is not None:
            excluir = fechadas_mes_1 & fechadas_mes_2
            print(f"   • Fechadas em AMBOS os meses: {len(excluir)}")
        elif df_mes_1 is not None:
            excluir = fechadas_mes_1
            print(f"   • Fechadas no mês -1: {len(excluir)}")
        elif df_mes_2 is not None:
            excluir = fechadas_mes_2
            print(f"   • Fechadas no mês -2: {len(excluir)}")
        
        # Detectar competências REABERTAS
        competencias_para_reprocessar = set()
        
        if df_mes_1 is not None and df_mes_2 is not None:
            chaves_atuais_fechadas = set(df_atual_filtrado['chave'])
            
            cenario_1 = fechadas_mes_2 & reabertas_mes_1 & chaves_atuais_fechadas
            cenario_2 = reabertas_mes_2 & fechadas_mes_1 & chaves_atuais_fechadas
            
            competencias_para_reprocessar = cenario_1 | cenario_2
            
            print(f"\n🔄 COMPETÊNCIAS DETECTADAS PARA REPROCESSAMENTO:")
            print(f"   • Cenário 1 (Fechada → Reaberta → Fechada): {len(cenario_1)}")
            print(f"   • Cenário 2 (Reaberta → Fechada → Fechada): {len(cenario_2)}")
            print(f"   • TOTAL: {len(competencias_para_reprocessar)}")
            
            excluir = excluir - competencias_para_reprocessar
        
        # Aplicar filtro
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
        
        if df_final.empty:
            print("\n⚠️ NENHUMA COMPETÊNCIA NOVA PARA PROCESSAR!")
            return None
        
        nome_filtrado = "competencias_todas_unidades_filtrado.xlsx"
        caminho_filtrado = os.path.join(
            os.path.dirname(arquivo_competencia_atual), 
            nome_filtrado
        )
        
        df_final.to_excel(caminho_filtrado, index=False)
        print(f"\n✅ Arquivo filtrado salvo: {caminho_filtrado}")
        
        return caminho_filtrado
    
    def _buscar_arquivo_api(self, diretorio, nome_base):
        """
        Busca arquivo de uma API (CSV ou XLSX) de forma robusta
        
        Args:
            diretorio: Diretório onde buscar
            nome_base: Nome base do arquivo (ex: 'api_estatistica')
            
        Returns:
            tuple: (caminho_completo, extensao) ou (None, None)
        """
        # Tenta encontrar arquivo com qualquer extensão
        for extensao in ['.csv', '.xlsx']:
            # Padrão: api_estatistica*.csv ou api_estatistica*.xlsx
            padrao = os.path.join(diretorio, f"{nome_base}*{extensao}")
            arquivos = glob.glob(padrao)
            
            if arquivos:
                # Retorna o primeiro encontrado
                return arquivos[0], extensao
        
        return None, None
    
    def _carregar_arquivo_api(self, caminho_arquivo, extensao):
        """
        Carrega arquivo CSV ou XLSX de forma robusta
        
        Args:
            caminho_arquivo: Caminho completo do arquivo
            extensao: '.csv' ou '.xlsx'
            
        Returns:
            DataFrame ou None
        """
        try:
            if extensao == '.csv':
                # Importa função robusta de leitura CSV
                from modules.csv_reader import ler_csv_robusto
                return ler_csv_robusto(caminho_arquivo, sep=';', encoding='utf-8-sig')
            else:  # .xlsx
                return pd.read_excel(caminho_arquivo)
        except Exception as e:
            print(f"   ❌ Erro ao carregar arquivo: {e}")
            return None
    
    def consolidar_dados_api(self, nome_arquivo_api):
        """
        Consolida dados de uma API: novos (mês atual) + histórico (mês -1)
        VERSÃO CORRIGIDA - Busca robusta e ordem correta
        
        Args:
            nome_arquivo_api: Nome base do arquivo (ex: 'api_estatistica.csv')
            
        Returns:
            bool: True se consolidou com sucesso
        """
        # Remove extensão para busca
        nome_base = nome_arquivo_api.replace('.csv', '').replace('.xlsx', '')
        
        print(f"\n🔄 Consolidando: {nome_base}")
        
        # ================================================================
        # PASSO 1: Buscar arquivo NOVO (mês atual)
        # ================================================================
        arquivo_novo, extensao_novo = self._buscar_arquivo_api(self.caminho_atual, nome_base)
        
        if not arquivo_novo:
            print(f"   ⚠️ Arquivo não encontrado no mês atual")
            return False
        
        print(f"   📄 Novo: {os.path.basename(arquivo_novo)}")
        
        # ================================================================
        # PASSO 2: Carregar arquivo NOVO
        # ================================================================
        df_novo = self._carregar_arquivo_api(arquivo_novo, extensao_novo)
        
        if df_novo is None:
            print(f"   ❌ Falha ao carregar arquivo novo")
            return False
        
        print(f"   📊 Registros novos: {len(df_novo):,}")
        
        # ================================================================
        # PASSO 3: Verificar se há mês -1
        # ================================================================
        if not self.caminho_mes_1:
            print(f"   ℹ️ Sem mês -1 - mantendo apenas dados novos")
            return True
        
        # ================================================================
        # PASSO 4: Buscar arquivo ANTIGO (mês -1)
        # ================================================================
        arquivo_antigo, extensao_antigo = self._buscar_arquivo_api(self.caminho_mes_1, nome_base)
        
        if not arquivo_antigo:
            print(f"   ℹ️ Arquivo não encontrado no mês -1 - mantendo apenas dados novos")
            return True
        
        print(f"   📄 Antigo: {os.path.basename(arquivo_antigo)}")
        
        # ================================================================
        # PASSO 5: Carregar arquivo ANTIGO
        # ================================================================
        df_antigo = self._carregar_arquivo_api(arquivo_antigo, extensao_antigo)
        
        if df_antigo is None:
            print(f"   ⚠️ Falha ao carregar arquivo antigo - mantendo apenas dados novos")
            return True
        
        print(f"   📊 Registros antigos: {len(df_antigo):,}")
        
        # ================================================================
        # PASSO 6: CONSOLIDAR - ORDEM CORRETA!
        # ================================================================
        try:
            # IMPORTANTE: Novo primeiro, antigo depois
            # Ao remover duplicatas com keep='first', mantém os NOVOS
            df_consolidado = pd.concat([df_novo, df_antigo], ignore_index=True)
            
            tamanho_antes = len(df_consolidado)
            df_consolidado = df_consolidado.drop_duplicates(keep='first')
            duplicatas = tamanho_antes - len(df_consolidado)
            
            if duplicatas > 0:
                print(f"   🗑️ Duplicatas removidas: {duplicatas}")
             
            print(f"   ✅ Total consolidado: {len(df_consolidado):,}")
            
            # ================================================================
            # PASSO 7: Salvar no formato ORIGINAL
            # ================================================================
            if extensao_novo == '.csv':
                df_consolidado.to_csv(arquivo_novo, index=False, sep=';', encoding='utf-8-sig')
            else:
                df_consolidado.to_excel(arquivo_novo, index=False)
            
            print(f"   💾 Arquivo consolidado salvo: {os.path.basename(arquivo_novo)}")
            
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
            nome_base = arquivo.replace('.csv', '').replace('.xlsx', '')
            print(f"{status} {nome_base}")
        
        print(f"\n📊 Total: {sucessos}/{total} consolidações bem-sucedidas")
        
        return resultados
    
    def consolidar_dados_api_inteligente(self, nome_arquivo_api):
        """
        Consolida dados mantendo APENAS os dados MAIS RECENTES
        Remove duplicatas mantendo dados NOVOS, descarta ANTIGOS
        """
        nome_base = nome_arquivo_api.replace('.csv', '').replace('.xlsx', '')
        
        print(f"\n🔄 Consolidando: {nome_base}")
        
        # Buscar arquivo NOVO (mês atual)
        arquivo_novo, extensao_novo = self._buscar_arquivo_api(self.caminho_atual, nome_base)
        
        if not arquivo_novo:
            print(f"   ⚠️ Arquivo não encontrado no mês atual")
            return False
        
        print(f"   📄 Novo: {os.path.basename(arquivo_novo)}")
        
        # Carregar arquivo NOVO
        df_novo = self._carregar_arquivo_api(arquivo_novo, extensao_novo)
        
        if df_novo is None:
            print(f"   ❌ Falha ao carregar arquivo novo")
            return False
        
        registros_novos = len(df_novo)
        print(f"   📊 Registros novos: {registros_novos:,}")
        
        # Verificar se há mês -1
        if not self.caminho_mes_1:
            print(f"   ℹ️ Sem mês -1 - mantendo apenas dados novos")
            return True
        
        # Buscar arquivo ANTIGO (mês -1)
        arquivo_antigo, extensao_antigo = self._buscar_arquivo_api(self.caminho_mes_1, nome_base)
        
        if not arquivo_antigo:
            print(f"   ℹ️ Arquivo não encontrado no mês -1 - mantendo apenas dados novos")
            return True
        
        print(f"   📄 Antigo: {os.path.basename(arquivo_antigo)}")
        
        # Carregar arquivo ANTIGO
        df_antigo = self._carregar_arquivo_api(arquivo_antigo, extensao_antigo)
        
        if df_antigo is None:
            print(f"   ⚠️ Falha ao carregar arquivo antigo - mantendo apenas dados novos")
            return True
        
        registros_antigos = len(df_antigo)
        print(f"   📊 Registros antigos: {registros_antigos:,}")
        
        # Identificar colunas-chave
        colunas_chave = self._identificar_colunas_chave(df_novo, nome_base)
        
        if not colunas_chave:
            print(f"   ⚠️ Não foi possível identificar colunas-chave")
            df_consolidado = pd.concat([df_antigo, df_novo], ignore_index=True)
        else:
            print(f"   🔑 Colunas-chave: {', '.join(colunas_chave[:3])}{'...' if len(colunas_chave) > 3 else ''}")
            
            # CRÍTICO: NOVO primeiro, ANTIGO depois
            # drop_duplicates(keep='first') mantém dados NOVOS
            df_consolidado = pd.concat([df_novo, df_antigo], ignore_index=True)
            
            tamanho_antes = len(df_consolidado)
            df_consolidado = df_consolidado.drop_duplicates(subset=colunas_chave, keep='first')
            
            duplicatas_removidas = tamanho_antes - len(df_consolidado)
            
            if duplicatas_removidas > 0:
                print(f"   🗑️ Duplicatas removidas: {duplicatas_removidas:,}")
                print(f"      (Mantidos dados NOVOS, removidos ANTIGOS)")
        
        registros_finais = len(df_consolidado)
        print(f"   ✅ Total consolidado: {registros_finais:,}")
        
        # Salvar
        try:
            if extensao_novo == '.csv':
                df_consolidado.to_csv(arquivo_novo, index=False, sep=';', encoding='utf-8-sig')
            else:
                df_consolidado.to_excel(arquivo_novo, index=False)
            
            print(f"   💾 Arquivo consolidado salvo")
            return True
            
        except Exception as e:
            print(f"   ❌ Erro ao salvar: {e}")
            return False

    def _identificar_colunas_chave(self, df, nome_api):
        """Identifica colunas-chave para remoção de duplicatas"""
        colunas = df.columns.tolist()
        colunas_lower = [c.lower() for c in colunas]
        
        # Mapeamento específico por API
        chaves_especificas = {
            'custosindividualizadoporcentro': ['centroDeCustoDescr', 'competenciaDescr', 'contaDescr', 'grupoContaDescr', 'tipoDescr', 'classificacaoDescr', 'unidade' ],
            'folhadepagamento': ['contaDeCustoDescr', 'centroDeCustoDescr', 'competenciaDescr', 'nomeFuncionario', 'unidade'],
            'notasfiscais': ['contaDeCustoDescr', 'centroDeCustoDescr', 'competenciaDescr', 'numero', 'fornecedor', 'unidade'],
            'quantidadecirurgia': ['centroDeCustoDescr', 'competenciaDescr', 'unidade'],
            'quantidadeleito': ['centroDeCustoDescr', 'competenciaDescr', 'unidade'],
            'consumo': ['contaDeCustoDescr', 'centroDeCustoDescr', 'competenciaDescr', 'itemDeEstoque', 'codigoTUSS' ,'unidade'],
            'benchmarkcomposicaodecustos': ['tipoCentroCusto', 'unidade', 'competencia'],
            'demonstracaocustounitariodosservicosauxiliares': ['competenciaDescr', 'grupo', 'descricao', 'unidade'],
            'custounitarioporponderacao': ['competenciaDescr', 'centroDeCustoDescr', 'criterioDeRateioDescr', 'ponderacaoDeRateioDescr', 'unidade'],
            'composicaoevolucaodereceita': ['tipo', 'grupoDaContaDescr', 'contaDescr', 'competenciaDescr', 'unidade'],
            'analisedepartamental': ['grupoContaDeCustoDescr', 'centroDeCustoDescr', 'competenciaDescr', 'unidade'],
            'custoporespecialidade': ['especialidadeDescr', 'centroCustoDestinoDescr', 'centroCustoOrigenDescr', 'unidadeProducaoDescr', 'competenciaDescr', 'unidade'],
            'painelcomparativodecustos': ['unidadeDeProducaoId', 'unidadeDeProducaoDescr', 'competencia'],
            'evolucaodecustos': ['grupoDaContaDescr', 'contaDeCustoDescr', 'competenciaDescr', 'tipoContaDeCustoDescr', 'classificacaoDoCustoDescr', 'unidade'], 
            'rankingdecusto': ['grupoDoCentroDescr', 'centroDeCustoDescr', 'competenciaDescr', 'unidade'],
            'estatistica': ['grupoDoCentroDescr','centroDeCustoDescr', 'competenciaDescr', 'criterioDeRateioDescr', 'unidade'],
            'composicaodecustos': ['grupoDaContaDescr', 'contaDeCustoDescr', 'tipoContaDeCustoDescr', 'competenciaDescr', 'tipo_composicao', 'unidade'],
            'demonstracaocustounitarioporsaida': ['especialidadeDescr', 'competenciaDescr', 'unidade'],
            'demonstracaocustounitario': ['centroDeCustoDescr', 'competenciaDescr', 'unidade'],
            'producoes': ['centroDeCustoDescr', 'competenciaDescr', 'unidadeDeProducaoDescr', 'unidade'],
        }
        
        # Tenta identificar a API
        for api_key, chaves in chaves_especificas.items():
            if api_key in nome_api.lower():
                chaves_encontradas = []
                for chave in chaves:
                    if chave.lower() in colunas_lower:
                        idx = colunas_lower.index(chave.lower())
                        chaves_encontradas.append(colunas[idx])
                
                if chaves_encontradas:
                    return chaves_encontradas
        
        # Fallback: busca 'competencia' + 'nome'
        chaves_encontradas = []
        for chave_comum in ['unidade', 'competencia', 'unidade']:
            if chave_comum.lower() in colunas_lower:
                idx = colunas_lower.index(chave_comum.lower())
                chaves_encontradas.append(colunas[idx])
        
        return chaves_encontradas if len(chaves_encontradas) >= 2 else []

    def consolidar_todas_apis_inteligente(self, nomes_arquivos_apis):
        """Consolida múltiplas APIs mantendo dados MAIS RECENTES"""
        print("\n" + "="*60)
        print("📦 CONSOLIDANDO DADOS DAS APIs")
        print("🧠 Modo: INTELIGENTE (mantém novos, remove antigos)")
        print("="*60)
        
        resultados = {}
        
        for nome_arquivo in nomes_arquivos_apis:
            sucesso = self.consolidar_dados_api_inteligente(nome_arquivo)
            resultados[nome_arquivo] = sucesso
        
        print("\n" + "="*60)
        print("📋 RESUMO DA CONSOLIDAÇÃO")
        print("="*60)
        
        sucessos = sum(1 for v in resultados.values() if v)
        total = len(resultados)
        
        for arquivo, sucesso in resultados.items():
            status = "✅" if sucesso else "❌"
            nome_base = arquivo.replace('.csv', '').replace('.xlsx', '')
            print(f"{status} {nome_base}")
        
        print(f"\n📊 Total: {sucessos}/{total} consolidações")
        
        return resultados
    
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
        
        for nome_arquivo_base in nomes_arquivos_apis:
            nome_base = nome_arquivo_base.replace('.csv', '').replace('.xlsx', '')
            
            # Busca arquivo no mês -1
            arquivo_origem, extensao = self._buscar_arquivo_api(self.caminho_mes_1, nome_base)
            
            if not arquivo_origem:
                print(f"⚠️ {nome_base} - não encontrado no mês -1")
                resultados[nome_arquivo_base] = False
                continue
            
            # Destino: mantém o nome original
            nome_arquivo_real = os.path.basename(arquivo_origem)
            arquivo_destino = os.path.join(self.caminho_atual, nome_arquivo_real)
            
            try:
                shutil.copy2(arquivo_origem, arquivo_destino)
                print(f"✅ {nome_arquivo_real} - copiado com sucesso")
                resultados[nome_arquivo_base] = True
            except Exception as e:
                print(f"❌ {nome_arquivo_real} - erro ao copiar: {e}")
                resultados[nome_arquivo_base] = False
        
        sucessos = sum(1 for v in resultados.values() if v)
        total = len(resultados)
        
        print(f"\n📊 Resumo: {sucessos}/{total} arquivos copiados com sucesso")
        
        return resultados


def processar_incremental(caminho_atual, arquivo_competencia_atual, nomes_arquivos_apis,
                         processar_somente_fechadas=True):
    """
    Função principal para processamento incremental
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
    
    return arquivo_filtrado, {}, 'processar'


def consolidar_apos_extracao(caminho_atual, nomes_arquivos_apis):
    """
    Consolida dados das APIs após a extração
    """
    print("\n" + "="*60)
    print("📦 CONSOLIDANDO DADOS (NOVOS + MÊS ANTERIOR)")
    print("="*60)   
    
    analisador = AnalisadorIncremental(caminho_atual)
    resultados = analisador.consolidar_todas_apis_inteligente(nomes_arquivos_apis)
    
    print("\n" + "="*60)
    print("✅ CONSOLIDAÇÃO CONCLUÍDA")
    print("="*60 + "\n")
    
    return resultados