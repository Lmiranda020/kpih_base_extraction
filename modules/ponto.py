"""
Módulo para análise incremental de competências com consolidação robusta

Fluxo:
    1. Localiza a BASE: a pasta de mês anterior mais recente que tenha
       'competencias_todas_unidades.xlsx' (ou a pasta informada explicitamente).
    2. Compara as competências de hoje com as da base e seleciona para extração
       apenas as FECHADAS que são novas, que estavam abertas/reabertas na base
       ou cujo rateio foi reexecutado (execucaoRateio diferente).
    3. Depois da extração, cada API é consolidada: dados novos + dados da base,
       removendo da base as (unidade, competência) que vieram de novo.
"""
import os
import re
import shutil
import pandas as pd
from dotenv import load_dotenv
from modules.rede import executar_com_retry, copiar_seguro, salvar_dataframe_seguro

ARQUIVO_COMPETENCIAS = "competencias_todas_unidades.xlsx"
ARQUIVO_FILTRADO = "competencias_todas_unidades_filtrado.xlsx"
ARQUIVO_FECHADAS = "competencias_todas_unidades_fechadas.xlsx"

SITUACOES_ABERTAS = ('ABERTA', 'REABERTA')
MAX_MESES_BUSCA_BASE = 36


def _normalizar_competencia(valor):
    """Converte '1/2024', '01/2024' ou '1/2024.0' para '01/2024'"""
    if pd.isna(valor):
        return valor
    texto = str(valor).strip()
    m = re.fullmatch(r"(\d{1,2})/(\d{4})", texto)
    if m:
        return f"{int(m.group(1)):02d}/{m.group(2)}"
    return texto


def _mes_anterior(mes, ano):
    return (12, ano - 1) if mes == 1 else (mes - 1, ano)


class AnalisadorIncremental:
    """Gerencia a análise incremental de competências entre a pasta atual e a base"""

    def __init__(self, caminho_atual, caminho_base=None):
        """
        Args:
            caminho_atual: Diretório da execução atual (ex: .../2026/09_2026)
            caminho_base: Pasta da base a usar ("MM_YYYY" ou caminho completo).
                          Se None, usa o mês anterior mais recente que tenha
                          o arquivo de competências.
        """
        load_dotenv()
        self.caminho_fixo = os.getenv("caminho_fixo")
        self.caminho_atual = caminho_atual
        self.sufixo_atual = os.path.basename(os.path.normpath(caminho_atual))
        self.caminho_base = self._localizar_base(caminho_base)

    # ------------------------------------------------------------------
    # Localização da base
    # ------------------------------------------------------------------
    def _caminho_da_pasta(self, mes, ano):
        return os.path.join(self.caminho_fixo, str(ano), f"{mes:02d}_{ano}")

    def _localizar_base(self, caminho_base):
        print("\n📂 LOCALIZANDO BASE PARA CONSOLIDAÇÃO:")

        if caminho_base:
            if re.fullmatch(r"\d{2}_\d{4}", caminho_base):
                mes, ano = (int(p) for p in caminho_base.split('_'))
                caminho_base = self._caminho_da_pasta(mes, ano)

            if not os.path.exists(os.path.join(caminho_base, ARQUIVO_COMPETENCIAS)):
                raise FileNotFoundError(
                    f"Base informada não tem {ARQUIVO_COMPETENCIAS}: {caminho_base}"
                )
            print(f"✅ Base informada: {caminho_base}")
            return caminho_base

        try:
            mes, ano = (int(p) for p in self.sufixo_atual.split('_'))
        except ValueError:
            print(f"❌ Formato inválido da pasta: {self.sufixo_atual}")
            return None

        for _ in range(MAX_MESES_BUSCA_BASE):
            mes, ano = _mes_anterior(mes, ano)
            pasta = self._caminho_da_pasta(mes, ano)
            if os.path.exists(os.path.join(pasta, ARQUIVO_COMPETENCIAS)):
                print(f"✅ Base encontrada: {pasta}")
                return pasta
            print(f"   ⏭️ {mes:02d}_{ano}: sem {ARQUIVO_COMPETENCIAS}")

        print("⚠️ Nenhuma base encontrada - será feita extração completa")
        return None

    # ------------------------------------------------------------------
    # Filtro de competências
    # ------------------------------------------------------------------
    def filtrar_competencias_nao_processadas(self, arquivo_competencia_atual,
                                             processar_somente_fechadas=True):
        """
        Gera o arquivo filtrado com as competências que precisam ser extraídas.

        Returns:
            str: Caminho do arquivo filtrado, ou None se não houver nada a extrair
        """
        print("\n" + "=" * 70)
        print("🔍 ANÁLISE INCREMENTAL (ATUAL x BASE)")
        print("=" * 70)

        if not os.path.exists(arquivo_competencia_atual):
            print(f"❌ Arquivo não encontrado: {arquivo_competencia_atual}")
            return None

        df_atual = executar_com_retry(lambda: pd.read_excel(arquivo_competencia_atual), ARQUIVO_COMPETENCIAS)
        total_inicial = len(df_atual)
        print(f"\n📊 MÊS ATUAL: {total_inicial} competências")

        if processar_somente_fechadas:
            df_atual = df_atual[~df_atual['situacao'].isin(SITUACOES_ABERTAS)].copy()
            print(f"🔒 Apenas FECHADAS: {len(df_atual)}")

        # Lista completa de fechadas: usada pelas APIs que não têm arquivo na base
        salvar_dataframe_seguro(df_atual, os.path.join(self.caminho_atual, ARQUIVO_FECHADAS))

        if df_atual.empty:
            print("\n⚠️ Nenhuma competência para processar no mês atual!")
            return None

        if self.caminho_base is None:
            print("\n⚠️ SEM BASE - processando TODAS as competências")
            df_final = df_atual
        else:
            df_base = executar_com_retry(
                lambda: pd.read_excel(os.path.join(self.caminho_base, ARQUIVO_COMPETENCIAS), dtype=str),
                f"base {ARQUIVO_COMPETENCIAS}"
            )
            print(f"📂 BASE ({os.path.basename(self.caminho_base)}): {len(df_base)} competências")

            df_base = (
                df_base[['nome', 'competencia', 'situacao', 'execucaoRateio']]
                .drop_duplicates(subset=['nome', 'competencia'], keep='last')
                .rename(columns={'situacao': 'situacao_base',
                                 'execucaoRateio': 'execucaoRateio_base'})
            )

            # Relê como texto para comparar exatamente com a base (lida também como texto)
            df_atual_txt = executar_com_retry(
                lambda: pd.read_excel(arquivo_competencia_atual, dtype=str), ARQUIVO_COMPETENCIAS
            ).loc[df_atual.index]
            comparacao = df_atual_txt[['nome', 'competencia', 'execucaoRateio']].merge(
                df_base, on=['nome', 'competencia'], how='left'
            )
            comparacao.index = df_atual.index

            nova = comparacao['situacao_base'].isna()
            estava_aberta = ~nova & comparacao['situacao_base'].isin(SITUACOES_ABERTAS)
            rateio_mudou = (
                ~nova & ~estava_aberta
                & (comparacao['execucaoRateio'].fillna('') != comparacao['execucaoRateio_base'].fillna(''))
            )
            processar = nova | estava_aberta | rateio_mudou

            print(f"\n🧮 MOTIVOS PARA EXTRAIR:")
            print(f"   • Novas (não existiam na base): {nova.sum()}")
            print(f"   • Estavam ABERTAS/REABERTAS na base e agora fechadas: {estava_aberta.sum()}")
            print(f"   • Rateio reexecutado desde a base: {rateio_mudou.sum()}")
            print(f"   • Sem alteração (mantidas da base): {(~processar).sum()}")

            df_final = df_atual[processar]

        print(f"\n   ✅ RESTANTES PARA PROCESSAR: {len(df_final)}")

        if df_final.empty:
            print("\n⚠️ NENHUMA COMPETÊNCIA NOVA PARA PROCESSAR!")
            return None

        caminho_filtrado = os.path.join(os.path.dirname(arquivo_competencia_atual), ARQUIVO_FILTRADO)
        salvar_dataframe_seguro(df_final, caminho_filtrado)
        print(f"\n💾 Arquivo filtrado salvo: {caminho_filtrado}")

        return caminho_filtrado

    # ------------------------------------------------------------------
    # Arquivos das APIs
    # ------------------------------------------------------------------
    @staticmethod
    def _nome_base(nome_arquivo_api):
        return os.path.splitext(nome_arquivo_api)[0].lower()

    def _buscar_arquivo_api(self, diretorio, nome_base):
        """
        Busca o arquivo exato da API ('<nome_base>_MM_YYYY.csv|xlsx').

        O match é exato para não confundir, por exemplo, 'api_demonstracaocustounitario'
        com 'api_demonstracaocustounitarioporsaida'.

        Returns:
            tuple: (caminho_completo, extensao) ou (None, None)
        """
        if not diretorio or not os.path.isdir(diretorio):
            return None, None

        padrao = re.compile(rf"^{re.escape(nome_base)}_\d{{2}}_\d{{4}}\.(csv|xlsx)$", re.IGNORECASE)
        candidatos = [
            os.path.join(diretorio, f) for f in os.listdir(diretorio) if padrao.match(f)
        ]
        if not candidatos:
            return None, None

        # Prefere CSV e, entre iguais, o mais recente
        candidatos.sort(key=lambda c: (c.lower().endswith('.csv'), os.path.getmtime(c)), reverse=True)
        arquivo = candidatos[0]
        return arquivo, os.path.splitext(arquivo)[1].lower()

    def api_tem_base(self, nome_arquivo_api):
        arquivo, _ = self._buscar_arquivo_api(self.caminho_base, self._nome_base(nome_arquivo_api))
        return arquivo is not None

    def _carregar_arquivo_api(self, caminho_arquivo, extensao):
        """Carrega como texto para regravar os valores exatamente como estavam"""
        try:
            descricao = os.path.basename(caminho_arquivo)
            if extensao == '.csv':
                try:
                    return executar_com_retry(
                        lambda: pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8-sig',
                                            dtype=str, keep_default_na=False, low_memory=False),
                        descricao
                    )
                except OSError:
                    raise
                except Exception:
                    from modules.csv_reader import ler_csv_robusto
                    return ler_csv_robusto(caminho_arquivo, sep=';', encoding='utf-8-sig')
            return executar_com_retry(lambda: pd.read_excel(caminho_arquivo, dtype=str), descricao)
        except Exception as e:
            print(f"   ❌ Erro ao carregar arquivo: {e}")
            return None

    @staticmethod
    def _chaves_unidade_competencia(df):
        """Série 'unidade|MM/YYYY' por linha, ou None se faltarem colunas"""
        coluna_comp = next((c for c in ('competenciaDescr', 'competencia') if c in df.columns), None)
        if coluna_comp is None or 'unidade' not in df.columns:
            return None
        return df['unidade'].astype(str).str.strip() + '|' + df[coluna_comp].map(_normalizar_competencia).astype(str)

    def _copiar_da_base(self, arquivo_base, extensao):
        nome_base = re.sub(r"_\d{2}_\d{4}$", "", os.path.splitext(os.path.basename(arquivo_base))[0])
        destino = os.path.join(self.caminho_atual, f"{nome_base}_{self.sufixo_atual}{extensao}")
        copiar_seguro(arquivo_base, destino)
        return destino

    def consolidar_dados_api(self, nome_arquivo_api):
        """
        Consolida uma API: dados novos (pasta atual) + base, sem duplicar
        as (unidade, competência) que foram extraídas de novo.

        Returns:
            tuple: (status, caminho_arquivo_final)
                status: 'consolidado' | 'apenas_novos' | 'copiado_base' | 'ausente' | 'erro'
        """
        nome_base = self._nome_base(nome_arquivo_api)
        print(f"\n🔄 Consolidando: {nome_base}")

        arquivo_novo, extensao_novo = self._buscar_arquivo_api(self.caminho_atual, nome_base)
        arquivo_antigo, extensao_antigo = self._buscar_arquivo_api(self.caminho_base, nome_base)

        if not arquivo_novo and not arquivo_antigo:
            print(f"   ⚠️ Sem arquivo novo e sem arquivo na base")
            return 'ausente', None

        if not arquivo_novo:
            destino = self._copiar_da_base(arquivo_antigo, extensao_antigo)
            print(f"   ⚠️ Nada extraído agora - copiado da base: {os.path.basename(destino)}")
            return 'copiado_base', destino

        print(f"   📄 Novo: {os.path.basename(arquivo_novo)}")

        if not arquivo_antigo:
            print(f"   ℹ️ Sem arquivo na base - mantendo apenas dados novos")
            return 'apenas_novos', arquivo_novo

        print(f"   📄 Base: {os.path.basename(arquivo_antigo)}")

        df_novo = self._carregar_arquivo_api(arquivo_novo, extensao_novo)
        df_antigo = self._carregar_arquivo_api(arquivo_antigo, extensao_antigo)

        if df_novo is None:
            print(f"   ❌ Falha ao carregar arquivo novo")
            return 'erro', arquivo_novo
        if df_antigo is None:
            print(f"   ❌ Falha ao carregar arquivo da base - arquivo novo mantido sem consolidar")
            return 'erro', arquivo_novo

        print(f"   📊 Registros novos: {len(df_novo):,} | base: {len(df_antigo):,}")

        try:
            chaves_novas = self._chaves_unidade_competencia(df_novo)
            chaves_antigas = self._chaves_unidade_competencia(df_antigo)

            if chaves_novas is None or chaves_antigas is None:
                print(f"   ⚠️ Colunas 'unidade'/'competência' ausentes - consolidando só por linhas idênticas")
                df_antigo_mantido = df_antigo
            else:
                substituidas = chaves_antigas.isin(set(chaves_novas))
                df_antigo_mantido = df_antigo[~substituidas]
                print(f"   🗑️ Registros da base substituídos pela nova extração: {substituidas.sum():,}")

            df_consolidado = pd.concat([df_novo, df_antigo_mantido], ignore_index=True)

            tamanho_antes = len(df_consolidado)
            df_consolidado = df_consolidado.drop_duplicates(keep='first')
            duplicatas = tamanho_antes - len(df_consolidado)
            if duplicatas > 0:
                print(f"   🗑️ Linhas idênticas removidas: {duplicatas:,}")

            print(f"   ✅ Total consolidado: {len(df_consolidado):,}")

            salvar_dataframe_seguro(df_consolidado, arquivo_novo, index=False, sep=';', encoding='utf-8-sig')

            print(f"   💾 Arquivo consolidado salvo: {os.path.basename(arquivo_novo)}")
            return 'consolidado', arquivo_novo

        except Exception as e:
            print(f"   ❌ Erro ao consolidar: {e}")
            import traceback
            traceback.print_exc()
            return 'erro', arquivo_novo

    def copiar_arquivos_da_base(self, nomes_arquivos_apis):
        """Copia os arquivos das APIs da base para a pasta atual (com o sufixo atual)"""
        print("\n" + "=" * 60)
        print("📂 COPIANDO ARQUIVOS DA BASE")
        print("=" * 60)

        if not self.caminho_base:
            print("❌ Não há base disponível")
            return {}

        resultados = {}

        for nome_arquivo in nomes_arquivos_apis:
            nome_base = self._nome_base(nome_arquivo)
            arquivo_origem, extensao = self._buscar_arquivo_api(self.caminho_base, nome_base)

            if not arquivo_origem:
                print(f"⚠️ {nome_base} - não encontrado na base")
                resultados[nome_arquivo] = False
                continue

            try:
                destino = self._copiar_da_base(arquivo_origem, extensao)
                print(f"✅ {os.path.basename(destino)} - copiado com sucesso")
                resultados[nome_arquivo] = True
            except Exception as e:
                print(f"❌ {nome_base} - erro ao copiar: {e}")
                resultados[nome_arquivo] = False

        sucessos = sum(1 for v in resultados.values() if v)
        print(f"\n📊 Resumo: {sucessos}/{len(resultados)} arquivos copiados com sucesso")

        return resultados


def processar_incremental(caminho_atual, arquivo_competencia_atual, nomes_arquivos_apis,
                          processar_somente_fechadas=True, caminho_base=None):
    """
    Função principal para processamento incremental

    Returns:
        dict com:
            modo: 'processar' ou 'copiar'
            caminho_base: pasta usada como base (ou None)
            arquivo_filtrado: competências a extrair (ou None)
            arquivo_fechadas: todas as competências fechadas (para APIs sem base)
            apis_sem_base: nomes de arquivo das APIs que não têm arquivo na base
    """
    print("\n" + "=" * 60)
    print("🚀 INICIANDO PROCESSAMENTO INCREMENTAL")
    print("=" * 60)

    analisador = AnalisadorIncremental(caminho_atual, caminho_base)

    arquivo_filtrado = analisador.filtrar_competencias_nao_processadas(
        arquivo_competencia_atual,
        processar_somente_fechadas=processar_somente_fechadas
    )
    arquivo_fechadas = os.path.join(caminho_atual, ARQUIVO_FECHADAS)

    apis_sem_base = set()
    if analisador.caminho_base:
        apis_sem_base = {n for n in nomes_arquivos_apis if not analisador.api_tem_base(n)}
        if apis_sem_base:
            print(f"\n⚠️ APIs sem arquivo na base (extração COMPLETA): ")
            for n in sorted(apis_sem_base):
                print(f"   • {n}")

    resultado = {
        'modo': 'processar',
        'caminho_base': analisador.caminho_base,
        'arquivo_filtrado': arquivo_filtrado,
        'arquivo_fechadas': arquivo_fechadas,
        'apis_sem_base': apis_sem_base,
    }

    if arquivo_filtrado is None and not apis_sem_base:
        print("\n" + "=" * 60)
        print("📋 MODO: CÓPIA (sem novas competências)")
        print("=" * 60)

        analisador.copiar_arquivos_da_base(nomes_arquivos_apis)
        resultado['modo'] = 'copiar'
        return resultado

    print("\n" + "=" * 60)
    print("📋 MODO: PROCESSAMENTO (há competências para extrair)")
    print("=" * 60)
    print("\n⚠️ Consolidação será executada após extração dos dados novos")

    return resultado


def consolidar_apos_extracao(caminho_atual, nomes_arquivos_apis, caminho_base=None):
    """
    Consolida todas as APIs (novos + base).

    Returns:
        dict: {nome_arquivo_api: (status, caminho_arquivo_final)}
    """
    analisador = AnalisadorIncremental(caminho_atual, caminho_base)
    print("\n" + "=" * 60)
    print("📦 CONSOLIDANDO DADOS (NOVOS + BASE)")
    print("=" * 60)

    resultados = {}
    for nome_arquivo in nomes_arquivos_apis:
        resultados[nome_arquivo] = analisador.consolidar_dados_api(nome_arquivo)

    print("\n" + "=" * 60)
    print("📋 RESUMO DA CONSOLIDAÇÃO")
    print("=" * 60)
    icones = {'consolidado': '✅', 'apenas_novos': '🆕', 'copiado_base': '📋', 'ausente': '⚠️', 'erro': '❌'}
    for nome_arquivo, (status, _) in resultados.items():
        print(f"{icones.get(status, '•')} {AnalisadorIncremental._nome_base(nome_arquivo)}: {status}")
    print("=" * 60 + "\n")

    return resultados
