from modules.api_competecia import api_competencia
from modules.diretorio import to_save
from modules.conectar_vpn import conectar_vpn
from modules.api_consumo import api_consumo
from modules.api_quantidadeLeito import api_quantidadeLeito
from modules.api_quantidadeCirurgia import api_quantidadeCirurgia
from modules.api_notasFiscais import api_notasFiscais
from modules.api_folhadepagamento import api_folhadepagamento
from modules.api_custosIndividualizadoPorCentro import api_custosIndividualizadoPorCentro
from modules.api_producoes import api_producoes
from modules.api_estatistica import api_estatistica
from modules.api_rankingDeCusto import api_rankingDeCusto
from modules.api_evolucaoDeCustos import api_evolucaoDeCustos
from modules.api_demonstracaoCustoUnitario import api_demonstracaoCustoUnitario
from modules.api_demonstracaoCustoUnitarioPorSaida import api_demonstracaoCustoUnitarioPorSaida
from modules.api_painelComparativoDeCustos import api_painelComparativoDeCustos
from modules.api_custoPorEspecialidade import api_custoPorEspecialidade
from modules.api_analisedepartamental import api_analisedepartamental
from modules.api_composicaoDeCustos import api_composicaoDeCustos
from modules.api_composicaoEvolucaoDeReceita import api_composicaoEvolucaoDeReceita
from modules.api_exercicioOrcamento import api_exercicioOrcamento
from modules.api_custoUnitarioPorPonderacao import api_custoUnitarioPorPonderacao
from modules.api_demonstracaoCustoUnitarioDosServicosAuxiliares import api_demonstracaoCustoUnitarioDosServicosAuxiliares
from modules.api_benchmarkComposicaoDeCustos import api_benchmarkComposicaoDeCustos
from modules.execution_tracker import ExecutionTracker
from modules.google_drive_upload import salvar_arquivos_no_drive
from modules.ponto import consolidar_apos_extracao, processar_incremental
from modules.resumo_incremental import ResumoIncremental
from modules.log_historico import LogHistorico
from dotenv import load_dotenv
from datetime import datetime
import sys
import os


# Pasta raiz do projeto (onde este main.py está)
PASTA_RAIZ_PROJETO = os.path.dirname(os.path.abspath(__file__))


def filtrar_arquivos_para_upload(arquivos_gerados):
    """
    Filtra apenas os arquivos que devem ser enviados ao Google Drive.
    """
    padroes_incluir = [
        'api_benchmarkcomposicaodecustos',
        'api_evolucaodecustos',
        'api_rankingdecusto',
        'api_demonstracaocustounitariodosservicosauxiliares'
    ]

    arquivos_filtrados = []
    arquivos_excluidos = []

    for arquivo in arquivos_gerados:
        if any(padrao in arquivo.lower() for padrao in padroes_incluir):
            arquivos_filtrados.append(arquivo)
        else:
            arquivos_excluidos.append(arquivo)

    if arquivos_excluidos:
        print(f"\n📋 Filtro de Upload:")
        print(f"   ✅ Arquivos a enviar: {len(arquivos_filtrados)}")
        print(f"   ⏭️  Arquivos excluídos: {len(arquivos_excluidos)}")
        print(f"\n   Excluídos:")
        for arq in arquivos_excluidos:
            print(f"      • {arq}")
        print()

    return arquivos_filtrados


def main():
    print("\n" + "=" * 60)
    print("🚀 INICIANDO AUTOMAÇÃO DE EXTRAÇÃO DE DADOS")
    print("=" * 60 + "\n")

    tracker = ExecutionTracker()

    # Setup inicial
    load_dotenv()
    print("✅ Variáveis de ambiente carregadas\n")
    print("🔐 Verificando conexão VPN...")
    try:
        conectar_vpn()
        print("✅ VPN conectada\n")
    except Exception as e:
        print(f"❌ Erro ao conectar VPN: {e}")
        sys.exit(1)

    try:
        caminho = to_save()
        print(f"📁 Diretório: {caminho}\n")
    except Exception as e:
        print(f"❌ Erro ao definir diretório: {e}")
        sys.exit(1)

    # =========================================================================
    # Inicializar log histórico (pasta raiz do projeto)
    # =========================================================================
    log_historico = LogHistorico(pasta_raiz_projeto=PASTA_RAIZ_PROJETO)
    log_historico.iniciar_execucao(competencia=caminho, modo="processar")

    # =========================================================================
    # PASSO 1: EXTRAIR COMPETÊNCIAS
    # =========================================================================
    print("=" * 60)
    print("📅 PASSO 1: Extraindo competências")
    print("=" * 60)

    diretorio_arquivo_competencia = api_competencia(caminho)

    if not diretorio_arquivo_competencia:
        print("\n❌ Arquivo de competências não gerado")
        log_historico.fechar_execucao(
            resultados={},
            observacoes="Falhou na extração de competências"
        )
        sys.exit(1)

    # =========================================================================
    # PASSO 2: PROCESSAMENTO INCREMENTAL
    # =========================================================================
    print("\n" + "=" * 60)
    print("📅 PASSO 2: Análise Incremental de Competências")
    print("=" * 60)

    arquivos_apis_para_consolidar = [
        "api_estatistica.csv",
        "api_rankingdecusto.csv",
        "api_evolucaodecustos.csv",
        "api_demonstracaocustounitario.csv",
        "api_demonstracaocustounitarioporsaida.csv",
        "api_painelcomparativodecustos.csv",
        "api_custoporespecialidade.csv",
        "api_analisedepartamental.csv",
        "api_composicaodecustos.csv",
        "api_composicaoevolucaodereceita.csv",
        "api_custoUnitarioporponderacao.csv",
        "api_demonstracaocustounitariodosservicosauxiliares.csv",
        "api_benchmarkcomposicaodecustos.csv",
        'api_consumo.csv',
        'api_quantidadeleito.csv',
        'api_quantidadecirurgia.csv',
        'api_notasfiscais.csv',
        'api_folhadepagamento.csv',
        'api_custosindividualizadoporcentro.csv',
        'api_producoes.csv'
    ]

    arquivo_filtrado, resultados_inc, modo = processar_incremental(
        caminho_atual=caminho,
        arquivo_competencia_atual=diretorio_arquivo_competencia,
        nomes_arquivos_apis=arquivos_apis_para_consolidar,
        processar_somente_fechadas=True
    )

    # =========================================================================
    # DECISÃO: COPIAR OU PROCESSAR
    # =========================================================================
    if modo == 'copiar':
        print("\n" + "=" * 60)
        print("✅ EXECUÇÃO FINALIZADA - MODO CÓPIA")
        print("   Todos os arquivos do mês anterior foram copiados")
        print("=" * 60 + "\n")

        log_historico.fechar_execucao(
            resultados={},
            observacoes="Modo cópia — nenhuma competência nova para processar"
        )
        sys.exit(0)

    if arquivo_filtrado is None:
        print("\n❌ Erro ao filtrar competências")
        log_historico.fechar_execucao(
            resultados={},
            observacoes="Erro ao filtrar competências"
        )
        sys.exit(1)

    diretorio_arquivo_competencia = arquivo_filtrado

    # =========================================================================
    # Inicializar resumo incremental (pasta da competência)
    # =========================================================================
    resumo = ResumoIncremental(caminho_saida=caminho)

    # =========================================================================
    # PASSO 3: EXTRAIR DADOS DAS APIs
    # =========================================================================
    print("\n" + "=" * 60)
    print("📡 PASSO 3: Extraindo dados das APIs (apenas competências novas)")
    print("=" * 60)

    apis_para_executar = [
        ("Consumo",                                        api_consumo),
        ("QuantidadeLeito",                                api_quantidadeLeito),
        ("QuantidadeCirurgia",                             api_quantidadeCirurgia),
        ("NotasFiscais",                                   api_notasFiscais),
        ("FolhadePagamento",                               api_folhadepagamento),
        ("custosIndividualizadoPorCentro",                 api_custosIndividualizadoPorCentro),
        ("producoes",                                      api_producoes),
        ("estatistica",                                    api_estatistica),
        ("rankingDeCusto",                                 api_rankingDeCusto),
        ("evolucaoDeCustos",                               api_evolucaoDeCustos),
        ("demonstracaoCustoUnitario",                      api_demonstracaoCustoUnitario),
        ("demonstracaoCustoUnitarioPorSaida",              api_demonstracaoCustoUnitarioPorSaida),
        ("painelComparativoDeCustos",                      api_painelComparativoDeCustos),
        ("custoPorEspecialidade",                          api_custoPorEspecialidade),
        ("analisedepartamental",                           api_analisedepartamental),
        ("composicaoDeCustos",                             api_composicaoDeCustos),
        ("composicaoEvolucaoDeReceita",                    api_composicaoEvolucaoDeReceita),
        ("custoUnitarioPorPonderacao",                     api_custoUnitarioPorPonderacao),
        ("demonstracaoCustoUnitarioDosServicosAuxiliares", api_demonstracaoCustoUnitarioDosServicosAuxiliares),
        ("benchmarkComposicaoDeCustos",                    api_benchmarkComposicaoDeCustos),
    ]

    resultados      = {}
    arquivos_gerados = []

    for nome_api, funcao_api in apis_para_executar:
        inicio_api = datetime.now()   # ← marca início desta API
        arquivo    = None
        erro_str   = None

        try:
            if nome_api in ["QuantidadeLeito", "QuantidadeCirurgia"]:
                arquivo = funcao_api(
                    diretorio_arquivo_competencia,
                    caminho,
                    tracker,
                    delay_entre_chamadas=2.0,
                    max_tentativas_403=4,
                    backoff_inicial=3.0,
                    agrupar_por_unidade=True,
                    delay_entre_unidades=5.0
                )
            else:
                arquivo = funcao_api(diretorio_arquivo_competencia, caminho, tracker)

            sucesso = arquivo is not None

        except Exception as e:
            print(f"❌ Erro ao executar {nome_api}: {e}")
            sucesso   = False
            erro_str  = str(e)

        # --- registrar resultado ---
        resultados[nome_api] = {
            "sucesso": sucesso,
            "arquivo": arquivo,
            "erro":    erro_str,
        }

        if arquivo:
            arquivos_gerados.append(os.path.basename(arquivo))

        # ── Salvamento incremental: persiste imediatamente ─────────────────
        resumo.registrar(
            api=nome_api,
            sucesso=sucesso,
            arquivo=arquivo,
            erro=erro_str,
            registros=0,            # se o seu tracker tiver contagem por API, passe aqui
            inicio=inicio_api,
            fim=datetime.now(),
        )
        # ───────────────────────────────────────────────────────────────────

    # =========================================================================
    # PASSO 4: CONSOLIDAR DADOS
    # =========================================================================
    consolidar_apos_extracao(
        caminho_atual=caminho,
        nomes_arquivos_apis=arquivos_apis_para_consolidar
    )

    # =========================================================================
    # PASSO 5: GERAR RELATÓRIO DO TRACKER
    # =========================================================================
    print("\n" + "=" * 60)
    print("📝 GERANDO RELATÓRIO RESUMO")
    print("=" * 60 + "\n")

    caminho_csv = None
    caminho_txt = None

    try:
        caminho_csv, caminho_txt = tracker.gerar_relatorio(caminho)

        if caminho_csv and caminho_txt:
            print(f"✅ Relatório CSV gerado: {caminho_csv}")
            print(f"✅ Relatório TXT gerado: {caminho_txt}\n")
            arquivos_gerados.append(os.path.basename(caminho_csv))
            arquivos_gerados.append(os.path.basename(caminho_txt))
        else:
            print("⚠️ Não foi possível gerar relatórios\n")
    except Exception as e:
        print(f"❌ Erro ao gerar relatório: {e}\n")

    # =========================================================================
    # PASSO 6: UPLOAD PARA GOOGLE DRIVE
    # =========================================================================
    print("=" * 60)
    print("📤 PASSO 6: Upload para Google Drive")
    print("=" * 60)

    if arquivos_gerados:
        try:
            arquivos_para_enviar = filtrar_arquivos_para_upload(arquivos_gerados)

            if not arquivos_para_enviar:
                print("⚠️ Nenhum arquivo corresponde aos critérios de upload\n")
            else:
                diretorio_com_competencia = os.path.join(caminho)

                print(f"📁 Diretório dos arquivos: {diretorio_com_competencia}")
                print(f"📊 Total de arquivos para enviar: {len(arquivos_para_enviar)}\n")

                folder_id = os.getenv('GOOGLE_DRIVE_FOLDER_ID')

                resultados_upload = salvar_arquivos_no_drive(
                    bases=arquivos_para_enviar,
                    diretorio=diretorio_com_competencia,
                    folder_id=folder_id,
                    criar_google_sheets=True
                )

                if resultados_upload:
                    print(f"\n✅ Upload para Google Drive concluído!")
                else:
                    print(f"\n⚠️ Houve problemas no upload para o Google Drive")

        except Exception as e:
            print(f"\n❌ Erro ao fazer upload para Google Drive: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("⚠️ Nenhum arquivo foi gerado para fazer upload\n")

    # =========================================================================
    # RELATÓRIO FINAL NO CONSOLE
    # =========================================================================
    print("=" * 60)
    print("📋 RELATÓRIO FINAL - CONSOLE")
    print("=" * 60 + "\n")

    resumo_tracker = tracker.obter_resumo()

    print(f"📊 ESTATÍSTICAS GERAIS:")
    print(f"   • Total de execuções: {resumo_tracker['total']}")
    if resumo_tracker['total'] > 0:
        pct = resumo_tracker['sucessos'] / resumo_tracker['total'] * 100
        print(f"   • Sucessos: {resumo_tracker['sucessos']} ({pct:.1f}%)")
    else:
        print(f"   • Sucessos: 0")
    print(f"   • Erros: {resumo_tracker['erros']}")
    print(f"   • Timeouts: {resumo_tracker['timeouts']}")
    print(f"   • Sem dados: {resumo_tracker['sem_dados']}")
    print(f"   • Total de registros extraídos: {resumo_tracker['total_registros']:,}\n")

    print(f"🔌 ENDPOINTS PROCESSADOS:")
    for endpoint in resumo_tracker['endpoints']:
        status_ep = "✅" if resultados.get(endpoint, {}).get('sucesso', False) else "❌"
        info = resultados.get(endpoint, {}).get('arquivo') or resultados.get(endpoint, {}).get('erro', 'Falhou')
        print(f"   {status_ep} {endpoint}")
        if resultados.get(endpoint, {}).get('arquivo'):
            print(f"      └─ Arquivo: {info}")

    print("\n" + "=" * 60)

    sucesso_total = sum(1 for r in resultados.values() if r['sucesso'])
    total_apis    = len(resultados)

    if sucesso_total == total_apis:
        print(f"✅ TODAS AS APIs EXECUTADAS COM SUCESSO ({sucesso_total}/{total_apis})")
    elif sucesso_total > 0:
        print(f"⚠️ EXECUÇÃO PARCIAL: {sucesso_total}/{total_apis} APIs concluídas")
    else:
        print(f"❌ NENHUMA API FOI EXECUTADA COM SUCESSO (0/{total_apis})")

    print("=" * 60 + "\n")

    if resumo_tracker.get('endpoints') and caminho_txt:
        print(f"📄 Para mais detalhes, consulte o relatório em:")
        print(f"   {caminho_txt}\n")

    # =========================================================================
    # Fechar log histórico ← sempre executado, mesmo em erros parciais
    # =========================================================================
    log_historico.fechar_execucao(
        resultados=resultados,
        total_registros=resumo_tracker.get('total_registros', 0),
        observacoes=""
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Interrompido pelo usuário")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)