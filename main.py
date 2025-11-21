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
from dotenv import load_dotenv
import sys
import os

def main():
    print("\n" + "="*60)
    print("🚀 INICIANDO AUTOMAÇÃO DE EXTRAÇÃO DE DADOS")
    print("="*60 + "\n")
    
    tracker = ExecutionTracker()
    
    # Setup inicial
    load_dotenv()
    print("✅ Variáveis de ambiente carregadas\n")
    caminho = os.getenv("CAMINHO_FIXO")
    print(caminho)
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
    
    # Extrair competências
    print("="*60)
    print("📅 PASSO 1: Extraindo competências")
    print("="*60)
    
    diretorio_arquivo_competencia = api_competencia(caminho)

    if not diretorio_arquivo_competencia:
        print("\n❌ Arquivo de competências não gerado")
        sys.exit(1)
    

    # Extrair competências
    print("="*60)
    print("📅 PASSO 1: Extraindo exercicioOrcamento")
    print("="*60)

    diretorio_arquivo_exercicioOrcamento = api_exercicioOrcamento(caminho)
    
    if not diretorio_arquivo_exercicioOrcamento:
        print("\n❌ Arquivo de exercicioOrcamento não gerado")

    # Executar todas as APIs
    apis_para_executar = [
        # ("Consumo", api_consumo),
        # ("QuantidadeLeito", api_quantidadeLeito),
        # ("QuantidadeCirurgia", api_quantidadeCirurgia),
        # ("NotasFiscais", api_notasFiscais),
        # ("FolhadePagamento", api_folhadepagamento),
        # ("custosIndividualizadoPorCentro", api_custosIndividualizadoPorCentro),
        # ("producoes", api_producoes), 
        # ("estatistica", api_estatistica),
        ("rankingDeCusto", api_rankingDeCusto),
        ("evolucaoDeCustos", api_evolucaoDeCustos),
        # ("demonstracaoCustoUnitario", api_demonstracaoCustoUnitario),
        # ("demonstracaoCustoUnitarioPorSaida", api_demonstracaoCustoUnitarioPorSaida),
        # ("painelComparativoDeCustos", api_painelComparativoDeCustos),
        # ("custoPorEspecialidade", api_custoPorEspecialidade),
        # ("analisedepartamental", api_analisedepartamental),
        # ("composicaoDeCustos", api_composicaoDeCustos),
        # ("composicaoEvolucaoDeReceita", api_composicaoEvolucaoDeReceita),
        # ("custoUnitarioPorPonderacao", api_custoUnitarioPorPonderacao),
        ("demonstracaoCustoUnitarioDosServicosAuxiliares", api_demonstracaoCustoUnitarioDosServicosAuxiliares),
        ("benchmarkComposicaoDeCustos", api_benchmarkComposicaoDeCustos),


    ]
    
    resultados = {}
    
    for nome_api, funcao_api in apis_para_executar:
        try:
            # Configuração específica para QuantidadeLeito
            if nome_api in ["QuantidadeLeito", "QuantidadeCirurgia"]:
                arquivo = funcao_api(
                    diretorio_arquivo_competencia, 
                    caminho, 
                    tracker,
                    delay_entre_chamadas=2.0,      # 2s entre requisições
                    max_tentativas_403=4,          # 3 tentativas em caso de 403
                    backoff_inicial=3.0,           # Espera inicial de 3s no retry
                    agrupar_por_unidade=True,      # Processa todas competências de uma unidade antes de passar para próxima
                    delay_entre_unidades=5.0       # 5s ao mudar de unidade
                )
            else:
                # Outras APIs usam configuração padrão
                arquivo = funcao_api(diretorio_arquivo_competencia, caminho, tracker)
            
            resultados[nome_api] = {
                "sucesso": arquivo is not None,
                "arquivo": arquivo
            }
        except Exception as e:
            print(f"❌ Erro ao executar {nome_api}: {e}")
            resultados[nome_api] = {
                "sucesso": False,
                "erro": str(e)
            }
    
    print("\n" + "="*60)
    print("📝 GERANDO RELATÓRIO RESUMO")
    print("="*60 + "\n")
    
    try:
        caminho_csv, caminho_txt = tracker.gerar_relatorio(caminho)
        
        if caminho_csv and caminho_txt:
            print(f"✅ Relatório CSV gerado: {caminho_csv}")
            print(f"✅ Relatório TXT gerado: {caminho_txt}\n")
        else:
            print("⚠️ Não foi possível gerar relatórios\n")
            
    except Exception as e:
        print(f"❌ Erro ao gerar relatório: {e}\n")
    
    # Relatório final no console
    print("="*60)
    print("📋 RELATÓRIO FINAL - CONSOLE")
    print("="*60 + "\n")
    
    # Estatísticas gerais
    resumo = tracker.obter_resumo()
    
    print(f"📊 ESTATÍSTICAS GERAIS:")
    print(f"   • Total de execuções: {resumo['total']}")
    print(f"   • Sucessos: {resumo['sucessos']} ({resumo['sucessos']/resumo['total']*100:.1f}%)" if resumo['total'] > 0 else "   • Sucessos: 0")
    print(f"   • Erros: {resumo['erros']}")
    print(f"   • Timeouts: {resumo['timeouts']}")
    print(f"   • Sem dados: {resumo['sem_dados']}")
    print(f"   • Total de registros extraídos: {resumo['total_registros']:,}\n")
    
    print(f"🔌 ENDPOINTS PROCESSADOS:")
    for endpoint in resumo['endpoints']:
        status_endpoint = "✅" if resultados.get(endpoint, {}).get('sucesso', False) else "❌"
        info = resultados.get(endpoint, {}).get('arquivo', resultados.get(endpoint, {}).get('erro', 'Falhou'))
        print(f"   {status_endpoint} {endpoint}")
        if resultados.get(endpoint, {}).get('arquivo'):
            print(f"      └─ Arquivo: {info}")
    
    print("\n" + "="*60)
    
    sucesso_total = sum(1 for r in resultados.values() if r['sucesso'])
    total_apis = len(resultados)
    
    if sucesso_total == total_apis:
        print(f"✅ TODAS AS APIs EXECUTADAS COM SUCESSO ({sucesso_total}/{total_apis})")
    elif sucesso_total > 0:
        print(f"⚠️ EXECUÇÃO PARCIAL: {sucesso_total}/{total_apis} APIs concluídas")
    else:
        print(f"❌ NENHUMA API FOI EXECUTADA COM SUCESSO (0/{total_apis})")
    
    print("="*60 + "\n")
    
    if caminho_txt:
        print(f"📄 Para mais detalhes, consulte o relatório em:")
        print(f"   {caminho_txt}\n")

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