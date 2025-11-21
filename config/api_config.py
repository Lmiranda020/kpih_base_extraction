"""
Configurações de todas as APIs
Cada API define seu próprio payload e processamento
"""

def payload_consumo(unidade):
    """Payload específico para API de Consumo"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
        "tipoAgrupamento": "CENTRO",
    }

def payload_quantidadeLeito(unidade):
    """Payload específico para API de QuantidadeLeito"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
    }

def payload_quantidadeCirurgia(unidade):
    """Payload específico para API de QuantidadeCirurgia"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
    }

def payload_notasFiscais(unidade):
    """Payload específico para API de QuantidadeCirurgia"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
    }

def payload_folhaPagamento(unidade):
    """Payload específico para API de Benchmarker"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
        "tipoAgrupamento": "CENTRO"
    }

def payload_custosIndividualizadoPorCentro(unidade):
    """Payload específico para API de Custos Fixos"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
        "quantificacaoUnidadeProducao": "ABSOLUTO",
        "considerarRecursosExternos": True,
        "considerarMatMed": True,
        "considerarHonorarios": True
    }

def payload_producoes(unidade):
    """Payload específico para API de Benchmarker"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
        "quantificacaoUnidadeProducao": "ABSOLUTO"
    }

def payload_estatistica(unidade):
    """Payload específico para API de Benchmarker"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
        "quantificacaoUnidadeProducao": "ABSOLUTO"
    }

def payload_rankingDeCusto(unidade):
    """Payload específico para API de Benchmarker"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia']
    }

def payload_evolucaoDeCustos(unidade):
    """Payload específico para API de Benchmarker"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
        "tipoPlanoDeConta": "UNIDADE",
        "tipoRelatorio": "ANALITICO",
        "considerarCustoComDepreciacao": True,
        "considerarRecursosExternos": True,
        "separarCustoFixoEVariavel": True,
        "exibirOutrasDespesas": True
    }

def payload_demonstracaoCustoUnitario(unidade):
    """Payload específico para API de Benchmarker"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
        "considerarRecursosExternos":True,
        "considerarMatMed":True,
        "considerarHonorarios":True,
        "considerarDepreciacao":True
    }

def payload_demonstracaoCustoUnitarioPorSaida(unidade):
    """Payload específico para API de Benchmarker"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
        "considerarRecursosExternos": True,
        "considerarMatMed": True
    }

def payload_painelComparativoDeCustos(unidade):
    """Payload específico para API de Benchmarker"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
        "compararPor": "GESTORA",
        "considerarRecursosExternos":True,
        "considerarMatMed":True,
        "considerarHonorarios":True,
        "exibeCustosInvalidos":True,
        "deveConsiderarSADT":True
    }

def payload_custoPorEspecialidade(unidade):
    """Payload específico para API de Benchmarker"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
        "considerarRecursosExternos":True,
        "considerarDepreciacao":True
    }

def payload_analisedepartamental(unidade):
    """Payload específico para API de Benchmarker"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
        "considerarMatMed":True,
        "considerarHonorarios":True
    }

def payload_composicaoDeCustos(unidade):
    """Payload específico para API composição de custo"""
    return {
        "competencia": unidade['competencia'],
        "considerarRecursosExternos":True,
        "considerarDepreciacao":True
    }

def payload_composicaoEvolucaoDeReceita(unidade):
    """Payload específico para API de Benchmarker"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia']
    }

def payload_exercicioOrcamento(unidade):
    """Payload específico para API de exercicioOrcamento - usa GET"""
    return None  # GET não usa payload

def payload_custoUnitarioPorPonderacao(unidade):
    """Payload específico para API de Benchmarker"""
    return {
        "competenciaInicial": unidade['competencia'],
        "competenciaFinal": unidade['competencia'],
        "tipoDeQualificacaoDeEstatistica": "PONDERADO",
        "exibirCodigoContabilDosCentrosDeCusto": True,
        "exibirCodigoTussDosExames": True
    }

# Dicionário de configuração de todas as APIs
APIS_CONFIG = {
    "Consumo": {
        "env_var": "url_consumo",
        "payload_func": payload_consumo,
        "processar_func": None, 
        "timeout": 60
    },
    "QuantidadeLeito": {
        "env_var": "url_quantidadeLeito",
        "payload_func": payload_quantidadeLeito,
        "processar_func": None, 
        "timeout": 60
    },
    "QuantidadeCirurgia": {
        "env_var": "url_quantidadeCirurgia",
        "payload_func": payload_quantidadeCirurgia,
        "processar_func": None,
        "timeout": 90
    },
    "NotasFiscais": {
        "env_var": "url_notasFiscais",
        "payload_func": payload_notasFiscais,
        "processar_func": None,
        "timeout": 60
    },
    "FolhadePagamento": {
        "env_var": "url_folhaPagamento",
        "payload_func": payload_folhaPagamento,
        "processar_func": None,
        "timeout": 60
    },
    "custosIndividualizadoPorCentro": { 
        "env_var": "url_custosIndividualizadoPorCentro",
        "payload_func": payload_custosIndividualizadoPorCentro,
        "processar_func": None,
        "timeout": 60
    },
    "producoes": { 
        "env_var": "url_producoes",
        "payload_func": payload_producoes,
        "processar_func": None,
        "timeout": 60
    },
     "estatistica": { 
        "env_var": "url_estatistica",
        "payload_func": payload_estatistica,
        "processar_func": None,
        "timeout": 60
    },
    "rankingDeCusto": { 
        "env_var": "url_rankingDeCusto",
        "payload_func": payload_rankingDeCusto,
        "processar_func": None,
        "timeout": 60
    },
    "evolucaoDeCustos": { 
        "env_var": "url_evolucaoDeCustos",
        "payload_func": payload_evolucaoDeCustos,
        "processar_func": None,
        "timeout": 60
    },
    "demonstracaoCustoUnitario": { 
        "env_var": "url_demonstracaoCustoUnitario",
        "payload_func": payload_demonstracaoCustoUnitario,
        "processar_func": None,
        "timeout": 60
    },
    "demonstracaoCustoUnitarioPorSaida": { 
        "env_var": "url_demonstracaoCustoUnitarioPorSaida",
        "payload_func": payload_demonstracaoCustoUnitarioPorSaida,
        "processar_func": None,
        "timeout": 60
    },
    "painelComparativoDeCustos": { 
        "env_var": "url_painelComparativoDeCustos",
        "payload_func": payload_painelComparativoDeCustos,
        "processar_func": None,
        "timeout": 60
    },
    "custoPorEspecialidade": { 
        "env_var": "url_custoPorEspecialidade",
        "payload_func": payload_custoPorEspecialidade,
        "processar_func": None,
        "timeout": 60
    },
    "analisedepartamental": { 
        "env_var": "url_analisedepartamental",
        "payload_func": payload_analisedepartamental,
        "processar_func": None,
        "timeout": 60
    },
    "composicaoDeCustos": { 
        "env_var": "url_composicaoDeCustos",
        "payload_func": payload_composicaoDeCustos,
        "processar_func": None,
        "timeout": 60
    },
    "composicaoEvolucaoDeReceita": { 
        "env_var": "url_composicaoEvolucaoDeReceita",
        "payload_func": payload_composicaoEvolucaoDeReceita,
        "processar_func": None,
        "timeout": 60
    },
    "exercicioOrcamento": { 
        "env_var": "url_exercicioOrcamento",
        "payload_func": payload_exercicioOrcamento,
        "processar_func": None,
        "timeout": 60,
        "method": "GET"
    },
    "custoUnitarioPorPonderacao": { 
        "env_var": "url_custoUnitarioPorPonderacao",
        "payload_func": payload_custoUnitarioPorPonderacao,
        "processar_func": None,
        "timeout": 60
    }
}