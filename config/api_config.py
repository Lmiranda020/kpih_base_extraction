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
        "mesReferencia": unidade['mes'],
        "anoReferencia": unidade['ano'],
        "categoriaCusto": "FIXO",
        "incluirDepreciacao": True
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
    "payload_custosIndividualizadoPorCentro": { 
        "env_var": "url_custosIndividualizadoPorCentro",
        "payload_func": payload_custosIndividualizadoPorCentro,
        "processar_func": None,
        "timeout": 60
    }
}