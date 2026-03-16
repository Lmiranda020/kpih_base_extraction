"""
resumo_incremental.py

Salva o resumo de execução de forma incremental a cada API concluída,
evitando perda de dados em execuções longas.

Arquivo gerado: resumo_execucao.csv (na pasta da competência)
"""

import os
import csv
from datetime import datetime


CAMPOS_CSV = [
    'api',
    'sucesso',
    'arquivo',
    'erro',
    'registros',
    'inicio',
    'fim',
    'duracao_segundos'
]


class ResumoIncremental:
    """
    Gerencia o salvamento incremental do resumo de execução.
    Assim que uma API termina, o resultado já é persistido em disco.
    """

    def __init__(self, caminho_saida: str, nome_arquivo: str = "resumo_execucao.csv"):
        """
        Args:
            caminho_saida: Pasta onde o arquivo será salvo (pasta da competência)
            nome_arquivo:  Nome do arquivo CSV de resumo
        """
        self.caminho = os.path.join(caminho_saida, nome_arquivo)
        self._garantir_cabecalho()

    # ------------------------------------------------------------------
    # Público
    # ------------------------------------------------------------------

    def registrar(
        self,
        api: str,
        sucesso: bool,
        arquivo: str | None = None,
        erro: str | None = None,
        registros: int = 0,
        inicio: datetime | None = None,
        fim: datetime | None = None,
    ) -> None:
        """
        Acrescenta uma linha ao CSV com o resultado da API.
        Pode ser chamado logo após cada api_xxx() retornar.

        Args:
            api:       Nome da API (ex: "rankingDeCusto")
            sucesso:   True se executou sem erros
            arquivo:   Caminho do arquivo gerado (ou None)
            erro:      Mensagem de erro (ou None)
            registros: Quantidade de registros extraídos
            inicio:    datetime de início da API
            fim:       datetime de fim da API
        """
        fim = fim or datetime.now()
        inicio = inicio or fim

        duracao = round((fim - inicio).total_seconds(), 1)

        linha = {
            'api':               api,
            'sucesso':           'Sim' if sucesso else 'Não',
            'arquivo':           os.path.basename(arquivo) if arquivo else '',
            'erro':              erro or '',
            'registros':         registros,
            'inicio':            inicio.strftime('%Y-%m-%d %H:%M:%S'),
            'fim':               fim.strftime('%Y-%m-%d %H:%M:%S'),
            'duracao_segundos':  duracao,
        }

        with open(self.caminho, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=CAMPOS_CSV, delimiter=';')
            writer.writerow(linha)

        status = "✅" if sucesso else "❌"
        print(f"   {status} [{api}] resumo salvo → {os.path.basename(self.caminho)}")

    # ------------------------------------------------------------------
    # Interno
    # ------------------------------------------------------------------

    def _garantir_cabecalho(self) -> None:
        """Cria o arquivo com cabeçalho se ainda não existir."""
        if not os.path.exists(self.caminho):
            os.makedirs(os.path.dirname(self.caminho), exist_ok=True)
            with open(self.caminho, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=CAMPOS_CSV, delimiter=';')
                writer.writeheader()
            print(f"📄 Resumo incremental criado: {self.caminho}")
        else:
            print(f"📄 Resumo incremental existente (continuando): {self.caminho}")