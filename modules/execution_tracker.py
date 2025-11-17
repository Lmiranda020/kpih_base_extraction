"""
Módulo para rastrear execuções de APIs e gerar relatório resumo
"""
import pandas as pd
from datetime import datetime
import os

class ExecutionTracker:
    """Rastreia execuções de APIs e gera relatório final"""
    
    def __init__(self):
        self.execucoes = []
        self.data_inicio = datetime.now()
        
    def registrar_execucao(self, 
                          endpoint,
                          unidade,
                          competencia,
                          status,
                          registros=0,
                          erro=None,
                          tempo_execucao=None):
        """
        Registra uma execução individual
        
        Args:
            endpoint: Nome do endpoint/API
            unidade: Nome da unidade
            competencia: Competência processada
            status: 'sucesso', 'erro', 'timeout', 'sem_dados'
            registros: Quantidade de registros extraídos
            erro: Mensagem de erro (se houver)
            tempo_execucao: Tempo de execução em segundos
        """

        # Formata o tempo de execução
        tempo_formatado = round(tempo_execucao, 2) if tempo_execucao else 0.0

        self.execucoes.append({
            'data_hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'endpoint': endpoint,
            'unidade': unidade,
            'competencia': competencia,
            'status': status,
            'registros': registros,
            'erro': erro if erro else '',
            'tempo_execucao_s': tempo_formatado
        })
    
    def gerar_relatorio(self, caminho_destino):
        """
        Gera relatório resumo em CSV e TXT
        
        Args:
            caminho_destino: Diretório onde salvar os relatórios
            
        Returns:
            tuple: (caminho_csv, caminho_txt)
        """
        if not self.execucoes:
            print("⚠️ Nenhuma execução registrada para gerar relatório")
            return None, None
        
        data_fim = datetime.now()
        duracao_total = (data_fim - self.data_inicio).total_seconds()
        
        # Cria DataFrame com todas as execuções
        df = pd.DataFrame(self.execucoes)
        
        df['tempo_execucao_s'] = df['tempo_execucao_s'].round(2)

        # Calcula estatísticas
        total_execucoes = len(df)
        sucessos = len(df[df['status'] == 'sucesso'])
        erros = len(df[df['status'] == 'erro'])
        timeouts = len(df[df['status'] == 'timeout'])
        sem_dados = len(df[df['status'] == 'sem_dados'])
        total_registros = df['registros'].sum()
        
        # Estatísticas por endpoint
        stats_por_endpoint = df.groupby('endpoint').agg({
            'status': lambda x: (x == 'sucesso').sum(),
            'registros': 'sum',
            'unidade': 'count',
            'tempo_execucao_s': 'mean'  # Tempo médio
        }).rename(columns={
            'status': 'sucessos',
            'registros': 'total_registros',
            'unidade': 'total_unidades',
            'tempo_execucao_s': 'tempo_medio_s'
        })
        
        # Nome dos arquivos
        timestamp = self.data_inicio.strftime('%Y%m%d_%H%M%S')
        nome_csv = f"relatorio_execucao_{timestamp}.csv"
        nome_txt = f"relatorio_execucao_{timestamp}.txt"
        
        caminho_csv = os.path.join(caminho_destino, nome_csv)
        caminho_txt = os.path.join(caminho_destino, nome_txt)
        
        # Salva CSV detalhado
        df.to_csv(caminho_csv, index=False, sep=';', encoding='utf-8-sig')
        
        # Gera relatório TXT formatado
        with open(caminho_txt, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("RELATÓRIO DE EXECUÇÃO - EXTRAÇÃO DE DADOS APIs\n")
            f.write("="*80 + "\n\n")
            
            # Informações gerais
            f.write("📅 INFORMAÇÕES GERAIS\n")
            f.write("-"*80 + "\n")
            f.write(f"Data/Hora Início: {self.data_inicio.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Data/Hora Fim:    {data_fim.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Duração Total:    {duracao_total:.2f} segundos ({duracao_total/60:.2f} minutos)\n")
            f.write(f"Total de Execuções: {total_execucoes}\n\n")
            
            # Resumo de status
            f.write("📊 RESUMO DE STATUS\n")
            f.write("-"*80 + "\n")
            f.write(f"✅ Sucessos:        {sucessos} ({sucessos/total_execucoes*100:.1f}%)\n")
            f.write(f"❌ Erros:           {erros} ({erros/total_execucoes*100:.1f}%)\n")
            f.write(f"⏱️  Timeouts:        {timeouts} ({timeouts/total_execucoes*100:.1f}%)\n")
            f.write(f"⚠️  Sem Dados:       {sem_dados} ({sem_dados/total_execucoes*100:.1f}%)\n")
            f.write(f"📈 Total Registros: {total_registros:,}\n\n")
            
            # Estatísticas por endpoint
            f.write("🔌 ESTATÍSTICAS POR ENDPOINT\n")
            f.write("-"*80 + "\n")
            for endpoint in stats_por_endpoint.index:
                stats = stats_por_endpoint.loc[endpoint]
                f.write(f"\n{endpoint}:\n")
                f.write(f"  • Unidades processadas: {stats['total_unidades']}\n")
                f.write(f"  • Sucessos: {stats['sucessos']}\n")
                f.write(f"  • Registros extraídos: {stats['total_registros']:,}\n")
                f.write(f"  • Tempo médio: {stats['tempo_medio_s']:.2f}s\n")
                taxa_sucesso = (stats['sucessos'] / stats['total_unidades'] * 100) if stats['total_unidades'] > 0 else 0
                f.write(f"  • Taxa de sucesso: {taxa_sucesso:.1f}%\n")
            
            # Lista de erros (se houver)
            df_erros = df[df['status'].isin(['erro', 'timeout'])]
            if not df_erros.empty:
                f.write("\n" + "="*80 + "\n")
                f.write("❌ DETALHAMENTO DE ERROS\n")
                f.write("-"*80 + "\n\n")
                
                for _, row in df_erros.iterrows():
                    f.write(f"Endpoint:    {row['endpoint']}\n")
                    f.write(f"Unidade:     {row['unidade']}\n")
                    f.write(f"Competência: {row['competencia']}\n")
                    f.write(f"Status:      {row['status']}\n")
                    f.write(f"Erro:        {row['erro']}\n")
                    f.write(f"Data/Hora:   {row['data_hora']}\n")
                    f.write("-"*80 + "\n\n")
            
            # Lista de execuções sem dados
            df_sem_dados = df[df['status'] == 'sem_dados']
            if not df_sem_dados.empty:
                f.write("⚠️ EXECUÇÕES SEM DADOS RETORNADOS\n")
                f.write("-"*80 + "\n\n")
                
                for _, row in df_sem_dados.iterrows():
                    f.write(f"{row['endpoint']} | {row['unidade']} | {row['competencia']}\n")
                
                f.write("\n")
            
            f.write("="*80 + "\n")
            f.write("FIM DO RELATÓRIO\n")
            f.write("="*80 + "\n")
        
        return caminho_csv, caminho_txt
    
    def obter_resumo(self):
        """
        Retorna resumo rápido das execuções
        
        Returns:
            dict: Dicionário com estatísticas resumidas
        """
        if not self.execucoes:
            return {
                'total': 0,
                'sucessos': 0,
                'erros': 0,
                'timeouts': 0,
                'sem_dados': 0,
                'total_registros': 0
            }
        
        df = pd.DataFrame(self.execucoes)
        
        return {
            'total': len(df),
            'sucessos': len(df[df['status'] == 'sucesso']),
            'erros': len(df[df['status'] == 'erro']),
            'timeouts': len(df[df['status'] == 'timeout']),
            'sem_dados': len(df[df['status'] == 'sem_dados']),
            'total_registros': df['registros'].sum(),
            'endpoints': df['endpoint'].unique().tolist()
        }