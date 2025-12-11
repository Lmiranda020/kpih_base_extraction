import pandas as pd
import chardet
import os

def detectar_tipo_arquivo(caminho_arquivo):
    """
    Detecta se o arquivo é realmente CSV ou Excel (independente da extensão)
    
    Returns:
        'excel' ou 'csv'
    """
    try:
        with open(caminho_arquivo, 'rb') as f:
            # Lê os primeiros bytes (assinatura do arquivo)
            primeiros_bytes = f.read(8)
            
            # Assinaturas de arquivos Excel
            # XLSX: 50 4B 03 04 (ZIP file, pois XLSX é um ZIP)
            # XLS:  D0 CF 11 E0 (OLE2/CFB)
            if primeiros_bytes[:4] == b'PK\x03\x04':  # ZIP = XLSX
                return 'excel'
            elif primeiros_bytes[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':  # XLS
                return 'excel'
            else:
                return 'csv'
    except Exception:
        # Se der erro, assume CSV baseado na extensão
        return 'csv' if caminho_arquivo.lower().endswith('.csv') else 'excel'


def ler_csv_robusto(caminho_arquivo, sep=';', encoding='utf-8-sig'):
    """
    Lê CSV ou Excel de forma robusta, detectando automaticamente o tipo
    
    Args:
        caminho_arquivo: Caminho do arquivo
        sep: Separador (padrão: ';')
        encoding: Encoding (padrão: 'utf-8-sig')
    
    Returns:
        DataFrame ou None em caso de erro
    """
    print(f"📂 Analisando: {caminho_arquivo}")
    
    # Detecta se é realmente CSV ou Excel
    tipo_real = detectar_tipo_arquivo(caminho_arquivo)
    extensao = os.path.splitext(caminho_arquivo)[1].lower()
    
    print(f"   📄 Extensão: {extensao}")
    print(f"   🔍 Tipo real: {tipo_real.upper()}")
    
    if extensao != f'.{tipo_real}':
        print(f"   ⚠️ ATENÇÃO: Arquivo com extensão {extensao} mas conteúdo é {tipo_real.upper()}!")
    
    # Se for Excel (independente da extensão), lê como Excel
    if tipo_real == 'excel':
        print(f"   📊 Lendo como Excel...")
        try:
            df = pd.read_excel(caminho_arquivo, engine='openpyxl')
            print(f"   ✅ Sucesso! {len(df)} linhas, {len(df.columns)} colunas")
            print(f"   📋 Colunas: {', '.join(df.columns[:5].tolist())}{'...' if len(df.columns) > 5 else ''}")
            return df
        except Exception as e:
            print(f"   ❌ Erro ao ler como Excel: {str(e)[:80]}")
            print(f"   🔄 Tentando como CSV...")
    
    # Tenta como CSV
    tentativas = [
        # Tentativa 1: Como foi salvo (sep=';', encoding='utf-8-sig')
        {'sep': ';', 'encoding': 'utf-8-sig', 'engine': 'python'},
        
        # Tentativa 2: UTF-8 sem BOM
        {'sep': ';', 'encoding': 'utf-8', 'engine': 'python'},
        
        # Tentativa 3: Com on_bad_lines='skip' para pular linhas problemáticas
        {'sep': ';', 'encoding': 'utf-8-sig', 'engine': 'python', 'on_bad_lines': 'skip'},
        
        # Tentativa 4: Com quoting para lidar com aspas
        {'sep': ';', 'encoding': 'utf-8-sig', 'engine': 'python', 
         'quoting': 1, 'on_bad_lines': 'skip'},  # QUOTE_MINIMAL
        
        # Tentativa 5: Detectar encoding automaticamente
        {'sep': ';', 'engine': 'python', 'on_bad_lines': 'skip'},
        
        # Tentativa 6: Tentar vírgula como separador
        {'sep': ',', 'encoding': 'utf-8-sig', 'engine': 'python', 'on_bad_lines': 'skip'},
        
        # Tentativa 7: Com escapechar
        {'sep': ';', 'encoding': 'utf-8-sig', 'engine': 'python', 
         'escapechar': '\\', 'on_bad_lines': 'skip'},
        
        # Tentativa 8: Modo mais permissivo (avisa mas não para)
        {'sep': ';', 'encoding': 'utf-8-sig', 'engine': 'python',
         'on_bad_lines': 'warn', 'quotechar': '"', 'doublequote': True}
    ]
    
    # Detecta encoding se necessário
    encoding_detectado = None
    try:
        with open(caminho_arquivo, 'rb') as f:
            resultado = chardet.detect(f.read(100000))
            encoding_detectado = resultado['encoding']
            confianca = resultado['confidence']
            print(f"   🔍 Encoding detectado: {encoding_detectado} (confiança: {confianca:.0%})")
    except Exception:
        pass
    
    for i, kwargs in enumerate(tentativas, 1):
        # Usa encoding detectado na tentativa 5
        if i == 5 and encoding_detectado:
            kwargs['encoding'] = encoding_detectado
        
        try:
            print(f"   Tentativa {i}: sep='{kwargs.get('sep', ';')}' | encoding={kwargs.get('encoding', 'default')}")
            df = pd.read_csv(caminho_arquivo, **kwargs)
            
            if df.empty:
                print(f"   ⚠️ DataFrame vazio")
                continue
            
            if len(df.columns) < 2:
                print(f"   ⚠️ Apenas {len(df.columns)} coluna(s) - provável erro de separador")
                continue
            
            print(f"   ✅ Sucesso! {len(df)} linhas, {len(df.columns)} colunas")
            print(f"   📋 Colunas: {', '.join(df.columns[:5].tolist())}{'...' if len(df.columns) > 5 else ''}")
            return df
            
        except Exception as e:
            print(f"   ❌ Falhou: {str(e)[:80]}")
            continue
    
    print(f"   ⛔ Todas as tentativas falharam")
    return None


def consolidar_dados_api_robusto(
    caminho_novo,
    caminho_anterior,
    colunas_chave,
    caminho_saida,
    nome_api="API"
):
    """
    Consolida dados de duas APIs de forma robusta
    
    Args:
        caminho_novo: Caminho do arquivo do mês atual
        caminho_anterior: Caminho do arquivo do mês anterior
        colunas_chave: Lista de colunas para identificar registros únicos
        caminho_saida: Caminho onde salvar o consolidado
        nome_api: Nome da API (para logs)
    """
    print(f"\n{'='*60}")
    print(f"🔄 Consolidando: {nome_api}")
    print(f"{'='*60}\n")
    
    # Lê arquivo novo
    print("📥 Lendo arquivo do mês atual...")
    df_novo = ler_csv_robusto(caminho_novo, sep=';', encoding='utf-8-sig')
    
    if df_novo is None:
        print(f"❌ Não foi possível ler o arquivo novo: {caminho_novo}")
        return None
    
    # Lê arquivo anterior
    print("\n📥 Lendo arquivo do mês anterior...")
    df_antigo = ler_csv_robusto(caminho_anterior, sep=';', encoding='utf-8-sig')
    
    if df_antigo is None:
        print(f"⚠️ Não foi possível ler o arquivo anterior: {caminho_anterior}")
        print(f"💾 Salvando apenas dados do mês atual...")
        df_novo.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig')
        print(f"✅ Arquivo salvo: {caminho_saida}")
        return caminho_saida
    
    # Valida colunas chave
    colunas_faltantes_novo = [col for col in colunas_chave if col not in df_novo.columns]
    colunas_faltantes_anterior = [col for col in colunas_chave if col not in df_antigo.columns]
    
    if colunas_faltantes_novo:
        print(f"⚠️ Colunas faltantes no arquivo novo: {colunas_faltantes_novo}")
    if colunas_faltantes_anterior:
        print(f"⚠️ Colunas faltantes no arquivo anterior: {colunas_faltantes_anterior}")
    
    # Remove duplicatas antes de concatenar
    print(f"\n🔄 Removendo duplicatas...")
    df_novo_limpo = df_novo.drop_duplicates(subset=colunas_chave, keep='last')
    df_anterior_limpo = df_antigo.drop_duplicates(subset=colunas_chave, keep='last')
    
    duplicatas_novo = len(df_novo) - len(df_novo_limpo)
    duplicatas_anterior = len(df_antigo) - len(df_anterior_limpo)
    
    if duplicatas_novo > 0:
        print(f"   ⚠️ Removidas {duplicatas_novo} duplicata(s) do arquivo novo")
    if duplicatas_anterior > 0:
        print(f"   ⚠️ Removidas {duplicatas_anterior} duplicata(s) do arquivo anterior")
    
    # Concatena
    print(f"\n🔗 Concatenando dados...")
    df_consolidado = pd.concat([df_anterior_limpo, df_novo_limpo], ignore_index=True)
    
    # Remove duplicatas finais (mantém o mais recente = do arquivo novo)
    df_final = df_consolidado.drop_duplicates(subset=colunas_chave, keep='last')
    
    # Estatísticas
    total_registros = len(df_final)
    registros_novo = len(df_novo_limpo)
    registros_anterior = len(df_anterior_limpo)
    registros_unicos = total_registros
    
    print(f"\n📊 Resultado da consolidação:")
    print(f"   Arquivo novo: {registros_novo} registros")
    print(f"   Arquivo anterior: {registros_anterior} registros")
    print(f"   Total após consolidação: {registros_unicos} registros únicos")
    
    # Salva
    try:
        df_final.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig')
        print(f"\n✅ Arquivo consolidado salvo:")
        print(f"   📍 {caminho_saida}")
        print(f"{'='*60}\n")
        return caminho_saida
    except Exception as e:
        print(f"\n❌ Erro ao salvar arquivo consolidado: {e}")
        return None
