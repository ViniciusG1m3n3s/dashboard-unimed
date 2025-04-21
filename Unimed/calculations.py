import pandas as pd
import os
import plotly.express as px
import math
import streamlit as st
from io import BytesIO
import requests
import base64
from datetime import timedelta

def load_data(usuario: str) -> pd.DataFrame:
    """
    Carrega os dados de um arquivo .parquet baseado no nome do usuário.
    Se o arquivo não existir, cria um novo com colunas padrão.
    """
    parquet_file = f'dados_acumulados_{usuario}.parquet'

    try:
        if os.path.exists(parquet_file):
            df_total = pd.read_parquet(parquet_file)

            if 'Justificativa' not in df_total.columns:
                df_total['Justificativa'] = ""
        else:
            raise FileNotFoundError

    except (FileNotFoundError, ValueError, OSError):
        st.toast("📁 Arquivo não encontrado ou corrompido. Criando novo arquivo vazio.")
        df_total = pd.DataFrame(columns=[
            'NÚMERO DO PROTOCOLO',
            'USUÁRIO QUE CONCLUIU A TAREFA',
            'SITUAÇÃO DA TAREFA',
            'TEMPO MÉDIO OPERACIONAL',
            'DATA DE CONCLUSÃO DA TAREFA',
            'FINALIZAÇÃO',
            'Justificativa'
        ])
        df_total.to_parquet(parquet_file, index=False)

    return df_total

def save_data(df: pd.DataFrame, usuario: str) -> bool:
    """
    Salva os dados do DataFrame localmente em formato Parquet, remove robôs e duplicatas reais
    (comparadas com o histórico) e realiza upload para o GitHub.
    """
    parquet_file = f'dados_acumulados_{usuario}.parquet'

    if 'Justificativa' not in df.columns:
        df['Justificativa'] = ""

    df = df.drop(columns=['Nº DA OAB'], errors='ignore')

    # Remove registros de robôs
    if 'USUÁRIO QUE CONCLUIU A TAREFA' in df.columns:
        df = df[
            df['USUÁRIO QUE CONCLUIU A TAREFA'].notna() &
            ~df['USUÁRIO QUE CONCLUIU A TAREFA'].str.lower().isin(['robohub_amil', 'robohub_uni'])
        ]

    # Carrega dados existentes
    df_existente = load_data(usuario)

    # Verifica duplicatas reais (dados do upload que já existem no histórico)
    duplicatas = pd.merge(df, df_existente, how='inner')
    if not duplicatas.empty:
        st.toast(f"🧹 {len(duplicatas)} linha(s) do novo arquivo já existiam no histórico e foram ignoradas.")

    # Junta os dados e remove duplicatas completas
    df_total = pd.concat([df_existente, df], ignore_index=True)
    df_total.drop_duplicates(keep='first', inplace=True)

    # 🔧 Conversão leve de colunas específicas que podem gerar erro no Parquet
    colunas_problema = ['CÓDIGO DO BENEFICIÁRIO', 'CPF', 'CNPJ', 'ID PROJURIS']
    for col in colunas_problema:
        if col in df_total.columns:
            df_total[col] = df_total[col].astype(str)

    # Salva o arquivo final
    df_total.to_parquet(parquet_file, index=False)
    st.toast(f"💾 Arquivo '{parquet_file}' salvo localmente com sucesso.")

    # Upload para GitHub
    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("GITHUB_REPO")
    branch = os.getenv("GITHUB_BRANCH", "main")

    if not all([token, repo]):
        st.toast("⚠️ Token ou repositório GitHub não configurados. Upload não realizado.")
        return False

    api_url = f"https://api.github.com/repos/{repo}/contents/{parquet_file}"

    try:
        with open(parquet_file, "rb") as f:
            content = base64.b64encode(f.read()).decode()

        response = requests.get(api_url, headers={"Authorization": f"token {token}"})
        sha = response.json().get("sha") if response.status_code == 200 else None

        payload = {
            "message": f"Atualizando {parquet_file}",
            "content": content,
            "branch": branch
        }
        if sha:
            payload["sha"] = sha

        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }

        r = requests.put(api_url, json=payload, headers=headers)

        if r.status_code in [200, 201]:
            st.toast(f"✅ Arquivo '{parquet_file}' atualizado com sucesso no GitHub.")
            return True
        else:
            st.toast(f"❌ Erro ao atualizar o GitHub: {r.text}")
            return False

    except Exception as e:
        st.toast(f"❌ Erro inesperado ao enviar para o GitHub: {e}")
        return False

def calcular_tmo_por_dia(df: pd.DataFrame) -> pd.DataFrame:
    df['Dia'] = pd.to_datetime(df['DATA DE CONCLUSÃO DA TAREFA']).dt.date
    df['SITUAÇÃO DA TAREFA'] = df['SITUAÇÃO DA TAREFA'].str.upper()
    df['TEMPO MÉDIO OPERACIONAL'] = pd.to_timedelta(df['TEMPO MÉDIO OPERACIONAL'], errors='coerce')

    df_finalizados = df[df['SITUAÇÃO DA TAREFA'].isin(['FINALIZADA', 'CANCELADA'])].copy()

    df_tmo = df_finalizados.groupby('Dia').agg(
        Tempo_Total=('TEMPO MÉDIO OPERACIONAL', 'sum'),
        Total_Finalizados_Cancelados=('SITUAÇÃO DA TAREFA', 'count')
    ).reset_index()

    df_tmo['TMO'] = df_tmo['Tempo_Total'] / df_tmo['Total_Finalizados_Cancelados']

    # Cria coluna formatada separada
    df_tmo['TMO_Formatado'] = df_tmo['TMO'].apply(format_timedelta)

    return df_tmo[['Dia', 'TMO', 'TMO_Formatado']]

def calcular_tmo_por_dia_geral(df):
    # Certifica-se de que a coluna de data está no formato correto
    df['Dia'] = pd.to_datetime(df['DATA DE CONCLUSÃO DA TAREFA']).dt.date

    # Filtra tarefas finalizadas ou canceladas, pois estas são relevantes para o cálculo do TMO
    df_finalizados = df[df['SITUAÇÃO DA TAREFA'].isin(['Finalizado', 'Cancelada'])].copy()
    
    # Agrupamento por dia para calcular o tempo médio diário
    df_tmo = df_finalizados.groupby('Dia').agg(
        Tempo_Total=('TEMPO MÉDIO OPERACIONAL', 'sum'),  # Soma total do tempo por dia
        Total_Finalizados_Cancelados=('SITUAÇÃO DA TAREFA', 'count')  # Total de tarefas finalizadas/canceladas por dia
    ).reset_index()

    # Calcula o TMO (Tempo Médio Operacional) diário
    df_tmo['TMO'] = df_tmo['Tempo_Total'] / df_tmo['Total_Finalizados_Cancelados']
    
    # Remove valores nulos e formata o tempo médio para o gráfico
    df_tmo['TMO'] = df_tmo['TMO'].fillna(pd.Timedelta(seconds=0))  # Preenche com zero se houver NaN
    df_tmo['TMO_Formatado'] = df_tmo['TMO'].apply(format_timedelta)  # Formata para exibição
    
    return df_tmo[['Dia', 'TMO', 'TMO_Formatado']]

def calcular_produtividade_diaria(df):
    # Garante que a coluna 'Próximo' esteja em formato de data
    df['Dia'] = df['DATA DE CONCLUSÃO DA TAREFA'].dt.date

    # Agrupa e soma os status para calcular a produtividade
    df_produtividade = df.groupby('Dia').agg(
        Finalizado=('FINALIZAÇÃO', 'count'),
    ).reset_index()

    # Calcula a produtividade total
    df_produtividade['Produtividade'] = + df_produtividade['Finalizado'] 
    return df_produtividade

def calcular_produtividade_diaria_cadastro(df):
    # Garante que a coluna 'Próximo' esteja em formato de data
    df['Dia'] = df['DATA DE CONCLUSÃO DA TAREFA'].dt.date

    # Agrupa e soma os status para calcular a produtividade
    df_produtividade_cadastro = df.groupby('Dia').agg(
        Finalizado=('FINALIZAÇÃO', lambda x: x[x == 'Cadastro realizado'].count()),
    ).reset_index()

    # Calcula a produtividade total
    df_produtividade_cadastro['Produtividade'] = + df_produtividade_cadastro['Finalizado']
    return df_produtividade_cadastro

def calcular_produtividade_diaria_subsidios(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a produtividade diária apenas para as filas que contenham 'Elaborar Subsídios'.
    A contagem considera tarefas finalizadas com qualquer status.
    """

    # Garante que a coluna de data esteja corretamente formatada
    df['Dia'] = pd.to_datetime(df['DATA DE CONCLUSÃO DA TAREFA']).dt.date

    # Filtra apenas as filas de subsídios
    df_subsidios = df[df['FILA'].str.contains(r'Elaborar\s+(Subsídios|Subsidios)', case=False, na=False, regex=True)]

    # Filtra tarefas finalizadas
    df_subsidios = df_subsidios[df_subsidios['SITUAÇÃO DA TAREFA'].str.upper() == 'FINALIZADA']

    # Agrupa por dia e conta quantas tarefas finalizadas houve
    df_produtividade_subsidios = df_subsidios.groupby('Dia').size().reset_index(name='Produtividade')

    return df_produtividade_subsidios

def convert_to_timedelta_for_calculations(df):
    df['TEMPO MÉDIO OPERACIONAL'] = pd.to_timedelta(df['TEMPO MÉDIO OPERACIONAL'], errors='coerce')
    return df

def convert_to_datetime_for_calculations(df):
    df['DATA DE CONCLUSÃO DA TAREFA'] = pd.to_datetime(df['DATA DE CONCLUSÃO DA TAREFA'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    return df
        
def format_timedelta(td):
    if pd.isnull(td):
        return "0 min"
    total_seconds = int(td.total_seconds())
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes} min {seconds}s"

def format_time_delta_hms(td):
    if pd.isnull(td):
        return "00:00:00"
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def format_timedelta_grafico_tmo(td):
    if pd.isnull(td):
        return "00:00:00"
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

# Função para calcular o TMO por analista
def calcular_tmo_por_dia(df):
    df['Dia'] = pd.to_datetime(df['DATA DE CONCLUSÃO DA TAREFA']).dt.date
    df_finalizados = df[df['SITUAÇÃO DA TAREFA'].isin(['Finalizada', 'Cancelada'])].copy()
    
    # Agrupando por dia
    df_tmo = df_finalizados.groupby('Dia').agg(
        Tempo_Total=('TEMPO MÉDIO OPERACIONAL', 'sum'),  # Soma total do tempo
        Total_Finalizados_Cancelados=('SITUAÇÃO DA TAREFA', 'count')  # Total de tarefas finalizadas ou canceladas
    ).reset_index()

    # Calcula o TMO (Tempo Médio Operacional)
    df_tmo['TMO'] = df_tmo['Tempo_Total'] / df_tmo['Total_Finalizados_Cancelados']
    
    # Formata o tempo médio no formato HH:MM:SS
    df_tmo['TMO'] = df_tmo['TMO'].apply(format_timedelta)
    return df_tmo[['Dia', 'TMO']]

def calcular_tmo_por_dia_cadastro(df):
    df['Dia'] = pd.to_datetime(df['DATA DE CONCLUSÃO DA TAREFA']).dt.date
    df_finalizados_cadastro = df[df['FINALIZAÇÃO'] == 'CADASTRADO'].copy()
    
    # Agrupando por dia
    df_tmo_cadastro = df_finalizados_cadastro.groupby('Dia').agg(
        Tempo_Total=('TEMPO MÉDIO OPERACIONAL', 'sum'),  # Soma total do tempo
        Total_Finalizados_Cancelados=('FINALIZAÇÃO', 'count')  # Total de tarefas finalizadas ou canceladas
    ).reset_index()

    # Calcula o TMO (Tempo Médio Operacional)
    df_tmo_cadastro['TMO'] = df_tmo_cadastro['Tempo_Total'] / df_tmo_cadastro['Total_Finalizados_Cancelados']
    
    # Formata o tempo médio no formato HH:MM:SS
    df_tmo_cadastro['TMO'] = df_tmo_cadastro['TMO'].apply(format_timedelta)
    return df_tmo_cadastro[['Dia', 'TMO']]

# Função para calcular o TMO por analista
def calcular_tmo(df):
    # Verifica se a coluna 'SITUAÇÃO DA TAREFA' existe no DataFrame
    if 'SITUAÇÃO DA TAREFA' not in df.columns:
        raise KeyError("A coluna 'SITUAÇÃO DA TAREFA' não foi encontrada no DataFrame.")

    # Filtra as tarefas finalizadas ou canceladas
    df_finalizados = df[df['SITUAÇÃO DA TAREFA'].isin(['Finalizada', 'Cancelada'])].copy()

    # Verifica se a coluna 'TEMPO MÉDIO OPERACIONAL' existe e converte para minutos
    if 'TEMPO MÉDIO OPERACIONAL' not in df_finalizados.columns:
        raise KeyError("A coluna 'TEMPO MÉDIO OPERACIONAL' não foi encontrada no DataFrame.")
    df_finalizados['TEMPO_MÉDIO_MINUTOS'] = df_finalizados['TEMPO MÉDIO OPERACIONAL'].dt.total_seconds() / 60

    # Verifica se a coluna 'FILA' existe antes de aplicar o filtro
    if 'FILA' in df_finalizados.columns:
        # Remove protocolos da fila "DÚVIDA" com mais de 1 hora de tempo médio
        df_finalizados = df_finalizados[~((df_finalizados['FILA'] == 'DÚVIDA') & (df_finalizados['TEMPO_MÉDIO_MINUTOS'] > 60))]

    # Agrupando por analista
    df_tmo_analista = df_finalizados.groupby('USUÁRIO QUE CONCLUIU A TAREFA').agg(
        Tempo_Total=('TEMPO MÉDIO OPERACIONAL', lambda x: x[df_finalizados['FINALIZAÇÃO'] == 'CADASTRADO'].sum()),  # Soma total do tempo das tarefas com finalização CADASTRADO
        Total_Tarefas=('FINALIZAÇÃO', lambda x: x[x == 'CADASTRADO'].count())  # Total de tarefas finalizadas ou canceladas por analista
    ).reset_index()

    # Calcula o TMO (Tempo Médio Operacional) como média
    df_tmo_analista['TMO'] = df_tmo_analista['Tempo_Total'] / df_tmo_analista['Total_Tarefas']

    # Formata o tempo médio no formato de minutos e segundos
    df_tmo_analista['TMO_Formatado'] = df_tmo_analista['TMO'].apply(format_timedelta_grafico_tmo)

    return df_tmo_analista[['USUÁRIO QUE CONCLUIU A TAREFA', 'TMO_Formatado', 'TMO']]

# Função para calcular o ranking dinâmico
def calcular_ranking(df_total, selected_users):
    # Filtra o DataFrame com os usuários selecionados
    df_filtered = df_total[df_total['USUÁRIO QUE CONCLUIU A TAREFA'].isin(selected_users)]

    df_ranking = df_filtered.groupby('USUÁRIO QUE CONCLUIU A TAREFA').agg(

        Finalizado=('FINALIZAÇÃO', lambda x: x[x == 'CADASTRADO'].count()),
        Distribuido=('FINALIZAÇÃO', lambda x: x[x == 'REALIZADO'].count()),
        Atualizado=('FINALIZAÇÃO', lambda x: x[x == 'ATUALIZADO'].count())
    ).reset_index()
    df_ranking['Total'] =df_ranking['Finalizado'] + df_ranking['Distribuido'] + df_ranking['Atualizado']
    df_ranking = df_ranking.sort_values(by  ='Total', ascending=False).reset_index(drop=True)
    df_ranking.index += 1
    df_ranking.index.name = 'Posição'

    # Define o tamanho dos quartis
    num_analistas = len(df_ranking)
    quartil_size = 4 if num_analistas > 12 else math.ceil(num_analistas / 4)

    # Função de estilo para os quartis dinâmicos
    def apply_dynamic_quartile_styles(row):
        if row.name <= quartil_size:
            color = 'rgba(135, 206, 250, 0.4)'  # Azul vibrante translúcido (primeiro quartil)
        elif quartil_size < row.name <= 2 * quartil_size:
            color = 'rgba(144, 238, 144, 0.4)'  # Verde vibrante translúcido (segundo quartil)
        elif 2 * quartil_size < row.name <= 3 * quartil_size:
            color = 'rgba(255, 255, 102, 0.4)'  # Amarelo vibrante translúcido (terceiro quartil)
        else:
            color = 'rgba(255, 99, 132, 0.4)'  # Vermelho vibrante translúcido (quarto quartil)
        return ['background-color: {}'.format(color) for _ in row]

    # Aplicar os estilos e retornar o DataFrame
    styled_df_ranking = df_ranking.style.apply(apply_dynamic_quartile_styles, axis=1).format(
        {'Andamento': '{:.0f}', 'Finalizado': '{:.0f}', 'Reclassificado': '{:.0f}', 'Total': '{:.0f}'}
    )

    return styled_df_ranking

#MÉTRICAS INDIVIDUAIS
def calcular_metrica_analista(df_analista):
    # Verifica se as colunas necessárias estão presentes no DataFrame
    colunas_necessarias = ['FILA', 'FINALIZAÇÃO', 'TEMPO MÉDIO OPERACIONAL', 'DATA DE CONCLUSÃO DA TAREFA']
    for coluna in colunas_necessarias:
        if coluna not in df_analista.columns:
            st.warning(f"A coluna '{coluna}' não está disponível nos dados. Verifique o arquivo carregado.")
            return None, None, None, None, None, None, None  # Atualizado para retornar sete valores

    # Excluir os registros com "FILA" como "Desconhecida"
    df_analista_filtrado = df_analista[df_analista['FILA'] != "Desconhecida"]

    # Filtrar os registros com status "CADASTRADO", "ATUALIZADO" e "REALIZADO"
    df_filtrados = df_analista_filtrado[df_analista_filtrado['SITUAÇÃO DA TAREFA'].isin(['Finalizada'])]

    # Converter "TEMPO MÉDIO OPERACIONAL" para minutos
    df_filtrados['TEMPO_MÉDIO_MINUTOS'] = df_filtrados['TEMPO MÉDIO OPERACIONAL'].dt.total_seconds() / 60

    # Excluir registros da fila "DÚVIDA" com tempo médio superior a 1 hora
    df_filtrados = df_filtrados[~((df_filtrados['FILA'] == 'DÚVIDA') & (df_filtrados['TEMPO_MÉDIO_MINUTOS'] > 60))]

    # Calcula totais conforme os filtros de status
    total_finalizados = len(df_filtrados[df_filtrados['FINALIZAÇÃO'] != 'FORA DO ESCOPO'])
    total_realizados = len(df_filtrados[df_filtrados['FINALIZAÇÃO'] == 'REALIZADO'])
    total_atualizado = len(df_filtrados[df_filtrados['FINALIZAÇÃO'] == 'ATUALIZADO'])

    # Calcula o tempo total para cada tipo de tarefa
    tempo_total_cadastrado = df_filtrados[df_filtrados['FINALIZAÇÃO'] != 'FORA DO ESCOPO']['TEMPO MÉDIO OPERACIONAL'].sum()
    tempo_total_atualizado = df_filtrados[df_filtrados['FINALIZAÇÃO'] == 'ATUALIZADO']['TEMPO MÉDIO OPERACIONAL'].sum()
    tempo_total_realizado = df_filtrados[df_filtrados['FINALIZAÇÃO'] == 'REALIZADO']['TEMPO MÉDIO OPERACIONAL'].sum()

    # Calcula o tempo médio para cada tipo de tarefa
    tmo_cadastrado = tempo_total_cadastrado / total_finalizados if total_finalizados > 0 else pd.Timedelta(0)
    tmo_atualizado = tempo_total_atualizado / total_atualizado if total_atualizado > 0 else pd.Timedelta(0)

    # Calcula o tempo médio geral considerando todas as tarefas
    tempo_total_analista = tempo_total_cadastrado + tempo_total_atualizado + tempo_total_realizado
    total_tarefas = total_finalizados + total_atualizado + total_realizados
    tempo_medio_analista = tempo_total_analista / total_tarefas if total_tarefas > 0 else pd.Timedelta(0)

    # Calcular a média de cadastros por dias trabalhados
    dias_trabalhados = df_filtrados[df_filtrados['FINALIZAÇÃO'] == 'CADASTRADO']['DATA DE CONCLUSÃO DA TAREFA'].dt.date.nunique()
    media_cadastros_por_dia = int(total_finalizados / dias_trabalhados) if dias_trabalhados > 0 else 0

    return total_finalizados, total_atualizado, tempo_medio_analista, tmo_cadastrado, tmo_atualizado, total_realizados, media_cadastros_por_dia, dias_trabalhados

def calcular_tempo_ocioso_por_analista(df):
    try:
        # Converte as colunas de data para datetime, tratando erros
        df['DATA DE INÍCIO DA TAREFA'] = pd.to_datetime(
            df['DATA DE INÍCIO DA TAREFA'], format='%d/%m/%Y %H:%M:%S', errors='coerce'
        )
        df['DATA DE CONCLUSÃO DA TAREFA'] = pd.to_datetime(
            df['DATA DE CONCLUSÃO DA TAREFA'], format='%d/%m/%Y %H:%M:%S', errors='coerce'
        )

        # Filtrar valores nulos e resetar index
        df = df.dropna(subset=['DATA DE INÍCIO DA TAREFA', 'DATA DE CONCLUSÃO DA TAREFA']).reset_index(drop=True)

        # Ordena os dados por usuário e data de início da tarefa (sem considerar fila)
        df = df.sort_values(by=['USUÁRIO QUE CONCLUIU A TAREFA', 'DATA DE INÍCIO DA TAREFA']).reset_index(drop=True)

        # Calcula o próximo horário de início da tarefa por usuário (sem considerar fila)
        df['PRÓXIMA_TAREFA'] = df.groupby(['USUÁRIO QUE CONCLUIU A TAREFA'])['DATA DE INÍCIO DA TAREFA'].shift(-1)

        # Calcula o tempo ocioso entre a conclusão de uma tarefa e o início da próxima
        df['TEMPO OCIOSO'] = df['PRÓXIMA_TAREFA'] - df['DATA DE CONCLUSÃO DA TAREFA']

        # Remove valores negativos ou muito grandes (exemplo: trocas de turno)
        df['TEMPO OCIOSO'] = df['TEMPO OCIOSO'].apply(lambda x: x if pd.notnull(x) and pd.Timedelta(0) < x <= pd.Timedelta(hours=1) else pd.Timedelta(0))

        # Agrupa os tempos ociosos por usuário e dia de conclusão, somando todas as filas
        df_soma_ocioso = df.groupby(['USUÁRIO QUE CONCLUIU A TAREFA', df['DATA DE CONCLUSÃO DA TAREFA'].dt.date])['TEMPO OCIOSO'].sum().reset_index()

        # Renomeia colunas para melhor entendimento
        df_soma_ocioso = df_soma_ocioso.rename(columns={
            'DATA DE CONCLUSÃO DA TAREFA': 'Data',
            'TEMPO OCIOSO': 'Tempo Ocioso'
        })

        # Formata o tempo ocioso total como string
        df_soma_ocioso['Tempo Ocioso'] = df_soma_ocioso['Tempo Ocioso'].astype(str).str.split("days").str[-1].str.strip()

        return df_soma_ocioso[['USUÁRIO QUE CONCLUIU A TAREFA', 'Data', 'Tempo Ocioso']]

    except Exception as e:
        return pd.DataFrame({'Erro': [f'Erro: {str(e)}']})

def format_timedelta_hms(td):
    """ Formata um timedelta para HH:MM:SS """
    if pd.isnull(td):
        return "00:00:00"
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def exibir_grafico_tempo_ocioso_por_dia(df_analista, analista_selecionado, custom_colors, st):
    """
    Gera e exibe um gráfico de barras com o Tempo Ocioso diário para um analista específico.

    Parâmetros:
        - df_analista: DataFrame contendo os dados de análise.
        - analista_selecionado: Nome do analista selecionado.
        - custom_colors: Lista de cores personalizadas para o gráfico.
        - st: Referência para o módulo Streamlit (necessário para exibir os resultados).
    """

    # Calcular o tempo ocioso diário por analista
    df_ocioso = calcular_tempo_ocioso_por_analista(df_analista)

    # Converter a coluna 'Tempo Ocioso' para timedelta para cálculos
    df_ocioso['Tempo Ocioso'] = pd.to_timedelta(df_ocioso['Tempo Ocioso'], errors='coerce')

    # Filtrar apenas o analista selecionado
    df_ocioso = df_ocioso[df_ocioso['USUÁRIO QUE CONCLUIU A TAREFA'] == analista_selecionado]

    # Determinar o período disponível no dataset
    data_minima = df_ocioso['Data'].min()
    data_maxima = df_ocioso['Data'].max()

    # Criar um slider interativo para seleção de período
    periodo_selecionado = st.slider(
        "Selecione o período",
        min_value=data_minima,
        max_value=data_maxima,
        value=(data_maxima - pd.Timedelta(days=30), data_maxima),  # Últimos 30 dias por padrão
        format="DD MMM YYYY"  # Formato: Dia Mês Ano (Exemplo: 01 Jan 2025)
    )

    # Filtrar os dados com base no período selecionado
    df_ocioso = df_ocioso[
        (df_ocioso['Data'] >= periodo_selecionado[0]) &
        (df_ocioso['Data'] <= periodo_selecionado[1])
    ]

    # Formatar a coluna 'Tempo Ocioso' para exibição no gráfico como HH:MM:SS
    df_ocioso['Tempo Ocioso Formatado'] = df_ocioso['Tempo Ocioso'].apply(format_timedelta_hms)

    # Converter tempo ocioso para total de segundos (para exibição correta no gráfico)
    df_ocioso['Tempo Ocioso Segundos'] = df_ocioso['Tempo Ocioso'].dt.total_seconds()

    # Criar o gráfico de barras
    fig_ocioso = px.bar(
        df_ocioso, 
        x='Data', 
        y='Tempo Ocioso Segundos', 
        labels={'Tempo Ocioso Segundos': 'Tempo Ocioso (HH:MM:SS)', 'Data': 'Data'},
        text=df_ocioso['Tempo Ocioso Formatado'],  # Exibir tempo formatado nas barras
        color_discrete_sequence=custom_colors
    )

    # Ajuste do layout
    fig_ocioso.update_layout(
        xaxis=dict(
            tickvals=df_ocioso['Data'],
            ticktext=[f"{dia.day} {dia.strftime('%b')} {dia.year}" for dia in df_ocioso['Data']],
            title='Data'
        ),
        yaxis=dict(
            title='Tempo Ocioso (HH:MM:SS)',
            tickvals=[i * 3600 for i in range(0, int(df_ocioso['Tempo Ocioso Segundos'].max() // 3600) + 1)],
            ticktext=[format_timedelta_hms(pd.Timedelta(seconds=i * 3600)) for i in range(0, int(df_ocioso['Tempo Ocioso Segundos'].max() // 3600) + 1)]
        ),
        bargap=0.2  # Espaçamento entre as barras
    )

    # Personalizar o gráfico
    fig_ocioso.update_traces(
        hovertemplate='Data = %{x}<br>Tempo Ocioso = %{text}',  # Formato do hover
        textposition="outside",  # Exibir rótulos acima das barras
        textfont_color='white'  # Define a cor do texto como branco
    )

    # Exibir o gráfico na dashboard
    st.plotly_chart(fig_ocioso, use_container_width=True)

def calcular_tmo_equipe_cadastro(df_total):
    return df_total[df_total['FINALIZAÇÃO'].isin(['CADASTRADO'])]['TEMPO MÉDIO OPERACIONAL'].mean()

def calcular_tmo_equipe_atualizado(df_total):
    return df_total[df_total['FINALIZAÇÃO'].isin(['ATUALIZADO'])]['TEMPO MÉDIO OPERACIONAL'].mean()

def calcular_filas_analista(df_analista):
    if 'Carteira' in df_analista.columns:
        # Filtra apenas os status relevantes para o cálculo (considerando FINALIZADO e RECLASSIFICADO)
        filas_finalizadas_analista = df_analista[
            df_analista['Status'].isin(['FINALIZADO', 'RECLASSIFICADO', 'ANDAMENTO_PRE'])
        ]
        
        # Agrupa por 'Carteira' e calcula a quantidade de FINALIZADO, RECLASSIFICADO e ANDAMENTO_PRE para cada fila
        carteiras_analista = filas_finalizadas_analista.groupby('Carteira').agg(
            Finalizados=('Status', lambda x: (x == 'FINALIZADO').sum()),
            Reclassificados=('Status', lambda x: (x == 'RECLASSIFICADO').sum()),
            Andamento=('Status', lambda x: (x == 'ANDAMENTO_PRE').sum()),
            TMO_médio=('Tempo de Análise', lambda x: x[x.index.isin(df_analista[(df_analista['Status'].isin(['FINALIZADO', 'RECLASSIFICADO']))].index)].mean())
        ).reset_index()

        # Converte o TMO médio para minutos e segundos
        carteiras_analista['TMO_médio'] = carteiras_analista['TMO_médio'].apply(format_timedelta)

        # Renomeia as colunas para exibição
        carteiras_analista = carteiras_analista.rename(
            columns={'Carteira': 'Fila', 'Finalizados': 'Finalizados', 'Reclassificados': 'Reclassificados', 'Andamento': 'Andamento', 'TMO_médio': 'TMO Médio por Fila'}
        )
        
        return carteiras_analista  # Retorna o DataFrame
    
    else:
        # Caso a coluna 'Carteira' não exista
        return pd.DataFrame({'Fila': [], 'Finalizados': [], 'Reclassificados': [], 'Andamento': [], 'TMO Médio por Fila': []})
    


def calcular_tmo_por_dia(df_analista):
    # Filtrar apenas as tarefas com finalização "CADASTRADO"
    df_analista = df_analista[df_analista['FINALIZAÇÃO'] == 'CADASTRADO']
    df_analista['Dia'] = df_analista['DATA DE CONCLUSÃO DA TAREFA'].dt.date
    tmo_por_dia = df_analista.groupby('Dia').agg(TMO=('TEMPO MÉDIO OPERACIONAL', 'mean')).reset_index()
    return tmo_por_dia

def calcular_carteiras_analista(df_analista):
    if 'Carteira' in df_analista.columns:
        filas_finalizadas = df_analista[(df_analista['Status'] == 'FINALIZADO') |
                                        (df_analista['Status'] == 'RECLASSIFICADO') |
                                        (df_analista['Status'] == 'ANDAMENTO_PRE')]

        carteiras_analista = filas_finalizadas.groupby('Carteira').agg(
            Quantidade=('Carteira', 'size'),
            TMO_médio=('Tempo de Análise', 'mean')
        ).reset_index()

        # Renomeando a coluna 'Carteira' para 'Fila' para manter consistência
        carteiras_analista = carteiras_analista.rename(columns={'Carteira': 'Fila'})

        return carteiras_analista
    else:
        return pd.DataFrame({'Fila': [], 'Quantidade': [], 'TMO Médio por Fila': []})
    
def get_points_of_attention(df):
    # Verifica se a coluna 'Carteira' existe no DataFrame
    if 'Carteira' not in df.columns:
        return "A coluna 'Carteira' não foi encontrada no DataFrame."
    
    # Filtra os dados para 'JV ITAU BMG' e outras carteiras
    dfJV = df[df['Carteira'] == 'JV ITAU BMG'].copy()
    dfOutras = df[df['Carteira'] != 'JV ITAU BMG'].copy()
    
    # Filtra os pontos de atenção com base no tempo de análise
    pontos_de_atencao_JV = dfJV[dfJV['Tempo de Análise'] > pd.Timedelta(minutes=2)]
    pontos_de_atencao_outros = dfOutras[dfOutras['Tempo de Análise'] > pd.Timedelta(minutes=5)]
    
    # Combina os dados filtrados
    pontos_de_atencao = pd.concat([pontos_de_atencao_JV, pontos_de_atencao_outros])

    # Verifica se o DataFrame está vazio
    if pontos_de_atencao.empty:
        return "Não existem dados a serem exibidos."

    # Cria o dataframe com as colunas 'PROTOCOLO', 'CARTEIRA' e 'TEMPO'
    pontos_de_atencao = pontos_de_atencao[['Protocolo', 'Carteira', 'Tempo de Análise']].copy()

    # Renomeia a coluna 'Tempo de Análise' para 'TEMPO'
    pontos_de_atencao = pontos_de_atencao.rename(columns={'Tempo de Análise': 'TEMPO'})

    # Converte a coluna 'TEMPO' para formato de minutos
    pontos_de_atencao['TEMPO'] = pontos_de_atencao['TEMPO'].apply(lambda x: f"{int(x.total_seconds() // 60)}:{int(x.total_seconds() % 60):02d}")

    # Remove qualquer protocolo com valores vazios ou NaN
    pontos_de_atencao = pontos_de_atencao.dropna(subset=['Protocolo'])

    # Remove as vírgulas e a parte ".0" do protocolo
    pontos_de_atencao['Protocolo'] = pontos_de_atencao['Protocolo'].astype(str).str.replace(',', '', regex=False)
    
    # Garantir que o número do protocolo não tenha ".0"
    pontos_de_atencao['Protocolo'] = pontos_de_atencao['Protocolo'].str.replace(r'\.0$', '', regex=True)

    return pontos_de_atencao

def calcular_tmo_por_carteira(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera um resumo por Fila com:
    - Quantidade total de tarefas
    - TMO médio para tarefas finalizadas (exceto Fora do Escopo)
    - TMO médio para tarefas com finalização 'Fora do Escopo'

    Retorna:
        pd.DataFrame com colunas:
        ['FILA', 'Quantidade de Tarefas', 'TMO Finalizadas', 'TMO Fora do Escopo']
    """

    def format_timedelta(td):
        if pd.isna(td):
            return "00:00:00"
        total_seconds = int(td.total_seconds())
        return f"{total_seconds // 3600:02d}:{(total_seconds % 3600) // 60:02d}:{total_seconds % 60:02d}"

    # Validação das colunas obrigatórias
    required_columns = {'FILA', 'SITUAÇÃO DA TAREFA', 'FINALIZAÇÃO', 'TEMPO MÉDIO OPERACIONAL'}
    if not required_columns.issubset(df.columns):
        return "As colunas necessárias não foram encontradas no DataFrame."

    # Remove linhas sem tempo registrado
    df = df.dropna(subset=['TEMPO MÉDIO OPERACIONAL'])

    if not pd.api.types.is_timedelta64_dtype(df['TEMPO MÉDIO OPERACIONAL']):
        return "A coluna 'TEMPO MÉDIO OPERACIONAL' precisa ser do tipo timedelta."

    # Contagem total de tarefas por fila
    quantidade_tarefas = df.groupby('FILA').size().reset_index(name='Quantidade de Tarefas')

    # TMO de tarefas finalizadas (exceto Fora do Escopo)
    df_finalizadas = df[
        (df['SITUAÇÃO DA TAREFA'].str.upper() == 'FINALIZADA') &
        (df['FINALIZAÇÃO'].str.upper() != 'FORA DO ESCOPO')
    ]
    tmo_finalizadas = df_finalizadas.groupby('FILA')['TEMPO MÉDIO OPERACIONAL'].mean().reset_index()
    tmo_finalizadas.rename(columns={'TEMPO MÉDIO OPERACIONAL': 'TMO Finalizadas'}, inplace=True)

    # TMO de tarefas Fora do Escopo
    df_escopo = df[df['FINALIZAÇÃO'].str.upper() == 'FORA DO ESCOPO']
    tmo_escopo = df_escopo.groupby('FILA')['TEMPO MÉDIO OPERACIONAL'].mean().reset_index()
    tmo_escopo.rename(columns={'TEMPO MÉDIO OPERACIONAL': 'TMO Fora do Escopo'}, inplace=True)

    # Junta tudo
    resumo = quantidade_tarefas.merge(tmo_finalizadas, on='FILA', how='left')\
                               .merge(tmo_escopo, on='FILA', how='left')

    # Formata os campos de tempo
    resumo['TMO Finalizadas'] = resumo['TMO Finalizadas'].apply(format_timedelta)
    resumo['TMO Fora do Escopo'] = resumo['TMO Fora do Escopo'].apply(format_timedelta)

    return resumo[['FILA', 'Quantidade de Tarefas', 'TMO Finalizadas', 'TMO Fora do Escopo']]

def calcular_producao_agrupada(df):
    required_columns = {'FILA', 'FINALIZAÇÃO', 'NÚMERO DO PROTOCOLO'}
    if not required_columns.issubset(df.columns):
        return "As colunas necessárias ('FILA', 'FINALIZAÇÃO', 'NÚMERO DO PROTOCOLO') não foram encontradas no DataFrame."

    df_unique = df.drop_duplicates(subset=['NÚMERO DO PROTOCOLO'])

    grupos = {
        'CAPTURA ANTECIPADA': [' CADASTRO ROBÔ', 'INCIDENTE PROCESSUAL', 'CADASTRO ANS'],
        'SHAREPOINT': ['CADASTRO SHAREPOINT', 'ATUALIZAÇÃO - SHAREPOINT'],
        'CITAÇÃO ELETRÔNICA': ['CADASTRO CITAÇÃO ELETRÔNICA', 'ATUALIZAÇÃO CITAÇÃO ELETRÔNICA'],
        'E-MAIL': ['CADASTRO E-MAIL', 'OFICIOS E-MAIL', 'CADASTRO DE ÓRGÃOS E OFÍCIOS'],
        'PRE CADASTRO E DIJUR': ['PRE CADASTRO E DIJUR']
    }

    df['GRUPO'] = df['FILA'].map(lambda x: next((k for k, v in grupos.items() if x in v), 'OUTROS'))

    df_agrupado = df.groupby('GRUPO').agg(
        Cadastrado=('FINALIZAÇÃO', lambda x: (x == 'CADASTRADO').sum()),
        Atualizado=('FINALIZAÇÃO', lambda x: (x == 'ATUALIZADO').sum()),
        Fora_do_Escopo=('FINALIZAÇÃO', lambda x: ((x != 'CADASTRADO') & (x != 'ATUALIZADO')).sum())
    ).reset_index()

    return df_agrupado

def calcular_producao_email_detalhada(df):
    required_columns = {'FILA', 'FINALIZAÇÃO', 'NÚMERO DO PROTOCOLO', 'TAREFA'}
    if not required_columns.issubset(df.columns):
        return "As colunas necessárias ('FILA', 'FINALIZAÇÃO', 'NÚMERO DO PROTOCOLO', 'TAREFA') não foram encontradas no DataFrame."

    # Filtrando apenas as filas do grupo E-MAIL
    df_email = df[df['FILA'].isin(['CADASTRO E-MAIL', 'OFICIOS', 'CADASTRO DE ÓRGÃOS E OFÍCIOS'])]

    # Separando os de CADASTRO E-MAIL para agrupar por TAREFA
    df_cadastro_email = df_email[df_email['FILA'] == 'CADASTRO E-MAIL']
    df_outros_email = df_email[df_email['FILA'].isin(['OFICIOS', 'CADASTRO DE ÓRGÃOS E OFÍCIOS'])]

    # Agrupando CADASTRO E-MAIL por TAREFA
    df_cadastro_email_agrupado = df_cadastro_email.groupby('TAREFA').agg(
        Quantidade=('FILA', 'size'),
        Cadastrado=('FINALIZAÇÃO', lambda x: (x == 'CADASTRADO').sum()),
        Atualizado=('FINALIZAÇÃO', lambda x: (x == 'ATUALIZADO').sum()),
        Fora_do_Escopo=('FINALIZAÇÃO', lambda x: ((x != 'CADASTRADO') & (x != 'ATUALIZADO')).sum())
    ).reset_index()

    # Agrupando os demais (OFICIOS E-MAIL e CADASTRO DE ÓRGÃOS E OFÍCIOS) por FILA
    df_outros_email_agrupado = df_outros_email.groupby('FILA').agg(
        Quantidade=('FILA', 'size'),
        Cadastrado=('FINALIZAÇÃO', lambda x: (x == 'CADASTRADO').sum()),
        Atualizado=('FINALIZAÇÃO', lambda x: (x == 'ATUALIZADO').sum()),
        Fora_do_Escopo=('FINALIZAÇÃO', lambda x: ((x != 'CADASTRADO') & (x != 'ATUALIZADO')).sum())
    ).reset_index().rename(columns={'FILA': 'TAREFA'})

    # Concatenando os resultados
    df_email_final = pd.concat([df_cadastro_email_agrupado, df_outros_email_agrupado], ignore_index=True)

    return df_email_final

def calcular_e_exibir_tmo_cadastro_atualizacao_por_fila (df_analista, format_timedelta_hms, st):
    """
    Calcula e exibe o TMO médio de Cadastro e Atualização por Fila,
    junto com a quantidade de tarefas realizadas, na dashboard do Streamlit.

    Parâmetros:
        - df_analista: DataFrame contendo os dados de análise.
        - format_timedelta_hms: Função para formatar a duração do TMO em HH:MM:SS.
        - st: Referência para o módulo Streamlit (necessário para exibir os resultados).
    """
    if 'FILA' in df_analista.columns and 'FINALIZAÇÃO' in df_analista.columns:
        # Filtrar apenas as tarefas finalizadas com CADASTRADO e ATUALIZADO
        filas_finalizadas_analista = df_analista[df_analista['FINALIZAÇÃO'].isin(['CADASTRADO', 'ATUALIZADO'])]

        # Agrupar os dados por 'FILA' e calcular a quantidade de tarefas por fila
        df_quantidade = filas_finalizadas_analista.groupby('FILA').size().reset_index(name='Quantidade')

        # Calcular o TMO médio para cada fila separadamente
        df_tmo_cadastro = filas_finalizadas_analista[filas_finalizadas_analista['FINALIZAÇÃO'] == 'CADASTRADO'].groupby('FILA')['TEMPO MÉDIO OPERACIONAL'].mean().reset_index()
        df_tmo_atualizacao = filas_finalizadas_analista[filas_finalizadas_analista['FINALIZAÇÃO'] == 'ATUALIZADO'].groupby('FILA')['TEMPO MÉDIO OPERACIONAL'].mean().reset_index()

        # Renomear colunas
        df_tmo_cadastro.rename(columns={'TEMPO MÉDIO OPERACIONAL': 'TMO_Cadastro'}, inplace=True)
        df_tmo_atualizacao.rename(columns={'TEMPO MÉDIO OPERACIONAL': 'TMO_Atualizacao'}, inplace=True)

        # Unir os DataFrames pela Fila
        df_resultado = df_quantidade.merge(df_tmo_cadastro, on='FILA', how='left').merge(df_tmo_atualizacao, on='FILA', how='left')

        # Substituir valores NaN por Timedelta(0)
        df_resultado.fillna(pd.Timedelta(seconds=0), inplace=True)

        # Converter os TMOs para HH:MM:SS
        df_resultado['TMO_Cadastro'] = df_resultado['TMO_Cadastro'].apply(format_timedelta_hms)
        df_resultado['TMO_Atualizacao'] = df_resultado['TMO_Atualizacao'].apply(format_timedelta_hms)

        # Renomear colunas para exibição
        df_resultado.rename(columns={
            'FILA': 'Fila',
            'Quantidade': 'Quantidade',
            'TMO_Cadastro': 'TMO Cadastro',
            'TMO_Atualizacao': 'TMO Atualização'
        }, inplace=True)

        # Estilizar tabela no Streamlit
        styled_df = df_resultado.style.format({'Quantidade': '{:.0f}', 'TMO Cadastro': '{}', 'TMO Atualização': '{}'}).set_properties(**{'text-align': 'left'})
        styled_df = styled_df.set_table_styles([dict(selector='th', props=[('text-align', 'left')])])

        # Exibir DataFrame estilizado na dashboard
        st.dataframe(styled_df, hide_index=True, use_container_width=True)
    else:
        st.warning("As colunas necessárias ('FILA' e 'FINALIZAÇÃO') não foram encontradas no DataFrame.")

def format_timedelta_hms(td):
    """
    Formata um timedelta para o formato hh:mm:ss.
    """
    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def calcular_e_exibir_tmo_por_fila(df_analista, analista_selecionado, format_timedelta_hms, st):
    """
    Calcula e exibe o TMO médio por fila, junto com a quantidade de tarefas realizadas, 
    para um analista específico, na dashboard Streamlit.

    Parâmetros:
        - df_analista: DataFrame contendo os dados de análise.
        - analista_selecionado: Nome do analista selecionado.
        - format_timedelta: Função para formatar a duração do TMO em minutos e segundos.
        - st: Referência para o módulo Streamlit (necessário para exibir os resultados).
    """
    if 'FILA' in df_analista.columns:
        # Filtrar apenas as tarefas finalizadas para cálculo do TMO
        filas_finalizadas_analista = df_analista[df_analista['SITUAÇÃO DA TAREFA'] == 'Finalizada']
        
        # Agrupa por 'FILA' e calcula a quantidade e o TMO médio para cada fila
        carteiras_analista = filas_finalizadas_analista.groupby('FILA').agg(
            Quantidade=('FILA', 'size'),
            TMO_médio=('TEMPO MÉDIO OPERACIONAL', 'mean')
        ).reset_index()

        # Converte o TMO médio para minutos e segundos
        carteiras_analista['TMO_médio'] = carteiras_analista['TMO_médio'].apply(format_timedelta_hms)

        # Renomeia as colunas
        carteiras_analista = carteiras_analista.rename(columns={
            'FILA': 'Fila', 
            'Quantidade': 'Quantidade', 
            'TMO_médio': 'TMO Médio por Fila'
        })
        
        # Configura o estilo do DataFrame para alinhar o conteúdo à esquerda
        styled_df = carteiras_analista.style.format({'Quantidade': '{:.0f}', 'TMO Médio por Fila': '{:s}'}).set_properties(**{'text-align': 'left'})
        styled_df = styled_df.set_table_styles([dict(selector='th', props=[('text-align', 'left')])])

        # Exibe a tabela com as colunas Fila, Quantidade e TMO Médio
        st.dataframe(styled_df, hide_index=True, use_container_width=True)
    else:
        st.write("A coluna 'FILA' não foi encontrada no dataframe.")
        carteiras_analista = pd.DataFrame({'Fila': [], 'Quantidade': [], 'TMO Médio por Fila': []})
        styled_df = carteiras_analista.style.format({'Quantidade': '{:.0f}', 'TMO Médio por Fila': '{:s}'}).set_properties(**{'text-align': 'left'})
        styled_df = styled_df.set_table_styles([dict(selector='th', props=[('text-align', 'left')])])
        st.dataframe(styled_df, hide_index=True, use_container_width=True)

def calcular_tmo_por_mes(df):
    # Converter coluna de tempo de análise para timedelta, se necessário
    if df['TEMPO MÉDIO OPERACIONAL'].dtype != 'timedelta64[ns]':
        df['TEMPO MÉDIO OPERACIONAL'] = pd.to_timedelta(df['TEMPO MÉDIO OPERACIONAL'], errors='coerce')
    
    # Adicionar coluna com ano e mês extraído da coluna 'Próximo'
    df['AnoMes'] = df['DATA DE CONCLUSÃO DA TAREFA'].dt.to_period('M')
    
    # Filtrar apenas os protocolos com status 'FINALIZADO'
    df_finalizados = df[df['SITUAÇÃO DA TAREFA'].isin(['Finalizada'])]
    
    # Agrupar por AnoMes e calcular o TMO
    df_tmo_mes = df_finalizados.groupby('AnoMes').agg(
        Tempo_Total=('TEMPO MÉDIO OPERACIONAL', 'sum'),
        Total_Protocolos=('TEMPO MÉDIO OPERACIONAL', 'count')
    ).reset_index()
    
    # Calcular o TMO em minutos
    df_tmo_mes['TMO'] = (df_tmo_mes['Tempo_Total'] / pd.Timedelta(minutes=1)) / df_tmo_mes['Total_Protocolos']
    
    # Converter a coluna AnoMes para datetime e formatar como "Mês XX de Ano"
    df_tmo_mes['AnoMes'] = df_tmo_mes['AnoMes'].dt.to_timestamp().dt.strftime('%B de %Y').str.capitalize()
    
    return df_tmo_mes[['AnoMes', 'TMO']]

# Função de formatação
def format_timedelta_mes(minutes):
    """Formata um valor em minutos (float) como 'Xh Ym Zs' se acima de 60 minutos, caso contrário, 'X min Ys'."""
    if minutes >= 60:
        hours = int(minutes // 60)
        minutes_remainder = int(minutes % 60)
        seconds = (minutes - hours * 60 - minutes_remainder) * 60
        seconds_int = round(seconds)
        return f"{hours}h {minutes_remainder}m {seconds_int}s"
    else:
        minutes_int = int(minutes)
        seconds = (minutes - minutes_int) * 60
        seconds_int = round(seconds)
        return f"{minutes_int} min {seconds_int}s"

def exibir_tmo_por_mes(df):
    # Calcule o TMO mensal usando a função importada
    df_tmo_mes = calcular_tmo_por_mes(df)
    
    # Verifique se há dados para exibir
    if df_tmo_mes.empty:
        st.warning("Nenhum dado finalizado disponível para calcular o TMO mensal.")
    else:
        # Formatar a coluna TMO como "X min Ys"
        df_tmo_mes['TMO_Formatado'] = df_tmo_mes['TMO'].apply(format_timedelta_mes)
        
        st.subheader("Tempo Médio Operacional Mensal")
        
        # Crie um multiselect para os meses
        meses_disponiveis = df_tmo_mes['AnoMes'].unique()
        meses_selecionados = st.multiselect(
            "Selecione os meses para exibição",
            options=meses_disponiveis,
            default=meses_disponiveis
        )
        
        # Filtrar os dados com base nos meses selecionados
        df_tmo_mes_filtrado = df_tmo_mes[df_tmo_mes['AnoMes'].isin(meses_selecionados)]
        
        # Verificar se há dados após o filtro
        if df_tmo_mes_filtrado.empty:
            st.warning("Nenhum dado disponível para os meses selecionados.")
            return None
        
        # Crie e exiba o gráfico de barras
        fig = px.bar(
            df_tmo_mes_filtrado, 
            x='AnoMes', 
            y='TMO', 
            labels={'AnoMes': 'Mês', 'TMO': 'TMO (minutos)'},
            text=df_tmo_mes_filtrado['TMO_Formatado'], # Usar o TMO formatado como rótulo
            color_discrete_sequence=['#ff571c', '#7f2b0e', '#4c1908', '#ff884d', '#a34b28', '#331309']  # Cor azul
        )
        # Garantir que o eixo X seja tratado como categórico
        fig.update_xaxes(type='category')
        
        # Configurar o layout do gráfico
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)
        
def exibir_dataframe_tmo_formatado(df):
    # Calcule o TMO mensal usando a função `calcular_tmo_por_mes`
    df_tmo_mes = calcular_tmo_por_mes(df)
    
    # Verifique se há dados para exibir
    if df_tmo_mes.empty:
        st.warning("Nenhum dado finalizado disponível para calcular o TMO mensal.")
        return None
    
    # Adicionar a coluna "Tempo Médio Operacional" com base no TMO calculado
    df_tmo_mes['Tempo Médio Operacional'] = df_tmo_mes['TMO'].apply(format_timedelta_mes)
    df_tmo_mes['Mês'] = df_tmo_mes['AnoMes']
    
    # Selecionar as colunas para exibição
    df_tmo_formatado = df_tmo_mes[['Mês', 'Tempo Médio Operacional']]
    
    st.dataframe(df_tmo_formatado, use_container_width=True, hide_index=True)
    
    return df_tmo_formatado

def export_dataframe(df):
    st.subheader("Exportar Dados")
    
    # Seleção de colunas
    colunas_disponiveis = list(df.columns)
    colunas_selecionadas = st.multiselect(
        "Selecione as colunas que deseja exportar:", colunas_disponiveis, default=[]
    )
    
    # Filtrar o DataFrame pelas colunas selecionadas
    if colunas_selecionadas:
        df_filtrado = df[colunas_selecionadas]
        
        # Botão de download
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df_filtrado.to_excel(writer, index=False, sheet_name='Dados_Exportados')
        buffer.seek(0)
        
        st.download_button(
            label="Baixar Excel",
            data=buffer,
            file_name="dados_exportados.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("Selecione pelo menos uma coluna para exportar.")
        
def calcular_melhor_tmo_por_dia(df_analista):
    """
    Calcula o melhor TMO de cadastro por dia para o analista.

    Parâmetros:
        - df_analista: DataFrame filtrado para o analista.

    Retorna:
        - O dia com o melhor TMO de cadastro e o valor do TMO.
    """
    # Filtrar apenas as finalizações do tipo 'CADASTRADO'
    df_cadastro = df_analista[df_analista['FINALIZAÇÃO'] == 'CADASTRADO']

    # Calcula o TMO por dia para o tipo 'CADASTRADO'
    df_tmo_por_dia = calcular_tmo_por_dia(df_cadastro)

    # Identifica o dia com o menor TMO
    if not df_tmo_por_dia.empty:
        melhor_dia = df_tmo_por_dia.loc[df_tmo_por_dia['TMO'].idxmin()]
        return melhor_dia['Dia'], melhor_dia['TMO']

    # Retorna None caso não haja dados para 'CADASTRADO'
    return None, None

def calcular_melhor_dia_por_cadastro(df_analista):
    # Agrupa os dados por dia e conta os cadastros
    if 'FINALIZAÇÃO' in df_analista.columns and 'DATA DE CONCLUSÃO DA TAREFA' in df_analista.columns:
        df_cadastros_por_dia = df_analista[df_analista['FINALIZAÇÃO'] == 'CADASTRADO'].groupby(
            df_analista['DATA DE CONCLUSÃO DA TAREFA'].dt.date
        ).size().reset_index(name='Quantidade')
        
        # Identifica o dia com maior quantidade de cadastros
        if not df_cadastros_por_dia.empty:
            melhor_dia = df_cadastros_por_dia.loc[df_cadastros_por_dia['Quantidade'].idxmax()]
            return melhor_dia['DATA DE CONCLUSÃO DA TAREFA'], melhor_dia['Quantidade']
    
    return None, 0

def exibir_tmo_por_mes_analista(df_analista, analista_selecionado):
    """
    Exibe o gráfico e a tabela do TMO mensal para um analista específico com filtro por mês.

    Parâmetros:
        - df_analista: DataFrame filtrado para o analista.
        - analista_selecionado: Nome do analista selecionado.
    """
    # Calcular o TMO por mês
    df_tmo_mes = calcular_tmo_por_mes(df_analista)

    # Verificar se há dados para exibir
    if df_tmo_mes.empty:
        st.warning(f"Não há dados para calcular o TMO mensal do analista {analista_selecionado}.")
        return None

    # Formatar o TMO para exibição
    df_tmo_mes['TMO_Formatado'] = df_tmo_mes['TMO'].apply(format_timedelta_mes)

    # Criar multiselect para os meses disponíveis
    meses_disponiveis = df_tmo_mes['AnoMes'].unique()
    meses_selecionados = st.multiselect(
        "Selecione os meses para exibição",
        options=meses_disponiveis,
        default=meses_disponiveis
    )

    # Filtrar os dados com base nos meses selecionados
    df_tmo_mes_filtrado = df_tmo_mes[df_tmo_mes['AnoMes'].isin(meses_selecionados)]

    # Verificar se há dados após o filtro
    if df_tmo_mes_filtrado.empty:
        st.warning("Nenhum dado disponível para os meses selecionados.")
        return None

    # Criar e exibir o gráfico de barras
    fig = px.bar(
        df_tmo_mes_filtrado,
        x='AnoMes',
        y='TMO',
        labels={'AnoMes': 'Mês', 'TMO': 'TMO (minutos)'},
        text=df_tmo_mes_filtrado['TMO_Formatado'],  # Usar o TMO formatado como rótulo
        color_discrete_sequence=['#ff571c', '#7f2b0e', '#4c1908', '#ff884d', '#a34b28', '#331309']
    )
    fig.update_xaxes(type='category')  # Tratar o eixo X como categórico
    fig.update_traces(textposition='outside')
    st.plotly_chart(fig, use_container_width=True)

    # Criar e exibir a tabela com os dados formatados
    df_tmo_mes_filtrado['Mês'] = df_tmo_mes_filtrado['AnoMes']  # Renomear para exibição
    df_tmo_formatado = df_tmo_mes_filtrado[['Mês', 'TMO_Formatado']].rename(columns={'TMO_Formatado': 'Tempo Médio Operacional'})
    st.dataframe(df_tmo_formatado, use_container_width=True, hide_index=True)

    return df_tmo_formatado


def calcular_grafico_tmo_analista_por_mes(df_analista):
    """
    Calcula o TMO Geral, Cadastro e Atualização por mês para um analista específico.
    
    Parâmetro:
        - df_analista: DataFrame contendo as tarefas do analista.
    
    Retorna:
        - DataFrame com TMO_Geral, TMO_Cadastro e TMO_Atualizacao por mês.
    """
    if df_analista.empty:
        return pd.DataFrame(columns=['AnoMes', 'TMO_Geral', 'TMO_Cadastro', 'TMO_Atualizacao'])
    
    # Converter 'TEMPO MÉDIO OPERACIONAL' para timedelta se necessário
    if not pd.api.types.is_timedelta64_dtype(df_analista['TEMPO MÉDIO OPERACIONAL']):
        df_analista['TEMPO MÉDIO OPERACIONAL'] = pd.to_timedelta(df_analista['TEMPO MÉDIO OPERACIONAL'], errors='coerce')

    df_analista['AnoMes'] = df_analista['DATA DE CONCLUSÃO DA TAREFA'].dt.to_period('M').astype(str)

    df_geral = df_analista[df_analista['SITUAÇÃO DA TAREFA'].isin(['Finalizada', 'Cancelada'])]
    df_cadastro = df_analista[df_analista['FINALIZAÇÃO'] == 'Cadastro realizado']
    df_atualizacao = df_analista[df_analista['FINALIZAÇÃO'] == 'ATUALIZADO']

    def calcular_tmo(df, nome_coluna):
        if df.empty:
            return pd.DataFrame(columns=['AnoMes', nome_coluna])
        
        df_tmo = df.groupby('AnoMes').agg(
            Tempo_Total=('TEMPO MÉDIO OPERACIONAL', 'sum'),
            Total_Protocolos=('TEMPO MÉDIO OPERACIONAL', 'count')
        ).reset_index()
        
        df_tmo[nome_coluna] = df_tmo['Tempo_Total'] / df_tmo['Total_Protocolos']
        df_tmo = df_tmo[['AnoMes', nome_coluna]]
        
        return df_tmo

    df_tmo_geral = calcular_tmo(df_geral, 'TMO_Geral')
    df_tmo_cadastro = calcular_tmo(df_cadastro, 'TMO_Cadastro')
    df_tmo_atualizacao = calcular_tmo(df_atualizacao, 'TMO_Atualizacao')

    df_tmo_mes = df_tmo_geral.merge(df_tmo_cadastro, on='AnoMes', how='left').merge(df_tmo_atualizacao, on='AnoMes', how='left')

    df_tmo_mes.fillna(pd.Timedelta(seconds=0), inplace=True)
    df_tmo_mes['AnoMes'] = pd.to_datetime(df_tmo_mes['AnoMes'], errors='coerce')  # Converter para datetime
    df_tmo_mes['AnoMes'] = df_tmo_mes['AnoMes'].dt.strftime('%B de %Y').str.capitalize()

    return df_tmo_mes

def format_timedelta_grafico_tmo_analista(td):
    """Formata um timedelta no formato HH:MM:SS"""
    if pd.isna(td) or td == pd.Timedelta(seconds=0):
        return "00:00:00"
    
    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return f"{hours:02}:{minutes:02}:{seconds:02}"

def format_timedelta_hms(timedelta_value):
    """Formata um timedelta em HH:MM:SS"""
    total_seconds = int(timedelta_value.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def exibir_grafico_tmo_analista_por_mes(df_analista, analista_selecionado):
    """
    Exibe um gráfico de barras agrupadas do TMO mensal (Geral, Cadastro, Atualização) para um analista específico.

    Parâmetros:
        - df_analista: DataFrame filtrado para o analista.
        - analista_selecionado: Nome do analista selecionado.
    """
    # Calcular o TMO por mês
    df_tmo_mes = calcular_grafico_tmo_analista_por_mes(df_analista)

    # Verificar se há dados para exibir
    if df_tmo_mes.empty:
        st.warning(f"Não há dados para calcular o TMO mensal do analista {analista_selecionado}.")
        return None

    # Formatar os tempos para HH:MM:SS
    for col in ['TMO_Geral', 'TMO_Cadastro', 'TMO_Atualizacao']:
        df_tmo_mes[col + '_Formatado'] = df_tmo_mes[col].apply(format_timedelta_hms)

    # Criar um multiselect para filtrar os meses disponíveis
    meses_disponiveis = df_tmo_mes['AnoMes'].unique()
    meses_selecionados = st.multiselect(
        "Selecione os meses para exibição",
        options=meses_disponiveis,
        default=meses_disponiveis
    )

    # Filtrar os dados com base nos meses selecionados
    df_tmo_mes_filtrado = df_tmo_mes[df_tmo_mes['AnoMes'].isin(meses_selecionados)]

    # Verificar se há dados após o filtro
    if df_tmo_mes_filtrado.empty:
        st.warning("Nenhum dado disponível para os meses selecionados.")
        return None

    # Transformar os dados para exibição no gráfico (de wide para long format)
    df_tmo_long = df_tmo_mes_filtrado.melt(
        id_vars=['AnoMes'], 
        value_vars=['TMO_Geral', 'TMO_Cadastro', 'TMO_Atualizacao'], 
        var_name='Tipo de TMO', 
        value_name='Tempo Médio Operacional'
    )

    # Criar dicionário correto para mapear valores formatados
    format_dict = df_tmo_mes_filtrado.set_index('AnoMes')[
        ['TMO_Geral_Formatado', 'TMO_Cadastro_Formatado', 'TMO_Atualizacao_Formatado']
    ].stack().reset_index()

    format_dict.columns = ['AnoMes', 'Tipo de TMO', 'Tempo Formatado']
    format_dict['Tipo de TMO'] = format_dict['Tipo de TMO'].str.replace('_Formatado', '')

    # Converter `format_dict` para dicionário correto
    format_map = format_dict.set_index(['AnoMes', 'Tipo de TMO'])['Tempo Formatado'].to_dict()

    # Definir a paleta de cores extraída da imagem
    custom_colors = {
        'TMO_Geral': '#ff6a1c',        # Laranja forte
        'TMO_Cadastro': '#d1491c',     # Vermelho queimado médio
        'TMO_Atualizacao': '#a3330f'   # Vermelho queimado escuro
    }

    # Criar rótulos corrigidos
    tipo_tmo_label = {
        'TMO_Geral': 'Geral',
        'TMO_Cadastro': 'Cadastro',
        'TMO_Atualizacao': 'Atualização'
    }

    df_tmo_long['Texto_Rotulo'] = df_tmo_long.apply(
        lambda row: f"{tipo_tmo_label[row['Tipo de TMO']]} - {format_map.get((row['AnoMes'], row['Tipo de TMO']), '')}",
        axis=1
    )

    # Criar o gráfico de barras
    fig = px.bar(
        df_tmo_long,
        x='AnoMes',
        y='Tempo Médio Operacional',
        color='Tipo de TMO',
        text=df_tmo_long['Texto_Rotulo'],
        barmode='group',
        labels={'AnoMes': 'Mês', 'Tempo Médio Operacional': 'Tempo Médio Operacional (HH:MM:SS)'},
        color_discrete_map=custom_colors
    )

    # Ajustar espaçamento entre as barras
    fig.update_layout(bargap=0.2, bargroupgap=0.15)

    # Posicionar os rótulos corretamente
    fig.update_traces(textposition='outside')

    # Remover a legenda
    fig.update_layout(showlegend=False)

    # Exibir o gráfico na dashboard
    st.plotly_chart(fig, use_container_width=True)

    # Criar e exibir a tabela com os dados formatados
    df_tmo_formatado = df_tmo_mes_filtrado[['AnoMes', 'TMO_Geral_Formatado', 'TMO_Cadastro_Formatado', 'TMO_Atualizacao_Formatado']]
    df_tmo_formatado.columns = ['AnoMes', 'TMO Geral', 'TMO Cadastro', 'TMO Atualização']
    st.dataframe(df_tmo_formatado, use_container_width=True, hide_index=True)

    return df_tmo_formatado

def calcular_tmo_personalizado(df):
    """
    Calcula o TMO considerando as regras específicas para cada tipo de tarefa.

    Parâmetros:
        - df: DataFrame com os dados filtrados.

    Retorno:
        - DataFrame com TMO calculado por analista.
    """
    # Filtrar tarefas por tipo de finalização
    total_finalizados = len(df[df['FINALIZAÇÃO'] == 'CADASTRADO'])
    total_realizados = len(df[df['FINALIZAÇÃO'] == 'REALIZADO'])
    total_atualizado = len(df[df['FINALIZAÇÃO'] == 'ATUALIZADO'])

    # Calcular o tempo total por tipo de finalização
    tempo_total_cadastrado = df[df['FINALIZAÇÃO'] == 'CADASTRADO']['TEMPO MÉDIO OPERACIONAL'].sum()
    tempo_total_atualizado = df[df['FINALIZAÇÃO'] == 'ATUALIZADO']['TEMPO MÉDIO OPERACIONAL'].sum()
    tempo_total_realizado = df[df['FINALIZAÇÃO'] == 'REALIZADO']['TEMPO MÉDIO OPERACIONAL'].sum()

    # Calcular o tempo médio por tipo de finalização
    tmo_cadastrado = tempo_total_cadastrado / total_finalizados if total_finalizados > 0 else pd.Timedelta(0)
    tmo_atualizado = tempo_total_atualizado / total_atualizado if total_atualizado > 0 else pd.Timedelta(0)

    # Calcular o TMO geral
    tempo_total_analista = tempo_total_cadastrado + tempo_total_atualizado + tempo_total_realizado
    total_tarefas = total_finalizados + total_atualizado + total_realizados
    tempo_medio_analista = tempo_total_analista / total_tarefas if total_tarefas > 0 else pd.Timedelta(0)

    return tempo_medio_analista


def exportar_planilha_com_tmo(df, periodo_selecionado, analistas_selecionados, tmo_tipo='GERAL'):
    """
    Exporta uma planilha com informações do período selecionado, analistas, TMO (geral, cadastrado ou cadastrado com tipo) e quantidade de tarefas,
    adicionando formatação condicional baseada na média do TMO.

    Parâmetros:
        - df: DataFrame com os dados.
        - periodo_selecionado: Tuple contendo a data inicial e final.
        - analistas_selecionados: Lista de analistas selecionados.
        - tmo_tipo: Tipo de TMO a ser usado ('GERAL', 'CADASTRADO', 'CADASTRADO_DETALHADO').
    """
    # Filtrar o DataFrame com base no período e analistas selecionados
    data_inicial, data_final = periodo_selecionado
    df_filtrado = df[
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date >= data_inicial) &
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date <= data_final) &
        (df['USUÁRIO QUE CONCLUIU A TAREFA'].isin(analistas_selecionados))
    ]

    # Calcular o TMO e a quantidade por analista
    analistas = []
    tmos = []
    quantidades = []
    tipos_causa = []  # Para armazenar os tipos de "TP CAUSA (TP COMPLEMENTO)"

    for analista in analistas_selecionados:
        df_analista = df_filtrado[df_filtrado['USUÁRIO QUE CONCLUIU A TAREFA'] == analista]
        if tmo_tipo == 'GERAL':
            # Considerar apenas as finalizações "CADASTRADO", "REALIZADO" e "ATUALIZADO"
            df_relevante = df_analista[df_analista['FINALIZAÇÃO'].isin(['CADASTRADO', 'REALIZADO', 'ATUALIZADO'])]
        elif tmo_tipo == 'CADASTRADO':
            # Considerar apenas as finalizações "CADASTRADO"
            df_relevante = df_analista[df_analista['FINALIZAÇÃO'] == 'CADASTRADO']
        elif tmo_tipo == 'CADASTRADO_DETALHADO':
            # Considerar apenas as finalizações "CADASTRADO" e detalhar por "TP CAUSA (TP COMPLEMENTO)"
            df_relevante = df_analista[df_analista['FINALIZAÇÃO'] == 'CADASTRADO']
            causa_detalhes = df_relevante.groupby('TP CAUSA (TP COMPLEMENTO)').size().reset_index(name='Quantidade')
            tipos_causa.append(causa_detalhes)
        else:
            st.error("Tipo de TMO inválido selecionado.")
            return

        tmo_analista = calcular_tmo_personalizado(df_relevante)
        quantidade_analista = len(df_relevante)

        analistas.append(analista)
        tmos.append(tmo_analista)
        quantidades.append(quantidade_analista)

    # Criar o DataFrame de resumo
    df_resumo = pd.DataFrame({
        'Analista': analistas,
        'TMO': tmos,
        'Quantidade': quantidades
    })

    # Adicionar o período ao DataFrame exportado
    df_resumo['Período Inicial'] = data_inicial
    df_resumo['Período Final'] = data_final

    # Formatar o TMO como HH:MM:SS
    df_resumo['TMO'] = df_resumo['TMO'].apply(
        lambda x: f"{int(x.total_seconds() // 3600):02}:{int((x.total_seconds() % 3600) // 60):02}:{int(x.total_seconds() % 60):02}"
    )

    # Calcular a média do TMO em segundos
    tmo_segundos = [timedelta(hours=int(t.split(":")[0]), minutes=int(t.split(":")[1]), seconds=int(t.split(":")[2])).total_seconds() for t in df_resumo['TMO']]
    media_tmo_segundos = sum(tmo_segundos) / len(tmo_segundos)

    # Criar um arquivo Excel em memória
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        # Exportar os dados do resumo
        df_resumo.to_excel(writer, index=False, sheet_name='Resumo')

        # Se for CADASTRADO_DETALHADO, incluir os tipos de causa
        if tmo_tipo == 'CADASTRADO_DETALHADO' and tipos_causa:
            for i, causa_detalhes in enumerate(tipos_causa):
                causa_detalhes.to_excel(writer, index=False, sheet_name=f'Tipos_{analistas[i]}')

        # Acessar o workbook e worksheet para aplicar formatação condicional
        workbook = writer.book
        worksheet = writer.sheets['Resumo']

        # Ajustar largura das colunas
        worksheet.set_column('A:A', 20)  # Coluna 'Analista'
        worksheet.set_column('B:B', 12)  # Coluna 'TMO'
        worksheet.set_column('C:C', 15)  # Coluna 'Quantidade'
        worksheet.set_column('D:E', 15)  # Colunas 'Período Inicial' e 'Período Final'

        # Formatação baseada na média do TMO
        format_tmo_green = workbook.add_format({'bg_color': '#CCFFCC', 'font_color': '#006600'})  # Verde
        format_tmo_yellow = workbook.add_format({'bg_color': '#FFFFCC', 'font_color': '#666600'})  # Amarelo
        format_tmo_red = workbook.add_format({'bg_color': '#FFCCCC', 'font_color': '#FF0000'})  # Vermelho

        # Aplicar formatação condicional
        for row, tmo in enumerate(tmo_segundos, start=2):
            if tmo < media_tmo_segundos * 0.9:  # Abaixo da média
                worksheet.write(f'B{row}', df_resumo.loc[row-2, 'TMO'], format_tmo_green)
            elif media_tmo_segundos * 0.9 <= tmo <= media_tmo_segundos * 1.1:  # Na média ou próximo
                worksheet.write(f'B{row}', df_resumo.loc[row-2, 'TMO'], format_tmo_yellow)
            else:  # Acima da média
                worksheet.write(f'B{row}', df_resumo.loc[row-2, 'TMO'], format_tmo_red)

    buffer.seek(0)

    # Oferecer download
    st.download_button(
        label="Baixar Planilha",
        data=buffer,
        file_name="resumo_analistas_formatado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    
import pandas as pd
import streamlit as st
from io import BytesIO
from datetime import timedelta

def exportar_planilha_com_tmo_completo(df, periodo_selecionado, analistas_selecionados):
    """
    Exporta uma planilha com informações do período selecionado, incluindo:
    - TMO de Cadastro
    - Quantidade de Cadastro
    - TMO de Atualizado
    - Quantidade de Atualização
    """

    # Filtrar o DataFrame com base no período e analistas selecionados
    data_inicial, data_final = periodo_selecionado
    df_filtrado = df[
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date >= data_inicial) &
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date <= data_final) &
        (df['USUÁRIO QUE CONCLUIU A TAREFA'].isin(analistas_selecionados))
    ]

    # Criar listas para armazenar os dados por analista
    analistas = []
    tmo_cadastrado = []
    quantidade_cadastrado = []
    tmo_atualizado = []
    quantidade_atualizado = []

    for analista in analistas_selecionados:
        df_analista = df_filtrado[df_filtrado['USUÁRIO QUE CONCLUIU A TAREFA'] == analista]

        # Cálculo para Cadastro
        df_cadastro = df_analista[df_analista['FINALIZAÇÃO'] == 'CADASTRADO']
        tmo_cadastro_analista = df_cadastro['TEMPO MÉDIO OPERACIONAL'].mean()
        total_cadastro = len(df_cadastro)

        # Cálculo para Atualizado
        df_atualizado = df_analista[df_analista['FINALIZAÇÃO'] == 'ATUALIZADO']
        tmo_atualizado_analista = df_atualizado['TEMPO MÉDIO OPERACIONAL'].mean()
        total_atualizado = len(df_atualizado)

        # Adicionar valores às listas
        analistas.append(analista)
        tmo_cadastrado.append(tmo_cadastro_analista)
        quantidade_cadastrado.append(total_cadastro)
        tmo_atualizado.append(tmo_atualizado_analista)
        quantidade_atualizado.append(total_atualizado)

    # Criar DataFrame de resumo
    df_resumo = pd.DataFrame({
        'Analista': analistas,
        'TMO Cadastro': tmo_cadastrado,
        'Quantidade Cadastro': quantidade_cadastrado,
        'TMO Atualizado': tmo_atualizado,
        'Quantidade Atualização': quantidade_atualizado
    })

    # Converter TMO para HH:MM:SS (removendo frações de segundos)
    def format_tmo(tmo):
        if pd.notnull(tmo):
            total_seconds = int(tmo.total_seconds())  # Removendo frações
            return str(timedelta(seconds=total_seconds))
        return '00:00:00'

    df_resumo['TMO Cadastro'] = df_resumo['TMO Cadastro'].apply(format_tmo)
    df_resumo['TMO Atualizado'] = df_resumo['TMO Atualizado'].apply(format_tmo)

    # Criar um arquivo Excel em memória
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df_resumo.to_excel(writer, index=False, sheet_name='Resumo')

        # Ajustes no Excel
        workbook = writer.book
        worksheet = writer.sheets['Resumo']
        worksheet.set_column('A:A', 20)  # Coluna Analista
        worksheet.set_column('B:E', 15)  # Colunas de TMO e Quantidade

    buffer.seek(0)

    # Oferecer download no Streamlit
    st.download_button(
        label="Baixar Planilha Completa de TMO",
        data=buffer,
        file_name="relatorio_tmo_completo.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

def exportar_relatorio_detalhado_por_analista(df, periodo_selecionado, analistas_selecionados):
    """
    Exporta um relatório detalhado por analista, com TMO de CADASTRADO e quantidade por dia, gerando uma aba para cada analista.

    Parâmetros:
        - df: DataFrame com os dados.
        - periodo_selecionado: Tuple contendo a data inicial e final.
        - analistas_selecionados: Lista de analistas selecionados.
    """
    data_inicial, data_final = periodo_selecionado

    # Filtrar o DataFrame pelo período e analistas selecionados
    df_filtrado = df[
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date >= data_inicial) &
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date <= data_final) &
        (df['USUÁRIO QUE CONCLUIU A TAREFA'].isin(analistas_selecionados)) &
        (df['FINALIZAÇÃO'] == 'CADASTRADO')  # Apenas tarefas cadastradas
    ]

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        # Criar relatório detalhado por analista
        for analista in analistas_selecionados:
            df_analista = df_filtrado[df_filtrado['USUÁRIO QUE CONCLUIU A TAREFA'] == analista]
            
            # Calcular TMO e quantidade por dia
            df_tmo_por_dia = df_analista.groupby(df_analista['DATA DE CONCLUSÃO DA TAREFA'].dt.date).agg(
                TMO=('TEMPO MÉDIO OPERACIONAL', lambda x: x.sum() / len(x) if len(x) > 0 else pd.Timedelta(0)),
                Quantidade=('DATA DE CONCLUSÃO DA TAREFA', 'count')
            ).reset_index()
            df_tmo_por_dia.rename(columns={'DATA DE CONCLUSÃO DA TAREFA': 'Dia'}, inplace=True)

            # Formatar TMO como HH:MM:SS
            df_tmo_por_dia['TMO'] = df_tmo_por_dia['TMO'].apply(
                lambda x: f"{int(x.total_seconds() // 3600):02}:{int((x.total_seconds() % 3600) // 60):02}:{int(x.total_seconds() % 60):02}"
            )

            # Adicionar coluna de analista
            df_tmo_por_dia.insert(0, 'Analista', analista)

            # Reordenar colunas para "ANALISTA, TMO, QUANTIDADE, DIA"
            df_tmo_por_dia = df_tmo_por_dia[['Analista', 'TMO', 'Quantidade', 'Dia']]

            # Exportar os dados para uma aba do Excel
            if not df_tmo_por_dia.empty:
                df_tmo_por_dia.to_excel(writer, index=False, sheet_name=analista[:31])

                # Acessar a aba para formatação condicional
                workbook = writer.book
                worksheet = writer.sheets[analista[:31]]

                # Ajustar largura das colunas
                worksheet.set_column('A:A', 20)  # Coluna 'Analista'
                worksheet.set_column('B:B', 12)  # Coluna 'TMO'
                worksheet.set_column('C:C', 12)  # Coluna 'Quantidade'
                worksheet.set_column('D:D', 15)  # Coluna 'Dia'

                # Criar formatos para formatação condicional
                format_tmo_green = workbook.add_format({'bg_color': '#CCFFCC', 'font_color': '#006600'})  # Verde
                format_tmo_yellow = workbook.add_format({'bg_color': '#FFFFCC', 'font_color': '#666600'})  # Amarelo
                format_tmo_red = workbook.add_format({'bg_color': '#FFCCCC', 'font_color': '#FF0000'})  # Vermelho

                # Aplicar formatação condicional com base no TMO
                worksheet.conditional_format(
                    'B2:B{}'.format(len(df_tmo_por_dia) + 1),
                    {
                        'type': 'formula',
                        'criteria': f'=LEN(B2)>0',
                        'format': format_tmo_yellow  # Formato padrão para demonstração
                    }
                )

    buffer.seek(0)

    # Oferecer download
    st.download_button(
        label="Baixar Relatório Detalhado por Analista (TMO CADASTRADO)",
        data=buffer,
        file_name="relatorio_tmo_detalhado_cadastrado_por_analista.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

def calcular_tmo_geral(df):
    """
    Calcula o TMO Geral considerando todas as tarefas finalizadas.
    """
    df_finalizados = df[df['FINALIZAÇÃO'].isin(['CADASTRADO', 'REALIZADO', 'ATUALIZADO'])]
    return df_finalizados['TEMPO MÉDIO OPERACIONAL'].mean()

def calcular_tmo_cadastro(df):
    """
    Calcula o TMO apenas para tarefas finalizadas como "CADASTRADO".
    """
    df_cadastro = df[df['FINALIZAÇÃO'] == 'CADASTRADO']
    return df_cadastro['TEMPO MÉDIO OPERACIONAL'].mean()

def calcular_tempo_ocioso(df):
    """
    Calcula o tempo ocioso total por analista.
    """
    df_ocioso = df.groupby('USUÁRIO QUE CONCLUIU A TAREFA')['TEMPO OCIOSO'].sum().reset_index()
    return df_ocioso

def gerar_relatorio_tmo_completo(df, periodo_selecionado, analistas_selecionados):
    """
    Gera um relatório Excel com TMO de Cadastro, TMO Geral, Quantidade de Cadastro,
    Quantidade Total de Protocolos e Tempo Ocioso.
    """
    data_inicial, data_final = periodo_selecionado

    # 🔹 Filtrar os dados pelo período e analistas selecionados
    df_filtrado = df[
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date >= data_inicial) &
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date <= data_final) &
        (df['USUÁRIO QUE CONCLUIU A TAREFA'].isin(analistas_selecionados))
    ].copy()  # Criar uma cópia para evitar alterações no DataFrame original

    # 🔹 Calcular o tempo ocioso por analista
    df_tempo_ocioso = calcular_tempo_ocioso_por_analista(df_filtrado)

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        for analista in analistas_selecionados:
            df_analista = df_filtrado[df_filtrado['USUÁRIO QUE CONCLUIU A TAREFA'] == analista]
            tmo_geral = calcular_tmo_geral(df_analista)
            tmo_cadastro = calcular_tmo_cadastro(df_analista)
            total_cadastros = len(df_analista[df_analista['FINALIZAÇÃO'] == 'CADASTRADO'])
            total_protocolos = len(df_analista)

            # 🔹 Ajuste para acessar a coluna correta do DataFrame `df_tempo_ocioso`
            tempo_ocioso = df_tempo_ocioso[df_tempo_ocioso['USUÁRIO QUE CONCLUIU A TAREFA'] == analista]['TEMPO OCIOSO'].sum() if not df_tempo_ocioso.empty else pd.Timedelta(0)

            # 🔹 Criar DataFrame com os dados do relatório
            df_resumo = pd.DataFrame({
                'Analista': [analista],
                'TMO Geral': [tmo_geral],
                'TMO Cadastro': [tmo_cadastro],
                'Quantidade Cadastro': [total_cadastros],
                'Quantidade Total': [total_protocolos],
                'Tempo Ocioso': [tempo_ocioso]
            })

            # 🔹 Converter TMO e Tempo Ocioso para HH:MM:SS
            df_resumo['TMO Geral'] = df_resumo['TMO Geral'].apply(lambda x: str(timedelta(seconds=x.total_seconds())) if pd.notnull(x) else '00:00:00')
            df_resumo['TMO Cadastro'] = df_resumo['TMO Cadastro'].apply(lambda x: str(timedelta(seconds=x.total_seconds())) if pd.notnull(x) else '00:00:00')
            df_resumo['Tempo Ocioso'] = df_resumo['Tempo Ocioso'].apply(lambda x: str(timedelta(seconds=x.total_seconds())) if pd.notnull(x) else '00:00:00')

            # 🔹 Escrever no Excel
            df_resumo.to_excel(writer, index=False, sheet_name=analista[:31])

            # 🔹 Ajustes no Excel
            workbook = writer.book
            worksheet = writer.sheets[analista[:31]]
            worksheet.set_column('A:A', 20)
            worksheet.set_column('B:F', 15)

    buffer.seek(0)

    # 🔹 Botão de download no Streamlit
    st.download_button(
        label="📊 Baixar Relatório Completo de TMO",
        data=buffer,
        file_name="relatorio_tmo_completo.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

def gerar_relatorio_html(df, data_inicio_antes, data_fim_antes, data_inicio_depois, data_fim_depois, usuarios_selecionados):
    """
    Gera um relatório HTML comparando o TMO antes e depois da mudança.
    """

    # 🔹 Filtrar apenas os usuários selecionados e as tarefas finalizadas como 'CADASTRADO'
    df = df[(df['USUÁRIO QUE CONCLUIU A TAREFA'].isin(usuarios_selecionados)) & (df['FINALIZAÇÃO'] == 'CADASTRADO')]

    # 🔹 Filtrar pelo período antes e depois da mudança
    df_antes = df[
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date >= data_inicio_antes) &
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date <= data_fim_antes)
    ]
    df_depois = df[
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date >= data_inicio_depois) &
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date <= data_fim_depois)
    ]

    # 🔹 Calcular TMO e quantidade de tarefas por analista
    df_tmo_antes = df_antes.groupby('USUÁRIO QUE CONCLUIU A TAREFA').agg(
        TMO=('TEMPO MÉDIO OPERACIONAL', lambda x: x.mean() if len(x) > 0 else pd.Timedelta(0)),
        Quantidade=('DATA DE CONCLUSÃO DA TAREFA', 'count')
    ).reset_index()

    df_tmo_depois = df_depois.groupby('USUÁRIO QUE CONCLUIU A TAREFA').agg(
        TMO=('TEMPO MÉDIO OPERACIONAL', lambda x: x.mean() if len(x) > 0 else pd.Timedelta(0)),
        Quantidade=('DATA DE CONCLUSÃO DA TAREFA', 'count')
    ).reset_index()

    # 🔹 Unir os dois DataFrames para comparação
    df_comparativo = pd.merge(df_tmo_antes, df_tmo_depois, on="USUÁRIO QUE CONCLUIU A TAREFA", how="outer", suffixes=("_antes", "_depois"))

    # 🔹 Função para formatar TMO no formato HH:MM:SS
    def format_tmo(value):
        if pd.isnull(value) or value == pd.Timedelta(0):
            return "00:00:00"
        total_seconds = value.total_seconds()
        return f"{int(total_seconds // 3600):02}:{int((total_seconds % 3600) // 60):02}:{int(total_seconds % 60):02}"

    df_comparativo['TMO_antes'] = df_comparativo['TMO_antes'].apply(format_tmo)
    df_comparativo['TMO_depois'] = df_comparativo['TMO_depois'].apply(format_tmo)

    # 🔹 Criar os dados para o gráfico
    nomes_analistas = df_comparativo['USUÁRIO QUE CONCLUIU A TAREFA'].tolist()
    tmo_antes_legenda = df_comparativo['TMO_antes'].tolist()
    tmo_depois_legenda = df_comparativo['TMO_depois'].tolist()
    tmo_antes_numerico = [int(pd.to_timedelta(t).total_seconds() // 60) for t in tmo_antes_legenda]
    tmo_depois_numerico = [int(pd.to_timedelta(t).total_seconds() // 60) for t in tmo_depois_legenda]

    # 🔹 Criar a tabela HTML
    tabela_html = ""
    for _, row in df_comparativo.iterrows():
        tabela_html += f"""
        <tr>
            <td>{row['USUÁRIO QUE CONCLUIU A TAREFA']}</td>
            <td>{row['TMO_antes']}</td>
            <td>{row['TMO_depois']}</td>
        </tr>
        """

    # 🔹 Criar o HTML final
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Relatório de TMO</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels"></script>
        <style>
            body {{ font-family: Arial, sans-serif; background-color: #f4f4f4; margin: 0; padding: 20px; }}
            .container {{ max-width: 900px; background-color: white; padding: 20px; border-radius: 10px; margin: auto; }}
            .header {{ text-align: center; padding-bottom: 20px; }}
            .header h1 {{ color: #333; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; text-align: center; padding: 10px; }}
            th {{ background-color: #FF5500; color: white; }}
            .header img {{ width: 150px; margin: 10px auto;}}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <img src="https://finchsolucoes.com.br/img/fefdd9df-1bd3-4107-ab22-f06d392c1f55.png" alt="Finch Soluções">
                <h1>Relatório de TMO</h1>
                <h2>Comparação entre Períodos</h2>
            </div>
            <canvas id="tmoChart" width="400" height="200"></canvas>
            <script>
                Chart.register(ChartDataLabels);
                var ctx = document.getElementById('tmoChart').getContext('2d');
                var tmoChart = new Chart(ctx, {{
                    type: 'bar',
                    data: {{
                        labels: {nomes_analistas},
                        datasets: [
                            {{
                                label: 'TMO Antes',
                                data: {tmo_antes_numerico},
                                backgroundColor: '#FF5500',
                                borderRadius: 10
                            }},
                            {{
                                label: 'TMO Depois',
                                data: {tmo_depois_numerico},
                                backgroundColor: '#330066',
                                borderRadius: 10
                            }}
                        ]
                    }},
                    options: {{
                        responsive: true,
                        plugins: {{
                            datalabels: {{
                                anchor: 'end',
                                align: 'top',
                                color: '#000',
                                font: {{
                                    size: 10  // 🔹 Diminuindo o tamanho da fonte dos valores no topo
                                }},
                                formatter: function(value, context) {{
                                    return context.dataset.label === 'TMO Antes' 
                                        ? {tmo_antes_legenda}[context.dataIndex] 
                                        : {tmo_depois_legenda}[context.dataIndex];
                                }}
                            }},
                            tooltip: {{
                                callbacks: {{
                                    label: function(tooltipItem) {{
                                        return tooltipItem.dataset.label + ': ' + {tmo_antes_legenda}[tooltipItem.dataIndex] 
                                            + ' → ' + {tmo_depois_legenda}[tooltipItem.dataIndex];
                                    }}
                                }}
                            }}
                        }},
                        scales: {{
                            y: {{
                                beginAtZero: true
                            }}
                        }}
                    }}
                }});
            </script>
            <table>
                <tr>
                    <th>Analista</th>
                    <th>TMO Antes</th>
                    <th>TMO Depois</th>
                </tr>
                {tabela_html}
            </table>
        </div>
    </body>
    </html>
    """

    return html_content

# **🔹 Função para baixar o HTML**
def download_html(df, data_inicio_antes, data_fim_antes, data_inicio_depois, data_fim_depois, usuarios_selecionados):
    html_content = gerar_relatorio_html(df, data_inicio_antes, data_fim_antes, data_inicio_depois, data_fim_depois, usuarios_selecionados)
    buffer = BytesIO()
    buffer.write(html_content.encode("utf-8"))
    buffer.seek(0)

    st.download_button(
        label="Baixar Relatório em HTML",
        data=buffer,
        file_name="relatorio_tmo.html",
        mime="text/html"
    )

def gerar_relatorio_html_tmo(df, data_inicio, data_fim):
    """
    Gera um relatório HTML de TMO de Cadastro com um gráfico e tabela detalhada.

    Parâmetros:
        - df: DataFrame contendo os dados de TMO.
        - data_inicio, data_fim: Período selecionado.
    """

    # Filtrar apenas cadastros e o período selecionado
    df_filtrado = df[
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date >= data_inicio) &
        (df['DATA DE CONCLUSÃO DA TAREFA'].dt.date <= data_fim) &
        (df['FINALIZAÇÃO'] == 'CADASTRADO')
    ]

    # Calcular TMO médio geral
    tmo_medio_geral = df_filtrado['TEMPO MÉDIO OPERACIONAL'].mean()
    
    # Agrupar dados por analista
    df_tmo_analista = df_filtrado.groupby('USUÁRIO QUE CONCLUIU A TAREFA').agg(
        TMO=('TEMPO MÉDIO OPERACIONAL', lambda x: x.mean() if len(x) > 0 else pd.Timedelta(0)),
        Quantidade=('DATA DE CONCLUSÃO DA TAREFA', 'count')
    ).reset_index()

    # Formatar TMO para exibição
    df_tmo_analista['TMO'] = df_tmo_analista['TMO'].apply(
        lambda x: f"{int(x.total_seconds() // 3600):02}:{int((x.total_seconds() % 3600) // 60):02}:{int(x.total_seconds() % 60):02}"
    )

    # Organizar os dados para gráfico
    nomes_analistas = df_tmo_analista['USUÁRIO QUE CONCLUIU A TAREFA'].tolist()
    tmo_valores = [int(pd.Timedelta(tmo).total_seconds() // 60) for tmo in df_tmo_analista['TMO']]  # Converter para minutos
    tmo_labels = df_tmo_analista['TMO'].tolist()

    # Criar a tabela HTML
    tabela_html = ""
    for _, row in df_tmo_analista.iterrows():
        tabela_html += f"""
        <tr>
            <td>{row['USUÁRIO QUE CONCLUIU A TAREFA']}</td>
            <td>{row['TMO']}</td>
            <td>{row['Quantidade']}</td>
            <td>{data_inicio.strftime('%B/%Y')}</td>
        </tr>
        """

    # Criar o HTML final
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Relatório de Produtividade</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels"></script>
        <style>
            body {{ font-family: Arial, sans-serif; background-color: #f4f4f4; margin: 0; padding: 20px; }}
            .container {{ max-width: 1280px; background-color: white; padding: 20px; border-radius: 15px; margin: auto; }}
            .header {{ text-align: center; padding-bottom: 20px; }}
            .header h1 {{ color: #333; }}
            .highlight {{ background-color: #FF5500; color: white; padding: 10px; text-align: center; border-radius: 10px; font-size: 16px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            table, th, td {{ border: 1px solid #ddd; text-align: center; }}
            th {{ background-color: #FF5500; color: white; padding: 10px; }}
            td {{ padding: 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <img src="https://finchsolucoes.com.br/img/fefdd9df-1bd3-4107-ab22-f06d392c1f55.png" alt="Finch Soluções" width="150px">
                <h1>Relatório de Produtividade</h1>
                <h2>{data_inicio.strftime('%B/%Y')}</h2>
            </div>
            <div class="highlight">
                <p style="font-size: 25px;"><strong>Média de TMO de Cadastro</strong> <br>{tmo_medio_geral}</p>
            </div>

            <canvas id="tmoChart" width="400" height="200"></canvas>

            <script>
                Chart.register(ChartDataLabels);
                var ctx = document.getElementById('tmoChart').getContext('2d');
                var tmoData = {tmo_labels};
                var tmoChart = new Chart(ctx, {{
                    type: 'bar',
                    data: {{
                        labels: {nomes_analistas},
                        datasets: [{{
                            label: 'TMO de Cadastro',
                            data: {tmo_valores},
                            backgroundColor: ['#330066', '#FF5500', '#330066', '#FF5500', '#330066', '#FF5500', '#330066', '#FF5500', '#330066'],
                            borderRadius: 10
                        }}]
                    }},
                    options: {{
                        responsive: true,
                        plugins: {{
                            datalabels: {{
                                anchor: 'end',
                                align: 'top',
                                formatter: (value, ctx) => tmoData[ctx.dataIndex],
                                color: '#000'
                            }},
                            tooltip: {{
                                callbacks: {{
                                    label: function(tooltipItem) {{
                                        return tmoData[tooltipItem.dataIndex];
                                    }}
                                }}
                            }}
                        }},
                        scales: {{
                            y: {{
                                beginAtZero: true
                            }}
                        }}
                    }}
                }});
            </script>

            <table>
                <tr>
                    <th>Analista</th>
                    <th>TMO de Cadastro</th>
                    <th>Quantidade de Cadastro</th>
                    <th>Mês de Referência</th>
                </tr>
                {tabela_html}
            </table>
        </div>
    </body>
    </html>
    """

    return html_content


def download_html_tmo(df, data_inicio, data_fim):
    """
    Função para gerar e baixar o relatório HTML de TMO.
    """
    html_content = gerar_relatorio_html_tmo(df, data_inicio, data_fim)
    buffer = BytesIO()
    buffer.write(html_content.encode("utf-8"))
    buffer.seek(0)

    st.download_button(
        label="Baixar Relatório HTML de TMO",
        data=buffer,
        file_name="relatorio_tmo.html",
        mime="text/html"
    )

# FILAS - INCIDENTE, CADASTRO ROBO E CADASTRO ANS - CONTAGEM DA QUANTIDADE DE TAREFAS QEU ENTRARAM POR DIA (PANDAS)
# CRIAÇÃO DO PROTOCOLO -> .cont()
# finalização NA - TIRAR A SIUTAÇÃO COMO CANCELADA E VERIFICAR DESTINO DA TAREFA

# Lista de colunas essenciais
COLUNAS_ESSENCIAIS = [
    'DATA CRIAÇÃO PROTOCOLO',
    'DATA CRIAÇÃO DA TAREFA',
    'DATA DE INÍCIO DA TAREFA',
    'DATA DE CONCLUSÃO DA TAREFA',
    'TEMPO MÉDIO OPERACIONAL',
    'NÚMERO DO PROTOCOLO',
    'CLASSIFICAÇÃO',
    'FILA',
    'ID TAREFA',
    'TAREFA',
    'SITUAÇÃO DA TAREFA',
    'USUÁRIO QUE CONCLUIU A TAREFA',
    'ID FINALIZAÇÃO',
    'FINALIZAÇÃO'
]

def load_sla_data(usuario):
    """
    Carrega ou cria a planilha de SLA para o usuário especificado.
    
    Parâmetros:
        - usuario: Nome do usuário para identificar o arquivo de SLA.
        
    Retorna:
        - Um DataFrame com os dados de SLA.
    """
    sla_file = f'sla_amil_{usuario}.xlsx'  # Nome do arquivo de SLA
    try:
        if os.path.exists(sla_file):
            df_sla = pd.read_excel(sla_file)

            # Adiciona colunas ausentes no arquivo existente
            for coluna in COLUNAS_ESSENCIAIS:
                if coluna not in df_sla.columns:
                    df_sla[coluna] = None
        else:
            raise FileNotFoundError
        
    except (FileNotFoundError, ValueError, OSError):
        # Cria um DataFrame vazio com as colunas essenciais e salva um novo arquivo
        df_sla = pd.DataFrame(columns=COLUNAS_ESSENCIAIS)
        df_sla.to_excel(sla_file, index=False)
    
    return df_sla

def save_sla_data(df, usuario):
    """
    Salva o DataFrame de SLA no arquivo correspondente ao usuário,
    verificando e evitando duplicatas.

    Parâmetros:
        - df: DataFrame com os dados de SLA.
        - usuario: Nome do usuário para identificar o arquivo de SLA.
    """
    sla_file = f'sla_amil_{usuario}.xlsx'  # Nome do arquivo de SLA

    # Carregar os dados existentes
    if os.path.exists(sla_file):
        existing_data = pd.read_excel(sla_file)
    else:
        existing_data = pd.DataFrame(columns=COLUNAS_ESSENCIAIS)

    # Adicionar colunas ausentes antes de salvar
    for coluna in COLUNAS_ESSENCIAIS:
        if coluna not in df.columns:
            df[coluna] = None
        if coluna not in existing_data.columns:
            existing_data[coluna] = None

    # Combinar os novos dados com os existentes, evitando duplicatas
    combined_data = pd.concat([existing_data, df]).drop_duplicates(subset=COLUNAS_ESSENCIAIS, keep='first')

    # Contar o número de linhas que não foram adicionadas
    linhas_nao_salvas = len(existing_data) + len(df) - len(combined_data)

    # Salvar o DataFrame combinado no arquivo Excel
    combined_data.to_excel(sla_file, index=False)

    return linhas_nao_salvas

def calcular_entrada_protocolos_por_dia(df):
    """
    Calcula a entrada de protocolos por dia para filas específicas e formata o resultado.

    Parâmetros:
        - df: DataFrame com os dados de SLA.

    Retorna:
        - Um DataFrame formatado no estilo hierárquico.
    """
    # Filtrar filas e tarefas específicas
    filas_interessadas = ['CADASTRO ROBÔ', 'INCIDENTE PROCESSUAL', 'CADASTRO ANS']
    tarefas_interessadas = ['ATUALIZAR', 'CADASTRAR ROBO', 'CADASTRAR ANS']

    df_filtrado = df[
        (df['FILA'].isin(filas_interessadas)) &
        (df['TAREFA'].isin(tarefas_interessadas))
    ]

    # Garantir que a coluna 'DATA CRIAÇÃO PROTOCOLO' esteja no formato datetime
    df_filtrado['DATA CRIAÇÃO PROTOCOLO'] = pd.to_datetime(df_filtrado['DATA CRIAÇÃO PROTOCOLO'], errors='coerce')

    # Agrupar por fila e data (apenas dia)
    df_agrupado = df_filtrado.groupby([
        'DATA CRIAÇÃO PROTOCOLO',
        'FILA'
    ]).size().reset_index(name='Contagem de Protocolos')

    # Renomear e formatar a data
    df_agrupado = df_agrupado.rename(columns={'DATA CRIAÇÃO PROTOCOLO': 'Data', 'FILA': 'Fila'})
    df_agrupado['Data'] = df_agrupado['Data'].dt.strftime('%d/%m/%Y')

    # Agrupar por Data para calcular o total diário
    totais_diarios = df_agrupado.groupby('Data')['Contagem de Protocolos'].sum().reset_index()
    totais_diarios = totais_diarios.rename(columns={'Contagem de Protocolos': 'Total Diário'})

    # Adicionar os totais diários ao DataFrame
    df_formatado = pd.concat([totais_diarios, df_agrupado], ignore_index=True).sort_values(by='Data')

    # Organizar o DataFrame para o formato hierárquico
    final_data = []
    for data in df_formatado['Data'].unique():
        daily_total = totais_diarios[totais_diarios['Data'] == data]['Total Diário'].values[0]
        final_data.append({'Data': data, 'Fila': '', 'Contagem de Protocolos': daily_total})
        filas_do_dia = df_agrupado[df_agrupado['Data'] == data]
        for _, row in filas_do_dia.iterrows():
            final_data.append({'Data': '', 'Fila': row['Fila'], 'Contagem de Protocolos': row['Contagem de Protocolos']})

    df_final_formatado = pd.DataFrame(final_data)

    return df_final_formatado

def calcular_entrada_por_dia_e_fila(df):
    """
    Calcula a entrada de protocolos por dia e por fila no formato hierárquico.
    """

    # Filtrar apenas as filas e tarefas desejadas
    filas_interessadas = ["CADASTRO ROBÔ", "INCIDENTE PROCESSUAL", "CADASTRO ANS"]
    tarefas_interessadas = ["ATUALIZAR", "CADASTRAR ROBO", "CADASTRAR ANS"]

    df_filtrado = df[
        (df["FILA"].isin(filas_interessadas)) &
        (df["FINALIZAÇÃO"].isin(tarefas_interessadas))
    ]

    # Garantir que as datas estão no formato correto
    df_filtrado['DATA CRIAÇÃO PROTOCOLO'] = pd.to_datetime(df_filtrado['DATA CRIAÇÃO PROTOCOLO'], errors='coerce')
    df_filtrado = df_filtrado.dropna(subset=['DATA CRIAÇÃO PROTOCOLO'])  # Remover linhas com datas inválidas

    # Agrupar por data e fila e contar os protocolos criados
    df_entrada = (
        df_filtrado.groupby(['DATA CRIAÇÃO PROTOCOLO', 'FILA'])
        .size()
        .reset_index(name='Quantidade')
    )

    # Adicionar uma coluna para totalizar por dia
    df_totais = (
        df_entrada.groupby('DATA CRIAÇÃO PROTOCOLO')['Quantidade']
        .sum()
        .reset_index()
        .rename(columns={'Quantidade': 'Total'})
    )

    # Mesclar os dados para exibir no formato desejado
    df_entrada_formatado = df_entrada.merge(df_totais, on='DATA CRIAÇÃO PROTOCOLO', how='left')

    # Ordenar por data
    df_entrada_formatado = df_entrada_formatado.sort_values(by=['DATA CRIAÇÃO PROTOCOLO', 'FILA'])

    # Formatar a data como DD/MM/AAAA
    df_entrada_formatado['DATA CRIAÇÃO PROTOCOLO'] = df_entrada_formatado['DATA CRIAÇÃO PROTOCOLO'].dt.strftime('%d/%m/%Y')

    return df_entrada_formatado


# Exibindo no Streamlit
def exibir_entrada_por_dia(df_entrada_formatado):
    """
    Exibe os dados hierarquicamente no estilo solicitado no Streamlit.
    """

    if df_entrada_formatado.empty:
        st.write("Não há dados para exibir.")
        return

    dias_unicos = df_entrada_formatado['DATA CRIAÇÃO PROTOCOLO'].unique()

    for dia in dias_unicos:
        st.write(f"**{dia}**")  # Exibe a data em negrito
        df_dia = df_entrada_formatado[df_entrada_formatado['DATA CRIAÇÃO PROTOCOLO'] == dia]
        
        for _, row in df_dia.iterrows():
            st.write(f"- {row['FILA']}: {row['Quantidade']}")
        
        total = df_dia['Total'].iloc[0]  # O total é o mesmo para todas as linhas do mesmo dia
        st.write(f"**Total: {total}**")


def gerar_planilha_sla(df):
    """
    Gera uma planilha Excel com abas separadas por dia, contendo as filas, entrada e quantidade tratada de protocolos.

    Parâmetros:
        - df: DataFrame com os dados de SLA.

    Retorna:
        - Um botão de download para a planilha gerada.
    """

    # Filtrar filas e tarefas específicas
    filas_interessadas = [' CADASTRO ROBÔ', 'INCIDENTE PROCESSUAL', 'CADASTRO ANS']

    # Garantir que as colunas de data estão no formato datetime
    df['DATA CRIAÇÃO PROTOCOLO'] = pd.to_datetime(df['DATA CRIAÇÃO PROTOCOLO'], errors='coerce')
    df['DATA DE CONCLUSÃO DA TAREFA'] = pd.to_datetime(df['DATA DE CONCLUSÃO DA TAREFA'], errors='coerce')

    # Criar um arquivo Excel em memória
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        for dia, df_dia in df.groupby(df['DATA CRIAÇÃO PROTOCOLO'].dt.date):
            # Filtrar apenas as filas relevantes para o dia
            df_dia = df_dia[df_dia['FILA'].isin(filas_interessadas)]
            
            # Calcular entradas e tratados por fila
            entradas = df_dia.groupby('FILA').size().reset_index(name='ENTRADA')
            tratados = df_dia[df_dia['DATA DE CONCLUSÃO DA TAREFA'].notnull()].groupby('FILA').size().reset_index(name='TRATADOS')

            # Mesclar entradas e tratados
            resultado = pd.merge(entradas, tratados, on='FILA', how='outer').fillna(0)
            resultado['ENTRADA'] = resultado['ENTRADA'].astype(int)
            resultado['TRATADOS'] = resultado['TRATADOS'].astype(int)

            # Adicionar total e SLA
            total_entradas = resultado['ENTRADA'].sum()
            total_tratados = resultado['TRATADOS'].sum()
            sla = f"{(total_tratados / total_entradas) * 100:.2f}%" if total_entradas > 0 else "0%"

            # Adicionar as linhas de total e SLA
            resultado = pd.concat([
                resultado,
                pd.DataFrame({'FILA': ['TOTAL DE ENTRADAS', 'SLA'], 'ENTRADA': [total_entradas, None], 'TRATADOS': [total_tratados, sla]})
            ], ignore_index=True)

            # Escrever no Excel
            resultado.to_excel(writer, index=False, sheet_name=dia.strftime('%d-%m-%Y'))

            # Formatar o Excel
            worksheet = writer.sheets[dia.strftime('%d-%m-%Y')]
            worksheet.set_column('A:A', 20)  # Largura da coluna FILA
            worksheet.set_column('B:B', 10)  # Largura da coluna ENTRADA
            worksheet.set_column('C:C', 10)  # Largura da coluna TRATADOS

    buffer.seek(0)

    # Botão de download
    st.download_button(
        label="Baixar Planilha SLA",
        data=buffer,
        file_name=f"SLA_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

def calcular_sla_por_fila(df, data_inicio, data_fim):
    """
    Calcula o SLA por fila, considerando o intervalo de datas no formato DD/MM/AAAA.
    
    Parâmetros:
        - df: DataFrame contendo os dados de SLA.
        - data_inicio: Data de início do filtro (formato DD/MM/AAAA).
        - data_fim: Data de fim do filtro (formato DD/MM/AAAA).
    
    Retorna:
        - resumo: DataFrame com as informações de SLA por fila.
        - sla_geral: Percentual de SLA geral (todas as filas).
    """
    filas = [' CADASTRO ROBÔ', 'INCIDENTE PROCESSUAL', 'CADASTRO ANS']
    tarefas_interesse = ['CADASTRAR ROBO', 'CADASTRAR ANS', 'ATUALIZAR']
    prazo_sla = 3  # SLA de D+3

    # Garantir que as colunas de data sejam convertidas corretamente
    df['DATA CRIAÇÃO PROTOCOLO'] = pd.to_datetime(df['DATA CRIAÇÃO PROTOCOLO'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    df['DATA DE CONCLUSÃO DA TAREFA'] = pd.to_datetime(df['DATA DE CONCLUSÃO DA TAREFA'], format='%d/%m/%Y %H:%M:%S', errors='coerce')

    # Filtrar pelo intervalo de datas no formato correto
    df = df[
        (df['DATA CRIAÇÃO PROTOCOLO'] >= pd.to_datetime(data_inicio, dayfirst=True)) & 
        (df['DATA CRIAÇÃO PROTOCOLO'] <= pd.to_datetime(data_fim, dayfirst=True))
    ]

    # Filtrar apenas as filas de interesse
    df = df[df['FILA'].isin(filas)]

    # Filtrar apenas as tarefas de interesse
    df = df[df['TAREFA'].isin(tarefas_interesse)]

    # Calcular se o SLA foi cumprido (D+3)
    df['SLA_OK'] = (
        (df['DATA DE CONCLUSÃO DA TAREFA'] - df['DATA CRIAÇÃO PROTOCOLO']).dt.days <= prazo_sla
    )

    # Agrupar por fila e calcular as métricas
    resumo = df.groupby('FILA').agg(
        ENTRADAS=('DATA CRIAÇÃO PROTOCOLO', 'count'),
        TRATADOS=('DATA DE CONCLUSÃO DA TAREFA', 'count'),
        SLA_OK=('SLA_OK', 'sum')
    ).reset_index()

    # Calcular o percentual de SLA para cada fila
    resumo['% SLA'] = ((resumo['SLA_OK'] / resumo['ENTRADAS']) * 100).round(2)

    # Calcular o SLA geral
    sla_geral = (resumo['SLA_OK'].sum() / resumo['ENTRADAS'].sum() * 100).round(2)

    return resumo, sla_geral

