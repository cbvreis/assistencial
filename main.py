from io import BytesIO

import streamlit as st
import pandas as pd
import cx_Oracle
import dask.dataframe as dd
import shutil
import gzip
import datetime



df = dd.read_csv('dw-assistencial.csv',sep=';',encoding= 'unicode_escape')



def download(df,origem):
        if(origem=="base"):
            filename = 'dw-assistencial.csv'
            zip_file = 'dw-assistencial.csv.gz'
        else:
            df.to_csv('saida-sql.csv')
            filename = 'saida-sql.csv'
            zip_file = 'saida-sql.csv.gz'

        a = st.markdown("<h2> Compactando arquivos... </h2>", unsafe_allow_html=True)
        gif_runner = st.image('gif.gif')

        with open(filename, 'rb') as f_in:
            with gzip.open(zip_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        gif_runner.empty()
        a.markdown("<h2> Copiando arquivos... </h2>", unsafe_allow_html=True)
        gif_runner = st.image('gif2.gif')
        shutil.copy(zip_file, '\\\\srvfiles\TEMPORARIO')
        gif_runner.empty()
        a.markdown("<h2> Database gerada com sucesso. Verificar a pasta T. </h2>", unsafe_allow_html=True)


def base():
    st.title("DW Assistencial -  BASE HISTÓRICA")
    st.sidebar.title("Base de Dados Assistencial")

    btn = st.slider('Número de linhas', 0, 1000, 25)
    if btn:
        st.dataframe(df.tail(btn))
    start_execution = st.button('Download da base')
    if start_execution:
        download(df,'base')

def main():
    #MENU LATERAL
    select = st.sidebar.selectbox('Escolha a busca...', ['Base Histórica', 'Filtros', 'SQL'], key='1')
    tst = 'f'

   #CONSULTA SQL
    if select == 'SQL':
        txt = st.text_area('Digite a consulta em SQL')
        btn_select = st.button('Consultar')
        if btn_select:
            try:
                df_sql = pd.read_sql(txt, con = conn)
                st.dataframe(df_sql.head(10))

                df_sql = pd.read_sql(txt, con=conn)
                filename = 'saida-sql.csv'
                zip_file = 'saida-sql.csv.gz'

                df_sql.to_csv(filename)

                a = st.markdown("<h2> Compactando arquivos... </h2>", unsafe_allow_html=True)
                gif_runner = st.image('gif.gif')

                with open(filename, 'rb') as f_in:
                    with gzip.open(zip_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                gif_runner.empty()
                a.markdown("<h2> Copiando arquivos... </h2>", unsafe_allow_html=True)
                gif_runner = st.image('gif2.gif')
                shutil.copy(zip_file, '\\\\srvfiles\TEMPORARIO')
                gif_runner.empty()
                a.markdown("<h2> Database gerada com sucesso. Verificar a pasta T. </h2>", unsafe_allow_html=True)

            except:
                st.text("Consulta digitada de forma incorreta.")





    elif select=='Filtros':
        st.title('Filtrando por datas')
        hoje =  datetime.datetime.now()

        date_inf = st.date_input('Data Inicial', datetime.date(2021,1,1))
        date_sup = st.date_input('Data Final', datetime.date(hoje.year,hoje.month,hoje.day))


        start_date = "2020-1-1"
        end_date = "2020-6-31"

        after_start_date = df["DATA_ATENDIMENTO_DT"] >= start_date
        st.dataframe(after_start_date.head(5))

        before_end_date = df["DATA_ATENDIMENTO_DT"] <= end_date
        between_two_dates = after_start_date & before_end_date
        filtered_dates = df.loc[between_two_dates]

        st.dataframe(filtered_dates.head(10))


    else:
        base()

if __name__ == '__main__':
    main()

