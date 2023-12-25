import streamlit as st
import plotly.express as px
import pandas as pd
from ir_util import postgre_sql_connection
import subprocess

def fetch_data(q):
    interstrate_db = postgre_sql_connection('streamlit')
    data = interstrate_db.postgre_download_data(query=q)
    return data

# setting up time series buttons
def plot_data(mode,data):
    time_buttons = [{'count': 6, 'step': 'month', 'stepmode': 'todate', 'label': '6M'},
    {'count': 1, 'step': 'year', 'stepmode': 'todate', 'label': 'YTD'},
    {'count':2,'step':'year','stepmode':'todate','label':'2Y'},
    {'count':5,'step':'year','stepmode':'todate','label':'5Y'},
    {'count':10,'step':'year','stepmode':'todate','label':'10Y'}]
# mapping the function
    plot_functions = {
        'area': px.area,
        'line': px.line,
        'bar': px.bar}
    
    if mode not in plot_functions:
        raise ValueError(f"Invalid mode: {mode}. Supported modes are {', '.join(plot_functions.keys())}.")
    
    fig = plot_functions[mode](data, x="release_date", y="actual_rate", color="currency", width=1000, height=800)
    fig.update_layout(hovermode="x unified")
    fig.update_xaxes(rangeselector={'buttons': time_buttons})
    return fig

def main():
    ''' Streamlit webpage layout
    '''
    option = st.sidebar.selectbox('Which Dashboard?', ('Historical Interest Rate','test'))
    st.header(option)

    if option == 'Historical Interest Rate':
        
        webscrape_button = st.button('Update latest data')
        if webscrape_button:
            st.write('web scraping in progress....')

            # Execute the Auto_IR_scrape.py file
            subprocess.run(["python",'Auto_IR_scrape.py'])
            st.write("\nweb scraping completed!!")
        
        selected_mode = st.sidebar.multiselect("Types", ["Line chart","Bar chart"])
        data = fetch_data("SELECT * FROM interest_rate ORDER BY release_date;")


        if not data.empty:
            if 'Line chart' in selected_mode:
                st.plotly_chart(plot_data(mode='line',data=data))
            if 'Bar chart' in selected_mode:
                st.plotly_chart(plot_data(mode='bar',data=data))    

if __name__ == "__main__":
    main()
