import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import plotly.express as px

username = 'root'
password = 'Dhanush5'
host = 'localhost'
port = '3306'
database = 'ANALYSIS'
table_name = 'population_spread'
connection_str = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'


##################################################################################
engine = create_engine(connection_str)
query = f"SELECT * FROM {table_name}"
df = pd.read_sql(query, con=engine)

figure,ax =plt.subplots()
ax.set_facecolor("black")
ax.pie(df['population_percentage'], labels=df['city_name'], autopct='%1.1f%%')
ax.set_title('Population Distribution by State')
ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

# Display the pie chart in Streamlit
st.title("Population Distribution by State")
st.pyplot(figure)
##################################################################################
table_name="property_price_per_locality"
query = f"SELECT * FROM {table_name}"
df = pd.read_sql(query, con=engine)
st.title('Real Estate Prices by Locality')
selected_locality = st.selectbox('Select Locality', df['locality_name'].unique())

# Filter data based on selected locality
filtered_data = df[df['locality_name'] == selected_locality]

# Streamlit layout to align plots and content
col1, col2 = st.columns([2, 3])

# Display the DataFrame in the first column
with col1:
    st.write("## Real Estate Prices by Locality")
    st.write(f"Showing data for {selected_locality}")
    st.write(filtered_data)

# Create a bar plot based on the filtered data in the second column
with col2:
    plt.figure(figsize=(8, 6))
    plt.bar(filtered_data['property_type'], filtered_data['average_price'])
    plt.xlabel('Property Type')
    plt.ylabel('Price')
    plt.title(f'Prices for {selected_locality}')
    st.pyplot(plt)
##################################################################################
table_name="bhk_price_per_locality"
query = f"SELECT * FROM {table_name}"
df = pd.read_sql(query, con=engine)

def extract_numeric(s):
    return s.split()[0]
# Function to remove '\r' from the suburban_name column
def remove_carriage_return(s):
    return s.replace(r'\r', '')
# Streamlit App
st.title('Real Estate Prices by BHK and Furnished Status')

# Select suburban name from dropdown menu
suburban_names = df['suburban_name'].apply(remove_carriage_return).unique()
selected_suburban = st.selectbox('Select Suburban Name', suburban_names)

# Filter data based on selected suburban name
filtered_data = df[df['suburban_name'].apply(remove_carriage_return) == selected_suburban]

# Filtered plots for each furnished status
fig, axes = plt.subplots(1, 3, figsize=(15, 5))

for i, status in enumerate(['Furnished', 'Semi-Furnished', 'Unfurnished']):
    ax = axes[i]
    filtered_status = filtered_data[filtered_data['is_furnished'] == status]
    filtered_status['numeric_bhk'] = filtered_status['number_of_bhk'].apply(extract_numeric)
    filtered_status['numeric_bhk'] = pd.to_numeric(filtered_status['numeric_bhk'])

    ax.bar(filtered_status['numeric_bhk'], filtered_status['average_price'])
    ax.set_title(f'{status} - {selected_suburban}')
    ax.set_xlabel('Number of BHK')
    ax.set_ylabel('Average Price')

plt.tight_layout()
st.pyplot(fig)

##################################################################################
table_name="price_trend_over_month"
query = f"SELECT * FROM {table_name}"
df = pd.read_sql(query, con=engine)
def filter_last_30_days(data):
    return data[data['time_column'] >= max(data['time_column']) - 29]

st.title('Price Change Data for Last 30 Days')

# Select property type from dropdown menu
property_types = df['property_type'].unique()
selected_property = st.selectbox('Select Property Type', property_types)

# Filter data for the last 30 days and based on selected property type
filtered_data = filter_last_30_days(df[df['property_type'] == selected_property])

# Display the filtered data in a table
st.write(f"Data for {selected_property} in the Last 30 Days")
st.write(filtered_data)

# Plotting the trend of price changes over the last 30 days
plt.figure(figsize=(10, 6))
plt.plot(filtered_data['time_column'], filtered_data['price_change'], marker='o')
plt.title(f'Price Change Trend for {selected_property} (Last 30 Days)')
plt.xlabel('Time Column')
plt.ylabel('Price Change (%)')
plt.grid(True)
st.pyplot(plt)

##################################################################################
table_name="builders_analysis"
query = f"SELECT * FROM {table_name}"
df = pd.read_sql(query, con=engine)
st.title('Builder Status Distribution')

# Create a pie chart for builder status distribution
builder_status_counts = df['builder_status'].value_counts()
fig, ax = plt.subplots()
ax.pie(builder_status_counts, labels=builder_status_counts.index, autopct='%1.1f%%', startangle=90)
ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
# Display the pie chart in Streamlit
st.pyplot(fig)

##################################################################################
table_name="geospatial_visualization"
query = f"SELECT * FROM {table_name}"
df = pd.read_sql(query, con=engine)

color_mapping = {'y': 'yellow', 'g': 'green', 'o': 'orange', 'v': 'violet','i':'indigo','b':'blue','r' : 'red'}
# Streamlit App
st.title('India Map with Latitude and Longitude Points')
df['color'] = df['demand'].map(color_mapping)
# Create an interactive scatter plot using Plotly Express
fig = px.scatter_mapbox(df, lat='latitude', lon='longitude', color='demand', zoom=5, height=600,color_discrete_map=color_mapping)
# Customize map appearance
fig.update_layout(
    mapbox_style='open-street-map',
    margin=dict(r=0, l=0, t=0, b=0))
# Display the plot in Streamlit
st.plotly_chart(fig)

##################################################################################
table_name="investment_per_city"
query = f"SELECT * FROM {table_name}"
df = pd.read_sql(query, con=engine)
st.title('Investment Trends by City')

# Create a line chart for each city
col1, col2 = st.columns([1, 1])
with col1:
    fig = px.line(df, x='year', y='investment', color='city_name', markers=True)

    # Customize the layout
    fig.update_layout(
        title='Investment Trends by City',
        xaxis_title='Year',
        yaxis_title='Investment',
        legend_title='City'
    )
    fig.update_layout(height=400, width=400)
    # Display the plot in Streamlit
    st.plotly_chart(fig)

with col2:
    selected_city_bar = st.selectbox('Select City for Bar Plot', df['city_name'].unique())
    filtered_data_bar = df[df['city_name'] == selected_city_bar]
    ordered_data_bar = filtered_data_bar.sort_values(by='year', key=lambda x: pd.Categorical(x, categories=['Before_last_year', 'Last_year', 'This_year'], ordered=True))

    plt.figure(figsize=(5, 3))
    plt.bar(ordered_data_bar['year'], ordered_data_bar['investment'])
    plt.xlabel('Year')
    plt.ylabel('Investment')
    plt.title(f'Investment in {selected_city_bar}')
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability

    # Display the bar plot
    st.pyplot(plt)
    