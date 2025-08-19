import streamlit as st
import snowflake.connector
import pandas as pd
import random
import time

# --- Snowflake Connection ---
# Securely connects to Snowflake using Streamlit's Secrets Management.
def snowflake_connection():
    """Establishes and returns a connection to Snowflake using st.secrets."""
    try:
        conn = snowflake.connector.connect(
            **st.secrets["snowflake"]
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        st.stop()

# --- Database Functions ---
def run_query(query):
    """Executes a query and returns the result as a pandas DataFrame."""
    conn = snowflake_connection()
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def execute_command(command, params=None):
    """Executes a command (INSERT, UPDATE) against the database."""
    conn = snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(command, params)
        return True
    except Exception as e:
        st.error(f"Command failed: {e}")
        return False
    finally:
        if conn:
            conn.close()

# --- Main Application Logic ---
st.set_page_config(page_title="Bootcamp Rally Racing", layout="wide")
st.title("Bootcamp Rally Racing Management App ")
st.write("Manage your racing teams, cars, drivers, and compete in an exciting rally!")

# --- Caching Data ---
@st.cache_data(ttl=60)
def get_data():
    teams_df = run_query("SELECT * FROM BOOTCAMP_RALLY.TEAMS_DATA.TEAMS")
    drivers_df = run_query("SELECT * FROM BOOTCAMP_RALLY.TEAMS_DATA.DRIVERS")
    cars_query = """
    SELECT
        c.CAR_ID, c.TEAM_NAME, c.MODEL, c.CATEGORY_NAME,
        cat.HORSEPOWER, cat.DRIVETRAIN, cat.MIN_WEIGHT_KG,
        d.DRIVER_NAME, d.SKILL_LEVEL, d.LUCK_LEVEL
    FROM BOOTCAMP_RALLY.CARS_DATA.CARS c
    JOIN BOOTCAMP_RALLY.CARS_DATA.CAR_CATEGORIES cat ON c.CATEGORY_NAME = cat.CATEGORY_NAME
    JOIN BOOTCAMP_RALLY.TEAMS_DATA.DRIVERS d ON c.DRIVER_ID = d.DRIVER_ID;
    """
    cars_df = run_query(cars_query)
    categories_df = run_query("SELECT CATEGORY_NAME FROM BOOTCAMP_RALLY.CARS_DATA.CAR_CATEGORIES")
    return teams_df, drivers_df, cars_df, categories_df

teams_df, drivers_df, cars_df, categories_df = get_data()

# --- Display Data from Snowflake ---
st.header("Racing Teams & Budgets")
if not teams_df.empty:
    st.dataframe(teams_df, use_container_width=True)

st.header("Drivers Roster")
if not drivers_df.empty:
    st.dataframe(drivers_df, use_container_width=True)

st.header("Registered Racing Cars")
if not cars_df.empty:
    st.dataframe(cars_df, use_container_width=True)

# --- Sidebar for Actions ---
st.sidebar.header("Management Actions")

# --- Add a New Team ---
with st.sidebar.expander("👥 Add a New Team"):
    with st.form("new_team_form", clear_on_submit=True):
        new_team_name = st.text_input("Team Name")
        initial_budget = st.number_input("Initial Budget ($)", min_value=1000, value=50000)
        if st.form_submit_button("Create Team"):
            if new_team_name:
                sql = "INSERT INTO BOOTCAMP_RALLY.TEAMS_DATA.TEAMS (team_name, budget) VALUES (%s, %s)"
                if execute_command(sql, (new_team_name, initial_budget)):
                    st.success(f"Team '{new_team_name}' created!")
                    st.cache_data.clear()
                    st.experimental_rerun()

# --- Add a New Driver ---
with st.sidebar.expander("👨‍🚀 Add a New Driver"):
    with st.form("new_driver_form", clear_on_submit=True):
        team_options = teams_df['TEAM_NAME'].tolist() if not teams_df.empty else []
        driver_name = st.text_input("Driver's Name")
        skill = st.slider("Skill Level", 1, 100, 50)
        luck = st.slider("Luck Level", 1, 100, 50)
        team_name = st.selectbox("Assign to Team", options=team_options)
        if st.form_submit_button("Add Driver"):
            if driver_name and team_name:
                sql = "INSERT INTO BOOTCAMP_RALLY.TEAMS_DATA.DRIVERS (driver_name, skill_level, luck_level, team_name) VALUES (%s, %s, %s, %s)"
                if execute_command(sql, (driver_name, skill, luck, team_name)):
                    st.success(f"Driver '{driver_name}' added to {team_name}!")
                    st.cache_data.clear()
                    st.experimental_rerun()

# --- Add a New Car ---
with st.sidebar.expander("➕ Add a New Car"):
    with st.form("new_car_form", clear_on_submit=True):
        team_options = teams_df['TEAM_NAME'].tolist() if not teams_df.empty else []
        category_options = categories_df['CATEGORY_NAME'].tolist() if not categories_df.empty else []
        driver_options = {f"{row.DRIVER_NAME} ({row.TEAM_NAME})": row.DRIVER_ID for index, row in drivers_df.iterrows()}

        car_model = st.text_input("Car Model")
        selected_team = st.selectbox("Car's Team", options=team_options)
        selected_category = st.selectbox("Car Category", options=category_options)
        selected_driver_label = st.selectbox("Assign Driver", options=driver_options.keys())
        
        if st.form_submit_button("Add Car"):
            if car_model and selected_team and selected_category and selected_driver_label:
                driver_id = driver_options[selected_driver_label]
                sql = "INSERT INTO BOOTCAMP_RALLY.CARS_DATA.CARS (team_name, model, category_name, driver_id) VALUES (%s, %s, %s, %s)"
                if execute_command(sql, (selected_team, car_model, selected_category, driver_id)):
                    st.success(f"Car '{car_model}' added!")
                    st.cache_data.clear()
                    st.experimental_rerun()

# --- Race Simulation ---
st.header("🏁 Rally Simulation")
if st.button("🚀 Start Race!"):
    if len(cars_df) < 2:
        st.warning("Not enough cars to start a race! You need at least 2.")
    else:
        st.info("The race is on! Simulating a 100km rally...")
        FEE = 1000
        PRIZE = len(cars_df['TEAM_NAME'].unique()) * FEE * 0.8
        
        st.write(f"**Race Details:** Fee: ${FEE:,.2f}, Prize: ${PRIZE:,.2f}")
        
        # Deduct fees
        teams_in_race = cars_df['TEAM_NAME'].unique()
        for team in teams_in_race:
            execute_command("UPDATE BOOTCAMP_RALLY.TEAMS_DATA.TEAMS SET budget = budget - %s WHERE team_name = %s", (FEE, team))
        st.success("Participation fees collected.")
        
        # Simulate race
        race_results = []
        with st.spinner('Cars are speeding down the track...'):
            for _, car in cars_df.iterrows():
                power_to_weight = car['HORSEPOWER'] / car['MIN_WEIGHT_KG']
                drivetrain_bonus = 1.05 if car['DRIVETRAIN'] == '4WD' else 1.0
                skill_factor = 1 + ((car['SKILL_LEVEL'] - 50) / 50) * 0.1 # +/- 10% effect
                luck_factor = random.uniform(1 - (car['LUCK_LEVEL'] / 1000), 1 + (car['LUCK_LEVEL'] / 1000)) # +/- up to 10%
                
                performance_score = power_to_weight * drivetrain_bonus * skill_factor * luck_factor
                time_taken = 100 / performance_score
                
                race_results.append({
                    "Team": car['TEAM_NAME'], "Driver": car['DRIVER_NAME'],
                    "Car Model": car['MODEL'], "Time": time_taken
                })
                time.sleep(0.2)

        # Show results and award prize
        results_df = pd.DataFrame(race_results).sort_values(by="Time").reset_index(drop=True)
        results_df.index += 1
        winner = results_df.iloc[0]
        
        st.subheader("🏆 Race Results! 🏆")
        st.dataframe(results_df, use_container_width=True)
        st.balloons()
        st.success(f"Winner: {winner['Driver']} for Team '{winner['Team']}'!")

        execute_command("UPDATE BOOTCAMP_RALLY.TEAMS_DATA.TEAMS SET budget = budget + %s WHERE team_name = %s", (PRIZE, winner['Team']))
        st.success(f"Awarded ${PRIZE:,.2f} to Team '{winner['Team']}'.")
        
        st.cache_data.clear()
        st.experimental_rerun()
