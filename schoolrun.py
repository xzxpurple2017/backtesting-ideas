"""
Trading strategy inspired by this: https://1drv.ms/b/s!Ah_-HVgdRxvtgdF7XU2i3vdUAeixaw?e=XhnHsE
We enter on the 2nd 15 minute candle. 
Take the high of that and use that as a long entry.
Take the low of that and use that as a short entry.
If you go long and hit the low of the 15 minute, or short entry, reverse the position. 
Exit at noon.
This was tested with the Hang Seng index CFD at Dukascopy.
Data was downloaded as 1 minute tick values at 
https://www.dukascopy.com/swiss/english/marketwatch/historical/
It was then inserted into a sqlite3 database so we may gather 
from diverse data sources and normalize them.
Entries are done at 9:30 AM HKT or 1:30 AM UTC.
Exits are done at 12:00 PM HKT or 4:00 AM UTC.
"""

# TODO:
# * Fix stats for totals of stop outs, it is just per day atm
# * Show if trade for example was long first and then short to ease confusion
# * Maybe add into Pandas the different trades
# * Show if trade exited at noon HKT
# * Perhaps assume the trade exits at 90 loss when stopped out rather than high or low price?

import sqlite3
import argparse
import pandas as pd

# At the beginning of your script
parser = argparse.ArgumentParser(description='Backtest trading strategy.')
parser.add_argument('--start', type=str, required=True, help='Start date in YYYY-MM-DD format.')
parser.add_argument('--end', type=str, required=True, help='End date in YYYY-MM-DD format.')
parser.add_argument('--entries', type=int, required=False, default=8,
                    help='Number of Pandas entries to output')
args = parser.parse_args()

start_date = args.start
end_date = args.end
pandas_entries = args.entries

# Create date range in YYYY-MM-DD 00:00:00 to YYYY-MM-DD 23:59:59 format
start_date_utc = f"{start_date} 00:00:00"
end_date_utc = f"{end_date} 23:59:59"

# Connect to SQLite database
SQLITE_DB = "hk40cfd"
conn = sqlite3.connect(f"C:\\Users\\philip\\hkstocks\\{SQLITE_DB}.db")

pd.set_option('display.max_rows', pandas_entries)

# Read data into DataFrame
df = pd.read_sql_query("SELECT * FROM hk40cfd", conn)
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Set timestamp as index
df.set_index('timestamp', inplace=True)

# Sort DataFrame index to ensure it's monotonic
df.sort_index(inplace=True)

# Slice DataFrame for the specific date range
# This will include only the dates that actually exist in the DataFrame
df_sliced = df.loc[start_date_utc:end_date_utc]

# Initialize an empty list to store results
results = []

# Initialize counters for reversals, stop-outs, and total trades
REVERSAL_COUNT = 0
TOTAL_TRADES = 0

STOP_LOSS = 90.0  # Initialize stop loss level

for date, daily_data in df_sliced.groupby(df_sliced.index.date):
    second_15min_data = daily_data.between_time('01:45', '02:00')
    if second_15min_data.empty:
        continue
    high_15min = second_15min_data['high'].max()
    low_15min = second_15min_data['low'].min()

    long_entry = high_15min + 5
    short_entry = low_15min - 5

    one_min_data = daily_data.between_time('02:01', '04:00')
    if one_min_data.empty:
        continue
    ENTRY_TYPE = None
    ENTRY_PRICE = 0.0
    EXIT_PRICE = 0.0
    LAST_EXIT_PRICE = 0.0   # This is set when we are stopped out
    LAST_ENTRY_TYPE = None
    REVERESED_TRADE = False

    CUMULATIVE_LOSS = 0.0  # Initialize cumulative loss for the day
    DAILY_STOP_OUT_COUNT = 0  # Initialize daily stop-out count

    for index, row in one_min_data.iterrows():
        if ENTRY_TYPE is None:
            if row['high'] >= long_entry:
                ENTRY_TYPE = 'Long'
                ENTRY_PRICE = long_entry
            elif row['low'] <= short_entry:
                ENTRY_TYPE = 'Short'
                ENTRY_PRICE = short_entry

            if ENTRY_TYPE:
                TOTAL_TRADES += 1  # Increment total trades counter

        # Check if to reverse trade immediately
        # This can only happen if the 2nd 15-minute bar is within 90 points
        if ENTRY_TYPE == 'Long' and row['low'] <= short_entry:
            CUMULATIVE_LOSS += long_entry - short_entry
            REVERESED_TRADE = True
            REVERSAL_COUNT += 1
            ENTRY_PRICE = short_entry
            ENTRY_TYPE = 'Short'
            TOTAL_TRADES += 1
        if ENTRY_TYPE == 'Short' and row['high'] >= long_entry:
            CUMULATIVE_LOSS += long_entry - short_entry
            REVERESED_TRADE = True
            REVERSAL_COUNT += 1
            ENTRY_PRICE = long_entry
            ENTRY_TYPE = 'Long'
            TOTAL_TRADES += 1

        # However, if the 2nd 15-minute bar is wide and greater than 90 points,
        # we will want to stop ourselves out and either wait re-enter or wait
        # till the closing time at noon HKT for a loss.
        # We also want to completely exit the trade if we already reversed and
        # are stopped out.
        if ENTRY_TYPE == 'Long' and row['low'] <= (ENTRY_PRICE - STOP_LOSS):
            CUMULATIVE_LOSS += long_entry - row['low']  # Should be roughly stop loss 
            if REVERESED_TRADE or DAILY_STOP_OUT_COUNT >= 1:
                DAILY_STOP_OUT_COUNT += 1
                EXIT_PRICE = row['low']
                break
            DAILY_STOP_OUT_COUNT += 1  # Increment daily stop-out count
            ENTRY_TYPE = None
            LAST_EXIT_PRICE = row['low']
            LAST_ENTRY_TYPE = 'Long'
            continue  # Skip to next iteration
        if ENTRY_TYPE == 'Short' and row['high'] >= (ENTRY_PRICE + STOP_LOSS):
            CUMULATIVE_LOSS += row['high'] - short_entry    # Should be roughly stop loss
            if REVERESED_TRADE or DAILY_STOP_OUT_COUNT >= 1:
                DAILY_STOP_OUT_COUNT += 1
                EXIT_PRICE = row['high']
                break
            DAILY_STOP_OUT_COUNT += 1  # Increment daily stop-out count
            ENTRY_TYPE = None
            LAST_EXIT_PRICE = row['high']
            LAST_ENTRY_TYPE = 'Short'
            continue  # Skip to next iteration

    # If the trade makes it to noon HKT, exit the trade.
    if ENTRY_TYPE and EXIT_PRICE == 0.0:
        EXIT_PRICE = one_min_data.iloc[-1]['close']

    # If we were stopped out and never re-entered, set the exit price to the 
    # price we were stopped out in.
    if ENTRY_TYPE is None:
        EXIT_PRICE = LAST_EXIT_PRICE

    if ENTRY_TYPE == 'Long':
        pnl = EXIT_PRICE - ENTRY_PRICE - CUMULATIVE_LOSS
    elif ENTRY_TYPE == 'Short':
        pnl = ENTRY_PRICE - EXIT_PRICE - CUMULATIVE_LOSS
    else:
        ENTRY_TYPE = LAST_ENTRY_TYPE
        pnl = 0 - CUMULATIVE_LOSS

    if ENTRY_PRICE != 0.0:
        results.append({
            'date': date, 
            'entry_type': ENTRY_TYPE, 
            'entry_price': ENTRY_PRICE, 
            'exit_price': EXIT_PRICE, 
            'pnl': pnl, 
            'reversed': REVERESED_TRADE,
            'stop_outs': DAILY_STOP_OUT_COUNT,
            'total_loss': CUMULATIVE_LOSS
        })

results_df = pd.DataFrame(results)
print(results_df)

# Calculate and display trading statistics
total_pnl = round(results_df['pnl'].sum(), 2)
average_daily_pnl = round(results_df['pnl'].mean(), 2)
greatest_loss = round(results_df['pnl'].min(), 2)
greatest_gain = round(results_df['pnl'].max(), 2)

print("--- Statistics ---")
print(f"Total P&L over the period: {total_pnl}")
print(f"Average daily P&L: {average_daily_pnl}")
print(f"Greatest loss: {greatest_loss}")
print(f"Greatest gain: {greatest_gain}")
print(f"Number of reversals: {REVERSAL_COUNT}")
print(f"Number of stop-outs: {DAILY_STOP_OUT_COUNT}")
print(f"Total number of trades: {TOTAL_TRADES}")
