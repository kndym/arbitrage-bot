import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit

# Initial investment
initial_investment = 1

delta_roi = 0.05

decay = 3.5

# Function for marginal daily ROI: 0.10 * e^(-3.5x), where x is in thousands of dollars
def marginal_roi(x):
    # Ensure x is not zero to avoid division by zero in the original formula
    if x == 0:
        return 0  # Or handle as appropriate for your model
    return delta_roi / decay * (1 - np.exp(-decay * x / (100))) / (x / 100)

# Simulation parameters
days = 150  # Number of days to simulate
money = initial_investment
money_over_time = [money]
daily_profit = []
daily_growth_rate = []

# Simulate accumulation over days
for day in range(1, days + 1):
    roi = marginal_roi(money)
    profit = roi * money
    money += profit
    
    money_over_time.append(money)
    daily_profit.append(profit)
    # Ensure money_over_time[-2] is not zero to avoid division by zero
    if money_over_time[-2] != 0:
        daily_growth_rate.append((profit / money_over_time[-2]) * 100)
    else:
        daily_growth_rate.append(0) # Or handle as appropriate


# --- Logistic Curve Fitting ---

# Define the logistic function
def logistic_function(x, L, k, x0):
    return L / (1 + np.exp(-k * (x - x0)))

# Prepare data for fitting
x_data = np.array(range(days))
y_data = np.array(daily_profit)

# Initial guess for parameters (L, k, x0)
# L: A rough estimate for the maximum money accumulated (could be the max value in y_data or higher)
# k: A positive growth rate, often between 0 and 1
# x0: Midpoint, roughly half of the total days
initial_guess = [max(y_data) * 1.5, 0.1, days / 2] # Adjusted L to be potentially higher than max observed

# Fit the curve
try:
    params, covariance = curve_fit(logistic_function, x_data, y_data, p0=initial_guess, maxfev=5000)
    L_fit, k_fit, x0_fit = params

    # Generate data for the fitted curve
    y_fit = logistic_function(x_data, L_fit, k_fit, x0_fit)

    print("\n--- Logistic Curve Fitting Results ---")
    print(f"Fitted L (Max Value): {L_fit:.4f}")
    print(f"Fitted k (Growth Rate): {k_fit:.4f}")
    print(f"Fitted x0 (Midpoint): {x0_fit:.4f}")

    # Plotting Accumulated Money with Logistic Fit
    plt.figure(figsize=(10, 6))
    plt.plot(x_data, y_data, label="Accumulated Money (Simulated)", color='blue')
    plt.plot(x_data, y_fit, label=f"Logistic Fit (L={L_fit:.2f}, k={k_fit:.2f}, x0={x0_fit:.2f})", color='orange', linestyle='--')
    plt.xlabel("Days")
    plt.ylabel("Total Money")
    plt.title("Accumulated Money Over Time with Logistic Curve Fit")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

except RuntimeError as e:
    print(f"\nError - Could not fit logistic curve: {e}")
    print("Consider adjusting the initial_guess or the range of x and y values.")
    # If fitting fails, still show the original plot
    plt.figure(figsize=(10, 6))
    plt.plot(range(days + 1), money_over_time, label="Accumulated Money")
    plt.xlabel("Days")
    plt.ylabel("Total Money")
    plt.title("Accumulated Money Over Time with Decreasing Marginal ROI")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()


# Plotting Daily Profit (existing code)
plt.figure(figsize=(10, 6))
plt.plot(range(1, days + 1), daily_profit, label="Daily Profit", color='green')
plt.xlabel("Days")
plt.ylabel("Profit")
plt.title("Daily Profit Over Time")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()

# Plotting Daily Growth Rate (existing code)
plt.figure(figsize=(10, 6))
plt.plot(range(1, days + 1), daily_growth_rate, label="Daily Growth Rate (%)", color='red')
plt.xlabel("Days")
plt.ylabel("Growth Rate (%)")
plt.title("Daily Growth Rate Over Time")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()