import numpy as np
import matplotlib.pyplot as plt

# Initial investment
initial_investment = 1

delta_roi=0.05

decay=3.5

# Function for marginal daily ROI: 0.10 * e^(-3.5x), where x is in thousands of dollars
def marginal_roi(x):
    return delta_roi/decay * (1- np.exp(-decay * x/(100)))/(x/100)

# Simulation parameters
days = 150  # Number of days to simulate
money = initial_investment
money_over_time = [money]

# Simulate accumulation over days
for day in range(1, days + 1):
    roi = marginal_roi(money)
    money += roi*money
    if day%10==0:
        print(roi, money)
    money_over_time.append(money)

# Plotting
plt.figure(figsize=(10, 6))
plt.plot(range(days + 1), money_over_time, label="Accumulated Money")
plt.xlabel("Days")
plt.ylabel("Total Money")
plt.title("Accumulated Money Over Time with Decreasing Marginal ROI")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()
