import pandas as pd
import matplotlib.pyplot as plt

# Load data 
df = pd.read_csv('rockies_pitching.csv')

# Separate home and away data
home = df[df['Home_Away']=='Home']  
away = df[df['Home_Away']=='Away']

# Calculate differences 
metrics = ['ERA','WHIP','HR9','SO9','SO/W']

diffs = pd.DataFrame(index=metrics)
for metric in metrics:
    diffs[metric] = home[metric] - away[metric] 

# Visualization
diffs.plot.bar(rot=45)
plt.title('Coors Field Effect')
plt.ylabel('Home - Away')
plt.show()

# Statistical tests
from scipy import stats

for metric in metrics:
    t, p = stats.ttest_rel(home[metric], away[metric])
    print(f"{metric}: Coors Field effect is statistically significant (p={p:.3f})")