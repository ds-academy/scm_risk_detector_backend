import numpy as np
from numpy import random

lotto = np.random.choice(np.arage(), size=(6, 5), replace=True)

print(lotto)