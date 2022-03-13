
import numpy as np
import matplotlib.pyplot as plt
from scipy.spatial import ConvexHull, convex_hull_plot_2d

rng = np.random.default_rng()
points = rng.random((30, 2))   # 30 random points in 2-D
hull = ConvexHull(points)


plt.plot(points[:,0], points[:,1], 'o')
plt.plot(points[hull.vertices,0], points[hull.vertices,1], 'r--', lw=2)
plt.show()