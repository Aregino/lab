# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Escolha um mecanismo de streaming apropriado


# CELL ********************

import matplotlib.pyplot as plt
import matplotlib.patches as patches

# Criar figura
fig, axes = plt.subplots(6, 1, figsize=(10, 15))
plt.subplots_adjust(hspace=1)

# Função para desenhar eventos
def draw_events(ax, times, y=0.5):
    for t in times:
        ax.plot(t, y, 'o', color='blue')
        ax.text(t, y+0.1, f"{t}", ha='center')

# 1. Cascata
ax = axes[0]
ax.set_title("1. Cascata (Cascade)", fontsize=12, weight="bold")
draw_events(ax, [1, 2, 3, 4])
ax.annotate("Etapa 1 → Etapa 2 → Etapa 3", xy=(2, 0.5), xytext=(5, 0.7),
            arrowprops=dict(arrowstyle="->", lw=1.5))
ax.set_xlim(0, 10)
ax.set_ylim(0, 1)
ax.axis("off")

# 2. Tumbling Window
ax = axes[1]
ax.set_title("2. Tumbling Window", fontsize=12, weight="bold")
draw_events(ax, [1.5, 2.5, 4.5, 6.2, 7.8])
for start in [0, 5]:
    rect = patches.Rectangle((start, 0.3), 5, 0.4, fill=False, edgecolor="red", linestyle="--")
    ax.add_patch(rect)
ax.set_xlim(0, 10)
ax.set_ylim(0, 1)
ax.axis("off")

# 3. Hopping Window
ax = axes[2]
ax.set_title("3. Hopping Window", fontsize=12, weight="bold")
draw_events(ax, [1.5, 2.5, 4.5, 6.2, 7.8])
for start in [0, 2.5, 5]:
    rect = patches.Rectangle((start, 0.3), 5, 0.4, fill=False, edgecolor="green", linestyle="--")
    ax.add_patch(rect)
ax.set_xlim(0, 10)
ax.set_ylim(0, 1)
ax.axis("off")

# 4. Sliding Window
ax = axes[3]
ax.set_title("4. Sliding Window", fontsize=12, weight="bold")
events = [2, 4, 6, 8]
draw_events(ax, events)
for e in events:
    rect = patches.Rectangle((e-2.5, 0.3), 2.5, 0.4, fill=False, edgecolor="orange", linestyle="--")
    ax.add_patch(rect)
ax.set_xlim(0, 10)
ax.set_ylim(0, 1)
ax.axis("off")

# 5. Session Window
ax = axes[4]
ax.set_title("5. Session Window", fontsize=12, weight="bold")
draw_events(ax, [1, 2, 4, 10, 11])
# Sessão 1
rect = patches.Rectangle((0.5, 0.3), 4, 0.4, fill=False, edgecolor="purple", linestyle="--")
ax.add_patch(rect)
# Sessão 2
rect = patches.Rectangle((9.5, 0.3), 2, 0.4, fill=False, edgecolor="purple", linestyle="--")
ax.add_patch(rect)
ax.set_xlim(0, 15)
ax.set_ylim(0, 1)
ax.axis("off")

# 6. Snapshot Window
ax = axes[5]
ax.set_title("6. Snapshot Window", fontsize=12, weight="bold")
draw_events(ax, [1, 3, 5, 7, 9])
ax.axvline(7, color="red", linestyle="--")
ax.text(7, 0.7, "Snapshot @7", color="red", ha="center")
ax.set_xlim(0, 10)
ax.set_ylim(0, 1)
ax.axis("off")

plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
