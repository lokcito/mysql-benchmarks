#!/bin/bash
SESSION="mysession"

# Crear nueva sesión en background
tmux new-session -d -s $SESSION

# Dividir en 4 paneles
tmux split-window -h       # divide horizontalmente
tmux split-window -v       # divide el panel de la izquierda en dos
tmux select-pane -R
tmux split-window -v       # divide el panel derecho en dos

# Opcional: enviar comandos iniciales a cada panel
tmux send-keys -t 0 'echo "Panel 0"' C-m
tmux send-keys -t 1 'echo "Panel 1"' C-m
tmux send-keys -t 2 'echo "Panel 2"' C-m
tmux send-keys -t 3 'echo "Panel 3"' C-m

# Adjuntar a la sesión
tmux attach -t $SESSION