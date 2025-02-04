filename = 'connection_pool'
n = 5000

array A[n]

samples(x) = $0 > (n-1) ? n : int($0+1)
mod(x) = int(x) % n
avg_n(x) = (A[mod($0)+1]=x, (sum [i=1:samples($0)] A[i]) / samples($0))

set term x11

set title 'Apollo Pool Size'
set autoscale y
set autoscale x
set datafile sep ','
set ylabel 'Connections'
set xlabel 'Time'
set xdata time
set timefmt '%Y-%m-%dT%H:%M:%S'
set format x '%H:%M:%.2S'
set samples 1000
plot filename.".log" using 3:1 w l lw 1 title "Acquired Connections", \
     filename.".log" using 3:2 w l lc rgb "red" lw 3 title "Idle Connections"

while (1) {
  pause 1
  replot
}
