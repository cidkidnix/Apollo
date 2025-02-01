filename = 'queue_size_filtered'

n = 5000

array A[n]

samples(x) = $0 > (n-1) ? n : int($0+1)
mod(x) = int(x) % n
avg_n(x) = (A[mod($0)+1]=x, (sum [i=1:samples($0)] A[i]) / samples($0))

set term x11

set title 'Apollo Queue Size Over Time'
set autoscale y
set autoscale x
set datafile sep ','
set xdata time
set timefmt '%Y-%m-%dT%H:%M:%S'
set format x '%H:%M:%.2S'
set ylabel 'Queue Size'
set xlabel 'Time'
set samples 1000
plot filename.".log" using 2:1 w l lw 1 title "Queue Size", \
     filename.".log" using 2:(avg_n($1)) w l lc rgb "red" lw 3 title "Average Queue Size"
while (1) {
  pause 1
  replot
}
